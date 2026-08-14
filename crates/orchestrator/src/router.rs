//! RuleRouter: deterministic routing from a task intent to one agent.
//!
//! Phase 9 routing is purely config-driven:
//!
//! 1. map the intent to a skill
//! 2. walk the config's preferred agent order for that intent
//! 3. the agent must exist in the directory and its card must declare the skill
//! 4. no preferred agent available → fall back to any capable agent
//! 5. none → [`RouteDecision::NoCapableAgent`]
//!
//! The router never branches on agent brand; it only reads the config and the
//! directory (which is built from Agent Cards).

use agentmesh_core::{RoutingConfig, TaskIntent};

use crate::directory::{AgentDirectory, AgentHealth};

/// Outcome of a routing decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Agent { agent_id: String, reason: String },
    NoCapableAgent { skill: String },
}

/// Deterministic rule router.
#[derive(Debug, Clone)]
pub struct RuleRouter {
    config: RoutingConfig,
}

impl RuleRouter {
    pub fn new(config: RoutingConfig) -> Self {
        Self { config }
    }

    /// Routing configuration backing this router.
    pub fn config(&self) -> &RoutingConfig {
        &self.config
    }

    /// Pick an agent for an intent following the phase-9 rules.
    pub fn route(&self, directory: &AgentDirectory, intent: TaskIntent) -> RouteDecision {
        self.route_with_constraints(directory, intent, &[])
    }

    /// Pick an agent for an intent, excluding the given agent ids (Phase 21 §4).
    ///
    /// Used by parallel evaluation groups so each evaluator is a distinct
    /// agent — never the same session counted as multiple votes. The exclusion
    /// applies to both the preferred order and the fallback. Agents are still
    /// chosen by Card + routing config, never by brand.
    pub fn route_with_constraints(
        &self,
        directory: &AgentDirectory,
        intent: TaskIntent,
        excluded: &[String],
    ) -> RouteDecision {
        let skill = intent.skill();

        // Preferred order from config, filtered by skill + online + exclusion.
        for agent_id in self.config.preferred(intent) {
            if excluded.iter().any(|e| e == agent_id) {
                continue;
            }
            if let Some(entry) = directory.get(agent_id)
                && entry.health == AgentHealth::Online
                && entry.has_skill(skill)
            {
                return RouteDecision::Agent {
                    agent_id: agent_id.clone(),
                    reason: format!("preferred agent with skill `{skill}`"),
                };
            }
        }

        // Fallback: any capable, online, non-excluded agent (deterministic).
        if let Some(entry) = directory
            .find_by_skill(skill)
            .into_iter()
            .find(|e| !excluded.iter().any(|x| x == &e.agent_id))
        {
            return RouteDecision::Agent {
                agent_id: entry.agent_id.clone(),
                reason: format!("fallback agent with skill `{skill}`"),
            };
        }

        RouteDecision::NoCapableAgent {
            skill: skill.to_string(),
        }
    }

    /// Explicit `--agent` override: bypasses routing but still validates that
    /// the agent exists, is online and has a fetched (valid) A2A card.
    pub fn explicit(
        &self,
        directory: &AgentDirectory,
        agent_id: &str,
    ) -> Result<RouteDecision, crate::error::OrchestratorError> {
        let entry = directory
            .get(agent_id)
            .ok_or_else(|| crate::error::OrchestratorError::AgentNotFound(agent_id.to_string()))?;
        if entry.health != AgentHealth::Online {
            return Err(crate::error::OrchestratorError::AgentOffline(
                agent_id.to_string(),
            ));
        }
        Ok(RouteDecision::Agent {
            agent_id: agent_id.to_string(),
            reason: "explicit --agent override".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::tests::entry;
    use crate::directory::{AgentDirectory, AgentHealth};

    fn directory(entries: &[(&str, &[&str], AgentHealth)]) -> AgentDirectory {
        let mut directory = AgentDirectory::new();
        for (agent_id, skills, health) in entries {
            directory.insert(entry(agent_id, skills, *health));
        }
        directory
    }

    fn config_with(implementation: &[&str], testing: &[&str], review: &[&str]) -> RoutingConfig {
        RoutingConfig {
            implementation: implementation.iter().map(|s| s.to_string()).collect(),
            testing: testing.iter().map(|s| s.to_string()).collect(),
            review: review.iter().map(|s| s.to_string()).collect(),
            ..RoutingConfig::default()
        }
    }

    #[test]
    fn preferred_order_wins_when_skill_matches() {
        let directory = directory(&[
            ("alpha", &["code"], AgentHealth::Online),
            ("beta", &["code"], AgentHealth::Online),
        ]);
        // beta preferred for implementation; both have `code`.
        let router = RuleRouter::new(config_with(&["beta", "alpha"], &[], &[]));
        let decision = router.route(&directory, TaskIntent::Implementation);
        assert_eq!(
            decision,
            RouteDecision::Agent {
                agent_id: "beta".into(),
                reason: "preferred agent with skill `code`".into()
            }
        );
    }

    #[test]
    fn preferred_agent_missing_skill_is_skipped() {
        // alpha is preferred for testing but its card lacks the skill; beta
        // (also preferred, later) has it.
        let directory = directory(&[
            ("alpha", &["code"], AgentHealth::Online),
            ("beta", &["testing"], AgentHealth::Online),
        ]);
        let router = RuleRouter::new(config_with(&[], &["alpha", "beta"], &[]));
        let decision = router.route(&directory, TaskIntent::Testing);
        assert_eq!(
            decision,
            RouteDecision::Agent {
                agent_id: "beta".into(),
                reason: "preferred agent with skill `testing`".into()
            }
        );
    }

    #[test]
    fn offline_preferred_agent_is_skipped() {
        // alpha (preferred) is offline; beta is the fallback capable agent.
        let directory = directory(&[
            ("alpha", &["testing"], AgentHealth::Offline),
            ("beta", &["testing"], AgentHealth::Online),
        ]);
        let router = RuleRouter::new(config_with(&[], &["alpha"], &[]));
        let decision = router.route(&directory, TaskIntent::Testing);
        assert_eq!(
            decision,
            RouteDecision::Agent {
                agent_id: "beta".into(),
                reason: "fallback agent with skill `testing`".into()
            }
        );
    }

    #[test]
    fn absent_preferred_agent_falls_back() {
        // Preferred agent not in directory at all; an unlisted capable agent
        // is chosen by fallback.
        let directory = directory(&[("zeta", &["review"], AgentHealth::Online)]);
        let router = RuleRouter::new(config_with(&[], &[], &["alpha", "claude"]));
        let decision = router.route(&directory, TaskIntent::Review);
        assert_eq!(
            decision,
            RouteDecision::Agent {
                agent_id: "zeta".into(),
                reason: "fallback agent with skill `review`".into()
            }
        );
    }

    #[test]
    fn no_capable_agent_when_nothing_matches() {
        let directory = directory(&[
            ("alpha", &["code"], AgentHealth::Online),
            ("beta", &["debug"], AgentHealth::Online),
        ]);
        let router = RuleRouter::new(config_with(&["alpha"], &[], &[]));
        let decision = router.route(&directory, TaskIntent::Review);
        assert_eq!(
            decision,
            RouteDecision::NoCapableAgent {
                skill: "review".into()
            }
        );
    }

    #[test]
    fn no_brand_specific_logic() {
        // Agent ids are opaque to the router: an arbitrary id wins when the
        // config prefers it and its card declares the skill.
        let directory = directory(&[
            ("thing-1", &["code"], AgentHealth::Online),
            ("thing-2", &["code"], AgentHealth::Online),
        ]);
        let router = RuleRouter::new(config_with(&["thing-2", "thing-1"], &[], &[]));
        let decision = router.route(&directory, TaskIntent::Implementation);
        assert_eq!(
            decision,
            RouteDecision::Agent {
                agent_id: "thing-2".into(),
                reason: "preferred agent with skill `code`".into()
            }
        );
    }

    #[test]
    fn constraints_skip_excluded_agents() {
        // Phase 21 §4: excluded agents are skipped in both the preferred order
        // and the fallback, so parallel evaluators get distinct agents.
        let directory = directory(&[
            ("alpha", &["review"], AgentHealth::Online),
            ("beta", &["review"], AgentHealth::Online),
            ("gamma", &["review"], AgentHealth::Online),
        ]);
        let router = RuleRouter::new(config_with(&[], &[], &["alpha", "beta", "gamma"]));
        let first = router.route(&directory, TaskIntent::Review);
        assert_eq!(
            first,
            RouteDecision::Agent {
                agent_id: "alpha".into(),
                reason: "preferred agent with skill `review`".into()
            }
        );
        let second =
            router.route_with_constraints(&directory, TaskIntent::Review, &["alpha".to_string()]);
        assert_eq!(
            second,
            RouteDecision::Agent {
                agent_id: "beta".into(),
                reason: "preferred agent with skill `review`".into()
            }
        );
        // All preferred excluded → fallback still honors the exclusion.
        let third = router.route_with_constraints(
            &directory,
            TaskIntent::Review,
            &["alpha".to_string(), "beta".to_string()],
        );
        assert_eq!(
            third,
            RouteDecision::Agent {
                agent_id: "gamma".into(),
                reason: "preferred agent with skill `review`".into()
            }
        );
    }

    #[test]
    fn constraints_all_excluded_is_no_capable_agent() {
        let directory = directory(&[("alpha", &["review"], AgentHealth::Online)]);
        let router = RuleRouter::new(config_with(&[], &[], &["alpha"]));
        let decision =
            router.route_with_constraints(&directory, TaskIntent::Review, &["alpha".to_string()]);
        assert_eq!(
            decision,
            RouteDecision::NoCapableAgent {
                skill: "review".into()
            }
        );
    }

    #[test]
    fn explicit_agent_bypasses_routing_but_validates() {
        let directory = directory(&[
            ("alpha", &["code"], AgentHealth::Online),
            ("offline", &["code"], AgentHealth::Offline),
        ]);
        let router = RuleRouter::new(RoutingConfig::default());

        let ok = router.explicit(&directory, "alpha").expect("explicit");
        assert_eq!(
            ok,
            RouteDecision::Agent {
                agent_id: "alpha".into(),
                reason: "explicit --agent override".into()
            }
        );

        assert!(matches!(
            router.explicit(&directory, "nope"),
            Err(crate::OrchestratorError::AgentNotFound(_))
        ));
        assert!(matches!(
            router.explicit(&directory, "offline"),
            Err(crate::OrchestratorError::AgentOffline(_))
        ));
    }
}
