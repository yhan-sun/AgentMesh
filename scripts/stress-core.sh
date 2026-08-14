#!/usr/bin/env bash
set -euo pipefail

ROUNDS="${1:-20}"
echo "============================================================"
echo " Starting AgentMesh 1.0 Core Concurrency Stress Suite"
echo " Target Rounds: $ROUNDS"
echo "============================================================"

TESTS=(
    "--test dag_workflow"
    "--test competition"
    "--test consensus_fix_loop"
    "--test recovery"
    "--test replan"
    "--test evaluation"
    "--test provenance"
    "--test apply_e2e"
    "--test cleanup_e2e"
)

for i in $(seq 1 "$ROUNDS"); do
    echo -n "[Round $i/$ROUNDS] Running core concurrency tests... "
    for test_target in "${TESTS[@]}"; do
        LOG_FILE=$(mktemp)
        # shellcheck disable=SC2086
        if ! cargo test -p agentmesh-daemon $test_target -- --quiet > "$LOG_FILE" 2>&1; then
            echo "FAILED on $test_target in round $i:"
            cat "$LOG_FILE"
            rm -f "$LOG_FILE"
            exit 1
        fi
        rm -f "$LOG_FILE"
    done
    echo "✓ PASS"
done

echo "============================================================"
echo " All $ROUNDS/$ROUNDS stress test rounds PASSED with ZERO flakes!"
echo "============================================================"
