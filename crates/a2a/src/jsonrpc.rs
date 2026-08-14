//! JSON-RPC 2.0 request parsing and dispatch plumbing.

use serde_json::Value;

use crate::types::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, error_code};

/// Build a JSON-RPC error response.
pub fn error_response(id: Option<Value>, code: i64, message: &str) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        JsonRpcError {
            code,
            message: message.to_string(),
            data: None,
        },
    )
}

/// Build an error response with extra data.
pub fn error_response_with_data(
    id: Option<Value>,
    code: i64,
    message: &str,
    data: Value,
) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        JsonRpcError {
            code,
            message: message.to_string(),
            data: Some(data),
        },
    )
}

/// Parse a JSON-RPC request body; returns the error response on failure.
pub fn parse_request(body: &[u8]) -> Result<JsonRpcRequest, Box<JsonRpcResponse>> {
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(err) => {
            return Err(Box::new(error_response(
                None,
                error_code::INVALID_REQUEST,
                &format!("invalid JSON: {err}"),
            )));
        }
    };
    if !value.is_object() {
        return Err(Box::new(error_response(
            None,
            error_code::INVALID_REQUEST,
            "request must be a JSON object",
        )));
    }
    match serde_json::from_value::<JsonRpcRequest>(value) {
        Ok(request) => {
            if request.jsonrpc != "2.0" {
                return Err(Box::new(error_response(
                    request.id,
                    error_code::INVALID_REQUEST,
                    "jsonrpc must be '2.0'",
                )));
            }
            Ok(request)
        }
        Err(err) => Err(Box::new(error_response(
            None,
            error_code::INVALID_REQUEST,
            &format!("invalid request: {err}"),
        ))),
    }
}
