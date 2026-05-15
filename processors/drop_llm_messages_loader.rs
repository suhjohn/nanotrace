use anyhow::Result;
use serde_json::Value;

pub fn transform_event(mut event: Value, _config: &Value) -> Result<Value> {
    if let Some(data) = event.get_mut("data").and_then(Value::as_object_mut) {
        if let Some(llm) = data.get_mut("llm").and_then(Value::as_object_mut) {
            llm.remove("messages");
        }
    }
    Ok(event)
}
