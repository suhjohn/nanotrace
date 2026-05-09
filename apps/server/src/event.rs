use serde::Deserialize;
use serde_json::{Map, Value};

use crate::event_log::WriteReceipt;

#[derive(Debug)]
pub struct IncomingEvent {
    pub event_id: String,
    pub timestamp: String,
    pub observed_timestamp: Option<String>,
    pub data: Map<String, Value>,
}

#[derive(Debug)]
pub enum IncomingEvents {
    Single(IncomingEvent),
    Batch(Vec<IncomingEvent>),
}

#[derive(Debug)]
pub struct PreparedEvents {
    pub is_batch: bool,
    pub events: Vec<IncomingEvent>,
}

impl IncomingEvents {
    pub fn is_batch(&self) -> bool {
        matches!(self, Self::Batch(_))
    }

    pub fn into_vec(self) -> Vec<IncomingEvent> {
        match self {
            Self::Single(event) => vec![event],
            Self::Batch(events) => events,
        }
    }
}

impl PreparedEvents {
    pub fn len(&self) -> usize {
        self.events.len()
    }
}

#[derive(Debug, Deserialize)]
struct RawIncomingEvent {
    event_id: Option<Value>,
    timestamp: Option<Value>,
    #[serde(default)]
    observed_timestamp: Option<Value>,
    data: Option<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("invalid JSON body: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("batch must contain at least one event")]
    EmptyBatch,
    #[error("event must be a JSON object")]
    InvalidEvent,
    #[error("{0} must be a non-empty string")]
    InvalidStringField(&'static str),
    #[error("data must be a JSON object")]
    InvalidData,
    #[error("event is too large")]
    TooLarge,
    #[error("serialized event length exceeds UInt32")]
    LengthOverflow,
    #[error("failed to serialize event: {0}")]
    Serialize(serde_json::Error),
    #[error("processor returned invalid event JSON: {0}")]
    ProcessorJson(serde_json::Error),
    #[error("processor returned event without string event_id")]
    ProcessorMissingEventID,
}

pub fn parse_events(bytes: &[u8]) -> Result<IncomingEvents, EventError> {
    match serde_json::from_slice::<Value>(bytes)? {
        Value::Array(values) => {
            if values.is_empty() {
                return Err(EventError::EmptyBatch);
            }
            values
                .into_iter()
                .map(event_from_value)
                .collect::<Result<Vec<_>, _>>()
                .map(IncomingEvents::Batch)
        }
        value => event_from_value(value).map(IncomingEvents::Single),
    }
}

pub fn prepare_events(bytes: &[u8]) -> Result<PreparedEvents, EventError> {
    let events = parse_events(bytes)?;
    Ok(PreparedEvents {
        is_batch: events.is_batch(),
        events: events.into_vec(),
    })
}

pub fn serialize_line(
    event: &IncomingEvent,
    source_file: String,
    source_offset: u64,
    max_event_bytes: usize,
) -> Result<(Vec<u8>, WriteReceipt), EventError> {
    serialize_row(
        event_row(event),
        source_file,
        source_offset,
        max_event_bytes,
    )
}

pub fn restamp_ndjson(
    bytes: &[u8],
    source_file: &str,
    max_event_bytes: usize,
) -> Result<Vec<u8>, EventError> {
    let mut output = Vec::with_capacity(bytes.len());
    let mut offset = 0u64;

    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let row = match serde_json::from_slice(line).map_err(EventError::ProcessorJson)? {
            Value::Object(row) => row,
            _ => return Err(EventError::InvalidEvent),
        };
        let (line, _) = serialize_row(row, source_file.to_string(), offset, max_event_bytes)?;
        offset += line.len() as u64;
        output.extend_from_slice(&line);
    }

    Ok(output)
}

fn serialize_row(
    mut row: Map<String, Value>,
    source_file: String,
    source_offset: u64,
    max_event_bytes: usize,
) -> Result<(Vec<u8>, WriteReceipt), EventError> {
    row.insert("source_file".to_owned(), Value::String(source_file.clone()));
    row.insert("source_offset".to_owned(), source_offset.into());
    row.insert("source_length".to_owned(), 0.into());

    let line = serialize_with_source_length(&mut row)?;
    if line.len() > max_event_bytes {
        return Err(EventError::TooLarge);
    }

    let source_length = u32::try_from(line.len()).map_err(|_| EventError::LengthOverflow)?;
    let event_id = row
        .get("event_id")
        .and_then(Value::as_str)
        .ok_or(EventError::ProcessorMissingEventID)?
        .to_string();
    Ok((
        line,
        WriteReceipt {
            event_id,
            source_file,
            source_offset,
            source_length,
        },
    ))
}

fn event_row(event: &IncomingEvent) -> Map<String, Value> {
    let mut row = Map::new();
    row.insert("event_id".to_owned(), Value::String(event.event_id.clone()));
    row.insert(
        "timestamp".to_owned(),
        Value::String(event.timestamp.clone()),
    );
    if let Some(observed_timestamp) = &event.observed_timestamp {
        row.insert(
            "observed_timestamp".to_owned(),
            Value::String(observed_timestamp.clone()),
        );
    }
    row.insert("data".to_owned(), Value::Object(event.data.clone()));
    row
}

fn event_from_value(value: Value) -> Result<IncomingEvent, EventError> {
    if !value.is_object() {
        return Err(EventError::InvalidEvent);
    }

    let raw: RawIncomingEvent = serde_json::from_value(value)?;
    let event_id = required_string(raw.event_id, "event_id")?;
    let timestamp = required_string(raw.timestamp, "timestamp")?;
    let observed_timestamp = optional_string(raw.observed_timestamp, "observed_timestamp")?;
    let data = match raw.data {
        Some(Value::Object(data)) => data,
        Some(_) | None => return Err(EventError::InvalidData),
    };

    Ok(IncomingEvent {
        event_id,
        timestamp,
        observed_timestamp,
        data,
    })
}

fn required_string(value: Option<Value>, key: &'static str) -> Result<String, EventError> {
    optional_string(value, key)?.ok_or(EventError::InvalidStringField(key))
}

fn optional_string(value: Option<Value>, key: &'static str) -> Result<Option<String>, EventError> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value)),
        Some(_) => Err(EventError::InvalidStringField(key)),
        None => Ok(None),
    }
}

fn serialize_with_source_length(row: &mut Map<String, Value>) -> Result<Vec<u8>, EventError> {
    loop {
        let mut line = serde_json::to_vec(row).map_err(EventError::Serialize)?;
        line.push(b'\n');
        let source_length = u32::try_from(line.len()).map_err(|_| EventError::LengthOverflow)?;

        if row.get("source_length").and_then(Value::as_u64) == Some(u64::from(source_length)) {
            return Ok(line);
        }

        row.insert("source_length".to_owned(), source_length.into());
    }
}

#[cfg(test)]
mod tests {
    use super::{IncomingEvents, parse_events, restamp_ndjson, serialize_line};

    #[test]
    fn rejects_empty_event() {
        let err = parse_events(b"{}").expect_err("empty event is invalid");

        assert_eq!(err.to_string(), "event_id must be a non-empty string");
    }

    #[test]
    fn accepts_batch() {
        let events = parse_events(
            br#"[
                {
                    "event_id": "evt_1",
                    "timestamp": "2026-05-10T12:34:56.789Z",
                    "data": {}
                },
                {
                    "event_id": "evt_2",
                    "timestamp": "2026-05-10T12:34:57.000Z",
                    "data": {"event_type":"metric"}
                }
            ]"#,
        )
        .expect("batch parses");

        match events {
            IncomingEvents::Batch(events) => assert_eq!(events.len(), 2),
            IncomingEvents::Single(_) => panic!("expected batch"),
        }
    }

    #[test]
    fn rejects_empty_batch() {
        let err = parse_events(b"[]").expect_err("empty batch is invalid");

        assert_eq!(err.to_string(), "batch must contain at least one event");
    }

    #[test]
    fn uses_data_when_present() {
        let events = parse_events(
            br#"{
                "event_id": "evt_1",
                "timestamp": "2026-05-10T12:34:56.789Z",
                "tenant_id": "ignored-top-level",
                "data": {"tenant_id": "tenant-a", "event_type": "log"}
            }"#,
        )
        .expect("event parses");
        let event = events.into_vec().remove(0);
        let (line, receipt) =
            serialize_line(&event, "events/test.ndjson".to_string(), 42, usize::MAX)
                .expect("event serializes");
        let row: serde_json::Value = serde_json::from_slice(&line).expect("line is JSON");

        assert_eq!(receipt.event_id, "evt_1");
        assert_eq!(row["event_id"], "evt_1");
        assert_eq!(row["data"]["tenant_id"], "tenant-a");
        assert!(row.get("tenant_id").is_none());
        assert_eq!(row["source_offset"], 42);
        assert_eq!(row["source_length"], line.len());
    }

    #[test]
    fn rejects_missing_data() {
        let err = parse_events(
            br#"{
                "event_id": "evt_1",
                "timestamp": "2026-05-10T12:34:56.789Z"
            }"#,
        )
        .expect_err("missing data is invalid");

        assert_eq!(err.to_string(), "data must be a JSON object");
    }

    #[test]
    fn restamps_processed_ndjson() {
        let output = restamp_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-10T12:34:56.789Z","source_file":"old","source_offset":999,"source_length":999,"data":{"kept":true}}"#,
            "events/final.ndjson",
            usize::MAX,
        )
        .expect("restamp succeeds");
        let row: serde_json::Value = serde_json::from_slice(&output).expect("line is JSON");

        assert_eq!(row["event_id"], "evt_1");
        assert_eq!(row["source_file"], "events/final.ndjson");
        assert_eq!(row["source_offset"], 0);
        assert_eq!(row["source_length"], output.len());
    }
}
