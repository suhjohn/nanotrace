use std::sync::Arc;

use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::Config;

#[derive(Clone)]
pub struct ReadStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
    s3: S3Client,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub parameters: serde_json::Map<String, Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("ClickHouse is not configured")]
    ClickHouseNotConfigured,
    #[error("S3 bucket is not configured")]
    S3NotConfigured,
    #[error("{0}")]
    InvalidQuery(String),
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("event not found")]
    NotFound,
    #[error("event source range is missing")]
    MissingSourceRange,
    #[error("S3 get object failed: {0}")]
    S3(String),
    #[error("invalid stored event JSON: {0}")]
    InvalidStoredEvent(serde_json::Error),
    #[error("stored event_id does not match requested event_id")]
    EventIDMismatch,
    #[error("invalid ClickHouse response")]
    InvalidClickHouseResponse,
}

impl ReadStore {
    pub fn new(cfg: Arc<Config>, s3: S3Client) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
            s3,
        }
    }

    pub async fn query(&self, request: QueryRequest, tenant_id: &str) -> Result<Value, ReadError> {
        let query = checked_select_query(&request.query)?;
        validate_query_sources(&query, &self.allowed_table_names())?;
        let query = self.scope_query(&query);
        let mut parameters = request.parameters;
        parameters.insert(
            "__nanotrace_tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let text = self.clickhouse_query(&query, &parameters).await?;
        serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)
    }

    pub async fn event_bytes(&self, event_id: &str, tenant_id: &str) -> Result<Bytes, ReadError> {
        if event_id.trim().is_empty() {
            return Err(ReadError::InvalidQuery("event_id is required".to_string()));
        }

        let pointer = self.event_pointer(event_id, tenant_id).await?;
        let bucket = self
            .cfg
            .s3_bucket
            .as_deref()
            .ok_or(ReadError::S3NotConfigured)?;
        let end = pointer
            .source_offset
            .checked_add(u64::from(pointer.source_length))
            .and_then(|value| value.checked_sub(1))
            .ok_or(ReadError::MissingSourceRange)?;
        let range = format!("bytes={}-{}", pointer.source_offset, end);

        let response = self
            .s3
            .get_object()
            .bucket(bucket)
            .key(&pointer.source_file)
            .range(range)
            .send()
            .await
            .map_err(|err| ReadError::S3(err.to_string()))?;
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|err| ReadError::S3(err.to_string()))?
            .into_bytes();

        validate_event_bytes(event_id, &bytes)?;
        Ok(bytes)
    }

    async fn event_pointer(
        &self,
        event_id: &str,
        tenant_id: &str,
    ) -> Result<EventPointer, ReadError> {
        let mut parameters = serde_json::Map::new();
        parameters.insert("event_id".to_string(), Value::String(event_id.to_string()));
        parameters.insert(
            "__nanotrace_tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let query = format!(
            "SELECT source_file, source_offset, source_length FROM {} WHERE tenant_id = {{__nanotrace_tenant_id:String}} AND event_id = {{event_id:String}} ORDER BY timestamp ASC, source_file ASC, source_offset ASC LIMIT 1",
            self.table_name()
        );
        let text = self.clickhouse_query(&query, &parameters).await?;
        let response: ClickHouseResponse<EventPointer> =
            serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)?;
        match response.data.len() {
            0 => Err(ReadError::NotFound),
            1 => {
                let pointer = response
                    .data
                    .into_iter()
                    .next()
                    .ok_or(ReadError::InvalidClickHouseResponse)?;
                if pointer.source_file.is_empty() || pointer.source_length == 0 {
                    return Err(ReadError::MissingSourceRange);
                }
                Ok(pointer)
            }
            _ => Err(ReadError::InvalidClickHouseResponse),
        }
    }

    async fn clickhouse_query(
        &self,
        query: &str,
        parameters: &serde_json::Map<String, Value>,
    ) -> Result<String, ReadError> {
        let url = self
            .cfg
            .clickhouse_url
            .as_deref()
            .ok_or(ReadError::ClickHouseNotConfigured)?;
        let mut request = self
            .http
            .post(url)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("readonly", "1"),
                ("type_json_skip_duplicated_paths", "1"),
                (
                    "type_json_allow_duplicated_key_with_literal_and_nested_object",
                    "1",
                ),
                (
                    "max_execution_time",
                    &self.cfg.clickhouse_max_execution_secs.to_string(),
                ),
                (
                    "max_result_rows",
                    &self.cfg.clickhouse_max_result_rows.to_string(),
                ),
                ("result_overflow_mode", "break"),
                (
                    "max_bytes_to_read",
                    &self.cfg.clickhouse_max_bytes_to_read.to_string(),
                ),
            ])
            .body(format!("{query} FORMAT JSON"));

        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }

        for (key, value) in parameters {
            validate_parameter_name(key)?;
            request = request.query(&[(format!("param_{key}"), parameter_value(value)?)]);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(ReadError::ClickHouseResponse { status, body });
        }
        Ok(body)
    }

    fn table_name(&self) -> String {
        format!(
            "{}.{}",
            self.cfg.clickhouse_database, self.cfg.clickhouse_table
        )
    }

    fn scope_query(&self, query: &str) -> String {
        let tables = self.allowed_table_names();
        let mut scoped = query.to_string();
        for table in tables.iter() {
            let scoped_table = format!(
                "(SELECT * FROM {table} WHERE tenant_id = {{__nanotrace_tenant_id:String}})"
            );
            for keyword in ["FROM", "JOIN"] {
                scoped = scoped.replace(
                    &format!("{keyword} {table}"),
                    &format!("{keyword} {scoped_table}"),
                );
                scoped = scoped.replace(
                    &format!("{} {table}", keyword.to_ascii_lowercase()),
                    &format!("{} {scoped_table}", keyword.to_ascii_lowercase()),
                );
            }
        }
        scoped
    }

    fn allowed_table_names(&self) -> Vec<String> {
        [
            self.cfg.clickhouse_table.as_str(),
            self.cfg.clickhouse_facets_table.as_str(),
            self.cfg.clickhouse_event_index_table.as_str(),
            self.cfg.clickhouse_hot_dimensions_table.as_str(),
        ]
        .into_iter()
        .flat_map(|table| {
            [
                table.to_string(),
                format!("{}.{}", self.cfg.clickhouse_database, table),
            ]
        })
        .collect()
    }
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct EventPointer {
    source_file: String,
    source_offset: u64,
    source_length: u32,
}

fn checked_select_query(query: &str) -> Result<String, ReadError> {
    let query = trim_single_statement(query)?;
    let tokens = sql_tokens(&query);
    let first_keyword = tokens
        .first()
        .map(String::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    if first_keyword != "SELECT" && first_keyword != "WITH" {
        return Err(ReadError::InvalidQuery(
            "query must start with SELECT or WITH".to_string(),
        ));
    }

    if tokens.iter().any(|token| token == "FORMAT") {
        return Err(ReadError::InvalidQuery(
            "query must not include FORMAT; the server adds FORMAT JSON".to_string(),
        ));
    }
    for forbidden in [
        "ALTER", "ATTACH", "CREATE", "DELETE", "DETACH", "DROP", "GRANT", "INSERT", "KILL",
        "OPTIMIZE", "RENAME", "REVOKE", "SET", "SYSTEM", "TRUNCATE", "USE",
    ] {
        if tokens.iter().any(|token| token == forbidden) {
            return Err(ReadError::InvalidQuery(format!(
                "query must not include {forbidden}"
            )));
        }
    }
    if query.contains('`') || query.contains('"') {
        return Err(ReadError::InvalidQuery(
            "query must not include quoted identifiers".to_string(),
        ));
    }

    Ok(query)
}

fn validate_query_sources(query: &str, allowed_tables: &[String]) -> Result<(), ReadError> {
    let code = sql_code(query);
    let bytes = code.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let Some((keyword_start, keyword)) = next_source_keyword(&code[index..]) else {
            break;
        };
        index += keyword_start + keyword.len();
        loop {
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if index >= bytes.len() || bytes[index] == b'(' {
                break;
            }
            let source_start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric()
                    || bytes[index] == b'_'
                    || bytes[index] == b'.')
            {
                index += 1;
            }
            let source = code[source_start..index].trim();
            if source.is_empty() {
                break;
            }
            if !allowed_tables
                .iter()
                .any(|allowed| source.eq_ignore_ascii_case(allowed))
            {
                return Err(ReadError::InvalidQuery(format!(
                    "query source is not allowed: {source}"
                )));
            }
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if index >= bytes.len() || bytes[index] != b',' {
                break;
            }
            index += 1;
        }
    }
    Ok(())
}

fn next_source_keyword(value: &str) -> Option<(usize, &'static str)> {
    let from = find_keyword(value, "FROM");
    let join = find_keyword(value, "JOIN");
    match (from, join) {
        (Some(from), Some(join)) if from <= join => Some((from, "FROM")),
        (Some(_), Some(join)) => Some((join, "JOIN")),
        (Some(from), None) => Some((from, "FROM")),
        (None, Some(join)) => Some((join, "JOIN")),
        (None, None) => None,
    }
}

fn find_keyword(value: &str, keyword: &str) -> Option<usize> {
    let upper = value.to_ascii_uppercase();
    let mut offset = 0;
    while let Some(index) = upper[offset..].find(keyword) {
        let absolute = offset + index;
        let before = absolute
            .checked_sub(1)
            .and_then(|idx| upper.as_bytes().get(idx))
            .copied();
        let after = upper.as_bytes().get(absolute + keyword.len()).copied();
        let before_boundary = before.is_none_or(|ch| !is_identifier_byte(ch));
        let after_boundary = after.is_none_or(|ch| !is_identifier_byte(ch));
        if before_boundary && after_boundary {
            return Some(absolute);
        }
        offset = absolute + keyword.len();
    }
    None
}

fn is_identifier_byte(value: u8) -> bool {
    value.is_ascii_alphanumeric() || value == b'_'
}

fn trim_single_statement(query: &str) -> Result<String, ReadError> {
    let mut query = query.trim();
    if query.is_empty() {
        return Err(ReadError::InvalidQuery("query is required".to_string()));
    }
    let code = sql_code(query);
    let mut semicolons = code.match_indices(';');
    if let Some((first_semicolon, _)) = semicolons.next()
        && (semicolons.next().is_some() || !code[first_semicolon + 1..].trim().is_empty())
    {
        return Err(ReadError::InvalidQuery(
            "query must contain exactly one statement".to_string(),
        ));
    }
    if let Some(stripped) = query.strip_suffix(';') {
        query = stripped.trim_end();
    }
    Ok(query.to_string())
}

fn sql_tokens(query: &str) -> Vec<String> {
    sql_code(query)
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .collect()
}

fn sql_code(query: &str) -> String {
    let chars: Vec<char> = query.chars().collect();
    let mut out = String::with_capacity(query.len());
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '-' && chars.get(i + 1) == Some(&'-') {
            out.push(' ');
            out.push(' ');
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(i + 1) == Some(&'*') {
            out.push(' ');
            out.push(' ');
            i += 2;
            while i < chars.len() {
                if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    out.push(' ');
                    out.push(' ');
                    i += 2;
                    break;
                }
                out.push(if chars[i] == '\n' { '\n' } else { ' ' });
                i += 1;
            }
            continue;
        }
        if ch == '\'' || ch == '"' || ch == '`' {
            let quote = ch;
            out.push(' ');
            i += 1;
            while i < chars.len() {
                let current = chars[i];
                out.push(if current == '\n' { '\n' } else { ' ' });
                if current == '\\' && quote != '`' && i + 1 < chars.len() {
                    i += 1;
                    out.push(if chars[i] == '\n' { '\n' } else { ' ' });
                } else if current == quote {
                    if quote == '\'' && chars.get(i + 1) == Some(&'\'') {
                        i += 1;
                        out.push(' ');
                    } else {
                        i += 1;
                        break;
                    }
                }
                i += 1;
            }
            continue;
        }

        out.push(ch);
        i += 1;
    }

    out
}

fn validate_parameter_name(name: &str) -> Result<(), ReadError> {
    let valid = !name.is_empty()
        && name.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || ch.is_ascii_alphabetic())
        });
    if valid {
        Ok(())
    } else {
        Err(ReadError::InvalidQuery(format!(
            "invalid parameter name: {name}"
        )))
    }
}

fn parameter_value(value: &Value) -> Result<String, ReadError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => Err(ReadError::InvalidQuery(
            "query parameters must not be null".to_string(),
        )),
        Value::Array(_) | Value::Object(_) => Err(ReadError::InvalidQuery(
            "query parameters must be scalar values".to_string(),
        )),
    }
}

fn validate_event_bytes(event_id: &str, bytes: &[u8]) -> Result<(), ReadError> {
    let value: Value = serde_json::from_slice(bytes).map_err(ReadError::InvalidStoredEvent)?;
    if value.get("event_id").and_then(Value::as_str) != Some(event_id) {
        return Err(ReadError::EventIDMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ReadError, checked_select_query, validate_parameter_name, validate_query_sources};

    #[test]
    fn accepts_select_and_with_queries() {
        assert_eq!(
            checked_select_query("SELECT count() FROM observatory.events;").unwrap(),
            "SELECT count() FROM observatory.events"
        );
        assert!(checked_select_query("WITH 1 AS x SELECT x").is_ok());
    }

    #[test]
    fn rejects_mutating_or_multi_statement_queries() {
        assert!(matches!(
            checked_select_query("DELETE FROM observatory.events"),
            Err(ReadError::InvalidQuery(_))
        ));
        assert!(matches!(
            checked_select_query("SELECT 1; SELECT 2"),
            Err(ReadError::InvalidQuery(_))
        ));
    }

    #[test]
    fn allows_forbidden_words_and_semicolons_inside_strings() {
        assert!(
            checked_select_query("SELECT 'delete; drop' AS message FROM observatory.events")
                .is_ok()
        );
    }

    #[test]
    fn rejects_format_clause() {
        assert!(matches!(
            checked_select_query("SELECT 1 FORMAT JSON"),
            Err(ReadError::InvalidQuery(_))
        ));
    }

    #[test]
    fn validates_parameter_names() {
        assert!(validate_parameter_name("event_id").is_ok());
        assert!(validate_parameter_name("_from").is_ok());
        assert!(validate_parameter_name("1bad").is_err());
        assert!(validate_parameter_name("bad-name").is_err());
    }

    #[test]
    fn rejects_unapproved_query_sources() {
        let allowed = vec![
            "events".to_string(),
            "observatory.events".to_string(),
            "observatory.event_facets".to_string(),
        ];
        assert!(validate_query_sources("SELECT * FROM observatory.events", &allowed).is_ok());
        assert!(
            validate_query_sources(
                "SELECT * FROM observatory.events JOIN system.tables ON 1",
                &allowed
            )
            .is_err()
        );
        assert!(validate_query_sources("SELECT * FROM numbers(10)", &allowed).is_err());
        assert!(
            validate_query_sources("SELECT * FROM observatory.events, system.tables", &allowed)
                .is_err()
        );
    }
}
