mod config;
mod definitions;
mod http;
mod materializations;
mod metrics;
mod openapi;
mod read;

use std::sync::Arc;

use nanotrace_auth::{AuthStore, DEFAULT_ORGANIZATION_ID};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::Config, definitions::DefinitionStore, http::AppState,
    materializations::MaterializationStore, metrics::ServerMetrics, read::ReadStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Arc::new(Config::from_env()?);
    let auth = nanotrace_auth::AuthStore::connect(cfg.auth.clone())
        .await?
        .map(Arc::new);
    let aws_config = aws_config::load_from_env().await;
    let ses = aws_sdk_sesv2::Client::new(&aws_config);
    let raw_ingest = Arc::new(nanotrace_ingest::RawBatchProducer::new(
        nanotrace_ingest::RawBatchProducerConfig {
            brokers: cfg.kafka_brokers.clone(),
            topic: cfg.kafka_ingest_topic.clone(),
            client_id: cfg.kafka_client_id.clone(),
            timeout: cfg.kafka_produce_timeout,
        },
    )?);
    let read_store = Arc::new(ReadStore::new(Arc::new(read_config(&cfg))));
    let definition_store = Arc::new(DefinitionStore::new(cfg.clone()));
    if cfg.clickhouse_url.is_some() {
        seed_sdk_default_definitions(definition_store.as_ref(), auth.as_deref()).await;
    }
    let materialization_store = Arc::new(MaterializationStore::new(cfg.clone()));
    let metrics = Arc::new(ServerMetrics::new());

    let app = http::router(AppState {
        cfg: cfg.clone(),
        auth,
        definitions: definition_store.clone(),
        materializations: materialization_store.clone(),
        read: read_store.clone(),
        raw_ingest,
        ses,
        metrics,
    });
    let address = format!("0.0.0.0:{}", cfg.port);
    let listener = TcpListener::bind(&address).await?;
    info!(address, "nanotrace server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn seed_sdk_default_definitions(
    definition_store: &DefinitionStore,
    auth: Option<&AuthStore>,
) {
    let tenant_ids = match auth {
        Some(auth) => match auth.list_organization_ids().await {
            Ok(ids) if !ids.is_empty() => ids,
            Ok(_) => vec![DEFAULT_ORGANIZATION_ID.to_string()],
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to list organizations for SDK default definition seeding"
                );
                vec![DEFAULT_ORGANIZATION_ID.to_string()]
            }
        },
        None => vec![DEFAULT_ORGANIZATION_ID.to_string()],
    };

    for tenant_id in tenant_ids {
        match definition_store.seed_sdk_defaults(&tenant_id).await {
            Ok(definitions) => {
                info!(
                    tenant_id,
                    definitions = definitions.len(),
                    "seeded SDK default definitions"
                );
            }
            Err(err) => {
                warn!(
                    tenant_id,
                    error = %err,
                    "failed to seed SDK default definitions"
                );
            }
        }
    }
}

fn read_config(cfg: &Config) -> read::Config {
    read::Config {
        clickhouse_url: cfg.clickhouse_url.clone(),
        clickhouse_user: cfg.clickhouse_user.clone(),
        clickhouse_password: cfg.clickhouse_password.clone(),
        clickhouse_database: cfg.clickhouse_database.clone(),
        clickhouse_table: cfg.clickhouse_table.clone(),
        clickhouse_max_result_rows: cfg.clickhouse_max_result_rows,
        clickhouse_max_execution_secs: cfg.clickhouse_max_execution_secs,
        clickhouse_max_bytes_to_read: cfg.clickhouse_max_bytes_to_read,
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
