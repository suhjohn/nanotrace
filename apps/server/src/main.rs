mod config;
mod dashboards;
mod event;
mod event_log;
mod facets;
mod http;
mod processors;
mod read;
mod retention;
mod uploader;

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use nanotrace_processor_runtime::{ProcessorRuntime, ProcessorSyncConfig};

use crate::{
    config::Config, dashboards::DashboardStore, event_log::EventLogWriter, facets::FacetStore,
    http::AppState, processors::ProcessorStore, read::ReadStore,
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
    let s3 = aws_sdk_s3::Client::new(&aws_config);
    let ses = aws_sdk_sesv2::Client::new(&aws_config);
    let upload_processors = match cfg.s3_bucket.clone() {
        Some(bucket) => ProcessorRuntime::start(
            s3.clone(),
            ProcessorSyncConfig {
                bucket,
                prefix: cfg.processor_prefix.clone(),
                interval: cfg.processor_poll_interval,
                root: std::path::PathBuf::from("/tmp/nanotrace-upload-processors"),
                stage: "upload".to_string(),
            },
        ),
        None => ProcessorRuntime::identity(),
    };
    let writer = Arc::new(EventLogWriter::new(cfg.clone()).await?);
    let read_store = Arc::new(ReadStore::new(cfg.clone(), s3.clone()));
    let facet_store = Arc::new(FacetStore::connect(cfg.clone()).await?);
    let dashboard_store = Arc::new(DashboardStore::connect(cfg.clone()).await?);
    let processor_store = Arc::new(ProcessorStore::new(cfg.clone(), s3));

    {
        let facet_store = facet_store.clone();
        tokio::spawn(async move { facet_store.run_backfill_worker().await });
    }

    {
        let cfg = cfg.clone();
        tokio::spawn(async move { uploader::run(cfg, upload_processors).await });
    }

    {
        let cfg = cfg.clone();
        tokio::spawn(async move { retention::run(cfg).await });
    }

    {
        let writer = writer.clone();
        let rotate_after = cfg.rotate_after;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(rotate_after.min(std::time::Duration::from_secs(10)));
            loop {
                interval.tick().await;
                if let Err(err) = writer.rotate_if_old().await {
                    error!(error = %err, "failed to rotate event file");
                }
            }
        });
    }

    let app = http::router(AppState {
        cfg: cfg.clone(),
        auth,
        dashboards: dashboard_store.clone(),
        facets: facet_store.clone(),
        processors: processor_store.clone(),
        read: read_store.clone(),
        ses,
        writer: writer.clone(),
    });
    let address = format!("0.0.0.0:{}", cfg.port);
    let listener = TcpListener::bind(&address).await?;
    info!(address, "nanotrace server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    writer.flush().await?;

    Ok(())
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
