use std::fs;
use std::path::Path;

use tokio_postgres::NoTls;

use pg_snapshot_reader::{
    ClickHouseConfig, ClickHouseSnapshotRowWriter, count_clickhouse_rows,
    create_clickhouse_snapshot_table, create_local_snapshot_boundary, discover_table_schema,
    execute_clickhouse_query, read_snapshot_rows_full_with_stage_and_checkpoint,
    save_snapshot_boundary, write_staged_snapshot_rows,
};

const POSTGRES_CONNECTION_STRING: &str =
    "host=localhost port=5432 user=postgres password=postgres dbname=snapshot_demo";

const SOURCE_TABLE: &str = "users";
const CLICKHOUSE_TABLE: &str = "users_snapshot";

const CLICKHOUSE_URL: &str = "http://localhost:8123";
const CLICKHOUSE_DATABASE: &str = "snapshot_demo";
const CLICKHOUSE_USER: &str = "snapshot_user";
const CLICKHOUSE_PASSWORD: &str = "snapshot_password";

const STAGE_FILE: &str = "users_snapshot_stage.jsonl";
const CHECKPOINT_FILE: &str = "users_snapshot_checkpoint.json";
const BOUNDARY_FILE: &str = "users_snapshot_boundary.json";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(POSTGRES_CONNECTION_STRING, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let schema = discover_table_schema(&client, SOURCE_TABLE).await?;

    let clickhouse_config = ClickHouseConfig {
        url: CLICKHOUSE_URL.to_string(),
        database: CLICKHOUSE_DATABASE.to_string(),
        user: CLICKHOUSE_USER.to_string(),
        password: CLICKHOUSE_PASSWORD.to_string(),
    };

    create_clickhouse_snapshot_table(&clickhouse_config, &schema, CLICKHOUSE_TABLE).await?;

    println!("clickhouse table ensured: {}", CLICKHOUSE_TABLE);

    execute_clickhouse_query(
        &clickhouse_config,
        &format!("TRUNCATE TABLE {}", CLICKHOUSE_TABLE),
    )
    .await?;

    println!("clickhouse table truncated: {}", CLICKHOUSE_TABLE);

    let stage_path = Path::new(STAGE_FILE);
    let checkpoint_path = Path::new(CHECKPOINT_FILE);
    let boundary_path = Path::new(BOUNDARY_FILE);

    if stage_path.exists() {
        fs::remove_file(stage_path)?;
    }

    if checkpoint_path.exists() {
        fs::remove_file(checkpoint_path)?;
    }

    if boundary_path.exists() {
        fs::remove_file(boundary_path)?;
    }

    let boundary = create_local_snapshot_boundary(SOURCE_TABLE);
    save_snapshot_boundary(boundary_path, &boundary)?;

    println!("snapshot boundary written to: {}", boundary_path.display());

    let rows = read_snapshot_rows_full_with_stage_and_checkpoint(
        &client,
        &schema,
        2,
        stage_path,
        checkpoint_path,
    )
    .await?;

    println!("snapshot rows read: {}", rows.len());
    println!("stage written to: {}", stage_path.display());
    println!("checkpoint written to: {}", checkpoint_path.display());

    let writer = ClickHouseSnapshotRowWriter {
        config: clickhouse_config,
        table_name: CLICKHOUSE_TABLE.to_string(),
    };

    write_staged_snapshot_rows(stage_path, &writer).await?;

    let clickhouse_row_count = count_clickhouse_rows(&writer.config, CLICKHOUSE_TABLE).await?;

    println!("clickhouse rows written: {}", clickhouse_row_count);

    if clickhouse_row_count != rows.len() as u64 {
        anyhow::bail!(
            "row count mismatch: snapshot read {} rows but ClickHouse has {} rows",
            rows.len(),
            clickhouse_row_count
        );
    }

    Ok(())
}
