use std::fs;
use std::path::Path;

use tokio_postgres::NoTls;

use pg_snapshot_reader::{
    ClickHouseConfig, ClickHouseSnapshotRowWriter, count_clickhouse_rows,
    create_clickhouse_snapshot_table, discover_table_schema,
    read_snapshot_rows_full_with_stage_and_checkpoint, write_staged_snapshot_rows,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection_string =
        "host=localhost port=5432 user=postgres password=postgres dbname=snapshot_demo";

    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let schema = discover_table_schema(&client, "users").await?;

    let clickhouse_config = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "snapshot_demo".to_string(),
        user: "snapshot_user".to_string(),
        password: "snapshot_password".to_string(),
    };

    create_clickhouse_snapshot_table(&clickhouse_config, &schema, "users_snapshot").await?;

    println!("clickhouse table ensured: users_snapshot");

    pg_snapshot_reader::execute_clickhouse_query(
        &clickhouse_config,
        "TRUNCATE TABLE users_snapshot",
    )
    .await?;

    println!("clickhouse table truncated: users_snapshot");

    let stage_path = Path::new("users_snapshot_stage.jsonl");
    let checkpoint_path = Path::new("users_snapshot_checkpoint.json");

    if stage_path.exists() {
        fs::remove_file(stage_path)?;
    }

    if checkpoint_path.exists() {
        fs::remove_file(checkpoint_path)?;
    }

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
        table_name: "users_snapshot".to_string(),
    };

    write_staged_snapshot_rows(stage_path, &writer).await?;

    let clickhouse_row_count = count_clickhouse_rows(&writer.config, "users_snapshot").await?;

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
