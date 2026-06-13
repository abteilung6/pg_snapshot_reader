use std::path::Path;

use tokio_postgres::NoTls;

use pg_snapshot_reader::{
    ClickHouseConfig, DebugSnapshotRowWriter, create_clickhouse_snapshot_table,
    discover_table_schema, read_snapshot_rows_full_with_stage_and_checkpoint,
    write_staged_snapshot_rows,
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

    let stage_path = Path::new("users_snapshot_stage.jsonl");
    let checkpoint_path = Path::new("users_snapshot_checkpoint.json");

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

    let writer = DebugSnapshotRowWriter;

    write_staged_snapshot_rows(stage_path, &writer).await?;

    Ok(())
}
