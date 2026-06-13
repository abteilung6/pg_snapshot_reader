use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pg_snapshot_reader::{
    ClickHouseConfig, ClickHouseSnapshotRowWriter, SnapshotCheckpoint, SnapshotValue,
    count_clickhouse_rows, create_clickhouse_snapshot_table, discover_table_schema,
    execute_clickhouse_query, read_snapshot_rows_batch, read_snapshot_rows_full,
    read_snapshot_rows_full_with_checkpoint, read_snapshot_rows_full_with_stage_and_checkpoint,
    write_staged_snapshot_rows,
};
use tokio_postgres::{Client, Error, NoTls};

static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_table_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let counter = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);

    format!("users_test_{}_{}", millis, counter)
}

async fn connect_to_postgres() -> Result<Client, Error> {
    let connection_string =
        "host=localhost port=5432 user=postgres password=postgres dbname=snapshot_demo";

    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

#[tokio::test]
async fn reads_generic_full_snapshot_in_batches() -> Result<(), Error> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            views INTEGER NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (title, views)
        VALUES
            ('First post', 10),
            ('Second post', 20),
            ('Third post', NULL)
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;
    let rows = read_snapshot_rows_full(&client, &schema, 2).await?;

    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0].values.get("title"),
        Some(&SnapshotValue::String("First post".to_string()))
    );

    assert_eq!(rows[2].values.get("views"), Some(&SnapshotValue::Null));

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn discovers_table_schema() -> Result<(), Error> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;

    assert_eq!(schema.table_name, table_name);
    assert_eq!(schema.columns.len(), 4);

    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].postgres_type, "integer");
    assert_eq!(schema.columns[0].is_nullable, false);
    assert_eq!(schema.columns[0].is_primary_key, true);

    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[1].postgres_type, "text");
    assert_eq!(schema.columns[1].is_nullable, false);
    assert_eq!(schema.columns[1].is_primary_key, false);

    assert_eq!(schema.columns[3].name, "age");
    assert_eq!(schema.columns[3].postgres_type, "integer");
    assert_eq!(schema.columns[3].is_nullable, true);
    assert_eq!(schema.columns[3].is_primary_key, false);

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn reads_generic_snapshot_rows_batch() -> Result<(), Error> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (name, email)
        VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;
    let rows = read_snapshot_rows_batch(&client, &schema, 0, 10).await?;

    assert_eq!(rows.len(), 2);

    assert_eq!(
        rows[0].values.get("id"),
        Some(&SnapshotValue::String("1".to_string()))
    );

    assert_eq!(
        rows[0].values.get("name"),
        Some(&SnapshotValue::String("Alice".to_string()))
    );

    assert_eq!(
        rows[0].values.get("email"),
        Some(&SnapshotValue::String("alice@example.com".to_string()))
    );

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn reads_generic_full_snapshot_with_non_id_primary_key() -> Result<(), Error> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            post_id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (title)
        VALUES
            ('First post'),
            ('Second post'),
            ('Third post')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;
    let rows = read_snapshot_rows_full(&client, &schema, 2).await?;

    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0].values.get("post_id"),
        Some(&SnapshotValue::String("1".to_string()))
    );

    assert_eq!(
        rows[2].values.get("title"),
        Some(&SnapshotValue::String("Third post".to_string()))
    );

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn reads_generic_snapshot_rows_from_schema() -> Result<(), Error> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            views INTEGER NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (title, views)
        VALUES
            ('First post', 10),
            ('Second post', NULL)
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;
    let rows = read_snapshot_rows_batch(&client, &schema, 0, 10).await?;

    assert_eq!(rows.len(), 2);

    assert_eq!(
        rows[0].values.get("title"),
        Some(&SnapshotValue::String("First post".to_string()))
    );

    assert_eq!(
        rows[0].values.get("views"),
        Some(&SnapshotValue::String("10".to_string()))
    );

    assert_eq!(rows[1].values.get("views"), Some(&SnapshotValue::Null));

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn reads_full_snapshot_and_writes_checkpoint() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let checkpoint_path =
        std::env::temp_dir().join(format!("pg_snapshot_reader_{}_checkpoint.json", table_name));

    if checkpoint_path.exists() {
        std::fs::remove_file(&checkpoint_path)?;
    }

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            event_id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (title)
        VALUES
            ('First event'),
            ('Second event'),
            ('Third event')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;

    let rows =
        read_snapshot_rows_full_with_checkpoint(&client, &schema, 2, &checkpoint_path).await?;

    assert_eq!(rows.len(), 3);

    let checkpoint_json = std::fs::read_to_string(&checkpoint_path)?;
    let checkpoint: SnapshotCheckpoint = serde_json::from_str(&checkpoint_json)?;

    assert_eq!(checkpoint.table_name, table_name);
    assert_eq!(checkpoint.primary_key_column, "event_id");
    assert_eq!(checkpoint.last_seen_primary_key, "3");

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    std::fs::remove_file(checkpoint_path)?;

    Ok(())
}

#[tokio::test]
async fn resumes_full_snapshot_from_checkpoint() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let checkpoint_path =
        std::env::temp_dir().join(format!("pg_snapshot_reader_{}_checkpoint.json", table_name));

    if checkpoint_path.exists() {
        std::fs::remove_file(&checkpoint_path)?;
    }

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            event_id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (title)
        VALUES
            ('First event'),
            ('Second event'),
            ('Third event')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let checkpoint = SnapshotCheckpoint {
        table_name: table_name.clone(),
        primary_key_column: "event_id".to_string(),
        last_seen_primary_key: "2".to_string(),
    };

    let checkpoint_json = serde_json::to_string_pretty(&checkpoint)?;
    std::fs::write(&checkpoint_path, checkpoint_json)?;

    let schema = discover_table_schema(&client, &table_name).await?;

    let rows =
        read_snapshot_rows_full_with_checkpoint(&client, &schema, 2, &checkpoint_path).await?;

    assert_eq!(rows.len(), 1);

    assert_eq!(
        rows[0].values.get("title"),
        Some(&SnapshotValue::String("Third event".to_string()))
    );

    let updated_checkpoint_json = std::fs::read_to_string(&checkpoint_path)?;
    let updated_checkpoint: SnapshotCheckpoint = serde_json::from_str(&updated_checkpoint_json)?;

    assert_eq!(updated_checkpoint.last_seen_primary_key, "3");

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    std::fs::remove_file(checkpoint_path)?;

    Ok(())
}

#[tokio::test]
async fn writes_snapshot_batches_to_stage_before_checkpoint() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();

    let stage_path =
        std::env::temp_dir().join(format!("pg_snapshot_reader_{}_stage.jsonl", table_name));

    let checkpoint_path =
        std::env::temp_dir().join(format!("pg_snapshot_reader_{}_checkpoint.json", table_name));

    if stage_path.exists() {
        std::fs::remove_file(&stage_path)?;
    }

    if checkpoint_path.exists() {
        std::fs::remove_file(&checkpoint_path)?;
    }

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            event_id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (title)
        VALUES
            ('First event'),
            ('Second event'),
            ('Third event')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;

    let rows = read_snapshot_rows_full_with_stage_and_checkpoint(
        &client,
        &schema,
        2,
        &stage_path,
        &checkpoint_path,
    )
    .await?;

    assert_eq!(rows.len(), 3);

    let stage_content = std::fs::read_to_string(&stage_path)?;
    let stage_lines: Vec<&str> = stage_content.lines().collect();

    assert_eq!(stage_lines.len(), 3);
    assert!(stage_lines[0].contains("First event"));
    assert!(stage_lines[1].contains("Second event"));
    assert!(stage_lines[2].contains("Third event"));

    let checkpoint_json = std::fs::read_to_string(&checkpoint_path)?;
    let checkpoint: SnapshotCheckpoint = serde_json::from_str(&checkpoint_json)?;

    assert_eq!(checkpoint.table_name, table_name);
    assert_eq!(checkpoint.primary_key_column, "event_id");
    assert_eq!(checkpoint.last_seen_primary_key, "3");

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    std::fs::remove_file(stage_path)?;
    std::fs::remove_file(checkpoint_path)?;

    Ok(())
}

#[tokio::test]
async fn writes_postgres_snapshot_to_clickhouse_end_to_end() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();
    let clickhouse_table_name = format!("{}_snapshot", table_name);

    let stage_path =
        std::env::temp_dir().join(format!("pg_snapshot_reader_{}_stage.jsonl", table_name));

    let checkpoint_path =
        std::env::temp_dir().join(format!("pg_snapshot_reader_{}_checkpoint.json", table_name));

    if stage_path.exists() {
        std::fs::remove_file(&stage_path)?;
    }

    if checkpoint_path.exists() {
        std::fs::remove_file(&checkpoint_path)?;
    }

    let create_postgres_table_sql = format!(
        "
        CREATE TABLE {} (
            user_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_postgres_table_sql, &[]).await?;

    let insert_postgres_rows_sql = format!(
        "
        INSERT INTO {} (name, email)
        VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com')
        ",
        table_name
    );

    client.execute(&insert_postgres_rows_sql, &[]).await?;

    let schema = discover_table_schema(&client, &table_name).await?;

    let clickhouse_config = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "snapshot_demo".to_string(),
        user: "snapshot_user".to_string(),
        password: "snapshot_password".to_string(),
    };

    let drop_clickhouse_table_sql = format!("DROP TABLE IF EXISTS {}", clickhouse_table_name);

    execute_clickhouse_query(&clickhouse_config, &drop_clickhouse_table_sql).await?;

    create_clickhouse_snapshot_table(&clickhouse_config, &schema, &clickhouse_table_name).await?;

    let rows = read_snapshot_rows_full_with_stage_and_checkpoint(
        &client,
        &schema,
        2,
        &stage_path,
        &checkpoint_path,
    )
    .await?;

    assert_eq!(rows.len(), 3);

    let writer = ClickHouseSnapshotRowWriter {
        config: clickhouse_config.clone(),
        table_name: clickhouse_table_name.clone(),
    };

    write_staged_snapshot_rows(&stage_path, &writer).await?;

    let clickhouse_count =
        count_clickhouse_rows(&clickhouse_config, &clickhouse_table_name).await?;

    assert_eq!(clickhouse_count, 3);

    let drop_postgres_table_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_postgres_table_sql, &[]).await?;

    let drop_clickhouse_table_sql = format!("DROP TABLE IF EXISTS {}", clickhouse_table_name);

    execute_clickhouse_query(&clickhouse_config, &drop_clickhouse_table_sql).await?;

    std::fs::remove_file(stage_path)?;
    std::fs::remove_file(checkpoint_path)?;

    Ok(())
}
