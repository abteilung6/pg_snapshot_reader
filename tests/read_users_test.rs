use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pg_snapshot_reader::{
    CdcEvent, CdcEventKind, CdcStageBatchStatus, ClickHouseCdcEventWriter, ClickHouseConfig,
    ClickHouseSnapshotRowWriter, SnapshotCheckpoint, SnapshotValue,
    check_postgres_cdc_prerequisites, count_clickhouse_rows, create_cdc_stage_batch_paths,
    create_clickhouse_snapshot_table, create_logical_replication_slot,
    create_logical_replication_slot_with_plugin, create_publication_for_table,
    deliver_cdc_stage_batch, discover_table_schema, execute_clickhouse_query,
    fetch_clickhouse_query, load_cdc_stage_batch_metadata, parse_decoded_wal_changes,
    read_decoded_wal_changes, read_decoded_wal_changes_into_stage, read_snapshot_rows_batch,
    read_snapshot_rows_full, read_snapshot_rows_full_with_checkpoint,
    read_snapshot_rows_full_with_stage_and_checkpoint, validate_cdc_stage_batch,
    write_cdc_stage_batch, write_staged_snapshot_rows,
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

#[tokio::test]
async fn creates_publication_for_table() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();
    let publication_name = format!("{}_publication", table_name);

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    create_publication_for_table(&client, &publication_name, &table_name).await?;

    let publication_rows = client
        .query(
            "
            SELECT pubname
            FROM pg_publication
            WHERE pubname = $1
            ",
            &[&publication_name],
        )
        .await?;

    assert_eq!(publication_rows.len(), 1);

    let table_rows = client
        .query(
            "
            SELECT
                p.pubname,
                c.relname
            FROM pg_publication p
            JOIN pg_publication_rel pr
              ON p.oid = pr.prpubid
            JOIN pg_class c
              ON pr.prrelid = c.oid
            WHERE p.pubname = $1
              AND c.relname = $2
            ",
            &[&publication_name, &table_name],
        )
        .await?;

    assert_eq!(table_rows.len(), 1);

    let drop_publication_sql = format!("DROP PUBLICATION IF EXISTS {}", publication_name);

    client.execute(&drop_publication_sql, &[]).await?;

    let drop_table_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_table_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn creates_logical_replication_slot() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let slot_name = unique_table_name();

    create_logical_replication_slot(&client, &slot_name).await?;

    let rows = client
        .query(
            "
            SELECT slot_name, plugin, slot_type
            FROM pg_replication_slots
            WHERE slot_name = $1
            ",
            &[&slot_name],
        )
        .await?;

    assert_eq!(rows.len(), 1);

    let plugin: String = rows[0].get("plugin");
    let slot_type: String = rows[0].get("slot_type");

    assert_eq!(plugin, "pgoutput");
    assert_eq!(slot_type, "logical");

    let drop_slot_sql = "
        SELECT pg_drop_replication_slot(slot_name)
        FROM pg_replication_slots
        WHERE slot_name = $1
    ";

    client.execute(drop_slot_sql, &[&slot_name]).await?;

    Ok(())
}

#[tokio::test]
async fn checks_postgres_cdc_prerequisites() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;

    let prerequisites = check_postgres_cdc_prerequisites(&client).await?;

    assert_eq!(prerequisites.wal_level, "logical");
    assert!(prerequisites.max_replication_slots > 0);
    assert!(prerequisites.max_wal_senders > 0);

    Ok(())
}

#[tokio::test]
async fn reads_decoded_wal_changes_with_test_decoding() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();
    let slot_name = format!("{}_slot", table_name);

    create_logical_replication_slot_with_plugin(&client, &slot_name, "test_decoding").await?;

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
        ",
        table_name
    );

    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (name)
        VALUES ('Alice')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let changes = read_decoded_wal_changes(&client, &slot_name, 1000).await?;

    let expected_table_name = format!("public.{}", table_name);
    let events = parse_decoded_wal_changes(changes);

    assert!(events.iter().any(|event| {
        event.kind == CdcEventKind::Insert
            && event.table_name.as_deref() == Some(expected_table_name.as_str())
            && event.column_values.get("name") == Some(&SnapshotValue::String("Alice".to_string()))
    }));

    let drop_slot_sql = "
        SELECT pg_drop_replication_slot(slot_name)
        FROM pg_replication_slots
        WHERE slot_name = $1
    ";

    client.execute(drop_slot_sql, &[&slot_name]).await?;

    let drop_table_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_table_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn reads_decoded_wal_changes_into_cdc_stage() -> anyhow::Result<()> {
    let client = connect_to_postgres().await?;
    let table_name = unique_table_name();
    let slot_name = format!("{}_slot", table_name);
    let stage_dir = std::env::temp_dir().join(format!("{}_cdc_stage", table_name));

    if stage_dir.exists() {
        std::fs::remove_dir_all(&stage_dir)?;
    }

    std::fs::create_dir_all(&stage_dir)?;

    create_logical_replication_slot_with_plugin(&client, &slot_name, "test_decoding").await?;

    let create_table_sql = format!(
        "
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
        ",
        table_name
    );
    client.execute(&create_table_sql, &[]).await?;

    let insert_sql = format!(
        "
        INSERT INTO {} (name)
        VALUES ('Alice')
        ",
        table_name
    );
    client.execute(&insert_sql, &[]).await?;

    let metadata = read_decoded_wal_changes_into_stage(&client, &slot_name, 1000, &stage_dir)
        .await?
        .expect("expected CDC stage batch metadata");

    let paths = create_cdc_stage_batch_paths(&stage_dir, &metadata.batch_id);

    assert!(paths.events_path.exists());
    assert!(paths.metadata_path.exists());

    let loaded_metadata =
        load_cdc_stage_batch_metadata(&paths.metadata_path)?.expect("expected CDC stage metadata");

    assert_eq!(loaded_metadata, metadata);

    let staged_events = validate_cdc_stage_batch(&metadata)?;

    assert_eq!(metadata.slot_name, slot_name);
    assert_eq!(metadata.event_count, staged_events.len());
    assert_eq!(metadata.status, CdcStageBatchStatus::Pending);
    assert_eq!(
        metadata.events_path,
        paths.events_path.to_string_lossy().to_string()
    );

    assert_eq!(
        metadata.start_lsn,
        staged_events.first().expect("expected first event").lsn
    );

    assert_eq!(
        metadata.end_lsn,
        staged_events.last().expect("expected last event").lsn
    );

    let expected_table_name = format!("public.{}", table_name);

    assert!(
        staged_events
            .iter()
            .any(|event| event.table_name.as_deref() == Some(expected_table_name.as_str()))
    );

    assert!(staged_events.iter().any(|event| {
        event.kind == CdcEventKind::Insert
            && event.table_name.as_deref() == Some(expected_table_name.as_str())
            && event.column_values.get("name") == Some(&SnapshotValue::String("Alice".to_string()))
    }));

    let drop_slot_sql = "
        SELECT pg_drop_replication_slot(slot_name)
        FROM pg_replication_slots
        WHERE slot_name = $1
    ";
    client.execute(drop_slot_sql, &[&slot_name]).await?;

    let drop_table_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_table_sql, &[]).await?;

    if stage_dir.exists() {
        std::fs::remove_dir_all(&stage_dir)?;
    }

    Ok(())
}

#[tokio::test]
async fn writes_staged_cdc_insert_events_to_clickhouse() -> anyhow::Result<()> {
    let table_name = unique_table_name();
    let clickhouse_table_name = format!("{}_cdc", table_name);

    let stage_dir = std::env::temp_dir().join(format!("{}_cdc_clickhouse_stage", table_name));

    if stage_dir.exists() {
        std::fs::remove_dir_all(&stage_dir)?;
    }

    std::fs::create_dir_all(&stage_dir)?;

    let clickhouse_config = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "snapshot_demo".to_string(),
        user: "snapshot_user".to_string(),
        password: "snapshot_password".to_string(),
    };

    execute_clickhouse_query(
        &clickhouse_config,
        &format!("DROP TABLE IF EXISTS {}", clickhouse_table_name),
    )
    .await?;

    execute_clickhouse_query(
        &clickhouse_config,
        &format!(
            "
            CREATE TABLE {} (
                id Int32,
                name String,
                _source_lsn String,
                _replication_batch_id String,
                _replication_deleted UInt8
            )
            ENGINE = MergeTree
            ORDER BY id
            ",
            clickhouse_table_name
        ),
    )
    .await?;

    let events = vec![
        CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: std::collections::HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        },
        CdcEvent {
            lsn: "0/120".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Insert,
            table_name: Some("public.users".to_string()),
            column_values: std::collections::HashMap::from([
                ("id".to_string(), SnapshotValue::String("1".to_string())),
                (
                    "name".to_string(),
                    SnapshotValue::String("Alice".to_string()),
                ),
            ]),
            raw_data: "table public.users: INSERT: id[integer]:1 name[text]:'Alice'".to_string(),
        },
        CdcEvent {
            lsn: "0/150".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Commit,
            table_name: None,
            column_values: std::collections::HashMap::new(),
            raw_data: "COMMIT 1".to_string(),
        },
    ];

    let metadata =
        write_cdc_stage_batch(&stage_dir, "test_slot", &events)?.expect("expected metadata");

    let paths = create_cdc_stage_batch_paths(&stage_dir, &metadata.batch_id);

    let writer = ClickHouseCdcEventWriter {
        config: clickhouse_config.clone(),
        table_name: clickhouse_table_name.clone(),
    };

    deliver_cdc_stage_batch(&paths.metadata_path, &writer).await?;

    let row_count = count_clickhouse_rows(&clickhouse_config, &clickhouse_table_name).await?;

    assert_eq!(row_count, 1);

    let loaded_metadata =
        load_cdc_stage_batch_metadata(&paths.metadata_path)?.expect("expected metadata");

    assert_eq!(loaded_metadata.status, CdcStageBatchStatus::Written);

    let batch_id = fetch_clickhouse_query(
        &clickhouse_config,
        &format!(
            "SELECT _replication_batch_id FROM {} LIMIT 1",
            clickhouse_table_name
        ),
    )
    .await?;

    assert!(batch_id.contains(&metadata.batch_id));

    let source_lsn = fetch_clickhouse_query(
        &clickhouse_config,
        &format!("SELECT _source_lsn FROM {} LIMIT 1", clickhouse_table_name),
    )
    .await?;

    assert!(source_lsn.contains("0/120"));

    execute_clickhouse_query(
        &clickhouse_config,
        &format!("DROP TABLE IF EXISTS {}", clickhouse_table_name),
    )
    .await?;

    if stage_dir.exists() {
        std::fs::remove_dir_all(&stage_dir)?;
    }

    Ok(())
}
