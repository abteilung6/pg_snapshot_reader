use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use tokio_postgres::{Client, Error};

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub postgres_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

#[derive(Debug, PartialEq)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SnapshotValue {
    String(String),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRow {
    pub values: HashMap<String, SnapshotValue>,
}

#[async_trait]
pub trait SnapshotRowWriter {
    async fn write_rows(&self, rows: &[SnapshotRow]) -> anyhow::Result<()>;
}

pub struct DebugSnapshotRowWriter;

#[async_trait]
impl SnapshotRowWriter for DebugSnapshotRowWriter {
    async fn write_rows(&self, rows: &[SnapshotRow]) -> anyhow::Result<()> {
        for row in rows {
            println!("{:#?}", row);
        }

        Ok(())
    }
}

pub struct ClickHouseSnapshotRowWriter {
    pub config: ClickHouseConfig,
    pub table_name: String,
}

#[async_trait]
impl SnapshotRowWriter for ClickHouseSnapshotRowWriter {
    async fn write_rows(&self, rows: &[SnapshotRow]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let payload = build_clickhouse_json_each_row_payload(rows)?;

        let query = format!(
            "INSERT INTO {} FORMAT JSONEachRow\n{}",
            quote_clickhouse_identifier(&self.table_name),
            payload
        );

        execute_clickhouse_query(&self.config, &query).await?;

        Ok(())
    }
}

#[async_trait]
pub trait CdcEventWriter {
    async fn write_events(&self, events: &[CdcEvent]) -> anyhow::Result<()>;
}

pub struct DebugCdcEventWriter;

#[async_trait]
impl CdcEventWriter for DebugCdcEventWriter {
    async fn write_events(&self, events: &[CdcEvent]) -> anyhow::Result<()> {
        for event in events {
            println!("{:#?}", event);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCheckpoint {
    pub table_name: String,
    pub primary_key_column: String,
    pub last_seen_primary_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotBoundary {
    pub table_name: String,
    pub snapshot_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostgresCdcPrerequisites {
    pub wal_level: String,
    pub max_replication_slots: i32,
    pub max_wal_senders: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedWalChange {
    pub lsn: String,
    pub xid: String,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcEventKind {
    Begin,
    Commit,
    Insert,
    Update,
    Delete,
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcEvent {
    pub lsn: String,
    pub xid: String,
    pub kind: CdcEventKind,
    pub table_name: Option<String>,
    pub column_values: HashMap<String, SnapshotValue>,
    pub raw_data: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcStageBatchStatus {
    Pending,
    Writing,
    Written,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcStageBatchMetadata {
    pub batch_id: String,
    pub slot_name: String,
    pub start_lsn: String,
    pub end_lsn: String,
    pub event_count: usize,
    pub events_path: String,
    pub status: CdcStageBatchStatus,
    pub retry_count: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CdcStageBatchPaths {
    pub events_path: std::path::PathBuf,
    pub metadata_path: std::path::PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CdcStageBatch {
    pub metadata: CdcStageBatchMetadata,
    pub events: Vec<CdcEvent>,
}

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

pub fn save_snapshot_checkpoint(
    path: &Path,
    checkpoint: &SnapshotCheckpoint,
) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(checkpoint)?;
    fs::write(path, json)?;

    Ok(())
}

pub fn load_snapshot_checkpoint(path: &Path) -> anyhow::Result<Option<SnapshotCheckpoint>> {
    if !path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(path)?;
    let checkpoint = serde_json::from_str(&json)?;

    Ok(Some(checkpoint))
}

pub fn save_snapshot_boundary(path: &Path, boundary: &SnapshotBoundary) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(boundary)?;
    fs::write(path, json)?;

    Ok(())
}

pub fn load_snapshot_boundary(path: &Path) -> anyhow::Result<Option<SnapshotBoundary>> {
    if !path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(path)?;
    let boundary = serde_json::from_str(&json)?;

    Ok(Some(boundary))
}

pub fn create_local_snapshot_boundary(table_name: &str) -> SnapshotBoundary {
    SnapshotBoundary {
        table_name: table_name.to_string(),
        snapshot_id: format!(
            "local-snapshot-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ),
        created_at: chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
    }
}

#[test]
fn saves_and_loads_snapshot_checkpoint() {
    let path = std::env::temp_dir().join("pg_snapshot_reader_checkpoint_test.json");

    let checkpoint = SnapshotCheckpoint {
        table_name: "users".to_string(),
        primary_key_column: "id".to_string(),
        last_seen_primary_key: "42".to_string(),
    };

    save_snapshot_checkpoint(&path, &checkpoint).unwrap();

    let loaded = load_snapshot_checkpoint(&path).unwrap();

    assert_eq!(loaded, Some(checkpoint));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn returns_none_when_checkpoint_file_does_not_exist() {
    let path = std::env::temp_dir().join("pg_snapshot_reader_missing_checkpoint.json");

    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }

    let loaded = load_snapshot_checkpoint(&path).unwrap();

    assert_eq!(loaded, None);
}

impl TableSchema {
    pub fn primary_key_column(&self) -> &Column {
        self.columns
            .iter()
            .find(|column| column.is_primary_key)
            .expect("expected table to have a primary key")
    }
}

pub async fn discover_table_schema(
    client: &Client,
    table_name: &str,
) -> Result<TableSchema, Error> {
    let columns_query = "
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = $1
        ORDER BY ordinal_position
    ";

    let pk_query = "
        SELECT
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = $1
    ";

    let pk_rows = client.query(pk_query, &[&table_name]).await?;

    let primary_keys: Vec<String> = pk_rows
        .into_iter()
        .map(|row| row.get("column_name"))
        .collect();

    let column_rows = client.query(columns_query, &[&table_name]).await?;

    let mut columns = Vec::new();

    for row in column_rows {
        let name: String = row.get("column_name");
        let postgres_type: String = row.get("data_type");
        let is_nullable_string: String = row.get("is_nullable");

        let is_primary_key = primary_keys.contains(&name);

        columns.push(Column {
            name,
            postgres_type,
            is_nullable: is_nullable_string == "YES",
            is_primary_key,
        });
    }

    Ok(TableSchema {
        table_name: table_name.to_string(),
        columns,
    })
}

pub async fn create_publication_for_table(
    client: &Client,
    publication_name: &str,
    table_name: &str,
) -> Result<(), Error> {
    let drop_query = format!(
        "DROP PUBLICATION IF EXISTS {}",
        quote_postgres_identifier(publication_name)
    );

    client.execute(&drop_query, &[]).await?;

    let create_query = format!(
        "CREATE PUBLICATION {} FOR TABLE {}",
        quote_postgres_identifier(publication_name),
        quote_postgres_identifier(table_name)
    );

    client.execute(&create_query, &[]).await?;

    Ok(())
}

pub async fn create_logical_replication_slot(
    client: &Client,
    slot_name: &str,
) -> Result<(), Error> {
    create_logical_replication_slot_with_plugin(client, slot_name, "pgoutput").await
}

pub async fn create_logical_replication_slot_with_plugin(
    client: &Client,
    slot_name: &str,
    plugin: &str,
) -> Result<(), Error> {
    let drop_query = "
        SELECT pg_drop_replication_slot(slot_name)
        FROM pg_replication_slots
        WHERE slot_name = $1
    ";

    client.execute(drop_query, &[&slot_name]).await?;

    let create_query = "
        SELECT *
        FROM pg_create_logical_replication_slot($1, $2)
    ";

    client.query(create_query, &[&slot_name, &plugin]).await?;

    Ok(())
}

pub async fn read_decoded_wal_changes(
    client: &Client,
    slot_name: &str,
    limit: i32,
) -> Result<Vec<DecodedWalChange>, Error> {
    let rows = client
        .query(
            "
            SELECT lsn::text AS lsn, xid::text AS xid, data
            FROM pg_logical_slot_get_changes($1, NULL, $2)
            ",
            &[&slot_name, &limit],
        )
        .await?;

    let mut changes = Vec::new();

    for row in rows {
        let lsn: String = row.get("lsn");
        let xid: String = row.get("xid");
        let data: String = row.get("data");

        changes.push(DecodedWalChange { lsn, xid, data });
    }

    Ok(changes)
}

pub async fn check_postgres_cdc_prerequisites(
    client: &Client,
) -> Result<PostgresCdcPrerequisites, Error> {
    let wal_level_row = client.query_one("SHOW wal_level", &[]).await?;
    let max_replication_slots_row = client.query_one("SHOW max_replication_slots", &[]).await?;
    let max_wal_senders_row = client.query_one("SHOW max_wal_senders", &[]).await?;

    let wal_level: String = wal_level_row.get(0);
    let max_replication_slots: String = max_replication_slots_row.get(0);
    let max_wal_senders: String = max_wal_senders_row.get(0);

    Ok(PostgresCdcPrerequisites {
        wal_level,
        max_replication_slots: max_replication_slots.parse::<i32>().unwrap(),
        max_wal_senders: max_wal_senders.parse::<i32>().unwrap(),
    })
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub fn parse_decoded_wal_change(change: DecodedWalChange) -> CdcEvent {
    let kind = if change.data.starts_with("BEGIN") {
        CdcEventKind::Begin
    } else if change.data.starts_with("COMMIT") {
        CdcEventKind::Commit
    } else if change.data.contains(": INSERT:") {
        CdcEventKind::Insert
    } else if change.data.contains(": UPDATE:") {
        CdcEventKind::Update
    } else if change.data.contains(": DELETE:") {
        CdcEventKind::Delete
    } else {
        CdcEventKind::Other
    };

    let table_name = extract_table_name_from_decoded_change(&change.data);
    let column_values = if kind == CdcEventKind::Insert {
        extract_insert_column_values_from_decoded_change(&change.data)
    } else {
        HashMap::new()
    };

    CdcEvent {
        lsn: change.lsn,
        xid: change.xid,
        kind,
        table_name,
        column_values,
        raw_data: change.data,
    }
}

pub fn extract_table_name_from_decoded_change(data: &str) -> Option<String> {
    let rest = data.strip_prefix("table ")?;
    let table_name = rest.split(':').next()?.trim();

    if table_name.is_empty() {
        return None;
    }

    Some(table_name.to_string())
}

pub fn extract_insert_column_values_from_decoded_change(
    data: &str,
) -> HashMap<String, SnapshotValue> {
    let mut values = HashMap::new();

    let Some((_prefix, columns_part)) = data.split_once(": INSERT:") else {
        return values;
    };

    for column_part in columns_part.trim().split(' ') {
        let Some((column_with_type, raw_value)) = column_part.split_once(':') else {
            continue;
        };

        let Some((column_name, _type_part)) = column_with_type.split_once('[') else {
            continue;
        };

        let value = raw_value.trim_matches('\'');

        values.insert(
            column_name.to_string(),
            SnapshotValue::String(value.to_string()),
        );
    }

    values
}

pub fn parse_decoded_wal_changes(changes: Vec<DecodedWalChange>) -> Vec<CdcEvent> {
    changes.into_iter().map(parse_decoded_wal_change).collect()
}

pub async fn read_snapshot_rows_batch(
    client: &Client,
    schema: &TableSchema,
    last_seen_id: i32,
    limit: i64,
) -> Result<Vec<SnapshotRow>, Error> {
    let query = build_select_query(schema);

    let rows = client.query(&query, &[&last_seen_id, &limit]).await?;

    let mut snapshot_rows = Vec::new();

    for row in rows {
        let mut values = HashMap::new();

        for column in &schema.columns {
            let value = match column.postgres_type.as_str() {
                "integer" => {
                    let value: Option<i32> = row.get(column.name.as_str());

                    match value {
                        Some(v) => SnapshotValue::String(v.to_string()),
                        None => SnapshotValue::Null,
                    }
                }
                "text" => {
                    let value: Option<String> = row.get(column.name.as_str());

                    match value {
                        Some(v) => SnapshotValue::String(v),
                        None => SnapshotValue::Null,
                    }
                }
                "timestamp without time zone" => {
                    let value: Option<std::time::SystemTime> = row.get(column.name.as_str());

                    match value {
                        Some(v) => {
                            let datetime: DateTime<Utc> = v.into();

                            SnapshotValue::String(
                                datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                            )
                        }
                        None => SnapshotValue::Null,
                    }
                }
                unsupported_type => {
                    panic!(
                        "unsupported postgres type '{}' for column '{}'",
                        unsupported_type, column.name
                    );
                }
            };

            values.insert(column.name.clone(), value);
        }

        snapshot_rows.push(SnapshotRow { values });
    }

    Ok(snapshot_rows)
}

pub async fn read_snapshot_rows_full(
    client: &Client,
    schema: &TableSchema,
    batch_size: i64,
) -> Result<Vec<SnapshotRow>, Error> {
    let mut all_rows = Vec::new();
    let mut last_seen_id = 0;

    loop {
        let batch = read_snapshot_rows_batch(client, schema, last_seen_id, batch_size).await?;

        if batch.is_empty() {
            break;
        }

        let last_row = batch.last().unwrap();

        let primary_key = schema.primary_key_column();

        let last_id = match last_row.values.get(&primary_key.name) {
            Some(SnapshotValue::String(value)) => value.parse::<i32>().unwrap(),
            _ => panic!("expected primary key column to exist and be a string"),
        };

        last_seen_id = last_id;

        all_rows.extend(batch);
    }

    Ok(all_rows)
}

pub async fn read_snapshot_rows_full_with_checkpoint(
    client: &Client,
    schema: &TableSchema,
    batch_size: i64,
    checkpoint_path: &Path,
) -> anyhow::Result<Vec<SnapshotRow>> {
    let primary_key = schema.primary_key_column();

    let checkpoint = load_snapshot_checkpoint(checkpoint_path)?;

    let mut last_seen_id = match checkpoint {
        Some(checkpoint) => {
            if checkpoint.table_name != schema.table_name {
                panic!(
                    "checkpoint table '{}' does not match schema table '{}'",
                    checkpoint.table_name, schema.table_name
                );
            }

            if checkpoint.primary_key_column != primary_key.name {
                panic!(
                    "checkpoint primary key '{}' does not match schema primary key '{}'",
                    checkpoint.primary_key_column, primary_key.name
                );
            }

            checkpoint.last_seen_primary_key.parse::<i32>()?
        }
        None => 0,
    };

    let mut all_rows = Vec::new();

    loop {
        let batch = read_snapshot_rows_batch(client, schema, last_seen_id, batch_size).await?;

        if batch.is_empty() {
            break;
        }

        let last_row = batch.last().unwrap();

        let last_id = match last_row.values.get(&primary_key.name) {
            Some(SnapshotValue::String(value)) => value.parse::<i32>()?,
            _ => panic!("expected primary key column to exist and be a string"),
        };

        all_rows.extend(batch);

        last_seen_id = last_id;

        let checkpoint = SnapshotCheckpoint {
            table_name: schema.table_name.clone(),
            primary_key_column: primary_key.name.clone(),
            last_seen_primary_key: last_seen_id.to_string(),
        };

        save_snapshot_checkpoint(checkpoint_path, &checkpoint)?;
    }

    Ok(all_rows)
}

pub async fn read_snapshot_rows_full_with_stage_and_checkpoint(
    client: &Client,
    schema: &TableSchema,
    batch_size: i64,
    stage_path: &Path,
    checkpoint_path: &Path,
) -> anyhow::Result<Vec<SnapshotRow>> {
    let primary_key = schema.primary_key_column();

    let checkpoint = load_snapshot_checkpoint(checkpoint_path)?;

    let mut last_seen_id = match checkpoint {
        Some(checkpoint) => {
            if checkpoint.table_name != schema.table_name {
                panic!(
                    "checkpoint table '{}' does not match schema table '{}'",
                    checkpoint.table_name, schema.table_name
                );
            }

            if checkpoint.primary_key_column != primary_key.name {
                panic!(
                    "checkpoint primary key '{}' does not match schema primary key '{}'",
                    checkpoint.primary_key_column, primary_key.name
                );
            }

            checkpoint.last_seen_primary_key.parse::<i32>()?
        }
        None => 0,
    };

    let mut all_rows = Vec::new();

    loop {
        let batch = read_snapshot_rows_batch(client, schema, last_seen_id, batch_size).await?;

        if batch.is_empty() {
            break;
        }

        let last_row = batch.last().unwrap();

        let last_id = match last_row.values.get(&primary_key.name) {
            Some(SnapshotValue::String(value)) => value.parse::<i32>()?,
            _ => panic!("expected primary key column to exist and be a string"),
        };

        write_snapshot_rows_jsonl(stage_path, &batch)?;

        all_rows.extend(batch);

        last_seen_id = last_id;

        let checkpoint = SnapshotCheckpoint {
            table_name: schema.table_name.clone(),
            primary_key_column: primary_key.name.clone(),
            last_seen_primary_key: last_seen_id.to_string(),
        };

        save_snapshot_checkpoint(checkpoint_path, &checkpoint)?;
    }

    Ok(all_rows)
}

pub fn map_postgres_type_to_clickhouse_type(postgres_type: &str) -> anyhow::Result<&'static str> {
    match postgres_type {
        "integer" => Ok("Int32"),
        "text" => Ok("String"),
        "timestamp without time zone" => Ok("DateTime64(3)"),
        unsupported_type => {
            anyhow::bail!(
                "unsupported postgres type for ClickHouse mapping: {}",
                unsupported_type
            )
        }
    }
}

pub fn build_select_query(schema: &TableSchema) -> String {
    let column_names = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<String>>()
        .join(", ");

    let primary_key = schema.primary_key_column();

    format!(
        "
        SELECT {}
        FROM {}
        WHERE {} > $1
        ORDER BY {}
        LIMIT $2
        ",
        column_names, schema.table_name, primary_key.name, primary_key.name
    )
}

pub fn build_clickhouse_create_snapshot_table_query(
    schema: &TableSchema,
    clickhouse_table: &str,
) -> String {
    let column_definitions = schema
        .columns
        .iter()
        .map(|column| {
            let clickhouse_type = map_postgres_type_to_clickhouse_type(&column.postgres_type)
                .expect("unsupported postgres type");

            format!(
                "    {} {}",
                quote_clickhouse_identifier(&column.name),
                clickhouse_type
            )
        })
        .collect::<Vec<String>>()
        .join(",\n");

    let primary_key = schema.primary_key_column();

    format!(
        "CREATE TABLE IF NOT EXISTS {}\n(\n{}\n)\nENGINE = MergeTree\nORDER BY {}",
        quote_clickhouse_identifier(clickhouse_table),
        column_definitions,
        quote_clickhouse_identifier(&primary_key.name)
    )
}

pub fn build_clickhouse_insert_query(
    table_name: &str,
    rows: &[SnapshotRow],
) -> anyhow::Result<String> {
    if rows.is_empty() {
        return Ok(String::new());
    }

    let first_row = &rows[0];

    let mut column_names = first_row.values.keys().cloned().collect::<Vec<String>>();

    column_names.sort();

    let columns_sql = column_names
        .iter()
        .map(|name| quote_clickhouse_identifier(name))
        .collect::<Vec<String>>()
        .join(", ");

    let mut values_sql = Vec::new();

    for row in rows {
        let mut row_values = Vec::new();

        for column_name in &column_names {
            let value = row
                .values
                .get(column_name)
                .ok_or_else(|| anyhow::anyhow!("missing column '{}'", column_name))?;

            let value_sql = match value {
                SnapshotValue::String(value) => {
                    format!("'{}'", escape_clickhouse_string(value))
                }
                SnapshotValue::Null => "NULL".to_string(),
            };

            row_values.push(value_sql);
        }

        values_sql.push(format!("({})", row_values.join(", ")));
    }

    Ok(format!(
        "INSERT INTO {} ({}) VALUES\n{}",
        quote_clickhouse_identifier(table_name),
        columns_sql,
        values_sql.join(",\n")
    ))
}

pub fn build_clickhouse_json_each_row_payload(rows: &[SnapshotRow]) -> anyhow::Result<String> {
    let mut lines = Vec::new();

    for row in rows {
        let mut json_row = serde_json::Map::new();

        for (column_name, value) in &row.values {
            match value {
                SnapshotValue::String(value) => {
                    json_row.insert(
                        column_name.clone(),
                        serde_json::Value::String(value.clone()),
                    );
                }
                SnapshotValue::Null => {
                    json_row.insert(column_name.clone(), serde_json::Value::Null);
                }
            }
        }

        lines.push(serde_json::Value::Object(json_row).to_string());
    }

    Ok(lines.join("\n"))
}

fn escape_clickhouse_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn quote_clickhouse_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

pub fn write_snapshot_rows_jsonl(path: &Path, rows: &[SnapshotRow]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    for row in rows {
        let json = serde_json::to_string(row)?;
        writeln!(file, "{}", json)?;
    }

    Ok(())
}

#[test]
fn writes_snapshot_rows_as_jsonl() {
    let path = std::env::temp_dir().join(format!(
        "pg_snapshot_reader_stage_{}.jsonl",
        std::process::id()
    ));

    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }

    let mut first_values = HashMap::new();
    first_values.insert("id".to_string(), SnapshotValue::String("1".to_string()));
    first_values.insert(
        "name".to_string(),
        SnapshotValue::String("Alice".to_string()),
    );

    let mut second_values = HashMap::new();
    second_values.insert("id".to_string(), SnapshotValue::String("2".to_string()));
    second_values.insert("name".to_string(), SnapshotValue::String("Bob".to_string()));

    let rows = vec![
        SnapshotRow {
            values: first_values,
        },
        SnapshotRow {
            values: second_values,
        },
    ];

    write_snapshot_rows_jsonl(&path, &rows).unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("Alice"));
    assert!(lines[1].contains("Bob"));

    std::fs::remove_file(path).unwrap();
}

pub fn read_snapshot_rows_jsonl(path: &Path) -> anyhow::Result<Vec<SnapshotRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(path)?;

    let mut rows = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let row: SnapshotRow = serde_json::from_str(line)?;
        rows.push(row);
    }

    Ok(rows)
}

#[test]
fn reads_snapshot_rows_from_jsonl() {
    let path = std::env::temp_dir().join(format!(
        "pg_snapshot_reader_stage_read_{}.jsonl",
        std::process::id()
    ));

    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }

    let mut first_values = HashMap::new();
    first_values.insert("id".to_string(), SnapshotValue::String("1".to_string()));
    first_values.insert(
        "name".to_string(),
        SnapshotValue::String("Alice".to_string()),
    );

    let mut second_values = HashMap::new();
    second_values.insert("id".to_string(), SnapshotValue::String("2".to_string()));
    second_values.insert("name".to_string(), SnapshotValue::String("Bob".to_string()));

    let original_rows = vec![
        SnapshotRow {
            values: first_values,
        },
        SnapshotRow {
            values: second_values,
        },
    ];

    write_snapshot_rows_jsonl(&path, &original_rows).unwrap();

    let loaded_rows = read_snapshot_rows_jsonl(&path).unwrap();

    assert_eq!(loaded_rows, original_rows);

    std::fs::remove_file(path).unwrap();
}

pub fn write_cdc_events_jsonl(path: &Path, events: &[CdcEvent]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    for event in events {
        let json = serde_json::to_string(event)?;
        writeln!(file, "{}", json)?;
    }

    Ok(())
}

pub fn read_cdc_events_jsonl(path: &Path) -> anyhow::Result<Vec<CdcEvent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)?;
    let mut events = Vec::new();

    for line in contents.lines() {
        let event = serde_json::from_str(line)?;
        events.push(event);
    }

    Ok(events)
}

pub async fn write_staged_snapshot_rows<W>(stage_path: &Path, writer: &W) -> anyhow::Result<()>
where
    W: SnapshotRowWriter + Sync,
{
    let rows = read_snapshot_rows_jsonl(stage_path)?;
    writer.write_rows(&rows).await?;

    Ok(())
}

pub async fn read_decoded_wal_changes_into_stage(
    client: &Client,
    slot_name: &str,
    limit: i32,
    stage_dir: &Path,
) -> anyhow::Result<Option<CdcStageBatchMetadata>> {
    let changes = read_decoded_wal_changes(client, slot_name, limit).await?;
    let events = parse_decoded_wal_changes(changes);

    write_cdc_stage_batch(stage_dir, slot_name, &events)
}

pub fn save_cdc_stage_batch_metadata(
    path: &Path,
    metadata: &CdcStageBatchMetadata,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(metadata)?;
    fs::write(path, json)?;

    Ok(())
}

pub fn load_cdc_stage_batch_metadata(path: &Path) -> anyhow::Result<Option<CdcStageBatchMetadata>> {
    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(path)?;
    let metadata = serde_json::from_str(&contents)?;

    Ok(Some(metadata))
}

pub fn create_cdc_stage_batch_metadata(
    slot_name: &str,
    events_path: &Path,
    events: &[CdcEvent],
) -> Option<CdcStageBatchMetadata> {
    let first_event = events.first()?;
    let last_event = events.last()?;

    let start_lsn = first_event.lsn.clone();
    let end_lsn = last_event.lsn.clone();

    let batch_id = create_cdc_stage_batch_id(slot_name, &start_lsn, &end_lsn);

    Some(CdcStageBatchMetadata {
        batch_id,
        slot_name: slot_name.to_string(),
        start_lsn,
        end_lsn,
        event_count: events.len(),
        events_path: events_path.to_string_lossy().to_string(),
        status: CdcStageBatchStatus::Pending,
        retry_count: 0,
        last_error: None,
    })
}

pub fn update_cdc_stage_batch_status(
    path: &Path,
    status: CdcStageBatchStatus,
    last_error: Option<String>,
) -> anyhow::Result<()> {
    let mut metadata =
        load_cdc_stage_batch_metadata(path)?.expect("expected CDC stage batch metadata");

    metadata.status = status;

    if last_error.is_some() {
        metadata.retry_count += 1;
    }

    metadata.last_error = last_error;

    save_cdc_stage_batch_metadata(path, &metadata)?;

    Ok(())
}

pub fn create_cdc_stage_batch_id(slot_name: &str, start_lsn: &str, end_lsn: &str) -> String {
    let safe_start_lsn = start_lsn.replace('/', "_");
    let safe_end_lsn = end_lsn.replace('/', "_");

    format!("{}_{}_{}", slot_name, safe_start_lsn, safe_end_lsn)
}

pub fn write_cdc_events_jsonl_atomic(path: &Path, events: &[CdcEvent]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_path = path.with_extension("jsonl.tmp");

    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }

    write_cdc_events_jsonl(&temp_path, events)?;

    fs::rename(&temp_path, path)?;

    Ok(())
}

pub fn validate_cdc_stage_batch(metadata: &CdcStageBatchMetadata) -> anyhow::Result<Vec<CdcEvent>> {
    let events_path = Path::new(&metadata.events_path);

    let events = read_cdc_events_jsonl(events_path)?;

    if events.len() != metadata.event_count {
        anyhow::bail!(
            "CDC stage batch event count mismatch: metadata says {}, file contains {}",
            metadata.event_count,
            events.len()
        );
    }

    let first_event = events
        .first()
        .ok_or_else(|| anyhow::anyhow!("CDC stage batch contains no events"))?;

    let last_event = events
        .last()
        .ok_or_else(|| anyhow::anyhow!("CDC stage batch contains no events"))?;

    if first_event.lsn != metadata.start_lsn {
        anyhow::bail!(
            "CDC stage batch start_lsn mismatch: metadata says {}, first event has {}",
            metadata.start_lsn,
            first_event.lsn
        );
    }

    if last_event.lsn != metadata.end_lsn {
        anyhow::bail!(
            "CDC stage batch end_lsn mismatch: metadata says {}, last event has {}",
            metadata.end_lsn,
            last_event.lsn
        );
    }

    Ok(events)
}

pub async fn deliver_cdc_stage_batch<W>(metadata_path: &Path, writer: &W) -> anyhow::Result<()>
where
    W: CdcEventWriter + Sync,
{
    let batch = read_cdc_stage_batch(metadata_path)?;

    if batch.metadata.status == CdcStageBatchStatus::Written {
        return Ok(());
    }

    let events = batch.events;

    update_cdc_stage_batch_status(metadata_path, CdcStageBatchStatus::Writing, None)?;

    let write_result = writer.write_events(&events).await;

    match write_result {
        Ok(()) => {
            update_cdc_stage_batch_status(metadata_path, CdcStageBatchStatus::Written, None)?;

            Ok(())
        }
        Err(error) => {
            update_cdc_stage_batch_status(
                metadata_path,
                CdcStageBatchStatus::Failed,
                Some(error.to_string()),
            )?;

            Err(error)
        }
    }
}

pub fn create_cdc_stage_batch_paths(stage_dir: &Path, batch_id: &str) -> CdcStageBatchPaths {
    CdcStageBatchPaths {
        events_path: stage_dir.join(format!("{}.jsonl", batch_id)),
        metadata_path: stage_dir.join(format!("{}.meta.json", batch_id)),
    }
}

pub fn write_cdc_stage_batch(
    stage_dir: &Path,
    slot_name: &str,
    events: &[CdcEvent],
) -> anyhow::Result<Option<CdcStageBatchMetadata>> {
    let Some(first_event) = events.first() else {
        return Ok(None);
    };

    let Some(last_event) = events.last() else {
        return Ok(None);
    };

    let batch_id = create_cdc_stage_batch_id(slot_name, &first_event.lsn, &last_event.lsn);

    let paths = create_cdc_stage_batch_paths(stage_dir, &batch_id);

    write_cdc_events_jsonl_atomic(&paths.events_path, events)?;

    let metadata = create_cdc_stage_batch_metadata(slot_name, &paths.events_path, events)
        .expect("expected metadata for non-empty CDC events");

    save_cdc_stage_batch_metadata(&paths.metadata_path, &metadata)?;

    Ok(Some(metadata))
}

pub fn read_cdc_stage_batch(metadata_path: &Path) -> anyhow::Result<CdcStageBatch> {
    let metadata =
        load_cdc_stage_batch_metadata(metadata_path)?.expect("expected CDC stage batch metadata");

    let events = validate_cdc_stage_batch(&metadata)?;

    Ok(CdcStageBatch { metadata, events })
}

pub fn list_cdc_stage_batch_metadata(
    stage_dir: &Path,
) -> anyhow::Result<Vec<CdcStageBatchMetadata>> {
    if !stage_dir.exists() {
        return Ok(Vec::new());
    }

    let mut metadata = Vec::new();

    for entry in fs::read_dir(stage_dir)? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        if !file_name.ends_with(".meta.json") {
            continue;
        }

        let Some(batch_metadata) = load_cdc_stage_batch_metadata(&path)? else {
            continue;
        };

        metadata.push(batch_metadata);
    }

    metadata.sort_by(|left, right| {
        left.start_lsn
            .cmp(&right.start_lsn)
            .then_with(|| left.batch_id.cmp(&right.batch_id))
    });

    Ok(metadata)
}

pub fn list_deliverable_cdc_stage_batch_metadata(
    stage_dir: &Path,
) -> anyhow::Result<Vec<CdcStageBatchMetadata>> {
    let metadata = list_cdc_stage_batch_metadata(stage_dir)?;

    let deliverable = metadata
        .into_iter()
        .filter(|batch| {
            batch.status == CdcStageBatchStatus::Pending
                || batch.status == CdcStageBatchStatus::Failed
        })
        .collect();

    Ok(deliverable)
}

pub async fn execute_clickhouse_query(
    config: &ClickHouseConfig,
    query: &str,
) -> anyhow::Result<()> {
    let response = reqwest::Client::new()
        .post(&config.url)
        .query(&[("database", &config.database)])
        .basic_auth(&config.user, Some(&config.password))
        .body(query.to_string())
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        anyhow::bail!("ClickHouse query failed with status {}: {}", status, body);
    }

    Ok(())
}

pub async fn deliver_pending_cdc_stage_batches<W>(
    stage_dir: &Path,
    writer: &W,
) -> anyhow::Result<usize>
where
    W: CdcEventWriter + Sync,
{
    let metadata = list_deliverable_cdc_stage_batch_metadata(stage_dir)?;
    let mut delivered_count = 0;

    for batch_metadata in metadata {
        let paths = create_cdc_stage_batch_paths(stage_dir, &batch_metadata.batch_id);

        deliver_cdc_stage_batch(&paths.metadata_path, writer).await?;
        delivered_count += 1;
    }

    Ok(delivered_count)
}

pub async fn fetch_clickhouse_query(
    config: &ClickHouseConfig,
    query: &str,
) -> anyhow::Result<String> {
    let response = reqwest::Client::new()
        .post(&config.url)
        .query(&[("database", &config.database)])
        .basic_auth(&config.user, Some(&config.password))
        .body(query.to_string())
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        anyhow::bail!("ClickHouse query failed with status {}: {}", status, body);
    }

    Ok(body)
}

pub async fn count_clickhouse_rows(
    config: &ClickHouseConfig,
    table_name: &str,
) -> anyhow::Result<u64> {
    let query = format!(
        "SELECT count(*) FROM {}",
        quote_clickhouse_identifier(table_name)
    );

    let body = fetch_clickhouse_query(config, &query).await?;

    let count = body.trim().parse::<u64>()?;

    Ok(count)
}

pub async fn create_clickhouse_snapshot_table(
    config: &ClickHouseConfig,
    schema: &TableSchema,
    clickhouse_table: &str,
) -> anyhow::Result<()> {
    let query = build_clickhouse_create_snapshot_table_query(schema, clickhouse_table);

    execute_clickhouse_query(config, &query).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn builds_select_query_from_schema() {
        let schema = TableSchema {
            table_name: "posts".to_string(),
            columns: vec![
                Column {
                    name: "post_id".to_string(),
                    postgres_type: "integer".to_string(),
                    is_nullable: false,
                    is_primary_key: true,
                },
                Column {
                    name: "title".to_string(),
                    postgres_type: "text".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                },
            ],
        };

        let query = build_select_query(&schema);

        assert!(query.contains("SELECT post_id, title"));
        assert!(query.contains("FROM posts"));
        assert!(query.contains("WHERE post_id > $1"));
        assert!(query.contains("ORDER BY post_id"));
        assert!(query.contains("LIMIT $2"));
    }

    struct CountingSnapshotRowWriter {
        expected_rows: usize,
    }

    #[async_trait]
    impl SnapshotRowWriter for CountingSnapshotRowWriter {
        async fn write_rows(&self, rows: &[SnapshotRow]) -> anyhow::Result<()> {
            assert_eq!(rows.len(), self.expected_rows);
            Ok(())
        }
    }

    #[tokio::test]
    async fn writes_staged_snapshot_rows_to_writer() {
        let path = std::env::temp_dir().join(format!(
            "pg_snapshot_reader_stage_writer_{}.jsonl",
            std::process::id()
        ));

        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }

        let mut values = HashMap::new();
        values.insert("id".to_string(), SnapshotValue::String("1".to_string()));
        values.insert(
            "name".to_string(),
            SnapshotValue::String("Alice".to_string()),
        );

        let rows = vec![SnapshotRow { values }];

        write_snapshot_rows_jsonl(&path, &rows).unwrap();

        let writer = CountingSnapshotRowWriter { expected_rows: 1 };

        write_staged_snapshot_rows(&path, &writer).await.unwrap();

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn builds_clickhouse_create_snapshot_table_query() {
        let schema = TableSchema {
            table_name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    postgres_type: "integer".to_string(),
                    is_nullable: false,
                    is_primary_key: true,
                },
                Column {
                    name: "name".to_string(),
                    postgres_type: "text".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                },
                Column {
                    name: "email".to_string(),
                    postgres_type: "text".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                },
            ],
        };

        let query = build_clickhouse_create_snapshot_table_query(&schema, "users_snapshot");

        assert!(query.contains("CREATE TABLE IF NOT EXISTS `users_snapshot`"));
        assert!(query.contains("`id` Int32"));
        assert!(query.contains("`name` String"));
        assert!(query.contains("`email` String"));
        assert!(query.contains("ENGINE = MergeTree"));
        assert!(query.contains("ORDER BY `id`"));
    }

    #[test]
    fn builds_clickhouse_insert_query() {
        let mut first_values = HashMap::new();
        first_values.insert("id".to_string(), SnapshotValue::String("1".to_string()));
        first_values.insert(
            "name".to_string(),
            SnapshotValue::String("Alice".to_string()),
        );

        let mut second_values = HashMap::new();
        second_values.insert("id".to_string(), SnapshotValue::String("2".to_string()));
        second_values.insert("name".to_string(), SnapshotValue::String("Bob".to_string()));

        let rows = vec![
            SnapshotRow {
                values: first_values,
            },
            SnapshotRow {
                values: second_values,
            },
        ];

        let query = build_clickhouse_insert_query("users_snapshot", &rows).unwrap();

        assert!(query.contains("INSERT INTO `users_snapshot`"));
        assert!(query.contains("`id`, `name`"));
        assert!(query.contains("'1'"));
        assert!(query.contains("'Alice'"));
        assert!(query.contains("'2'"));
        assert!(query.contains("'Bob'"));
    }

    #[test]
    fn maps_basic_postgres_types_to_clickhouse_types() {
        assert_eq!(
            map_postgres_type_to_clickhouse_type("integer").unwrap(),
            "Int32"
        );

        assert_eq!(
            map_postgres_type_to_clickhouse_type("text").unwrap(),
            "String"
        );

        assert_eq!(
            map_postgres_type_to_clickhouse_type("timestamp without time zone").unwrap(),
            "DateTime64(3)"
        );

        assert!(map_postgres_type_to_clickhouse_type("jsonb").is_err());
    }

    #[test]
    fn builds_clickhouse_json_each_row_payload() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), SnapshotValue::String("1".to_string()));
        values.insert(
            "name".to_string(),
            SnapshotValue::String("Alice".to_string()),
        );
        values.insert("deleted_at".to_string(), SnapshotValue::Null);

        let rows = vec![SnapshotRow { values }];

        let payload = build_clickhouse_json_each_row_payload(&rows).unwrap();

        assert!(payload.contains("\"id\":\"1\""));
        assert!(payload.contains("\"name\":\"Alice\""));
        assert!(payload.contains("\"deleted_at\":null"));
    }

    #[test]
    fn saves_and_loads_snapshot_boundary() {
        let path = std::env::temp_dir().join(format!(
            "pg_snapshot_reader_boundary_{}.json",
            std::process::id()
        ));

        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }

        let boundary = SnapshotBoundary {
            table_name: "users".to_string(),
            snapshot_id: "local-snapshot-test".to_string(),
            created_at: "2026-01-01 00:00:00.000".to_string(),
        };

        save_snapshot_boundary(&path, &boundary).unwrap();

        let loaded = load_snapshot_boundary(&path).unwrap();

        assert_eq!(loaded, Some(boundary));

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn quotes_postgres_identifier() {
        assert_eq!(quote_postgres_identifier("users"), "\"users\"");
        assert_eq!(
            quote_postgres_identifier("weird\"name"),
            "\"weird\"\"name\""
        );
    }

    #[test]
    fn parses_begin_decoded_wal_change() {
        let change = DecodedWalChange {
            lsn: "0/16B6C50".to_string(),
            xid: "123".to_string(),
            data: "BEGIN 123".to_string(),
        };

        let event = parse_decoded_wal_change(change);

        assert_eq!(event.kind, CdcEventKind::Begin);
    }

    #[test]
    fn parses_commit_decoded_wal_change() {
        let change = DecodedWalChange {
            lsn: "0/16B6C80".to_string(),
            xid: "123".to_string(),
            data: "COMMIT 123".to_string(),
        };

        let event = parse_decoded_wal_change(change);

        assert_eq!(event.kind, CdcEventKind::Commit);
    }

    #[test]
    fn parses_insert_decoded_wal_change() {
        let change = DecodedWalChange {
            lsn: "0/16B6C60".to_string(),
            xid: "123".to_string(),
            data: "table public.users: INSERT: id[integer]:1 name[text]:'Alice'".to_string(),
        };

        let event = parse_decoded_wal_change(change);

        assert_eq!(event.kind, CdcEventKind::Insert);
        assert_eq!(event.table_name, Some("public.users".to_string()));

        assert_eq!(
            event.column_values.get("id"),
            Some(&SnapshotValue::String("1".to_string()))
        );

        assert_eq!(
            event.column_values.get("name"),
            Some(&SnapshotValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn parses_update_decoded_wal_change() {
        let change = DecodedWalChange {
            lsn: "0/16B6C70".to_string(),
            xid: "123".to_string(),
            data: "table public.users: UPDATE: id[integer]:1 name[text]:'Alice Updated'"
                .to_string(),
        };

        let event = parse_decoded_wal_change(change);

        assert_eq!(event.kind, CdcEventKind::Update);
    }

    #[test]
    fn parses_delete_decoded_wal_change() {
        let change = DecodedWalChange {
            lsn: "0/16B6C75".to_string(),
            xid: "123".to_string(),
            data: "table public.users: DELETE: id[integer]:1".to_string(),
        };

        let event = parse_decoded_wal_change(change);

        assert_eq!(event.kind, CdcEventKind::Delete);
    }

    #[test]
    fn extracts_table_name_from_decoded_insert_change() {
        let table_name = extract_table_name_from_decoded_change(
            "table public.users: INSERT: id[integer]:1 name[text]:'Alice'",
        );

        assert_eq!(table_name, Some("public.users".to_string()));
    }

    #[test]
    fn returns_none_for_decoded_begin_change_table_name() {
        let table_name = extract_table_name_from_decoded_change("BEGIN 123");

        assert_eq!(table_name, None);
    }

    #[test]
    fn extracts_column_values_from_decoded_insert_change() {
        let values = extract_insert_column_values_from_decoded_change(
            "table public.users: INSERT: id[integer]:1 name[text]:'Alice'",
        );

        assert_eq!(
            values.get("id"),
            Some(&SnapshotValue::String("1".to_string()))
        );

        assert_eq!(
            values.get("name"),
            Some(&SnapshotValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn writes_and_reads_cdc_events_jsonl() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join("pg_snapshot_reader_cdc_events_test.jsonl");

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        let event = CdcEvent {
            lsn: "0/16B6C60".to_string(),
            xid: "123".to_string(),
            kind: CdcEventKind::Insert,
            table_name: Some("public.users".to_string()),
            column_values: HashMap::from([
                ("id".to_string(), SnapshotValue::String("1".to_string())),
                (
                    "name".to_string(),
                    SnapshotValue::String("Alice".to_string()),
                ),
            ]),
            raw_data: "table public.users: INSERT: id[integer]:1 name[text]:'Alice'".to_string(),
        };

        write_cdc_events_jsonl(&path, &[event.clone()])?;

        let events = read_cdc_events_jsonl(&path)?;

        assert_eq!(events, vec![event]);

        std::fs::remove_file(&path)?;

        Ok(())
    }

    #[test]
    fn creates_cdc_stage_batch_metadata_from_events() {
        let events_path = Path::new("cdc_events.jsonl");

        let events = vec![
            CdcEvent {
                lsn: "0/100".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Begin,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "BEGIN 1".to_string(),
            },
            CdcEvent {
                lsn: "0/120".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Insert,
                table_name: Some("public.users".to_string()),
                column_values: HashMap::new(),
                raw_data: "table public.users: INSERT: id[integer]:1".to_string(),
            },
            CdcEvent {
                lsn: "0/150".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Commit,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "COMMIT 1".to_string(),
            },
        ];

        let metadata = create_cdc_stage_batch_metadata("test_slot", events_path, &events)
            .expect("expected metadata");

        assert_eq!(metadata.slot_name, "test_slot");
        assert_eq!(metadata.start_lsn, "0/100");
        assert_eq!(metadata.end_lsn, "0/150");
        assert_eq!(metadata.event_count, 3);
        assert_eq!(metadata.events_path, "cdc_events.jsonl");
        assert_eq!(metadata.status, CdcStageBatchStatus::Pending);
        assert_eq!(metadata.retry_count, 0);
        assert_eq!(metadata.last_error, None);
    }

    #[test]
    fn does_not_create_cdc_stage_batch_metadata_for_empty_events() {
        let events_path = Path::new("cdc_events.jsonl");

        let metadata = create_cdc_stage_batch_metadata("test_slot", events_path, &[]);

        assert_eq!(metadata, None);
    }

    #[test]
    fn saves_and_loads_cdc_stage_batch_metadata() -> anyhow::Result<()> {
        let path =
            std::env::temp_dir().join("pg_snapshot_reader_cdc_stage_batch_metadata_test.json");

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        let metadata = CdcStageBatchMetadata {
            batch_id: "test_slot_0_100_0_100".to_string(),
            slot_name: "test_slot".to_string(),
            start_lsn: "0/100".to_string(),
            end_lsn: "0/150".to_string(),
            event_count: 3,
            events_path: "cdc_events.jsonl".to_string(),
            status: CdcStageBatchStatus::Pending,
            retry_count: 0,
            last_error: None,
        };

        save_cdc_stage_batch_metadata(&path, &metadata)?;

        let loaded = load_cdc_stage_batch_metadata(&path)?;

        assert_eq!(loaded, Some(metadata));

        std::fs::remove_file(&path)?;

        Ok(())
    }

    #[test]
    fn writes_cdc_events_jsonl_atomically() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join("pg_snapshot_reader_cdc_events_atomic_test.jsonl");

        let temp_path = path.with_extension("jsonl.tmp");

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        if temp_path.exists() {
            std::fs::remove_file(&temp_path)?;
        }

        let event = CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Insert,
            table_name: Some("public.users".to_string()),
            column_values: HashMap::from([(
                "id".to_string(),
                SnapshotValue::String("1".to_string()),
            )]),
            raw_data: "table public.users: INSERT: id[integer]:1".to_string(),
        };

        write_cdc_events_jsonl_atomic(&path, &[event.clone()])?;

        assert!(path.exists());
        assert!(!temp_path.exists());

        let events = read_cdc_events_jsonl(&path)?;

        assert_eq!(events, vec![event]);

        std::fs::remove_file(&path)?;

        Ok(())
    }

    #[test]
    fn updates_cdc_stage_batch_status() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join("pg_snapshot_reader_cdc_stage_batch_status_test.json");

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        let metadata = CdcStageBatchMetadata {
            batch_id: "test_slot_0_100_0_100".to_string(),
            slot_name: "test_slot".to_string(),
            start_lsn: "0/100".to_string(),
            end_lsn: "0/150".to_string(),
            event_count: 3,
            events_path: "cdc_events.jsonl".to_string(),
            status: CdcStageBatchStatus::Pending,
            retry_count: 0,
            last_error: None,
        };

        save_cdc_stage_batch_metadata(&path, &metadata)?;

        update_cdc_stage_batch_status(
            &path,
            CdcStageBatchStatus::Failed,
            Some("ClickHouse unavailable".to_string()),
        )?;

        let loaded = load_cdc_stage_batch_metadata(&path)?.expect("expected metadata");

        assert_eq!(loaded.status, CdcStageBatchStatus::Failed);
        assert_eq!(loaded.retry_count, 1);
        assert_eq!(
            loaded.last_error,
            Some("ClickHouse unavailable".to_string())
        );

        std::fs::remove_file(&path)?;

        Ok(())
    }

    #[test]
    fn validates_cdc_stage_batch() -> anyhow::Result<()> {
        let events_path =
            std::env::temp_dir().join("pg_snapshot_reader_valid_cdc_stage_batch.jsonl");

        if events_path.exists() {
            std::fs::remove_file(&events_path)?;
        }

        let events = vec![
            CdcEvent {
                lsn: "0/100".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Begin,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "BEGIN 1".to_string(),
            },
            CdcEvent {
                lsn: "0/120".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Insert,
                table_name: Some("public.users".to_string()),
                column_values: HashMap::new(),
                raw_data: "table public.users: INSERT: id[integer]:1".to_string(),
            },
            CdcEvent {
                lsn: "0/150".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Commit,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "COMMIT 1".to_string(),
            },
        ];

        write_cdc_events_jsonl_atomic(&events_path, &events)?;

        let metadata = CdcStageBatchMetadata {
            batch_id: "test_slot_0_100_0_100".to_string(),
            slot_name: "test_slot".to_string(),
            start_lsn: "0/100".to_string(),
            end_lsn: "0/150".to_string(),
            event_count: 3,
            events_path: events_path.to_string_lossy().to_string(),
            status: CdcStageBatchStatus::Pending,
            retry_count: 0,
            last_error: None,
        };

        let validated_events = validate_cdc_stage_batch(&metadata)?;

        assert_eq!(validated_events, events);

        std::fs::remove_file(&events_path)?;

        Ok(())
    }

    #[test]
    fn rejects_cdc_stage_batch_with_wrong_event_count() -> anyhow::Result<()> {
        let events_path =
            std::env::temp_dir().join("pg_snapshot_reader_invalid_cdc_stage_batch_count.jsonl");

        if events_path.exists() {
            std::fs::remove_file(&events_path)?;
        }

        let events = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        write_cdc_events_jsonl_atomic(&events_path, &events)?;

        let metadata = CdcStageBatchMetadata {
            batch_id: "test_slot_0_100_0_100".to_string(),
            slot_name: "test_slot".to_string(),
            start_lsn: "0/100".to_string(),
            end_lsn: "0/100".to_string(),
            event_count: 2,
            events_path: events_path.to_string_lossy().to_string(),
            status: CdcStageBatchStatus::Pending,
            retry_count: 0,
            last_error: None,
        };

        let result = validate_cdc_stage_batch(&metadata);

        assert!(result.is_err());

        std::fs::remove_file(&events_path)?;

        Ok(())
    }

    #[test]
    fn rejects_cdc_stage_batch_with_wrong_lsn_bounds() -> anyhow::Result<()> {
        let events_path =
            std::env::temp_dir().join("pg_snapshot_reader_invalid_cdc_stage_batch_lsn.jsonl");

        if events_path.exists() {
            std::fs::remove_file(&events_path)?;
        }

        let events = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        write_cdc_events_jsonl_atomic(&events_path, &events)?;

        let metadata = CdcStageBatchMetadata {
            batch_id: "test_slot_0_100_0_100".to_string(),
            slot_name: "test_slot".to_string(),
            start_lsn: "0/999".to_string(),
            end_lsn: "0/100".to_string(),
            event_count: 1,
            events_path: events_path.to_string_lossy().to_string(),
            status: CdcStageBatchStatus::Pending,
            retry_count: 0,
            last_error: None,
        };

        let result = validate_cdc_stage_batch(&metadata);

        assert!(result.is_err());

        std::fs::remove_file(&events_path)?;

        Ok(())
    }

    struct CountingCdcEventWriter {
        pub expected_count: usize,
    }

    #[async_trait]
    impl CdcEventWriter for CountingCdcEventWriter {
        async fn write_events(&self, events: &[CdcEvent]) -> anyhow::Result<()> {
            assert_eq!(events.len(), self.expected_count);
            Ok(())
        }
    }

    #[tokio::test]
    async fn delivers_cdc_stage_batch_and_marks_it_written() -> anyhow::Result<()> {
        let events_path = std::env::temp_dir().join("pg_snapshot_reader_deliver_cdc_events.jsonl");

        let metadata_path =
            std::env::temp_dir().join("pg_snapshot_reader_deliver_cdc_events.meta.json");

        if events_path.exists() {
            std::fs::remove_file(&events_path)?;
        }

        if metadata_path.exists() {
            std::fs::remove_file(&metadata_path)?;
        }

        let events = vec![
            CdcEvent {
                lsn: "0/100".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Begin,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "BEGIN 1".to_string(),
            },
            CdcEvent {
                lsn: "0/120".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Insert,
                table_name: Some("public.users".to_string()),
                column_values: HashMap::from([(
                    "id".to_string(),
                    SnapshotValue::String("1".to_string()),
                )]),
                raw_data: "table public.users: INSERT: id[integer]:1".to_string(),
            },
            CdcEvent {
                lsn: "0/150".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Commit,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "COMMIT 1".to_string(),
            },
        ];

        write_cdc_events_jsonl_atomic(&events_path, &events)?;

        let metadata = create_cdc_stage_batch_metadata("test_slot", &events_path, &events)
            .expect("expected metadata");

        save_cdc_stage_batch_metadata(&metadata_path, &metadata)?;

        let writer = CountingCdcEventWriter { expected_count: 3 };

        deliver_cdc_stage_batch(&metadata_path, &writer).await?;

        let loaded = load_cdc_stage_batch_metadata(&metadata_path)?.expect("expected metadata");

        assert_eq!(loaded.status, CdcStageBatchStatus::Written);
        assert_eq!(loaded.retry_count, 0);
        assert_eq!(loaded.last_error, None);

        std::fs::remove_file(&events_path)?;
        std::fs::remove_file(&metadata_path)?;

        Ok(())
    }

    struct FailingCdcEventWriter;

    #[async_trait]
    impl CdcEventWriter for FailingCdcEventWriter {
        async fn write_events(&self, _events: &[CdcEvent]) -> anyhow::Result<()> {
            anyhow::bail!("target unavailable")
        }
    }

    #[tokio::test]
    async fn marks_cdc_stage_batch_failed_when_delivery_fails() -> anyhow::Result<()> {
        let events_path = std::env::temp_dir().join("pg_snapshot_reader_failed_cdc_events.jsonl");

        let metadata_path =
            std::env::temp_dir().join("pg_snapshot_reader_failed_cdc_events.meta.json");

        if events_path.exists() {
            std::fs::remove_file(&events_path)?;
        }

        if metadata_path.exists() {
            std::fs::remove_file(&metadata_path)?;
        }

        let events = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        write_cdc_events_jsonl_atomic(&events_path, &events)?;

        let metadata = create_cdc_stage_batch_metadata("test_slot", &events_path, &events)
            .expect("expected metadata");

        save_cdc_stage_batch_metadata(&metadata_path, &metadata)?;

        let writer = FailingCdcEventWriter;

        let result = deliver_cdc_stage_batch(&metadata_path, &writer).await;

        assert!(result.is_err());

        let loaded = load_cdc_stage_batch_metadata(&metadata_path)?.expect("expected metadata");

        assert_eq!(loaded.status, CdcStageBatchStatus::Failed);
        assert_eq!(loaded.retry_count, 1);
        assert_eq!(loaded.last_error, Some("target unavailable".to_string()));

        std::fs::remove_file(&events_path)?;
        std::fs::remove_file(&metadata_path)?;

        Ok(())
    }

    struct PanicCdcEventWriter;

    #[async_trait]
    impl CdcEventWriter for PanicCdcEventWriter {
        async fn write_events(&self, _events: &[CdcEvent]) -> anyhow::Result<()> {
            panic!("writer should not be called for already written batches");
        }
    }

    #[tokio::test]
    async fn skips_already_written_cdc_stage_batch() -> anyhow::Result<()> {
        let events_path =
            std::env::temp_dir().join("pg_snapshot_reader_skip_written_cdc_events.jsonl");

        let metadata_path =
            std::env::temp_dir().join("pg_snapshot_reader_skip_written_cdc_events.meta.json");

        if events_path.exists() {
            std::fs::remove_file(&events_path)?;
        }

        if metadata_path.exists() {
            std::fs::remove_file(&metadata_path)?;
        }

        let events = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        write_cdc_events_jsonl_atomic(&events_path, &events)?;

        let mut metadata = create_cdc_stage_batch_metadata("test_slot", &events_path, &events)
            .expect("expected metadata");

        metadata.status = CdcStageBatchStatus::Written;

        save_cdc_stage_batch_metadata(&metadata_path, &metadata)?;

        let writer = PanicCdcEventWriter;

        deliver_cdc_stage_batch(&metadata_path, &writer).await?;

        let loaded = load_cdc_stage_batch_metadata(&metadata_path)?.expect("expected metadata");

        assert_eq!(loaded.status, CdcStageBatchStatus::Written);
        assert_eq!(loaded.retry_count, 0);
        assert_eq!(loaded.last_error, None);

        std::fs::remove_file(&events_path)?;
        std::fs::remove_file(&metadata_path)?;

        Ok(())
    }

    #[test]
    fn creates_cdc_stage_batch_id_from_slot_and_lsn_bounds() {
        let batch_id = create_cdc_stage_batch_id("test_slot", "0/100", "0/150");

        assert_eq!(batch_id, "test_slot_0_100_0_150");
    }

    #[test]
    fn creates_cdc_stage_batch_paths_from_stage_dir_and_batch_id() {
        let stage_dir = Path::new("cdc_stage");
        let batch_id = "test_slot_0_100_0_150";

        let paths = create_cdc_stage_batch_paths(stage_dir, batch_id);

        assert_eq!(
            paths.events_path,
            Path::new("cdc_stage").join("test_slot_0_100_0_150.jsonl")
        );

        assert_eq!(
            paths.metadata_path,
            Path::new("cdc_stage").join("test_slot_0_100_0_150.meta.json")
        );
    }

    #[test]
    fn writes_cdc_stage_batch_with_events_and_metadata() -> anyhow::Result<()> {
        let stage_dir = std::env::temp_dir().join("pg_snapshot_reader_write_cdc_stage_batch_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let events = vec![
            CdcEvent {
                lsn: "0/100".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Begin,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "BEGIN 1".to_string(),
            },
            CdcEvent {
                lsn: "0/120".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Insert,
                table_name: Some("public.users".to_string()),
                column_values: HashMap::from([(
                    "id".to_string(),
                    SnapshotValue::String("1".to_string()),
                )]),
                raw_data: "table public.users: INSERT: id[integer]:1".to_string(),
            },
            CdcEvent {
                lsn: "0/150".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Commit,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "COMMIT 1".to_string(),
            },
        ];

        let metadata =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events)?.expect("expected metadata");

        assert_eq!(metadata.batch_id, "test_slot_0_100_0_150");
        assert_eq!(metadata.slot_name, "test_slot");
        assert_eq!(metadata.start_lsn, "0/100");
        assert_eq!(metadata.end_lsn, "0/150");
        assert_eq!(metadata.event_count, 3);
        assert_eq!(metadata.status, CdcStageBatchStatus::Pending);

        let paths = create_cdc_stage_batch_paths(&stage_dir, &metadata.batch_id);

        assert!(paths.events_path.exists());
        assert!(paths.metadata_path.exists());

        let loaded_metadata =
            load_cdc_stage_batch_metadata(&paths.metadata_path)?.expect("expected metadata");

        assert_eq!(loaded_metadata, metadata);

        let loaded_events = read_cdc_events_jsonl(&paths.events_path)?;

        assert_eq!(loaded_events, events);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }

    #[test]
    fn does_not_write_cdc_stage_batch_for_empty_events() -> anyhow::Result<()> {
        let stage_dir = std::env::temp_dir().join("pg_snapshot_reader_empty_cdc_stage_batch_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let metadata = write_cdc_stage_batch(&stage_dir, "test_slot", &[])?;

        assert_eq!(metadata, None);

        assert_eq!(std::fs::read_dir(&stage_dir)?.count(), 0);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }

    #[test]
    fn reads_cdc_stage_batch_from_metadata() -> anyhow::Result<()> {
        let stage_dir = std::env::temp_dir().join("pg_snapshot_reader_read_cdc_stage_batch_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let events = vec![
            CdcEvent {
                lsn: "0/100".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Begin,
                table_name: None,
                column_values: HashMap::new(),
                raw_data: "BEGIN 1".to_string(),
            },
            CdcEvent {
                lsn: "0/120".to_string(),
                xid: "1".to_string(),
                kind: CdcEventKind::Insert,
                table_name: Some("public.users".to_string()),
                column_values: HashMap::from([(
                    "id".to_string(),
                    SnapshotValue::String("1".to_string()),
                )]),
                raw_data: "table public.users: INSERT: id[integer]:1".to_string(),
            },
        ];

        let metadata =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events)?.expect("expected metadata");

        let paths = create_cdc_stage_batch_paths(&stage_dir, &metadata.batch_id);

        let batch = read_cdc_stage_batch(&paths.metadata_path)?;

        assert_eq!(batch.metadata, metadata);
        assert_eq!(batch.events, events);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }

    #[test]
    fn lists_cdc_stage_batch_metadata() -> anyhow::Result<()> {
        let stage_dir =
            std::env::temp_dir().join("pg_snapshot_reader_list_cdc_stage_metadata_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let events_a = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        let events_b = vec![CdcEvent {
            lsn: "0/200".to_string(),
            xid: "2".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 2".to_string(),
        }];

        let metadata_a =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events_a)?.expect("expected metadata");

        let metadata_b =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events_b)?.expect("expected metadata");

        let metadata = list_cdc_stage_batch_metadata(&stage_dir)?;

        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0], metadata_a);
        assert_eq!(metadata[1], metadata_b);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }

    #[test]
    fn lists_only_deliverable_cdc_stage_batch_metadata() -> anyhow::Result<()> {
        let stage_dir = std::env::temp_dir()
            .join("pg_snapshot_reader_list_deliverable_cdc_stage_metadata_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let pending_events = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        let written_events = vec![CdcEvent {
            lsn: "0/200".to_string(),
            xid: "2".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 2".to_string(),
        }];

        let failed_events = vec![CdcEvent {
            lsn: "0/300".to_string(),
            xid: "3".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 3".to_string(),
        }];

        let pending_metadata = write_cdc_stage_batch(&stage_dir, "test_slot", &pending_events)?
            .expect("expected metadata");

        let mut written_metadata = write_cdc_stage_batch(&stage_dir, "test_slot", &written_events)?
            .expect("expected metadata");

        let mut failed_metadata = write_cdc_stage_batch(&stage_dir, "test_slot", &failed_events)?
            .expect("expected metadata");

        written_metadata.status = CdcStageBatchStatus::Written;
        failed_metadata.status = CdcStageBatchStatus::Failed;
        failed_metadata.retry_count = 1;
        failed_metadata.last_error = Some("target unavailable".to_string());

        let written_paths = create_cdc_stage_batch_paths(&stage_dir, &written_metadata.batch_id);
        let failed_paths = create_cdc_stage_batch_paths(&stage_dir, &failed_metadata.batch_id);

        save_cdc_stage_batch_metadata(&written_paths.metadata_path, &written_metadata)?;
        save_cdc_stage_batch_metadata(&failed_paths.metadata_path, &failed_metadata)?;

        let deliverable = list_deliverable_cdc_stage_batch_metadata(&stage_dir)?;

        assert_eq!(deliverable.len(), 2);
        assert_eq!(deliverable[0], pending_metadata);
        assert_eq!(deliverable[1], failed_metadata);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }

    #[tokio::test]
    async fn delivers_pending_cdc_stage_batches() -> anyhow::Result<()> {
        let stage_dir =
            std::env::temp_dir().join("pg_snapshot_reader_deliver_pending_cdc_stage_batches_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let events_a = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        let events_b = vec![CdcEvent {
            lsn: "0/200".to_string(),
            xid: "2".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 2".to_string(),
        }];

        let metadata_a =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events_a)?.expect("expected metadata");

        let metadata_b =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events_b)?.expect("expected metadata");

        let writer = CountingCdcEventWriter { expected_count: 1 };

        let delivered_count = deliver_pending_cdc_stage_batches(&stage_dir, &writer).await?;

        assert_eq!(delivered_count, 2);

        let paths_a = create_cdc_stage_batch_paths(&stage_dir, &metadata_a.batch_id);
        let paths_b = create_cdc_stage_batch_paths(&stage_dir, &metadata_b.batch_id);

        let loaded_a =
            load_cdc_stage_batch_metadata(&paths_a.metadata_path)?.expect("expected metadata");
        let loaded_b =
            load_cdc_stage_batch_metadata(&paths_b.metadata_path)?.expect("expected metadata");

        assert_eq!(loaded_a.status, CdcStageBatchStatus::Written);
        assert_eq!(loaded_b.status, CdcStageBatchStatus::Written);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }

    #[tokio::test]
    async fn deliver_pending_cdc_stage_batches_skips_written_batches() -> anyhow::Result<()> {
        let stage_dir =
            std::env::temp_dir().join("pg_snapshot_reader_skip_written_pending_delivery_test");

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir)?;
        }

        std::fs::create_dir_all(&stage_dir)?;

        let events = vec![CdcEvent {
            lsn: "0/100".to_string(),
            xid: "1".to_string(),
            kind: CdcEventKind::Begin,
            table_name: None,
            column_values: HashMap::new(),
            raw_data: "BEGIN 1".to_string(),
        }];

        let mut metadata =
            write_cdc_stage_batch(&stage_dir, "test_slot", &events)?.expect("expected metadata");

        metadata.status = CdcStageBatchStatus::Written;

        let paths = create_cdc_stage_batch_paths(&stage_dir, &metadata.batch_id);

        save_cdc_stage_batch_metadata(&paths.metadata_path, &metadata)?;

        let writer = PanicCdcEventWriter;

        let delivered_count = deliver_pending_cdc_stage_batches(&stage_dir, &writer).await?;

        assert_eq!(delivered_count, 0);

        std::fs::remove_dir_all(&stage_dir)?;

        Ok(())
    }
}
