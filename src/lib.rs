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
    let drop_query = "
        SELECT pg_drop_replication_slot(slot_name)
        FROM pg_replication_slots
        WHERE slot_name = $1
    ";

    client.execute(drop_query, &[&slot_name]).await?;

    let create_query = "
        SELECT *
        FROM pg_create_logical_replication_slot($1, 'pgoutput')
    ";

    client.query(create_query, &[&slot_name]).await?;

    Ok(())
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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

pub async fn write_staged_snapshot_rows<W>(stage_path: &Path, writer: &W) -> anyhow::Result<()>
where
    W: SnapshotRowWriter + Sync,
{
    let rows = read_snapshot_rows_jsonl(stage_path)?;
    writer.write_rows(&rows).await?;

    Ok(())
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
}
