use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
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

#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotValue {
    String(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRow {
    pub values: HashMap<String, SnapshotValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCheckpoint {
    pub table_name: String,
    pub primary_key_column: String,
    pub last_seen_primary_key: String,
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
                        Some(v) => SnapshotValue::String(format!("{:?}", v)),
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
