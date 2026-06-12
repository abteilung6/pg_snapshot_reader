use std::collections::HashMap;
use tokio_postgres::{Client, Error};

#[derive(Debug, PartialEq)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
}

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

impl TableSchema {
    pub fn primary_key_column(&self) -> &Column {
        self.columns
            .iter()
            .find(|column| column.is_primary_key)
            .expect("expected table to have a primary key")
    }
}

pub async fn read_users_batch(
    client: &Client,
    table_name: &str,
    last_seen_id: i32,
    limit: i64,
) -> Result<Vec<User>, Error> {
    let query = format!(
        "
        SELECT id, name, email
        FROM {}
        WHERE id > $1
        ORDER BY id
        LIMIT $2
        ",
        table_name
    );

    let rows = client.query(&query, &[&last_seen_id, &limit]).await?;

    let mut users = Vec::new();

    for row in rows {
        users.push(User {
            id: row.get("id"),
            name: row.get("name"),
            email: row.get("email"),
        });
    }

    Ok(users)
}

pub async fn read_full_snapshot(
    client: &Client,
    table_name: &str,
    batch_size: i64,
) -> Result<Vec<User>, Error> {
    let mut all_users = Vec::new();
    let mut last_seen_id = 0;

    loop {
        let batch = read_users_batch(client, table_name, last_seen_id, batch_size).await?;

        if batch.is_empty() {
            break;
        }

        last_seen_id = batch.last().unwrap().id;

        all_users.extend(batch);
    }

    Ok(all_users)
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
