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
    Int(i32),
    Text(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRow {
    pub values: Vec<(String, SnapshotValue)>,
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
    table_name: &str,
    last_seen_id: i32,
    limit: i64,
) -> Result<Vec<SnapshotRow>, Error> {
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

    let mut snapshot_rows = Vec::new();

    for row in rows {
        let snapshot_row = SnapshotRow {
            values: vec![
                ("id".to_string(), SnapshotValue::Int(row.get("id"))),
                ("name".to_string(), SnapshotValue::Text(row.get("name"))),
                ("email".to_string(), SnapshotValue::Text(row.get("email"))),
            ],
        };

        snapshot_rows.push(snapshot_row);
    }

    Ok(snapshot_rows)
}
