use std::time::{SystemTime, UNIX_EPOCH};

use pg_snapshot_reader::{
    SnapshotValue, discover_table_schema, read_full_snapshot, read_snapshot_rows_batch,
    read_users_batch,
};
use tokio_postgres::{Client, Error, NoTls};

fn unique_table_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    format!("users_test_{}", millis)
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
async fn reads_users_in_batches() -> Result<(), Error> {
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
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let first_batch = read_users_batch(&client, &table_name, 0, 2).await?;

    assert_eq!(first_batch.len(), 2);
    assert_eq!(first_batch[0].name, "Alice");
    assert_eq!(first_batch[1].name, "Bob");

    let last_seen_id = first_batch.last().unwrap().id;

    let second_batch = read_users_batch(&client, &table_name, last_seen_id, 2).await?;

    assert_eq!(second_batch.len(), 1);
    assert_eq!(second_batch[0].name, "Charlie");

    let final_last_seen_id = second_batch.last().unwrap().id;

    let third_batch = read_users_batch(&client, &table_name, final_last_seen_id, 2).await?;

    assert_eq!(third_batch.len(), 0);

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}

#[tokio::test]
async fn reads_full_snapshot_in_batches() -> Result<(), Error> {
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
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com')
        ",
        table_name
    );

    client.execute(&insert_sql, &[]).await?;

    let users = read_full_snapshot(&client, &table_name, 2).await?;

    assert_eq!(users.len(), 3);
    assert_eq!(users[0].name, "Alice");
    assert_eq!(users[1].name, "Bob");
    assert_eq!(users[2].name, "Charlie");

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

    let rows = read_snapshot_rows_batch(&client, &table_name, 0, 10).await?;

    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].values[0], ("id".to_string(), SnapshotValue::Int(1)));

    assert_eq!(
        rows[0].values[1],
        ("name".to_string(), SnapshotValue::Text("Alice".to_string()))
    );

    assert_eq!(
        rows[0].values[2],
        (
            "email".to_string(),
            SnapshotValue::Text("alice@example.com".to_string())
        )
    );

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}
