use std::time::{SystemTime, UNIX_EPOCH};

use pg_snapshot_reader::read_users_batch;
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
