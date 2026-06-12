use std::time::{SystemTime, UNIX_EPOCH};

use pg_snapshot_reader::read_users_from_table;
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

    let (client, connection) =
        tokio_postgres::connect(connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

#[tokio::test]
async fn reads_users_from_custom_table() -> Result<(), Error> {
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

    let users = read_users_from_table(&client, &table_name).await?;

    assert_eq!(users.len(), 2);
    assert_eq!(users[0].name, "Alice");
    assert_eq!(users[1].email, "bob@example.com");

    let drop_sql = format!("DROP TABLE {}", table_name);
    client.execute(&drop_sql, &[]).await?;

    Ok(())
}
