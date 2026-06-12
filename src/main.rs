use tokio_postgres::{Error, NoTls};

use pg_snapshot_reader::{discover_table_schema, read_full_snapshot};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let connection_string =
        "host=localhost port=5432 user=postgres password=postgres dbname=snapshot_demo";

    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let schema = discover_table_schema(&client, "users").await?;

    println!("{:#?}", schema);

    let users = read_full_snapshot(&client, "users", 2).await?;

    for user in users {
        println!("id={}, name={}, email={}", user.id, user.name, user.email);
    }

    Ok(())
}
