use tokio_postgres::{Error, NoTls};

use pg_snapshot_reader::read_users_from_table;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let connection_string =
        "host=localhost port=5432 user=postgres password=postgres dbname=snapshot_demo";

    let (client, connection) =
        tokio_postgres::connect(connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let users = read_users_from_table(&client, "users").await?;

    for user in users {
        println!(
            "id={}, name={}, email={}",
            user.id, user.name, user.email
        );
    }

    Ok(())
}