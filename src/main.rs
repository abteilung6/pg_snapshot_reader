use tokio_postgres::{Error, NoTls};

use pg_snapshot_reader::read_users_batch;

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

    let mut last_seen_id = 0;
    let batch_size = 2;

    loop {
        let users = read_users_batch(
            &client,
            "users",
            last_seen_id,
            batch_size,
        )
        .await?;

        if users.is_empty() {
            break;
        }

        for user in &users {
            println!(
                "id={}, name={}, email={}",
                user.id, user.name, user.email
            );
        }

        last_seen_id = users.last().unwrap().id;
    }

    Ok(())
}
