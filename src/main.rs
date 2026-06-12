use tokio_postgres::{Error, NoTls};

struct User {
    id: i32,
    name: String,
    email: String,
}

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

    let users = read_users(&client).await?;

    for user in users {
        println!(
            "id={}, name={}, email={}",
            user.id,
            user.name,
            user.email
        );
    }

    Ok(())
}

async fn read_users(
    client: &tokio_postgres::Client
) -> Result<Vec<User>, Error> {
    let rows = client
        .query("SELECT id, name, email FROM users", &[])
        .await?;

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
