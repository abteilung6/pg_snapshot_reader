use tokio_postgres::{NoTls, Error};

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

    let rows = client
        .query("SELECT id, name, email FROM users", &[])
        .await?;

    for row in rows {
        let id: i32 = row.get("id");
        let name: String = row.get("name");
        let email: String = row.get("email");

        println!("id={}, name={}, email={}", id, name, email);
    }

    Ok(())
}
