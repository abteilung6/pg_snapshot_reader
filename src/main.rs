use tokio_postgres::{Error, NoTls};

use pg_snapshot_reader::{discover_table_schema, read_snapshot_rows_batch};

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

    let rows = read_snapshot_rows_batch(&client, &schema, 0, 10).await?;

    for row in rows {
        println!("{:#?}", row);
    }

    Ok(())
}
