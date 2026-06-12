use tokio_postgres::{Client, Error};

#[derive(Debug, PartialEq)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
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
