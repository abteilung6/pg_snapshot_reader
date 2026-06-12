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
