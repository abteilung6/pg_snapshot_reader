use tokio_postgres::{Client, Error};

#[derive(Debug, PartialEq)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
}

pub async fn read_users_from_table(
    client: &Client,
    table_name: &str,
) -> Result<Vec<User>, Error> {
    let query = format!(
        "SELECT id, name, email FROM {} ORDER BY id",
        table_name
    );

    let rows = client.query(&query, &[]).await?;

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