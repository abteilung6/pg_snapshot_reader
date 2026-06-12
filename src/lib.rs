use tokio_postgres::{Client, Error};

#[derive(Debug, PartialEq)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
}

pub async fn read_users(client: &Client) -> Result<Vec<User>, Error> {
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
