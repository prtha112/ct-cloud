use sqlx::mssql::MssqlPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let pool = MssqlPoolOptions::new()
        .connect("mssql://sa:Password123!@localhost:1435/testct").await?;

    let val = "2026-02-20T00:00:00.000";
    let res = sqlx::query("INSERT INTO [User] (id, username, password, email, lll, test, create_at) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)")
        .bind(99i64)
        .bind("test")
        .bind("test")
        .bind("test")
        .bind(None::<String>) // real
        .bind(None::<String>) // numeric
        .bind(val) // datetime
        .execute(&pool).await?;

    println!("Success: {:?}", res);
    Ok(())
}
