use sqlx::mssql::MssqlPoolOptions;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = MssqlPoolOptions::new().connect("mssql://sa:Password123!@localhost:1435/testct").await?;
    let sql = "SET IDENTITY_INSERT Customer ON; INSERT INTO Customer (CustomerId, ExternalCode, FullName) VALUES (@p1, @p2, @p3); SET IDENTITY_INSERT Customer OFF;";
    sqlx::query(sql).bind(99).bind("ext").bind("Test").execute(&pool).await?;
    println!("SUCCESS");
    Ok(())
}
