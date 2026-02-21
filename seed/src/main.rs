use sqlx::mssql::MssqlPoolOptions;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "mssql://sa:Password123!@localhost:1434/testct";

    println!("Connecting to database at {}...", database_url);
    
    let pool = MssqlPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    println!("Connected successfully!\n");

    // ==========================================
    // 1. Seed Product Table
    // ==========================================
    let total_products = 200_000;
    
    println!("--- [1] Processing dbo.Product ---");
    println!("Clearing old data from dbo.Product...");
    sqlx::query("DELETE FROM dbo.Product").execute(&pool).await?;

    println!("Starting to insert {} Product records using transactions...", total_products);
    let start_time_product = Instant::now();

    let mut tx = pool.begin().await?;
    let mut batch_count = 0;

    for i in 1..=total_products {
        let name = format!("Product {}", i);
        let category = format!("Category {}", (i % 10) + 1);
        let price = format!("{}.99", i % 500);

        sqlx::query("INSERT INTO dbo.Product (id, Name, Category, Price) VALUES (@p1, @p2, @p3, @p4)")
            .bind(i)
            .bind(name)
            .bind(category)
            .bind(price)
            .execute(&mut tx)
            .await?;
            
        batch_count += 1;
        
        if batch_count % 500 == 0 {
            tx.commit().await?;
            println!("Inserted {} / {} Product records", batch_count, total_products);
            tx = pool.begin().await?;
        }
    }
    
    if batch_count % 500 != 0 {
        tx.commit().await?;
    }

    let duration_product = start_time_product.elapsed();
    println!("Successfully inserted {} Product records in {:.2} seconds!\n", total_products, duration_product.as_secs_f64());

    // ==========================================
    // 2. Seed Customer Table
    // ==========================================
    let total_customers = 50_000;
    
    println!("--- [2] Processing dbo.Customer ---");
    println!("Clearing old data from dbo.Customer...");
    sqlx::query("DELETE FROM dbo.Customer").execute(&pool).await?;

    println!("Starting to insert {} Customer records using transactions...", total_customers);
    let start_time_customer = Instant::now();

    let mut tx_cust = pool.begin().await?;
    let mut batch_count_cust = 0;

    for i in 1..=total_customers {
        let external_code = format!("EXT-CUST-{:07}", i); 
        let full_name = format!("Customer Name {}", i);
        let email = format!("customer{}@testct.local", i);
        let status = (i % 3) + 1; 

        sqlx::query("INSERT INTO dbo.Customer (ExternalCode, FullName, Email, Status) VALUES (@p1, @p2, @p3, @p4)")
            .bind(external_code)
            .bind(full_name)
            .bind(email)
            .bind(status as i16) 
            .execute(&mut tx_cust)
            .await?;
            
        batch_count_cust += 1;
        
        if batch_count_cust % 500 == 0 {
            tx_cust.commit().await?;
            println!("Inserted {} / {} Customer records", batch_count_cust, total_customers);
            tx_cust = pool.begin().await?;
        }
    }
    
    if batch_count_cust % 500 != 0 {
        tx_cust.commit().await?;
    }

    let duration_customer = start_time_customer.elapsed();
    println!("Successfully inserted {} Customer records in {:.2} seconds!\n", total_customers, duration_customer.as_secs_f64());

    println!("All seeding completed successfully!");
    Ok(())
}
