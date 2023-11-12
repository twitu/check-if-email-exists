use sqlx::PgPool;
use sqlx::Result;
use log::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().expect("Unable to load environment variables from .env file");
    env_logger::init(); // Initialize the logger

    let db_url = std::env::var("DATABASE_URL").expect("Unable to read DATABASE_URL env var");
    let days_old_str = std::env::var("DAYS_OLD").expect("Unable to read DAYS_OLD env var");
    let days_old: i32 = days_old_str.parse().expect("Unable to parse DAYS_OLD as integer");

    let pool = PgPool::connect(&db_url).await?;

    // Fetch the list of job IDs that match the criteria
    let interval_days: i32 = 0; // Set the interval to 1 for testing purposes, adjust as needed
    let query = format!(
        "SELECT b.id
        FROM bulk_jobs b
        JOIN (
            SELECT job_id, COUNT(*) as total_processed
            FROM email_results
            GROUP BY job_id
        ) e ON b.id = e.job_id
        WHERE b.total_records = e.total_processed
        AND b.created_at <= current_date - interval '{} days'",
        interval_days
    );

    let job_ids_to_delete: Vec<(i32,)> = sqlx::query_as(&query).fetch_all(&pool).await?;
    let job_ids_to_delete: Vec<i32> = job_ids_to_delete_vec.into_iter().map(|&(id,)| id).collect();

    if !job_ids_to_delete.is_empty() {
        
        // Convert job_ids to comma separated string to pass to query
        let job_ids_string = vec.iter().map(|x| x.to_string() + ",").collect::<String>();
        let job_ids_string = job_ids_string.trim_end_matches(",");

        // Create transaction to delete records from email_results and bulk_jobs tables
        let delete_query = sqlx::query!(
			r#"
            BEGIN;
            DELETE FROM email_results WHERE job_id IN ($1);
            DELETE FROM bulk_jobs WHERE id IN ($1);
            END;
			"#,
            job_ids_string
		);

        // Execute the delete query
        sqlx::query(delete_query)
            .execute(&pool) // Use execute on the query builder
            .await?;

        info!(
            "Email results and bulk job records for  IDs {:?} deleted successfully.",
            job_ids_to_delete
        );
    }

    Ok(())
}
