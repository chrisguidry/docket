//! An application's own keys in the docket's Redis, through the connection
//! that `Docket::redis` opens, as through pydocket's `docket.redis()`.

use docket::Docket;
use redis::AsyncCommands;
use redis::aio::ConnectionLike;

use crate::support::{docket, shared_url};

#[tokio::test]
async fn an_application_keeps_its_own_keys_in_the_dockets_redis() {
    let docket = docket().await;
    let key = format!("{}:cleanup", docket.name());
    let mut connection = docket.redis().await.unwrap();

    let () = connection.zadd(&key, "run-1", 10).await.unwrap();

    // A second docket on the same URL reaches the same keys, memory://
    // included, where no other Redis client can connect.
    let other = Docket::connect("other", shared_url(&docket)).await.unwrap();
    let mut other = other.redis().await.unwrap();
    let members: Vec<(String, f64)> = other.zrange_withscores(&key, 0, -1).await.unwrap();
    assert_eq!(members, [("run-1".to_owned(), 10.0)]);
}

#[tokio::test]
async fn the_connection_runs_pipelines_on_the_dockets_database() {
    let docket = docket().await;
    let key = format!("{}:counts", docket.name());
    let mut connection = docket.redis().await.unwrap();

    let (first, second): (i64, i64) = redis::pipe()
        .hincr(&key, "runs", 1)
        .hincr(&key, "runs", 2)
        .query_async(&mut connection)
        .await
        .unwrap();

    assert_eq!((first, second), (1, 3));
    assert_eq!(connection.get_db(), 0);
}
