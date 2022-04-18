use bson::{doc, Document};

use crate::{error::Result, db::options::{CreateCollectionOptions, ClusteredIndex}};

use super::TestClient;

use futures::stream::{StreamExt, TryStreamExt};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() -> Result<()> {
    let client = TestClient::new().await;
    let db = client.database("clustered_coll_tests");
    db.drop(None).await?;
    db.create_collection("test_coll",
        CreateCollectionOptions::builder()
            .clustered_index(ClusteredIndex::Options(doc! {
                "key": { "_id": 1 },
                "unique": true,
            }))
            .build()
    ).await?;
    let colls: Vec<_> = db.list_collections(None, None).await?
        .try_collect().await?;
    println!("{:#?}", colls);
    let indexes: Vec<_> = db.collection::<Document>("test_coll")
        .list_indexes(None).await?
        .try_collect().await?;
    println!("{:#?}", indexes);
    
    Ok(())
}