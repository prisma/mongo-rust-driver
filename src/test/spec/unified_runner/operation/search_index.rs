use bson::{Bson, to_bson, Document};
use futures_core::future::BoxFuture;
use futures_util::{FutureExt, TryStreamExt};
use serde::Deserialize;

use crate::{error::Result, search_index::options::{CreateSearchIndexOptions, DropSearchIndexOptions, ListSearchIndexOptions, UpdateSearchIndexOptions}, SearchIndexModel, test::spec::unified_runner::{TestRunner, Entity}, coll::options::AggregateOptions};

use super::TestOperation;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateSearchIndex {
    model: SearchIndexModel,
    #[serde(flatten)]
    options: CreateSearchIndexOptions,
}

impl TestOperation for CreateSearchIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let name = collection.create_search_index(self.model.clone(), self.options.clone()).await?;
            Ok(Some(Bson::String(name).into()))
        }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateSearchIndexes {
    models: Vec<SearchIndexModel>,
    #[serde(flatten)]
    options: CreateSearchIndexOptions,
}

impl TestOperation for CreateSearchIndexes {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let names = collection.create_search_indexes(self.models.clone(), self.options.clone()).await?;
            Ok(Some(to_bson(&names)?.into()))
        }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DropSearchIndex {
    name: String,
    #[serde(flatten)]
    options: DropSearchIndexOptions,
}

impl TestOperation for DropSearchIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            collection.drop_search_index(&self.name, self.options.clone()).await?;
            Ok(None)
        }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListSearchIndexes {
    name: Option<String>,
    aggregation_options: Option<AggregateOptions>,
    #[serde(flatten)]
    options: ListSearchIndexOptions,
}

impl TestOperation for ListSearchIndexes {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let cursor = collection.list_search_indexes(self.name.as_ref().map(|s| s.as_str()), self.aggregation_options.clone(), self.options.clone()).await?;
            let values: Vec<_> = cursor.try_collect().await?;
            Ok(Some(to_bson(&values)?.into()))
        }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateSearchIndex {
    name: String,
    definition: Document,
    #[serde(flatten)]
    options: UpdateSearchIndexOptions,
}

impl TestOperation for UpdateSearchIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            collection.update_search_index(&self.name, self.definition.clone(), self.options.clone()).await?;
            Ok(None)
        }.boxed()
    }
}