//! Action builder types.

mod aggregate;
mod count;
mod create_collection;
mod create_index;
#[cfg(feature = "in-use-encryption-unstable")]
pub mod csfle;
mod delete;
mod distinct;
mod drop;
mod drop_index;
mod find;
mod find_and_modify;
pub mod gridfs;
mod insert_many;
mod insert_one;
mod list_collections;
mod list_databases;
mod list_indexes;
mod perf;
mod replace_one;
mod run_command;
mod search_index;
mod session;
mod shutdown;
mod transaction;
mod update;
mod watch;

use std::{future::IntoFuture, marker::PhantomData, ops::Deref};

pub use aggregate::Aggregate;
use bson::Document;
pub use count::{CountDocuments, EstimatedDocumentCount};
pub use create_collection::CreateCollection;
pub use create_index::CreateIndex;
pub use delete::Delete;
pub use distinct::Distinct;
pub use drop::{DropCollection, DropDatabase};
pub use drop_index::DropIndex;
pub use find::{Find, FindOne};
pub use find_and_modify::{FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate};
pub use insert_many::InsertMany;
pub use insert_one::InsertOne;
pub use list_collections::ListCollections;
pub use list_databases::ListDatabases;
pub use list_indexes::ListIndexes;
pub use perf::WarmConnectionPool;
pub use replace_one::ReplaceOne;
pub use run_command::{RunCommand, RunCursorCommand};
pub use search_index::{CreateSearchIndex, DropSearchIndex, ListSearchIndexes, UpdateSearchIndex};
pub use session::StartSession;
pub use shutdown::Shutdown;
pub use transaction::{CommitTransaction, StartTransaction};
pub use update::Update;
pub use watch::Watch;

#[allow(missing_docs)]
pub struct ListSpecifications;
#[allow(missing_docs)]
pub struct ListNames;

#[allow(missing_docs)]
pub struct ImplicitSession;
#[allow(missing_docs)]
pub struct ExplicitSession<'a>(&'a mut crate::ClientSession);

#[allow(missing_docs)]
pub struct Single;
#[allow(missing_docs)]
pub struct Multiple;

macro_rules! option_setters {
    // Include options aggregate accessors.
    (
        $opt_field:ident: $opt_field_ty:ty;
        $(
            $(#[$($attrss:tt)*])*
            $opt_name:ident: $opt_ty:ty,
        )*
    ) => {
        #[allow(unused)]
        fn options(&mut self) -> &mut $opt_field_ty {
            self.$opt_field.get_or_insert_with(<$opt_field_ty>::default)
        }

        /// Set all options.  Note that this will replace all previous values set.
        pub fn with_options(mut self, value: impl Into<Option<$opt_field_ty>>) -> Self {
            self.$opt_field = value.into();
            self
        }

        crate::action::option_setters!($opt_field_ty;
            $(
                $(#[$($attrss)*])*
                $opt_name: $opt_ty,
            )*
        );
    };
    // Just generate field setters.
    (
        $opt_field_ty:ty;
        $(
            $(#[$($attrss:tt)*])*
            $opt_name:ident: $opt_ty:ty,
        )*
    ) => {
        $(
            #[doc = concat!("Set the [`", stringify!($opt_field_ty), "::", stringify!($opt_name), "`] option.")]
            $(#[$($attrss)*])*
            pub fn $opt_name(mut self, value: $opt_ty) -> Self {
                self.options().$opt_name = Some(value);
                self
            }
        )*
    };
}
use option_setters;

pub(crate) mod private {
    pub trait Sealed {}
}

/// A pending action to execute on the server.  The action can be configured via chained methods and
/// executed via `await` (or `run` if using the sync client).
pub trait Action: private::Sealed + IntoFuture {
    /// If the value is `Some`, call the provided function on `self`.  Convenient for chained
    /// updates with values that need to be set conditionally.  For example:
    /// ```rust
    /// # use mongodb::{Client, error::Result};
    /// # use bson::Document;
    /// use mongodb::action::Action;
    /// async fn list_my_collections(client: &Client, filter: Option<Document>) -> Result<Vec<String>> {
    ///     client.database("my_db")
    ///         .list_collection_names()
    ///         .optional(filter, |a, f| a.filter(f))
    ///         .await
    /// }
    /// ```
    fn optional<Value>(self, value: Option<Value>, f: impl FnOnce(Self, Value) -> Self) -> Self
    where
        Self: Sized,
    {
        match value {
            Some(value) => f(self, value),
            None => self,
        }
    }
}

pub(crate) use action_macro::{action_impl, deeplink};

use crate::Collection;

pub(crate) struct CollRef<'a> {
    inner: Collection<Document>,
    _ref: PhantomData<&'a ()>,
}

impl<'a> CollRef<'a> {
    fn new<T: Send + Sync>(coll: &'a Collection<T>) -> Self {
        Self {
            inner: coll.clone_with_type(),
            _ref: PhantomData,
        }
    }
}

impl<'a> Deref for CollRef<'a> {
    type Target = Collection<Document>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
