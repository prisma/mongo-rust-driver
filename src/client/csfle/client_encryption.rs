//! Support for explicit encryption.

use crate::{
    bson::Binary,
    client::options::TlsOptions,
    coll::options::CollectionOptions,
    error::{Error, Result},
    options::{ReadConcern, WriteConcern},
    results::DeleteResult,
    Client,
    Collection,
    Cursor,
    Namespace,
};
use bson::{doc, spec::BinarySubtype, RawBinaryRef, RawDocumentBuf};
use mongocrypt::{
    ctx::{Algorithm, Ctx, KmsProvider},
    Crypt,
};
use serde::{Deserialize, Serialize};

use super::{options::KmsProviders, state_machine::CryptExecutor};

/// A handle to the key vault.  Used to create data encryption keys, and to explicitly encrypt and
/// decrypt values when auto-encryption is not an option.
pub struct ClientEncryption {
    crypt: Crypt,
    exec: CryptExecutor,
    key_vault: Collection<RawDocumentBuf>,
}

impl ClientEncryption {
    /// Construct a `ClientEncryptionBuilder` to initialize a new `ClientEncryption`.
    ///
    /// ```no_run
    /// # use bson::doc;
    /// # use mongocrypt::ctx::KmsProvider;
    /// # use mongodb::client_encryption::ClientEncryption;
    /// # use mongodb::error::Result;
    /// # fn func() -> Result<()> {
    /// # let kv_client = todo!();
    /// # let kv_namespace = todo!();
    /// # let local_key = doc! { };
    /// let enc = ClientEncryption::builder()
    ///     .key_vault_client(kv_client)
    ///     .key_vault_namespace(kv_namespace)
    ///     .kms_providers([
    ///         (KmsProvider::Local, doc! { "key": local_key }, None),
    ///         (KmsProvider::Kmip, doc! { "endpoint": "localhost:5698" }, None),
    ///     ])?
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> ClientEncryptionBuilder<BUILDER_UNSET, BUILDER_UNSET, BUILDER_UNSET> {
        ClientEncryptionBuilder {
            key_vault_client: None,
            key_vault_namespace: None,
            kms_providers: None,
        }
    }

    /// Creates a new key document and inserts into the key vault collection.
    /// `CreateDataKeyAction::run` returns a `Binary` (subtype 0x04) with the _id of the created
    /// document as a UUID.
    ///
    /// The returned `CreateDataKeyAction` must be executed via `run`, e.g.
    /// ```no_run
    /// # use mongocrypt::ctx::Algorithm;
    /// # use mongodb::client_encryption::ClientEncryption;
    /// # use mongodb::error::Result;
    /// # async fn func() -> Result<()> {
    /// # let client_encryption: ClientEncryption = todo!();
    /// # let master_key = todo!();
    /// let key = client_encryption
    ///     .create_data_key(master_key)
    ///     .key_alt_names(["altname1".to_string(), "altname2".to_string()])
    ///     .run().await?;
    /// # }
    /// ```
    #[must_use]
    pub fn create_data_key(&self, master_key: MasterKey) -> CreateDataKeyAction {
        CreateDataKeyAction {
            client_enc: self,
            opts: DataKeyOptions {
                master_key,
                key_alt_names: None,
                key_material: None,
            },
        }
    }

    async fn create_data_key_final(
        &self,
        kms_provider: &KmsProvider,
        opts: impl Into<Option<DataKeyOptions>>,
    ) -> Result<Binary> {
        let ctx = self.create_data_key_ctx(kms_provider, opts.into().as_ref())?;
        let data_key = self.exec.run_ctx(ctx, None).await?;
        self.key_vault.insert_one(&data_key, None).await?;
        let bin_ref = data_key
            .get_binary("_id")
            .map_err(|e| Error::internal(format!("invalid data key id: {}", e)))?;
        Ok(bin_ref.to_binary())
    }

    fn create_data_key_ctx(
        &self,
        kms_provider: &KmsProvider,
        opts: Option<&DataKeyOptions>,
    ) -> Result<Ctx> {
        let mut builder = self.crypt.ctx_builder();
        let mut key_doc = doc! { "provider": kms_provider.name() };
        if let Some(opts) = opts {
            if !matches!(opts.master_key, MasterKey::Local) {
                let master_doc = bson::to_document(&opts.master_key)?;
                key_doc.extend(master_doc);
            }
            if let Some(alt_names) = &opts.key_alt_names {
                for name in alt_names {
                    builder = builder.key_alt_name(name)?;
                }
            }
            if let Some(material) = &opts.key_material {
                builder = builder.key_material(material)?;
            }
        }
        builder = builder.key_encryption_key(&key_doc)?;
        Ok(builder.build_datakey()?)
    }

    // pub async fn rewrap_many_data_key(&self, _filter: Document, _opts: impl
    // Into<Option<RewrapManyDataKeyOptions>>) -> Result<RewrapManyDataKeyResult> {
    // todo!("RUST-1441") }

    /// Removes the key document with the given UUID (BSON binary subtype 0x04) from the key vault
    /// collection. Returns the result of the internal deleteOne() operation on the key vault
    /// collection.
    pub async fn delete_key(&self, id: &Binary) -> Result<DeleteResult> {
        self.key_vault.delete_one(doc! { "_id": id }, None).await
    }

    /// Finds a single key document with the given UUID (BSON binary subtype 0x04).
    /// Returns the result of the internal find() operation on the key vault collection.
    pub async fn get_key(&self, id: &Binary) -> Result<Option<RawDocumentBuf>> {
        self.key_vault.find_one(doc! { "_id": id }, None).await
    }

    /// Finds all documents in the key vault collection.
    /// Returns the result of the internal find() operation on the key vault collection.
    pub async fn get_keys(&self) -> Result<Cursor<RawDocumentBuf>> {
        self.key_vault.find(doc! {}, None).await
    }

    /// Adds a keyAltName to the keyAltNames array of the key document in the key vault collection
    /// with the given UUID (BSON binary subtype 0x04). Returns the previous version of the key
    /// document.
    pub async fn add_key_alt_name(
        &self,
        id: &Binary,
        key_alt_name: &str,
    ) -> Result<Option<RawDocumentBuf>> {
        self.key_vault
            .find_one_and_update(
                doc! { "_id": id },
                doc! { "$addToSet": { "keyAltNames": key_alt_name } },
                None,
            )
            .await
    }

    /// Removes a keyAltName from the keyAltNames array of the key document in the key vault
    /// collection with the given UUID (BSON binary subtype 0x04). Returns the previous version
    /// of the key document.
    pub async fn remove_key_alt_name(
        &self,
        id: &Binary,
        key_alt_name: &str,
    ) -> Result<Option<RawDocumentBuf>> {
        let update = doc! {
            "$set": {
                "keyAltNames": {
                    "$cond": [
                        { "$eq": ["$keyAltNames", [key_alt_name]] },
                        "$$REMOVE",
                        {
                            "$filter": {
                                "input": "$keyAltNames",
                                "cond": { "$ne": ["$$this", key_alt_name] },
                            }
                        }
                    ]
                }
            }
        };
        self.key_vault
            .find_one_and_update(doc! { "_id": id }, vec![update], None)
            .await
    }

    /// Returns a key document in the key vault collection with the given keyAltName.
    pub async fn get_key_by_alt_name(
        &self,
        key_alt_name: impl AsRef<str>,
    ) -> Result<Option<RawDocumentBuf>> {
        self.key_vault
            .find_one(doc! { "keyAltNames": key_alt_name.as_ref() }, None)
            .await
    }

    /// Encrypts a BsonValue with a given key and algorithm.
    /// `EncryptAction::run` returns a `Binary` (subtype 6) containing the encrypted value.
    ///
    /// To insert or query with an "Indexed" encrypted payload, use a `Client` configured with
    /// `AutoEncryptionOptions`. `AutoEncryptionOptions.bypass_query_analysis` may be true.
    /// `AutoEncryptionOptions.bypass_auto_encryption` must be false.
    ///
    /// The returned `EncryptAction` must be executed via `run`, e.g.
    /// ```no_run
    /// # use mongocrypt::ctx::Algorithm;
    /// # use mongodb::client_encryption::ClientEncryption;
    /// # use mongodb::error::Result;
    /// # async fn func() -> Result<()> {
    /// # let client_encryption: ClientEncryption = todo!();
    /// # let key = todo!();
    /// let encrypted = client_encryption
    ///     .encrypt(
    ///         "plaintext",
    ///         key,
    ///         Algorithm::AeadAes256CbcHmacSha512Deterministic,
    ///     )
    ///     .contention_factor(10)
    ///     .run().await?;
    /// # }
    /// ```
    #[must_use]
    pub fn encrypt(
        &self,
        value: impl Into<bson::RawBson>,
        key: EncryptKey,
        algorithm: Algorithm,
    ) -> EncryptAction {
        EncryptAction {
            client_enc: self,
            value: value.into(),
            opts: EncryptOptions {
                key,
                algorithm,
                contention_factor: None,
                query_type: None,
            },
        }
    }

    async fn encrypt_final(&self, value: bson::RawBson, opts: EncryptOptions) -> Result<Binary> {
        let ctx = self.encrypt_ctx(value, &opts)?;
        let result = self.exec.run_ctx(ctx, None).await?;
        let bin_ref = result
            .get_binary("v")
            .map_err(|e| Error::internal(format!("invalid encryption result: {}", e)))?;
        Ok(bin_ref.to_binary())
    }

    fn encrypt_ctx(&self, value: bson::RawBson, opts: &EncryptOptions) -> Result<Ctx> {
        let mut builder = self.crypt.ctx_builder();
        match &opts.key {
            EncryptKey::Id(id) => {
                builder = builder.key_id(&id.bytes)?;
            }
            EncryptKey::AltName(name) => {
                builder = builder.key_alt_name(name)?;
            }
        }
        builder = builder.algorithm(opts.algorithm)?;
        if let Some(factor) = opts.contention_factor {
            builder = builder.contention_factor(factor)?;
        }
        if let Some(qtype) = &opts.query_type {
            builder = builder.query_type(qtype)?;
        }
        Ok(builder.build_explicit_encrypt(value)?)
    }

    /// Decrypts an encrypted value (BSON binary of subtype 6).
    /// Returns the original BSON value.
    pub async fn decrypt<'a>(&self, value: RawBinaryRef<'a>) -> Result<bson::RawBson> {
        if value.subtype != BinarySubtype::Encrypted {
            return Err(Error::invalid_argument(format!(
                "Invalid binary subtype for decrypt: expected {:?}, got {:?}",
                BinarySubtype::Encrypted,
                value.subtype
            )));
        }
        let ctx = self
            .crypt
            .ctx_builder()
            .build_explicit_decrypt(value.bytes)?;
        let result = self.exec.run_ctx(ctx, None).await?;
        Ok(result
            .get("v")?
            .ok_or_else(|| Error::internal("invalid decryption result"))?
            .to_raw_bson())
    }
}

/// The `ClientEncryptionBuilder` has this required field unset.
pub const BUILDER_UNSET: bool = false;
/// The `ClientEncryptionBuilder` has this required field set.
pub const BUILDER_SET: bool = true;

/// Builder for convenient construction of a new `ClientEncryption`.
#[derive(Debug)]
pub struct ClientEncryptionBuilder<
    const KV_CLIENT: bool,
    const KV_NAMESPACE: bool,
    const KMS_PROVIDERS: bool,
> {
    key_vault_client: Option<Client>,
    key_vault_namespace: Option<Namespace>,
    kms_providers: Option<KmsProviders>,
}

impl<const KV_NAMESPACE: bool, const KMS_PROVIDERS: bool>
    ClientEncryptionBuilder<BUILDER_UNSET, KV_NAMESPACE, KMS_PROVIDERS>
{
    /// Set the key vault `Client`.
    ///
    /// Typically this is the same as the main client; it can be different in order to route data
    /// key queries to a separate MongoDB cluster, or the same cluster but with a different
    /// credential.
    pub fn key_vault_client(
        self,
        client: Client,
    ) -> ClientEncryptionBuilder<BUILDER_SET, KV_NAMESPACE, KMS_PROVIDERS> {
        ClientEncryptionBuilder {
            key_vault_client: Some(client),
            key_vault_namespace: self.key_vault_namespace,
            kms_providers: self.kms_providers,
        }
    }
}

impl<const KV_CLIENT: bool, const KMS_PROVIDERS: bool>
    ClientEncryptionBuilder<KV_CLIENT, BUILDER_UNSET, KMS_PROVIDERS>
{
    /// Set the key vault `Namespace`, referring to a collection that contains all data keys used
    /// for encryption and decryption.
    pub fn key_vault_namespace(
        self,
        namespace: Namespace,
    ) -> ClientEncryptionBuilder<KV_CLIENT, BUILDER_SET, KMS_PROVIDERS> {
        ClientEncryptionBuilder {
            key_vault_client: self.key_vault_client,
            key_vault_namespace: Some(namespace),
            kms_providers: self.kms_providers,
        }
    }
}

impl<const KV_CLIENT: bool, const KV_NAMESPACE: bool>
    ClientEncryptionBuilder<KV_CLIENT, KV_NAMESPACE, BUILDER_UNSET>
{
    /// Add configuration for multiple KMS providers.
    ///
    /// ```no_run
    /// # use bson::doc;
    /// # use mongocrypt::ctx::KmsProvider;
    /// # use mongodb::client_encryption::ClientEncryption;
    /// # use mongodb::error::Result;
    /// # fn func() -> Result<()> {
    /// # let kv_client = todo!();
    /// # let kv_namespace = todo!();
    /// # let local_key = doc! { };
    /// let enc = ClientEncryption::builder()
    ///     .key_vault_client(kv_client)
    ///     .key_vault_namespace(kv_namespace)
    ///     .kms_providers([
    ///         (KmsProvider::Local, doc! { "key": local_key }, None),
    ///         (KmsProvider::Kmip, doc! { "endpoint": "localhost:5698" }, None),
    ///      ])?
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn kms_providers(
        self,
        providers: impl IntoIterator<Item = (KmsProvider, bson::Document, Option<TlsOptions>)>,
    ) -> Result<ClientEncryptionBuilder<KV_CLIENT, KV_NAMESPACE, BUILDER_SET>> {
        Ok(ClientEncryptionBuilder {
            key_vault_client: self.key_vault_client,
            key_vault_namespace: self.key_vault_namespace,
            kms_providers: Some(KmsProviders::new(providers)?),
        })
    }
}

impl ClientEncryptionBuilder<BUILDER_SET, BUILDER_SET, BUILDER_SET> {
    /// Create a new key vault handle with the given options.
    pub fn build(self) -> Result<ClientEncryption> {
        let key_vault_client = self
            .key_vault_client
            .ok_or_else(|| Error::internal("missing key_vault_client"))?;
        let key_vault_namespace = self
            .key_vault_namespace
            .ok_or_else(|| Error::internal("misisng key_vault_namespace"))?;
        let kms_providers = self
            .kms_providers
            .ok_or_else(|| Error::internal("missing kms_providedrs"))?;
        let crypt = Crypt::builder()
            .kms_providers(&kms_providers.credentials()?)?
            .build()?;
        let exec = CryptExecutor::new_explicit(
            key_vault_client.weak(),
            key_vault_namespace.clone(),
            kms_providers.tls_options().clone(),
        )?;
        let key_vault = key_vault_client
            .database(&key_vault_namespace.db)
            .collection_with_options(
                &key_vault_namespace.coll,
                CollectionOptions::builder()
                    .write_concern(WriteConcern::MAJORITY)
                    .read_concern(ReadConcern::MAJORITY)
                    .build(),
            );
        Ok(ClientEncryption {
            crypt,
            exec,
            key_vault,
        })
    }
}

/// Options for creating a data key.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct DataKeyOptions {
    pub(crate) master_key: MasterKey,
    pub(crate) key_alt_names: Option<Vec<String>>,
    pub(crate) key_material: Option<Vec<u8>>,
}

/// A pending `ClientEncryption::create_data_key` action.
pub struct CreateDataKeyAction<'a> {
    client_enc: &'a ClientEncryption,
    opts: DataKeyOptions,
}

impl<'a> CreateDataKeyAction<'a> {
    /// Execute the pending data key creation.
    pub async fn run(self) -> Result<Binary> {
        self.client_enc
            .create_data_key_final(&self.opts.master_key.provider(), self.opts)
            .await
    }

    /// Set an optional list of alternate names that can be used to reference the key.
    pub fn key_alt_names(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.opts.key_alt_names = Some(names.into_iter().collect());
        self
    }

    /// Set a buffer of 96 bytes to use as custom key material for the data key being
    /// created.  If unset, key material for the new data key is generated from a cryptographically
    /// secure random device.
    pub fn key_material(mut self, material: impl IntoIterator<Item = u8>) -> Self {
        self.opts.key_material = Some(material.into_iter().collect());
        self
    }
}

/// A KMS-specific key used to encrypt data keys.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum MasterKey {
    #[serde(rename_all = "camelCase")]
    Aws {
        region: String,
        /// The Amazon Resource Name (ARN) to the AWS customer master key (CMK).
        key: String,
        /// An alternate host identifier to send KMS requests to. May include port number. Defaults
        /// to "kms.REGION.amazonaws.com"
        endpoint: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Azure {
        /// Host with optional port. Example: "example.vault.azure.net".
        key_vault_endpoint: String,
        key_name: String,
        /// A specific version of the named key, defaults to using the key's primary version.
        key_version: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Gcp {
        project_id: String,
        location: String,
        key_ring: String,
        key_name: String,
        /// A specific version of the named key, defaults to using the key's primary version.
        key_version: Option<String>,
        /// Host with optional port. Defaults to "cloudkms.googleapis.com".
        endpoint: Option<String>,
    },
    /// Master keys are not applicable to `KmsProvider::Local`.
    Local,
    #[serde(rename_all = "camelCase")]
    Kmip {
        /// keyId is the KMIP Unique Identifier to a 96 byte KMIP Secret Data managed object.  If
        /// keyId is omitted, the driver creates a random 96 byte KMIP Secret Data managed object.
        key_id: Option<String>,
        /// Host with optional port.
        endpoint: Option<String>,
    },
}

impl MasterKey {
    /// Returns the `KmsProvider` associated with this key.
    pub fn provider(&self) -> KmsProvider {
        match self {
            MasterKey::Aws { .. } => KmsProvider::Aws,
            MasterKey::Azure { .. } => KmsProvider::Azure,
            MasterKey::Gcp { .. } => KmsProvider::Gcp,
            MasterKey::Kmip { .. } => KmsProvider::Kmip,
            MasterKey::Local => KmsProvider::Local,
        }
    }
}

// #[non_exhaustive]
// pub struct RewrapManyDataKeyOptions {
// pub provider: KmsProvider,
// pub master_key: Option<Document>,
// }
//
//
// #[non_exhaustive]
// pub struct RewrapManyDataKeyResult {
// pub bulk_write_result: Option<BulkWriteResult>,
// }

/// The options for explicit encryption.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct EncryptOptions {
    pub(crate) key: EncryptKey,
    pub(crate) algorithm: Algorithm,
    pub(crate) contention_factor: Option<i64>,
    pub(crate) query_type: Option<String>,
}

/// A pending `ClientEncryption::encrypt` action.
pub struct EncryptAction<'a> {
    client_enc: &'a ClientEncryption,
    value: bson::RawBson,
    opts: EncryptOptions,
}

impl<'a> EncryptAction<'a> {
    /// Execute the encryption.
    pub async fn run(self) -> Result<Binary> {
        self.client_enc.encrypt_final(self.value, self.opts).await
    }

    /// Set the contention factor.
    pub fn contention_factor(mut self, factor: impl Into<Option<i64>>) -> Self {
        self.opts.contention_factor = factor.into();
        self
    }

    /// Set the query type.
    #[allow(clippy::redundant_clone)]
    pub fn query_type(mut self, qtype: impl ToOwned<Owned = String>) -> Self {
        self.opts.query_type = Some(qtype.to_owned());
        self
    }
}

/// An encryption key reference.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EncryptKey {
    /// Find the key by _id value.
    Id(Binary),
    /// Find the key by alternate name.
    AltName(String),
}
