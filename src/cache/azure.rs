// Copyright 2018 Benjamin Bader
// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use opendal::Operator;

use opendal::layers::{HttpClientLayer, LoggingLayer};
use opendal::services::Azblob;

use crate::errors::*;

use super::http_client::set_user_agent;

/// Cache backend that accesses Azure Blob Storage via a connection string, using
/// the OpenDAL abstraction. This is the simpler of the two Azure backends—it
/// doesn't need any credential chain because the connection string already
/// carries the account key or SAS token.
pub struct AzureBlobCache;

impl AzureBlobCache {
    /// Construct an OpenDAL `Operator` wired to the given Azure Blob container.
    /// The returned operator is used by the generic storage layer to get/put
    /// cached compiler outputs.
    pub fn build(connection_string: &str, container: &str, key_prefix: &str) -> Result<Operator> {
        debug!("azure blob cache build: container={container:?}, key_prefix={key_prefix:?}");
        let builder = Azblob::from_connection_string(connection_string)?
            .container(container)
            .root(key_prefix);

        let op = Operator::new(builder)?
            .layer(HttpClientLayer::new(set_user_agent()))
            .layer(LoggingLayer::default())
            .finish();
        debug!("azure blob cache build: OpenDAL operator ready");
        Ok(op)
    }
}

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use azure_core::http::{RequestContent, StatusCode};
use azure_storage_blob::BlobClient;

use super::cache::{CacheMode, CacheRead, CacheWrite, Storage, normalize_key};

/// A chained credential that tries multiple Azure credential sources in order,
/// similar to Azure's DefaultAzureCredential pattern. Sources are attempted in
/// priority order: client-secret env vars, managed identity, then developer
/// tools (Azure CLI / azd). Used by `AzureBlobCredentialCache` to authenticate
/// without a connection string.
#[derive(Debug)]
struct ChainedCredential {
    sources: Vec<Arc<dyn TokenCredential>>,
}

impl ChainedCredential {
    fn new() -> Result<Arc<Self>> {
        let mut sources: Vec<Arc<dyn TokenCredential>> = Vec::new();

        // Try environment-based credentials (ClientSecretCredential)
        let has_client_id = std::env::var("AZURE_CLIENT_ID").is_ok();
        let has_tenant_id = std::env::var("AZURE_TENANT_ID").is_ok();
        let has_client_secret = std::env::var("AZURE_CLIENT_SECRET").is_ok();
        debug!(
            "azure credentials: AZURE_CLIENT_ID={}, AZURE_TENANT_ID={}, AZURE_CLIENT_SECRET={}",
            if has_client_id { "<set>" } else { "<not set>" },
            if has_tenant_id { "<set>" } else { "<not set>" },
            if has_client_secret {
                "<set>"
            } else {
                "<not set>"
            },
        );
        if has_client_id && has_tenant_id && has_client_secret {
            debug!("azure credentials: attempting ClientSecretCredential");
            match azure_identity::ClientSecretCredential::new(
                &std::env::var("AZURE_TENANT_ID").unwrap(),
                std::env::var("AZURE_CLIENT_ID").unwrap(),
                azure_core::credentials::Secret::new(std::env::var("AZURE_CLIENT_SECRET").unwrap()),
                None,
            ) {
                Ok(cred) => {
                    debug!("azure credentials: ClientSecretCredential added to chain");
                    sources.push(cred);
                }
                Err(e) => {
                    debug!("azure credentials: ClientSecretCredential failed to construct: {e:?}");
                }
            }
        } else {
            debug!("azure credentials: skipping ClientSecretCredential (env vars incomplete)");
        }

        // Try managed identity
        let mi_options = if has_client_id {
            debug!("azure credentials: attempting ManagedIdentityCredential with client_id");
            Some(azure_identity::ManagedIdentityCredentialOptions {
                user_assigned_id: Some(azure_identity::UserAssignedId::ClientId(
                    std::env::var("AZURE_CLIENT_ID").unwrap(),
                )),
                ..Default::default()
            })
        } else {
            debug!("azure credentials: attempting ManagedIdentityCredential (system-assigned)");
            None
        };
        match azure_identity::ManagedIdentityCredential::new(mi_options) {
            Ok(cred) => {
                debug!("azure credentials: ManagedIdentityCredential added to chain");
                sources.push(cred);
            }
            Err(e) => {
                debug!("azure credentials: ManagedIdentityCredential unavailable: {e:?}");
            }
        }

        // Try developer tools (Azure CLI + azd)
        debug!("azure credentials: attempting DeveloperToolsCredential");
        match azure_identity::DeveloperToolsCredential::new(None) {
            Ok(cred) => {
                debug!("azure credentials: DeveloperToolsCredential added to chain");
                sources.push(cred);
            }
            Err(e) => {
                debug!("azure credentials: DeveloperToolsCredential unavailable: {e:?}");
            }
        }

        debug!("azure credentials: chain has {} source(s)", sources.len());
        if sources.is_empty() {
            bail!("No Azure credential sources available");
        }

        Ok(Arc::new(Self { sources }))
    }
}

#[async_trait]
impl TokenCredential for ChainedCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<azure_core::credentials::TokenRequestOptions<'_>>,
    ) -> azure_core::Result<azure_core::credentials::AccessToken> {
        debug!("azure get_token: requesting token for scopes={scopes:?}");
        let mut last_error = None;
        for (i, source) in self.sources.iter().enumerate() {
            debug!("azure get_token: trying credential source [{i}]");
            match source.get_token(scopes, options.clone()).await {
                Ok(token) => {
                    debug!("azure get_token: credential source [{i}] succeeded");
                    return Ok(token);
                }
                Err(e) => {
                    debug!("azure get_token: credential source [{i}] failed: {e:?}");
                    last_error = Some(e);
                }
            }
        }
        debug!("azure get_token: all credential sources exhausted");
        Err(last_error.unwrap_or_else(|| {
            azure_core::Error::with_message(
                azure_core::error::ErrorKind::Credential,
                "No credential source succeeded",
            )
        }))
    }
}

/// Cache backend that accesses Azure Blob Storage using token-based
/// credentials (Entra ID / managed identity) instead of a connection string.
/// Implements the `Storage` trait directly against the Azure SDK, bypassing
/// OpenDAL, so it can use the `ChainedCredential` for authentication.
pub struct AzureBlobCredentialCache {
    endpoint: String,
    container: String,
    key_prefix: String,
    credential: Arc<dyn TokenCredential>,
}

impl AzureBlobCredentialCache {
    /// Create a new credential-based cache, initialising the `ChainedCredential`
    /// that will be shared across all blob operations for this session.
    pub fn build(endpoint: &str, container: &str, key_prefix: &str) -> Result<Self> {
        debug!(
            "azure build: endpoint={endpoint:?}, container={container:?}, key_prefix={key_prefix:?}"
        );
        let credential = ChainedCredential::new()?;
        debug!("azure build: credential chain ready");
        Ok(Self {
            endpoint: endpoint.to_owned(),
            container: container.to_owned(),
            key_prefix: key_prefix.to_owned(),
            credential,
        })
    }

    /// Build a one-shot `BlobClient` for a specific blob, sharing the
    /// credential across calls.
    fn blob_client(&self, blob_name: &str) -> Result<BlobClient> {
        BlobClient::new(
            &self.endpoint,
            &self.container,
            blob_name,
            Some(self.credential.clone()),
            None,
        )
        .map_err(|e| anyhow!("Failed to create BlobClient: {e}"))
    }

    /// Map a cache key to its full blob path, prepending the key prefix if set.
    fn blob_path(&self, key: &str) -> String {
        let normalized = normalize_key(key);
        if self.key_prefix.is_empty() {
            normalized
        } else {
            format!("{}/{}", self.key_prefix, normalized)
        }
    }
}

/// `Storage` implementation that maps sccache get/put/check operations to
/// Azure Blob download/upload calls. `check` probes both read and write
/// access so the cache can fall back to read-only when permissions are limited.
#[async_trait]
impl Storage for AzureBlobCredentialCache {
    async fn get(&self, key: &str) -> Result<super::cache::Cache> {
        let blob_name = self.blob_path(key);
        debug!("azure get: key={key:?} -> blob={blob_name:?}");
        let client = self.blob_client(&blob_name)?;
        match client.download(None).await {
            Ok(response) => {
                debug!("azure get: cache HIT for {blob_name:?}");
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| anyhow!("Failed to read blob body: {e}"))?;
                let hit = CacheRead::from(std::io::Cursor::new(body))?;
                Ok(super::cache::Cache::Hit(hit))
            }
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    debug!("azure get: cache MISS for {blob_name:?} (404)");
                    Ok(super::cache::Cache::Miss)
                } else {
                    warn!("azure get: unexpected error for {blob_name:?}: {e:?}");
                    Ok(super::cache::Cache::Miss)
                }
            }
        }
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        let start = std::time::Instant::now();
        let blob_name = self.blob_path(key);
        debug!("azure put: key={key:?} -> blob={blob_name:?}");
        let client = self.blob_client(&blob_name)?;
        let data = entry.finish()?;
        let len = data.len() as u64;
        debug!("azure put: uploading {len} bytes to {blob_name:?}");
        client
            .upload(RequestContent::from(data), true, len, None)
            .await
            .map_err(|e| anyhow!("Azure blob upload failed: {e}"))?;
        let elapsed = start.elapsed();
        debug!("azure put: upload complete in {elapsed:?}");
        Ok(elapsed)
    }

    async fn check(&self) -> Result<CacheMode> {
        let check_blob = ".sccache_check";
        let blob_name = if self.key_prefix.is_empty() {
            check_blob.to_string()
        } else {
            format!("{}/{}", self.key_prefix, check_blob)
        };
        debug!("azure check: probing read/write access via blob {blob_name:?}");
        let client = self.blob_client(&blob_name)?;

        // Check read capability
        debug!("azure check: testing read access (get_properties)");
        match client.get_properties(None).await {
            Ok(_) => {
                debug!("azure check: read access confirmed (blob exists)");
            }
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    debug!(
                        "azure check: read access confirmed (404 = container readable, blob absent)"
                    );
                } else {
                    debug!("azure check: read access FAILED: {e:?}");
                    bail!("Azure credential cache storage failed to read: {:?}", e);
                }
            }
        }

        // Check write capability
        debug!("azure check: testing write access (upload probe)");
        let data = b"Hello, World!".to_vec();
        let len = data.len() as u64;
        let can_write = match client
            .upload(RequestContent::from(data), true, len, None)
            .await
        {
            Ok(_) => {
                debug!("azure check: write access confirmed");
                true
            }
            Err(e) => {
                debug!("azure check: write access FAILED: {e:?}");
                false
            }
        };

        let mode = if can_write {
            CacheMode::ReadWrite
        } else {
            CacheMode::ReadOnly
        };
        debug!("azure check: final mode={mode:?}");
        Ok(mode)
    }

    fn location(&self) -> String {
        format!(
            "Azure, endpoint: {}, container: {}, key_prefix: {}",
            self.endpoint, self.container, self.key_prefix
        )
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}
