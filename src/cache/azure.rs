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
        let builder = Azblob::from_connection_string(connection_string)?
            .container(container)
            .root(key_prefix);

        let op = Operator::new(builder)?
            .layer(HttpClientLayer::new(set_user_agent()))
            .layer(LoggingLayer::default())
            .finish();
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
        if std::env::var("AZURE_CLIENT_ID").is_ok()
            && std::env::var("AZURE_TENANT_ID").is_ok()
            && std::env::var("AZURE_CLIENT_SECRET").is_ok()
        {
            if let Ok(cred) = azure_identity::ClientSecretCredential::new(
                &std::env::var("AZURE_TENANT_ID").unwrap(),
                std::env::var("AZURE_CLIENT_ID").unwrap(),
                azure_core::credentials::Secret::new(
                    std::env::var("AZURE_CLIENT_SECRET").unwrap(),
                ),
                None,
            ) {
                sources.push(cred);
            }
        }

        // Try managed identity
        if let Ok(cred) = azure_identity::ManagedIdentityCredential::new(None) {
            sources.push(cred);
        }

        // Try developer tools (Azure CLI + azd)
        if let Ok(cred) = azure_identity::DeveloperToolsCredential::new(None) {
            sources.push(cred);
        }

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
        let mut last_error = None;
        for source in &self.sources {
            match source.get_token(scopes, options.clone()).await {
                Ok(token) => return Ok(token),
                Err(e) => {
                    debug!("Credential source failed: {:?}", e);
                    last_error = Some(e);
                }
            }
        }
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
        let credential = ChainedCredential::new()?;
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
        let client = self.blob_client(&blob_name)?;
        match client.download(None).await {
            Ok(response) => {
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
                    Ok(super::cache::Cache::Miss)
                } else {
                    warn!("Azure blob get unexpected error: {:?}", e);
                    Ok(super::cache::Cache::Miss)
                }
            }
        }
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        let start = std::time::Instant::now();
        let blob_name = self.blob_path(key);
        let client = self.blob_client(&blob_name)?;
        let data = entry.finish()?;
        let len = data.len() as u64;
        client
            .upload(RequestContent::from(data), true, len, None)
            .await
            .map_err(|e| anyhow!("Azure blob upload failed: {e}"))?;
        Ok(start.elapsed())
    }

    async fn check(&self) -> Result<CacheMode> {
        let check_blob = ".sccache_check";
        let blob_name = if self.key_prefix.is_empty() {
            check_blob.to_string()
        } else {
            format!("{}/{}", self.key_prefix, check_blob)
        };
        let client = self.blob_client(&blob_name)?;

        // Check read capability
        match client.get_properties(None).await {
            Ok(_) => {}
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    // Not found is ok, means we can read
                } else {
                    bail!("Azure credential cache storage failed to read: {:?}", e);
                }
            }
        }

        // Check write capability
        let data = b"Hello, World!".to_vec();
        let len = data.len() as u64;
        let can_write = match client
            .upload(RequestContent::from(data), true, len, None)
            .await
        {
            Ok(_) => true,
            Err(e) => {
                eprintln!("Azure credential cache write check failed: {e:?}");
                false
            }
        };

        let mode = if can_write {
            CacheMode::ReadWrite
        } else {
            CacheMode::ReadOnly
        };
        debug!("Azure credential cache check result: {mode:?}");
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
