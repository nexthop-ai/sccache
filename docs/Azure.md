# Azure

sccache supports Azure Blob Storage as a cache backend. There are two authentication methods: connection strings and Azure credential-based authentication (via `DefaultAzureCredential`).

In both cases, you need an _existing_ Blob Storage container. Set `SCCACHE_AZURE_BLOB_CONTAINER` to the name of the container to use.

You can also define a prefix that will be prepended to the keys of all cache objects created and read within the container, effectively creating a scope. To do that use the `SCCACHE_AZURE_KEY_PREFIX` environment variable. This can be useful when sharing a bucket with another application.

## Connection String Authentication

Set the `SCCACHE_AZURE_CONNECTION_STRING` environment variable to your Azure Storage connection string, and `SCCACHE_AZURE_BLOB_CONTAINER` to the name of the container.

## Credential-Based Authentication (DefaultAzureCredential)

Instead of a connection string, you can authenticate using Azure's `DefaultAzureCredential` chain. This supports managed identities, Azure CLI credentials, environment-based service principals, and more.

Set the following environment variables:
- `SCCACHE_AZURE_BLOB_CONTAINER` with the name of the container
- `SCCACHE_AZURE_BLOB_ENDPOINT` with the storage account blob endpoint, e.g. `https://myaccount.blob.core.windows.net`

The `DefaultAzureCredential` will automatically try the following credential sources in order:
1. Environment variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`)
2. Managed Identity (when running on Azure)
3. Azure CLI (`az login`)

## TOML Configuration

You can also configure Azure in the sccache config file:

```toml
[cache.azure]
container = "my-container"
# Use connection string auth:
connection_string = "DefaultEndpointsProtocol=https;AccountName=..."
# Or use credential-based auth:
# storage_account_endpoint = "https://myaccount.blob.core.windows.net"
key_prefix = "sccache"
```

**Important:** The environment variables are only taken into account when the server starts, i.e. only on the first run.
