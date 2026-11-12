using Azure;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace PerformanceMonitor.Exporter.Upload;

/// <summary>
/// Uploads to an Azure Blob container. Each upload overwrites the blob and is checked with an MD5 transfer hash. A
/// block-blob commit is atomic, so a reader never sees a half-written blob. Never logs a credential.
/// </summary>
public sealed class AzureBlobUploader : IUploader
{
    private readonly BlobContainerClient _container;

    public AzureBlobUploader(UploadSettings.AzureBlob settings, Func<string, string?>? getEnvironmentVariable = null)
    {
        _container = BuildContainerClient(settings, getEnvironmentVariable ?? Environment.GetEnvironmentVariable);
    }

    /// <summary>Builds the container client for <paramref name="settings"/>, reading any named environment variable.</summary>
    public static BlobContainerClient BuildContainerClient(UploadSettings.AzureBlob settings, Func<string, string?> getEnvironmentVariable)
    {
        string Required(string name)
        {
            var value = getEnvironmentVariable(name);
            return string.IsNullOrWhiteSpace(value)
                ? throw new InvalidOperationException($"Environment variable {name} (named in the Azure auth setting) is not set.")
                : value;
        }

        switch (settings.Auth)
        {
            case AzureAuth.ConnectionStringEnv cs:
                return new BlobContainerClient(Required(cs.Name), settings.Container);

            case AzureAuth.SasUrlEnv sas:
                var sasValue = Required(sas.Name);
                var question = sasValue.IndexOf('?');
                var token = question >= 0 ? sasValue[(question + 1)..] : sasValue.TrimStart('?');
                var builder = new UriBuilder(ContainerUri(settings)) { Query = token };
                return new BlobContainerClient(builder.Uri);

            case AzureAuth.DefaultCredential:
                return new BlobContainerClient(ContainerUri(settings), new DefaultAzureCredential());

            default:
                throw new InvalidOperationException("Unknown Azure auth setting.");
        }
    }

    private static Uri ContainerUri(UploadSettings.AzureBlob settings) =>
        new($"{settings.AccountUrl}/{Uri.EscapeDataString(settings.Container)}");

    /// <summary>The options every upload uses: overwrite (no conditions), 4 parallel blocks, MD5 transfer validation.</summary>
    public static BlobUploadOptions BuildUploadOptions() => new()
    {
        TransferOptions = new StorageTransferOptions { MaximumConcurrency = 4 },
        TransferValidation = new UploadTransferValidationOptions { ChecksumAlgorithm = StorageChecksumAlgorithm.MD5 },
    };

    public string Describe(string key) => $"azure://{_container.AccountName}/{_container.Name}/{key}";

    public async Task UploadFileAsync(string localPath, string key, CancellationToken ct)
    {
        var blob = _container.GetBlobClient(key);
        await blob.UploadAsync(localPath, BuildUploadOptions(), ct).ConfigureAwait(false);
    }

    /// <summary>401, 403 and 404 (bad credentials, no access, no such container): a retry cannot fix any of them.</summary>
    public bool IsPermanent(Exception ex) => ex is RequestFailedException { Status: 401 or 403 or 404 };

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
