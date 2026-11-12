using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace PerformanceMonitor.Exporter.Upload;

/// <summary>
/// Uploads to S3, or to an S3-compatible store (MinIO, R2) when an endpoint is set. Never logs a credential. A file
/// above 100 MB goes up as a multipart upload.
/// </summary>
public sealed class S3Uploader : IUploader
{
    private const long MultipartThresholdBytes = 100L * 1024 * 1024;

    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly bool _sha256;
    private readonly bool _customEndpoint;

    public S3Uploader(UploadSettings.S3 settings, Func<string, string?>? getEnvironmentVariable = null)
    {
        _bucket = settings.Bucket;
        _sha256 = settings.Checksum == S3Checksum.Sha256;
        _customEndpoint = !string.IsNullOrWhiteSpace(settings.EndpointUrl);
        var credentials = ResolveCredentials(settings, getEnvironmentVariable ?? Environment.GetEnvironmentVariable);
        var config = BuildConfig(settings);
        _client = credentials is null ? new AmazonS3Client(config) : new AmazonS3Client(credentials, config);
    }

    /// <summary>
    /// The client configuration for <paramref name="settings"/>. Without an endpoint this is the region only, as the
    /// exporter has always built it. With an endpoint it sets the service URL, path-style addressing, the signing
    /// region and checksums "when required": SDK v4 sends a checksum trailer by default and some S3-compatible stores
    /// reject it.
    /// </summary>
    public static AmazonS3Config BuildConfig(UploadSettings.S3 settings)
    {
        var region = string.IsNullOrWhiteSpace(settings.Region) ? UploadSettings.DefaultRegion : settings.Region;
        var config = new AmazonS3Config();
        if (string.IsNullOrWhiteSpace(settings.EndpointUrl))
        {
            // No region given: leave it unset so the AWS SDK resolves it (AWS_REGION, profile, instance metadata).
            if (!string.IsNullOrWhiteSpace(settings.Region))
            {
                config.RegionEndpoint = RegionEndpoint.GetBySystemName(settings.Region);
            }

            config.ForcePathStyle = settings.ForcePathStyle;
            return config;
        }

        config.ServiceURL = settings.EndpointUrl;
        config.ForcePathStyle = settings.ForcePathStyle;
        config.AuthenticationRegion = region;
        config.RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED;
        config.ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED;
        return config;
    }

    /// <summary>The explicit credentials for <paramref name="settings"/>, or null to use the default chain.</summary>
    public static AWSCredentials? ResolveCredentials(UploadSettings.S3 settings, Func<string, string?> getEnvironmentVariable)
    {
        switch (settings.Credentials)
        {
            case null:
            case S3Credentials.Default:
                return null;

            case S3Credentials.Profile profile:
                if (!new CredentialProfileStoreChain().TryGetAWSCredentials(profile.Name, out var fromProfile))
                {
                    throw new InvalidOperationException($"AWS profile '{profile.Name}' was not found or has no usable credentials.");
                }

                return fromProfile;

            case S3Credentials.EnvKeys keys:
                var accessKey = Required(keys.AccessKeyIdEnv);
                var secretKey = Required(keys.SecretAccessKeyEnv);
                var token = keys.SessionTokenEnv is null ? null : getEnvironmentVariable(keys.SessionTokenEnv);
                return string.IsNullOrEmpty(token)
                    ? new BasicAWSCredentials(accessKey, secretKey)
                    : new SessionAWSCredentials(accessKey, secretKey, token);

            default:
                throw new InvalidOperationException("Unknown S3 credentials setting.");
        }

        string Required(string name)
        {
            var value = getEnvironmentVariable(name);
            return string.IsNullOrEmpty(value)
                ? throw new InvalidOperationException($"Environment variable {name} (named in the S3 credentials setting) is not set.")
                : value;
        }
    }

    public string Describe(string key) => $"s3://{_bucket}/{key}";

    public async Task UploadFileAsync(string localPath, string key, CancellationToken ct)
    {
        var length = new FileInfo(localPath).Length;

        if (length > MultipartThresholdBytes)
        {
            using var transferUtility = new TransferUtility(_client);
            var request = new TransferUtilityUploadRequest
            {
                BucketName = _bucket,
                Key = key,
                FilePath = localPath,
            };
            // With "when required" checksums (a custom endpoint) a multipart part carries no checksum, and a store that
            // was told to expect SHA-256 rejects it; so the multipart path asks for SHA-256 only on real S3.
            if (_sha256 && !_customEndpoint)
            {
                request.ChecksumAlgorithm = ChecksumAlgorithm.SHA256;
            }

            await transferUtility.UploadAsync(request, ct).ConfigureAwait(false);
            return;
        }

        var putRequest = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            FilePath = localPath,
        };
        if (_sha256)
        {
            putRequest.ChecksumAlgorithm = ChecksumAlgorithm.SHA256;
        }

        var response = await _client.PutObjectAsync(putRequest, ct).ConfigureAwait(false);
        if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
        {
            throw new InvalidOperationException($"S3 PutObject for '{key}' returned {response.HttpStatusCode}.");
        }
    }

    /// <summary>403 (no access) and 404 (no such bucket): a retry cannot fix either.</summary>
    public bool IsPermanent(Exception ex) =>
        ex is AmazonS3Exception s3 && (s3.StatusCode == System.Net.HttpStatusCode.Forbidden || s3.StatusCode == System.Net.HttpStatusCode.NotFound);

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
