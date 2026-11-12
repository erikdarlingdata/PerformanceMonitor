using Amazon.Runtime;
using PerformanceMonitor.Exporter.Upload;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Upload;

/// <summary>The S3 and Azure clients are configured the way the settings say. No network.</summary>
public class UploaderConfigTests
{
    private static Func<string, string?> Env(params (string Name, string Value)[] vars) =>
        name => vars.Where(v => v.Name == name).Select(v => v.Value).FirstOrDefault();

    [Fact]
    public void S3_without_an_endpoint_is_just_the_region_as_before()
    {
        var config = S3Uploader.BuildConfig(new UploadSettings.S3("b", "p", Region: "eu-west-1"));

        Assert.Equal("eu-west-1", config.RegionEndpoint.SystemName);
        Assert.Null(config.ServiceURL);
        Assert.False(config.ForcePathStyle);
        Assert.Equal(RequestChecksumCalculation.WHEN_SUPPORTED, config.RequestChecksumCalculation);
    }

    [Fact]
    public void S3_without_a_region_leaves_it_to_the_AWS_SDK()
    {
        // The SDK's own resolution (environment, profile, instance metadata) is whatever a bare config reports.
        Assert.Equal(new Amazon.S3.AmazonS3Config().RegionEndpoint?.SystemName, S3Uploader.BuildConfig(new UploadSettings.S3("b")).RegionEndpoint?.SystemName);
    }

    [Fact]
    public void S3_with_an_endpoint_sets_service_url_path_style_signing_region_and_checksum_when_required()
    {
        var config = S3Uploader.BuildConfig(new UploadSettings.S3("b", EndpointUrl: "http://127.0.0.1:9000", ForcePathStyle: true));

        Assert.Equal(new Uri("http://127.0.0.1:9000"), new Uri(config.ServiceURL));
        Assert.True(config.ForcePathStyle);
        Assert.Equal("us-east-1", config.AuthenticationRegion);
        Assert.Equal(RequestChecksumCalculation.WHEN_REQUIRED, config.RequestChecksumCalculation);
        Assert.Equal(ResponseChecksumValidation.WHEN_REQUIRED, config.ResponseChecksumValidation);
    }

    [Fact]
    public void S3_with_an_endpoint_signs_for_the_given_region()
    {
        var config = S3Uploader.BuildConfig(new UploadSettings.S3("b", Region: "auto", EndpointUrl: "https://acct.r2.example"));

        Assert.Equal("auto", config.AuthenticationRegion);
    }

    [Fact]
    public void S3_default_credentials_use_the_sdk_chain()
    {
        Assert.Null(S3Uploader.ResolveCredentials(new UploadSettings.S3("b"), Env()));
        Assert.Null(S3Uploader.ResolveCredentials(new UploadSettings.S3("b", Credentials: new S3Credentials.Default()), Env()));
    }

    [Fact]
    public void S3_env_key_credentials_read_the_named_variables()
    {
        var settings = new UploadSettings.S3("b", Credentials: new S3Credentials.EnvKeys("AK", "SK", null));

        var creds = S3Uploader.ResolveCredentials(settings, Env(("AK", "key-id"), ("SK", "secret")))!.GetCredentials();

        Assert.Equal("key-id", creds.AccessKey);
        Assert.Equal("secret", creds.SecretKey);
        Assert.False(creds.UseToken);
    }

    [Fact]
    public void S3_env_key_credentials_include_a_session_token_when_one_is_set()
    {
        var settings = new UploadSettings.S3("b", Credentials: new S3Credentials.EnvKeys("AK", "SK", "ST"));

        var creds = S3Uploader.ResolveCredentials(settings, Env(("AK", "a"), ("SK", "s"), ("ST", "tok")))!.GetCredentials();

        Assert.Equal("tok", creds.Token);
    }

    [Fact]
    public void S3_env_key_credentials_name_the_missing_variable_and_never_a_value()
    {
        var settings = new UploadSettings.S3("b", Credentials: new S3Credentials.EnvKeys("AK", "SK", null));

        var ex = Assert.Throws<InvalidOperationException>(() => S3Uploader.ResolveCredentials(settings, Env(("AK", "key-id"))));

        Assert.Contains("SK", ex.Message);
        Assert.DoesNotContain("key-id", ex.Message);
    }

    [Fact]
    public void S3_a_missing_profile_is_an_error_naming_the_profile()
    {
        var settings = new UploadSettings.S3("b", Credentials: new S3Credentials.Profile("no-such-profile-xyz"));

        var ex = Assert.Throws<InvalidOperationException>(() => S3Uploader.ResolveCredentials(settings, Env()));

        Assert.Contains("no-such-profile-xyz", ex.Message);
    }

    [Fact]
    public void S3_permanent_errors_are_403_and_404_only()
    {
        var uploader = new S3Uploader(new UploadSettings.S3("b", Region: "us-east-1", Credentials: new S3Credentials.EnvKeys("AK", "SK", null)), Env(("AK", "a"), ("SK", "s")));

        Assert.True(uploader.IsPermanent(new Amazon.S3.AmazonS3Exception("x") { StatusCode = System.Net.HttpStatusCode.Forbidden }));
        Assert.True(uploader.IsPermanent(new Amazon.S3.AmazonS3Exception("x") { StatusCode = System.Net.HttpStatusCode.NotFound }));
        Assert.False(uploader.IsPermanent(new Amazon.S3.AmazonS3Exception("x") { StatusCode = System.Net.HttpStatusCode.BadRequest }));
        Assert.False(uploader.IsPermanent(new IOException("x")));
        Assert.Equal("s3://b/k/f", uploader.Describe("k/f"));
    }

    private const string AzuriteLikeConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    [Fact]
    public void Azure_connection_string_env_builds_a_client_for_the_container()
    {
        var settings = new UploadSettings.AzureBlob(null, "example-container", "p", new AzureAuth.ConnectionStringEnv("CS"));

        var client = AzureBlobUploader.BuildContainerClient(settings, Env(("CS", AzuriteLikeConnectionString)));

        Assert.Equal("example-container", client.Name);
        Assert.Equal("devstoreaccount1", client.AccountName);
    }

    [Fact]
    public void Azure_sas_env_uses_the_account_url_container_and_only_the_token()
    {
        var settings = new UploadSettings.AzureBlob("https://acct.blob.core.windows.net", "example-container", "p", new AzureAuth.SasUrlEnv("SAS"));

        var fromUrl = AzureBlobUploader.BuildContainerClient(settings, Env(("SAS", "https://other.blob.core.windows.net/zzz?sv=2024&sig=abc%2Fdef")));
        var fromToken = AzureBlobUploader.BuildContainerClient(settings, Env(("SAS", "?sv=2024&sig=abc%2Fdef")));

        foreach (var client in new[] { fromUrl, fromToken })
        {
            Assert.Equal("acct", client.AccountName);
            Assert.Equal("example-container", client.Name);
            Assert.Equal("acct.blob.core.windows.net", client.Uri.Host);
            Assert.Contains("sv=2024", client.Uri.Query);
            Assert.DoesNotContain("zzz", client.Uri.AbsolutePath);
        }
    }

    [Fact]
    public void Azure_default_credential_builds_a_client_without_reading_any_variable()
    {
        var settings = new UploadSettings.AzureBlob("https://acct.blob.core.windows.net", "example-container", "p", new AzureAuth.DefaultCredential());

        var client = AzureBlobUploader.BuildContainerClient(settings, _ => throw new InvalidOperationException("must not read env"));

        Assert.Equal("acct.blob.core.windows.net", client.Uri.Host);
        Assert.Equal("example-container", client.Name);
    }

    [Fact]
    public void Azure_a_missing_variable_is_an_error_naming_the_variable()
    {
        var settings = new UploadSettings.AzureBlob(null, "example-container", "p", new AzureAuth.ConnectionStringEnv("MY_CS_VAR"));

        var ex = Assert.Throws<InvalidOperationException>(() => AzureBlobUploader.BuildContainerClient(settings, Env()));

        Assert.Contains("MY_CS_VAR", ex.Message);
    }

    [Fact]
    public void Azure_upload_options_overwrite_with_md5_validation_and_four_blocks_in_parallel()
    {
        var options = AzureBlobUploader.BuildUploadOptions();

        Assert.Equal(Azure.Storage.StorageChecksumAlgorithm.MD5, options.TransferValidation.ChecksumAlgorithm);
        Assert.Equal(4, options.TransferOptions.MaximumConcurrency);
        Assert.Null(options.Conditions);   // no conditions: the upload overwrites an existing blob
    }

    [Fact]
    public void Azure_permanent_errors_are_401_403_404_only()
    {
        var uploader = new AzureBlobUploader(
            new UploadSettings.AzureBlob(null, "example-container", "p", new AzureAuth.ConnectionStringEnv("CS")), Env(("CS", AzuriteLikeConnectionString)));

        foreach (var status in new[] { 401, 403, 404 })
        {
            Assert.True(uploader.IsPermanent(new Azure.RequestFailedException(status, "x")));
        }

        Assert.False(uploader.IsPermanent(new Azure.RequestFailedException(500, "x")));
        Assert.False(uploader.IsPermanent(new Azure.RequestFailedException(409, "x")));
        Assert.False(uploader.IsPermanent(new IOException("x")));
    }

    [Fact]
    public void UploaderFactory_builds_one_uploader_per_destination_type()
    {
        using var dir = new TempFolder();
        Assert.IsType<LocalUploader>(UploaderFactory.Create(new UploadSettings.Local(dir.Path)));
        Assert.IsType<S3Uploader>(UploaderFactory.Create(new UploadSettings.S3("b", Region: "us-east-1", Credentials: new S3Credentials.EnvKeys("AK", "SK", null)), Env(("AK", "a"), ("SK", "s"))));
        Assert.IsType<AzureBlobUploader>(UploaderFactory.Create(
            new UploadSettings.AzureBlob(null, "c", "", new AzureAuth.ConnectionStringEnv("CS")), Env(("CS", AzuriteLikeConnectionString))));
    }

    private sealed class TempFolder : IDisposable
    {
        public string Path { get; } = UploadTestSupport.TempDir();

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
