using Amazon.S3;
using Amazon.S3.Model;
using Azure.Storage.Blobs;
using PerformanceMonitor.Exporter.Upload;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Upload.UploadTestSupport;

namespace PerformanceMonitor.Exporter.Tests.Upload;

/// <summary>
/// Runs against a real S3-compatible store and a real Azure Blob emulator, only when the environment says where they are:
/// <c>DARLING_EXPORT_TEST_S3_ENDPOINT</c> (e.g. http://127.0.0.1:9000; keys from <c>DARLING_EXPORT_TEST_S3_ACCESS_KEY</c> /
/// <c>DARLING_EXPORT_TEST_S3_SECRET_KEY</c>, default minioadmin) and <c>DARLING_EXPORT_TEST_AZURE_CS</c> (an Azurite connection
/// string). Otherwise every test here is skipped, so CI needs no containers.
/// </summary>
public class LiveS3UploaderTests : IDisposable
{
    private const string EndpointVar = "DARLING_EXPORT_TEST_S3_ENDPOINT";
    private readonly string _outDir = TempDir();
    private readonly string _bucket = "darling-export-" + Guid.NewGuid().ToString("N")[..16];

    public void Dispose() => Directory.Delete(_outDir, recursive: true);

    private static string Endpoint => Environment.GetEnvironmentVariable(EndpointVar)!;

    private static string AccessKey => Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_S3_ACCESS_KEY") ?? "minioadmin";

    private static string SecretKey => Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_S3_SECRET_KEY") ?? "minioadmin";

    private static Func<string, string?> Env(string key, string secret) => name => name switch
    {
        "T_AK" => key,
        "T_SK" => secret,
        _ => null,
    };

    private UploadSettings.S3 Settings(string bucket, S3Checksum checksum = S3Checksum.Sha256, string prefix = "exports") =>
        new(bucket, prefix, "us-east-1", Endpoint, ForcePathStyle: true, Checksum: checksum, Credentials: new S3Credentials.EnvKeys("T_AK", "T_SK", null));

    private async Task<AmazonS3Client> AdminClient(bool createBucket = true)
    {
        var client = new AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials(AccessKey, SecretKey), S3Uploader.BuildConfig(Settings(_bucket)));
        if (createBucket)
        {
            await client.PutBucketAsync(_bucket);
        }

        return client;
    }

    private static async Task<string> ReadAsync(AmazonS3Client client, string bucket, string key)
    {
        using var response = await client.GetObjectAsync(bucket, key);
        using var reader = new StreamReader(response.ResponseStream);
        return await reader.ReadToEndAsync();
    }

    [Theory]
    [InlineData(S3Checksum.Sha256)]
    [InlineData(S3Checksum.None)]
    public async Task A_full_run_lands_every_file_under_the_same_keys_as_the_s3_flag_gives(S3Checksum checksum)
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EndpointVar) is not null, EndpointVar + " is not set");
        using var admin = await AdminClient();
        var (manifest, manifestPath) = WriteRun(_outDir, "store_a", "wait_stats", "plan_shape");
        await using var uploader = UploaderFactory.Create(Settings(_bucket, checksum), Env(AccessKey, SecretKey));
        var log = new StringWriter();

        await RunUpload.UploadRunAsync(uploader, new UploadLayout("exports"), Options(_outDir), manifest, manifestPath, log, CancellationToken.None);

        foreach (var table in manifest.Tables)
        {
            Assert.Equal("parquet:" + table.Table, await ReadAsync(admin, _bucket, "exports/store_a/" + table.OutputFile));
        }

        Assert.Equal("{\"manifest\":true}", await ReadAsync(admin, _bucket, "exports/store_a/_manifests/store_a__run.json"));
        Assert.Contains($"-> s3://{_bucket}/exports/store_a/_manifests/store_a__run.json", log.ToString());
    }

    [Fact]
    public async Task A_re_export_of_the_same_window_overwrites_the_object()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EndpointVar) is not null, EndpointVar + " is not set");
        using var admin = await AdminClient();
        var source = Path.Combine(_outDir, "f.bin");
        await using var uploader = UploaderFactory.Create(Settings(_bucket), Env(AccessKey, SecretKey));

        File.WriteAllText(source, "first version");
        await uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None);
        File.WriteAllText(source, "second");
        await uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None);

        Assert.Equal("second", await ReadAsync(admin, _bucket, "k/f.bin"));
    }

    [Fact]
    public async Task A_file_above_100_MB_goes_up_as_a_multipart_upload_intact()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EndpointVar) is not null, EndpointVar + " is not set");
        using var admin = await AdminClient();
        var source = Path.Combine(_outDir, "big.bin");
        var bytes = new byte[101 * 1024 * 1024];
        new Random(7).NextBytes(bytes);
        await File.WriteAllBytesAsync(source, bytes);
        await using var uploader = UploaderFactory.Create(Settings(_bucket), Env(AccessKey, SecretKey));

        await uploader.UploadFileAsync(source, "k/big.bin", CancellationToken.None);

        var meta = await admin.GetObjectMetadataAsync(_bucket, "k/big.bin");
        Assert.Equal(bytes.Length, meta.ContentLength);
        // A multipart upload's ETag ends in -<parts>; a single PutObject's does not.
        Assert.Matches("-\\d+\"?$", meta.ETag);
        using var response = await admin.GetObjectAsync(_bucket, "k/big.bin");
        using var downloaded = new MemoryStream();
        await response.ResponseStream.CopyToAsync(downloaded);
        Assert.True(bytes.AsSpan().SequenceEqual(downloaded.ToArray()));
    }

    [Fact]
    public async Task A_missing_bucket_is_a_permanent_404()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EndpointVar) is not null, EndpointVar + " is not set");
        var source = Path.Combine(_outDir, "f.bin");
        File.WriteAllText(source, "x");
        await using var uploader = UploaderFactory.Create(Settings("darling-export-no-such-bucket-" + Guid.NewGuid().ToString("N")[..8]), Env(AccessKey, SecretKey));

        var ex = await Assert.ThrowsAsync<AmazonS3Exception>(() => uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None));

        Assert.Equal(System.Net.HttpStatusCode.NotFound, ex.StatusCode);
        Assert.True(uploader.IsPermanent(ex));
    }

    [Fact]
    public async Task Wrong_credentials_are_a_permanent_403()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EndpointVar) is not null, EndpointVar + " is not set");
        using var admin = await AdminClient();
        var source = Path.Combine(_outDir, "f.bin");
        File.WriteAllText(source, "x");
        await using var uploader = UploaderFactory.Create(Settings(_bucket), Env(AccessKey, "definitely-the-wrong-secret"));

        var ex = await Assert.ThrowsAsync<AmazonS3Exception>(() => uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None));

        Assert.Equal(System.Net.HttpStatusCode.Forbidden, ex.StatusCode);
        Assert.True(uploader.IsPermanent(ex));
    }
}

public class LiveAzureBlobUploaderTests : IDisposable
{
    private const string ConnectionStringVar = "DARLING_EXPORT_TEST_AZURE_CS";
    private readonly string _outDir = TempDir();
    private readonly string _container = "darlingexport" + Guid.NewGuid().ToString("N")[..16];

    public void Dispose() => Directory.Delete(_outDir, recursive: true);

    private static string ConnectionString => Environment.GetEnvironmentVariable(ConnectionStringVar)!;

    private static Func<string, string?> Env(string connectionString) => name => name == "T_CS" ? connectionString : null;

    private UploadSettings.AzureBlob Settings(string container) =>
        new(null, container, "exports", new AzureAuth.ConnectionStringEnv("T_CS"));

    private async Task<BlobContainerClient> AdminContainer()
    {
        var client = new BlobContainerClient(ConnectionString, _container);
        await client.CreateAsync();
        return client;
    }

    [Fact]
    public async Task A_full_run_lands_every_blob_under_the_same_keys_as_s3_would_use()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(ConnectionStringVar) is not null, ConnectionStringVar + " is not set");
        var admin = await AdminContainer();
        var (manifest, manifestPath) = WriteRun(_outDir, "store_a", "wait_stats", "plan_shape");
        var settings = Settings(_container);
        await using var uploader = UploaderFactory.Create(settings, Env(ConnectionString));
        var log = new StringWriter();

        await RunUpload.UploadRunAsync(uploader, settings.Layout, Options(_outDir), manifest, manifestPath, log, CancellationToken.None);

        foreach (var table in manifest.Tables)
        {
            var blob = await admin.GetBlobClient("exports/store_a/" + table.OutputFile).DownloadContentAsync();
            Assert.Equal("parquet:" + table.Table, blob.Value.Content.ToString());
        }

        var manifestBlob = await admin.GetBlobClient("exports/store_a/_manifests/store_a__run.json").DownloadContentAsync();
        Assert.Equal("{\"manifest\":true}", manifestBlob.Value.Content.ToString());
        Assert.Contains($"azure://devstoreaccount1/{_container}/exports/store_a/_manifests/store_a__run.json", log.ToString());
    }

    [Fact]
    public async Task A_second_upload_to_the_same_key_overwrites_the_blob()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(ConnectionStringVar) is not null, ConnectionStringVar + " is not set");
        var admin = await AdminContainer();
        var source = Path.Combine(_outDir, "f.bin");
        await using var uploader = UploaderFactory.Create(Settings(_container), Env(ConnectionString));

        File.WriteAllText(source, "first version");
        await uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None);
        File.WriteAllText(source, "second");
        await uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None);

        var blob = await admin.GetBlobClient("k/f.bin").DownloadContentAsync();
        Assert.Equal("second", blob.Value.Content.ToString());
    }

    [Fact]
    public async Task A_file_larger_than_one_block_uploads_intact_with_md5_validation()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(ConnectionStringVar) is not null, ConnectionStringVar + " is not set");
        var admin = await AdminContainer();
        var source = Path.Combine(_outDir, "big.bin");
        var bytes = new byte[9 * 1024 * 1024];
        new Random(11).NextBytes(bytes);
        await File.WriteAllBytesAsync(source, bytes);
        await using var uploader = UploaderFactory.Create(Settings(_container), Env(ConnectionString));

        await uploader.UploadFileAsync(source, "k/big.bin", CancellationToken.None);

        var blob = await admin.GetBlobClient("k/big.bin").DownloadContentAsync();
        Assert.True(bytes.AsSpan().SequenceEqual(blob.Value.Content.ToArray()));
    }

    [Fact]
    public async Task A_container_SAS_from_an_environment_variable_uploads_with_no_account_key()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(ConnectionStringVar) is not null, ConnectionStringVar + " is not set");
        var admin = await AdminContainer();
        var sas = admin.GenerateSasUri(Azure.Storage.Sas.BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(10));
        var accountUrl = new UriBuilder(sas) { Path = "", Query = "" }.Uri.ToString().TrimEnd('/') + "/" + admin.AccountName;
        var settings = new UploadSettings.AzureBlob(accountUrl, _container, "p", new AzureAuth.SasUrlEnv("T_SAS"));
        var source = Path.Combine(_outDir, "f.bin");
        File.WriteAllText(source, "via sas");
        await using var uploader = UploaderFactory.Create(settings, name => name == "T_SAS" ? sas.ToString() : null);

        await uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None);

        var blob = await admin.GetBlobClient("k/f.bin").DownloadContentAsync();
        Assert.Equal("via sas", blob.Value.Content.ToString());
    }

    [Fact]
    public async Task A_missing_container_is_a_permanent_404()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(ConnectionStringVar) is not null, ConnectionStringVar + " is not set");
        var source = Path.Combine(_outDir, "f.bin");
        File.WriteAllText(source, "x");
        await using var uploader = UploaderFactory.Create(Settings("nosuchcontainer" + Guid.NewGuid().ToString("N")[..8]), Env(ConnectionString));

        var ex = await Assert.ThrowsAsync<Azure.RequestFailedException>(() => uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None));

        Assert.Equal(404, ex.Status);
        Assert.True(uploader.IsPermanent(ex));
    }

    [Fact]
    public async Task A_wrong_account_key_is_a_permanent_403()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(ConnectionStringVar) is not null, ConnectionStringVar + " is not set");
        await AdminContainer();
        var source = Path.Combine(_outDir, "f.bin");
        File.WriteAllText(source, "x");
        // Same account and endpoint, a key that is the right shape but wrong.
        var wrongKey = System.Text.RegularExpressions.Regex.Replace(ConnectionString, "AccountKey=[^;]+", "AccountKey=" + Convert.ToBase64String(new byte[64]));
        await using var uploader = UploaderFactory.Create(Settings(_container), Env(wrongKey));

        var ex = await Assert.ThrowsAsync<Azure.RequestFailedException>(() => uploader.UploadFileAsync(source, "k/f.bin", CancellationToken.None));

        Assert.True(ex.Status is 401 or 403, $"status {ex.Status}");
        Assert.True(uploader.IsPermanent(ex));
    }
}
