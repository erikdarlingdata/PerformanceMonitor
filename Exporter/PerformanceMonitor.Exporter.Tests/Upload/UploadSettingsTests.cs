using System.Text.Json;
using PerformanceMonitor.Exporter.Upload;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Upload;

public class UploadSettingsTests
{
    private static UploadSettings Json(string json) => UploadSettings.FromJson(JsonDocument.Parse(json).RootElement);

    [Fact]
    public void FromUri_s3_carries_the_region()
    {
        var s3 = Assert.IsType<UploadSettings.S3>(UploadSettings.FromUri("s3://b/p", "eu-west-1"));

        Assert.Equal("eu-west-1", s3.Region);
        Assert.Null(s3.EndpointUrl);
        Assert.IsType<S3Credentials.Default>(s3.Credentials ?? new S3Credentials.Default());
        Assert.False(s3.Layout.HiveStoreFolder);
    }

    [Theory]
    [InlineData("/var/landing/landing")]
    [InlineData(@"C:\landing\landing")]
    [InlineData(@"\\fileserver\share\landing")]
    public void FromUri_accepts_bare_local_and_unc_paths(string path)
    {
        var local = Assert.IsType<UploadSettings.Local>(UploadSettings.FromUri(path, null));

        Assert.Equal(path, local.Path);
        Assert.Equal("", local.Prefix);
    }

    [Fact]
    public void FromUri_accepts_a_file_uri()
    {
        var local = Assert.IsType<UploadSettings.Local>(UploadSettings.FromUri("file:///var/landing/my%20landing", null));

        Assert.Equal("/var/landing/my landing", local.Path);
    }

    [Theory]
    [InlineData("https://host/x")]
    [InlineData("gs://bucket/x")]
    [InlineData("")]
    public void FromUri_rejects_other_schemes_and_empty(string uri) =>
        Assert.Throws<ArgumentException>(() => UploadSettings.FromUri(uri, null));

    [Fact]
    public void FromJson_local()
    {
        var local = Assert.IsType<UploadSettings.Local>(Json("""{"type":"local","path":"\\\\fs\\share","prefix":"/landing/","hive_store_folder":true}"""));

        Assert.Equal(@"\\fs\share", local.Path);
        Assert.Equal("landing", local.Prefix);
        Assert.Equal(new UploadLayout("landing", true), local.Layout);
    }

    [Fact]
    public void FromJson_s3_defaults_match_the_command_line_behaviour()
    {
        var s3 = Assert.IsType<UploadSettings.S3>(Json("""{"type":"s3","bucket":"b","prefix":"p"}"""));

        Assert.Equal(new UploadSettings.S3("b", "p"), s3 with { Credentials = null });
        Assert.Equal(S3Checksum.Sha256, s3.Checksum);
        Assert.False(s3.ForcePathStyle);
        Assert.False(s3.Layout.HiveStoreFolder);
    }

    [Fact]
    public void FromJson_s3_full()
    {
        var s3 = Assert.IsType<UploadSettings.S3>(Json("""
            {"type":"s3","bucket":"b","prefix":"p/","region":"auto","endpoint_url":"https://x.example","force_path_style":true,
             "checksum":"none","credentials":{"access_key_id_env":"AK","secret_access_key_env":"SK","session_token_env":"ST"}}
            """));

        Assert.Equal("p", s3.Prefix);
        Assert.Equal("auto", s3.Region);
        Assert.Equal("https://x.example", s3.EndpointUrl);
        Assert.True(s3.ForcePathStyle);
        Assert.Equal(S3Checksum.None, s3.Checksum);
        Assert.Equal(new S3Credentials.EnvKeys("AK", "SK", "ST"), s3.Credentials);
    }

    [Fact]
    public void FromJson_s3_credentials_default_and_profile()
    {
        Assert.IsType<S3Credentials.Default>(Assert.IsType<UploadSettings.S3>(Json("""{"type":"s3","bucket":"b","credentials":"default"}""")).Credentials);
        Assert.Equal(new S3Credentials.Profile("example-container"), Assert.IsType<UploadSettings.S3>(Json("""{"type":"s3","bucket":"b","credentials":{"profile":"example-container"}}""")).Credentials);
    }

    [Fact]
    public void FromJson_azure_blob_for_each_auth_method()
    {
        var d = Assert.IsType<UploadSettings.AzureBlob>(Json("""{"type":"azure_blob","account_url":"https://acct.blob.core.windows.net/","container":"c","prefix":"p","auth":{"method":"default_credential"}}"""));
        Assert.Equal("https://acct.blob.core.windows.net", d.AccountUrl);
        Assert.Equal("azure://acct/c", d.Display);
        Assert.IsType<AzureAuth.DefaultCredential>(d.Auth);

        var sas = Assert.IsType<UploadSettings.AzureBlob>(Json("""{"type":"azure_blob","account_url":"https://acct.blob.core.windows.net","container":"c","auth":{"method":"sas_url_env","name":"SAS"}}"""));
        Assert.Equal(new AzureAuth.SasUrlEnv("SAS"), sas.Auth);
        Assert.Equal("", sas.Prefix);

        var cs = Assert.IsType<UploadSettings.AzureBlob>(Json("""{"type":"azure_blob","container":"c","auth":{"method":"connection_string_env","name":"CS"}}"""));
        Assert.Null(cs.AccountUrl);
    }

    [Theory]
    [InlineData("""{"path":"x"}""")]                                                               // no type
    [InlineData("""{"type":"ftp"}""")]                                                              // unknown type
    [InlineData("""{"type":"local"}""")]                                                            // no path
    [InlineData("""{"type":"local","path":"x","bogus":1}""")]                                       // unknown field
    [InlineData("""{"type":"s3"}""")]                                                               // no bucket
    [InlineData("""{"type":"s3","bucket":"b","checksum":"md5"}""")]                                 // bad checksum
    [InlineData("""{"type":"s3","bucket":"b","force_path_style":"yes"}""")]                         // wrong type
    [InlineData("""{"type":"s3","bucket":"b","credentials":{"access_key_id_env":"A"}}""")]          // missing secret
    [InlineData("""{"type":"s3","bucket":"b","credentials":{"profile":"p","extra":1}}""")]          // unknown field
    [InlineData("""{"type":"azure_blob","container":"c","auth":{"method":"default_credential"}}""")] // no account_url
    [InlineData("""{"type":"azure_blob","account_url":"https://a","container":"c"}""")]             // no auth
    [InlineData("""{"type":"azure_blob","account_url":"https://a","container":"c","auth":{"method":"password"}}""")]
    [InlineData("""{"type":"azure_blob","account_url":"https://a","container":"c","auth":{"method":"sas_url_env"}}""")] // no name
    public void FromJson_rejects_bad_settings(string json) =>
        Assert.Throws<ArgumentException>(() => Json(json));

    [Fact]
    public void FromJson_rejects_a_non_object() =>
        Assert.Throws<ArgumentException>(() => UploadSettings.FromJson(JsonDocument.Parse("\"s3://b\"").RootElement));
}
