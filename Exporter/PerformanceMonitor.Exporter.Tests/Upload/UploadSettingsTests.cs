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
        var d = Assert.IsType<UploadSettings.AzureBlob>(Json("""{"type":"azure_blob","account_url":"https://acct.blob.core.windows.net/","container":"exports","prefix":"p","auth":{"method":"default_credential"} }"""));
        Assert.Equal("https://acct.blob.core.windows.net", d.AccountUrl);
        Assert.Equal("azure://acct/exports", d.Display);
        Assert.IsType<AzureAuth.DefaultCredential>(d.Auth);

        var sas = Assert.IsType<UploadSettings.AzureBlob>(Json("""{"type":"azure_blob","account_url":"https://acct.blob.core.windows.net","container":"exports","auth":{"method":"sas_url_env","name":"SAS"}}"""));
        Assert.Equal(new AzureAuth.SasUrlEnv("SAS"), sas.Auth);
        Assert.Equal("", sas.Prefix);

        var cs = Assert.IsType<UploadSettings.AzureBlob>(Json("""{"type":"azure_blob","container":"exports","auth":{"method":"connection_string_env","name":"CS"}}"""));
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
    [InlineData("""{"type":"azure_blob","container":"exports","auth":{"method":"default_credential"} }""")] // no account_url
    [InlineData("""{"type":"azure_blob","account_url":"https://a","container":"exports"}""")]             // no auth
    [InlineData("""{"type":"azure_blob","account_url":"https://a","container":"exports","auth":{"method":"password"}}""")]
    [InlineData("""{"type":"azure_blob","account_url":"https://a","container":"exports","auth":{"method":"sas_url_env"}}""")] // no name
    public void FromJson_rejects_bad_settings(string json) =>
        Assert.Throws<ArgumentException>(() => Json(json));

    // ---- no secret or redirect hides in a URL (F1, F2) ----

    private static string S3Endpoint(string url) => $$"""{"type":"s3","bucket":"b","endpoint_url":{{JsonSerializer.Serialize(url)}} }""";

    private static string AzureAccount(string url) =>
        $$"""{"type":"azure_blob","account_url":{{JsonSerializer.Serialize(url)}},"container":"exports","auth":{"method":"default_credential"} }""";

    [Theory]
    [InlineData("https://minio.example/#lab-secret-value")]
    [InlineData("https://minio.example/bucket%3Flab-secret-value")]
    [InlineData("https://minio.example/bucket%3flab-secret-value")]
    [InlineData("https://minio.example/sig=lab-secret-value")]
    [InlineData("https://minio.example/a/x&lab-secret-value")]
    [InlineData("https://minio.example/a/sv%3Dlab-secret-value")]
    [InlineData("https://minio.example/?")]
    [InlineData("https://minio.example/#")]
    public void FromJson_refuses_an_endpoint_url_that_carries_a_secret_in_the_fragment_or_the_path(string url)
    {
        var ex = Assert.Throws<ArgumentException>(() => Json(S3Endpoint(url)));

        Assert.StartsWith("destination.endpoint_url must not carry", ex.Message);
        Assert.DoesNotContain("lab-secret-value", ex.Message);
        Assert.DoesNotContain("minio.example", ex.Message);
    }

    [Theory]
    [InlineData("https://acct.blob.core.windows.net/#lab-secret-value")]
    [InlineData("https://acct.blob.core.windows.net/c%3Flab-secret-value")]
    [InlineData("https://acct.blob.core.windows.net/c%3flab-secret-value")]
    [InlineData("https://acct.blob.core.windows.net/sig=lab-secret-value")]
    [InlineData("https://acct.blob.core.windows.net/a/x&lab-secret-value")]
    [InlineData("http://127.0.0.1:10000/devstoreaccount1#lab-secret-value")]
    [InlineData("http://127.0.0.1:10000/devstoreaccount1%3Fsv=lab-secret-value")]
    public void FromJson_refuses_an_account_url_that_carries_a_secret_in_the_fragment_or_the_path(string url)
    {
        var ex = Assert.Throws<ArgumentException>(() => Json(AzureAccount(url)));

        Assert.StartsWith("destination.account_url must not carry", ex.Message);
        Assert.DoesNotContain("lab-secret-value", ex.Message);
        Assert.DoesNotContain("blob.core.windows.net", ex.Message);
    }

    [Theory]
    [InlineData("http://127.0.0.1:10000/devstoreaccount1")]
    [InlineData("http://localhost:10000/devstoreaccount1")]
    [InlineData("http://LOCALHOST:10000/devstoreaccount1")]
    [InlineData("http://[::1]:10000/devstoreaccount1")]
    [InlineData("http://127.1.2.3:10000/devstoreaccount1")]
    [InlineData("https://acct.blob.core.windows.net")]
    [InlineData("https://acct.blob.core.windows.net/")]
    [InlineData("https://127.0.0.1:10000/devstoreaccount1")]
    public void FromJson_accepts_an_https_account_url_and_a_loopback_http_one(string url) =>
        Assert.IsType<UploadSettings.AzureBlob>(Json(AzureAccount(url)));

    [Theory]
    [InlineData("http://acct.blob.core.windows.net")]
    [InlineData("http://10.0.0.5:10000/devstoreaccount1")]
    [InlineData("http://128.0.0.1:10000/devstoreaccount1")]
    [InlineData("file:///var/lab-secret-value")]
    [InlineData("ftp://acct.example/lab-secret-value")]
    [InlineData("s3://acct/lab-secret-value")]
    public void FromJson_refuses_an_account_url_that_is_not_https_unless_it_is_loopback_http(string url)
    {
        var ex = Assert.Throws<ArgumentException>(() => Json(AzureAccount(url)));

        Assert.StartsWith("destination.account_url must be an https URL", ex.Message);
        Assert.DoesNotContain("lab-secret-value", ex.Message);
        Assert.DoesNotContain("acct", ex.Message);
        Assert.DoesNotContain("10.0.0.5", ex.Message);
    }

    [Theory]
    [InlineData("endpoint_url", "http://169.254.169.254")]
    [InlineData("endpoint_url", "https://169.254.169.254/latest/lab-secret-value")]
    [InlineData("endpoint_url", "http://169.254.1.1:9000")]
    [InlineData("endpoint_url", "http://[fe80::1]:9000")]
    [InlineData("endpoint_url", "http://[febf::1]:9000")]
    [InlineData("endpoint_url", "http://[::ffff:169.254.169.254]/")]
    [InlineData("endpoint_url", "http://2852039166/")]
    [InlineData("account_url", "https://169.254.169.254/")]
    [InlineData("account_url", "https://[fe80::1]/")]
    public void FromJson_refuses_a_link_local_host_in_either_url(string key, string url)
    {
        var json = key == "endpoint_url" ? S3Endpoint(url) : AzureAccount(url);

        var ex = Assert.Throws<ArgumentException>(() => Json(json));

        Assert.Equal($"destination.{key} must not point at a link-local address (169.254.0.0/16 or fe80::/10)", ex.Message);
    }

    [Theory]
    [InlineData("http://minio.internal:9000")]
    [InlineData("http://10.0.0.5:9000")]
    [InlineData("http://127.0.0.1:9000")]
    [InlineData("http://169.253.1.1:9000")]
    [InlineData("http://[fec0::1]:9000")]
    [InlineData("https://minio.example/some/path")]
    [InlineData("https://s3.us-east-2.amazonaws.com")]
    public void FromJson_accepts_an_s3_endpoint_url_over_http_or_https_on_a_normal_host(string url) =>
        Assert.Equal(url, Assert.IsType<UploadSettings.S3>(Json(S3Endpoint(url))).EndpointUrl);

    // ---- a prefix, a bucket and a container stay where the config names them (F4) ----

    private static string DestinationWithPrefix(string type, string prefix)
    {
        var p = JsonSerializer.Serialize(prefix);
        return type switch
        {
            "local" => $$"""{"type":"local","path":"/srv/landing","prefix":{{p}} }""",
            "s3" => $$"""{"type":"s3","bucket":"b","prefix":{{p}} }""",
            _ => $$"""{"type":"azure_blob","account_url":"https://acct.blob.core.windows.net","container":"exports","prefix":{{p}},"auth":{"method":"default_credential"} }""",
        };
    }

    public static TheoryData<string, string> BadPrefixes()
    {
        var data = new TheoryData<string, string>();
        foreach (var type in new[] { "local", "s3", "azure_blob" })
        {
            foreach (var prefix in new[] { "../lab-secret-value", "a/../lab-secret-value", "..", ".", "./a", "a/./b", "a/..", "...", ".. ", "x/ ./y", @"a\lab-secret-value", @"..\other", @"\\host\share" })
            {
                data.Add(type, prefix);
            }
        }

        data.Add("local", "C:x");
        data.Add("local", "c:/x");
        data.Add("local", "Z:");
        return data;
    }

    [Theory]
    [MemberData(nameof(BadPrefixes))]
    public void FromJson_refuses_a_prefix_that_leaves_the_destination(string type, string prefix)
    {
        var ex = Assert.Throws<ArgumentException>(() => Json(DestinationWithPrefix(type, prefix)));

        Assert.StartsWith("destination.prefix must ", ex.Message);
        Assert.DoesNotContain("lab-secret-value", ex.Message);
        Assert.DoesNotContain("other", ex.Message);
    }

    [Theory]
    [InlineData("local", "a/b")]
    [InlineData("local", "/landing/hourly/")]
    [InlineData("s3", "..a/b..")]
    [InlineData("s3", "my.folder/a b")]
    [InlineData("azure_blob", "a..b/.hidden")]
    [InlineData("azure_blob", "")]
    public void FromJson_keeps_an_ordinary_prefix(string type, string prefix)
    {
        var settings = Json(DestinationWithPrefix(type, prefix));

        Assert.Equal(prefix.Trim('/'), settings.Layout.Prefix);
    }

    [Theory]
    [InlineData("s3://b/../lab-secret-value")]
    [InlineData("s3://b/a/./c")]
    [InlineData(@"s3://b/a\c")]
    public void FromUri_refuses_an_s3_prefix_that_leaves_the_bucket_folder(string uri) =>
        Assert.Throws<ArgumentException>(() => UploadSettings.FromUri(uri, null));

    [Theory]
    [InlineData("../lab-secret-value")]
    [InlineData("lab/secret-value")]
    [InlineData(@"lab\secret-value")]
    [InlineData(".")]
    [InlineData("..")]
    public void FromJson_refuses_an_s3_bucket_that_is_a_path(string bucket)
    {
        var ex = Assert.Throws<ArgumentException>(() => Json($$"""{"type":"s3","bucket":{{JsonSerializer.Serialize(bucket)}},"force_path_style":true}"""));

        Assert.StartsWith("destination.bucket must be a bucket name", ex.Message);
        Assert.DoesNotContain("lab", ex.Message);
    }

    [Theory]
    [InlineData("my-bucket")]
    [InlineData("my.bucket.name")]
    [InlineData("My_Legacy_Bucket")]
    [InlineData("a..b")]
    [InlineData("...")]
    public void FromJson_keeps_a_legacy_s3_bucket_name(string bucket) =>
        Assert.Equal(bucket, Assert.IsType<UploadSettings.S3>(Json($$"""{"type":"s3","bucket":{{JsonSerializer.Serialize(bucket)}} }""")).Bucket);

    [Theory]
    [InlineData("exports")]
    [InlineData("abc")]
    [InlineData("a-b-c")]
    [InlineData("1a2")]
    [InlineData("$root")]
    [InlineData("$web")]
    public void FromJson_accepts_an_azure_container_name(string container) =>
        Assert.Equal(container, Assert.IsType<UploadSettings.AzureBlob>(Json(AzureContainer(container))).Container);

    private static string AzureContainer(string container) =>
        $$"""{"type":"azure_blob","account_url":"https://acct.blob.core.windows.net","container":{{JsonSerializer.Serialize(container)}},"auth":{"method":"default_credential"} }""";

    [Fact]
    public void FromJson_accepts_a_container_name_of_63_characters_and_refuses_one_of_64()
    {
        Assert.Equal(new string('a', 63), Assert.IsType<UploadSettings.AzureBlob>(Json(AzureContainer(new string('a', 63)))).Container);
        Assert.Throws<ArgumentException>(() => Json(AzureContainer(new string('a', 64))));
    }

    [Theory]
    [InlineData("ab")]
    [InlineData("Lab-Secret-Value")]
    [InlineData("-lab-secret")]
    [InlineData("lab-secret-")]
    [InlineData("lab--secret")]
    [InlineData("lab_secret")]
    [InlineData("lab.secret")]
    [InlineData("lab secret")]
    [InlineData("$logs")]
    [InlineData("$Root")]
    [InlineData("../lab-secret")]
    public void FromJson_refuses_an_azure_container_name_that_is_not_one(string container)
    {
        var ex = Assert.Throws<ArgumentException>(() => Json(AzureContainer(container)));

        Assert.StartsWith("destination.container must be 3 to 63 characters", ex.Message);
        Assert.DoesNotContain("lab", ex.Message);
    }

    [Fact]
    public void FromJson_rejects_a_non_object() =>
        Assert.Throws<ArgumentException>(() => UploadSettings.FromJson(JsonDocument.Parse("\"s3://b\"").RootElement));
}
