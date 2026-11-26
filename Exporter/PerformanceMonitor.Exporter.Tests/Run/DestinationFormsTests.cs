using System.Text.Json;
using PerformanceMonitor.Exporter.Run;
using PerformanceMonitor.Exporter.Tests.Upload;
using PerformanceMonitor.Exporter.Upload;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>
/// Every documented destination form reaches the uploader. A plain s3 destination (bucket, prefix, region) and a local one keep
/// the child's `--upload` call and upload the same keys and bytes. Every other form (an S3 endpoint, path style, checksum or
/// credentials, and azure_blob) runs the child as `export --config F --upload-destination`, which reads the destination from
/// the config. No key value reaches the plan, the console or the printed config.
/// </summary>
public class DestinationFormsTests
{
    private static readonly DateTime Now = new(2026, 9, 28, 14, 20, 0, DateTimeKind.Utc);

    private static string Json(string s) => JsonSerializer.Serialize(s);

    private static string Config(RunRoot r, string destination, string credentialFile = "unused") => $$"""
        {
          "config_version": 1,
          "store": { "name": "destforms", "host": "127.0.0.1", "port": 5641, "database": "darling", "user": "darling",
                     "password": { "source": "file", "path": {{Json(credentialFile)}} } },
          "paths": { "out": {{Json(r.Out)}}, "state": {{Json(r.State)}}, "logs": {{Json(r.Logs)}} },
          "destination": {{destination}}
        }
        """;

    private static string[] ChildArgs(RunSettings s)
    {
        var mode = Planner.ResolveMode(s, []);
        var plan = Planner.PlanFamily(s, mode, Planner.Fresh, new DateTime(2026, 9, 28, 12, 0, 0, DateTimeKind.Utc), Now);
        return [.. Planner.BuildExportArgs(s, Planner.CallFor(mode, plan, plan.Windows[0]))];
    }

    /// <summary>The upload target the child works out from the call `run` builds for <paramref name="json"/>.</summary>
    private static (ExportOptions Options, string[] Args, RunSettings Settings) Child(string json, string configPath)
    {
        var config = ConfigLoader.Parse(json, isWindows: false);
        var settings = RunConfigMapping.ToRunSettings(config, configPath);
        Assert.Null(settings.Validate());
        var args = ChildArgs(settings);
        Assert.Equal("export", args[0]);
        return (ExportCommand.Parse(args[1..], Now, config).Options, args, settings);
    }

    private const string S3Endpoint = """{ "type": "s3", "bucket": "b", "prefix": "p", "endpoint_url": "http://127.0.0.1:9000", "force_path_style": true }""";
    private const string S3EnvKeys = """{ "type": "s3", "bucket": "b", "region": "us-east-2", "credentials": { "access_key_id_env": "LAB_AK", "secret_access_key_env": "LAB_SK", "session_token_env": "LAB_ST" } }""";
    private const string S3Profile = """{ "type": "s3", "bucket": "b", "credentials": { "profile": "exporter" } }""";
    private const string S3NoChecksum = """{ "type": "s3", "bucket": "b", "checksum": "none" }""";
    private const string AzureDefault = """{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net", "container": "exports", "prefix": "p", "auth": { "method": "default_credential" } }""";
    private const string AzureConnectionString = """{ "type": "azure_blob", "container": "exports", "auth": { "method": "connection_string_env", "name": "LAB_AZ_CS" } }""";
    private const string AzureSas = """{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net", "container": "exports", "auth": { "method": "sas_url_env", "name": "LAB_AZ_SAS" } }""";

    public static TheoryData<string> ConfigOnlyForms => [S3Endpoint, S3EnvKeys, S3Profile, S3NoChecksum, AzureDefault, AzureConnectionString, AzureSas];

    // ---- run: the child call ----

    [Theory]
    [MemberData(nameof(ConfigOnlyForms))]
    public void A_form_the_upload_flag_cannot_express_runs_the_child_with_the_config_and_upload_destination(string destination)
    {
        using var r = new RunRoot();
        var cfg = Path.Combine(r.Root, "c.json");
        var (options, args, settings) = Child(Config(r, destination), cfg);

        Assert.True(settings.UploadDestination);
        Assert.Null(settings.Upload);
        Assert.Null(settings.AwsRegion);
        Assert.Equal(["export", "--config", Path.GetFullPath(cfg)], args[..3]);
        Assert.Contains("--upload-destination", args);
        Assert.DoesNotContain("--upload", args);
        Assert.DoesNotContain("--aws-region", args);

        // The child parses the same object `run` and `check-config` validated.
        using var doc = JsonDocument.Parse(destination);
        Assert.Equal(UploadSettings.FromJson(doc.RootElement), options.UploadTarget());
        Assert.Null(options.Upload);
    }

    [Fact]
    public void A_dpapi_box_config_with_an_endpoint_destination_also_runs_the_child_with_the_config()
    {
        // The flags-only call has no config for the child to read, so a config-only destination turns it off.
        var json = $$"""
            { "config_version": 1,
              "store": { "name": "store_a", "password": { "source": "dpapi_file", "path": "C:\\ProgramData\\PerformanceMonitorDarling\\pg-credential.dpapi" } },
              "paths": { "out": "C:\\exporter\\out", "state": "C:\\exporter\\state", "logs": "C:\\exporter\\logs" },
              "destination": {{S3Endpoint}} }
            """;
        var settings = RunConfigMapping.ToRunSettings(ConfigLoader.Parse(json, isWindows: true), @"C:\exporter\config\darling-export.json");
        var args = ChildArgs(settings);
        Assert.Equal(["export", "--config", Path.GetFullPath(@"C:\exporter\config\darling-export.json")], args[..3]);
        Assert.Contains("--upload-destination", args);
        Assert.DoesNotContain("--pg-credential-file", args);

        var plain = RunConfigMapping.ToRunSettings(ConfigLoader.Parse(json.Replace(S3Endpoint, """{ "type": "s3", "bucket": "b" }"""), isWindows: true), @"C:\exporter\config\darling-export.json");
        Assert.DoesNotContain("--config", ChildArgs(plain));
    }

    [Theory]
    [InlineData("""{ "type": "s3", "bucket": "b", "hive_store_folder": true }""")]
    [InlineData("""{ "type": "local", "path": "/srv/landing", "hive_store_folder": true }""")]
    [InlineData("""{ "type": "azure_blob", "container": "exports", "hive_store_folder": true, "auth": { "method": "connection_string_env", "name": "X" } }""")]
    public void The_hive_store_folder_layout_is_refused_for_every_type(string destination)
    {
        using var r = new RunRoot();
        var ex = Assert.Throws<ConfigException>(() => RunConfigMapping.ToRunSettings(ConfigLoader.Parse(Config(r, destination), isWindows: false), "c.json"));
        Assert.Equal("destination.hive_store_folder: run does not support it for now", ex.Message);
    }

    [Fact]
    public void Run_settings_refuse_upload_destination_without_a_config_for_the_child()
    {
        using var r = new RunRoot();
        Assert.NotNull(r.Settings(tweak: s => s with { UploadDestination = true, Upload = null }).Validate());
        Assert.NotNull(r.Settings(tweak: s => s with { UploadDestination = true, Upload = "s3://b", ChildBaseArgs = ["export", "--config", "c.json"] }).Validate());
        Assert.Null(r.Settings(tweak: s => s with { UploadDestination = true, Upload = null, AwsRegion = null, ChildBaseArgs = ["export", "--config", "c.json"] }).Validate());
    }

    // ---- plain s3 and local: the same child call, the same keys, the same bytes ----

    [Fact]
    public async Task A_plain_s3_destination_keeps_the_upload_flag_and_uploads_the_same_keys()
    {
        using var r = new RunRoot();
        var (options, args, settings) = Child(Config(r, """{ "type": "s3", "bucket": "example-bucket", "prefix": "/landing/hourly/", "region": "us-east-2" }"""), Path.Combine(r.Root, "c.json"));

        Assert.False(settings.UploadDestination);
        Assert.DoesNotContain("--upload-destination", args);
        Assert.Equal("s3://example-bucket/landing/hourly", args[Array.IndexOf(args, "--upload") + 1]);
        Assert.Equal("us-east-2", args[Array.IndexOf(args, "--aws-region") + 1]);

        // The `--upload` value with `--aws-region` builds the bucket, the prefix and the region, and nothing else.
        var flagForm = UploadSettings.FromUri("s3://example-bucket/landing/hourly", "us-east-2");
        var target = Assert.IsType<UploadSettings.S3>(options.UploadTarget());
        Assert.Null(options.Destination);
        Assert.Equal(flagForm, target);
        Assert.Equal(new UploadSettings.S3("example-bucket", "landing/hourly", "us-east-2"), target);
        var client = S3Uploader.BuildConfig(target);
        Assert.Equal("us-east-2", client.RegionEndpoint.SystemName);
        Assert.Null(client.ServiceURL);
        Assert.False(client.ForcePathStyle);

        var (manifest, manifestPath) = UploadTestSupport.WriteRun(r.Out, "destforms", "wait_stats", "query_stats");
        var flagKeys = new FakeUploader();
        var runKeys = new FakeUploader();
        await RunUpload.UploadRunAsync(flagKeys, flagForm.Layout, UploadTestSupport.Options(r.Out, "destforms"), manifest, manifestPath, TextWriter.Null, CancellationToken.None);
        await RunUpload.UploadRunAsync(runKeys, target.Layout, UploadTestSupport.Options(r.Out, "destforms"), manifest, manifestPath, TextWriter.Null, CancellationToken.None);
        Assert.Equal(3, runKeys.Keys.Count);
        Assert.All(runKeys.Keys, k => Assert.StartsWith("landing/hourly/destforms/", k));
        Assert.Equal(flagKeys.Keys, runKeys.Keys);
    }

    [Theory]
    [InlineData("")]
    [InlineData("hourly")]
    public async Task A_local_destination_keeps_the_upload_flag_and_uploads_the_same_keys_and_bytes(string prefix)
    {
        using var r = new RunRoot();
        var landingFlag = Path.Combine(r.Root, "landing-flag");
        var landingRun = Path.Combine(r.Root, "landing-run");
        var rootFlag = prefix.Length == 0 ? landingFlag : Path.Combine(landingFlag, prefix);
        var rootRun = prefix.Length == 0 ? landingRun : Path.Combine(landingRun, prefix);
        Directory.CreateDirectory(rootFlag);
        Directory.CreateDirectory(rootRun);
        var destination = $$"""{ "type": "local", "path": {{Json(landingRun)}}{{(prefix.Length == 0 ? "" : ", \"prefix\": " + Json(prefix))}} }""";

        var (options, args, settings) = Child(Config(r, destination), Path.Combine(r.Root, "c.json"));
        Assert.False(settings.UploadDestination);
        Assert.DoesNotContain("--upload-destination", args);
        Assert.Equal(rootRun, args[Array.IndexOf(args, "--upload") + 1]);
        Assert.Null(options.Destination);
        var target = options.UploadTarget()!;
        Assert.Equal(UploadSettings.FromUri(rootRun, null), target);

        var (manifest, manifestPath) = UploadTestSupport.WriteRun(r.Out, "destforms", "wait_stats", "query_stats");
        var exportOptions = UploadTestSupport.Options(r.Out, "destforms");
        // One upload with a folder given as the `--upload` value, one with the target the child works out from the config.
        var flagForm = UploadSettings.FromUri(rootFlag, null);
        await using (var uploader = UploaderFactory.Create(flagForm))
        {
            await RunUpload.UploadRunAsync(uploader, flagForm.Layout, exportOptions, manifest, manifestPath, TextWriter.Null, CancellationToken.None);
        }
        await using (var uploader = UploaderFactory.Create(target))
        {
            await RunUpload.UploadRunAsync(uploader, target.Layout, exportOptions, manifest, manifestPath, TextWriter.Null, CancellationToken.None);
        }

        static Dictionary<string, string> Tree(string root) => Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
            .ToDictionary(f => Path.GetRelativePath(root, f).Replace('\\', '/'), f => Convert.ToHexString(File.ReadAllBytes(f)));
        var treeFlag = Tree(rootFlag);
        var treeRun = Tree(rootRun);
        Assert.Equal(3, treeRun.Count);
        Assert.All(treeRun.Keys, k => Assert.StartsWith("destforms/", k));
        Assert.Equal(treeFlag.OrderBy(kv => kv.Key, StringComparer.Ordinal), treeRun.OrderBy(kv => kv.Key, StringComparer.Ordinal));
    }

    // ---- export --upload-destination ----

    private static readonly string[] Window = ["--from", "2026-09-28T12:00:00Z", "--to", "2026-09-28T13:00:00Z", "--tables", "wait_stats"];

    [Fact]
    public void Export_upload_destination_reads_the_full_s3_form_from_the_config()
    {
        using var r = new RunRoot();
        var config = ConfigLoader.Parse(Config(r, """
            { "type": "s3", "bucket": "b", "prefix": "p/", "region": "auto", "endpoint_url": "https://x.example", "force_path_style": true, "checksum": "none",
              "credentials": { "access_key_id_env": "LAB_AK", "secret_access_key_env": "LAB_SK" } }
            """), isWindows: false);
        var options = ExportCommand.Parse(["--config", "c.json", "--upload-destination", .. Window], Now, config).Options;
        Assert.Equal(
            new UploadSettings.S3("b", "p", "auto", "https://x.example", true, S3Checksum.None, new S3Credentials.EnvKeys("LAB_AK", "LAB_SK", null)),
            options.UploadTarget());
    }

    [Fact]
    public void Export_with_a_config_and_no_upload_flag_still_uploads_nothing()
    {
        using var r = new RunRoot();
        var config = ConfigLoader.Parse(Config(r, AzureConnectionString), isWindows: false);
        Assert.Null(ExportCommand.Parse(["--config", "c.json", .. Window], Now, config).Options.UploadTarget());
    }

    [Fact]
    public void Export_upload_destination_needs_a_config_destination_and_no_upload_flag()
    {
        using var r = new RunRoot();
        var config = ConfigLoader.Parse(Config(r, AzureConnectionString), isWindows: false);
        var none = ConfigLoader.Parse(Config(r, "null"), isWindows: false);

        Assert.Contains("needs a --config file", Assert.Throws<ConfigException>(() => ExportCommand.Parse(["--store", "store_a", "--out", "/o", "--state-dir", "/s", "--upload-destination", .. Window], Now)).Message);
        Assert.Contains("needs a --config file", Assert.Throws<ConfigException>(() => ExportCommand.Parse(["--config", "c.json", "--upload-destination", .. Window], Now, none)).Message);
        Assert.Contains("not allowed with --upload", Assert.Throws<ConfigException>(() => ExportCommand.Parse(["--config", "c.json", "--upload-destination", "--upload", "s3://b", .. Window], Now, config)).Message);
        Assert.Contains("not allowed with --upload", Assert.Throws<ConfigException>(() => ExportCommand.Parse(["--config", "c.json", "--upload-destination", "--aws-region", "us-east-1", .. Window], Now, config)).Message);

        var typo = ConfigLoader.Parse(Config(r, """{ "type": "s3", "bucket": "b", "endpoint": "http://127.0.0.1:9000" }"""), isWindows: false);
        Assert.Equal("destination has an unknown field 'endpoint'", Assert.Throws<ConfigException>(() => ExportCommand.Parse(["--config", "c.json", "--upload-destination", .. Window], Now, typo)).Message);
    }

    // ---- keys never reach the console, the plan or the printed config ----

    [Theory]
    [InlineData(S3EnvKeys, "LAB_AK,LAB_SK,LAB_ST")]
    [InlineData(AzureConnectionString, "LAB_AZ_CS")]
    [InlineData(AzureSas, "LAB_AZ_SAS")]
    public async Task The_plan_and_the_printed_config_name_the_key_variables_and_never_hold_their_values(string destination, string variables)
    {
        using var r = new RunRoot();
        // Each variable gets a name of its own and a value that is easy to find.
        var suffix = "_" + Guid.NewGuid().ToString("N").ToUpperInvariant();
        var names = variables.Split(',').Select(v => v + suffix).ToList();
        foreach (var v in variables.Split(',')) destination = destination.Replace($"\"{v}\"", $"\"{v}{suffix}\"");
        var values = names.Select((n, i) => $"key-value-{i}-{Guid.NewGuid():N}").ToList();
        for (var i = 0; i < names.Count; i++) Environment.SetEnvironmentVariable(names[i], values[i]);
        try
        {
            var cfg = Path.Combine(r.Root, "c.json");
            File.WriteAllText(cfg, Config(r, destination));
            var o = new StringWriter(); var e = new StringWriter();
            var exit = await RunCommand.MainAsync(["--config", cfg, "--plan-only", "--now", "2026-09-28T16:30:00Z"], o, e, CancellationToken.None,
                planStoreCheck: (_, _) => Task.FromResult<string?>(null));
            Assert.Equal(0, exit);
            var plan = File.ReadAllText(Directory.GetFiles(r.Logs, "plan-only-*.jsonl").Single());
            var printed = ConfigPrinter.Render(ConfigLoader.Load(cfg, isWindows: false), credentialResolved: true);

            Assert.Contains("--upload-destination", plan);
            foreach (var name in names) Assert.Contains(name, printed);
            foreach (var value in values)
            {
                Assert.DoesNotContain(value, o.ToString());
                Assert.DoesNotContain(value, e.ToString());
                Assert.DoesNotContain(value, plan);
                Assert.DoesNotContain(value, printed);
            }
        }
        finally
        {
            foreach (var name in names) Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Theory]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "https://lab-user:lab-secret-value@minio.example:9000" }""", "destination.endpoint_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "https://minio.example/?X-Amz-Signature=lab-secret-value" }""", "destination.endpoint_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net/?sv=2025&sig=lab-secret-value", "container": "exports", "auth": { "method": "default_credential" } }""", "destination.account_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "minio.example:9000" }""", "destination.endpoint_url is not a valid http or https URL", "minio.example")]
    [InlineData("""{ "type": "azure_blob", "container": "exports", "auth": { "method": "connection_string_env", "name": "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=lab-secret-value" } }""", "destination.auth.name must be the name of an environment variable", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://a.blob.core.windows.net", "container": "exports", "auth": { "method": "sas_url_env", "name": "sv=2025&sig=lab-secret-value" } }""", "destination.auth.name must be the name of an environment variable", "lab-secret-value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "credentials": { "access_key_id_env": "LAB_AK", "secret_access_key_env": "lab/secret+value" } }""", "destination.credentials.secret_access_key_env must be the name of an environment variable", "lab/secret+value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "https://minio.example/#lab-secret-value" }""", "destination.endpoint_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "https://minio.example/bucket%3Flab-secret-value" }""", "destination.endpoint_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net/#lab-secret-value", "container": "exports", "auth": { "method": "default_credential" } }""", "destination.account_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net/sig=lab-secret-value", "container": "exports", "auth": { "method": "default_credential" } }""", "destination.account_url must not carry", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "http://acct.blob.core.windows.net", "container": "exports", "auth": { "method": "sas_url_env", "name": "LAB_AZ_SAS" } }""", "destination.account_url must be an https URL", "acct.blob")]
    [InlineData("""{ "type": "azure_blob", "account_url": "file:///lab-secret-value", "container": "exports", "auth": { "method": "default_credential" } }""", "destination.account_url must be an https URL", "lab-secret-value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "http://169.254.169.254/lab-secret-value" }""", "destination.endpoint_url must not point at a link-local address", "169.254.169.254")]
    [InlineData("""{ "type": "local", "path": "/srv/landing", "prefix": "../lab-secret-value" }""", "destination.prefix must not contain a '.' or '..' segment", "lab-secret-value")]
    [InlineData("""{ "type": "s3", "bucket": "b", "prefix": "a/../lab-secret-value" }""", "destination.prefix must not contain a '.' or '..' segment", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net", "container": "exports", "prefix": "../lab-secret-value", "auth": { "method": "default_credential" } }""", "destination.prefix must not contain a '.' or '..' segment", "lab-secret-value")]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://acct.blob.core.windows.net", "container": "Lab_Secret_Value", "auth": { "method": "default_credential" } }""", "destination.container must be 3 to 63 characters", "Lab_Secret_Value")]
    [InlineData("""{ "type": "s3", "bucket": "lab/secret-value" }""", "destination.bucket must be a bucket name", "lab/secret-value")]
    public async Task A_url_or_a_variable_name_that_carries_a_key_is_refused_and_the_message_does_not_repeat_it(string destination, string message, string mustNotAppear)
    {
        using var r = new RunRoot();
        var cfg = Path.Combine(r.Root, "c.json");
        File.WriteAllText(cfg, Config(r, destination));
        var o = new StringWriter(); var e = new StringWriter();
        var exit = await CliVerbs.CheckConfigAsync(["check-config", "--config", cfg], o, e, CancellationToken.None);
        Assert.Equal(2, exit);
        Assert.StartsWith("Config error: " + message, e.ToString());
        Assert.DoesNotContain(mustNotAppear, e.ToString());
        Assert.Empty(o.ToString());
    }

    // ---- the key variables are checked before the first window ----

    [Theory]
    [InlineData(S3EnvKeys, "LAB_AK", "destination.credentials.access_key_id_env: the environment variable it names is not set")]
    [InlineData(S3EnvKeys, "LAB_SK", "destination.credentials.secret_access_key_env: the environment variable it names is not set")]
    [InlineData(AzureConnectionString, "LAB_AZ_CS", "destination.auth.name: the environment variable it names is not set")]
    [InlineData(AzureSas, "LAB_AZ_SAS", "destination.auth.name: the environment variable it names is not set")]
    public void An_unset_key_variable_is_a_config_error_that_names_the_config_key_and_not_the_variable(string destination, string unset, string message)
    {
        using var r = new RunRoot();
        var config = ConfigLoader.Parse(Config(r, destination), isWindows: false);
        var ex = Assert.Throws<ConfigException>(() => DestinationCheck.AssertCredentials(config, name => name == unset ? null : "set-value"));
        Assert.Equal(message, ex.Message);
        Assert.DoesNotContain("set-value", ex.Message);
        Assert.DoesNotContain(unset, ex.Message);
        DestinationCheck.AssertCredentials(config, _ => "set-value");
    }

    [Theory]
    [InlineData("""{ "type": "s3", "bucket": "b", "region": "us-east-1" }""")]
    [InlineData(S3Profile)]
    [InlineData(AzureDefault)]
    [InlineData("""{ "type": "local", "path": "/srv/landing" }""")]
    public void A_destination_that_names_no_key_variable_has_none_to_check(string destination)
    {
        using var r = new RunRoot();
        DestinationCheck.AssertCredentials(ConfigLoader.Parse(Config(r, destination), isWindows: false), _ => null);
    }

    [Fact]
    public async Task Run_with_an_unset_key_variable_exits_2_with_one_line_that_does_not_name_the_variable_and_starts_no_window_and_plan_only_still_plans()
    {
        using var r = new RunRoot(); r.SeedState("destforms");
        var credential = Path.Combine(r.Root, "pw.txt");
        File.WriteAllText(credential, "lab-value");
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(credential, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var name = "LAB_AZ_CS_UNSET_" + Guid.NewGuid().ToString("N").ToUpperInvariant();
        var cfg = Path.Combine(r.Root, "c.json");
        File.WriteAllText(cfg, Config(r, AzureConnectionString.Replace("LAB_AZ_CS", name), credential));

        var child = new FakeChild();
        var o = new StringWriter { NewLine = "\n" }; var e = new StringWriter { NewLine = "\n" };
        var exit = await RunCommand.MainAsync(["--config", cfg, "--now", "2026-09-28T16:30:00Z"], o, e, CancellationToken.None, child);
        Assert.Equal(2, exit);
        Assert.Empty(child.Calls);
        Assert.Equal("Config error: destination.auth.name: the environment variable it names is not set", e.ToString().Trim());

        var check = new StringWriter();
        Assert.Equal(2, await CliVerbs.CheckConfigAsync(["check-config", "--config", cfg], new StringWriter(), check, CancellationToken.None));
        Assert.Equal("Config error: destination.auth.name: the environment variable it names is not set", check.ToString().Trim());
        Assert.DoesNotContain(name, check.ToString());

        var plan = await RunCommand.MainAsync(["--config", cfg, "--plan-only", "--now", "2026-09-28T16:30:00Z"], new StringWriter(), new StringWriter(), CancellationToken.None,
            planStoreCheck: (_, _) => Task.FromResult<string?>(null));
        Assert.Equal(0, plan);
    }

    // ---- a duplicate key is a config error for every verb that reads the config ----

    private static string DuplicateBucketConfig(RunRoot r, string credentialFile) =>
        Config(r, """{ "type": "s3", "bucket": "lab-first-value", "bucket": "lab-second-value" }""", credentialFile);

    [Fact]
    public async Task A_duplicate_key_is_exit_2_with_one_line_naming_the_key_from_check_config_run_plan_only_and_export()
    {
        using var r = new RunRoot(); r.SeedState("destforms");
        var credential = Path.Combine(r.Root, "pw.txt");
        File.WriteAllText(credential, "lab-value");
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(credential, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var cfg = Path.Combine(r.Root, "c.json");
        File.WriteAllText(cfg, DuplicateBucketConfig(r, credential));
        const string line = "Config error: destination.bucket: duplicate key";

        var checkOut = new StringWriter(); var checkErr = new StringWriter { NewLine = "\n" };
        Assert.Equal(2, await CliVerbs.CheckConfigAsync(["check-config", "--config", cfg], checkOut, checkErr, CancellationToken.None));
        Assert.Equal(line, checkErr.ToString().Trim());
        Assert.Empty(checkOut.ToString());

        var child = new FakeChild();
        var runOut = new StringWriter(); var runErr = new StringWriter { NewLine = "\n" };
        Assert.Equal(2, await RunCommand.MainAsync(["--config", cfg, "--now", "2026-09-28T16:30:00Z"], runOut, runErr, CancellationToken.None, child));
        Assert.Equal(line, runErr.ToString().Trim());
        Assert.Empty(child.Calls);

        var planErr = new StringWriter { NewLine = "\n" };
        Assert.Equal(2, await RunCommand.MainAsync(["--config", cfg, "--plan-only", "--now", "2026-09-28T16:30:00Z"], new StringWriter(), planErr, CancellationToken.None,
            planStoreCheck: (_, _) => Task.FromResult<string?>(null)));
        Assert.Equal(line, planErr.ToString().Trim());

        var exportErr = new StringWriter { NewLine = "\n" };
        Assert.Equal(2, await CliVerbs.ExportAsync(["--config", cfg, "--upload-destination", .. Window], new StringWriter(), exportErr, CancellationToken.None));
        Assert.Equal(line, exportErr.ToString().Trim());

        foreach (var text in new[] { checkErr.ToString(), runErr.ToString(), planErr.ToString(), exportErr.ToString() })
        {
            Assert.DoesNotContain("lab-first-value", text);
            Assert.DoesNotContain("lab-second-value", text);
        }
    }
}
