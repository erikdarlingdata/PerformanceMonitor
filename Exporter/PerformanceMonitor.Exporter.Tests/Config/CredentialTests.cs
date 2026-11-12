using Npgsql;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Config;

public class CredentialTests : IDisposable
{
    private readonly List<string> _tempFiles = [];
    private readonly List<string> _envVars = [];

    private string Env(string value)
    {
        var name = "DARLING_TEST_CRED_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(name, value);
        _envVars.Add(name);
        return name;
    }

    private string TempFile(string content)
    {
        var path = Path.Combine(Path.GetTempPath(), "cred-" + Guid.NewGuid().ToString("N"));
        File.WriteAllText(path, content);
        _tempFiles.Add(path);
        return path;
    }

    public void Dispose()
    {
        foreach (var f in _tempFiles) File.Delete(f);
        foreach (var e in _envVars) Environment.SetEnvironmentVariable(e, null);
    }

    [Fact]
    public void Env_source_returns_the_variable()
    {
        var name = Env("s3cret");
        Assert.Equal("s3cret", CredentialResolver.Resolve(new CredentialSpec("env", Name: name)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Env_source_unset_or_empty_is_a_credential_error(string? value)
    {
        var name = Env(value!);
        var ex = Assert.Throws<CredentialException>(() => CredentialResolver.Resolve(new CredentialSpec("env", Name: name)));
        Assert.Contains(name, ex.Message);
    }

    [Fact]
    public void Env_source_does_not_fall_back_to_the_legacy_variable_or_a_file()
    {
        // A set DARLING_EXPORT_PG_PASSWORD (today's no-config path) must not rescue a config whose own source is missing.
        var legacy = Environment.GetEnvironmentVariable(CredentialResolver.PasswordEnvVar);
        try
        {
            Environment.SetEnvironmentVariable(CredentialResolver.PasswordEnvVar, "legacy");
            var name = Env(null!);
            Assert.Throws<CredentialException>(() => CredentialResolver.Resolve(new CredentialSpec("env", Name: name)));
            Assert.Throws<CredentialException>(() =>
                CredentialResolver.Resolve(new CredentialSpec("file", Path: Path.Combine(Path.GetTempPath(), "missing-" + Guid.NewGuid()))));
        }
        finally
        {
            Environment.SetEnvironmentVariable(CredentialResolver.PasswordEnvVar, legacy);
        }
    }

    [Fact]
    public void File_source_trims_whitespace_and_newlines()
    {
        var path = TempFile("  pa ss\r\n");
        Assert.Equal("pa ss", CredentialResolver.Resolve(new CredentialSpec("file", Path: path), TextWriter.Null));
    }

    [Fact]
    public void File_source_missing_or_empty_is_a_credential_error()
    {
        Assert.Throws<CredentialException>(() =>
            CredentialResolver.Resolve(new CredentialSpec("file", Path: Path.Combine(Path.GetTempPath(), "nope-" + Guid.NewGuid()))));
        var empty = TempFile("  \n");
        Assert.Throws<CredentialException>(() => CredentialResolver.Resolve(new CredentialSpec("file", Path: empty), TextWriter.Null));
    }

    [Fact]
    public void File_source_warns_on_group_or_world_readable_and_stays_quiet_otherwise()
    {
        if (OperatingSystem.IsWindows()) return;

        var loose = TempFile("pw");
        File.SetUnixFileMode(loose, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead);
        var warn = new StringWriter();
        Assert.Equal("pw", CredentialResolver.Resolve(new CredentialSpec("file", Path: loose), warn));
        Assert.Contains("readable by group or others", warn.ToString());
        Assert.DoesNotContain("pw\n", warn.ToString().Replace(loose, ""));

        var tight = TempFile("pw");
        File.SetUnixFileMode(tight, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var quiet = new StringWriter();
        CredentialResolver.Resolve(new CredentialSpec("file", Path: tight), quiet);
        Assert.Equal("", quiet.ToString());
    }

    [Fact]
    public void Dpapi_file_is_refused_off_Windows()
    {
        if (OperatingSystem.IsWindows()) return;
        var ex = Assert.Throws<CredentialException>(() => CredentialResolver.Resolve(new CredentialSpec("dpapi_file", Path: TempFile("x"))));
        Assert.Contains("only be used on Windows", ex.Message);
    }

    [Fact]
    public void Connection_string_env_source_returns_the_variable_or_fails()
    {
        var name = Env("Host=h;Username=u;Password=p");
        Assert.Equal("Host=h;Username=u;Password=p", CredentialResolver.Resolve(new CredentialSpec("connection_string_env", Name: name)));
        var unset = Env(null!);
        var ex = Assert.Throws<CredentialException>(() => CredentialResolver.Resolve(new CredentialSpec("connection_string_env", Name: unset)));
        Assert.Contains("store.connection_string_env", ex.Message);
    }

    [Fact]
    public void An_error_message_never_contains_the_secret()
    {
        var name = Env("topsecretvalue");
        var ex = Assert.Throws<CredentialException>(() =>
            StoreConnectionFactory.BuildFromConnectionString(Options(), "Host=h;Bogus Key=1;Password=topsecretvalue"));
        Assert.DoesNotContain("topsecretvalue", ex.Message);
        _ = name;
    }

    private static ExportOptions Options(string? applicationName = "darling-export", int timeout = 120) => ExportOptions.Validate(
        "store_a", "2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z", "/tmp/out", "perfmon_stats",
        "127.0.0.1", 5641, "darling", "darling", null, timeout, false, true, new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc),
        applicationName: applicationName);

    [Fact]
    public void Connection_string_keeps_its_login_but_the_guards_are_forced()
    {
        // A connection string that tries every way to switch the guards off.
        const string hostile = "Host=db.example;Port=6543;Database=other;Username=login;Password=pw;" +
            "Options=-c default_transaction_read_only=off -c statement_timeout=0 -c lock_timeout=0;" +
            "Application Name=sneaky;Timeout=999;Command Timeout=0;Search Path=attacker";
        var b = StoreConnectionFactory.BuildFromConnectionString(Options(timeout: 90), hostile);

        Assert.Equal("db.example", b.Host);
        Assert.Equal(6543, b.Port);
        Assert.Equal("other", b.Database);
        Assert.Equal("login", b.Username);
        Assert.Equal("pw", b.Password);

        Assert.Equal("-c default_transaction_read_only=on -c statement_timeout=90000 -c lock_timeout=5000", b.Options);
        Assert.Equal("darling-export", b.ApplicationName);
        Assert.Equal(30, b.Timeout);
        Assert.Equal(90, b.CommandTimeout);

        // What Npgsql would actually send: the round trip through the string keeps the forced values.
        var reparsed = new NpgsqlConnectionStringBuilder(b.ConnectionString);
        Assert.Equal(b.Options, reparsed.Options);
        Assert.DoesNotContain("default_transaction_read_only=off", reparsed.ConnectionString);
    }

    [Fact]
    public void Connection_string_with_options_in_the_RuntimeParameters_form_cannot_override_the_startup_options()
    {
        var b = StoreConnectionFactory.BuildFromConnectionString(Options(), "Host=h;Username=u;Password=p;Options=-c default_transaction_read_only=off");
        Assert.Contains("default_transaction_read_only=on", b.Options);
        Assert.DoesNotContain("=off", b.Options);
    }

    [Fact]
    public void Garbage_connection_string_is_a_credential_error_that_does_not_echo_it()
    {
        var ex = Assert.Throws<CredentialException>(() => StoreConnectionFactory.BuildFromConnectionString(Options(), "not a connection string;;;=="));
        Assert.StartsWith("store.connection_string_env:", ex.Message);
        Assert.DoesNotContain("not a connection string", ex.Message);
    }

    [Fact]
    public void Build_uses_the_credential_spec_for_every_source()
    {
        var pw = Env("fromenv");
        var viaEnv = ExportOptionsWith(new CredentialSpec("env", Name: pw));
        Assert.Equal("fromenv", StoreConnectionFactory.Build(viaEnv).Password);

        var cs = Env("Host=csbox;Username=csuser;Password=cspw;Options=-c default_transaction_read_only=off");
        var viaCs = ExportOptionsWith(new CredentialSpec("connection_string_env", Name: cs));
        var b = StoreConnectionFactory.Build(viaCs);
        Assert.Equal("csbox", b.Host);
        Assert.Contains("default_transaction_read_only=on", b.Options);

        var file = TempFile("filepw\n");
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(file, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        Assert.Equal("filepw", StoreConnectionFactory.Build(ExportOptionsWith(new CredentialSpec("file", Path: file))).Password);
    }

    [Fact]
    public void Build_with_a_missing_credential_throws_and_does_not_use_the_legacy_login()
    {
        var legacy = Environment.GetEnvironmentVariable(CredentialResolver.PasswordEnvVar);
        try
        {
            Environment.SetEnvironmentVariable(CredentialResolver.PasswordEnvVar, "legacy");
            var unset = Env(null!);
            Assert.Throws<CredentialException>(() => StoreConnectionFactory.Build(ExportOptionsWith(new CredentialSpec("env", Name: unset))));
        }
        finally
        {
            Environment.SetEnvironmentVariable(CredentialResolver.PasswordEnvVar, legacy);
        }
    }

    [Fact]
    public void Without_a_config_the_legacy_env_var_still_supplies_the_password_and_the_default_application_name()
    {
        var legacy = Environment.GetEnvironmentVariable(CredentialResolver.PasswordEnvVar);
        try
        {
            Environment.SetEnvironmentVariable(CredentialResolver.PasswordEnvVar, "legacy");
            var b = StoreConnectionFactory.Build(Options(applicationName: null));
            Assert.Equal("legacy", b.Password);
            Assert.Equal("darling-export", b.ApplicationName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(CredentialResolver.PasswordEnvVar, legacy);
        }
    }

    private static ExportOptions ExportOptionsWith(CredentialSpec spec) => ExportOptions.Validate(
        "store_a", "2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z", "/tmp/out", "perfmon_stats",
        "127.0.0.1", 5641, "darling", "darling", null, 120, false, true, new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc),
        applicationName: "darling-export", credential: spec);
}
