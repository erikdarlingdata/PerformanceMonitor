/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;
using Host = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService;
using WebHost = PerformanceMonitor.Darling.Service.Mcp.DarlingWebHostService;

namespace Darling.Tests;

/// <summary>
/// The <c>--test-connection</c>/<c>--validate-config</c> CLI verb. The connectivity probe itself needs a live
/// SQL Server, but the verb recognition and the pure per-server PASS/FAIL formatting are unit-tested here; the
/// probe is shared with the <c>test_connect</c> command (<see cref="DarlingServerConnector.ProbeAsync"/>), so
/// what validates from the CLI connects identically under the running service.
/// </summary>
public sealed class DarlingCliCommandsTests
{
    [Theory]
    [InlineData("--test-connection", true)]
    [InlineData("--validate-config", true)]
    [InlineData("--TEST-CONNECTION", true)]
    [InlineData("--encrypt-password", false)]
    [InlineData("--nonsense", false)]
    public void IsValidateConfigVerb_RecognizesBothAliases_CaseInsensitive(string arg, bool expected)
    {
        Assert.Equal(expected, DarlingCliCommands.IsValidateConfigVerb(arg));
    }

    [Fact]
    public void FormatProbeLine_Success_ShowsVersionEditionAndMsdb()
    {
        var probe = new ConnectionProbeResult(
            Success: true, MajorVersion: 16, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null);

        var line = DarlingCliCommands.FormatProbeLine("SQL01", probe);

        Assert.Contains("[PASS]", line, StringComparison.Ordinal);
        Assert.Contains("SQL01", line, StringComparison.Ordinal);
        Assert.Contains("SQL major version 16", line, StringComparison.Ordinal);
        Assert.Contains("Enterprise", line, StringComparison.Ordinal);
        Assert.Contains("msdb access: yes", line, StringComparison.Ordinal);
    }

    [Fact]
    public void FormatProbeLine_Success_NoMsdb_WarnsFailedJobsUnavailable()
    {
        var probe = new ConnectionProbeResult(
            Success: true, MajorVersion: 15, EngineEdition: 2, EngineEditionDescription: "Standard",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: false, Error: null);

        var line = DarlingCliCommands.FormatProbeLine("SQL02", probe);

        Assert.Contains("[PASS]", line, StringComparison.Ordinal);
        Assert.Contains("msdb access: NO", line, StringComparison.Ordinal);
    }

    [Fact]
    public void FormatProbeLine_MissingDescription_FallsBackToEditionDescriber()
    {
        var probe = new ConnectionProbeResult(
            Success: true, MajorVersion: 16, EngineEdition: 8, EngineEditionDescription: null,
            IsAzureSqlDb: false, IsAzureManagedInstance: true, IsAwsRds: false, HasMsdbAccess: true, Error: null);

        var line = DarlingCliCommands.FormatProbeLine("MI01", probe);

        /* Edition 8 -> Managed Instance, resolved by the shared describer when the probe carries no text. */
        Assert.Contains("Azure SQL Managed Instance", line, StringComparison.Ordinal);
    }

    [Fact]
    public void FormatProbeLine_Failure_ShowsError()
    {
        var probe = new ConnectionProbeResult(
            Success: false, MajorVersion: 0, EngineEdition: 0, EngineEditionDescription: null,
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: false,
            Error: "Login failed for user 'monitor'.");

        var line = DarlingCliCommands.FormatProbeLine("SQL03", probe);

        Assert.Contains("[FAIL]", line, StringComparison.Ordinal);
        Assert.Contains("SQL03", line, StringComparison.Ordinal);
        Assert.Contains("Login failed", line, StringComparison.Ordinal);
    }

    /// <summary>
    /// A PostgreSQL target has no SQL major version, no engine edition and no msdb, so the line must not
    /// claim any of them. Before the engine branch existed this printed "SQL major version 0,
    /// Unknown (0), msdb access: yes" for a perfectly healthy Aurora cluster — a PASS that reads like a
    /// misconfiguration, on the one verb whose whole job is to be trusted as a deployment gate.
    /// </summary>
    [Fact]
    public void FormatProbeLine_PostgresTarget_ReportsPostgresFactsAndNoSqlServerOnes()
    {
        var line = DarlingCliCommands.FormatProbeLine("aurora-writer", PostgresProbe());

        Assert.Contains("[PASS]", line, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL 17", line, StringComparison.Ordinal);
        Assert.Contains("170007", line, StringComparison.Ordinal);
        Assert.Contains("writer", line, StringComparison.Ordinal);
        Assert.Contains("Aurora", line, StringComparison.Ordinal);

        Assert.DoesNotContain("SQL major version", line, StringComparison.Ordinal);
        Assert.DoesNotContain("msdb", line, StringComparison.Ordinal);
        Assert.DoesNotContain("Unknown (0)", line, StringComparison.Ordinal);
    }

    /// <summary>An Aurora writer clears every gate, so the count says so rather than listing nothing.</summary>
    [Fact]
    public void FormatProbeLine_AuroraWriter_ReportsEveryPostgresCollectorApplies()
    {
        var expected = CollectorCatalog.All.Count(d => d.TargetEngine == CollectorTargetEngine.PostgreSql);

        var line = DarlingCliCommands.FormatProbeLine("aurora-writer", PostgresProbe());

        Assert.Contains($"all {expected} PostgreSQL collectors apply", line, StringComparison.Ordinal);
        Assert.DoesNotContain("skipped", line, StringComparison.Ordinal);
    }

    /// <summary>
    /// The case the count exists for. A stock-PostgreSQL 15 reader is the worst realistic target: no
    /// Aurora wait instrumentation, no pg_stat_io, and autovacuum stats that read as all zeros on a
    /// standby. Finding that out at pre-flight is the difference between "this is configured" and "this
    /// will collect" — which is why the negative assertion below matters as much as the positive ones.
    /// </summary>
    [Fact]
    public void FormatProbeLine_StockPostgresReader_NamesTheCollectorsThatWillNotRun()
    {
        var probe = PostgresProbe() with
        {
            PostgresMajorVersion = 15,
            PostgresVersionNum = 150012,
            IsAurora = false,
            IsInRecovery = true,
        };

        var line = DarlingCliCommands.FormatProbeLine("selfhosted-replica", probe);

        Assert.Contains("reader (in recovery)", line, StringComparison.Ordinal);
        Assert.Contains("not Aurora", line, StringComparison.Ordinal);

        /* The Aurora-only one, the writer-only one and the 16+ one — each named, so nobody has to
           reverse-engineer an empty table later. */
        Assert.Contains("skipped:", line, StringComparison.Ordinal);
        Assert.Contains("pg_wait_stats", line, StringComparison.Ordinal);
        Assert.Contains("pg_autovacuum_stats", line, StringComparison.Ordinal);
        Assert.Contains("pg_io_stats", line, StringComparison.Ordinal);

        /* pg_statement_stats was in that list until #2625 gave it a vanilla pg_stat_statements path. It
           must NOT be skipped here — this pre-flight line is where an operator learns what a target will
           and will not collect, and listing a collector that now runs would tell them to stop expecting
           the one answer this change exists to deliver. */
        Assert.DoesNotContain("pg_statement_stats", line, StringComparison.Ordinal);
    }

    /// <summary>
    /// The count is derived from the real gate, not a parallel list that can rot. Asking the catalog the
    /// same question the runner asks must give the same answer.
    /// </summary>
    [Fact]
    public void ToTargetInfo_RoundTripsTheFactsTheGateReads()
    {
        var target = PostgresProbe().ToTargetInfo();

        Assert.Equal(CollectorTargetEngine.PostgreSql, target.Engine);
        Assert.Equal(17, target.PostgresMajorVersion);
        Assert.Equal(170007, target.PostgresVersionNum);
        Assert.True(target.IsAurora);
        Assert.False(target.IsInRecovery);

        Assert.All(
            CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer),
            d => Assert.False(CollectorCatalog.AppliesTo(d, target)));
    }

    private static ConnectionProbeResult PostgresProbe() => new(
        Success: true, MajorVersion: 0, EngineEdition: 0, EngineEditionDescription: null,
        IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null,
        Engine: CollectorTargetEngine.PostgreSql, PostgresMajorVersion: 17, PostgresVersionNum: 170007,
        IsAurora: true, IsInRecovery: false);

    [Fact]
    public void DescribeEngineEdition_MapsKnownEditions()
    {
        Assert.Equal("Enterprise", DarlingServerConnector.DescribeEngineEdition(3));
        Assert.Equal("Azure SQL Database", DarlingServerConnector.DescribeEngineEdition(5));
        Assert.Equal("Azure SQL Managed Instance", DarlingServerConnector.DescribeEngineEdition(8));
        Assert.Contains("Unknown", DarlingServerConnector.DescribeEngineEdition(999), StringComparison.Ordinal);
    }

    /* ---- the collapse verb's adaptive narrowing decision (#2105 round three) — pure pins ---- */

    private static readonly TimeSpan Day = TimeSpan.FromDays(1);

    [Fact]
    public void NextNarrowingFailureCount_FullWidthSlice_TakesTheFirstHalvingStep()
    {
        /* A failed 24h slice narrows to 12h — one more failure than before. */
        Assert.Equal(1, DarlingCliCommands.NextNarrowingFailureCount(Day, 0, Day));
        /* And a 12h slice that fails again narrows to 6h. */
        Assert.Equal(2, DarlingCliCommands.NextNarrowingFailureCount(Day, 1, TimeSpan.FromHours(12)));
    }

    [Fact]
    public void NextNarrowingFailureCount_ClampedTail_SkipsStepsThatWouldRerunTheSameWindow()
    {
        /* The review catch: a clamped 30-minute final slice is already narrower than the 12h/6h/3h/1.5h/45m
           nominal steps — re-running any of them is the identical window. The first step that actually
           narrows 30m is the 22.5m floor (failure count 6). */
        Assert.Equal(6, DarlingCliCommands.NextNarrowingFailureCount(Day, 0, TimeSpan.FromMinutes(30)));
    }

    [Fact]
    public void NextNarrowingFailureCount_AtOrBelowTheFloor_ReturnsNull_TheSameWidthRetryTakesOver()
    {
        /* The 24h schedule floors at 22.5m (6 halvings). A slice at or under that width cannot be
           narrowed — the caller's one fresh-connection same-width retry is the only move left, and it
           must NOT be skipped just because narrowing is impossible (the run's usual last slice is a
           partial-day clamp of arbitrary width). */
        Assert.Null(DarlingCliCommands.NextNarrowingFailureCount(Day, 0, TimeSpan.FromMinutes(22.5)));
        Assert.Null(DarlingCliCommands.NextNarrowingFailureCount(Day, 0, TimeSpan.FromMinutes(5)));
        Assert.Null(DarlingCliCommands.NextNarrowingFailureCount(Day, 6, TimeSpan.FromMinutes(22.5)));
    }
}

/// <summary>
/// The #1581 startup-argument classification (Fix B): the pure <see cref="DarlingCliCommands.ClassifyStartupArgs"/>
/// decision and the verb-recognition helpers it composes. The incident was <c>Service.exe --version</c> falling
/// through into a real service startup and spawning a second instance — so the contract these pin is: only no-arg
/// or a RECOGNIZED verb reaches the host; <c>--version</c>/<c>--help</c> print + exit; ANYTHING else is an unknown
/// option that must NOT start the host.
/// </summary>
public sealed class DarlingStartupArgsTests
{
    [Theory]
    [InlineData("--version", true)]
    [InlineData("-v", true)]
    [InlineData("--VERSION", true)]
    [InlineData("-V", true)]
    [InlineData("--help", false)]
    [InlineData("--nonsense", false)]
    public void IsVersionVerb_RecognizesVersionFlags_CaseInsensitive(string arg, bool expected) =>
        Assert.Equal(expected, DarlingCliCommands.IsVersionVerb(arg));

    [Theory]
    [InlineData("--help", true)]
    [InlineData("-h", true)]
    [InlineData("-?", true)]
    [InlineData("/?", true)]
    [InlineData("--HELP", true)]
    [InlineData("--version", false)]
    [InlineData("--nonsense", false)]
    public void IsHelpVerb_RecognizesHelpFlags_CaseInsensitive(string arg, bool expected) =>
        Assert.Equal(expected, DarlingCliCommands.IsHelpVerb(arg));

    [Theory]
    [InlineData("--encrypt-password", true)]
    [InlineData("--test-connection", true)]
    [InlineData("--validate-config", true)]
    [InlineData("--print-viewer-connection", true)]
    [InlineData("--export-viewer-config", true)]
    [InlineData("--configure-network", true)]
    [InlineData("--backfill-rollups", true)]
    [InlineData("--collapse-legacy-slices", true)]
    [InlineData("--recompress-plan-dim", true)]
    [InlineData("--version", false)]   // its own classification, not a "known verb"
    [InlineData("--help", false)]
    [InlineData("--nonsense", false)]
    public void IsKnownVerb_CoversEveryDispatchedVerb(string arg, bool expected) =>
        Assert.Equal(expected, DarlingCliCommands.IsKnownVerb(arg));

    /// <summary>
    /// The drift this guards against SHIPPED (#1912's field deploy): --collapse-legacy-slices had a full
    /// dispatch block in Program.cs, its help text listed it, and IsKnownVerb never learned it - so the
    /// #1581 startup classifier bounced the verb to "Unknown option" and the dispatch was unreachable. The
    /// verb's own live tests could not see it because they call DarlingCliCommands directly, never the
    /// Program.Main seam. Every Is*Verb classifier on the class must therefore be REACHABLE through
    /// IsKnownVerb - a new verb whose author forgets the allow-list fails here by name, not in the field.
    /// </summary>
    [Fact]
    public void IsKnownVerb_ReachesEveryVerbClassifierOnTheClass()
    {
        var classifiers = typeof(DarlingCliCommands)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(m => m.Name.StartsWith("Is", StringComparison.Ordinal)
                && m.Name.EndsWith("Verb", StringComparison.Ordinal)
                && m.Name != "IsKnownVerb"
                && m.ReturnType == typeof(bool))
            .ToList();
        Assert.True(classifiers.Count >= 12, $"expected the full classifier family, found {classifiers.Count}");

        /* Version/help classify separately by design (StartupAction handles them before the verb dispatch). */
        var separatelyClassified = new[] { "IsVersionVerb", "IsHelpVerb" };

        foreach (var classifier in classifiers.Where(c => !separatelyClassified.Contains(c.Name)))
        {
            var verb = VerbLiteralFor(classifier.Name);
            Assert.True((bool)classifier.Invoke(null, new object[] { verb })!,
                $"{classifier.Name} does not accept its own literal '{verb}' - fix VerbLiteralFor");
            Assert.True(DarlingCliCommands.IsKnownVerb(verb),
                $"IsKnownVerb does not reach {classifier.Name}'s verb '{verb}' - the Program.cs dispatch for it is UNREACHABLE");
        }
    }

    /// <summary>The CLI literal for a classifier name: IsEncryptPasswordVerb -> --encrypt-password.</summary>
    private static string VerbLiteralFor(string classifierName)
    {
        var core = classifierName.Substring(2, classifierName.Length - 2 - 4);
        var sb = new System.Text.StringBuilder("--");
        for (var i = 0; i < core.Length; i++)
        {
            if (char.IsUpper(core[i]) && i > 0)
            {
                sb.Append('-');
            }
            sb.Append(char.ToLowerInvariant(core[i]));
        }
        return sb.ToString();
    }

    [Fact]
    public void ClassifyStartupArgs_NoArgs_StartsHost()
    {
        Assert.Equal(StartupAction.StartHost, DarlingCliCommands.ClassifyStartupArgs(Array.Empty<string>()));
        Assert.Equal(StartupAction.StartHost, DarlingCliCommands.ClassifyStartupArgs(null));
    }

    [Theory]
    [InlineData("--version", StartupAction.PrintVersion)]
    [InlineData("-v", StartupAction.PrintVersion)]
    [InlineData("--help", StartupAction.PrintHelp)]
    [InlineData("-h", StartupAction.PrintHelp)]
    [InlineData("--encrypt-password", StartupAction.RunKnownVerb)]
    [InlineData("--test-connection", StartupAction.RunKnownVerb)]
    [InlineData("--configure-network", StartupAction.RunKnownVerb)]
    [InlineData("--export-viewer-config", StartupAction.RunKnownVerb)]
    [InlineData("--version-bogus", StartupAction.UnknownOption)]
    [InlineData("--nonsense", StartupAction.UnknownOption)]
    [InlineData("/install", StartupAction.UnknownOption)]
    public void ClassifyStartupArgs_ClassifiesFirstArg(string arg, StartupAction expected) =>
        Assert.Equal(expected, DarlingCliCommands.ClassifyStartupArgs(new[] { arg }));

    [Fact]
    public void ClassifyStartupArgs_UsesOnlyTheFirstArg()
    {
        /* Extra args after a recognized first arg do not change the classification. */
        Assert.Equal(StartupAction.PrintVersion, DarlingCliCommands.ClassifyStartupArgs(new[] { "--version", "extra" }));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { "--test-connection", "cfg.json" }));
    }

    [Fact]
    public void ProductVersion_IsNonEmpty_AndStripsBuildMetadata()
    {
        var version = DarlingCliCommands.ProductVersion();
        Assert.False(string.IsNullOrWhiteSpace(version));
        /* Any SemVer +build metadata is stripped for a clean --version line. */
        Assert.DoesNotContain('+', version);
        /* The leading component parses as a version (e.g. "3.1.0"). */
        Assert.NotNull(System.Version.Parse(version.Split('-', '+')[0]));
    }

    [Fact]
    public void UsageText_ListsTheKeyVerbs_AndIsAscii()
    {
        var usage = DarlingCliCommands.UsageText();
        Assert.Contains("--version", usage, StringComparison.Ordinal);
        Assert.Contains("--help", usage, StringComparison.Ordinal);
        Assert.Contains("--test-connection", usage, StringComparison.Ordinal);
        Assert.Contains("--encrypt-password", usage, StringComparison.Ordinal);
        Assert.Contains("--configure-network", usage, StringComparison.Ordinal);
        Assert.All(usage, ch => Assert.True(ch < 128, $"usage text must be ASCII; found U+{(int)ch:X4}"));
    }
}

/// <summary>
/// The <c>--print-viewer-connection</c> verb (darling-network-endpoints D8): the pure connection-string /
/// host builders (unit-testable without DPAPI or a store), plus a Windows-gated end-to-end that decrypts a
/// temp <c>viewer</c> credential + emits the cert, asserting the paste-ready shape and the live-secret warning.
/// </summary>
public sealed class DarlingPrintViewerConnectionTests
{
    [Theory]
    [InlineData("--print-viewer-connection", true)]
    [InlineData("--PRINT-VIEWER-CONNECTION", true)]
    [InlineData("--validate-config", false)]
    [InlineData("--encrypt-password", false)]
    [InlineData("--nonsense", false)]
    public void IsPrintViewerConnectionVerb_RecognizesTheVerb_CaseInsensitive(string arg, bool expected)
    {
        Assert.Equal(expected, DarlingCliCommands.IsPrintViewerConnectionVerb(arg));
    }

    [Fact]
    public void BuildViewerConnectionString_CarriesSearchPath_VerifyFull_RootCert_Role_HostAndPort()
    {
        var cs = DarlingCliCommands.BuildViewerConnectionString(
            "192.168.1.205", 5641, "viewer", "s3cretPW", "server.crt");

        Assert.Contains("Host=192.168.1.205", cs, StringComparison.Ordinal);
        Assert.Contains("Port=5641", cs, StringComparison.Ordinal);
        Assert.Contains("Username=viewer", cs, StringComparison.Ordinal);
        Assert.Contains("Password=s3cretPW", cs, StringComparison.Ordinal);
        Assert.Contains("Database=darling", cs, StringComparison.Ordinal);
        Assert.Contains("Search Path=collect,config,public", cs, StringComparison.Ordinal);
        Assert.Contains("SSL Mode=VerifyFull", cs, StringComparison.Ordinal);
        Assert.Contains("Root Certificate=server.crt", cs, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildViewerConnectionString_NamesTheSelectedRole()
    {
        /* role:admin flows through to Username= so the admin opt-in prints an admin connection. */
        var cs = DarlingCliCommands.BuildViewerConnectionString("host", 1, "admin", "pw", "c.crt");
        Assert.Contains("Username=admin", cs, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolveViewerHost_ConcreteIp_ReturnsTheIp()
    {
        /* A concrete bind IP is used verbatim — verify-full validates it against the cert's iPAddress SAN. */
        Assert.Equal("192.168.1.205", DarlingCliCommands.ResolveViewerHost("192.168.1.205"));
        Assert.Equal("10.0.0.7", DarlingCliCommands.ResolveViewerHost("  10.0.0.7  "));
    }

    [Theory]
    [InlineData("0.0.0.0")]     // IPv4 wildcard — can't be dialed
    [InlineData("::")]          // IPv6 wildcard
    [InlineData("127.0.0.1")]   // loopback
    [InlineData("localhost")]   // not an IP
    [InlineData("")]            // unset
    [InlineData(null)]
    public void ResolveViewerHost_WildcardLoopbackOrHostname_FallsBackToTheMachineDnsSan(string? listen)
    {
        /* The fallback is the machine hostname, which the cert carries as a dnsName SAN. */
        Assert.Equal(Environment.MachineName, DarlingCliCommands.ResolveViewerHost(listen));
    }

    [Fact]
    public async Task PrintViewerConnectionAsync_ByoMode_ReturnsError_WithoutTouchingDpapi()
    {
        var root = Directory.CreateTempSubdirectory("darling-printconn-byo-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath,
                """{ "postgres": { "connectionString": "Host=localhost;Database=darling" } }""");

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.PrintViewerConnectionAsync(configPath, output, error, CancellationToken.None);

            Assert.Equal(1, exit);
            Assert.Contains("bring-your-own", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal("", output.ToString());
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task PrintViewerConnectionAsync_InvalidRole_ReturnsError()
    {
        var root = Directory.CreateTempSubdirectory("darling-printconn-role-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var configPath = Path.Combine(root.FullName, "darling.json");
            var json = $$"""
                {
                  "postgres": {
                    "managed": true,
                    "port": 5641,
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}},
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "role": "superadmin" }
                  }
                }
                """;
            await File.WriteAllTextAsync(configPath, json);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.PrintViewerConnectionAsync(configPath, output, error, CancellationToken.None);

            Assert.Equal(1, exit);
            Assert.Contains("role", error.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task PrintViewerConnectionAsync_ManagedViewer_PrintsConnection_Cert_AndSecretWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-printconn-");
        try
        {
            /* Lay down the managed layout the verb reads on the store host: the viewer role's DPAPI credential
               and the generated server cert, both beside the data directory. */
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var viewerCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);
            File.WriteAllText(viewerCredential, PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("viewer-secret-pw"));

            var certPath = Path.Combine(
                Path.GetDirectoryName(viewerCredential)!,
                PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ServerCertFileName);
            const string pem = "-----BEGIN CERTIFICATE-----\nMIIBTESTCERTPEM\n-----END CERTIFICATE-----";
            File.WriteAllText(certPath, pem);

            var configPath = Path.Combine(root.FullName, "darling.json");
            var json = $$"""
                {
                  "postgres": {
                    "managed": true,
                    "port": 5641,
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}},
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "role": "viewer" }
                  },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """;
            await File.WriteAllTextAsync(configPath, json);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.PrintViewerConnectionAsync(configPath, output, error, CancellationToken.None);
            var stdout = output.ToString();
            var stderr = error.ToString();

            Assert.Equal(0, exit);

            /* The paste-ready connection string on STDOUT: verify-full, the search path, the client cert path,
               the viewer role, the decrypted password, and Host=the IP (Round 4 #12). */
            Assert.Contains("Host=192.168.1.205", stdout, StringComparison.Ordinal);
            Assert.Contains("Username=viewer", stdout, StringComparison.Ordinal);
            Assert.Contains("Password=viewer-secret-pw", stdout, StringComparison.Ordinal);
            Assert.Contains("Search Path=collect,config,public", stdout, StringComparison.Ordinal);
            Assert.Contains("SSL Mode=VerifyFull", stdout, StringComparison.Ordinal);
            Assert.Contains("Root Certificate=server.crt", stdout, StringComparison.Ordinal);

            /* The server cert PEM is emitted so the operator can place it on the client. */
            Assert.Contains(pem, stdout, StringComparison.Ordinal);

            /* The live-secret warning is printed (to STDERR, so a STDOUT redirect keeps it visible). */
            Assert.Contains("LIVE database password", stderr, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// #2117's print-verb half, pinned on the CHAIN-shaped store the sibling test cannot see (it lays down
    /// only the legacy server.crt): when root.crt exists beside server.crt, the verb must emit the ROOT —
    /// that is what verify-full's Root Certificate anchors on against a chain-serving store — and the
    /// header must name the file whose content is actually below (the review-caught label lie: it said
    /// server.crt over root.crt's bytes).
    /// </summary>
    [Fact]
    public async Task PrintViewerConnectionAsync_ChainShapedStore_EmitsTheRoot_AndLabelsItHonestly()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-printconn-chain-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var viewerCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);
            File.WriteAllText(viewerCredential, PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("viewer-secret-pw"));

            var certPath = Path.Combine(
                Path.GetDirectoryName(viewerCredential)!,
                PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ServerCertFileName);
            const string leafPem = "-----BEGIN CERTIFICATE-----\nMIIBLEAFCHAINPEM\n-----END CERTIFICATE-----";
            const string rootPem = "-----BEGIN CERTIFICATE-----\nMIIBROOTCAPEM\n-----END CERTIFICATE-----";
            File.WriteAllText(certPath, leafPem);
            File.WriteAllText(
                PerformanceMonitor.Darling.Service.DarlingManagedPostgres.RootCertificatePathFor(certPath), rootPem);

            var configPath = Path.Combine(root.FullName, "darling.json");
            var json = $$"""
                {
                  "postgres": {
                    "managed": true,
                    "port": 5641,
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}},
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "role": "viewer" }
                  },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """;
            await File.WriteAllTextAsync(configPath, json);

            var output = new StringWriter();
            var exit = await DarlingCliCommands.PrintViewerConnectionAsync(configPath, output, new StringWriter(), CancellationToken.None);
            var stdout = output.ToString();

            Assert.Equal(0, exit);

            /* The ROOT's content, labeled as root.crt — never the leaf chain the server serves. */
            Assert.Contains(rootPem, stdout, StringComparison.Ordinal);
            Assert.DoesNotContain(leafPem, stdout, StringComparison.Ordinal);
            Assert.Contains("(root.crt)", stdout, StringComparison.Ordinal);

            /* The client-side FILE name stays server.crt (ViewerClientCertificateFileName) on purpose —
               the save-as path in the connection string does not change with the store's shape. */
            Assert.Contains("Root Certificate=server.crt", stdout, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }
}

/// <summary>
/// The <c>--configure-network</c> wizard (#1561): pure verb recognition (unconditional), plus scripted-input
/// end-to-end runs (Windows-gated, like the sibling <c>--print-viewer-connection</c> E2E, because the wizard
/// generates a DPAPI token and queries the Windows service). Every run drives the whole flow with a
/// <see cref="StringReader"/> and asserts the delegated-validation contract: the edit PARSES, the REAL
/// resolvers accept it, a timestamped backup is created, and the generated token is printed to STDOUT
/// exactly once with the save-this warning on STDERR. The comment-surgery internals are pinned separately by
/// <see cref="DarlingNetworkConfigEditorTests"/>.
/// </summary>
public sealed class DarlingConfigureNetworkTests
{
    private const string CertPath = @"C:\ProgramData\PerformanceMonitorDarling\server.crt";
    private const string KeyPath = @"C:\ProgramData\PerformanceMonitorDarling\server.key";

    [Theory]
    [InlineData("--configure-network", true)]
    [InlineData("--CONFIGURE-NETWORK", true)]
    [InlineData("--print-viewer-connection", false)]
    [InlineData("--validate-config", false)]
    [InlineData("--nonsense", false)]
    public void IsConfigureNetworkVerb_RecognizesTheVerb_CaseInsensitive(string arg, bool expected)
    {
        Assert.Equal(expected, DarlingCliCommands.IsConfigureNetworkVerb(arg));
    }

    [Fact]
    public async Task ConfigureNetwork_Store_WritesBlock_MakesBackup_ParsesAndResolverExposed()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard queries the Windows service + uses DPAPI.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-store-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* choice=Store, bind IP typed directly, CIDR, role, then decline restart. */
            var input = Script("1", "192.168.1.205", "192.168.1.0/24", "viewer", "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            /* The written file parses and the STORE RESOLVER reports it exposed with the entered values. */
            var written = await File.ReadAllTextAsync(configPath);
            var config = DarlingConfig.Parse(written);
            var decision = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath);
            Assert.True(decision.Exposed);
            Assert.Equal("192.168.1.205", decision.ListenIp);
            Assert.Equal("192.168.1.0/24", decision.Cidr);
            Assert.Equal("viewer", decision.Role);

            /* A timestamped backup exists, and the commented template survived the edit. */
            Assert.NotEmpty(Directory.GetFiles(root.FullName, "darling.json.bak-*"));
            Assert.Contains("// \"network\": {", written, StringComparison.Ordinal);
            Assert.Contains("Backup saved", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// #2097 (gotqn): in the PowerShell ISE / remote sessions / redirected stdin, ReadLine() returns null
    /// immediately and stderr is not surfaced — so the wizard "bailed" with no visible reason. EOF at the
    /// menu must be told apart from an explicit quit: it writes the non-interactive guidance to STDOUT
    /// (the one stream every host shows) and exits nonzero so scripts notice. An explicit 'q' keeps the
    /// quiet "No changes made." + 0 contract.
    /// </summary>
    [Fact]
    public async Task ConfigureNetwork_EofAtMenu_ExplainsNonInteractiveConsole_OnStdout()
    {
        var root = Directory.CreateTempSubdirectory("darling-confignet-eof-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* An exhausted reader IS the ISE shape: first ReadLine returns null. */
            var input = new StringReader(string.Empty);
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);

            Assert.Equal(1, exit);
            Assert.Contains("non-interactive", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Read-Host", output.ToString(), StringComparison.Ordinal);

            /* And an explicit quit is still the quiet success it always was. */
            var quitOutput = new StringWriter();
            var quitExit = await DarlingCliCommands.ConfigureNetworkAsync(
                configPath, Script("q"), quitOutput, new StringWriter(), CancellationToken.None);
            Assert.Equal(0, quitExit);
            Assert.Contains("No changes made.", quitOutput.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("non-interactive", quitOutput.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_Mcp_GeneratesToken_PrintsPlaintextOnce_WarnsOnStderr_StoresEncrypted()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard generates a DPAPI-protected token.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-mcp-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* choice=MCP, bind IP, CIDR, decline restart. No existing token -> one is generated. */
            var input = Script("2", "192.168.1.205", "192.168.1.0/24", "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            var written = await File.ReadAllTextAsync(configPath);
            var config = DarlingConfig.Parse(written);
            Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(config.Mcp, managed: true).Mode);

            /* Stored only DPAPI-encrypted; the plaintext it decrypts to is the token printed to STDOUT. */
            var encrypted = config.Mcp.Network!.EncryptedToken;
            Assert.False(string.IsNullOrWhiteSpace(encrypted));
            var plaintext = DarlingSecrets.Unprotect(encrypted!);

            Assert.Equal(1, CountOccurrences(output.ToString(), plaintext));   // STDOUT: exactly once
            Assert.Contains("SAVE THIS NOW", error.ToString(), StringComparison.Ordinal); // STDERR: the warning
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_Web_GeneratesToken_PrintsPlaintextOnce_StoresEncrypted_HintsLoginUrl()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard generates a DPAPI-protected token.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-web-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* choice=Web, bind IP, CIDR, decline restart. No existing token -> one is generated. */
            var input = Script("3", "192.168.1.205", "192.168.1.0/24", "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            /* The written file parses and the WEB BIND RESOLVER (the one the web host fail-closes on)
               reports it network-exposed — the exact acceptance for #1617's hand-edit incident. */
            var written = await File.ReadAllTextAsync(configPath);
            var config = DarlingConfig.Parse(written);
            var decision = WebHost.ResolveWebBind(config.Web, managed: true);
            Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, decision.Mode);
            Assert.Equal("192.168.1.205", config.Web.Network!.Listen);
            Assert.Equal("192.168.1.0/24", config.Web.Network!.AllowFrom);

            /* Stored only DPAPI-encrypted; the plaintext it decrypts to is the token printed to STDOUT. */
            var encrypted = config.Web.Network!.EncryptedToken;
            Assert.False(string.IsNullOrWhiteSpace(encrypted));
            var plaintext = DarlingSecrets.Unprotect(encrypted!);

            Assert.Equal(1, CountOccurrences(output.ToString(), plaintext));   // STDOUT: exactly once
            Assert.Contains("SAVE THIS NOW", error.ToString(), StringComparison.Ordinal); // STDERR: the warning

            /* The next-steps handoff includes the browser login hint — the one step Web does differently. */
            Assert.Contains("http://192.168.1.205:5153/?token=", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_Web_ExistingToken_KeptByDefault()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard queries the Windows service + uses DPAPI.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-web-keep-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, """
                {
                  "postgres": { "managed": true },
                  "web": {
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "encryptedToken": "KEEP-ME-BLOB" }
                  },
                  "servers": [ { "host": "S" } ]
                }
                """);

            /* choice=Web, keep-token default (empty line), bind IP, CIDR, decline restart. */
            var input = Script("3", "", "10.0.0.5", "10.0.0.0/24", "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            /* The existing DPAPI blob survives; new listen/allowFrom land; no plaintext was generated. */
            var config = DarlingConfig.Parse(await File.ReadAllTextAsync(configPath));
            Assert.Equal("KEEP-ME-BLOB", config.Web.Network!.EncryptedToken);
            Assert.Equal("10.0.0.5", config.Web.Network!.Listen);
            Assert.DoesNotContain("SAVE THIS NOW", error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_CommaCombination_McpAndWeb_WritesBothBlocks()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard generates DPAPI-protected tokens.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-combo-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* choice="2,3": MCP inputs first (bind, CIDR), then web (bind, CIDR), decline restart.
               Both surfaces generate fresh tokens -> two SAVE THIS warnings, two STDOUT plaintexts. */
            var input = Script("2,3", "192.168.1.205", "192.168.1.0/24", "192.168.1.205", "192.168.1.0/24", "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            var config = DarlingConfig.Parse(await File.ReadAllTextAsync(configPath));
            Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(config.Mcp, managed: true).Mode);
            Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, WebHost.ResolveWebBind(config.Web, managed: true).Mode);

            /* The store block was NOT touched (surface selection is exact). */
            Assert.False(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);

            /* Two distinct tokens, each printed exactly once. */
            var mcpPlain = DarlingSecrets.Unprotect(config.Mcp.Network!.EncryptedToken!);
            var webPlain = DarlingSecrets.Unprotect(config.Web.Network!.EncryptedToken!);
            Assert.NotEqual(mcpPlain, webPlain);
            Assert.Equal(1, CountOccurrences(output.ToString(), mcpPlain));
            Assert.Equal(1, CountOccurrences(output.ToString(), webPlain));
            Assert.Equal(2, CountOccurrences(error.ToString(), "SAVE THIS NOW"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_All_WritesAllThreeBlocks_EachResolverExposed()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard generates DPAPI-protected tokens.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-all-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* choice="4" (all three): store inputs first (bind, CIDR, role), then MCP (bind, CIDR),
               then web (bind, CIDR), decline restart — the three-upsert + three-guard path in one run. */
            var input = Script("4",
                "192.168.1.205", "192.168.1.0/24", "viewer",
                "192.168.1.205", "192.168.1.0/24",
                "192.168.1.205", "192.168.1.0/24",
                "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            var config = DarlingConfig.Parse(await File.ReadAllTextAsync(configPath));
            Assert.True(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);
            Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(config.Mcp, managed: true).Mode);
            Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, WebHost.ResolveWebBind(config.Web, managed: true).Mode);

            /* All three next-steps blocks made it out (store handoff, MCP + web firewall commands, login URL). */
            var stdout = output.ToString();
            Assert.Contains("--print-viewer-connection", stdout, StringComparison.Ordinal);
            Assert.Contains("PerformanceMonitor Darling MCP (port", stdout, StringComparison.Ordinal);
            Assert.Contains("PerformanceMonitor Darling Web (port", stdout, StringComparison.Ordinal);
            Assert.Contains("?token=", stdout, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData("1", true, false, false)]
    [InlineData("2", false, true, false)]
    [InlineData("3", false, false, true)]
    [InlineData("4", true, true, true)]
    [InlineData("1,3", true, false, true)]
    [InlineData("2 , 3", false, true, true)] // tokens are trimmed
    [InlineData("1,2,3", true, true, true)]
    [InlineData("1,4", true, true, true)] // 4 dominates
    public void TryParseSurfaceChoice_ValidSelections(string choice, bool store, bool mcp, bool web)
    {
        Assert.True(DarlingCliCommands.TryParseSurfaceChoice(choice, out var doStore, out var doMcp, out var doWeb));
        Assert.Equal(store, doStore);
        Assert.Equal(mcp, doMcp);
        Assert.Equal(web, doWeb);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("5")] // Disable is handled BEFORE the parser; it is not a surface
    [InlineData("shop")]
    [InlineData("1,shop")] // a typo rejects the WHOLE input — never silently configure a subset
    [InlineData(",")]
    [InlineData("")]
    public void TryParseSurfaceChoice_RejectsUnknownTokens_NothingSelected(string choice)
    {
        Assert.False(DarlingCliCommands.TryParseSurfaceChoice(choice, out var doStore, out var doMcp, out var doWeb));
        Assert.False(doStore);
        Assert.False(doMcp);
        Assert.False(doWeb);
    }

    [Fact]
    public async Task ConfigureNetwork_Byo_RefusesAndWritesNothing()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard queries the Windows service.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-byo-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath,
                """{ "postgres": { "connectionString": "Host=localhost;Database=darling" } }""");

            var input = Script(); // no blocks present -> no disable offer, straight refusal
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);

            Assert.Equal(1, exit);
            Assert.Contains("bring-your-own", output.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Empty(Directory.GetFiles(root.FullName, "darling.json.bak-*")); // nothing written
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_Disable_RemovesAllBlocks_WebIncluded_BacksUp_ResolversLoopback()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard queries the Windows service.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-disable-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, """
                {
                  "postgres": {
                    "managed": true,
                    "port": 5641,
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "role": "viewer" }
                  },
                  "web": {
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "encryptedToken": "BLOB" }
                  },
                  "servers": [ { "host": "S" } ]
                }
                """);

            var input = Script("5", "n"); // Disable, then decline restart
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            var written = await File.ReadAllTextAsync(configPath);
            var config = DarlingConfig.Parse(written);
            Assert.False(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);
            Assert.Equal(
                DarlingHostBinding.BindMode.LoopbackOnly,
                WebHost.ResolveWebBind(config.Web, managed: true).Mode);
            Assert.NotEmpty(Directory.GetFiles(root.FullName, "darling.json.bak-*"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ConfigureNetwork_QuitAtMenu_WritesNothing()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard queries the Windows service.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-quit-");
        try
        {
            var configPath = CopySampleTo(root.FullName);
            var before = await File.ReadAllTextAsync(configPath);

            var input = Script("q");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);

            Assert.Equal(0, exit);
            Assert.Empty(Directory.GetFiles(root.FullName, "darling.json.bak-*"));
            Assert.Equal(before, await File.ReadAllTextAsync(configPath)); // untouched
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    private static string CopySampleTo(string directory)
    {
        var configPath = Path.Combine(directory, "darling.json");
        File.Copy(Path.Combine(AppContext.BaseDirectory, "darling.sample.json"), configPath);
        return configPath;
    }

    private static StringReader Script(params string[] lines) => new(string.Join("\n", lines) + "\n");

    private static int CountOccurrences(string haystack, string needle)
    {
        if (string.IsNullOrEmpty(needle))
        {
            return 0;
        }

        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}

/// <summary>
/// #2197 — the missing-credential refusals every managed-store verb shares. Absence of a credential has two
/// causes that want OPPOSITE advice (a genuine first run, and a bootstrap that has already failed), and
/// before this every verb gave the first-run advice to both. The pure tests pin the evidence probe (including
/// the two ways it must NOT fire, since a wrong "your bootstrap failed" is the same defect pointed somewhere
/// new) and the two message voices; the end-to-end tests drive both branches through two real verbs, because
/// a correct builder nothing calls is exactly what the sibling #1738 defect already was.
/// </summary>
public sealed class DarlingMissingCredentialMessageTests
{
    /* ---------------- pure: what counts as evidence, and what must not ---------------- */

    [Fact]
    public void FindBootstrapEvidence_NothingOnDisk_FindsNone()
    {
        var root = Directory.CreateTempSubdirectory("darling-evidence-none-");
        try
        {
            /* The store folder itself was never created — a genuine first run. */
            Assert.Null(DarlingStoreBootstrapEvidence.FindBootstrapEvidence(
                Path.Combine(root.FullName, "store", "pg")));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The field case (#2185): initdb died in the Windows loader, and the service writes the store's own
    /// credential IMMEDIATELY BEFORE running initdb — so that one file survives the exact failure that
    /// produces the role-credential refusal, and is what makes the sharper branch reachable at all.
    /// </summary>
    [Fact]
    public void FindBootstrapEvidence_StoreCredentialWrittenBeforeInitdb_IsTheFieldCase()
    {
        var root = Directory.CreateTempSubdirectory("darling-evidence-cred-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(Path.Combine(root.FullName, "store"));
            var storeCredential = DarlingManagedPostgres.CredentialPathFor(dataDirectory);
            File.WriteAllText(storeCredential, "not-a-real-credential");

            var evidence = DarlingStoreBootstrapEvidence.FindBootstrapEvidence(dataDirectory);

            Assert.NotNull(evidence);
            Assert.Contains(storeCredential, evidence, StringComparison.Ordinal);
            Assert.Contains("immediately before it runs initdb", evidence, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void FindBootstrapEvidence_InitializedCluster_NamesTheCluster()
    {
        var root = Directory.CreateTempSubdirectory("darling-evidence-pgver-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18\n");

            var evidence = DarlingStoreBootstrapEvidence.FindBootstrapEvidence(dataDirectory);

            Assert.NotNull(evidence);
            Assert.Contains(dataDirectory, evidence, StringComparison.Ordinal);
            Assert.Contains("already initialized", evidence, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void FindBootstrapEvidence_ServerLog_NamesIt()
    {
        var root = Directory.CreateTempSubdirectory("darling-evidence-pglog-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(Path.Combine(root.FullName, "store"));
            var serverLog = Path.Combine(root.FullName, "store", DarlingManagedPostgres.ServerLogFileName);
            File.WriteAllText(serverLog, "FATAL: something\n");

            var evidence = DarlingStoreBootstrapEvidence.FindBootstrapEvidence(dataDirectory);

            Assert.NotNull(evidence);
            Assert.Contains(serverLog, evidence, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The false-positive guard. An operator who pre-creates the data directory before the first run is
    /// still ON their first run, and telling them to go read a service log that does not exist would be the
    /// same misdirection this issue is about, merely pointed somewhere new.
    /// </summary>
    [Fact]
    public void FindBootstrapEvidence_EmptyDataDirectory_IsNotEvidence()
    {
        var root = Directory.CreateTempSubdirectory("darling-evidence-empty-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(dataDirectory);

            Assert.Null(DarlingStoreBootstrapEvidence.FindBootstrapEvidence(dataDirectory));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void FindBootstrapEvidence_EmptyPath_FindsNone_WithoutProbingTheWorkingDirectory(string dataDirectory)
    {
        /* An empty path must never become a RELATIVE one, which would answer about whatever directory the
           operator happened to run the verb from. */
        Assert.Null(DarlingStoreBootstrapEvidence.FindBootstrapEvidence(dataDirectory));
    }

    /* ---------------- pure: the two voices ---------------- */

    [Fact]
    public void MissingCredentialMessage_NoEvidence_KeepsTheFirstRunAdvice_AndHedgesForAnAlreadyStartedService()
    {
        var root = Directory.CreateTempSubdirectory("darling-msg-firstrun-");
        try
        {
            var message = DarlingStoreBootstrapEvidence.MissingCredentialMessage(
                @"The 'viewer' role credential (C:\store\pg-viewer-credential.dpapi)",
                "provisions the least-privilege roles and their credentials",
                Path.Combine(root.FullName, "store", "pg"));

            /* The advice that is CORRECT for a genuine first run is unchanged — and still searchable. */
            Assert.Contains("does not exist yet", message, StringComparison.Ordinal);
            Assert.Contains("Start the PerformanceMonitor Darling service once", message, StringComparison.Ordinal);
            Assert.Contains("provisions the least-privilege roles and their credentials", message, StringComparison.Ordinal);

            /* Plus the sentence the old message was missing entirely: the operator who has ALREADY started
               it is told where the reason is, and told it is not in darling.json. */
            Assert.Contains("ALREADY started it", message, StringComparison.Ordinal);
            Assert.Contains(DarlingStoreBootstrapEvidence.ServiceLogPath, message, StringComparison.Ordinal);
            Assert.Contains("darling.json", message, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void MissingCredentialMessage_BootstrapAlreadyAttempted_PointsAtTheLog_AndNeverAtStartingTheServiceAgain()
    {
        var root = Directory.CreateTempSubdirectory("darling-msg-failed-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(Path.Combine(root.FullName, "store"));
            File.WriteAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory), "not-a-real-credential");

            var message = DarlingStoreBootstrapEvidence.MissingCredentialMessage(
                @"The 'viewer' role credential (C:\store\pg-viewer-credential.dpapi)",
                "provisions the least-privilege roles and their credentials",
                dataDirectory);

            /* The whole point: this operator must NOT be sent to start the service again, and must not be
               sent to darling.json either. */
            Assert.DoesNotContain("Start the PerformanceMonitor Darling service once", message, StringComparison.Ordinal);
            Assert.DoesNotContain("does not exist yet", message, StringComparison.Ordinal);
            Assert.Contains("NOT a first run", message, StringComparison.Ordinal);
            Assert.Contains("starting it again is not the fix", message, StringComparison.Ordinal);

            /* Where to look, named — and why the log is worth reading now (#2194 decodes a bundled tool
               that Windows killed instead of printing a bare number). */
            Assert.Contains(DarlingStoreBootstrapEvidence.ServiceLogPath, message, StringComparison.Ordinal);
            Assert.Contains("FIRST error", message, StringComparison.Ordinal);
            Assert.Contains("bare exit code", message, StringComparison.Ordinal);
            Assert.Contains("Nothing in darling.json produces this", message, StringComparison.Ordinal);

            /* The evidence is QUOTED rather than asserted, so the verdict is checkable by the operator. */
            Assert.Contains(DarlingManagedPostgres.CredentialPathFor(dataDirectory), message, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The lead clause is the same in both voices: field reports and the issue tracker are
    /// searchable by it, so the branch changes what FOLLOWS it, never what an operator pastes into a
    /// search box.</summary>
    [Fact]
    public void MissingCredentialMessage_BothVoices_KeepTheSameSearchableLead()
    {
        var root = Directory.CreateTempSubdirectory("darling-msg-lead-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            const string subject = "The managed store credential (C:\\store\\pg-credential.dpapi)";

            var firstRun = DarlingStoreBootstrapEvidence.MissingCredentialMessage(
                subject, "initializes the store", dataDirectory);

            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18\n");
            var attempted = DarlingStoreBootstrapEvidence.MissingCredentialMessage(
                subject, "initializes the store", dataDirectory);

            Assert.StartsWith(subject + " does not exist", firstRun, StringComparison.Ordinal);
            Assert.StartsWith(subject + " does not exist", attempted, StringComparison.Ordinal);
            Assert.NotEqual(firstRun, attempted);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void MissingStoreCredentialMessage_NamesTheCredentialPath_WhichTheOldMessageNeverDid()
    {
        var root = Directory.CreateTempSubdirectory("darling-msg-storecred-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            var postgres = DarlingConfig.Parse($$"""
                {
                  "postgres": {
                    "managed": true,
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}}
                  }
                }
                """).Postgres;

            var message = DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres);

            Assert.Contains("The managed store credential", message, StringComparison.Ordinal);
            Assert.Contains(DarlingManagedPostgres.CredentialPathFor(dataDirectory), message, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ---------------- end-to-end: both branches, through two real verbs ---------------- */

    [Fact]
    public async Task PrintViewerConnection_TrueFirstRun_StillTellsThemToStartTheService()
    {
        var root = Directory.CreateTempSubdirectory("darling-e2e-firstrun-");
        try
        {
            /* The store folder does not exist at all — nothing has ever run against it. */
            var configPath = WriteManagedConfig(root.FullName, Path.Combine(root.FullName, "store", "pg"));

            var error = new StringWriter();
            var exit = await DarlingCliCommands.PrintViewerConnectionAsync(
                configPath, new StringWriter(), error, CancellationToken.None);
            var stderr = error.ToString();

            Assert.Equal(1, exit);
            Assert.Contains("pg-viewer-credential.dpapi", stderr, StringComparison.Ordinal);
            Assert.Contains("Start the PerformanceMonitor Darling service once", stderr, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task PrintViewerConnection_AfterAFailedBootstrap_NamesTheLogInsteadOfTheService()
    {
        var root = Directory.CreateTempSubdirectory("darling-e2e-failed-");
        try
        {
            /* The #2185 shape, on disk: the service ran, wrote the store credential, and its initdb died —
               so the role credentials were never provisioned. */
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(Path.Combine(root.FullName, "store"));
            File.WriteAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory), "not-a-real-credential");
            var configPath = WriteManagedConfig(root.FullName, dataDirectory);

            var error = new StringWriter();
            var exit = await DarlingCliCommands.PrintViewerConnectionAsync(
                configPath, new StringWriter(), error, CancellationToken.None);
            var stderr = error.ToString();

            Assert.Equal(1, exit);
            Assert.Contains("pg-viewer-credential.dpapi", stderr, StringComparison.Ordinal);
            Assert.DoesNotContain("Start the PerformanceMonitor Darling service once", stderr, StringComparison.Ordinal);
            Assert.Contains(DarlingStoreBootstrapEvidence.ServiceLogPath, stderr, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task EnableMcp_TrueFirstRun_StillTellsThemToStartTheService()
    {
        var root = Directory.CreateTempSubdirectory("darling-e2e-mcp-firstrun-");
        try
        {
            var configPath = WriteManagedConfig(root.FullName, Path.Combine(root.FullName, "store", "pg"));

            var error = new StringWriter();
            var exit = await DarlingCliCommands.EnableMcpAsync(
                configPath, new StringWriter(), error, CancellationToken.None);
            var stderr = error.ToString();

            Assert.Equal(1, exit);
            Assert.Contains("The managed store credential", stderr, StringComparison.Ordinal);
            Assert.Contains("Start the PerformanceMonitor Darling service once", stderr, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The store-credential verbs' own nasty state: a cluster EXISTS but its credential does not, so initdb
    /// will never run again (it only runs on an empty data directory) and no number of restarts produces the
    /// file. "Start the service once so its first run initializes the store" was a closed loop there.
    /// </summary>
    [Fact]
    public async Task EnableMcp_ClusterExistsButCredentialDoesNot_NamesTheLogInsteadOfTheService()
    {
        var root = Directory.CreateTempSubdirectory("darling-e2e-mcp-failed-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18\n");
            var configPath = WriteManagedConfig(root.FullName, dataDirectory);

            var error = new StringWriter();
            var exit = await DarlingCliCommands.EnableMcpAsync(
                configPath, new StringWriter(), error, CancellationToken.None);
            var stderr = error.ToString();

            Assert.Equal(1, exit);
            Assert.DoesNotContain("Start the PerformanceMonitor Darling service once", stderr, StringComparison.Ordinal);
            Assert.Contains("NOT a first run", stderr, StringComparison.Ordinal);
            Assert.Contains(DarlingStoreBootstrapEvidence.ServiceLogPath, stderr, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ---------------- wiring: no verb keeps a private copy of the old advice ---------------- */

    /// <summary>
    /// The defect was five INDEPENDENT copies of one sentence, so the fix is only real if none of them
    /// survives. Parsed at the source because four of the five sit behind a store that a test cannot stand
    /// up, and a sixth copy added later would reintroduce the bug silently.
    /// </summary>
    [Fact]
    public void NoVerbStillCarriesItsOwnFirstRunAdvice()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs"));

        Assert.DoesNotContain("so its first run initializes the store", source, StringComparison.Ordinal);
        Assert.DoesNotContain("first run provisions the least-privilege roles", source, StringComparison.Ordinal);

        /* Every managed missing-credential refusal goes through the shared builder instead — one for the
           role credentials, FIVE for the store's own (--add-server became the fifth in #2256; the count grows
           with each new store verb, and growing it is the point — a verb that grew its OWN copy of the advice
           instead would fail the two DoesNotContain assertions above). */
        Assert.Equal(1, CountOccurrences(source, "DarlingStoreBootstrapEvidence.MissingCredentialMessage("));
        Assert.Equal(5, CountOccurrences(source, "DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage("));
    }

    private static string WriteManagedConfig(string directory, string dataDirectory)
    {
        var configPath = Path.Combine(directory, "darling.json");
        File.WriteAllText(configPath, $$"""
            {
              "postgres": {
                "managed": true,
                "port": 5641,
                "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}}
              },
              "servers": []
            }
            """);
        return configPath;
    }

    /* Locate the repo from this file — the DarlingEnumerationProbeFailureTests idiom; no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
