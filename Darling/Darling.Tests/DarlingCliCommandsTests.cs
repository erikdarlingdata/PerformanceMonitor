/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Host = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService;
using WebHost = PerformanceMonitor.Darling.Service.Mcp.DarlingWebHostService;
using static Darling.Tests.RepoFile;

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

    /// <summary>
    /// Azure SQL Database (engine edition 5) has no SQL Agent and no real msdb surface, so
    /// "msdb access: yes" teaches the onboarding operator something false about the target (#3237).
    /// The clause reports the Agent surface as not applicable instead — whichever way the meaningless
    /// HAS_DBACCESS probe answered — matching dispatch, which never sends the Agent-family collectors
    /// to an edition-5 target.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void FormatProbeLine_AzureSqlDb_ReportsAgentSurfaceNotApplicable_NotMsdb(bool hasMsdbAccess)
    {
        var probe = new ConnectionProbeResult(
            Success: true, MajorVersion: 12, EngineEdition: 5, EngineEditionDescription: "Azure SQL Database",
            IsAzureSqlDb: true, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: hasMsdbAccess, Error: null);

        var line = DarlingCliCommands.FormatProbeLine("AZ-SVLESS", probe);

        Assert.Contains("[PASS]", line, StringComparison.Ordinal);
        Assert.Contains("SQL major version 12", line, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", line, StringComparison.Ordinal);
        Assert.Contains("Agent surface: not applicable (Azure SQL Database)", line, StringComparison.Ordinal);
        Assert.DoesNotContain("msdb", line, StringComparison.Ordinal);
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

    /// <summary>
    /// An Aurora writer clears every gate but ONE, and the line names it. Until #3604 Aurora was a strict
    /// superset of what the PostgreSQL collectors read and the line said "all N apply"; <c>pg_wait_sampling</c>
    /// is now gated off Aurora — the engine cannot preload the module and has <c>pg_wait_stats</c> instead —
    /// so the pre-flight is where an operator first sees that one collector, by name, does not run there.
    /// Counted from the catalog and the collectors' own gates rather than hard-coded, so a second Aurora
    /// gap shows up here as a changed count rather than a silently passing pin.
    /// </summary>
    [Fact]
    public void FormatProbeLine_AuroraWriter_ReportsEveryPostgresCollectorButTheOneAuroraCannotHave()
    {
        var target = PostgresProbe().ToTargetInfo();
        var postgres = CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql).ToList();
        var skipped = postgres.Where(d => !CollectorCatalog.AppliesTo(d, target)).Select(d => d.Name).ToList();
        Assert.Equal(new[] { PgWaitSamplingCollector.Instance.Name }, skipped);

        var line = DarlingCliCommands.FormatProbeLine("aurora-writer", PostgresProbe());

        Assert.Contains($"{postgres.Count - 1} of {postgres.Count} PostgreSQL collectors apply", line, StringComparison.Ordinal);
        Assert.Contains("skipped: pg_wait_sampling", line, StringComparison.Ordinal);
    }

    /// <summary>The line an Aurora writer used to get, every PostgreSQL collector applying, is now the STOCK
    /// writer's with the extension present — the shape #3604 made the finest stock tier.</summary>
    [Fact]
    public void FormatProbeLine_StockWriterWithTheExtension_ReportsEveryPostgresCollectorApplies()
    {
        var expected = CollectorCatalog.All.Count(d => d.TargetEngine == CollectorTargetEngine.PostgreSql);
        var probe = PostgresProbe() with { IsAurora = false, HasPgWaitSamplingExtension = true };

        /* pg_wait_stats and pg_cpu_utilization are Aurora-only, so a stock target skips those two instead. */
        var line = DarlingCliCommands.FormatProbeLine("stock-writer", probe);
        Assert.Contains($"{expected - 2} of {expected} PostgreSQL collectors apply", line, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_wait_sampling", line, StringComparison.Ordinal);
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
    [InlineData("--enable-collector", true)]
    [InlineData("--disable-collector", true)]
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
    /// #2665: an exposure admitting both roles prints one paste-ready string PER ROLE. The two credentials
    /// are deliberately different here, because the failure worth catching is not "only one string printed"
    /// but a string carrying the WRONG role's password — which is invisible in a fixture where both
    /// passwords are the same, and which would hand a teammate admin while the label said viewer. The
    /// certificate is shared by both seats, so it must appear ONCE, not per string.
    /// </summary>
    [Fact]
    public async Task PrintViewerConnectionAsync_BothRoles_PrintsOneStringPerRole_WithEachRolesOwnPassword()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-printconn-roles-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var viewerCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);
            File.WriteAllText(viewerCredential, PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("viewer-secret-pw"));
            File.WriteAllText(
                PerformanceMonitor.Darling.Service.DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory),
                PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("admin-secret-pw"));

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
                    "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "role": "viewer,admin" }
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

            /* One string per role, each pairing its OWN credential. */
            Assert.Contains("Username=admin;Password=admin-secret-pw", stdout, StringComparison.Ordinal);
            Assert.Contains("Username=viewer;Password=viewer-secret-pw", stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("Username=admin;Password=viewer-secret-pw", stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("Username=viewer;Password=admin-secret-pw", stdout, StringComparison.Ordinal);

            /* Each is labelled with the seat it authenticates as — the strings differ only in those two
               fields, so an unlabelled pair cannot be told apart after it leaves the screen. */
            Assert.Contains("the 'admin' seat:", stdout, StringComparison.Ordinal);
            Assert.Contains("the 'viewer' seat:", stdout, StringComparison.Ordinal);

            /* The shared cert is emitted once, not once per seat. */
            Assert.Equal(1, CountOccurrences(stdout, pem));

            /* Both notices: that there is more than one credential here, and the admin pivot caveat. */
            Assert.Contains("admits 2 roles", stderr, StringComparison.Ordinal);
            Assert.Contains("'admin' is a WRITE credential", stderr, StringComparison.Ordinal);
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
            Assert.Equal("viewer", decision.Roles?[0]);

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
    /// #2665: the wizard admits BOTH roles, and writes the resolver's CANONICAL text rather than what was
    /// typed — so a messy "viewer + admin" lands as "admin,viewer" and a later re-run of the wizard is a
    /// no-op instead of a reorder that rewrites pg_hba and reloads the server. The admin pivot warning must
    /// still appear: it is keyed on the normalized value, so a check for the literal string "admin" would go
    /// silent for exactly the list forms this feature adds.
    /// </summary>
    [Fact]
    public async Task ConfigureNetwork_Store_BothRoles_WritesCanonicalListAndStillWarnsAboutAdmin()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The wizard queries the Windows service + uses DPAPI.");

        var root = Directory.CreateTempSubdirectory("darling-confignet-roles-");
        try
        {
            var configPath = CopySampleTo(root.FullName);

            /* choice=Store, bind IP, CIDR, both roles typed in the "wrong" order and spacing, decline restart. */
            var input = Script("1", "192.168.1.205", "192.168.1.0/24", "viewer + admin", "n");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = await DarlingCliCommands.ConfigureNetworkAsync(configPath, input, output, error, CancellationToken.None);
            Assert.Equal(0, exit);

            var written = await File.ReadAllTextAsync(configPath);
            Assert.Contains("\"role\": \"admin,viewer\"", written, StringComparison.Ordinal);

            var config = DarlingConfig.Parse(written);
            var decision = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath);
            Assert.True(decision.Exposed);
            Assert.Equal(new[] { "admin", "viewer" }, decision.Roles!);

            Assert.Contains("WARNING: 'admin' is a remote WRITE credential", output.ToString(), StringComparison.Ordinal);
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

/// <summary>
/// The <c>--enable-collector</c> / <c>--disable-collector</c> verbs (#3752): the pure half — verb recognition, the
/// strict argument grammar, the canonical-name rule, the plan IDENTITY with the executor, the no-store refusals
/// and their stream discipline, the read-back rendering, the read-back SQL dialect, and the README entry.
///
/// <para><b>The load-bearing pin is the plan identity.</b> The issue's table found <c>config_collector_schedules</c>
/// written by the service's <c>DarlingCommandExecutor</c> and by nothing headless; the fix's whole claim is that
/// the verb adds a CALLER, not a second writer. So the verb's plan is asserted EQUAL to the executor's plan for the
/// same command — SQL, parameters, success status — rather than merely "contains ON CONFLICT", and the unknown-name
/// refusal is asserted to be the executor's own sentence. A copy of the SQL would pass a shape test; it cannot pass
/// this one without being kept byte-identical forever, which is the point.</para>
///
/// <para>The store half — the write landing, the override columns surviving, the rows reading back — is
/// <see cref="DarlingCollectorToggleVerbLivePostgresTests"/>.</para>
/// </summary>
public sealed class DarlingCollectorToggleVerbTests
{
    [Theory]
    [InlineData("--enable-collector", true)]
    [InlineData("--ENABLE-COLLECTOR", true)]
    [InlineData("--disable-collector", false)]
    [InlineData("--DISABLE-collector", false)]
    public void BothVerbsAreRecognized_CaseInsensitively_AndDispatchedRatherThanStartingTheHost(string arg, bool enable)
    {
        Assert.Equal(enable, DarlingCliCommands.IsEnableCollectorVerb(arg));
        Assert.Equal(!enable, DarlingCliCommands.IsDisableCollectorVerb(arg));

        /* The #1581 contract: a recognized verb dispatches; it never falls through to a real service startup. The
           #1912 drift (a verb with a dispatch block IsKnownVerb never learned) is what the second line catches. */
        Assert.True(DarlingCliCommands.IsKnownVerb(arg));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { arg, "wait_stats" }));
    }

    [Fact]
    public void TheVerbsAreDiscoverable_FromHelp_WithTheirGrammar()
    {
        var usage = DarlingCliCommands.UsageText();
        Assert.Contains("--enable-collector <name> [--server <server>] [--config <path>]", usage, StringComparison.Ordinal);
        Assert.Contains("--disable-collector <name> [--server <server>] [--config <path>]", usage, StringComparison.Ordinal);
        /* The default scope is stated where the verb is discovered, not only in the README. */
        Assert.Contains("fleet-wide by default", usage, StringComparison.Ordinal);
    }

    /// <summary>
    /// Program.cs dispatches both verbs to <c>ToggleCollectorAsync</c> — pinned structurally, as the sibling verbs
    /// are, because the classifier-to-dispatch seam is where #1912's drift lived and no test calling
    /// <c>DarlingCliCommands</c> directly can see it. And pinned that the dispatch carries NO Windows guard: the
    /// --add-server posture (Windows is needed only for a MANAGED store credential, which the verb checks itself),
    /// so a Linux host on bring-your-own Postgres keeps the verb.
    /// </summary>
    [Fact]
    public void ProgramDispatchesBothVerbs_WithoutAWindowsGuard_TheAddServerPosture()
    {
        var program = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");

        var at = program.IndexOf("DarlingCliCommands.IsEnableCollectorVerb(args[0])", StringComparison.Ordinal);
        Assert.True(at >= 0, "Program.cs no longer dispatches --enable-collector (#3752)");
        Assert.Contains("DarlingCliCommands.IsDisableCollectorVerb(args[0])", program, StringComparison.Ordinal);

        var block = program[at..Math.Min(program.Length, at + 400)];
        Assert.Contains("DarlingCliCommands.ToggleCollectorAsync(", block, StringComparison.Ordinal);
        Assert.Contains("args[1..]", block, StringComparison.Ordinal);
        Assert.DoesNotContain("OperatingSystem.IsWindows()", block, StringComparison.Ordinal);
        Assert.DoesNotContain("requires Windows", block, StringComparison.Ordinal);
    }

    /* ---------------- the strict argument grammar ---------------- */

    [Fact]
    public void Parse_NameOnly_IsFleetWide_NoConfig()
    {
        Assert.True(DarlingCliCommands.TryParseCollectorToggleArgs(
            "--enable-collector", new[] { "long_query_completions" }, out var name, out var server, out var config, out var error));
        Assert.Equal("long_query_completions", name);
        Assert.Null(server);
        Assert.Null(config);
        Assert.Null(error);
    }

    [Fact]
    public void Parse_ServerAndConfig_InEitherOrder()
    {
        Assert.True(DarlingCliCommands.TryParseCollectorToggleArgs(
            "--enable-collector", new[] { "wait_stats", "--server", "sql01", "--config", @"C:\x\darling.json" },
            out var name, out var server, out var config, out _));
        Assert.Equal("wait_stats", name);
        Assert.Equal("sql01", server);
        Assert.Equal(@"C:\x\darling.json", config);

        Assert.True(DarlingCliCommands.TryParseCollectorToggleArgs(
            "--disable-collector", new[] { "--SERVER", "sql01", "--Config", "d.json", "wait_stats" },
            out name, out server, out config, out _));
        Assert.Equal("wait_stats", name);
        Assert.Equal("sql01", server);
        Assert.Equal("d.json", config);
    }

    /// <summary>
    /// Every refusal: no name; two names; an unknown flag (NOT taken as a name — the later "unknown collector
    /// '--sever'" would be the worse message); <c>--server</c> / <c>--config</c> with no value or with the next
    /// flag where the value should be. Each names the verb the operator typed.
    /// </summary>
    [Theory]
    [InlineData(new string[0], "needs a collector name")]
    [InlineData(new[] { "wait_stats", "query_stats" }, "ONE collector name")]
    [InlineData(new[] { "--sever", "sql01" }, "Unknown option")]
    [InlineData(new[] { "wait_stats", "--server" }, "--server needs a server name")]
    [InlineData(new[] { "wait_stats", "--server", "--config", "x" }, "--server needs a server name")]
    [InlineData(new[] { "wait_stats", "--config" }, "--config needs a path")]
    public void Parse_Refuses_WithTheVerbNamed(string[] rest, string expectedFragment)
    {
        Assert.False(DarlingCliCommands.TryParseCollectorToggleArgs(
            "--disable-collector", rest, out _, out _, out _, out var error));
        Assert.NotNull(error);
        Assert.Contains(expectedFragment, error, StringComparison.Ordinal);
        Assert.Contains("--disable-collector", error, StringComparison.Ordinal);
    }

    /* ---------------- the canonical spelling ---------------- */

    /// <summary>
    /// The dictionary matches case-insensitively and so does the runner's ResolveSchedule, but the store's
    /// partial unique indexes compare <c>collector_name</c> exactly — so the verb must write the dictionary's
    /// spelling or <c>Wait_Stats</c> and <c>wait_stats</c> become two fleet rows and first-match picks one.
    /// </summary>
    [Theory]
    [InlineData("wait_stats", "wait_stats")]
    [InlineData("Wait_Stats", "wait_stats")]
    [InlineData("  LONG_QUERY_COMPLETIONS ", "long_query_completions")]
    [InlineData("not_a_collector", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void CanonicalCollectorName_ReturnsTheDictionaryKey_OrNull(string? typed, string? expected) =>
        Assert.Equal(expected, DarlingCliCommands.CanonicalCollectorName(typed));

    /* ---------------- plan identity with the executor (the ONE-WRITE-PATH pin) ---------------- */

    [Fact]
    public void BuildCollectorToggleCommand_IsTheCommandPlanesRow_MinusTheQueue()
    {
        var enable = DarlingCliCommands.BuildCollectorToggleCommand(enable: true, "long_query_completions", serverId: null);
        Assert.Equal("enable_collector", enable.CommandType);
        Assert.Null(enable.TargetServerId);
        Assert.Equal("cli", enable.RequestedBy);
        using (var args = JsonDocument.Parse(enable.ArgsJson!))
        {
            /* The executor reads collector_name (or collectorName); the verb writes the snake_case the Viewer
               and the executor's own tests use. */
            Assert.Equal("long_query_completions", args.RootElement.GetProperty("collector_name").GetString());
        }

        var disable = DarlingCliCommands.BuildCollectorToggleCommand(enable: false, "wait_stats", serverId: 7);
        Assert.Equal("disable_collector", disable.CommandType);
        Assert.Equal(7, disable.TargetServerId);
    }

    [Fact]
    public void PlanCollectorToggle_FleetWide_EqualsTheExecutorsPlan_ForTheSameCommand()
    {
        var verbPlan = DarlingCliCommands.PlanCollectorToggle(enable: true, "long_query_completions", serverId: null);
        var executorPlan = DarlingCommandExecutor.ResolvePlan(
            new ClaimedCommand(1, "enable_collector", null, "{\"collector_name\":\"long_query_completions\"}", "tester"));

        Assert.Equal(CommandKind.StoreWrite, verbPlan.Kind);
        Assert.Equal(executorPlan.Kind, verbPlan.Kind);
        Assert.Equal(executorPlan.Sql, verbPlan.Sql);
        Assert.Equal(executorPlan.Parameters, verbPlan.Parameters);
        Assert.Equal(executorPlan.SuccessStatus, verbPlan.SuccessStatus);

        /* And it IS the fleet arbiter the executor's own tests pin — said here too so a reader of this test
           alone sees which row the default scope writes. */
        Assert.Contains("ON CONFLICT (collector_name) WHERE server_id IS NULL", verbPlan.Sql, StringComparison.Ordinal);
        Assert.Contains("VALUES (NULL, $1, TRUE)", verbPlan.Sql, StringComparison.Ordinal);
        Assert.Equal("collector enabled (fleet-wide)", verbPlan.SuccessStatus);
    }

    [Fact]
    public void PlanCollectorToggle_PerServer_EqualsTheExecutorsPlan_ForTheSameCommand()
    {
        var verbPlan = DarlingCliCommands.PlanCollectorToggle(enable: false, "wait_stats", serverId: 42);
        var executorPlan = DarlingCommandExecutor.ResolvePlan(
            new ClaimedCommand(1, "disable_collector", 42, "{\"collector_name\":\"wait_stats\"}", "tester"));

        Assert.Equal(CommandKind.StoreWrite, verbPlan.Kind);
        Assert.Equal(executorPlan.Sql, verbPlan.Sql);
        Assert.Equal(executorPlan.Parameters, verbPlan.Parameters);
        Assert.Equal(executorPlan.SuccessStatus, verbPlan.SuccessStatus);

        Assert.Contains("ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL", verbPlan.Sql, StringComparison.Ordinal);
        Assert.Contains("VALUES ($1, $2, FALSE)", verbPlan.Sql, StringComparison.Ordinal);
        Assert.Equal(new object?[] { 42, "wait_stats" }, verbPlan.Parameters);

        /* Only the enabled column: a frequency/retention override already on the row must survive the toggle. */
        Assert.Contains("DO UPDATE SET enabled = EXCLUDED.enabled", verbPlan.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("frequency_minutes", verbPlan.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("retention_days", verbPlan.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCollectorToggle_UnknownName_IsTheExecutorsOwnRefusal()
    {
        var plan = DarlingCliCommands.PlanCollectorToggle(enable: true, "not_a_collector", serverId: null);
        Assert.Equal(CommandKind.Fail, plan.Kind);
        Assert.Null(plan.Sql);
        Assert.Equal("enable_collector: unknown collector 'not_a_collector'", plan.FailReason);

        var executorPlan = DarlingCommandExecutor.ResolvePlan(
            new ClaimedCommand(1, "enable_collector", null, "{\"collector_name\":\"not_a_collector\"}", "tester"));
        Assert.Equal(executorPlan.FailReason, plan.FailReason);
    }

    /* ---------------- refusals that never open the store, and where each stream carries what ---------------- */

    /// <summary>No name: the one-line refusal on STDERR, the usage + collector list on STDOUT (the [#2097] split),
    /// exit 1 — and it returned before loading any config (no path was given and none was needed).</summary>
    [Fact]
    public async Task Toggle_NoName_RefusesOnStderr_ExplainsOnStdout_ChangesNothing()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingCliCommands.ToggleCollectorAsync(
            enable: true, Array.Empty<string>(), output, error, CancellationToken.None);

        Assert.Equal(1, exit);
        Assert.Contains("--enable-collector needs a collector name", error.ToString(), StringComparison.Ordinal);

        var text = output.ToString();
        Assert.Contains("--enable-collector <collector> [--server <server>]", text, StringComparison.Ordinal);
        Assert.Contains("--disable-collector <collector> [--server <server>]", text, StringComparison.Ordinal);
        Assert.Contains("FLEET-WIDE", text, StringComparison.Ordinal);
        Assert.Contains("long_query_completions (ships OFF)", text, StringComparison.Ordinal);
        Assert.Contains("wait_stats", text, StringComparison.Ordinal);
    }

    /// <summary>An unknown collector is refused with the EXECUTOR's sentence (not a CLI paraphrase), exit 1, the
    /// known list on STDOUT — and no config was loaded: the name is validated before anything else, so a typo
    /// never opens a connection. Proven by passing a config path that does not exist and seeing no complaint about it.</summary>
    [Fact]
    public async Task Toggle_UnknownCollector_RefusesWithTheExecutorsText_BeforeTouchingConfig()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingCliCommands.ToggleCollectorAsync(
            enable: false, new[] { "not_a_collector", "--config", Path.Combine(Path.GetTempPath(), "does-not-exist-3752.json") },
            output, error, CancellationToken.None);

        Assert.Equal(1, exit);
        Assert.Contains("disable_collector: unknown collector 'not_a_collector'", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Could not load configuration", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("Known collectors", output.ToString(), StringComparison.Ordinal);
        Assert.Contains("wait_stats", output.ToString(), StringComparison.Ordinal);
    }

    /// <summary>A KNOWN name with a missing config: now the config IS loaded (the name passed), and the failure
    /// is the sibling verbs' "Could not load configuration" on STDERR, exit 1, nothing on STDOUT.</summary>
    [Fact]
    public async Task Toggle_KnownCollector_MissingConfig_FailsAtConfigLoad_LikeTheSiblingVerbs()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingCliCommands.ToggleCollectorAsync(
            enable: true, new[] { "Long_Query_Completions", "--config", Path.Combine(Path.GetTempPath(), "does-not-exist-3752.json") },
            output, error, CancellationToken.None);

        Assert.Equal(1, exit);
        Assert.Contains("Could not load configuration", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(string.Empty, output.ToString());
    }

    /* ---------------- the read-back rendering ---------------- */

    [Fact]
    public void FormatRows_NoRows_SaysTheDefaultApplies_AndNamesTheDefault()
    {
        var lines = DarlingCliCommands.FormatCollectorScheduleRows("long_query_completions", Array.Empty<DarlingCliCommands.CollectorScheduleReadbackRow>());
        Assert.Equal(2, lines.Count);
        Assert.Contains("long_query_completions", lines[0], StringComparison.Ordinal);
        /* The opt-in collector's default is stated as OFF, so a printout with no rows reads as "not running". */
        Assert.Contains("every 1 min, 30-day retention, ships OFF", lines[0], StringComparison.Ordinal);
        Assert.Contains("none", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public void FormatRows_FleetAndServer_RendersScope_Enabled_Overrides_AndDatabases()
    {
        var rows = new[]
        {
            new DarlingCliCommands.CollectorScheduleReadbackRow(null, "wait_stats", null, null, true, null, null),
            new DarlingCliCommands.CollectorScheduleReadbackRow(7, "wait_stats", 5, 14, false, new[] { "sales", "hr" }, "sql01"),
            new DarlingCliCommands.CollectorScheduleReadbackRow(8, "wait_stats", 0, null, true, Array.Empty<string>(), null),
        };

        var lines = DarlingCliCommands.FormatCollectorScheduleRows("wait_stats", rows);

        Assert.Contains("ships ON", lines[0], StringComparison.Ordinal);
        Assert.Contains("  fleet-wide: enabled=true  frequency=(default)  retention=(default)", lines);
        Assert.Contains("  sql01 (server_id 7): enabled=false  frequency=every 5 min  retention=14 days  databases=[sales, hr]", lines);
        /* A server_id with no registry row (never connected) prints as its id, and frequency 0 is the on-load tier;
           an EMPTY databases array is the explicit V125 "no scope at this level" and is shown as such, not hidden. */
        Assert.Contains("  server_id 8 (not in the servers registry): enabled=true  frequency=on load only  retention=(default)  databases=[] (explicit: no scope at this level)", lines);
        Assert.Contains(lines, l => l.Contains("server's own row wins", StringComparison.Ordinal));
    }

    [Fact]
    public void KnownCollectorsText_ListsEveryDefault_TagsTheOptInOnes()
    {
        var text = DarlingCliCommands.KnownCollectorsText();
        foreach (var name in CollectorScheduleDefaults.All.Keys)
        {
            Assert.Contains(name, text, StringComparison.Ordinal);
        }

        Assert.Contains("long_query_completions (ships OFF)", text, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_stats (ships OFF)", text, StringComparison.Ordinal);
    }

    /* ---------------- the read-back SQL ---------------- */

    [Fact]
    public void ReadbackSql_IsSchemaQualified_Parameterized_MatchesLikeTheResolver_FleetFirst()
    {
        var sql = DarlingCliCommands.CollectorScheduleReadbackSql;
        Assert.Contains("FROM config.config_collector_schedules", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN collect.servers", sql, StringComparison.Ordinal);
        Assert.Contains("lower(cs.collector_name) = lower($1)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY cs.server_id NULLS FIRST", sql, StringComparison.Ordinal);
        /* A read, and only a read: the verb's ONE write is the executor's plan. */
        Assert.DoesNotContain("INSERT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DELETE", sql, StringComparison.OrdinalIgnoreCase);
    }

    /* ---------------- the README ---------------- */

    [Fact]
    public void Readme_DocumentsBothVerbs_TheDefaultScope_AndTheXeSessionConsequence()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        Assert.Contains("--enable-collector long_query_completions", readme, StringComparison.Ordinal);
        Assert.Contains("--disable-collector long_query_completions", readme, StringComparison.Ordinal);
        Assert.Contains("--enable-collector long_query_completions --server", readme, StringComparison.Ordinal);
        Assert.Contains("fleet-wide**", readme, StringComparison.Ordinal);
        Assert.Contains("creates the `PerformanceMonitor_LongQueryCompletions` Extended Events session", readme, StringComparison.Ordinal);
        Assert.Contains("disabling it drops that session", readme, StringComparison.Ordinal);
        Assert.Contains("no MCP tool", readme, StringComparison.Ordinal);
    }
}

/// <summary>
/// The store half of the collector toggle verbs (#3752), against the shared <c>DARLING_TEST_PG</c> store: the
/// verb run END TO END through <see cref="DarlingCliCommands.ToggleCollectorAsync"/> with a bring-your-own
/// darling.json pointed at the test store — parse, plan, connect, write through the executor's static store-write,
/// read back, print — for the fleet scope and for a sentinel server, asserting the row the service will resolve
/// and that a frequency override already on the row survives the toggle (the plan touches only <c>enabled</c>).
/// Serialized with the other live classes because it writes rows the shared store's other tests read.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCollectorToggleVerbLivePostgresTests
{
    /// <summary>Distinctive sentinel — a real server_id is a storage-name hash, never this; distinct from
    /// <c>DarlingCommandExecutorTests</c>' sentinel so the two classes never clean each other's rows.</summary>
    private const int SentinelServerId = -375200;
    private const string SentinelServerName = "lane-3752-sentinel:xedb1";
    private const string SentinelDisplayName = "lane-3752 sentinel";

    [Fact]
    public async Task Toggle_FleetAndServer_WritesTheExecutorsRow_ReadsItBack_PreservesOverrides_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the collector toggle verb end to end.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var directory = Path.Combine(Path.GetTempPath(), "darling-3752-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var configPath = Path.Combine(directory, "darling.json");
        File.WriteAllText(configPath, $$"""
            {
              "postgres": {
                "managed": false,
                "connectionString": {{JsonSerializer.Serialize(connectionString)}}
              },
              "servers": []
            }
            """);

        try
        {
            await CleanupAsync(connection, ct);

            /* A registry row for the sentinel, so --server can resolve it by display name, and a PRE-EXISTING
               per-server row carrying a frequency override with enabled = FALSE — the thing the toggle must not
               erase. (The Viewer's editor writes every column; the executor's toggle writes one.) */
            await ExecAsync(connection,
                "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) " +
                $"VALUES ({SentinelServerId}, '{SentinelServerName}', '{SentinelDisplayName}', TRUE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) " +
                $"VALUES ({SentinelServerId}, 'long_query_completions', 5, NULL, FALSE)", ct);

            /* 1. Fleet-wide enable, typed in the wrong case: exit 0, the canonical name written, the row read back. */
            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "Long_Query_Completions", "--config", configPath }, output, error, ct);

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(string.Empty, error.ToString());
            var text = output.ToString();
            Assert.Contains("[ENABLED] long_query_completions — fleet-wide (collector enabled (fleet-wide)).", text, StringComparison.Ordinal);
            Assert.Contains("  fleet-wide: enabled=true  frequency=(default)  retention=(default)", text, StringComparison.Ordinal);
            /* The sentinel's own row is in the read-back too — still disabled, frequency override intact. */
            Assert.Contains($"  {SentinelDisplayName} (server_id {SentinelServerId}): enabled=false  frequency=every 5 min  retention=(default)", text, StringComparison.Ordinal);
            Assert.Contains("no restart is needed", text, StringComparison.OrdinalIgnoreCase);

            Assert.Equal(true, await ScalarAsync(connection,
                "SELECT enabled FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'long_query_completions'", ct));
            /* The dictionary spelling, not the typed one — one fleet row, the same row the Viewer writes. */
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT COUNT(*) FROM config.config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = 'long_query_completions'", ct));

            /* 2. Per-server enable by DISPLAY name: the pre-existing row's enabled flips, its frequency survives. */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "long_query_completions", "--server", SentinelDisplayName, "--config", configPath }, output, error, ct);

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains($"[ENABLED] long_query_completions — {SentinelDisplayName} ({SentinelServerName}) (collector enabled).", output.ToString(), StringComparison.Ordinal);
            Assert.Contains($"  {SentinelDisplayName} (server_id {SentinelServerId}): enabled=true  frequency=every 5 min  retention=(default)", output.ToString(), StringComparison.Ordinal);

            await using (var read = new NpgsqlCommand(
                $"SELECT enabled, frequency_minutes FROM config.config_collector_schedules WHERE server_id = {SentinelServerId} AND collector_name = 'long_query_completions'", connection))
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.GetBoolean(0));
                Assert.Equal(5, reader.GetInt32(1));
                Assert.False(await reader.ReadAsync(ct), "exactly one per-server row");
            }

            /* 3. Disable fleet-wide: the fleet row flips back; the per-server row is untouched (it is its own scope). */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: false, new[] { "long_query_completions", "--config", configPath }, output, error, ct);

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains("[DISABLED] long_query_completions — fleet-wide (collector disabled (fleet-wide)).", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(false, await ScalarAsync(connection,
                "SELECT enabled FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'long_query_completions'", ct));
            Assert.Equal(true, await ScalarAsync(connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {SentinelServerId} AND collector_name = 'long_query_completions'", ct));

            /* 4. A server nobody has: refused with the resolver's listing, exit 1, nothing written. */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "wait_stats", "--server", "no-such-server-3752", "--config", configPath }, output, error, ct);

            Assert.Equal(1, exit);
            Assert.Contains("Could not resolve server", error.ToString(), StringComparison.Ordinal);
            Assert.Contains("Nothing was changed.", error.ToString(), StringComparison.Ordinal);
            Assert.Equal(0L, await ScalarAsync(connection,
                $"SELECT COUNT(*) FROM config.config_collector_schedules WHERE server_id = {SentinelServerId} AND collector_name = 'wait_stats'", ct));
        }
        finally
        {
            await CleanupAsync(connection, ct);
            try { Directory.Delete(directory, recursive: true); } catch (IOException) { }
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* The fleet row for long_query_completions is deleted too: this class is the only live test that writes it,
           and leaving it would change what a later test's ResolveSchedule sees for the opt-in collector. */
        await ExecAsync(connection,
            $"DELETE FROM config.config_collector_schedules WHERE server_id = {SentinelServerId}; " +
            "DELETE FROM config.config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = 'long_query_completions'; " +
            $"DELETE FROM collect.servers WHERE server_id = {SentinelServerId}", ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
