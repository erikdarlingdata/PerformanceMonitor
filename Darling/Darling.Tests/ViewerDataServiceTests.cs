/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows.Media;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the viewer's SQL against the Darling store contract (no live Postgres needed). (The naive-UTC →
/// display conversion moved to the mode-aware <c>ViewerTimeHelper</c>, pinned in
/// <c>ViewerTimeHelperTests</c>; the Overview trend reads + wait-category roll-up are pinned in
/// <c>ViewerTrendsTests</c>; the server VERSION label moved to <c>MonitoredEngineVersion</c> when it became
/// engine-aware at #3145, and is pinned — table, callers, and the scans that discover the render sites — in
/// <c>EngineAwareVersionLabelTests</c>.)
/// </summary>
public sealed class ViewerDataServiceTests
{
    [Fact]
    public void ServersSql_ReadsTheServerListOrderedByDisplayName()
    {
        Assert.Contains("FROM servers", ViewerDataService.ServersSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY display_name", ViewerDataService.ServersSql, StringComparison.Ordinal);
        Assert.Contains("server_id, server_name, display_name, is_enabled, sql_major_version", ViewerDataService.ServersSql, StringComparison.Ordinal);
    }

    /* The Collection Health SQL is no longer the shell's DISTINCT-ON latest-run placeholder; W1i
       replaced it with Lite's 7-day aggregate, pinned in ViewerDailyHealthTests along with the Daily
       Summary reads. */

    /* The version-label theory that lived here pinned ViewerDataService.SqlVersionLabel, an engine-BLIND
       formatter: it mapped a SQL Server major to a product name and every caller had to remember to ask
       what engine the row was. Two of the four did not, and a PostgreSQL target — whose sql_major_version
       is 0 — was labelled "SQL Server v0" in the fleet sidebar (#3145). The table is now private inside
       MonitoredEngineVersion behind an entry point that takes the engine discriminator, so there is no
       longer an engine-blind label to pin here. Its successors, including the 11/12 arms this theory never
       covered and the whole caller population, are in EngineAwareVersionLabelTests.

       The naive-UTC → display conversion moved from ViewerDataService.ToLocalTime to the mode-aware
       ViewerTimeHelper (Server/Local/UTC); it is pinned in ViewerTimeHelperTests. */

    /* The old placeholder CollectorHealthRow record (a single latest-run snapshot with
       CollectionTimeLocal) was replaced by Lite's rich 7-day aggregate class in W1i; its HealthStatus
       banding + formatting are pinned in ViewerDailyHealthTests. */
}

/// <summary>
/// The viewer's sliver of darling.json: the service's resolution order and lenient JSON
/// (comments, trailing commas, case-insensitive names), but only the postgres section —
/// including the managed bundled-Postgres mode, whose derivation (credential path convention +
/// DPAPI entropy) is pinned against the SERVICE's DarlingSecrets/DarlingManagedPostgres here,
/// because the viewer deliberately duplicates those constants instead of referencing the
/// service project.
/// <para>In the <c>darling-config-env</c> collection because the resolution test sets the process-wide
/// <c>DARLING_CONFIG</c> variable — see <see cref="DarlingConfigEnvironmentCollection"/> for why saving
/// and restoring it in a <c>finally</c> is not enough on its own.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class ViewerSettingsTests
{
    [Fact]
    public void Parse_ManagedMode_DefaultsToAdminRole_DerivesFromTheAdminCredential()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-viewer-managed-");
        try
        {
            /* The SERVICE provisions the least-privilege roles and writes their credentials; the
               VIEWER (default connectAs = admin) must derive the connection string for the admin
               role from the admin credential (V8 security split — no longer the darling superuser). */
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var adminCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory);
            File.WriteAllText(adminCredential, PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("admin-pw"));

            var json = $$"""
                {
                  "postgres": {
                    "managed": true,
                    "port": 5991,
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}}
                  }
                }
                """;

            var settings = ViewerSettings.Parse(json);
            /* The flag the startup diagnostics report (#1954): the string was DERIVED, so
               postgres.connectionString in the file is not consulted at all. */
            Assert.True(settings.Managed);
            var parsed = new NpgsqlConnectionStringBuilder(settings.ConnectionString);
            /* 127.0.0.1, not "localhost" — the viewer half of the managed-Host parity pair with the
               service's DarlingManagedPostgres.BuildConnectionString (darling-network-endpoints). */
            Assert.Equal("127.0.0.1", parsed.Host);
            Assert.Equal(5991, parsed.Port);
            Assert.Equal("admin", parsed.Username);
            Assert.Equal("admin-pw", parsed.Password);
            Assert.Equal("darling", parsed.Database);
            /* The bare table names resolve to the V8 collect/config schemas on every connection. */
            Assert.Equal("collect,config,public", parsed.SearchPath);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Parse_ManagedMode_ConnectAsViewer_DerivesFromTheViewerCredential()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-viewer-ro-");
        try
        {
            /* A locked-down deployment (connectAs = "viewer") reads the read-only viewer role's
               credential and connects as that role — the write surfaces then degrade gracefully. */
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var viewerCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);
            File.WriteAllText(viewerCredential, PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("viewer-pw"));

            var json = $$"""
                {
                  "postgres": {
                    "managed": true,
                    "port": 5991,
                    "connectAs": "viewer",
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}}
                  }
                }
                """;

            var settings = ViewerSettings.Parse(json);
            var parsed = new NpgsqlConnectionStringBuilder(settings.ConnectionString);
            Assert.Equal("viewer", parsed.Username);
            Assert.Equal("viewer-pw", parsed.Password);
            Assert.Equal("collect,config,public", parsed.SearchPath);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Parse_ManagedMode_UnknownConnectAs_FallsBackToAdmin()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-viewer-unknown-");
        try
        {
            /* A typo'd connectAs degrades to the admin role (the DarlingConfig default), not a crash. */
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var adminCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory);
            File.WriteAllText(adminCredential, PerformanceMonitor.Darling.Service.DarlingSecrets.Protect("admin-pw"));

            var json = $$"""
                {
                  "postgres": {
                    "managed": true,
                    "connectAs": "superadmin",
                    "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}}
                  }
                }
                """;

            var parsed = new NpgsqlConnectionStringBuilder(ViewerSettings.Parse(json).ConnectionString);
            Assert.Equal("admin", parsed.Username);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Parse_ManagedMode_MissingCredential_ThrowsAReadableFirstRunHint()
    {
        var missing = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName(), "pg");
        var json = $$"""{ "postgres": { "managed": true, "dataDirectory": {{JsonSerializer.Serialize(missing)}} } }""";

        var ex = Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse(json));
        Assert.Contains("service", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Start the PerformanceMonitor Darling service once", ex.Message, StringComparison.Ordinal);
        /* #2197: even the first-run voice carries the one sentence for the operator who HAS already
           started it — this string is what the main window shows, so it cannot be a dead end either. */
        Assert.Contains("ALREADY started it", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2197, the viewer half of the CLI's DarlingStoreBootstrapEvidence. This message is rendered by the
    /// main window, so it reaches the same operator with the same absence — and before this it gave the
    /// same first-run advice to a store whose bootstrap had already failed.
    /// </summary>
    [Fact]
    public void Parse_ManagedMode_MissingCredentialAfterAFailedBootstrap_PointsAtTheServiceLogInstead()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-failedboot-");
        try
        {
            /* The #2185 shape: the service wrote the store's own credential immediately before initdb, and
               initdb died — so the admin role credential the viewer wants was never provisioned. */
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(Path.Combine(root.FullName, "store"));
            var storeCredential = PerformanceMonitor.Darling.Service.DarlingManagedPostgres.CredentialPathFor(dataDirectory);
            File.WriteAllText(storeCredential, "not-a-real-credential");

            var json = $$"""{ "postgres": { "managed": true, "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}} } }""";
            var ex = Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse(json));

            Assert.DoesNotContain("Start the PerformanceMonitor Darling service once", ex.Message, StringComparison.Ordinal);
            Assert.Contains("NOT a first run", ex.Message, StringComparison.Ordinal);
            Assert.Contains(storeCredential, ex.Message, StringComparison.Ordinal);
            Assert.Contains("darling-service_yyyyMMdd.log", ex.Message, StringComparison.Ordinal);
            Assert.Contains("Nothing in darling.json produces this", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The viewer does not reference the Service project, so the file names it probes for bootstrap evidence
    /// are DUPLICATED under the file's sliver rule — the same rule the DPAPI entropy and the role/credential
    /// names already live under. Pin them, or a rename on the service side silently turns the viewer's
    /// sharper branch off and nothing fails.
    /// </summary>
    [Fact]
    public void ManagedDerivation_BootstrapEvidenceFileNames_MatchTheServiceConstants()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-sliver-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            var storeFolder = Path.Combine(root.FullName, "store");
            Directory.CreateDirectory(storeFolder);

            /* Each service-side name, one at a time, must be the one the viewer's probe recognises. */
            foreach (var evidenceFile in new[]
                     {
                         PerformanceMonitor.Darling.Service.DarlingManagedPostgres.CredentialFileName,
                         PerformanceMonitor.Darling.Service.DarlingManagedPostgres.McpCredentialFileName,
                         PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ServerLogFileName,
                     })
            {
                var path = Path.Combine(storeFolder, evidenceFile);
                File.WriteAllText(path, "x");

                var json = $$"""{ "postgres": { "managed": true, "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}} } }""";
                var ex = Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse(json));
                Assert.Contains("NOT a first run", ex.Message, StringComparison.Ordinal);
                Assert.Contains(path, ex.Message, StringComparison.Ordinal);

                File.Delete(path);
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Parse_ReadsTheConnectionString_WithCommentsAndTrailingCommas()
    {
        var json = """
            {
              // the same file the service reads
              "Postgres": {
                "ConnectionString": "Host=localhost;Database=darling",
              },
              "servers": [ { "name": "SQL2022", "host": "SQL2022" } ],
            }
            """;

        var settings = ViewerSettings.Parse(json);

        Assert.Equal("Host=localhost;Database=darling", settings.ConnectionString);
        /* Bring-your-own: the string came out of the file verbatim, which is what the startup
           diagnostics report as postgres.managed = false (#1954). */
        Assert.False(settings.Managed);
    }

    [Fact]
    public void Parse_MissingConnectionString_Throws()
    {
        Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse("{}"));
        Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse("""{ "postgres": { "connectionString": "" } }"""));
    }

    [Fact]
    public void ResolveConfigPath_ExplicitPathWins()
    {
        Assert.Equal(@"C:\somewhere\darling.json", ViewerSettings.ResolveConfigPath(@"C:\somewhere\darling.json"));
    }

    /// <summary>
    /// The release-zip layout: viewer\ under the service root, darling.json beside the SERVICE
    /// exe. Beside-binary wins when present; the parent probe covers the shipped layout; when
    /// neither exists the beside-binary path comes back so the not-found hint names the
    /// viewer's own directory.
    /// </summary>
    [Fact]
    public void ResolveConfigPath_PackagedLayout_FallsBackToParentDirectory()
    {
        /* DARLING_CONFIG is resolution step 2 — ahead of BOTH probed locations — so a machine with it set
           (any developer pointing a viewer at a remote store) fails this test on the environment rather
           than on the code. Clear it for the duration and restore. The sibling explicit-path test needs no
           such guard: an explicit path is step 1 and outranks the variable. */
        var savedConfigEnvironment = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Environment.SetEnvironmentVariable("DARLING_CONFIG", null);
        var root = Directory.CreateTempSubdirectory("darling-viewer-resolve-");
        try
        {
            var viewerDirectory = Path.Combine(root.FullName, "viewer");
            Directory.CreateDirectory(viewerDirectory);
            var missing = Path.Combine(viewerDirectory, "darling.json");

            /* Neither location has a config: report the viewer's own (missing) path. */
            Assert.Equal(missing, ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory, installedServiceFolder: () => null));

            /* Only the service root has one — the shipped-zip case. */
            var atServiceRoot = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(atServiceRoot, "{}");
            Assert.Equal(atServiceRoot, ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory, installedServiceFolder: () => null));

            /* Beside the viewer binary still wins over the parent when both exist. */
            File.WriteAllText(missing, "{}");
            Assert.Equal(missing, ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory, installedServiceFolder: () => null));
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", savedConfigEnvironment);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void TryLoad_MissingFile_ReturnsNull()
    {
        Assert.Null(ViewerSettings.TryLoad(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName(), "darling.json")));
    }
}

/// <summary>Status colors: SUCCESS green, PERMISSIONS orange, ERROR red, anything else default.</summary>
public sealed class StatusToBrushConverterTests
{
    private static Color ColorFor(string? status)
    {
        var converter = new StatusToBrushConverter();
        var brush = Assert.IsType<SolidColorBrush>(converter.Convert(status, typeof(Brush), null, CultureInfo.InvariantCulture));
        return brush.Color;
    }

    [Fact]
    public void Convert_MapsTheThreeStatuses_ToDistinctColors_CaseInsensitively()
    {
        var success = ColorFor("SUCCESS");
        var permissions = ColorFor("PERMISSIONS");
        var error = ColorFor("ERROR");
        var other = ColorFor("SKIPPED");

        Assert.NotEqual(success, permissions);
        Assert.NotEqual(success, error);
        Assert.NotEqual(permissions, error);
        Assert.NotEqual(success, other);

        Assert.Equal(success, ColorFor("success"));
        Assert.Equal(error, ColorFor("Error"));
        Assert.Equal(other, ColorFor(null));
    }
}

/// <summary>
/// The viewer's read-only degradation (V8 security hardening): the authoritative capability probe
/// (has_table_privilege on a config table, resolved through search_path) and the friendly
/// read-only exception the write paths translate a Postgres 42501 into. The live admin-writes /
/// viewer-denied round-trip is in the gated <c>DarlingSecuritySplitLiveTests</c>.
/// </summary>
public sealed class ViewerReadOnlyTests
{
    [Fact]
    public void ReadOnlyProbeSql_TestsTheAlertDismissWrite()
    {
        /* has_table_privilege on config_alert_log UPDATE (the alert-dismiss write): true => writable
           (admin/owner), false => read-only viewer. The bare name resolves to config.config_alert_log via
           search_path. It probed config_mute_rules INSERT until #3450 granted the viewer ROLE exactly that
           privilege for the web dashboard's mute-rule endpoints — which would have offered a connectAs =
           "viewer" seat the dismiss buttons its writes still 42501 on — so the probe moved to a write the
           viewer role never holds, one the gated surfaces actually perform. */
        Assert.Equal("SELECT has_table_privilege('config_alert_log', 'UPDATE')", ViewerDataService.ReadOnlyProbeSql);
    }

    [Fact]
    public async System.Threading.Tasks.Task IsReadOnly_DefaultsFalse_BeforeProbing()
    {
        /* Defaults writable until DetectReadOnlyAsync runs (which fails safe to read-only). The
           constructor only creates the pooled data source; no connection is opened here. */
        await using var service = new ViewerDataService("Host=localhost;Port=1;Database=darling;Timeout=1");
        Assert.False(service.IsReadOnly);
    }

    [Fact]
    public void ViewerReadOnlyException_ExplainsHowToEnableWrites()
    {
        var ex = new ViewerReadOnlyException(new InvalidOperationException("42501"));

        Assert.Contains("read-only", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("connectAs", ex.Message, StringComparison.Ordinal);
        /* connectAs is ignored on a bring-your-own store, so that case is told where its login lives (#7077). */
        Assert.Contains("postgres.connectionString", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("mute rules", ex.Message, StringComparison.Ordinal);
        Assert.Equal("42501", ex.InnerException?.Message);
    }

    [Fact]
    public async System.Threading.Tasks.Task InvestigationInputRefusal_ShownWithoutTheParameterName()
    {
        /* The refusal is thrown before any connection is opened, so no store is needed. The window shows
           InputErrorText(ex), not ex.Message, which ends " (Parameter 'reason')" (#7077). */
        await using var service = new ViewerDataService("Host=localhost;Port=1;Database=darling;Timeout=1");
        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => service.CloseInvestigationRequestAsync(Guid.NewGuid(), "tester", ""));

        Assert.Contains("(Parameter 'reason')", ex.Message, StringComparison.Ordinal);
        var shown = ViewerDataService.InputErrorText(ex);
        Assert.DoesNotContain("Parameter", shown, StringComparison.Ordinal);
        Assert.StartsWith(ex.Message[..20], shown, StringComparison.Ordinal);
        Assert.Equal("plain failure", ViewerDataService.InputErrorText(new InvalidOperationException("plain failure")));
    }
}

/// <summary>
/// The connect-time schema-skew gate (finding B1): the viewer probes the store's effective version via
/// <c>information_schema</c> (the owner-only <c>darling_schema_version</c> table can't be read) and blocks a
/// store behind the version this build needs, instead of letting a control-plane write throw a raw 42703.
/// </summary>
public sealed class ViewerSchemaVersionGateTests
{
    /// <summary>
    /// The Collection Health tab reads the store's schema version once per session (#4767), not on every
    /// refresh. The version probe is one round trip of about 130 EXISTS arms, and the tab refreshes every 30
    /// seconds; the Query Store and trend reads already go through <c>_cachedStoreSchemaVersion</c>. There is
    /// no seam that counts probe round trips without a live store, so this pins the call shape: every call
    /// to <c>GetStoreSchemaVersionAsync</c> in the tab's read file is the cached-field form, which keeps
    /// reading again only while the result is null.
    /// </summary>
    [Fact]
    public void TheCollectionHealthTab_ReadsTheSchemaVersionThroughTheCachedField()
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(ThisFile())!, "..", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.CollectionHealth.cs"));
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

        var searchFrom = 0;
        var calls = 0;
        while (true)
        {
            var at = code.IndexOf("GetStoreSchemaVersionAsync(", searchFrom, StringComparison.Ordinal);
            if (at < 0)
            {
                break;
            }

            calls++;
            var before = code[Math.Max(0, at - 80)..at];
            Assert.EndsWith("_cachedStoreSchemaVersion ??= await ", before.TrimEnd() + " ", StringComparison.Ordinal);
            searchFrom = at + 1;
        }

        Assert.True(calls > 0, "the tab no longer reads the schema version; retire this pin with the read");
    }

    /// <summary>
    /// A build that reads the run-time column V160 added refuses a store without it at connect (#4938). The Collector Schedules
    /// read and save name that column, so a store below V160 would answer them with a raw "column does not exist" error. It never
    /// gets that far: the connect gate in <c>MainWindow</c> reads the probed version, and a store below the build's version
    /// stops the load with a message that says to update or restart the service, before any tab or editor loads. That is why
    /// the viewer's schedule read has no older-store fallback of its own, the way the CLI's read-back has one (the CLI works
    /// against a store older than its binary and the viewer does not). This pins the three parts: the probe carries the column,
    /// the map puts a store without it below the required version, and the gate stops on a store below it. Relaxing the gate to
    /// open an older store fails this, and the read then needs the fallback.
    ///
    /// <para>The probe is the one thing that can fail open: a probe that throws returns no version and the gate does not block
    /// (a healthy store is never refused for a probe hiccup). That path is not covered here, and a store below V160 on it
    /// meets the write executors' skew translation.</para>
    /// </summary>
    [Fact]
    public void ABuildThatReadsTheRunTimeColumn_RefusesAStoreWithoutIt_AtConnect()
    {
        /* The schedule read and save never name the run time (the schedule Save deletes and re-inserts a scope's rows, so a column
           there would be cleared), and the run-time read and write name the table the V160 rung created. */
        Assert.DoesNotContain("run_at_minute", ViewerDataService.CollectorSchedulesSelectSql, StringComparison.Ordinal);
        Assert.DoesNotContain("run_at_minute", ViewerDataService.CollectorScheduleFleetUpsertSql, StringComparison.Ordinal);
        Assert.DoesNotContain("run_at_minute", ViewerDataService.CollectorScheduleServerUpsertSql, StringComparison.Ordinal);
        Assert.Contains("FROM config.config_collector_run_times", ViewerDataService.CollectorRunTimesSelectSql, StringComparison.Ordinal);
        Assert.Contains("INTO config.config_collector_run_times", ViewerDataService.CollectorRunTimeServerUpsertSql, StringComparison.Ordinal);

        /* The probe carries the table as a sentinel. */
        Assert.Contains(
            "to_regclass('config.config_collector_run_times') IS NOT NULL",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        /* The gate: a probed version below the required one shows the failure and returns, and it does so before the first tab
           loads from the store. */
        var main = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));
        var gate = System.Text.RegularExpressions.Regex.Match(
            main,
            @"version < ViewerDataService\.RequiredStoreSchemaVersion\)\s*\{.*?ShowConnectionFailure\(.*?\);\s*return;\s*\}",
            System.Text.RegularExpressions.RegexOptions.Singleline);
        Assert.True(gate.Success, "MainWindow no longer refuses a store below the required version at connect");

        var firstTabLoad = main.IndexOf("AlertsHistoryContent.Initialize(", StringComparison.Ordinal);
        Assert.True(firstTabLoad > gate.Index, "the version gate must come before the first tab loads from the store");
    }

    private static string ThisFile([System.Runtime.CompilerServices.CallerFilePath] string path = "") => path;

    [Fact]
    public void StoreSchemaProbeSql_ProbesInformationSchema_ForTheV17ToV25Sentinels()
    {
        var sql = ViewerDataService.StoreSchemaProbeSql;

        /* Reads information_schema (visible to every role) — not the owner-only darling_schema_version. */
        Assert.Contains("information_schema.tables", sql, StringComparison.Ordinal);
        Assert.Contains("information_schema.columns", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("darling_schema_version", sql, StringComparison.Ordinal);

        /* The version sentinels: V17 config control plane, V18 delivery-override column, V19 marker,
           V20 tuning knobs, V21 default_trace_events, V22 idx_index_object_stats_latest. */
        Assert.Contains("config_monitored_servers", sql, StringComparison.Ordinal);
        Assert.Contains("alert_delivery_mode_override", sql, StringComparison.Ordinal);
        Assert.Contains("analysis_state", sql, StringComparison.Ordinal);
        Assert.Contains("config_alert_settings", sql, StringComparison.Ordinal);
        Assert.Contains("notify_connection_changes", sql, StringComparison.Ordinal);
        Assert.Contains("default_trace_events", sql, StringComparison.Ordinal);
        /* V22's sentinel is an INDEX — Postgres has no index listing in information_schema, so the probe reads
           the world-readable pg_indexes catalog for it. */
        Assert.Contains("pg_indexes", sql, StringComparison.Ordinal);
        Assert.Contains("idx_index_object_stats_latest", sql, StringComparison.Ordinal);

        /* V23's sentinel is collection_log-IS-a-hypertable OR no-timescaledb. It MUST stay plain-PostgreSQL-safe:
           it reads the core pg_inherits catalog for collection_log's chunk children (TimescaleDB 2.x links chunks
           to the hypertable root via inheritance; NOT timescaledb_information.hypertables, which does not exist
           without the extension and would make the whole probe throw on plain PG — and NOT the ts_insert_blocker
           trigger, which TimescaleDB 2.x no longer creates), OR'd with a pg_extension check so a plain-PG store at
           V23 (an object-invisible no-op there) isn't gated. */
        Assert.Contains("pg_inherits", sql, StringComparison.Ordinal);
        Assert.Contains("inhparent", sql, StringComparison.Ordinal);
        Assert.Contains("collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("pg_extension", sql, StringComparison.Ordinal);
        Assert.Contains("timescaledb", sql, StringComparison.Ordinal);
        /* Never the extension-only view — that would throw on plain PG and take the whole probe (and gate) down. */
        Assert.DoesNotContain("timescaledb_information", sql, StringComparison.Ordinal);

        /* V24/V25 (#1433): job_history + agent_status table-existence sentinels (engine-agnostic). */
        Assert.Contains("job_history", sql, StringComparison.Ordinal);
        Assert.Contains("agent_status", sql, StringComparison.Ordinal);

        /* V26 (#1506): the generic webhook's generic_url column on config_notification. */
        Assert.Contains("generic_url", sql, StringComparison.Ordinal);

        /* V27 (#1535): the Azure per-database watermark key on deadlocks. */
        Assert.Contains("database_name", sql, StringComparison.Ordinal);
        Assert.Contains("deadlocks", sql, StringComparison.Ordinal);

        /* V30 (#1562) web dashboard toggle + V31 (#1563) custom_views — the two newest sentinels. */
        Assert.Contains("web_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("custom_views", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, 32)]  // fully migrated V32 (config.server_tags, fleet tags)
    [InlineData(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, 31)]  // V31: config.custom_views present, server_tags not yet (#1563)
    [InlineData(true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, 30)]  // V30: config_service web_enabled present, custom_views not yet (#1562)
    [InlineData(true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, 29)]  // V29: long_query_completions present, web config not yet (#1496)
    [InlineData(true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, 28)]  // V28: query_store_stats replica_role present, long_query_completions not yet (#1546)
    [InlineData(true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, 27)] // V27: deadlocks database_name present, replica_role not yet (#1535)
    [InlineData(true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, 26)] // V26: generic webhook present, deadlocks column not yet
    [InlineData(true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, 25)] // V25: agent_status present, generic webhook not yet
    [InlineData(true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, 24)] // V24: job_history present, agent_status not yet
    [InlineData(true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, 23)]    // V23 (collection_log hypertable, or plain-PG at V23)
    [InlineData(true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false, 22)]   // Timescale store still at V22 (index present, collection_log not yet a hypertable)
    [InlineData(true, true, true, true, true, false, false, false, false, false, false, false, false, false, false, false, 21)]  // pre-V22: has default_trace_events, no V22 index
    [InlineData(true, true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, 20)] // pre-V21: no default_trace_events
    [InlineData(true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, 19)]// pre-V20: no alert-tuning knobs
    [InlineData(true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, 18)]// pre-V19: no analysis_state
    [InlineData(true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, 17)]// pre-V18: no delivery-override column
    [InlineData(false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, 16)]// pre-V17: no config control plane at all
    public void MapProbedSchemaVersion_TakesTheHighestSatisfiedSentinel(
        bool hasConfigControlPlane, bool hasAlertDeliveryOverride, bool hasAnalysisState, bool hasAlertTuningKnobs, bool hasDefaultTraceEvents, bool hasIndexObjectStatsLatestIndex, bool hasCollectionLogHypertableOrPlainPg, bool hasJobHistory, bool hasAgentStatus, bool hasGenericWebhook, bool hasDeadlocksDatabaseName, bool hasQueryStoreReplicaRole, bool hasLongQueryCompletions, bool hasWebDashboardConfig, bool hasCustomViews, bool hasServerTags, int expected)
    {
        Assert.Equal(expected, ViewerDataService.MapProbedSchemaVersion(
            hasConfigControlPlane, hasAlertDeliveryOverride, hasAnalysisState, hasAlertTuningKnobs, hasDefaultTraceEvents, hasIndexObjectStatsLatestIndex, hasCollectionLogHypertableOrPlainPg, hasJobHistory, hasAgentStatus, hasGenericWebhook, hasDeadlocksDatabaseName, hasQueryStoreReplicaRole, hasLongQueryCompletions, hasWebDashboardConfig, hasCustomViews, hasServerTags));
    }

    /// <summary>An upgraded store mid-state: fleet tags present (V32) but the #1659 knobs not yet — the
    /// probe reports 32, not 33, so the gate correctly blocks a V33 viewer until the service migrates.</summary>
    [Fact]
    public void MapProbedSchemaVersion_V33KnobsAbsent_CapsAt32() =>
        Assert.Equal(32, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>The next rung: the #1659 knobs present (V33) but neither #991 sentinel yet — the probe reports
    /// 33, so the gate blocks a V34/V35 viewer until the service migrates.</summary>
    [Fact]
    public void MapProbedSchemaVersion_AgSentinelsAbsent_CapsAt33() =>
        Assert.Equal(33, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>The rung that only exists because #991 shipped as two stacked migrations: the AG collector
    /// TABLES are present (V34) but the AG ALERT knobs are not (V35). The probe must report 34, not 35 — a
    /// store that took the collector migration and not the alert one is genuinely mid-upgrade, and reporting
    /// 35 would let a V35 viewer open against a store whose config_alert_settings has no notify_ag_health.</summary>
    [Fact]
    public void MapProbedSchemaVersion_AgCollectorsPresentButAlertKnobsAbsent_CapsAt34() =>
        Assert.Equal(34, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>The #1767 rung: every sentinel through V37 present but query_plan_dim absent — the probe must
    /// report 37, so the gate blocks a V38 viewer until the service migrates. Gating this matters more than
    /// most: at V38 the collectors stop writing query_text/query_plan_xml inline, so a viewer that opened
    /// against a store it thought was current would read NULL for every new row's text and plan and show
    /// nothing, with no error to explain it.</summary>
    [Fact]
    public void MapProbedSchemaVersion_PayloadDimensionsAbsent_CapsAt37() =>
        Assert.Equal(37, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>The #1839 rung: every sentinel through V39 present but blocking_wait_seconds_threshold absent —
    /// the probe must report 39, so the gate blocks a V40 viewer until the service migrates. Gating matters
    /// here because the viewer's Settings window names that column in both its alert-settings SELECT and its
    /// upsert: against a V39 store the read would fail outright with 42703 rather than degrade.</summary>
    [Fact]
    public void MapProbedSchemaVersion_BlockingWaitThresholdAbsent_CapsAt39() =>
        Assert.Equal(39, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    [Fact]
    public void MapProbedSchemaVersion_V23CompositeIsGatedBehindV22_NotAStandaloneTopArm()
    {
        /* A plain-PG store BELOW V22 has the no-extension arm TRUE (so hasCollectionLogHypertableOrPlainPg=true)
           but must NOT be reported as 23 — the composite only counts once the engine-agnostic V22 index is
           present. Here: pre-V22 (no index) with the composite true still maps to 21 (its real V21 sentinel),
           proving the composite is gated behind V22 rather than treated as a newest-first arm. */
        Assert.Equal(21, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, false, true, false, false, false, false, false, false, false, false, false));
    }

    /// <summary>The #1962 rung: every sentinel through V43 present but collector_state absent — the probe
    /// must report 43, so a V43 store is correctly seen as one migration behind. This rung is unusual in
    /// that the viewer never reads collector_state (service-only state, no view, no viewer query), so
    /// nothing here is about a failing read: it exists so a FULLY-migrated store maps to 44 instead of
    /// capping at 43, which is what would make the connect-time gate refuse every healthy store.</summary>
    [Fact]
    public void MapProbedSchemaVersion_CollectorStateAbsent_CapsAt43() =>
        Assert.Equal(43, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>V46 (#1952) rung: plan_correction absent means the store stops at the V44 rung. Unlike the
    /// V44 arm above, the viewer DOES read this table (the Plan Corrections and Automatic Tuning grids), so a
    /// store below V46 genuinely cannot serve those reads — the cap is a real gate here, not only a
    /// don't-under-report guard. Note the permanent V45 gap: 45 is not a rung and never will be (#1951 was
    /// renumbered to 47), so no store can carry one.</summary>
    [Fact]
    public void MapProbedSchemaVersion_PlanCorrectionAbsent_CapsAt44() =>
        Assert.Equal(44, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>V47 (#1951) rung: pvs_stats absent means the store stops at the V46 rung. The viewer READS
    /// this table (the FinOps Version Store grid names it), so a store below V47 would fail 42P01 rather
    /// than degrade — the cap is a real gate here too.</summary>
    [Fact]
    public void MapProbedSchemaVersion_PvsStatsAbsent_CapsAt46() =>
        Assert.Equal(46, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>V48 (#1984) rung: every sentinel through V47 present but pvs_enabled absent — the probe
    /// must report 47, so the gate blocks a V48 viewer until the service migrates. Gating matters for the
    /// same reason as the V40 rung: the Settings window names all three PVS-pressure columns in its
    /// alert-settings SELECT and upsert, so against a V47 store the read would fail outright with 42703
    /// rather than degrade.</summary>
    [Fact]
    public void MapProbedSchemaVersion_PvsPressureKnobsAbsent_CapsAt47() =>
        Assert.Equal(47, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>V50 (#2008 2a) rung: every sentinel through V49 present but the server_tags.colour column
    /// absent — the probe must report 49, so a store one migration behind maps to 49 not 50. The viewer names
    /// the colour column in its tag SELECT, so the cap is a real gate: a V49 store would fail 42703.</summary>
    [Fact]
    public void MapProbedSchemaVersion_ServerTagColourAbsent_CapsAt49() =>
        Assert.Equal(49, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    /// <summary>V53 (#2068) rung: every sentinel through V52 present but collect.store_metrics absent — the
    /// probe must report 52, so a store one migration behind maps to 52 not 53. Like the V44 rung the viewer
    /// never reads this table (the service's own hourly capacity series, surfaced over MCP/REST), so the arm
    /// is the don't-under-report guard, not a failing-read gate.</summary>
    [Fact]
    public void MapProbedSchemaVersion_StoreMetricsAbsent_CapsAt52() =>
        Assert.Equal(52, ViewerDataService.MapProbedSchemaVersion(true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true));

    [Fact]
    public void RequiredStoreSchemaVersion_TracksTheBuildSchemaVersion_AndTheProbeCoversIt()
    {
        /* The gate compares against the build's known schema version. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        /* Pin: a fully-migrated store (all sentinels present) must map to exactly the required version. If a
           future migration bumps StorageVersion, this fails until a matching sentinel + map arm is added —
           the guard against the probe silently under-reporting a newer store as skewed, which would make
           the connect-time gate refuse to open the viewer against a perfectly healthy store.

           Built by REFLECTION so the call's arity tracks the signature: the literal-true form silently
           defaults every newly added sentinel parameter to false, maps one version low, and fails this test
           on every probe extension — it broke on the V61 bump and again on the V61+V62 merge. All-true IS
           the contract here (a fully-migrated store has every sentinel), so the arity is the only thing the
           literals ever expressed. */
        var map = typeof(ViewerDataService).GetMethod(
            nameof(ViewerDataService.MapProbedSchemaVersion),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var allTrue = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        Assert.Equal(ViewerDataService.RequiredStoreSchemaVersion, (int)map.Invoke(null, allTrue)!);

        /* #9043: the V281 sentinel (the Query Store text liveness table) is the one before the last, so a store that has every sentinel but
           that one and V282's is a V280 store and maps to 280, not 281. #8570: the V280 sentinel (the plan_correction recommendation index) is the one
           before it, so a store without it (and without V281's) is a V279 store. The V279 sentinel (the aggregate compression order history) is the one before it,
           so a store without it (and without V280's) is a V278 store. #8865: the V278 sentinel (the error-log facts on the database protection rows) is
           the one before it, so a store without it (and without V279's) is a V277 store. #8967: the V277 sentinel (the progress table of the attention_events
           copy) is the one before that, so a store without it (and without the two above) is a V276 store. #9000: the V276 sentinel (the Store Growth
           settings on the alert settings) is the one before that, #8737: the V275 sentinel (the personal token table) the one before that,
           and #8978: the V274 sentinel the one before that. */
        /* #9110: the V282 sentinel (the job outcome daily rollup's tables) is the one before V283's, so a store that has every sentinel but
           that one and the five above it is a V281 store and maps to 281, not 282. */
        /* #9164: the V287 sentinel (the hourly host rollup's tables) is the one before the last, so a store that has every sentinel but that one and V288's is a
           V286 store and maps to 286, not 287. #9147: the V286 sentinel (the query_stats per-collection summary table) is the one before it, so a
           store without it (and without V287's) is a V285 store. #9148: the V285 sentinel (the Query Store clutter rollups' tables) is the one before it,
           so a store without it (and without V286's) is a V284 store. #9149: the V284 sentinel (the covering hash index on the interval hourly plan
           cache rollup) is the one before that, so a store without it (and without the two above) is a V283 store. #9111: the V283 sentinel (the Query
           Store closed-day rollups' tables) is the one before that. Each sentinel below shifts one place from the end. */
        /* #9151: the V288 sentinel (the covering index on the collector log's watermark index) is the one before the last, so a store that has every sentinel but
           that one and V289's is a V287 store and maps to 287, not 288. Each sentinel below shifts one place from the end. */
        /* #9173: the V289 sentinel (the hourly Query Store clutter rollups' tables) is the one before the last, so a store that has every sentinel but
           that one and V290's is a V288 store and maps to 288, not 289. Each sentinel below shifts one place from the end. */
        /* #9223: the V292 sentinel (the covering hash index on the corrected daily Query Store rollup) is the one before the last, so a store that has every
           sentinel but that one and V293's, V294's and V295's is a V291 store and maps to 291, not 292. Each sentinel below shifts one place from the end. */
        /* #9863: the V322 sentinel (the hourly host rollup's hash index is gone) is the last parameter, so a store that has every sentinel but that one is a V321 store
           and maps to 321, not 322. Each sentinel below shifts one place from the end. */
        var allButV322 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV322[^1] = false;
        Assert.Equal("hasHostHourlyHashIndexDropped", map.GetParameters()[^1].Name);
        Assert.Equal(321, (int)map.Invoke(null, allButV322)!);

        /* #9923: the V321 sentinel (the connection open columns on the slow read record) is the one before the last, so a store that has every sentinel but that one and V322's is a V320 store
           and maps to 320, not 321. Each sentinel below shifts one place from the end. */
        var allButV321 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV321[^1] = false;
        allButV321[^2] = false;
        Assert.Equal("hasSlowReadConnectionOpens", map.GetParameters()[^2].Name);
        Assert.Equal(320, (int)map.Invoke(null, allButV321)!);

        /* #9705: the V320 sentinel (the slim query-tag sightings table) is the last parameter, so a store that has every sentinel but that one is a V319 store
           and maps to 319, not 320. Each sentinel below shifts one place from the end. */
        var allButV320 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV320[^1] = false;
        allButV320[^2] = false;
        allButV320[^3] = false;
        Assert.Equal(319, (int)map.Invoke(null, allButV320)!);

        /* #9662: the V319 sentinel (the three argument advanced web access setter) is the last parameter, so a store that has every sentinel but that one is a V318 store
           and maps to 318, not 319. Each sentinel below shifts one place from the end. */
        var allButV319 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV319[^1] = false;
        allButV319[^2] = false;
        allButV319[^3] = false;
        allButV319[^4] = false;
        Assert.Equal("hasWebAccessAdvancedActor", map.GetParameters()[^4].Name);
        Assert.Equal(318, (int)map.Invoke(null, allButV319)!);

        /* #9485: the V318 sentinel (the planned change windows table) is the last parameter, so a store that has every sentinel but that one is a V317 store
           and maps to 317, not 318. Each sentinel below shifts one place from the end. */
        var allButV318 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV318[^1] = false;
        allButV318[^2] = false;
        allButV318[^3] = false;
        Assert.Equal("hasInvestigationChangeWindows", map.GetParameters()[^5].Name);
        allButV318[^4] = false;
        allButV318[^5] = false;
        Assert.Equal(317, (int)map.Invoke(null, allButV318)!);

        /* #9433: the V317 sentinel (the reply size columns) is the last parameter, so a store that has every sentinel but that one is a V316 store
           and maps to 316, not 317. Each sentinel below shifts one place from the end. */
        var allButV317 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV317[^1] = false;
        allButV317[^2] = false;
        allButV317[^3] = false;
        allButV317[^4] = false;
        Assert.Equal("hasReadReplyBytes", map.GetParameters()[^6].Name);
        allButV317[^5] = false;
        allButV317[^6] = false;
        Assert.Equal(316, (int)map.Invoke(null, allButV317)!);

        /* #9439: the V316 sentinel (the lock waits table) is the last parameter, so a store that has every sentinel but that one is a V315 store
           and maps to 315, not 316. Each sentinel below shifts one place from the end. */
        var allButV316 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV316[^1] = false;
        allButV316[^2] = false;
        allButV316[^3] = false;
        allButV316[^4] = false;
        allButV316[^5] = false;
        Assert.Equal("hasStoreLockWaits", map.GetParameters()[^7].Name);
        allButV316[^6] = false;
        allButV316[^7] = false;
        Assert.Equal(315, (int)map.Invoke(null, allButV316)!);

        /* #9434: the V315 sentinel (the read_latency unique key index) is the last parameter, so a store that has every sentinel but that one is a V314 store
           and maps to 314, not 315. Each sentinel below shifts one place from the end. */
        var allButV315 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV315[^1] = false;
        allButV315[^2] = false;
        allButV315[^3] = false;
        allButV315[^4] = false;
        allButV315[^5] = false;
        allButV315[^6] = false;
        Assert.Equal("hasReadLatencyProcessStart", map.GetParameters()[^8].Name);
        allButV315[^7] = false;
        allButV315[^8] = false;
        Assert.Equal(314, (int)map.Invoke(null, allButV315)!);

        /* #9437: the V314 sentinel (the two store connection tables) is the last parameter, so a store that has every sentinel but that one is a V313 store
           and maps to 313, not 314. Each sentinel below shifts one place from the end. */
        var allButV314 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV314[^1] = false;
        allButV314[^2] = false;
        allButV314[^3] = false;
        allButV314[^4] = false;
        allButV314[^5] = false;
        allButV314[^6] = false;
        allButV314[^7] = false;
        Assert.Equal("hasStoreConnectionSamples", map.GetParameters()[^9].Name);
        allButV314[^8] = false;
        allButV314[^9] = false;
        Assert.Equal(313, (int)map.Invoke(null, allButV314)!);

        /* #9427: the V313 sentinel (the store setting state and changes tables) is the last parameter, so a store that has every sentinel but that one is a V312 store
           and maps to 312, not 313. Each sentinel below shifts one place from the end. */
        var allButV313 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV313[^1] = false;
        allButV313[^2] = false;
        allButV313[^3] = false;
        allButV313[^4] = false;
        allButV313[^5] = false;
        allButV313[^6] = false;
        allButV313[^7] = false;
        allButV313[^8] = false;
        Assert.Equal("hasStoreSettingChanges", map.GetParameters()[^10].Name);
        allButV313[^9] = false;
        allButV313[^10] = false;
        Assert.Equal(312, (int)map.Invoke(null, allButV313)!);

        /* #9432: the V312 sentinel (the upgrade run table) is the last parameter, so a store that has every sentinel but that one is a V311 store
           and maps to 311, not 312. Each sentinel below shifts one place from the end. */
        var allButV312 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV312[^1] = false;
        allButV312[^2] = false;
        allButV312[^3] = false;
        allButV312[^4] = false;
        allButV312[^5] = false;
        allButV312[^6] = false;
        allButV312[^7] = false;
        allButV312[^8] = false;
        allButV312[^9] = false;
        Assert.Equal("hasUpgradeRuns", map.GetParameters()[^11].Name);
        allButV312[^10] = false;
        allButV312[^11] = false;
        Assert.Equal(311, (int)map.Invoke(null, allButV312)!);

        /* #9438: the V311 sentinel (the migration rung object table) is the last parameter, so a store that has every sentinel but that one is a V310 store
           and maps to 310, not 311. Each sentinel below shifts one place from the end. */
        var allButV311 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV311[^1] = false;
        allButV311[^2] = false;
        allButV311[^3] = false;
        allButV311[^4] = false;
        allButV311[^5] = false;
        allButV311[^6] = false;
        allButV311[^7] = false;
        allButV311[^8] = false;
        allButV311[^9] = false;
        allButV311[^10] = false;
        Assert.Equal("hasServiceStartRungObjects", map.GetParameters()[^12].Name);
        allButV311[^11] = false;
        allButV311[^12] = false;
        Assert.Equal(310, (int)map.Invoke(null, allButV311)!);

        /* #9452: the V310 sentinel (the partial index of the oversized-plan backlog's pending rows) is the last parameter, so a store that has every sentinel but that
           one is a V309 store and maps to 309, not 310. Each sentinel below shifts one place from the end. */
        var allButV310 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV310[^1] = false;
        allButV310[^2] = false;
        allButV310[^3] = false;
        allButV310[^4] = false;
        allButV310[^5] = false;
        allButV310[^6] = false;
        allButV310[^7] = false;
        allButV310[^8] = false;
        allButV310[^9] = false;
        allButV310[^10] = false;
        allButV310[^11] = false;
        Assert.Equal("hasOversizedPlanBacklogPendingIndex", map.GetParameters()[^13].Name);
        allButV310[^12] = false;
        allButV310[^13] = false;
        Assert.Equal(309, (int)map.Invoke(null, allButV310)!);

        /* #9414: the V309 sentinel (the store checkpoint rows' table) is the one before the last, so a store that has every sentinel but that one and V310's is a V308 store
           and maps to 308, not 309. Each sentinel below shifts one place from the end. */
        var allButV309 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV309[^1] = false;
        allButV309[^2] = false;
        allButV309[^3] = false;
        allButV309[^4] = false;
        allButV309[^5] = false;
        allButV309[^6] = false;
        allButV309[^7] = false;
        allButV309[^8] = false;
        allButV309[^9] = false;
        allButV309[^10] = false;
        allButV309[^11] = false;
        allButV309[^12] = false;
        Assert.Equal("hasStoreCheckpoints", map.GetParameters()[^14].Name);
        allButV309[^13] = false;
        allButV309[^14] = false;
        Assert.Equal(308, (int)map.Invoke(null, allButV309)!);

        /* #9413: the V308 sentinel (the per-table activity tables) is the last parameter, so a store that has every sentinel but that one is a V307 store
           and maps to 307, not 308. Each sentinel below shifts one place from the end. */
        var allButV308 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV308[^1] = false;
        allButV308[^2] = false;
        allButV308[^3] = false;
        allButV308[^4] = false;
        allButV308[^5] = false;
        allButV308[^6] = false;
        allButV308[^7] = false;
        allButV308[^8] = false;
        allButV308[^9] = false;
        allButV308[^10] = false;
        allButV308[^11] = false;
        allButV308[^12] = false;
        allButV308[^13] = false;
        Assert.Equal("hasStoreTableActivity", map.GetParameters()[^15].Name);
        allButV308[^14] = false;
        allButV308[^15] = false;
        Assert.Equal(307, (int)map.Invoke(null, allButV308)!);

        /* #9415: the V307 sentinel (the service start history's tables) is the last parameter, so a store that has every sentinel but that one is a V306 store
           and maps to 306, not 307. Each sentinel below shifts one place from the end. */
        var allButV307 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV307[^1] = false;
        allButV307[^2] = false;
        allButV307[^3] = false;
        allButV307[^4] = false;
        allButV307[^5] = false;
        allButV307[^6] = false;
        allButV307[^7] = false;
        allButV307[^8] = false;
        allButV307[^9] = false;
        allButV307[^10] = false;
        allButV307[^11] = false;
        allButV307[^12] = false;
        allButV307[^13] = false;
        allButV307[^14] = false;
        Assert.Equal("hasServiceStartHistory", map.GetParameters()[^16].Name);
        allButV307[^15] = false;
        allButV307[^16] = false;
        Assert.Equal(306, (int)map.Invoke(null, allButV307)!);

        /* #9411: the V306 sentinel (the background pass step history table) is the last parameter, so a store that has every sentinel but that one is a V305 store
           and maps to 305, not 306. Each sentinel below shifts one place from the end. */
        var allButV306 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV306[^1] = false;
        allButV306[^2] = false;
        allButV306[^3] = false;
        allButV306[^4] = false;
        allButV306[^5] = false;
        allButV306[^6] = false;
        allButV306[^7] = false;
        allButV306[^8] = false;
        allButV306[^9] = false;
        allButV306[^10] = false;
        allButV306[^11] = false;
        allButV306[^12] = false;
        allButV306[^13] = false;
        allButV306[^14] = false;
        allButV306[^15] = false;
        Assert.Equal("hasStorePassSteps", map.GetParameters()[^17].Name);
        allButV306[^16] = false;
        allButV306[^17] = false;
        Assert.Equal(305, (int)map.Invoke(null, allButV306)!);

        /* #9419: the V305 sentinel (the WAL columns on the store metrics table) is the last parameter, so a store that has every sentinel but that one is a V304
           store and maps to 304, not 305. Each sentinel below shifts one place from the end. */
        var allButV305 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV305[^1] = false;
        allButV305[^2] = false;
        allButV305[^3] = false;
        allButV305[^4] = false;
        allButV305[^5] = false;
        allButV305[^6] = false;
        allButV305[^7] = false;
        allButV305[^8] = false;
        allButV305[^9] = false;
        allButV305[^10] = false;
        allButV305[^11] = false;
        allButV305[^12] = false;
        allButV305[^13] = false;
        allButV305[^14] = false;
        allButV305[^15] = false;
        allButV305[^16] = false;
        Assert.Equal("hasStoreWalFigures", map.GetParameters()[^18].Name);
        allButV305[^17] = false;
        allButV305[^18] = false;
        Assert.Equal(304, (int)map.Invoke(null, allButV305)!);

        /* #9410: the V304 sentinel (the statement history's write counters) is the last parameter, so a store that has every sentinel but that one is a V303 store
           and maps to 303, not 304. Each sentinel below shifts one place from the end. */
        var allButV304 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV304[^1] = false;
        allButV304[^2] = false;
        allButV304[^3] = false;
        allButV304[^4] = false;
        allButV304[^5] = false;
        allButV304[^6] = false;
        allButV304[^7] = false;
        allButV304[^8] = false;
        allButV304[^9] = false;
        allButV304[^10] = false;
        allButV304[^11] = false;
        allButV304[^12] = false;
        allButV304[^13] = false;
        allButV304[^14] = false;
        allButV304[^15] = false;
        allButV304[^16] = false;
        allButV304[^17] = false;
        Assert.Equal("hasStatementHistoryWriteCounters", map.GetParameters()[^19].Name);
        allButV304[^18] = false;
        allButV304[^19] = false;
        Assert.Equal(303, (int)map.Invoke(null, allButV304)!);

        /* #9384: the V303 sentinel (the clutter-day stamp table) is the last parameter, so a store that has every sentinel but that one is a V302 store
           and maps to 302, not 303. Each sentinel below shifts one place from the end. */
        var allButV303 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV303[^1] = false;
        allButV303[^2] = false;
        allButV303[^3] = false;
        allButV303[^4] = false;
        allButV303[^5] = false;
        allButV303[^6] = false;
        allButV303[^7] = false;
        allButV303[^8] = false;
        allButV303[^9] = false;
        allButV303[^10] = false;
        allButV303[^11] = false;
        allButV303[^12] = false;
        allButV303[^13] = false;
        allButV303[^14] = false;
        allButV303[^15] = false;
        allButV303[^16] = false;
        allButV303[^17] = false;
        allButV303[^18] = false;
        Assert.Equal("hasClutterDayBuilt", map.GetParameters()[^20].Name);
        allButV303[^19] = false;
        allButV303[^20] = false;
        Assert.Equal(302, (int)map.Invoke(null, allButV303)!);

        /* #8570: the V302 sentinel (the partial index of failed Agent job outcomes on job_history) is the last parameter, so a store that has every sentinel
           but that one is a V301 store and maps to 301, not 302. Each sentinel below shifts one place from the end. */
        var allButV302 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV302[^1] = false;
        allButV302[^2] = false;
        allButV302[^3] = false;
        allButV302[^4] = false;
        allButV302[^5] = false;
        allButV302[^6] = false;
        allButV302[^7] = false;
        allButV302[^8] = false;
        allButV302[^9] = false;
        allButV302[^10] = false;
        allButV302[^11] = false;
        allButV302[^12] = false;
        allButV302[^13] = false;
        allButV302[^14] = false;
        allButV302[^15] = false;
        allButV302[^16] = false;
        allButV302[^17] = false;
        allButV302[^18] = false;
        allButV302[^19] = false;
        Assert.Equal("hasJobHistoryFailedIndex", map.GetParameters()[^21].Name);
        allButV302[^20] = false;
        allButV302[^21] = false;
        Assert.Equal(301, (int)map.Invoke(null, allButV302)!);

        /* #9374: the V301 sentinel (the last measured relation sizes and their measured time) is the last parameter, so a store that has every sentinel but
           that one is a V300 store and maps to 300, not 301. Each sentinel below shifts one place from the end. */
        var allButV301 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV301[^1] = false;
        allButV301[^2] = false;
        allButV301[^3] = false;
        allButV301[^4] = false;
        allButV301[^5] = false;
        allButV301[^6] = false;
        allButV301[^7] = false;
        allButV301[^8] = false;
        allButV301[^9] = false;
        allButV301[^10] = false;
        allButV301[^11] = false;
        allButV301[^12] = false;
        allButV301[^13] = false;
        allButV301[^14] = false;
        allButV301[^15] = false;
        allButV301[^16] = false;
        allButV301[^17] = false;
        allButV301[^18] = false;
        allButV301[^19] = false;
        allButV301[^20] = false;
        allButV301[^21] = false;
        allButV301[^22] = false;
        Assert.Equal(300, (int)map.Invoke(null, allButV301)!);

        /* #9357: the V300 sentinel (the procedure_stats hour ledger's tables) is the one before the last, so a store that has every sentinel but that one
           and V301's is a V299 store and maps to 299, not 300. Each sentinel below shifts one place from the end. */
        var allButV300 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV300[^1] = false;
        allButV300[^2] = false;
        allButV300[^3] = false;
        allButV300[^4] = false;
        allButV300[^5] = false;
        allButV300[^6] = false;
        allButV300[^7] = false;
        allButV300[^8] = false;
        allButV300[^9] = false;
        allButV300[^10] = false;
        allButV300[^11] = false;
        allButV300[^12] = false;
        allButV300[^13] = false;
        allButV300[^14] = false;
        allButV300[^15] = false;
        allButV300[^16] = false;
        allButV300[^17] = false;
        allButV300[^18] = false;
        allButV300[^19] = false;
        allButV300[^20] = false;
        allButV300[^21] = false;
        allButV300[^22] = false;
        allButV300[^23] = false;
        Assert.Equal(299, (int)map.Invoke(null, allButV300)!);

        /* #9349: the V299 sentinel (the object usage chain's tables) is the last parameter, so a store that has every sentinel but that one
           is a V298 store and maps to 298, not 299. Each sentinel below shifts one place from the end. */
        var allButV299 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV299[^1] = false;
        allButV299[^2] = false;
        allButV299[^3] = false;
        allButV299[^4] = false;
        allButV299[^5] = false;
        allButV299[^6] = false;
        allButV299[^7] = false;
        allButV299[^8] = false;
        allButV299[^9] = false;
        allButV299[^10] = false;
        allButV299[^11] = false;
        allButV299[^12] = false;
        allButV299[^13] = false;
        allButV299[^14] = false;
        allButV299[^15] = false;
        allButV299[^16] = false;
        allButV299[^17] = false;
        allButV299[^18] = false;
        allButV299[^19] = false;
        allButV299[^20] = false;
        allButV299[^21] = false;
        allButV299[^22] = false;
        allButV299[^23] = false;
        allButV299[^24] = false;
        Assert.Equal(298, (int)map.Invoke(null, allButV299)!);

        /* #9304: the V298 sentinel (the payload dimension tables' stamp tables) is the last parameter, so a store that has every sentinel but that one
           is a V297 store and maps to 297, not 298. Each sentinel below shifts one place from the end. */
        var allButV298 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV298[^1] = false;
        allButV298[^2] = false;
        allButV298[^3] = false;
        allButV298[^4] = false;
        allButV298[^5] = false;
        allButV298[^6] = false;
        allButV298[^7] = false;
        allButV298[^8] = false;
        allButV298[^9] = false;
        allButV298[^10] = false;
        allButV298[^11] = false;
        allButV298[^12] = false;
        allButV298[^13] = false;
        allButV298[^14] = false;
        allButV298[^15] = false;
        allButV298[^16] = false;
        allButV298[^17] = false;
        allButV298[^18] = false;
        allButV298[^19] = false;
        allButV298[^20] = false;
        allButV298[^21] = false;
        allButV298[^22] = false;
        allButV298[^23] = false;
        allButV298[^24] = false;
        allButV298[^25] = false;
        Assert.Equal(297, (int)map.Invoke(null, allButV298)!);

        /* #8784 part 2: the V297 sentinel (the dependency list and the per-day referenced-names summary) is the last parameter, so a store that has
           every sentinel but that one is a V296 store and maps to 296, not 297. Each sentinel below shifts one place from the end. */
        var allButV297 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV297[^1] = false;
        allButV297[^2] = false;
        allButV297[^3] = false;
        allButV297[^4] = false;
        allButV297[^5] = false;
        allButV297[^6] = false;
        allButV297[^7] = false;
        allButV297[^8] = false;
        allButV297[^9] = false;
        allButV297[^10] = false;
        allButV297[^11] = false;
        allButV297[^12] = false;
        allButV297[^13] = false;
        allButV297[^14] = false;
        allButV297[^15] = false;
        allButV297[^16] = false;
        allButV297[^17] = false;
        allButV297[^18] = false;
        allButV297[^19] = false;
        allButV297[^20] = false;
        allButV297[^21] = false;
        allButV297[^22] = false;
        allButV297[^23] = false;
        allButV297[^24] = false;
        allButV297[^25] = false;
        allButV297[^26] = false;
        Assert.Equal(296, (int)map.Invoke(null, allButV297)!);

        /* #9280: the V296 sentinel (the hourly Query Store summary's index) is the one before the last, so a store that has every sentinel but that
           one and V297's is a V295 store and maps to 295, not 296. Each sentinel below shifts one place from the end. */
        var allButV296 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV296[^1] = false;
        allButV296[^2] = false;
        allButV296[^3] = false;
        allButV296[^4] = false;
        allButV296[^5] = false;
        allButV296[^6] = false;
        allButV296[^7] = false;
        allButV296[^8] = false;
        allButV296[^9] = false;
        allButV296[^10] = false;
        allButV296[^11] = false;
        allButV296[^12] = false;
        allButV296[^13] = false;
        allButV296[^14] = false;
        allButV296[^15] = false;
        allButV296[^16] = false;
        allButV296[^17] = false;
        allButV296[^18] = false;
        allButV296[^19] = false;
        allButV296[^20] = false;
        allButV296[^21] = false;
        allButV296[^22] = false;
        allButV296[^23] = false;
        allButV296[^24] = false;
        allButV296[^25] = false;
        allButV296[^26] = false;
        allButV296[^27] = false;
        Assert.Equal(295, (int)map.Invoke(null, allButV296)!);

        /* #9225: the V295 sentinel (the plan regression block tables) is the one before the last, so a store that has every sentinel but that one and V296's
           is a V294 store and maps to 294, not 295. Each sentinel below shifts one place from the end. */
        var allButV295 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV295[^1] = false;
        allButV295[^2] = false;
        allButV295[^3] = false;
        allButV295[^4] = false;
        allButV295[^5] = false;
        allButV295[^6] = false;
        allButV295[^7] = false;
        allButV295[^8] = false;
        allButV295[^9] = false;
        allButV295[^10] = false;
        allButV295[^11] = false;
        allButV295[^12] = false;
        allButV295[^13] = false;
        allButV295[^14] = false;
        allButV295[^15] = false;
        allButV295[^16] = false;
        allButV295[^17] = false;
        allButV295[^18] = false;
        allButV295[^19] = false;
        allButV295[^20] = false;
        allButV295[^21] = false;
        allButV295[^22] = false;
        allButV295[^23] = false;
        allButV295[^24] = false;
        allButV295[^25] = false;
        allButV295[^26] = false;
        allButV295[^27] = false;
        allButV295[^28] = false;
        Assert.Equal(294, (int)map.Invoke(null, allButV295)!);

        /* #9284: the V294 sentinel (the view-and-time index on the aggregate compression order history) is the one before V295's, so a store that has every
           sentinel but that one and V295's and V296's is a V293 store and maps to 293, not 294. Each sentinel below shifts one place from the end. */
        var allButV294 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV294[^1] = false;
        allButV294[^2] = false;
        allButV294[^3] = false;
        allButV294[^4] = false;
        allButV294[^5] = false;
        allButV294[^6] = false;
        allButV294[^7] = false;
        allButV294[^8] = false;
        allButV294[^9] = false;
        allButV294[^10] = false;
        allButV294[^11] = false;
        allButV294[^12] = false;
        allButV294[^13] = false;
        allButV294[^14] = false;
        allButV294[^15] = false;
        allButV294[^16] = false;
        allButV294[^17] = false;
        allButV294[^18] = false;
        allButV294[^19] = false;
        allButV294[^20] = false;
        allButV294[^21] = false;
        allButV294[^22] = false;
        allButV294[^23] = false;
        allButV294[^24] = false;
        allButV294[^25] = false;
        allButV294[^26] = false;
        allButV294[^27] = false;
        allButV294[^28] = false;
        allButV294[^29] = false;
        Assert.Equal(293, (int)map.Invoke(null, allButV294)!);

        /* #9277: the V293 sentinel (the partial counter-row index on attention_events) is the one before the last, so a store that has every
           sentinel but that one is a V292 store and maps to 292, not 293. Each sentinel below shifts one place from the end. */
        var allButV293 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV293[^1] = false;
        allButV293[^2] = false;
        allButV293[^3] = false;
        allButV293[^4] = false;
        allButV293[^5] = false;
        allButV293[^6] = false;
        allButV293[^7] = false;
        allButV293[^8] = false;
        allButV293[^9] = false;
        allButV293[^10] = false;
        allButV293[^11] = false;
        allButV293[^12] = false;
        allButV293[^13] = false;
        allButV293[^14] = false;
        allButV293[^15] = false;
        allButV293[^16] = false;
        allButV293[^17] = false;
        allButV293[^18] = false;
        allButV293[^19] = false;
        allButV293[^20] = false;
        allButV293[^21] = false;
        allButV293[^22] = false;
        allButV293[^23] = false;
        allButV293[^24] = false;
        allButV293[^25] = false;
        allButV293[^26] = false;
        allButV293[^27] = false;
        allButV293[^28] = false;
        allButV293[^29] = false;
        allButV293[^30] = false;
        Assert.Equal(292, (int)map.Invoke(null, allButV293)!);

        var allButV292 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV292[^1] = false;
        allButV292[^2] = false;
        allButV292[^3] = false;
        allButV292[^4] = false;
        allButV292[^5] = false;
        allButV292[^6] = false;
        allButV292[^7] = false;
        allButV292[^8] = false;
        allButV292[^9] = false;
        allButV292[^10] = false;
        allButV292[^11] = false;
        allButV292[^12] = false;
        allButV292[^13] = false;
        allButV292[^14] = false;
        allButV292[^15] = false;
        allButV292[^16] = false;
        allButV292[^17] = false;
        allButV292[^18] = false;
        allButV292[^19] = false;
        allButV292[^20] = false;
        allButV292[^21] = false;
        allButV292[^22] = false;
        allButV292[^23] = false;
        allButV292[^24] = false;
        allButV292[^25] = false;
        allButV292[^26] = false;
        allButV292[^27] = false;
        allButV292[^28] = false;
        allButV292[^29] = false;
        allButV292[^30] = false;
        allButV292[^31] = false;
        Assert.Equal(291, (int)map.Invoke(null, allButV292)!);

        /* #9215: the V291 sentinel (the covering server index on the corrected hourly Query Store rollup) is the one before the last, so a store that has every
           sentinel but that one is a V290 store and maps to 290, not 291. Each sentinel below shifts one place from the end. */
        var allButV291 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV291[^1] = false;
        allButV291[^2] = false;
        allButV291[^3] = false;
        allButV291[^4] = false;
        allButV291[^5] = false;
        allButV291[^6] = false;
        allButV291[^7] = false;
        allButV291[^8] = false;
        allButV291[^9] = false;
        allButV291[^10] = false;
        allButV291[^11] = false;
        allButV291[^12] = false;
        allButV291[^13] = false;
        allButV291[^14] = false;
        allButV291[^15] = false;
        allButV291[^16] = false;
        allButV291[^17] = false;
        allButV291[^18] = false;
        allButV291[^19] = false;
        allButV291[^20] = false;
        allButV291[^21] = false;
        allButV291[^22] = false;
        allButV291[^23] = false;
        allButV291[^24] = false;
        allButV291[^25] = false;
        allButV291[^26] = false;
        allButV291[^27] = false;
        allButV291[^28] = false;
        allButV291[^29] = false;
        allButV291[^30] = false;
        allButV291[^31] = false;
        allButV291[^32] = false;
        Assert.Equal(290, (int)map.Invoke(null, allButV291)!);

        /* #9193: the V290 sentinel (the query_stats summary floor table) is the third from the end, so a store that has every sentinel but that one
           and V291's and V292's is a V289 store and maps to 289, not 290. Each sentinel below shifts one place from the end. */
        var allButV290 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV290[^1] = false;
        allButV290[^2] = false;
        allButV290[^3] = false;
        allButV290[^4] = false;
        allButV290[^5] = false;
        allButV290[^6] = false;
        allButV290[^7] = false;
        allButV290[^8] = false;
        allButV290[^9] = false;
        allButV290[^10] = false;
        allButV290[^11] = false;
        allButV290[^12] = false;
        allButV290[^13] = false;
        allButV290[^14] = false;
        allButV290[^15] = false;
        allButV290[^16] = false;
        allButV290[^17] = false;
        allButV290[^18] = false;
        allButV290[^19] = false;
        allButV290[^20] = false;
        allButV290[^21] = false;
        allButV290[^22] = false;
        allButV290[^23] = false;
        allButV290[^24] = false;
        allButV290[^25] = false;
        allButV290[^26] = false;
        allButV290[^27] = false;
        allButV290[^28] = false;
        allButV290[^29] = false;
        allButV290[^30] = false;
        allButV290[^31] = false;
        allButV290[^32] = false;
        allButV290[^33] = false;
        Assert.Equal(289, (int)map.Invoke(null, allButV290)!);

        var allButV289 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV289[^1] = false;
        allButV289[^2] = false;
        allButV289[^3] = false;
        allButV289[^4] = false;
        allButV289[^5] = false;
        allButV289[^6] = false;
        allButV289[^7] = false;
        allButV289[^8] = false;
        allButV289[^9] = false;
        allButV289[^10] = false;
        allButV289[^11] = false;
        allButV289[^12] = false;
        allButV289[^13] = false;
        allButV289[^14] = false;
        allButV289[^15] = false;
        allButV289[^16] = false;
        allButV289[^17] = false;
        allButV289[^18] = false;
        allButV289[^19] = false;
        allButV289[^20] = false;
        allButV289[^21] = false;
        allButV289[^22] = false;
        allButV289[^23] = false;
        allButV289[^24] = false;
        allButV289[^25] = false;
        allButV289[^26] = false;
        allButV289[^27] = false;
        allButV289[^28] = false;
        allButV289[^29] = false;
        allButV289[^30] = false;
        allButV289[^31] = false;
        allButV289[^32] = false;
        allButV289[^33] = false;
        allButV289[^34] = false;
        Assert.Equal(288, (int)map.Invoke(null, allButV289)!);

        var allButV288 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV288[^1] = false;
        allButV288[^2] = false;
        allButV288[^3] = false;
        allButV288[^4] = false;
        allButV288[^5] = false;
        allButV288[^6] = false;
        allButV288[^7] = false;
        allButV288[^8] = false;
        allButV288[^9] = false;
        allButV288[^10] = false;
        allButV288[^11] = false;
        allButV288[^12] = false;
        allButV288[^13] = false;
        allButV288[^14] = false;
        allButV288[^15] = false;
        allButV288[^16] = false;
        allButV288[^17] = false;
        allButV288[^18] = false;
        allButV288[^19] = false;
        allButV288[^20] = false;
        allButV288[^21] = false;
        allButV288[^22] = false;
        allButV288[^23] = false;
        allButV288[^24] = false;
        allButV288[^25] = false;
        allButV288[^26] = false;
        allButV288[^27] = false;
        allButV288[^28] = false;
        allButV288[^29] = false;
        allButV288[^30] = false;
        allButV288[^31] = false;
        allButV288[^32] = false;
        allButV288[^33] = false;
        allButV288[^34] = false;
        allButV288[^35] = false;
        Assert.Equal(287, (int)map.Invoke(null, allButV288)!);

        var allButV287 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV287[^1] = false;
        allButV287[^2] = false;
        allButV287[^3] = false;
        allButV287[^4] = false;
        allButV287[^5] = false;
        allButV287[^6] = false;
        allButV287[^7] = false;
        allButV287[^8] = false;
        allButV287[^9] = false;
        allButV287[^10] = false;
        allButV287[^11] = false;
        allButV287[^12] = false;
        allButV287[^13] = false;
        allButV287[^14] = false;
        allButV287[^15] = false;
        allButV287[^16] = false;
        allButV287[^17] = false;
        allButV287[^18] = false;
        allButV287[^19] = false;
        allButV287[^20] = false;
        allButV287[^21] = false;
        allButV287[^22] = false;
        allButV287[^23] = false;
        allButV287[^24] = false;
        allButV287[^25] = false;
        allButV287[^26] = false;
        allButV287[^27] = false;
        allButV287[^28] = false;
        allButV287[^29] = false;
        allButV287[^30] = false;
        allButV287[^31] = false;
        allButV287[^32] = false;
        allButV287[^33] = false;
        allButV287[^34] = false;
        allButV287[^35] = false;
        allButV287[^36] = false;
        Assert.Equal(286, (int)map.Invoke(null, allButV287)!);

        var allButV286 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV286[^1] = false;
        allButV286[^2] = false;
        allButV286[^3] = false;
        allButV286[^4] = false;
        allButV286[^5] = false;
        allButV286[^6] = false;
        allButV286[^7] = false;
        allButV286[^8] = false;
        allButV286[^9] = false;
        allButV286[^10] = false;
        allButV286[^11] = false;
        allButV286[^12] = false;
        allButV286[^13] = false;
        allButV286[^14] = false;
        allButV286[^15] = false;
        allButV286[^16] = false;
        allButV286[^17] = false;
        allButV286[^18] = false;
        allButV286[^19] = false;
        allButV286[^20] = false;
        allButV286[^21] = false;
        allButV286[^22] = false;
        allButV286[^23] = false;
        allButV286[^24] = false;
        allButV286[^25] = false;
        allButV286[^26] = false;
        allButV286[^27] = false;
        allButV286[^28] = false;
        allButV286[^29] = false;
        allButV286[^30] = false;
        allButV286[^31] = false;
        allButV286[^32] = false;
        allButV286[^33] = false;
        allButV286[^34] = false;
        allButV286[^35] = false;
        allButV286[^36] = false;
        allButV286[^37] = false;
        Assert.Equal(285, (int)map.Invoke(null, allButV286)!);

        var allButV285 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV285[^1] = false;
        allButV285[^2] = false;
        allButV285[^3] = false;
        allButV285[^4] = false;
        allButV285[^5] = false;
        allButV285[^6] = false;
        allButV285[^7] = false;
        allButV285[^8] = false;
        allButV285[^9] = false;
        allButV285[^10] = false;
        allButV285[^11] = false;
        allButV285[^12] = false;
        allButV285[^13] = false;
        allButV285[^14] = false;
        allButV285[^15] = false;
        allButV285[^16] = false;
        allButV285[^17] = false;
        allButV285[^18] = false;
        allButV285[^19] = false;
        allButV285[^20] = false;
        allButV285[^21] = false;
        allButV285[^22] = false;
        allButV285[^23] = false;
        allButV285[^24] = false;
        allButV285[^25] = false;
        allButV285[^26] = false;
        allButV285[^27] = false;
        allButV285[^28] = false;
        allButV285[^29] = false;
        allButV285[^30] = false;
        allButV285[^31] = false;
        allButV285[^32] = false;
        allButV285[^33] = false;
        allButV285[^34] = false;
        allButV285[^35] = false;
        allButV285[^36] = false;
        allButV285[^37] = false;
        allButV285[^38] = false;
        Assert.Equal(284, (int)map.Invoke(null, allButV285)!);

        var allButV284 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV284[^1] = false;
        allButV284[^2] = false;
        allButV284[^3] = false;
        allButV284[^4] = false;
        allButV284[^5] = false;
        allButV284[^6] = false;
        allButV284[^7] = false;
        allButV284[^8] = false;
        allButV284[^9] = false;
        allButV284[^10] = false;
        allButV284[^11] = false;
        allButV284[^12] = false;
        allButV284[^13] = false;
        allButV284[^14] = false;
        allButV284[^15] = false;
        allButV284[^16] = false;
        allButV284[^17] = false;
        allButV284[^18] = false;
        allButV284[^19] = false;
        allButV284[^20] = false;
        allButV284[^21] = false;
        allButV284[^22] = false;
        allButV284[^23] = false;
        allButV284[^24] = false;
        allButV284[^25] = false;
        allButV284[^26] = false;
        allButV284[^27] = false;
        allButV284[^28] = false;
        allButV284[^29] = false;
        allButV284[^30] = false;
        allButV284[^31] = false;
        allButV284[^32] = false;
        allButV284[^33] = false;
        allButV284[^34] = false;
        allButV284[^35] = false;
        allButV284[^36] = false;
        allButV284[^37] = false;
        allButV284[^38] = false;
        allButV284[^39] = false;
        Assert.Equal(283, (int)map.Invoke(null, allButV284)!);

        var allButV283 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV283[^1] = false;
        allButV283[^2] = false;
        allButV283[^3] = false;
        allButV283[^4] = false;
        allButV283[^5] = false;
        allButV283[^6] = false;
        allButV283[^7] = false;
        allButV283[^8] = false;
        allButV283[^9] = false;
        allButV283[^10] = false;
        allButV283[^11] = false;
        allButV283[^12] = false;
        allButV283[^13] = false;
        allButV283[^14] = false;
        allButV283[^15] = false;
        allButV283[^16] = false;
        allButV283[^17] = false;
        allButV283[^18] = false;
        allButV283[^19] = false;
        allButV283[^20] = false;
        allButV283[^21] = false;
        allButV283[^22] = false;
        allButV283[^23] = false;
        allButV283[^24] = false;
        allButV283[^25] = false;
        allButV283[^26] = false;
        allButV283[^27] = false;
        allButV283[^28] = false;
        allButV283[^29] = false;
        allButV283[^30] = false;
        allButV283[^31] = false;
        allButV283[^32] = false;
        allButV283[^33] = false;
        allButV283[^34] = false;
        allButV283[^35] = false;
        allButV283[^36] = false;
        allButV283[^37] = false;
        allButV283[^38] = false;
        allButV283[^39] = false;
        allButV283[^40] = false;
        Assert.Equal(282, (int)map.Invoke(null, allButV283)!);

        var allButV282 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV282[^1] = false;
        allButV282[^2] = false;
        allButV282[^3] = false;
        allButV282[^4] = false;
        allButV282[^5] = false;
        allButV282[^6] = false;
        allButV282[^7] = false;
        allButV282[^8] = false;
        allButV282[^9] = false;
        allButV282[^10] = false;
        allButV282[^11] = false;
        allButV282[^12] = false;
        allButV282[^13] = false;
        allButV282[^14] = false;
        allButV282[^15] = false;
        allButV282[^16] = false;
        allButV282[^17] = false;
        allButV282[^18] = false;
        allButV282[^19] = false;
        allButV282[^20] = false;
        allButV282[^21] = false;
        allButV282[^22] = false;
        allButV282[^23] = false;
        allButV282[^24] = false;
        allButV282[^25] = false;
        allButV282[^26] = false;
        allButV282[^27] = false;
        allButV282[^28] = false;
        allButV282[^29] = false;
        allButV282[^30] = false;
        allButV282[^31] = false;
        allButV282[^32] = false;
        allButV282[^33] = false;
        allButV282[^34] = false;
        allButV282[^35] = false;
        allButV282[^36] = false;
        allButV282[^37] = false;
        allButV282[^38] = false;
        allButV282[^39] = false;
        allButV282[^40] = false;
        allButV282[^41] = false;
        Assert.Equal(281, (int)map.Invoke(null, allButV282)!);

        var allButV281 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV281[^1] = false;
        allButV281[^2] = false;
        allButV281[^3] = false;
        allButV281[^4] = false;
        allButV281[^5] = false;
        allButV281[^6] = false;
        allButV281[^7] = false;
        allButV281[^8] = false;
        allButV281[^9] = false;
        allButV281[^10] = false;
        allButV281[^11] = false;
        allButV281[^12] = false;
        allButV281[^13] = false;
        allButV281[^14] = false;
        allButV281[^15] = false;
        allButV281[^16] = false;
        allButV281[^17] = false;
        allButV281[^18] = false;
        allButV281[^19] = false;
        allButV281[^20] = false;
        allButV281[^21] = false;
        allButV281[^22] = false;
        allButV281[^23] = false;
        allButV281[^24] = false;
        allButV281[^25] = false;
        allButV281[^26] = false;
        allButV281[^27] = false;
        allButV281[^28] = false;
        allButV281[^29] = false;
        allButV281[^30] = false;
        allButV281[^31] = false;
        allButV281[^32] = false;
        allButV281[^33] = false;
        allButV281[^34] = false;
        allButV281[^35] = false;
        allButV281[^36] = false;
        allButV281[^37] = false;
        allButV281[^38] = false;
        allButV281[^39] = false;
        allButV281[^40] = false;
        allButV281[^41] = false;
        allButV281[^42] = false;
        Assert.Equal(280, (int)map.Invoke(null, allButV281)!);

        var allButV280 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV280[^1] = false;
        allButV280[^2] = false;
        allButV280[^3] = false;
        allButV280[^4] = false;
        allButV280[^5] = false;
        allButV280[^6] = false;
        allButV280[^7] = false;
        allButV280[^8] = false;
        allButV280[^9] = false;
        allButV280[^10] = false;
        allButV280[^11] = false;
        allButV280[^12] = false;
        allButV280[^13] = false;
        allButV280[^14] = false;
        allButV280[^15] = false;
        allButV280[^16] = false;
        allButV280[^17] = false;
        allButV280[^18] = false;
        allButV280[^19] = false;
        allButV280[^20] = false;
        allButV280[^21] = false;
        allButV280[^22] = false;
        allButV280[^23] = false;
        allButV280[^24] = false;
        allButV280[^25] = false;
        allButV280[^26] = false;
        allButV280[^27] = false;
        allButV280[^28] = false;
        allButV280[^29] = false;
        allButV280[^30] = false;
        allButV280[^31] = false;
        allButV280[^32] = false;
        allButV280[^33] = false;
        allButV280[^34] = false;
        allButV280[^35] = false;
        allButV280[^36] = false;
        allButV280[^37] = false;
        allButV280[^38] = false;
        allButV280[^39] = false;
        allButV280[^40] = false;
        allButV280[^41] = false;
        allButV280[^42] = false;
        allButV280[^43] = false;
        Assert.Equal(279, (int)map.Invoke(null, allButV280)!);

        var allButV279 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV279[^1] = false;
        allButV279[^2] = false;
        allButV279[^3] = false;
        allButV279[^4] = false;
        allButV279[^5] = false;
        allButV279[^6] = false;
        allButV279[^7] = false;
        allButV279[^8] = false;
        allButV279[^9] = false;
        allButV279[^10] = false;
        allButV279[^11] = false;
        allButV279[^12] = false;
        allButV279[^13] = false;
        allButV279[^14] = false;
        allButV279[^15] = false;
        allButV279[^16] = false;
        allButV279[^17] = false;
        allButV279[^18] = false;
        allButV279[^19] = false;
        allButV279[^20] = false;
        allButV279[^21] = false;
        allButV279[^22] = false;
        allButV279[^23] = false;
        allButV279[^24] = false;
        allButV279[^25] = false;
        allButV279[^26] = false;
        allButV279[^27] = false;
        allButV279[^28] = false;
        allButV279[^29] = false;
        allButV279[^30] = false;
        allButV279[^31] = false;
        allButV279[^32] = false;
        allButV279[^33] = false;
        allButV279[^34] = false;
        allButV279[^35] = false;
        allButV279[^36] = false;
        allButV279[^37] = false;
        allButV279[^38] = false;
        allButV279[^39] = false;
        allButV279[^40] = false;
        allButV279[^41] = false;
        allButV279[^42] = false;
        allButV279[^43] = false;
        allButV279[^44] = false;
        Assert.Equal(278, (int)map.Invoke(null, allButV279)!);

        var allButV278 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV278[^1] = false;
        allButV278[^2] = false;
        allButV278[^3] = false;
        allButV278[^4] = false;
        allButV278[^5] = false;
        allButV278[^6] = false;
        allButV278[^7] = false;
        allButV278[^8] = false;
        allButV278[^9] = false;
        allButV278[^10] = false;
        allButV278[^11] = false;
        allButV278[^12] = false;
        allButV278[^13] = false;
        allButV278[^14] = false;
        allButV278[^15] = false;
        allButV278[^16] = false;
        allButV278[^17] = false;
        allButV278[^18] = false;
        allButV278[^19] = false;
        allButV278[^20] = false;
        allButV278[^21] = false;
        allButV278[^22] = false;
        allButV278[^23] = false;
        allButV278[^24] = false;
        allButV278[^25] = false;
        allButV278[^26] = false;
        allButV278[^27] = false;
        allButV278[^28] = false;
        allButV278[^29] = false;
        allButV278[^30] = false;
        allButV278[^31] = false;
        allButV278[^32] = false;
        allButV278[^33] = false;
        allButV278[^34] = false;
        allButV278[^35] = false;
        allButV278[^36] = false;
        allButV278[^37] = false;
        allButV278[^38] = false;
        allButV278[^39] = false;
        allButV278[^40] = false;
        allButV278[^41] = false;
        allButV278[^42] = false;
        allButV278[^43] = false;
        allButV278[^44] = false;
        allButV278[^45] = false;
        Assert.Equal(277, (int)map.Invoke(null, allButV278)!);

        var allButV277 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV277[^1] = false;
        allButV277[^2] = false;
        allButV277[^3] = false;
        allButV277[^4] = false;
        allButV277[^5] = false;
        allButV277[^6] = false;
        allButV277[^7] = false;
        allButV277[^8] = false;
        allButV277[^9] = false;
        allButV277[^10] = false;
        allButV277[^11] = false;
        allButV277[^12] = false;
        allButV277[^13] = false;
        allButV277[^14] = false;
        allButV277[^15] = false;
        allButV277[^16] = false;
        allButV277[^17] = false;
        allButV277[^18] = false;
        allButV277[^19] = false;
        allButV277[^20] = false;
        allButV277[^21] = false;
        allButV277[^22] = false;
        allButV277[^23] = false;
        allButV277[^24] = false;
        allButV277[^25] = false;
        allButV277[^26] = false;
        allButV277[^27] = false;
        allButV277[^28] = false;
        allButV277[^29] = false;
        allButV277[^30] = false;
        allButV277[^31] = false;
        allButV277[^32] = false;
        allButV277[^33] = false;
        allButV277[^34] = false;
        allButV277[^35] = false;
        allButV277[^36] = false;
        allButV277[^37] = false;
        allButV277[^38] = false;
        allButV277[^39] = false;
        allButV277[^40] = false;
        allButV277[^41] = false;
        allButV277[^42] = false;
        allButV277[^43] = false;
        allButV277[^44] = false;
        allButV277[^45] = false;
        allButV277[^46] = false;
        Assert.Equal(276, (int)map.Invoke(null, allButV277)!);

        var allButV276 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV276[^1] = false;
        allButV276[^2] = false;
        allButV276[^3] = false;
        allButV276[^4] = false;
        allButV276[^5] = false;
        allButV276[^6] = false;
        allButV276[^7] = false;
        allButV276[^8] = false;
        allButV276[^9] = false;
        allButV276[^10] = false;
        allButV276[^11] = false;
        allButV276[^12] = false;
        allButV276[^13] = false;
        allButV276[^14] = false;
        allButV276[^15] = false;
        allButV276[^16] = false;
        allButV276[^17] = false;
        allButV276[^18] = false;
        allButV276[^19] = false;
        allButV276[^20] = false;
        allButV276[^21] = false;
        allButV276[^22] = false;
        allButV276[^23] = false;
        allButV276[^24] = false;
        allButV276[^25] = false;
        allButV276[^26] = false;
        allButV276[^27] = false;
        allButV276[^28] = false;
        allButV276[^29] = false;
        allButV276[^30] = false;
        allButV276[^31] = false;
        allButV276[^32] = false;
        allButV276[^33] = false;
        allButV276[^34] = false;
        allButV276[^35] = false;
        allButV276[^36] = false;
        allButV276[^37] = false;
        allButV276[^38] = false;
        allButV276[^39] = false;
        allButV276[^40] = false;
        allButV276[^41] = false;
        allButV276[^42] = false;
        allButV276[^43] = false;
        allButV276[^44] = false;
        allButV276[^45] = false;
        allButV276[^46] = false;
        allButV276[^47] = false;
        Assert.Equal(275, (int)map.Invoke(null, allButV276)!);

        var allButV275 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV275[^1] = false;
        allButV275[^2] = false;
        allButV275[^3] = false;
        allButV275[^4] = false;
        allButV275[^5] = false;
        allButV275[^6] = false;
        allButV275[^7] = false;
        allButV275[^8] = false;
        allButV275[^9] = false;
        allButV275[^10] = false;
        allButV275[^11] = false;
        allButV275[^12] = false;
        allButV275[^13] = false;
        allButV275[^14] = false;
        allButV275[^15] = false;
        allButV275[^16] = false;
        allButV275[^17] = false;
        allButV275[^18] = false;
        allButV275[^19] = false;
        allButV275[^20] = false;
        allButV275[^21] = false;
        allButV275[^22] = false;
        allButV275[^23] = false;
        allButV275[^24] = false;
        allButV275[^25] = false;
        allButV275[^26] = false;
        allButV275[^27] = false;
        allButV275[^28] = false;
        allButV275[^29] = false;
        allButV275[^30] = false;
        allButV275[^31] = false;
        allButV275[^32] = false;
        allButV275[^33] = false;
        allButV275[^34] = false;
        allButV275[^35] = false;
        allButV275[^36] = false;
        allButV275[^37] = false;
        allButV275[^38] = false;
        allButV275[^39] = false;
        allButV275[^40] = false;
        allButV275[^41] = false;
        allButV275[^42] = false;
        allButV275[^43] = false;
        allButV275[^44] = false;
        allButV275[^45] = false;
        allButV275[^46] = false;
        allButV275[^47] = false;
        allButV275[^48] = false;
        Assert.Equal(274, (int)map.Invoke(null, allButV275)!);

        var allButV274 = Enumerable.Repeat((object)true, map.GetParameters().Length).ToArray();
        allButV274[^1] = false;
        allButV274[^2] = false;
        allButV274[^3] = false;
        allButV274[^4] = false;
        allButV274[^5] = false;
        allButV274[^6] = false;
        allButV274[^7] = false;
        allButV274[^8] = false;
        allButV274[^9] = false;
        allButV274[^10] = false;
        allButV274[^11] = false;
        allButV274[^12] = false;
        allButV274[^13] = false;
        allButV274[^14] = false;
        allButV274[^15] = false;
        allButV274[^16] = false;
        allButV274[^17] = false;
        allButV274[^18] = false;
        allButV274[^19] = false;
        allButV274[^20] = false;
        allButV274[^21] = false;
        allButV274[^22] = false;
        allButV274[^23] = false;
        allButV274[^24] = false;
        allButV274[^25] = false;
        allButV274[^26] = false;
        allButV274[^27] = false;
        allButV274[^28] = false;
        allButV274[^29] = false;
        allButV274[^30] = false;
        allButV274[^31] = false;
        allButV274[^32] = false;
        allButV274[^33] = false;
        allButV274[^34] = false;
        allButV274[^35] = false;
        allButV274[^36] = false;
        allButV274[^37] = false;
        allButV274[^38] = false;
        allButV274[^39] = false;
        allButV274[^40] = false;
        allButV274[^41] = false;
        allButV274[^42] = false;
        allButV274[^43] = false;
        allButV274[^44] = false;
        allButV274[^45] = false;
        allButV274[^46] = false;
        allButV274[^47] = false;
        allButV274[^48] = false;
        allButV274[^49] = false;
        Assert.Equal(273, (int)map.Invoke(null, allButV274)!);

        /* Probe row width vs mapper arity is not checked HERE, and the reason it was left to the live probes
           was that one ordinal is legitimately a compound (two EXISTS OR-ed), so counting the token EXISTS
           lies — it reads one high. That is true of token counting and not of the seam: counting top-level
           select items by PAREN DEPTH is immune to the compound, because the nested EXISTS sit inside
           parentheses. StoreSchemaProbe_ColumnCount_MatchesTheMapArity below does that, so the seam is now
           pinned at build time rather than only on a real store. */
    }

    /// <summary>
    /// The probe SQL's column count must equal <see cref="ViewerDataService.MapProbedSchemaVersion"/>'s
    /// parameter count, because <c>GetStoreSchemaVersionAsync</c> reads them positionally.
    ///
    /// <para><b>Nothing else catches this.</b> Add a sentinel to the SQL and forget the parameter and the
    /// last column is silently ignored — a fully-migrated store reports one rung short and the viewer refuses
    /// a healthy store. Add the parameter and forget the SQL column and
    /// <c>reader.GetBoolean(n)</c> throws IndexOutOfRange at connect time. Both are runtime-only against a
    /// live store, both are invisible to every other test here (the arity test above builds its arguments
    /// from the signature, so it agrees with itself either way), and the second one is the same class of
    /// ordinal-drift defect that a live-target review had to find by hand.</para>
    ///
    /// <para>Top-level select items are counted by paren depth rather than by counting the string
    /// <c>EXISTS</c>: the V22/V23 composite column contains two nested <c>EXISTS</c> of its own, so a naive
    /// substring count reads one HIGHER than the select list actually is. Paren depth is immune, because the
    /// nested pair sits inside parentheses — which is why this can be pinned at build time at all.</para>
    ///
    /// <para><b><c>/* */</c> comments are stripped before counting, and that is load-bearing.</b> A comma
    /// inside a comment sits at paren depth zero and is indistinguishable from a select-item separator, so
    /// one explanatory sentence in the probe SQL silently inflates the count and fails this test for a
    /// reason that has nothing to do with the arity it exists to protect. V95 (#2599) hit exactly that.
    /// The sentinels are the part worth commenting — the test must not tax anyone for doing it.</para>
    /// </summary>
    [Fact]
    public void StoreSchemaProbe_ColumnCount_MatchesTheMapArity()
    {
        var sql = System.Text.RegularExpressions.Regex.Replace(
            ViewerDataService.StoreSchemaProbeSql,
            @"/\*.*?\*/",
            " ",
            System.Text.RegularExpressions.RegexOptions.Singleline);

        var selectAt = sql.IndexOf("SELECT", StringComparison.Ordinal);
        Assert.True(selectAt >= 0, "the probe must be a SELECT");

        var depth = 0;
        var columns = 1;
        foreach (var c in sql.AsSpan(selectAt + "SELECT".Length))
        {
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) columns++;
        }

        var parameters = typeof(ViewerDataService)
            .GetMethod(
                nameof(ViewerDataService.MapProbedSchemaVersion),
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic
                | System.Reflection.BindingFlags.Public)!
            .GetParameters().Length;

        Assert.Equal(parameters, columns);
    }
}

/// <summary>
/// The write-executor schema-skew translation (finding B2): a control-plane write against a lagging store
/// throws Postgres 42703 (undefined_column) / 42P01 (undefined_table); the executors translate both into a
/// <see cref="ViewerSchemaSkewException"/> the callers surface like the read-only exception, instead of a raw
/// "column ... does not exist" error.
/// </summary>
public sealed class ViewerSchemaSkewTests
{
    [Fact]
    public void SchemaSkewSqlStates_AreTheUndefinedColumnAndTableStates()
    {
        Assert.Equal("42703", ViewerDataService.UndefinedColumnSqlState);
        Assert.Equal("42P01", ViewerDataService.UndefinedTableSqlState);
    }

    [Fact]
    public void ViewerSchemaSkewException_NamesTheRequiredVersion_AndTheServiceRemedy()
    {
        var ex = new ViewerSchemaSkewException(new InvalidOperationException("42703"));

        Assert.Contains($"v{ViewerDataService.RequiredStoreSchemaVersion}", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Darling service", ex.Message, StringComparison.Ordinal);
        Assert.Contains("migrate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("42703", ex.InnerException?.Message);
    }
}

/// <summary>
/// Distinguishing an unreachable store from a read-only seat (finding B3): a connection-level failure
/// (service down, wrong host/port) is a <see cref="ViewerStoreUnreachableException"/> with its own message,
/// NOT a false read-only verdict; a server error RESPONSE (a PostgresException) means the store is reachable.
/// </summary>
public sealed class ViewerStoreUnreachableTests
{
    [Fact]
    public void ViewerStoreUnreachableException_AsksWhetherTheServiceIsRunning_AndNamesDarlingJson()
    {
        var ex = new ViewerStoreUnreachableException(new InvalidOperationException("connection refused"));

        Assert.Contains("Darling service", ex.Message, StringComparison.Ordinal);
        Assert.Contains("darling.json", ex.Message, StringComparison.Ordinal);
        Assert.Equal("connection refused", ex.InnerException?.Message);

        /* #2117 finding 2: the message itself carries the underlying error's first line — the swallowed
           detail cost a field operator hours, and the fixed prose alone reads identically for a TLS chain
           rejection, a wrong password, a pg_hba refusal, and a dead host. */
        Assert.Contains("Underlying error: connection refused", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The surfaced first line is trimmed of the CR a CRLF-raised exception leaves behind (Windows is
    /// exactly where this fix is aimed), and only the FIRST line is taken — a multi-line inner message
    /// must not flood the one-line status surface.
    /// </summary>
    [Fact]
    public void ViewerStoreUnreachableException_SurfacesTheFirstLineOnly_WithNoTrailingCarriageReturn()
    {
        var ex = new ViewerStoreUnreachableException(new InvalidOperationException("boom\r\nmore detail\r\neven more"));

        Assert.Contains("Underlying error: boom", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("boom\r", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("more detail", ex.Message, StringComparison.Ordinal);

        /* And a null inner message degrades to the explicit placeholder, never a crash. */
        Assert.Contains("Underlying error: (none)", new ViewerStoreUnreachableException(null!).Message, StringComparison.Ordinal);
    }

    [Fact]
    public void IsConnectionFailure_TrueForConnectLevelFailures_FalseForServerResponses()
    {
        /* Connection-level failures — the server was never reached. */
        Assert.True(ViewerDataService.IsConnectionFailure(new NpgsqlException("could not connect")));
        Assert.True(ViewerDataService.IsConnectionFailure(new System.Net.Sockets.SocketException()));
        Assert.True(ViewerDataService.IsConnectionFailure(new TimeoutException()));

        /* A PostgresException is a server RESPONSE (carries a SQLSTATE) — the store IS reachable. */
        var serverResponse = new PostgresException("permission denied", "ERROR", "ERROR", "42501");
        Assert.False(ViewerDataService.IsConnectionFailure(serverResponse));

        /* An unrelated exception is not a connection failure either. */
        Assert.False(ViewerDataService.IsConnectionFailure(new InvalidOperationException()));
    }
}

/// <summary>
/// The read-only viewer's per-section Settings degradation (darling-network-endpoints D7): a read-only
/// <c>viewer</c> role is column-denied the secret columns of <c>config_notification</c> and
/// <c>config_monitored_servers</c>, so those reads fall back to secret-free projections, and the Settings load
/// reads each section independently so one 42501 never blanks the rest.
/// </summary>
public sealed class ViewerReadOnlyDegradationTests
{
    [Fact]
    public void NotificationSelectNoSecretSql_OmitsTheCarvedSecrets_KeepsTheNonSecretColumns()
    {
        var noSecret = ViewerDataService.NotificationSelectNoSecretSql;

        /* The four carved secret columns the viewer role loses SELECT on (#1262). */
        Assert.DoesNotContain("smtp_encrypted_password", noSecret, StringComparison.Ordinal);
        Assert.DoesNotContain("smtp_username", noSecret, StringComparison.Ordinal);
        Assert.DoesNotContain("teams_url", noSecret, StringComparison.Ordinal);
        Assert.DoesNotContain("slack_url", noSecret, StringComparison.Ordinal);

        /* The non-secret fields the read-only Settings window still prefills. */
        Assert.Contains("smtp_host", noSecret, StringComparison.Ordinal);
        Assert.Contains("smtp_from_address", noSecret, StringComparison.Ordinal);
        Assert.Contains("teams_proxy", noSecret, StringComparison.Ordinal);
        Assert.Contains("slack_proxy", noSecret, StringComparison.Ordinal);
        Assert.Contains("FROM config_notification WHERE id = 1", noSecret, StringComparison.Ordinal);
    }

    [Fact]
    public void NotificationSelectSql_FullProjection_StillCarriesTheSecrets_ForAWritableSeat()
    {
        /* The admin/owner seat still reads the secrets to prefill the SMTP password + webhook URLs. */
        var full = ViewerDataService.NotificationSelectSql;
        Assert.Contains("smtp_encrypted_password", full, StringComparison.Ordinal);
        Assert.Contains("smtp_username", full, StringComparison.Ordinal);
        Assert.Contains("teams_url", full, StringComparison.Ordinal);
        Assert.Contains("slack_url", full, StringComparison.Ordinal);
    }

    [Fact]
    public void MonitoredServerByIdNoSecretSql_OmitsEncryptedPassword_ButKeepsTheByIdFilter()
    {
        var noSecret = ViewerDataService.MonitoredServerByIdNoSecretSql;
        Assert.DoesNotContain("encrypted_password", noSecret, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", noSecret, StringComparison.Ordinal);

        /* The full by-id read (an admin action) still selects the DPAPI blob for the password box. */
        Assert.Contains("encrypted_password", ViewerDataService.MonitoredServerByIdSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.MonitoredServerByIdSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadStoreSectionsAsync_NotificationThrows42501_DoesNotBlankTheOtherSections()
    {
        var alert = AlertSettingsRow.Defaults();
        var service = new ServiceConfigRow();

        var sections = await SettingsWindow.ReadStoreSectionsAsync(
            () => Task.FromResult<AlertSettingsRow?>(alert),
            () => Task.FromException<NotificationRow?>(
                new PostgresException("permission denied for table config_notification", "ERROR", "ERROR", "42501")),
            () => Task.FromResult<ServiceConfigRow?>(service));

        /* The notification section degrades to null (its defaults stand); the OTHER two survive intact. */
        Assert.Same(alert, sections.Alert);
        Assert.Null(sections.Notification);
        Assert.Same(service, sections.Service);
    }

    [Fact]
    public async Task ReadStoreSectionsAsync_AllSucceed_ReturnsAllThreeSections()
    {
        var alert = AlertSettingsRow.Defaults();
        var notify = NotificationRow.Defaults();
        var service = new ServiceConfigRow();

        var sections = await SettingsWindow.ReadStoreSectionsAsync(
            () => Task.FromResult<AlertSettingsRow?>(alert),
            () => Task.FromResult<NotificationRow?>(notify),
            () => Task.FromResult<ServiceConfigRow?>(service));

        Assert.Same(alert, sections.Alert);
        Assert.Same(notify, sections.Notification);
        Assert.Same(service, sections.Service);
    }

    [Fact]
    public async Task ReadStoreSectionsAsync_Cancellation_Propagates()
    {
        /* A genuine cancellation is NOT swallowed as a degraded section — it propagates. */
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            SettingsWindow.ReadStoreSectionsAsync(
                () => Task.FromResult<AlertSettingsRow?>(AlertSettingsRow.Defaults()),
                () => Task.FromException<NotificationRow?>(new OperationCanceledException()),
                () => Task.FromResult<ServiceConfigRow?>(new ServiceConfigRow())));
    }
}

/// <summary>
/// Pins the connection-timeout application (the formerly-dead <c>ConnectionTimeoutSeconds</c> knob): the viewer's
/// preference is applied to the store connection string as Npgsql's connect <c>Timeout</c> — set on the
/// managed-derived string (which carries none) and appended to a BYO string only when the operator did not
/// already specify one. Pure string logic (no live Postgres), the same discipline as the SQL-const pins.
/// </summary>
public sealed class ViewerConnectionTimeoutTests
{
    [Fact]
    public void ApplyConnectionTimeout_ManagedStyleStringWithoutTimeout_SetsThePreference()
    {
        /* The managed-derived shape (DarlingManagedPostgres): no Timeout key, so the preference is applied. */
        const string managed =
            "Host=127.0.0.1;Port=5641;Username=admin;Password=secret;Database=darling;Search Path=collect,config,public";

        var result = ViewerDataService.ApplyConnectionTimeout(managed, 42);

        var builder = new NpgsqlConnectionStringBuilder(result);
        Assert.Equal(42, builder.Timeout);
        /* Every other setting is preserved. */
        Assert.Equal("127.0.0.1", builder.Host);
        Assert.Equal(5641, builder.Port);
        Assert.Equal("darling", builder.Database);
        /* Search Path is the load-bearing one: it resolves the bare table names to the collect/config schemas
           on every connection, so a query would silently break if this ever went back to emitting through a
           builder round trip instead of appending to the caller's own string (round-1 review on #4285's PR).
           Pin that it survives verbatim. */
        Assert.Equal("collect,config,public", builder.SearchPath);
    }

    [Fact]
    public void ApplyConnectionTimeout_ByoStringWithoutTimeout_AppendsThePreference()
    {
        var result = ViewerDataService.ApplyConnectionTimeout("Host=db.corp;Port=5432;Database=darling", 30);

        var builder = new NpgsqlConnectionStringBuilder(result);
        Assert.Equal(30, builder.Timeout);
        Assert.Equal("db.corp", builder.Host);
    }

    [Fact]
    public void ApplyConnectionTimeout_ByoStringWithExplicitTimeout_LeavesTheOperatorsValue()
    {
        /* The operator's explicit Timeout in a BYO connection string wins — the preference must not override it. */
        var result = ViewerDataService.ApplyConnectionTimeout("Host=db.corp;Database=darling;Timeout=25", 5);

        Assert.Equal(25, new NpgsqlConnectionStringBuilder(result).Timeout);
    }

    [Fact]
    public void ApplyConnectionTimeout_DetectsAnExistingTimeoutCaseInsensitively()
    {
        /* A lowercase keyword still counts as "already specified" (connection-string keys are case-insensitive). */
        var result = ViewerDataService.ApplyConnectionTimeout("Host=db.corp;Database=darling;timeout=25", 5);

        Assert.Equal(25, new NpgsqlConnectionStringBuilder(result).Timeout);
    }
}
