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
/// Pins the viewer's SQL against the Darling store contract (no live Postgres needed) and
/// unit-tests the pure display helpers: the version label. (The naive-UTC → display conversion moved to
/// the mode-aware <c>ViewerTimeHelper</c>, pinned in <c>ViewerTimeHelperTests</c>; the Overview trend
/// reads + wait-category roll-up are pinned in <c>ViewerTrendsTests</c>.)
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

    [Theory]
    [InlineData(13, "SQL Server 2016")]
    [InlineData(14, "SQL Server 2017")]
    [InlineData(15, "SQL Server 2019")]
    [InlineData(16, "SQL Server 2022")]
    [InlineData(17, "SQL Server 2025")]
    [InlineData(99, "SQL Server v99")]
    [InlineData(null, "")]
    public void SqlVersionLabel_MapsKnownMajors_AndFallsBack(int? major, string expected)
    {
        Assert.Equal(expected, ViewerDataService.SqlVersionLabel(major));
    }

    /* The naive-UTC → display conversion moved from ViewerDataService.ToLocalTime to the mode-aware
       ViewerTimeHelper (Server/Local/UTC); it is pinned in ViewerTimeHelperTests. */

    [Fact]
    public void DarlingServer_VersionLabel_ComesFromTheMajorVersion()
    {
        var server = new DarlingServer(1, "SQL2022", "SQL2022", true, 16);
        Assert.Equal("SQL Server 2022", server.VersionLabel);
    }

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
            Assert.Equal(missing, ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory));

            /* Only the service root has one — the shipped-zip case. */
            var atServiceRoot = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(atServiceRoot, "{}");
            Assert.Equal(atServiceRoot, ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory));

            /* Beside the viewer binary still wins over the parent when both exist. */
            File.WriteAllText(missing, "{}");
            Assert.Equal(missing, ViewerSettings.ResolveConfigPath(baseDirectory: viewerDirectory));
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
    public void ReadOnlyProbeSql_TestsConfigInsertPrivilege()
    {
        /* has_table_privilege on config_mute_rules INSERT: true => writable (admin/owner), false =>
           read-only viewer. The bare name resolves to config.config_mute_rules via search_path. */
        Assert.Equal("SELECT has_table_privilege('config_mute_rules', 'INSERT')", ViewerDataService.ReadOnlyProbeSql);
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
        Assert.Equal("42501", ex.InnerException?.Message);
    }
}

/// <summary>
/// The connect-time schema-skew gate (finding B1): the viewer probes the store's effective version via
/// <c>information_schema</c> (the owner-only <c>darling_schema_version</c> table can't be read) and blocks a
/// store behind the version this build needs, instead of letting a control-plane write throw a raw 42703.
/// </summary>
public sealed class ViewerSchemaVersionGateTests
{
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
           on every connection, so if the base DbConnectionStringBuilder round-trip ever mangled it, every
           managed-path query would silently break. Pin that it survives verbatim (the reason we detect + emit
           via the base builder rather than NpgsqlConnectionStringBuilder). */
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
