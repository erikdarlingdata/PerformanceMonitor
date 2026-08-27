/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Phase-5 analysis slice AN1 storage foundation. Ungated: the V4 "analysis-tables"
/// migration creates analysis_findings in Lite's v3 shape PLUS the Dashboard's
/// remediation_action_json (no primary key — collector-table reasoning), the PK'd
/// analysis_muted registry, and every passthrough view the ported analysis SQL reads (each
/// pinned against the collector catalog so a view can never target a nonexistent table); the
/// PgFindingStore SQL is PG-dialect ($N positional parameters, no bare now(), no N''
/// literals); and PgPlanFetcher implements the shared seam with the Dashboard's query,
/// degrading to null on a resolver miss without touching a server. Gated on DARLING_TEST_PG:
/// migrate → 6 schema versions, then the full store round-trip — filter (absolution dropped)
/// → insert with a BUILT RemediationAction → read back with every field intact including the
/// deserialized action → mute excludes → unmute restores → cleanup purges aged rows —
/// plus a v_wait_stats SELECT as the passthrough proof.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingAnalysisStoreTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -616161;
    private const string TestServerName = "analysis-store-e2e";
    private const string TestStoryPathHash = "an1-e2e-hash";

    /* Global/all-servers mute coverage: distinctive hashes + ids for the NULL-persistence and legacy-0
       honoring test, kept off the per-server round-trip's TestServerId. */
    private const string GlobalNullHash = "an1-global-null-hash";
    private const string LegacyZeroHash = "an1-legacy-zero-hash";
    private const string OtherServerHash = "an1-other-server-hash";
    private const string GlobalSurvivorHash = "an1-global-survivor-hash";
    private const int GlobalReaderServerId = -929292;
    private const int GlobalOtherServerId = -939393;

    /* #2448's fixture, kept off every other test's server so the "nothing was persisted" assertion
       cannot be satisfied by another test's cleanup or broken by its rows. */
    private const int PartialBatchServerId = -646464;
    private const string PartialBatchServerName = "analysis-store-partial-batch";

    /// <summary>
    /// Every v_&lt;table&gt; view the V4 migration must create: the fourteen the ported fact
    /// collectors read plus the three Lite's drill-down/storage collectors also read
    /// (tempdb_stats, query_snapshots, database_config).
    /// </summary>
    private static readonly string[] PassthroughViewTables =
    {
        "wait_stats",
        "query_stats",
        "query_store_stats",
        "cpu_utilization_stats",
        "memory_grant_stats",
        "memory_stats",
        "perfmon_stats",
        "session_stats",
        "file_io_stats",
        "blocked_process_reports",
        "deadlocks",
        "dmv_blocking_snapshots",
        "index_object_stats",
        "database_size_stats",
        "tempdb_stats",
        "query_snapshots",
        "database_config"
    };

    private static readonly string[] AllFindingStoreSql =
    {
        PgFindingStore.InsertFindingSql,
        PgFindingStore.GetRecentFindingsSql,
        PgFindingStore.GetLatestFindingsSql,
        PgFindingStore.GetMutedHashesSql,
        PgFindingStore.MuteStorySql,
        PgFindingStore.UnmuteStorySql,
        PgFindingStore.CleanupOldFindingsSql
    };

    /* ---------------- ungated: V4 migration pins ---------------- */

    [Fact]
    public void V4Migration_CreatesAnalysisTables_WithDashboardShape_AndAllPassthroughViews()
    {
        var v4 = PgMigrations.Scripts.Single(m => m.Version == 4);
        Assert.Equal("analysis-tables", v4.Name);

        /* analysis_findings — Lite AnalysisSchema v3 column-for-column... */
        Assert.Contains("CREATE TABLE IF NOT EXISTS analysis_findings (", v4.Sql, StringComparison.Ordinal);
        foreach (var column in new[]
        {
            "finding_id", "analysis_time", "server_id", "server_name", "database_name",
            "time_range_start", "time_range_end", "severity", "confidence", "category",
            "story_path", "story_path_hash", "story_text", "root_fact_key", "root_fact_value",
            "leaf_fact_key", "leaf_fact_value", "fact_count", "incident_id"
        })
        {
            Assert.Contains(column, v4.Sql, StringComparison.Ordinal);
        }

        /* ...PLUS the Dashboard's remediation column (recommendations rebuild D2). */
        Assert.Contains("remediation_action_json text", v4.Sql, StringComparison.Ordinal);

        /* No primary key on the findings table — the collector-table hypertable/COPY
           reasoning. The only PRIMARY KEY in V4 belongs to the muted registry. */
        var findingsSegment = v4.Sql.Substring(0, v4.Sql.IndexOf("CREATE TABLE IF NOT EXISTS analysis_muted", StringComparison.Ordinal));
        Assert.DoesNotContain("PRIMARY KEY", findingsSegment, StringComparison.Ordinal);

        /* The two Lite findings indexes, column-for-column. */
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_analysis_findings_time ON analysis_findings(server_id, analysis_time)",
            v4.Sql, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_analysis_findings_hash ON analysis_findings(story_path_hash)",
            v4.Sql, StringComparison.Ordinal);

        /* analysis_muted — Lite's columns, keeping its small-registry PRIMARY KEY (like V2's
           servers) and its hash index. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS analysis_muted (", v4.Sql, StringComparison.Ordinal);
        Assert.Contains("mute_id bigint NOT NULL PRIMARY KEY", v4.Sql, StringComparison.Ordinal);
        foreach (var column in new[] { "story_path_hash", "muted_date", "reason" })
        {
            Assert.Contains(column, v4.Sql, StringComparison.Ordinal);
        }
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_analysis_muted_hash ON analysis_muted(story_path_hash)",
            v4.Sql, StringComparison.Ordinal);

        /* Lite's DuckDB DEFAULT CURRENT_TIMESTAMP is deliberately NOT carried — in PG it
           would stamp the server's LOCAL clock; the store supplies naive-UTC explicitly. */
        Assert.DoesNotContain("CURRENT_TIMESTAMP", v4.Sql, StringComparison.OrdinalIgnoreCase);

        /* Every passthrough view, exactly the Lite-verbatim shape. */
        foreach (var table in PassthroughViewTables)
        {
            Assert.Contains($"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table};",
                v4.Sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void V4PassthroughViews_TargetOnlyRealCollectorTables()
    {
        /* A passthrough view over a missing table fails the migration — every view target
           must be a V1 collector table from the shared catalog. */
        var collectorTables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);
        foreach (var table in PassthroughViewTables)
        {
            Assert.Contains(table, collectorTables);
        }
    }

    /* ---------------- ungated: finding-store dialect pins ---------------- */

    [Fact]
    public void FindingStoreSql_PgDialect_NoBareNow_NoNLiterals_PositionalParams()
    {
        foreach (var sql in AllFindingStoreSql)
        {
            /* Bare now() is timestamptz — the naive-UTC columns would compare in the server's
               time zone. Every "now" must arrive as a bound Kind-Unspecified parameter. */
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            /* Postgres has no N'' literals and no @named parameters — $N positional only. */
            Assert.DoesNotContain("N'", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Contains("$1", sql);
        }
    }

    [Fact]
    public void FindingStoreSql_CarriesTheDashboardShape()
    {
        /* The write persists all 20 columns including the built action... */
        Assert.Contains("remediation_action_json", PgFindingStore.InsertFindingSql);
        Assert.Contains("$20", PgFindingStore.InsertFindingSql);
        /* #2060: the capped drill-down persists beside the built action and survives BOTH reads —
           appended last so every earlier ordinal stays put. */
        Assert.Contains("drill_down_json", PgFindingStore.InsertFindingSql);
        Assert.Contains("$21", PgFindingStore.InsertFindingSql);
        Assert.Contains("drill_down_json", PgFindingStore.GetRecentFindingsSql);
        Assert.Contains("drill_down_json", PgFindingStore.GetLatestFindingsSql);

        /* ...and BOTH reads return it (the Dashboard twin omits it from GetLatest — here the
           reads share one column list), with the twins' ordering/window semantics. */
        Assert.Contains("remediation_action_json", PgFindingStore.GetRecentFindingsSql);
        Assert.Contains("ORDER BY analysis_time DESC, severity DESC", PgFindingStore.GetRecentFindingsSql);
        Assert.Contains("LIMIT $4", PgFindingStore.GetRecentFindingsSql);

        /* #2506: the window is BOUNDED at both ends. Pinned because the read had a start and no end for
           its whole life, and an as_of anchor over a half-open window is the worst outcome the anchor
           convention has — validated, advertised, and answering as of now anyway. */
        Assert.Contains("AND   analysis_time >= $2", PgFindingStore.GetRecentFindingsSql);
        Assert.Contains("AND   analysis_time <= $3", PgFindingStore.GetRecentFindingsSql);
        Assert.Contains("remediation_action_json", PgFindingStore.GetLatestFindingsSql);
        Assert.Contains("SELECT MAX(analysis_time) FROM analysis_findings WHERE server_id = $1", PgFindingStore.GetLatestFindingsSql);

        /* Mute reads span per-server AND global rows — NULL (the canonical all-servers marker) plus
           legacy server_id = 0 rows written by the pre-fix tool path — like both twins. */
        Assert.Contains("server_id = $1 OR server_id IS NULL", PgFindingStore.GetMutedHashesSql);
        Assert.Contains("OR server_id = 0", PgFindingStore.GetMutedHashesSql);
    }

    /* ---------------- ungated: plan fetcher pins ---------------- */

    [Fact]
    public void PlanFetcher_ImplementsTheSharedSeam_WithTheDashboardQuery()
    {
        Assert.True(typeof(IPlanFetcher).IsAssignableFrom(typeof(PgPlanFetcher)));
        Assert.Contains("sys.dm_exec_query_plan(CONVERT(varbinary(64), @plan_handle, 1))",
            PgPlanFetcher.PlanQuery, StringComparison.Ordinal);
        Assert.Contains("SET NOCOUNT ON;", PgPlanFetcher.PlanQuery, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PlanFetcher_DegradesToNull_OnResolverMiss_AndSkipsResolveOnEmptyHandle()
    {
        /* Unknown/disconnected server: the resolver returns null and the fetch degrades
           to null without touching any server (Lite's ServerManager-miss semantics). */
        var resolved = false;
        var fetcher = new PgPlanFetcher(_ => { resolved = true; return null; });
        Assert.Null(await fetcher.FetchPlanXmlAsync(TestServerId, "0x0600FF00", TestContext.Current.CancellationToken));
        Assert.True(resolved);

        /* An empty plan handle short-circuits before the resolver is consulted. */
        resolved = false;
        Assert.Null(await fetcher.FetchPlanXmlAsync(TestServerId, "", TestContext.Current.CancellationToken));
        Assert.False(resolved);
    }

    /* ---------------- gated: live store round-trip ---------------- */

    [Fact]
    public async Task EndToEnd_FindingStoreRoundTrip_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live finding-store test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        /* Migrations are idempotent — an older store comes up to current, a current store no-ops. */
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        /* Two separate invariants, because the single COUNT(*) == SchemaVersion check they replace conflated
           them: it only holds while migration versions are DENSE from 1, so the first concurrently-developed
           pair of migrations (V35 alerts / V36 AG latency, built on separate branches) broke it with a
           temporary gap that is completely inert to the applier — MigrateAsync applies every script whose
           version exceeds MAX(version) and never assumes contiguity. Assert what actually matters instead,
           which is also strictly stronger than the old proxy. */
        using (var maxVersion = new NpgsqlCommand("SELECT COALESCE(MAX(version), 0) FROM darling_schema_version", connection))
        {
            /* The store reached the version this build knows — the same expression MigrateAsync reads. */
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await maxVersion.ExecuteScalarAsync(TestContext.Current.CancellationToken), CultureInfo.InvariantCulture));
        }

        using (var versions = new NpgsqlCommand("SELECT COUNT(*) FROM darling_schema_version", connection))
        {
            /* And EVERY script ran, one stamped row apiece — so a script silently skipped still fails here. */
            Assert.Equal((long)PgMigrations.Scripts.Count, await versions.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        }

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgFindingStore(postgres);

        var bodySucceeded = false;
        try
        {
            /* Whole-second window bounds so the PG microsecond timestamp round-trips exactly. */
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var windowStart = windowEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };

            var stories = new List<AnalysisStory>
            {
                new AnalysisStory
                {
                    Severity = 1.2,
                    Confidence = 0.8,
                    Category = "cpu",
                    StoryPath = "CPU_PRESSURE -> THREADPOOL",
                    StoryPathHash = TestStoryPathHash,
                    StoryText = "CPU pressure escalating into worker-thread starvation.",
                    RootFactKey = "CPU_PRESSURE",
                    RootFactValue = 91.5,
                    LeafFactKey = "THREADPOOL",
                    LeafFactValue = 0.42,
                    FactCount = 3,
                    IncidentId = "an1-incident",
                    DatabaseName = "StackOverflow"
                },
                /* Absolution story — severity 0 confirms health and must be dropped. */
                new AnalysisStory { Severity = 0, StoryPathHash = "an1-absolution", IsAbsolution = true }
            };

            /* Phase 1: filter — the absolution story is dropped, the real one survives. */
            var survivors = await store.FilterMutedFindingsAsync(stories, context);
            var finding = Assert.Single(survivors);
            Assert.Equal("StackOverflow", finding.DatabaseName);

            /* Phase 2: attach a BUILT action (what the orchestrator does between the phases)
               and insert — remediation_action_json is persisted on the row. */
            finding.Remediation = new RemediationAction(
                "PLAN_REGRESSION",
                "force",
                new[] { new ForcePlanTarget("StackOverflow", 42, 7, "0xBEST", "0xLATEST", 1500.5, 250.25, 6.0) });
            await store.InsertFindingsAsync(survivors, context);

            /* Read back — every persisted field intact, including the deserialized action. */
            var recent = await store.GetRecentFindingsAsync(TestServerId);
            var roundTripped = Assert.Single(recent);
            Assert.Equal(finding.FindingId, roundTripped.FindingId);
            Assert.Equal(TestServerId, roundTripped.ServerId);
            Assert.Equal(TestServerName, roundTripped.ServerName);
            Assert.Equal("StackOverflow", roundTripped.DatabaseName);
            Assert.Equal(windowStart, roundTripped.TimeRangeStart);
            Assert.Equal(windowEnd, roundTripped.TimeRangeEnd);
            Assert.Equal(1.2, roundTripped.Severity);
            Assert.Equal(0.8, roundTripped.Confidence);
            Assert.Equal("cpu", roundTripped.Category);
            Assert.Equal("CPU_PRESSURE -> THREADPOOL", roundTripped.StoryPath);
            Assert.Equal(TestStoryPathHash, roundTripped.StoryPathHash);
            Assert.Equal("CPU pressure escalating into worker-thread starvation.", roundTripped.StoryText);
            Assert.Equal("CPU_PRESSURE", roundTripped.RootFactKey);
            Assert.Equal(91.5, roundTripped.RootFactValue);
            Assert.Equal("THREADPOOL", roundTripped.LeafFactKey);
            Assert.Equal(0.42, roundTripped.LeafFactValue);
            Assert.Equal(3, roundTripped.FactCount);
            Assert.Equal("an1-incident", roundTripped.IncidentId);
            Assert.Equal(DateTimeKind.Utc, roundTripped.AnalysisTime.Kind);

            Assert.NotNull(roundTripped.Remediation);
            Assert.Equal("PLAN_REGRESSION", roundTripped.Remediation!.FactKey);
            Assert.Equal("force", roundTripped.Remediation.Action);
            var target = Assert.Single(roundTripped.Remediation.Targets);
            Assert.Equal("StackOverflow", target.Database);
            Assert.Equal(42, target.QueryId);
            Assert.Equal(7, target.PlanId);
            Assert.Equal(6.0, target.RegressionFactor);

            /* GetLatest returns the same run, action included (the uniform read). */
            var latest = await store.GetLatestFindingsAsync(TestServerId);
            Assert.Equal(roundTripped.FindingId, Assert.Single(latest).FindingId);
            Assert.NotNull(Assert.Single(latest).Remediation);

            /* Mute the story — the same stories now filter to nothing. */
            await store.MuteStoryAsync(TestServerId, TestStoryPathHash, "CPU_PRESSURE -> THREADPOOL", "e2e mute");
            Assert.Empty(await store.FilterMutedFindingsAsync(stories, context));

            /* Unmute (Dashboard-twin surface) — the story flows again. */
            long muteId;
            using (var readMute = new NpgsqlCommand(
                "SELECT mute_id FROM analysis_muted WHERE server_id = $1 AND story_path_hash = $2", connection))
            {
                readMute.Parameters.AddWithValue(TestServerId);
                readMute.Parameters.AddWithValue(TestStoryPathHash);
                muteId = Assert.IsType<long>(await readMute.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            }
            await store.UnmuteStoryAsync(muteId);
            Assert.Single(await store.FilterMutedFindingsAsync(stories, context));

            /* Retention: an aged finding is purged, the fresh one survives. */
            var aged = new AnalysisFinding
            {
                FindingId = CollectionIdGenerator.Next(),
                AnalysisTime = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-40), DateTimeKind.Unspecified),
                ServerId = TestServerId,
                ServerName = TestServerName,
                Severity = 0.5,
                Confidence = 0.5,
                Category = "aged",
                StoryPath = "AGED",
                StoryPathHash = "an1-aged-hash",
                StoryText = "aged row for the retention sweep",
                RootFactKey = "AGED",
                FactCount = 1
            };
            await store.InsertFindingsAsync(new List<AnalysisFinding> { aged }, context);
            await store.CleanupOldFindingsAsync(retentionDays: 30);

            var afterCleanup = await store.GetRecentFindingsAsync(TestServerId, hoursBack: 24 * 90);
            Assert.Equal(roundTripped.FindingId, Assert.Single(afterCleanup).FindingId);

            /* Passthrough proof: the ported analysis SQL's view names resolve. */
            using (var view = new NpgsqlCommand("SELECT COUNT(*) FROM v_wait_stats WHERE server_id = $1", connection))
            {
                view.Parameters.AddWithValue(TestServerId);
                Assert.Equal(0L, await view.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {TestServerId}; DELETE FROM analysis_muted WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---------------- gated: all-servers (NULL) + legacy-0 mute honoring ---------------- */

    [Fact]
    public async Task MuteReads_HonorGlobalNull_AndLegacyZero_ButNotOtherServers()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live all-servers mute test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteGlobalMuteRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgFindingStore(postgres);

        var bodySucceeded = false;
        try
        {
            /* The MCP "mute across all servers" path hands the store serverId 0; the write must persist
               NULL (the canonical global marker), not the legacy 0 sentinel. */
            await store.MuteStoryAsync(0, GlobalNullHash, "GLOBAL_NULL", "all-servers e2e mute");
            using (var readServerId = new NpgsqlCommand(
                "SELECT server_id FROM analysis_muted WHERE story_path_hash = $1", connection))
            {
                readServerId.Parameters.AddWithValue(GlobalNullHash);
                var stored = await readServerId.ExecuteScalarAsync(TestContext.Current.CancellationToken);
                Assert.True(stored is null or DBNull, "all-servers mute must persist server_id NULL, not 0");
            }

            /* A legacy pre-fix all-servers row literally carries server_id = 0. */
            await InsertMutedRowAsync(connection, serverId: 0, LegacyZeroHash, "LEGACY_ZERO");

            /* A per-server mute for a DIFFERENT server must NOT leak into the reader's server. */
            await InsertMutedRowAsync(connection, GlobalOtherServerId, OtherServerHash, "OTHER_SERVER");

            var context = new AnalysisContext
            {
                ServerId = GlobalReaderServerId,
                ServerName = "global-mute-reader",
                TimeRangeStart = DateTime.UtcNow.AddHours(-4),
                TimeRangeEnd = DateTime.UtcNow,
            };
            var stories = new List<AnalysisStory>
            {
                GlobalStory(GlobalNullHash, "GLOBAL_NULL"),     /* muted globally via NULL -> dropped */
                GlobalStory(LegacyZeroHash, "LEGACY_ZERO"),     /* muted globally via legacy 0 -> dropped */
                GlobalStory(OtherServerHash, "OTHER_SERVER"),   /* muted only for another server -> survives here */
                GlobalStory(GlobalSurvivorHash, "SURVIVOR"),    /* never muted -> survives */
            };

            var survivors = await store.FilterMutedFindingsAsync(stories, context);
            var survivorHashes = survivors.Select(s => s.StoryPathHash).ToHashSet();

            Assert.DoesNotContain(GlobalNullHash, survivorHashes);    /* NULL global honored */
            Assert.DoesNotContain(LegacyZeroHash, survivorHashes);    /* legacy 0 honored */
            Assert.Contains(OtherServerHash, survivorHashes);         /* other server's mute did not leak */
            Assert.Contains(GlobalSurvivorHash, survivorHashes);
            Assert.Equal(2, survivors.Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteGlobalMuteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static AnalysisStory GlobalStory(string hash, string path) => new()
    {
        Severity = 1.5,
        Confidence = 0.9,
        Category = "cpu",
        StoryPath = path,
        StoryPathHash = hash,
        StoryText = "global-mute reader story",
        RootFactKey = path,
        FactCount = 1
    };

    private static async Task InsertMutedRowAsync(
        NpgsqlConnection connection, int serverId, string hash, string path)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(hash);
        command.Parameters.AddWithValue(path);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("global-mute test row");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteGlobalMuteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_muted WHERE story_path_hash IN ('{GlobalNullHash}', '{LegacyZeroHash}', '{OtherServerHash}', '{GlobalSurvivorHash}');", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---------------- gated: #2448 — a faulted batch persists nothing ---------------- */

    /// <summary>
    /// #2448: a finding batch that faults partway through must persist NOTHING, not the rows that
    /// happened to land first.
    ///
    /// <para>The damage this prevents is invisible by construction, which is why it is worth a live
    /// test rather than a source pin. Every row in a batch shares one <c>analysis_time</c> and
    /// <see cref="PgFindingStore.GetLatestFindingsSql"/> keys on <c>MAX(analysis_time)</c>, so four
    /// committed rows of an intended forty do not read as a truncated set — they read as a complete
    /// analysis that found four problems. The server looks HEALTHIER for the store having failed,
    /// and nothing anywhere says otherwise.</para>
    ///
    /// <para>The fault is induced with a NUL byte in <c>story_text</c>, which PostgreSQL rejects as
    /// 22021 (<c>invalid byte sequence for encoding "UTF8": 0x00</c>). That is the realistic
    /// per-row fault on this table — it carries no primary key, no CHECK and no foreign key, and
    /// every NOT NULL column maps to a non-nullable property with a default — so it is also the
    /// exact case the old per-row catch was there to survive. It should not survive: publishing the
    /// other rows as a complete analysis is the wrong answer, and it is the answer this test
    /// forbids.</para>
    ///
    /// <para>Reverted, the batch commits its first two rows and this fails on
    /// <c>Assert.Empty</c> — the truncated set stated exactly.</para>
    /// </summary>
    [Fact]
    public async Task AFaultedBatch_PersistsNothing_RatherThanATruncatedSetThatReadsAsComplete()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live partial-persist test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeletePartialBatchRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgFindingStore(postgres);

        var bodySucceeded = false;
        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var context = new AnalysisContext
            {
                ServerId = PartialBatchServerId,
                ServerName = PartialBatchServerName,
                TimeRangeStart = windowEnd.AddHours(-4),
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };

            /* Row 3 of 5 faults, so rows 1-2 are already durable at the moment it does — under
               autocommit they stay durable, which is the whole defect. */
            var doomed = await store.FilterMutedFindingsAsync(PartialBatchStories(faultAtRow: 3), context);
            Assert.Equal(5, doomed.Count);

            /* It THROWS rather than degrading, which is the one place this store departs from its
               own no-throw discipline and is deliberate: swallowing a total rollback returns the
               same list a full success returns, so DarlingAnalysisService would set LastAnalysisTime,
               fire AnalysisCompleted and log "Analysis complete - 5 finding(s)" over a store holding
               none of them. That is this very defect moved one layer out. */
            await Assert.ThrowsAsync<PostgresException>(() => store.InsertFindingsAsync(doomed, context));

            /* The assertion #2448 exists for: not "fewer rows", NO rows. A partial set here would
               be indistinguishable from a healthy server. */
            Assert.Empty(await store.GetRecentFindingsAsync(PartialBatchServerId));

            /* And the rollback is not the store simply refusing to write: the same batch without
               the poisoned row commits in full, through the same code path. */
            var clean = await store.FilterMutedFindingsAsync(PartialBatchStories(faultAtRow: null), context);
            await store.InsertFindingsAsync(clean, context);
            Assert.Equal(5, (await store.GetRecentFindingsAsync(PartialBatchServerId)).Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeletePartialBatchRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Five stories a pass would produce, optionally with a NUL byte in the story text of one of
    /// them — <paramref name="faultAtRow"/> is 1-based, or null for a batch that must commit whole.
    /// </summary>
    private static List<AnalysisStory> PartialBatchStories(int? faultAtRow)
    {
        var stories = new List<AnalysisStory>();

        for (var row = 1; row <= 5; row++)
        {
            stories.Add(new AnalysisStory
            {
                Severity = 1.0,
                Confidence = 0.9,
                Category = "waits",
                StoryPath = $"PARTIAL_BATCH_{row}",
                StoryPathHash = $"an1-partial-batch-{row}",
                StoryText = row == faultAtRow ? "log flush detail\0truncated here" : $"row {row} of a five-finding pass",
                RootFactKey = "WRITELOG",
                RootFactValue = row,
                FactCount = 1
            });
        }

        return stories;
    }

    private static async Task DeletePartialBatchRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {PartialBatchServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
