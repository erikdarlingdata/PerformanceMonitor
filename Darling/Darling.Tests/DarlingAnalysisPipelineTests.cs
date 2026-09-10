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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Phase-5 analysis slice AN3 — the pipeline orchestrator (DarlingAnalysisService),
/// the drill-down enrichment port (PgDrillDownCollector), the worker's analysis due-loop
/// constants, and the notification adapter. Ungated: the orchestrator carries the twins'
/// method/property/event surface with Lite's 24h gate default and Lite's multi-server
/// data-span SQL; the drill-down exposes EXACTLY Lite's 17 collect-method surface and every
/// query passes the PG-dialect hygiene pins (no QUALIFY, no bare NOW()/CURRENT_TIMESTAMP, no
/// read_parquet, $N positional only) with every FROM/JOIN target resolving to a V4 view or
/// collector table; the worker's hardcoded cadence/budget mirror Lite's App defaults
/// (30 min / 120 s), and the settings adapter carries Lite's notify defaults (1.5 / 360).
/// Gated on DARLING_TEST_PG: migrate, plant a >24h wait_stats history containing a thin-baseline
/// wait spike (the AN2b recipe — one distinct day, so change 1's detector fires ONE
/// ANOMALY_WAIT_PROFILE via the is_new absolute-bar fallback), run the FULL
/// DarlingAnalysisService.AnalyzeAsync, and prove findings persist to analysis_findings
/// (severity &gt; 0, story fields populated, the
/// remediation_action_json column round-tripping), the shared AnalysisNotificationService
/// dispatches them to a recording IFindingAlertSender when above threshold, a second run
/// respects MuteFindingAsync, and the insufficient-data gate no-ops an empty server.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingAnalysisPipelineTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int TestServerId = -646464;
    private const int EmptyServerId = -646465;
    private const string TestServerName = "analysis-pipeline-e2e";
    private const string TestWaitType = "AN3_TEST_WAIT";

    /* ---------------- ungated: orchestrator surface + dialect pins ---------------- */

    /// <summary>
    /// The twins' AnalysisService public surface (Lite/Analysis/AnalysisService.cs and
    /// Dashboard/Analysis/AnalysisService.cs share it) — the port must carry every member.
    /// Dashboard's GetServerLocalNowAsync is deliberately absent: it exists only for its
    /// server-local collection clock; Darling collects host-UTC like Lite.
    /// </summary>
    private static readonly string[] TwinMethodSurface =
    {
        "AnalyzeAsync",
        "CleanupAsync",
        "CollectAndScoreFactsAsync",
        "ComparePeriodsAsync",
        "GetLatestFindingsAsync",
        "GetRecentFindingsAsync",
        "MuteFindingAsync"
    };

    [Fact]
    public void AnalysisService_CarriesTheTwinsSurface_AndLitesGateDefault()
    {
        var type = typeof(DarlingAnalysisService);

        foreach (var method in TwinMethodSurface)
        {
            Assert.True(type.GetMethods().Any(m => m.Name == method), $"missing twin method '{method}'");
        }

        /* Both AnalyzeAsync shapes: the (serverId, serverName, hoursBack) window builder and
           the explicit-context pipeline entry. */
        Assert.Equal(2, type.GetMethods().Count(m => m.Name == "AnalyzeAsync"));
        Assert.NotNull(type.GetMethod("AnalyzeAsync", new[] { typeof(AnalysisContext) }));

        /* The twins' run-state surface + completion event. */
        Assert.NotNull(type.GetProperty("IsAnalyzing"));
        Assert.NotNull(type.GetProperty("LastAnalysisTime"));
        Assert.NotNull(type.GetProperty("InsufficientDataMessage"));
        Assert.NotNull(type.GetEvent("AnalysisCompleted"));

        /* Dashboard's server-clock probe is NOT ported — host-UTC windows (Lite semantics). */
        Assert.Null(type.GetMethod("GetServerLocalNowAsync"));

        /* Lite's empirically-validated minimum data span, defaulted identically. Data-source
           construction is lazy (no connection is opened), so this stays ungated. */
        using var neverOpened = NpgsqlDataSource.Create("Host=localhost;Database=never-opened");
        var service = new DarlingAnalysisService(neverOpened);
        Assert.Equal(24.0, service.MinimumDataHours);
    }

    [Fact]
    public void AnalysisService_DataSpanSql_IsLitesMultiServerShape_PgDialect()
    {
        var sql = DarlingAnalysisService.TotalDataSpanSql;

        /* Lite's query verbatim: EXTRACT(EPOCH FROM ...) over the raw wait_stats table with
           the multi-server filter (the single-server Dashboard twin drops the filter). */
        Assert.Contains("EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0", sql, StringComparison.Ordinal);
        Assert.Contains("FROM wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);

        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("qualify", lower);
        Assert.DoesNotContain("now(", lower);
        Assert.DoesNotContain("current_timestamp", lower);
        Assert.DoesNotContain("N'", sql);
        Assert.DoesNotContain("@", sql);
    }

    /* ---------------- ungated: drill-down surface + dialect pins ---------------- */

    /// <summary>
    /// Lite's DrillDownCollector collect-method surface (ordinal-sorted), enumerated literally:
    /// the port must carry every one of these — same names — and nothing extra. A future Lite
    /// addition must be ported AND added here, so drift fails this test rather than silently
    /// thinning Darling's finding detail. (CollectPlanNodes is a static tree-walk helper, not
    /// a collector — the instance-only filter excludes it, like Lite.)
    /// </summary>
    private static readonly string[] LiteDrillDownMethodSurface =
    {
        "CollectAutogrowthPercentFiles",
        "CollectBadActorDetail",
        "CollectConfigIssues",
        "CollectFileLatencyBreakdown",
        "CollectLockModeBreakdown",
        "CollectParameterSensitiveQueries",
        "CollectPendingGrants",
        "CollectPlanAdvisoryDetail",
        "CollectPlanAnalysis",
        "CollectQueriesAtSpike",
        "CollectReconstructedBlockingChains",
        "CollectRegressedQueries",
        "CollectTempDbBreakdown",
        "CollectTopBlockingChains",
        "CollectTopCpuQueries",
        "CollectTopDeadlocks",
        "CollectTopSpillingQueries"
    };

    [Fact]
    public void DrillDown_CarriesLitesCollectMethodSurface()
    {
        var ported = typeof(PgDrillDownCollector)
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .Where(m => m.Name.StartsWith("Collect", StringComparison.Ordinal))
            .Select(m => m.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(LiteDrillDownMethodSurface, ported);

        /* The public enrichment entry point Lite's AnalysisService calls. */
        Assert.NotNull(typeof(PgDrillDownCollector).GetMethod("EnrichFindingsAsync"));
    }

    [Fact]
    public void DrillDown_AllSql_CoversEveryQuery_TwoForTheSpikeMethod()
    {
        /* 17 collect methods, one query each, except CollectQueriesAtSpike (peak lookup +
           queries-at-peak = 2). The reconstructed-chain method's DMV-snapshot fallback runs
           through the shared PgBlockingPairRowQuery.DmvSnapshotSql, already pinned in the
           fact-collector suite. */
        Assert.Equal(LiteDrillDownMethodSurface.Length + 1, PgDrillDownCollector.AllSql.Count);
    }

    [Fact]
    public void DrillDown_AllSql_PgDialect_NoDuckDbOnlyConstructs_NoBareNow_PositionalParams()
    {
        foreach (var sql in PgDrillDownCollector.AllSql)
        {
            var upper = sql.ToUpperInvariant();

            /* QUALIFY is DuckDB-only — Lite's drill-down never used it; the port must not either. */
            Assert.DoesNotContain("QUALIFY", upper, StringComparison.Ordinal);

            /* Bare now()/CURRENT_TIMESTAMP is timestamptz — it would compare the naive-UTC
               columns in the PG server's time zone. Every window bound is a bound
               Kind-Unspecified parameter. */
            Assert.DoesNotContain("NOW(", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", upper, StringComparison.Ordinal);

            /* Lite's v_* views union the parquet archive; Darling has no parquet tier. */
            Assert.DoesNotContain("READ_PARQUET", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("UNION ALL BY NAME", upper, StringComparison.Ordinal);

            /* Postgres has no N'' literals and no @named parameters — $N positional only. */
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DrillDown_AllSql_AnyValue_OnlyInTheRegressedQueriesQuery()
    {
        /* any_value() is standard SQL:2023, in Postgres since 16 (product minimum PG is 17).
           It is deliberate in the plan-regression re-detection and nowhere else — the same
           confinement the fact collector pins for its PlanRegressionSql. */
        Assert.Contains("any_value(query_text)", PgDrillDownCollector.RegressedQueriesSql, StringComparison.Ordinal);
        foreach (var sql in PgDrillDownCollector.AllSql)
        {
            if (sql.Contains("any_value", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Equal(PgDrillDownCollector.RegressedQueriesSql, sql);
            }
        }
    }

    [Fact]
    public void DrillDown_AllSql_EveryFromJoinTarget_ResolvesToAV4ViewOrACollectorTable()
    {
        /* The V4 passthrough views, parsed from the migration itself so this can't drift. */
        var v4 = PgMigrations.Scripts.Single(m => m.Version == 4);
        var views = Regex.Matches(v4.Sql, @"CREATE OR REPLACE VIEW (v_\w+)")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        var tables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);

        /* Keyed SIDE tables are a third legal category, and deliberately not in either set above: they
           are not collector tables (nothing collects INTO them per sweep; a bespoke upsert path writes
           them, and they are pruned on last_seen rather than by drop_chunks) and they are not V4
           passthrough views. #2150's query_store_text is the first one a drill-down reads, because
           statement text moved out of the fact row and has to be resolved back. Sourced from the store
           class's own TableName rather than spelled here, so a rename cannot leave this guard asserting
           against a table that no longer exists. */
        var sideTables = new[] { QueryStoreTextStore.TableName }
            .Select(t => t.Contains('.', StringComparison.Ordinal) ? t.Split('.')[^1] : t)
            .ToHashSet(StringComparer.Ordinal);

        foreach (var sql in PgDrillDownCollector.AllSql)
        {
            /* Scan the SQL the SERVER sees, not the comments explaining it. Two things in the raw text
               are not relation references and would be read as one:

               1. `--` comments. These queries carry long rationale comments, and English prose about a
                  join contains the word "join" followed by a word ("an equi-join here would ...") —
                  which a bare FROM/JOIN scan reads as a relation named `here`. A guard that fails
                  because someone explained a join in a comment is a broken guard, not a strict one.
               2. `IS [NOT] DISTINCT FROM`, a COMPARISON OPERATOR that happens to end in the word FROM,
                  so its right-hand operand parses as a relation name.

               Stripping both rather than loosening the assertion: the point of this guard is that every
               real relation reference resolves, and that must stay exact. */
            var scanSql = Regex.Replace(sql, @"--[^\n]*", " ");
            scanSql = Regex.Replace(scanSql, @"\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\b", " ", RegexOptions.IgnoreCase);

            /* CTE names defined by this query are legal FROM targets too (WITH x AS ( / , y AS (). */
            var ctes = Regex.Matches(scanSql, @"(?:WITH|,)\s*(\w+)\s+AS\s*\(", RegexOptions.Singleline)
                .Select(m => m.Groups[1].Value)
                .ToHashSet(StringComparer.Ordinal);

            foreach (Match m in Regex.Matches(scanSql, @"\b(?:FROM|JOIN)\s+(\w+)", RegexOptions.IgnoreCase))
            {
                var target = m.Groups[1].Value;
                Assert.True(
                    views.Contains(target) || tables.Contains(target) || ctes.Contains(target)
                        || sideTables.Contains(target),
                    $"FROM/JOIN target '{target}' resolves to no V4 view, collector table, keyed side table, or CTE in:\n{sql}");
            }
        }
    }

    [Fact]
    public void DrillDown_ReconstructedChainSql_SharesThePairRowFragments_AndTruncatesSqlText()
    {
        /* The SPID-0 phantom-root filter and the pair cap are behavior, not style — the
           drill-down consumer must carry the same shared fragments as the fact collector so
           the apex and the reader's column order cannot drift between the two. */
        var sql = PgDrillDownCollector.ReconstructedChainsSql;
        Assert.Contains("blocking_spid IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("blocking_spid <> 0", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 5000", sql, StringComparison.Ordinal);
        Assert.Contains("monitor_loop", sql, StringComparison.Ordinal);
        Assert.Contains("contentious_object", sql, StringComparison.Ordinal);

        /* Unlike the fact collector's full-text pair rows, the drill-down payload truncates
           both SQL texts (Lite's shape). */
        Assert.Contains("LEFT(blocked_sql_text, 500)", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(blocking_sql_text, 500)", sql, StringComparison.Ordinal);
    }

    /* ---------------- ungated: worker cadence + notification adapter pins ---------------- */

    [Fact]
    public void Worker_AnalysisCadence_MirrorsLitesAppDefaults()
    {
        /* Lite's App.xaml.cs defaults, now seeded from config.Analysis into config_alert_settings and read
           live (control-plane Stage 1): AnalysisEnabled = true, AnalysisIntervalMinutes = 30, notify-severity
           1.5. NotificationsEnabled keeps Darling's shipped notify-on default (Lite's App default is false).
           AnalysisTimeoutSeconds = 120 stays hardcoded (not a knob). */
        var analysis = new AnalysisConfig();
        Assert.True(analysis.Enabled);
        Assert.Equal(30, analysis.IntervalMinutes);
        Assert.True(analysis.NotificationsEnabled);
        Assert.Equal(1.5, analysis.NotifySeverity);
        Assert.Equal(TimeSpan.FromSeconds(120), DarlingWorker.AnalysisTimeout);
        /* #2299: the shutdown grace a stopping sweep grants its in-flight analysis pass. Must stay WELL
           inside the worker's 15s shutdown drain budget and the host's 30s ShutdownTimeout — the await
           runs inside a drained sweep body, so a grace near either ceiling would turn a clean stop into
           a force-kill. */
        Assert.Equal(TimeSpan.FromSeconds(5), DarlingWorker.AnalysisShutdownGrace);
    }

    [Fact]
    public void FindingAlertSender_ImplementsTheSharedSeam_AndSettingsCarryLitesNotifyDefaults()
    {
        Assert.True(typeof(IFindingAlertSender).IsAssignableFrom(typeof(DarlingFindingAlertSender)));

        /* Lite's App defaults: AnalysisNotifySeverity = 1.5, AnalysisNotifyCooldownMinutes =
           360 — the shared AnalysisNotificationService consumes these off IAlertSettings. */
        var settings = new DarlingAlertSettings(new DarlingConfig());
        Assert.Equal(1.5, settings.AnalysisNotifySeverity);
        Assert.Equal(360, settings.AnalysisNotifyCooldownMinutes);
    }

    /* ---------------- gated: full-pipeline end-to-end ---------------- */

    [Fact]
    public async Task EndToEnd_FullAnalysisPipeline_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis-pipeline test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* ---- plant >24h of wait_stats history — the AN2b recipe: one hour×dow bucket
                    (a past Monday 10:00 UTC, 8-14 days back), 12 collections at 5-minute
                    spacing, 60000ms of TestWaitType per collection except two zero rows
                    (counter-reset + genuine idle). The span from the first history row to the
                    anomalous window a week later is ~7 days, so the pipeline's 24h data-span
                    gate passes on real MIN/MAX arithmetic. */
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var historyStart = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);

            for (var i = 0; i < 12; i++)
            {
                var delta = (i == 5 || i == 6) ? 0L : 60000L;
                await InsertWaitAsync(connection, (long)(i + 1), historyStart.AddMinutes(5 * i), 10L, delta);
            }

            /* The FOLLOWING Monday 10:00 UTC — same (hour, dow) baseline bucket, 1-7 days back,
               still in the past. The history is ONE distinct Monday, so under the change-2 quality
               gate the WaitMsPerSec baseline (Full tier) is UNtrustworthy (Full needs >= 3 distinct
               days), and change 1's wait detector falls back to the absolute peak-rate bar (is_new)
               rather than a ratio. Three 5-minute-spaced collections at 200000ms each: the first is
               dropped (no prior in-window interval), the second and third rate at 200000/300s ≈ 666.7
               ms/sec — past the 250 ms/sec WaitProfileFallbackMsPerSec bar → ONE ANOMALY_WAIT_PROFILE
               fact (ratio = the is_new sentinel 100 → severity saturates to 1.0), enough to root a story. */
            var analysisTime = historyStart.AddDays(7);
            await InsertWaitAsync(connection, 100L, analysisTime.AddMinutes(5), 50L, 200000L);
            await InsertWaitAsync(connection, 101L, analysisTime.AddMinutes(10), 50L, 200000L);
            await InsertWaitAsync(connection, 102L, analysisTime.AddMinutes(15), 50L, 200000L);

            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(30),
                ServerUtcOffset = TimeSpan.Zero
            };

            var service = new DarlingAnalysisService(postgres);

            /* ---- run 1: the full pipeline — gate passes, facts collect, the anomaly
                    detector fires, the story persists through the two-phase save. */
            var findings = await service.AnalyzeAsync(context);

            Assert.Null(service.InsufficientDataMessage);
            Assert.NotNull(service.LastAnalysisTime);
            Assert.False(service.IsAnalyzing);

            var anomaly = Assert.Single(findings,
                f => f.StoryPath.Contains("ANOMALY_WAIT_PROFILE", StringComparison.Ordinal));
            Assert.True(anomaly.Severity > 0, "the persisted finding must carry positive severity");
            Assert.True(anomaly.Confidence > 0);
            Assert.False(string.IsNullOrEmpty(anomaly.StoryPathHash));
            Assert.False(string.IsNullOrEmpty(anomaly.Category));
            Assert.Equal(TestServerId, anomaly.ServerId);
            Assert.Equal(TestServerName, anomaly.ServerName);

            /* Persisted: the store read-back returns the same run with every story field
               populated and remediation_action_json round-tripping (null is legal for an
               anomaly finding — the column itself must exist and read back without error). */
            var persisted = await service.GetRecentFindingsAsync(TestServerId);
            var persistedAnomaly = Assert.Single(persisted, f => f.StoryPathHash == anomaly.StoryPathHash);
            Assert.Equal(anomaly.FindingId, persistedAnomaly.FindingId);
            Assert.True(persistedAnomaly.Severity > 0);
            Assert.NotNull(persistedAnomaly.StoryText);
            Assert.Equal(DateTimeKind.Utc, persistedAnomaly.AnalysisTime.Kind);

            using (var actionColumn = new NpgsqlCommand(
                "SELECT remediation_action_json FROM analysis_findings WHERE server_id = $1 AND finding_id = $2", connection))
            {
                actionColumn.Parameters.AddWithValue(TestServerId);
                actionColumn.Parameters.AddWithValue(anomaly.FindingId);
                var value = await actionColumn.ExecuteScalarAsync(ct);
                /* Present-or-null: a stored action deserializes via the shared serializer
                   (already proven by the AN1 round-trip); an anomaly finding without one
                   stores NULL. Either way the column reads back. */
                Assert.True(value is DBNull || value is string);
            }

            /* ---- notification: the shared AnalysisNotificationService routes the run's
                    findings through the IFindingAlertSender seam — exactly the at-or-above-
                    threshold set, composed with the analysis metric-name shape. */
            var sender = new RecordingFindingAlertSender();
            var notifier = new AnalysisNotificationService(
                sender,
                new TestAlertSettings { AnalysisNotifySeverity = 0.3, AnalysisNotifyCooldownMinutes = 360 },
                f => f.ServerId.ToString(),
                NullLogger<AnalysisNotificationService>.Instance);
            await notifier.NotifyAsync(findings);

            var expectedNotified = findings.Where(f => f.Severity >= 0.3).ToList();
            Assert.NotEmpty(expectedNotified);
            Assert.Equal(expectedNotified.Count, sender.Sent.Count);
            var sentAnomaly = Assert.Single(sender.Sent,
                a => a.MetricName.Contains(anomaly.StoryPathHash[..8], StringComparison.Ordinal));
            Assert.StartsWith("Analysis: ", sentAnomaly.MetricName, StringComparison.Ordinal);
            Assert.Equal(TestServerId.ToString(), sentAnomaly.ServerId);
            Assert.Equal(TestServerName, sentAnomaly.ServerName);
            Assert.Equal(anomaly.Severity, sentAnomaly.Severity);
            Assert.Equal(0.3, sentAnomaly.NotifyThreshold);

            /* Inside the cooldown, an identical batch does not re-notify. */
            await notifier.NotifyAsync(findings);
            Assert.Equal(expectedNotified.Count, sender.Sent.Count);

            /* ---- mute: the second run re-detects the same story and the mute filter drops
                    it BEFORE enrichment/insert — nothing new persists under that hash. */
            await service.MuteFindingAsync(anomaly, "an3 e2e mute");
            var findings2 = await service.AnalyzeAsync(context);
            Assert.DoesNotContain(findings2, f => f.StoryPathHash == anomaly.StoryPathHash);

            using (var countRows = new NpgsqlCommand(
                "SELECT COUNT(*) FROM analysis_findings WHERE server_id = $1 AND story_path_hash = $2", connection))
            {
                countRows.Parameters.AddWithValue(TestServerId);
                countRows.Parameters.AddWithValue(anomaly.StoryPathHash);
                Assert.Equal(1L, await countRows.ExecuteScalarAsync(ct));
            }

            /* ---- the insufficient-data gate: an empty server no-ops with the message set
                    (the worker's due-loop relies on this to run-on-schedule safely). */
            var emptyContext = new AnalysisContext
            {
                ServerId = EmptyServerId,
                ServerName = "analysis-pipeline-empty",
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(30),
                ServerUtcOffset = TimeSpan.Zero
            };
            Assert.Empty(await service.AnalyzeAsync(emptyContext));
            Assert.NotNull(service.InsufficientDataMessage);
            Assert.Contains("Not enough data", service.InsufficientDataMessage, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertWaitAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, long tasks, long waitMs)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(TestWaitType);
        command.Parameters.AddWithValue(tasks);
        command.Parameters.AddWithValue(waitMs);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id IN ({TestServerId}, {EmptyServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({TestServerId}, {EmptyServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({TestServerId}, {EmptyServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Records every dispatched finding alert; no persisted history (seed = null).</summary>
    private sealed class RecordingFindingAlertSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName)
            => Task.FromResult<DateTime?>(null);

        public Task SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.CompletedTask;
        }
    }

    /// <summary>Minimal IAlertSettings for the notification test — channels all off.</summary>
    private sealed class TestAlertSettings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 587;
        public bool SmtpUseSsl => true;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity { get; init; } = 1.5;
        public int AnalysisNotifyCooldownMinutes { get; init; } = 360;
        public string TriageBaseUrl { get; init; } = "";
    }
}
