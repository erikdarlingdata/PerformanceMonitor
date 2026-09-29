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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the viewer wave-3 write-back SQL against the Darling store contract (no live Postgres):
/// the alert-history read over config_alert_log, the mute-rule CRUD's column parity with the V3
/// schema and with the service's PgMuteRuleStore shape, and the muted-story read the finding-mute
/// surface uses.
/// </summary>
public sealed class ViewerWave3SqlTests
{
    [Fact]
    public void AlertHistorySql_ReadsConfigAlertLog_PerServer_NewestFirst_ExcludesDismissed()
    {
        Assert.Contains("FROM config_alert_log", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        Assert.Contains("WHERE alert_time >= $1", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        Assert.Contains("server_id = $2", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        /* Mirrors Lite's GetAlertHistoryAsync — dismissed rows are hidden from the view. */
        Assert.Contains("dismissed = FALSE", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY alert_time DESC", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
    }

    [Fact]
    public void AlertHistorySql_SelectsTheHumanColumns_AndNoArchiveView()
    {
        foreach (var column in new[]
        {
            "alert_time", "metric_name", "current_value", "threshold_value",
            "alert_sent", "notification_type", "send_error", "muted", "detail_text", "context_json",
        })
        {
            Assert.Contains(column, ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        }

        /* Darling has no parquet archive tier, so no v_config_alert_log union / source column. */
        Assert.DoesNotContain("v_config_alert_log", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
    }

    [Fact]
    public void AlertHistorySql_PgDialect_NoBareNow_NoNLiterals_PositionalParams()
    {
        Assert.DoesNotContain("now(", ViewerDataService.AlertHistorySql.ToLowerInvariant());
        Assert.DoesNotContain("N'", ViewerDataService.AlertHistorySql);
        Assert.DoesNotContain("@", ViewerDataService.AlertHistorySql);
        Assert.Contains("$1", ViewerDataService.AlertHistorySql);
    }

    private static readonly string[] AllMuteRuleWriteSql =
    {
        ViewerDataService.MuteRulesSelectSql,
        ViewerDataService.MuteRuleInsertSql,
        ViewerDataService.MuteRuleUpdateSql,
        ViewerDataService.MuteRuleSetEnabledSql,
        ViewerDataService.MuteRuleDeleteSql,
    };

    /// <summary>The 11 config_mute_rules columns the store round-trips (PgMuteRuleStore's shape).</summary>
    private static readonly string[] MuteRuleColumns =
    {
        "id", "enabled", "created_at_utc", "expires_at_utc", "reason",
        "server_name", "metric_name", "database_pattern",
        "query_text_pattern", "wait_type_pattern", "job_name_pattern",
    };

    [Fact]
    public void MuteRuleSql_SelectAndInsert_CarryEveryConfigMuteRulesColumn()
    {
        foreach (var column in MuteRuleColumns)
        {
            Assert.Contains(column, ViewerDataService.MuteRulesSelectSql, StringComparison.Ordinal);
            Assert.Contains(column, ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        }

        /* All 11 columns are bound in the insert; newest-first read like PgMuteRuleStore/Lite. */
        Assert.Contains("$11", ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY created_at_utc DESC", ViewerDataService.MuteRulesSelectSql, StringComparison.Ordinal);
    }

    [Fact]
    public void MuteRuleSql_MatchesTheV3SchemaColumns()
    {
        /* Every column the viewer writes must exist in the migration-owned table. */
        var v3 = PgMigrations.Scripts.Single(m => m.Version == 3).Sql;
        Assert.Contains("CREATE TABLE IF NOT EXISTS config_mute_rules", v3, StringComparison.Ordinal);
        foreach (var column in MuteRuleColumns)
        {
            Assert.Contains(column, v3, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void MuteRuleSql_KeyedById_PgDialect()
    {
        Assert.Contains("WHERE id = $1", ViewerDataService.MuteRuleUpdateSql, StringComparison.Ordinal);
        Assert.Contains("WHERE id = $1", ViewerDataService.MuteRuleDeleteSql, StringComparison.Ordinal);
        Assert.Contains("WHERE id = $1", ViewerDataService.MuteRuleSetEnabledSql, StringComparison.Ordinal);

        /* Dialect check on every statement; the parameterless SELECT has no $N to assert. */
        foreach (var sql in AllMuteRuleWriteSql)
        {
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            Assert.DoesNotContain("N'", sql);
            Assert.DoesNotContain("@", sql);
        }

        foreach (var sql in new[]
        {
            ViewerDataService.MuteRuleInsertSql,
            ViewerDataService.MuteRuleUpdateSql,
            ViewerDataService.MuteRuleSetEnabledSql,
            ViewerDataService.MuteRuleDeleteSql,
        })
        {
            Assert.Contains("$1", sql);
        }
    }

    [Fact]
    public void MutedStoriesSql_CarriesMuteId_AndSpansPerServerPlusGlobal()
    {
        /* The finding-mute surface needs mute_id (GetMutedHashesSql omits it) and the same per-server
           + global span the engine's mute filter uses — NULL (the canonical all-servers marker) plus
           legacy server_id = 0 rows written before the NULL fix. */
        Assert.Contains("mute_id", PgFindingStore.GetMutedStoriesSql, StringComparison.Ordinal);
        Assert.Contains("story_path_hash", PgFindingStore.GetMutedStoriesSql, StringComparison.Ordinal);
        Assert.Contains("story_path", PgFindingStore.GetMutedStoriesSql, StringComparison.Ordinal);
        Assert.Contains("FROM analysis_muted", PgFindingStore.GetMutedStoriesSql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 OR server_id IS NULL", PgFindingStore.GetMutedStoriesSql, StringComparison.Ordinal);
        Assert.Contains("OR server_id = 0", PgFindingStore.GetMutedStoriesSql, StringComparison.Ordinal);
    }
}

/// <summary>
/// The wave-3 pure display helpers: the alert row's metric-keyed value formatting and status
/// mapping (Lite's AlertHistoryRow), the shared severity classification, the alert-detail
/// composition (fingerprint disclosure), and the finding row's muted label.
/// </summary>
public sealed class ViewerWave3DisplayTests
{
    private static ViewerAlertRow AlertRow(
        string metric = "High CPU",
        double current = 95.5,
        double threshold = 90,
        bool alertSent = false,
        string notificationType = "tray",
        string? sendError = null,
        bool muted = false,
        string? detailText = null,
        string? contextJson = null)
        => new()
        {
            AlertTime = new DateTime(2026, 7, 1, 3, 30, 0, DateTimeKind.Unspecified),
            MetricName = metric,
            CurrentValue = current,
            ThresholdValue = threshold,
            AlertSent = alertSent,
            NotificationType = notificationType,
            SendError = sendError,
            Muted = muted,
            DetailText = detailText,
            ContextJson = contextJson,
        };

    [Theory]
    [InlineData("High CPU", 95.5, "95.5%")]
    [InlineData("Volume Free Space", 12.34, "12.3%")]
    [InlineData("Poison Wait", 1234.0, "1234 ms")]
    [InlineData("Long-Running Query", 12.0, "12 m")]
    [InlineData("Deadlocks Detected", 3.0, "3")]
    [InlineData("Blocking Detected", 7.0, "7")]
    [InlineData("Some Unmapped Metric", 0.97460, "0.97")]
    public void CurrentValueDisplay_MirrorsLitesMetricKeyedFormatting(string metric, double value, string expected)
    {
        Assert.Equal(expected, AlertRow(metric: metric, current: value).CurrentValueDisplay);
    }

    [Fact]
    public void LegacyTempDbSpaceName_StillRendersAsPercent()
    {
        /* Lite's twin pin (AlertHistoryValueFormatTests). The tempdb token was lowercased across both
           apps' UI (c0109f34), changing this metric's metric_name KEY from "TempDB Space" to
           "tempdb Space"; stored alert-history rows keep the old name, and ordinal matching sent them to
           the :F2 default with no unit. Both spellings must format identically, in both apps. */
        Assert.Equal("87.3%", AlertRow(metric: "TempDB Space", current: 87.3).CurrentValueDisplay);
        Assert.Equal(
            AlertRow(metric: "tempdb Space", current: 87.3).CurrentValueDisplay,
            AlertRow(metric: "TempDB Space", current: 87.3).CurrentValueDisplay);
    }

    /* ── #1846 state-only metrics ──────────────────────────────────────────────────────────────────
       Lite's twin pins live in AlertHistoryValueFormatTests; both grids run the SAME shared predicate
       (AlertMetricClassifier.IsStateOnly), so this suite pins the Darling side of that contract. Darling
       carries the bulk of these rows: every self-alert recovery and every shared-engine resolution
       reaches alert history through Darling's deliverer, where Lite's resolution callback is toast-only
       and writes no row at all. */

    /// <summary>
    /// The exact metric-name strings whose stored 0 is a sentinel rather than a measurement. Held
    /// separately from Lite's copy on purpose: if the two lists ever diverge, that IS the bug.
    /// </summary>
    public static TheoryData<string> StateOnlyMetrics() => new()
    {
        /* The five actionable state metrics — a role_desc, a connected_state_desc, JudgeSync's prose
           reason, the suspend_reason_desc, and the connection failure reason. */
        "AG Failover",
        "AG Replica Disconnected",
        "AG Sync Fell Behind",
        "AG Database Suspended",
        "Server Unreachable",

        /* The AG/connection resolutions. */
        "AG Replica Reconnected",
        "AG Sync Recovered",
        "AG Data Movement Resumed",
        "Server Restored",

        /* Darling's own self-alert recoveries, all written by BuildResolutionRecord, which hardcodes
           CurrentValueText: "resolved" and ThresholdValueText: "" with both numerics null. */
        "Collection Resumed",
        "Capture Restored",
        "Agent Restarted",
        "Store Disk Pressure Resolved",
        "Compression Job Recovered",

        /* The shared alert engine's resolution callback, which reaches history only in Darling. */
        "CPU Resolved",
        "Blocking Cleared",
        "Blocking Wait Cleared",
        "Deadlocks Cleared",
        "Poison Waits Cleared",
        "Long-Running Queries Cleared",
        "tempdb Space Resolved",
        "Volume Free Space Resolved",
        "Long-Running Jobs Cleared",
    };

    [Theory]
    [MemberData(nameof(StateOnlyMetrics))]
    public void StateOnlyMetric_StoredZero_RendersDashNotZeroPointZeroZero(string metric)
    {
        Assert.Equal("—", AlertRow(metric: metric, current: 0).CurrentValueDisplay);
        Assert.Equal("—", AlertRow(metric: metric, threshold: 0).ThresholdValueDisplay);
    }

    [Theory]
    [MemberData(nameof(StateOnlyMetrics))]
    public void StateOnlyMetric_NonZero_StillRendersTheNumber(string metric)
    {
        /* Defensive: the dash is gated on the 0 sentinel, not the metric name alone, so a future
           producer that starts passing a real numeric is not hidden behind it. */
        Assert.Equal("42.00", AlertRow(metric: metric, current: 42).CurrentValueDisplay);
    }

    [Theory]
    [InlineData("High CPU", "0.0%")]
    [InlineData("tempdb Space", "0.0%")]
    [InlineData("Volume Free Space", "0.0%")]
    [InlineData("Long-Running Job", "0.0%")]
    [InlineData("Poison Wait", "0 ms")]
    [InlineData("Long-Running Query", "0 m")]
    [InlineData("Blocking Wait Time", "0 s")]
    [InlineData("Blocking Detected", "0")]
    [InlineData("Deadlocks Detected", "0")]
    [InlineData("Failed Agent Job", "0")]
    public void MeasuredMetric_AtZero_KeepsItsNumber(string metric, string expected)
    {
        /* The dash must never swallow a real measurement that happens to be zero — "no blocking right
           now" and "no value was ever measured" are different statements. */
        Assert.Equal(expected, AlertRow(metric: metric, current: 0).CurrentValueDisplay);
    }

    [Fact]
    public void StoreDiskPressure_AtZero_RendersZero_BecauseZeroPercentFreeIsReal()
    {
        /* Deliberately NOT state-only even though its producer passes no numeric: its text
           ("… has only 3% free (…)") carries a digit, so the parser stores percent-free, and a genuine 0
           there means a FULL VOLUME. This is a Darling-only self-alert, so this pin only exists here. */
        Assert.Equal("0.00", AlertRow(metric: "Store Disk Pressure", current: 0).CurrentValueDisplay);
    }

    /// <summary>
    /// #3169: the status column no longer mirrors Lite's channel mapping, and the two <c>tray</c> rows are
    /// why. This SKU's store is written by the HEADLESS service, which has no tray and no toast code, so a
    /// stored <c>tray</c> claims a UI event that cannot have happened — the reading #2781/#2814 already
    /// removed on the web surface and this grid still had. Both surfaces now share one description with the
    /// SKU's tray answer passed in.
    ///
    /// <para><c>true</c> alongside <c>tray</c> reads "No channel": it is unreachable for a new row, so it
    /// can only be the resolution builder's old hardcoded constant. <c>false</c> alongside <c>tray</c> gets
    /// #2781's neutral "Logged", because it merges no-channel, throttled and failed-webhook and nothing in
    /// the row separates them — decoding it would be asserting the new semantics over old rows.</para>
    /// </summary>
    [Theory]
    [InlineData("email", true, null, "Delivered")]
    [InlineData("email", false, "smtp exploded", "Failed")]
    [InlineData("email", false, null, "Not sent")]
    [InlineData("webhook", true, null, "Delivered")]
    [InlineData("email+webhook", true, null, "Delivered")]
    [InlineData("none", false, null, "No channel")]
    [InlineData("unconfigured", false, null, "No channel configured")]
    [InlineData("undelivered", false, null, "Not sent")]
    [InlineData("muted", false, null, "Muted")]
    [InlineData("tray", true, null, "No channel")]
    [InlineData("tray", false, null, "Logged")]
    public void StatusDisplay_NamesTheChannelState_AndNeverClaimsATrayThisSkuLacks(
        string notif, bool sent, string? error, string expected)
    {
        Assert.Equal(expected, AlertRow(notificationType: notif, alertSent: sent, sendError: error).StatusDisplay);
        Assert.NotEqual("Shown", AlertRow(notificationType: notif, alertSent: sent, sendError: error).StatusDisplay);
    }

    [Fact]
    public void Classification_UsesTheSharedAlertMetricClassifier()
    {
        Assert.True(AlertRow(metric: "Deadlocks Detected").IsCritical);
        Assert.True(AlertRow(metric: "Poison Wait").IsCritical);
        Assert.True(AlertRow(metric: "Blocking Cleared").IsResolved);
        Assert.True(AlertRow(metric: "High CPU").IsWarning);
        Assert.False(AlertRow(metric: "High CPU").IsCritical);
    }

    [Fact]
    public void TimeLocal_TreatsTheStoredValueAsUtc()
    {
        var row = AlertRow();
        var expected = DateTime.SpecifyKind(row.AlertTime, DateTimeKind.Utc).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
        Assert.Equal(expected, row.TimeLocal);
    }

    [Fact]
    public void MutedLabel_ReflectsTheMutedFlag()
    {
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 1,
            ServerName = "SQL2022",
            Severity = 1.0,
            Confidence = 0.9,
            Category = "cpu",
            StoryPath = "a -> b",
            StoryPathHash = "hash",
            StoryText = "",
            RootFactKey = "CPU",
            FactCount = 1,
        };
        var row = new ViewerFindingRow
        {
            SeverityBand = "WARNING",
            RawSeverity = 1.0,
            Title = "t",
            Category = "cpu",
            DatabaseName = "",
            Finding = finding,
        };

        Assert.Equal("", row.MutedLabel);
        Assert.False(row.IsMuted);

        row.IsMuted = true;
        Assert.Equal("Muted", row.MutedLabel);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for every wave-3 write path: the finding mute/unmute
/// visible to a fresh viewer read, the mute-rule CRUD round-trip, and the alert-history read shape
/// (including the dismissed-row exclusion). Shares the serialized "live-postgres" collection so the
/// row churn can't race another class.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerWave3LivePostgresTests
{
    private const int FindingServerId = -717171;
    private const string FindingServerName = "viewer-wave3-finding-e2e";
    private const string FindingHash = "v3-mute-e2e-hash";

    private const string MuteRuleId = "viewer-wave3-mute-rule-e2e";
    private const string MuteRuleServer = "viewer-wave3-rule-server";

    private const int AlertServerId = -818181;
    private const string AlertServerName = "viewer-wave3-alert-e2e";

    [Fact]
    public async Task FindingMuteUnmute_RoundTrips_VisibleToAFreshViewerRead()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live finding-mute test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteFindingRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgFindingStore(postgres);
        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* Seed one persisted finding for the server. */
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var context = new AnalysisContext
            {
                ServerId = FindingServerId,
                ServerName = FindingServerName,
                TimeRangeStart = windowEnd.AddHours(-4),
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var stories = new List<AnalysisStory>
            {
                new AnalysisStory
                {
                    Severity = 1.2,
                    Confidence = 0.8,
                    Category = "cpu",
                    StoryPath = "CPU_PRESSURE -> THREADPOOL",
                    StoryPathHash = FindingHash,
                    StoryText = "seed finding for the viewer mute round-trip",
                    RootFactKey = "CPU_PRESSURE",
                    FactCount = 2,
                },
            };
            var survivors = await store.FilterMutedFindingsAsync(stories, context);
            await store.InsertFindingsAsync(survivors, context);

            /* Before mute: the viewer read shows it, unmuted. */
            var before = Assert.Single(await viewer.GetLatestFindingsAsync(FindingServerId));
            Assert.False(before.IsMuted);
            Assert.Null(before.MuteId);

            /* Mute via the viewer's write-back — a fresh read flags it muted with a mute id (the
               persisted batch is unchanged; the engine would drop it on the next analysis run). */
            await viewer.MuteFindingAsync(FindingServerId, before.Finding);
            var afterMute = Assert.Single(await viewer.GetLatestFindingsAsync(FindingServerId));
            Assert.True(afterMute.IsMuted);
            Assert.NotNull(afterMute.MuteId);

            /* Unmute via the viewer — the flag clears on the next read. */
            await viewer.UnmuteFindingAsync(afterMute.MuteId!.Value);
            Assert.False(Assert.Single(await viewer.GetLatestFindingsAsync(FindingServerId)).IsMuted);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteFindingRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task MuteRuleCrud_RoundTrips_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-rule CRUD test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteMuteRuleRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var rule = new MuteRule
            {
                Id = MuteRuleId,
                Enabled = true,
                CreatedAtUtc = DateTime.UtcNow,
                Reason = "e2e",
                ServerName = MuteRuleServer,
                MetricName = "High CPU",
                DatabasePattern = "StackOverflow",
            };
            await viewer.InsertMuteRuleAsync(rule);

            var inserted = Assert.Single(await viewer.GetMuteRulesAsync(), r => r.Id == MuteRuleId);
            Assert.True(inserted.Enabled);
            Assert.Equal(MuteRuleServer, inserted.ServerName);
            Assert.Equal("High CPU", inserted.MetricName);
            Assert.Equal("StackOverflow", inserted.DatabasePattern);
            Assert.Equal(DateTimeKind.Utc, inserted.CreatedAtUtc.Kind);

            /* Update swaps metric + reason. */
            rule.MetricName = "Poison Wait";
            rule.Reason = "updated";
            await viewer.UpdateMuteRuleAsync(rule);
            var updated = Assert.Single(await viewer.GetMuteRulesAsync(), r => r.Id == MuteRuleId);
            Assert.Equal("Poison Wait", updated.MetricName);
            Assert.Equal("updated", updated.Reason);

            /* Toggle enabled off. */
            await viewer.SetMuteRuleEnabledAsync(MuteRuleId, false);
            Assert.False(Assert.Single(await viewer.GetMuteRulesAsync(), r => r.Id == MuteRuleId).Enabled);

            /* Delete removes it. */
            await viewer.DeleteMuteRuleAsync(MuteRuleId);
            Assert.DoesNotContain(await viewer.GetMuteRulesAsync(), r => r.Id == MuteRuleId);

            /* Purge-expired removes an already-expired rule. */
            await viewer.InsertMuteRuleAsync(new MuteRule
            {
                Id = MuteRuleId,
                Enabled = true,
                CreatedAtUtc = DateTime.UtcNow.AddDays(-2),
                ExpiresAtUtc = DateTime.UtcNow.AddHours(-1),
                ServerName = MuteRuleServer,
            });
            var removed = await viewer.PurgeExpiredMuteRulesAsync();
            Assert.True(removed >= 1);
            Assert.DoesNotContain(await viewer.GetMuteRulesAsync(), r => r.Id == MuteRuleId);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteMuteRuleRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AlertHistory_ReadsRecent_ExcludesDismissed_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-history test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAlertRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToSeconds(DateTime.UtcNow);
            /* One visible row (with a dedup fingerprint) and one dismissed row that must be hidden. */
            await InsertAlertRowAsync(connection, now, "High CPU", 95.5, 90, alertSent: false,
                notificationType: "tray", dismissed: false,
                detailText: "  Database: StackOverflow", contextJson: "{\"DedupKey\":\"abc123\"}");
            await InsertAlertRowAsync(connection, now.AddMinutes(-5), "Deadlocks Detected", 3, 1, alertSent: true,
                notificationType: "email", dismissed: true, detailText: null, contextJson: null);

            var rows = await viewer.GetAlertHistoryAsync(now.AddHours(-1), AlertServerId);

            /* Dismissed row excluded — only the live one comes back. */
            var row = Assert.Single(rows);
            Assert.Equal("High CPU", row.MetricName);
            Assert.Equal("95.5%", row.CurrentValueDisplay);
            Assert.Equal("tray", row.NotificationType);
            /* #3169: a pre-change row on a headless store, read conservatively — #2781's neutral label,
               not the "Shown" this grid used to render for a surface with no tray. */
            Assert.Equal("Logged", row.StatusDisplay);
            /* The read carries the raw context_json (the #1140 dedup fingerprint) through unchanged. */
            Assert.NotNull(row.ContextJson);
            Assert.Contains("DedupKey", row.ContextJson, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAlertRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertAlertRowAsync(
        NpgsqlConnection connection, DateTime alertTimeUtc, string metric,
        double current, double threshold, bool alertSent, string notificationType,
        bool dismissed, string? detailText, string? contextJson)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, dismissed, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(alertTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(AlertServerId);
        command.Parameters.AddWithValue(AlertServerName);
        command.Parameters.AddWithValue(metric);
        command.Parameters.AddWithValue(current);
        command.Parameters.AddWithValue(threshold);
        command.Parameters.AddWithValue(alertSent);
        command.Parameters.AddWithValue(notificationType);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)null ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(dismissed);
        command.Parameters.AddWithValue(false);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)detailText ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)contextJson ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteFindingRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {FindingServerId}; DELETE FROM analysis_muted WHERE server_id = {FindingServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteMuteRuleRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM config_mute_rules WHERE id = $1 OR server_name = $2", connection);
        cleanup.Parameters.AddWithValue(MuteRuleId);
        cleanup.Parameters.AddWithValue(MuteRuleServer);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteAlertRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config_alert_log WHERE server_id = {AlertServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
