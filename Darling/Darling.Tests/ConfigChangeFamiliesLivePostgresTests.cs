/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A10 slice two on the real store (gated, DARLING_TEST_PG): the <c>CONFIG_CHANGED</c> finding through
/// the Darling pass for the two families slice one did not cover — <c>DarlingAnalysisService.AttributeConfigChangesAsync</c>
/// reading <c>database_config</c> and <c>trace_flags</c> with the window rule, diffing, folding same-connect
/// captures across families, and appending the one fact. Lite's <c>ConfigChangeAttributionPipelineTests</c>
/// is the twin over DuckDB; the reads here are Postgres text (the 27-column cast projection, the COALESCE
/// baseline bound) and a read is proven on the engine that runs it.
///
/// <para><b>Two arms.</b> A database's recovery model captured FULL 30 h ago and SIMPLE 3 h ago yields one
/// finding with the database in its seam and the family's headline. MAXDOP 0 → 8 captured 3 h ago and trace
/// flag 4199 appearing two seconds later — the two on-load collectors of one connect — yield ONE finding
/// naming both under the folded headline, with no "earlier event" counted; the server arm's trace read
/// still runs (no line planted, so the observation anchor stands). The server family's own live arms are in
/// <see cref="ConfigChangeTraceAnchorLivePostgresTests"/> and are unchanged.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ConfigChangeFamiliesLivePostgresTests
{
    private const string ServerName = "darling-config-families-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Maxdop = "max degree of parallelism";
    private const string Db = "AdventureWorks";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ADatabaseOptionObservedInsideTheWindow_YieldsOneFinding_NamingTheDatabase()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database-config attribution test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToMinutes(DateTime.UtcNow);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await PlantWaitSeriesAsync(connection, now, ct);
            await PlantDatabaseConfigAsync(connection, now.AddHours(-30), Db, "FULL", "NOTHING", ct);
            await PlantDatabaseConfigAsync(connection, now.AddHours(-3), Db, "SIMPLE", "LOG_BACKUP", ct);

            var service = new DarlingAnalysisService(postgres);
            var findings = await service.AnalyzeAsync(ServerId, ServerName, 4, ct);

            Assert.Null(service.WindowEmptyMessage);
            Assert.Null(service.InsufficientDataMessage);

            var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
            Assert.Equal(Db, finding.DatabaseName);
            Assert.Equal(ConfigChangeAttribution.InformationSeverity, finding.Severity, precision: 9);
            Assert.Equal("config", finding.Category);

            var advice = FactAdvice.TryReadStoryText(finding.StoryText);
            Assert.NotNull(advice);
            Assert.StartsWith($"Database configuration changed: `{Db}` recovery_model FULL → SIMPLE", advice!.Headline, StringComparison.Ordinal);
            Assert.Contains("first observed by the configuration snapshot at", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("27 h since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
            /* The status column flipped on the same capture and is not a change. */
            Assert.DoesNotContain("log_reuse_wait_desc", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("`get_database_config_changes` lists the change", advice.Remediation, StringComparison.Ordinal);

            var m = finding.RootFactMetadata!;
            Assert.Equal(2, m[ConfigChangeAttribution.MetaChangeFamily]);
            Assert.Equal(1, m[ConfigChangeAttribution.MetaChangedSettings]);
            Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, m[ConfigChangeAttribution.MetaAnchorClock]);
            Assert.Equal(27.0, m[ConfigChangeAttribution.MetaObservationGapHours], precision: 3);
            Assert.Equal(3.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 1);
            Assert.Equal(1, m[ConfigChangeAttribution.MetaAfterWindowClamped]);
            Assert.Equal(0, m[ConfigChangeAttribution.MetaCompareUnavailable]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AServerSettingAndATraceFlagObservedAtOneConnect_AreOneFinding()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live same-connect fold test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToMinutes(DateTime.UtcNow);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await PlantWaitSeriesAsync(connection, now, ct);
            await PlantServerConfigAsync(connection, now.AddHours(-30), Maxdop, 0, ct);
            await PlantServerConfigAsync(connection, now.AddHours(-3), Maxdop, 8, ct);
            await PlantTraceFlagAsync(connection, now.AddHours(-30).AddSeconds(2), 3226, ct);
            await PlantTraceFlagAsync(connection, now.AddHours(-3).AddSeconds(2), 3226, ct);
            await PlantTraceFlagAsync(connection, now.AddHours(-3).AddSeconds(2), 4199, ct);

            var service = new DarlingAnalysisService(postgres);
            var findings = await service.AnalyzeAsync(ServerId, ServerName, 4, ct);

            var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
            Assert.Null(finding.DatabaseName);
            var advice = FactAdvice.TryReadStoryText(finding.StoryText)!;
            Assert.StartsWith("Configuration changed: 2 configuration changes observed together", advice.Headline, StringComparison.Ordinal);
            Assert.Contains($"`{Maxdop}` 0 → 8, trace flag 4199 enabled (GLOBAL) — first observed", advice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("earlier configuration change", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("`get_server_config_changes` / `get_trace_flag_changes` list the change", advice.Remediation, StringComparison.Ordinal);

            var m = finding.RootFactMetadata!;
            Assert.Equal(5, m[ConfigChangeAttribution.MetaChangeFamily]);
            Assert.Equal(2, m[ConfigChangeAttribution.MetaChangedSettings]);
            Assert.Equal(0, m[ConfigChangeAttribution.MetaEarlierEventsInWindow]);
            Assert.Equal(8, m[ConfigChangeAttribution.NewInUseKey(Maxdop)]);
            Assert.Equal(1, m[ConfigChangeAttribution.NewInUseKey("trace flag 4199")]);
            Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, m[ConfigChangeAttribution.MetaAnchorClock]);
            /* The folded event's time is the later capture's (the flag's, +2 s), its span the earlier previous. */
            Assert.Equal(
                new DateTimeOffset(DateTime.SpecifyKind(now.AddHours(-3).AddSeconds(2), DateTimeKind.Utc)).ToUnixTimeSeconds(),
                m[ConfigChangeAttribution.MetaObservedAtUnix]);
            Assert.Equal(27.0, m[ConfigChangeAttribution.MetaObservationGapHours], precision: 2);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ─────────────────────────── fixtures ─────────────────────────── */

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>
    /// 30 h of history so the data-span gate passes, then a benign reading every fifteen minutes from 10 h
    /// ago to now, so the pass window and both halves of a compare anchored inside it are observed by the
    /// coverage witness — otherwise the dead-collector envelope returns before step 2.5.
    /// </summary>
    private static async Task PlantWaitSeriesAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await PlantWaitAsync(connection, now.AddHours(-30), "CC3653B_STALE_WAIT", 60_000L, ct);
        for (var minutesAgo = 600; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(connection, now.AddMinutes(-minutesAgo), "CC3653B_BENIGN_WAIT", 100L, ct);
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, string waitType, long deltaWaitMs, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, 50, $6, 0, 50, $6, 0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, waitType, deltaWaitMs);

    /// <summary>One sys.configurations capture row: the collector's shape, value_configured == value_in_use (dynamic).</summary>
    private static async Task PlantServerConfigAsync(NpgsqlConnection connection, DateTime captureTime, string name, long value, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $6, TRUE, TRUE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(captureTime), ServerId, ServerName, name, value);

    /// <summary>One wide sys.databases capture row: the collector's shape with the two columns the arms move set
    /// and the other 25 at plausible defaults, so they compare equal between captures.</summary>
    private static async Task PlantDatabaseConfigAsync(
        NpgsqlConnection connection, DateTime captureTime, string databaseName, string recoveryModel, string logReuseWait, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name,
     state_desc, compatibility_level, collation_name, recovery_model, is_read_only,
     is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on,
     is_auto_update_stats_async_on, is_read_committed_snapshot_on, snapshot_isolation_state,
     is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
     is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
     target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on,
     is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1, $2, $3, $4, $5,
        'ONLINE', 160, 'SQL_Latin1_General_CP1_CI_AS', $6, FALSE,
        FALSE, FALSE, TRUE, TRUE,
        FALSE, TRUE, 'OFF',
        FALSE, TRUE, FALSE, FALSE, FALSE,
        FALSE, FALSE, FALSE, $7, 'CHECKSUM',
        60, 'DISABLED', FALSE,
        FALSE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(captureTime), ServerId, ServerName, databaseName, recoveryModel, logReuseWait);

    /// <summary>One enabled-flag row of a trace_flags capture: the collector writes one per flag DBCC TRACESTATUS(-1)
    /// lists, global scope, and none at all when nothing is enabled.</summary>
    private static async Task PlantTraceFlagAsync(NpgsqlConnection connection, DateTime captureTime, int traceFlag, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(captureTime), ServerId, ServerName, traceFlag);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM server_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM database_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM trace_flags WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
