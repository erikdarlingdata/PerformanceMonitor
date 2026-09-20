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
/// #3740 on the real store (gated, DARLING_TEST_PG): the <c>CONFIG_CHANGED</c> finding through the Darling
/// pass — <c>DarlingAnalysisService.AttributeConfigChangesAsync</c> and its trace read — with a stored
/// msg 15457 line. Lite's <c>ConfigChangeAttributionPipelineTests</c> is the twin over DuckDB; this is the
/// same two arms on the engine whose SQL actually carries the de-skew, because a de-skew is a QUERY and a
/// query is proven on the engine that runs it.
///
/// <para><b>The server is planted at UTC+10</b>, and the line's <c>event_time</c> is stored in the server's
/// local frame the way the collector writes <c>fn_trace_gettable</c>'s StartTime. That direction is chosen
/// to SUPPRESS rather than mis-render: a read that forgot to subtract the offset would see the line ten
/// hours in the future — past the capture that observed the change — select nothing, and fall back to the
/// observation anchor, which is the silent no-op #3740 exists to end. So the positive arm fails on
/// selection if the de-skew is lost, not merely on the hour in the prose.</para>
///
/// <para><b>What the anchor buys, asserted.</b> The change is planted 5 h ago and observed 3 h ago. On the
/// trace anchor the after half is COMPLETE (5 h ago + 4 h is in the past) where the observation anchor
/// would have reported 3 h, clamped — the compare answers the question about the change rather than about
/// the snapshot. The control plants a line for a DIFFERENT option and shows the observation anchor intact,
/// with the clamp.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ConfigChangeTraceAnchorLivePostgresTests
{
    private const string ServerName = "darling-config-trace-anchor-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Maxdop = "max degree of parallelism";
    private const string Ctfp = "cost threshold for parallelism";

    /// <summary>UTC+10: the collected offset is <c>DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())</c> — local minus
    /// UTC — so a trace line at UTC instant <c>t</c> is stored as <c>t + 600 min</c>.</summary>
    private const int ServerOffsetMinutes = 600;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AMatchingTraceLineInTheSpan_AnchorsTheCompareOnTheTraceTime_AndSaysChangedAt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-anchor test.");

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
            var changedAtUtc = now.AddHours(-5);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await PlantWaitSeriesAsync(connection, now, ct);
            await PlantServerOffsetAsync(connection, now.AddHours(-30), ct);
            await PlantServerConfigAsync(connection, now.AddHours(-30), Maxdop, 0, ct);
            await PlantServerConfigAsync(connection, now.AddHours(-3), Maxdop, 8, ct);
            await PlantReconfigureLineAsync(connection, changedAtUtc, Maxdop, 0, 8, ct);

            var service = new DarlingAnalysisService(postgres);
            var findings = await service.AnalyzeAsync(ServerId, ServerName, 4, ct);

            Assert.Null(service.WindowEmptyMessage);
            Assert.Null(service.InsufficientDataMessage);

            var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
            var advice = FactAdvice.TryReadStoryText(finding.StoryText);
            Assert.NotNull(advice);
            Assert.Contains($"`{Maxdop}` 0 → 8", advice!.Headline, StringComparison.Ordinal);
            Assert.Contains($"changed at {changedAtUtc:yyyy-MM-dd HH:mm} UTC (default trace: the sp_configure line, msg 15457)", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("first observed it 2 h later", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("compared the 4 h before the change with the 4 h after it", advice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("first observed by the configuration snapshot", advice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("still filling in", advice.Investigation, StringComparison.Ordinal);

            var m = finding.RootFactMetadata!;
            Assert.Equal(ConfigChangeAttribution.AnchorSourceDefaultTrace, m[ConfigChangeAttribution.MetaAnchorClock]);
            Assert.Equal(
                new DateTimeOffset(DateTime.SpecifyKind(changedAtUtc, DateTimeKind.Utc)).ToUnixTimeSeconds(),
                m[ConfigChangeAttribution.MetaChangeTimeUnix]);
            Assert.Equal(
                new DateTimeOffset(DateTime.SpecifyKind(now.AddHours(-3), DateTimeKind.Utc)).ToUnixTimeSeconds(),
                m[ConfigChangeAttribution.MetaObservedAtUnix]);
            Assert.Equal(2.0, m[ConfigChangeAttribution.MetaObservedLagHours], precision: 3);
            Assert.Equal(0, m[ConfigChangeAttribution.MetaObservationGapHours]);
            Assert.True(m.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)));
            Assert.Equal(4.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 3);
            Assert.Equal(0, m[ConfigChangeAttribution.MetaAfterWindowClamped]);
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
    public async Task ATraceLineForADifferentOption_LeavesTheObservationAnchor()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-anchor control.");

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
            await PlantServerOffsetAsync(connection, now.AddHours(-30), ct);
            await PlantServerConfigAsync(connection, now.AddHours(-30), Maxdop, 0, ct);
            await PlantServerConfigAsync(connection, now.AddHours(-3), Maxdop, 8, ct);
            await PlantReconfigureLineAsync(connection, now.AddHours(-5), Ctfp, 5, 50, ct);

            var service = new DarlingAnalysisService(postgres);
            var findings = await service.AnalyzeAsync(ServerId, ServerName, 4, ct);

            var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
            var advice = FactAdvice.TryReadStoryText(finding.StoryText)!;
            Assert.Contains("first observed by the configuration snapshot at", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("27 h since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("default trace", advice.Investigation, StringComparison.Ordinal);

            var m = finding.RootFactMetadata!;
            Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, m[ConfigChangeAttribution.MetaAnchorClock]);
            Assert.False(m.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)));
            Assert.Equal(27.0, m[ConfigChangeAttribution.MetaObservationGapHours], precision: 3);
            Assert.Equal(3.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 1);
            Assert.Equal(1, m[ConfigChangeAttribution.MetaAfterWindowClamped]);

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
    /// ago to now, so the pass window and both halves of a compare anchored anywhere from 6 h ago onward are
    /// observed by the coverage witness — otherwise the dead-collector envelope returns before step 2.5.
    /// </summary>
    private static async Task PlantWaitSeriesAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await PlantWaitAsync(connection, now.AddHours(-30), "CC3740_STALE_WAIT", 60_000L, ct);
        for (var minutesAgo = 600; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(connection, now.AddMinutes(-minutesAgo), "CC3740_BENIGN_WAIT", 100L, ct);
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

    /// <summary>The server's collected offset (<c>server_properties.utc_offset_minutes</c>, V16), which the
    /// trace read de-skews by.</summary>
    private static async Task PlantServerOffsetAsync(NpgsqlConnection connection, DateTime collectionTimeUtc, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4265.3', 'RTM', 3, $5)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName, ServerOffsetMinutes);

    /// <summary>
    /// One stored msg 15457 line as the collector writes it: an ErrorLog event (class 22), <c>error_number</c>
    /// 15457, severity 10, the TextData the raw error-log line (timestamp, spid, then the message — measured
    /// on SQL Server 2022), and <c>event_time</c> in the SERVER's local wall clock: the UTC instant plus the
    /// planted offset. The session's database context is master (DatabaseID 1) — the case the collector
    /// dropped before #3740.
    /// </summary>
    private static async Task PlantReconfigureLineAsync(
        NpgsqlConnection connection, DateTime changedAtUtc, string option, long oldValue, long newValue, CancellationToken ct)
    {
        var storedLocal = DarlingMcpTestData.Naive(changedAtUtc.AddMinutes(ServerOffsetMinutes));
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name,
     event_time, event_name, event_class, spid, database_id, database_name, error_number, severity, text_data)
VALUES ($1, $2, $3, $4, $5, 'ErrorLog', 22, 95, 1, 'master', 15457, 10, $6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(changedAtUtc.AddMinutes(1)), ServerId, ServerName,
            storedLocal,
            $"{storedLocal:yyyy-MM-dd HH:mm:ss.ff} spid95      Configuration option '{option}' changed from {oldValue} to {newValue}. Run the RECONFIGURE statement to install.");
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM server_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM server_properties WHERE server_id = {ServerId}; " +
            $"DELETE FROM default_trace_events WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
