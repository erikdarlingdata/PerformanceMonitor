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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_client_timeouts</c> (#5640) end to end on a real PostgreSQL store: <see cref="DarlingMcpClientTimeoutTools.GetClientTimeouts"/>
/// from the server resolver to the serialized answer, over seeded <c>attention_events</c> and <c>collection_log</c> rows. A quiet
/// window, a busy one, a stale collector, a PostgreSQL target, and ranges that cut a run's span (checked against what
/// <see cref="AttentionEvaluator.ReadSpan"/> says over the same rows, so the tool and the alert cannot disagree). Gated on DARLING_TEST_PG.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class ClientTimeoutsToolLiveTests
{
    private const int QuietId = -5640101;
    private const int BusyId = -5640102;
    private const int StaleId = -5640103;
    private const int PostgresTargetId = -5640104;
    private const int CutId = -5640105;
    private const int AsOfEdgeId = -5640106;
    private const int OneRunId = -5640107;

    private static readonly int[] AllIds = { QuietId, BusyId, StaleId, PostgresTargetId, CutId, AsOfEdgeId, OneRunId };

    private static string Name(int id) => "ct-live" + (-id).ToString(CultureInfo.InvariantCulture);

    [Fact]
    public async Task AQuietWindow_RunsWithNoCountRows_IsEmpty_AtRateZero()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, QuietId, "sqlserver", ct);
            await SeedRunsAsync(connection, QuietId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(QuietId), 1, cancellationToken: ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("No client timeouts", json, StringComparison.Ordinal);

            /* The same rows through the shared judgement: counted, and the count is zero. */
            var (window, _) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, QuietId, now.AddHours(-1), now, 20, ct);
            var (kind, reading) = ClientTimeoutsRead.Classify(window, now);
            Assert.Equal(ClientTimeoutsKind.Quiet, kind);
            Assert.Equal(0, reading!.TotalCount);
            Assert.Equal(12, reading.RunCount + 1);
        });
    }

    [Fact]
    public async Task ABusyWindow_GivesTheExactCountAndRate_AndTheSampledTopLists_WithTheNote()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, BusyId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, BusyId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);

            /* A count row of 5 for EVERY run, the oldest too: the oldest is the base run and its own row is not in the span. */
            foreach (var run in runs)
            {
                await RowAsync(connection, BusyId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            await RowAsync(connection, BusyId, runs[0].AddSeconds(2), "sample", 0, "db1", "etl", "fp-a", "SELECT a FROM t", ct);
            await RowAsync(connection, BusyId, runs[1].AddSeconds(2), "sample", 0, "db1", "etl", "fp-a", "SELECT a FROM t", ct);
            await RowAsync(connection, BusyId, runs[2].AddSeconds(2), "sample", 0, "db1", "etl", "fp-a", "SELECT a FROM t", ct);
            await RowAsync(connection, BusyId, runs[1].AddSeconds(2), "sample", 0, "db2", "web", "fp-b", "SELECT b FROM t", ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(BusyId), 1, limit: 2, cancellationToken: ct);

            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.Equal(55, body.GetProperty("count").GetInt64());
            Assert.Equal(11, body.GetProperty("minutes_covered").GetDouble());
            Assert.Equal(5.0, body.GetProperty("rate_per_minute").GetDouble());
            Assert.Equal(11, body.GetProperty("collector_runs").GetInt32());
            Assert.Equal(4, body.GetProperty("sampled_events_in_range").GetInt64());
            Assert.Contains("subset", body.GetProperty("samples_note").GetString(), StringComparison.Ordinal);
            Assert.Contains("never how many", body.GetProperty("samples_note").GetString(), StringComparison.Ordinal);

            var statements = body.GetProperty("top_statements");
            Assert.Equal("fp-a", statements[0].GetProperty("statement_fingerprint").GetString());
            Assert.Equal(3, statements[0].GetProperty("sampled_events").GetInt64());
            Assert.Equal("SELECT a FROM t", statements[0].GetProperty("sample_statement").GetString());
            Assert.Equal("fp-b", statements[1].GetProperty("statement_fingerprint").GetString());
            Assert.Equal("db1", body.GetProperty("top_databases")[0].GetProperty("database_name").GetString());
            Assert.Equal(3, body.GetProperty("top_databases")[0].GetProperty("sampled_events").GetInt64());
            Assert.Equal("etl", body.GetProperty("top_client_apps")[0].GetProperty("client_app_name").GetString());
            Assert.Equal("web", body.GetProperty("top_client_apps")[1].GetProperty("client_app_name").GetString());

            /* The limit cuts the sampled events only, never the count or the top lists. */
            Assert.True(body.GetProperty("sampled_events_truncated").GetBoolean());
            Assert.Equal(2, body.GetProperty("sampled_events").GetArrayLength());
            Assert.Equal(2, body.GetProperty("sampled_events_returned").GetInt32());
            Assert.Equal(2, statements.GetArrayLength());

            /* The boundary pair: a limit equal to the four sampled events is not truncated, one below is. */
            foreach (var (limitValue, truncated) in new[] { (4, false), (3, true) })
            {
                using var page = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(BusyId), 1, limit: limitValue, cancellationToken: ct));
                var pageBody = page.RootElement.GetProperty("client_timeouts");
                Assert.Equal(truncated, pageBody.GetProperty("sampled_events_truncated").GetBoolean());
                Assert.Equal(limitValue, pageBody.GetProperty("sampled_events_returned").GetInt32());
                Assert.Equal(55, pageBody.GetProperty("count").GetInt64());
            }
        });
    }

    [Fact]
    public async Task AStaleCollector_NewestSuccessfulRunOutsideTheFreshnessWindow_IsUnavailable()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, StaleId, "sqlserver", ct);

            /* One-minute cadence, newest run 25 minutes ago: the freshness window is 10 minutes. Counts exist, and must not be claimed. */
            var runs = await SeedRunsAsync(connection, StaleId, now.AddMinutes(-25), 12, TimeSpan.FromMinutes(1), ct);
            await RowAsync(connection, StaleId, runs[0].AddSeconds(2), "count", 9, null, null, null, null, ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(StaleId), 1, cancellationToken: ct);

            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("has not observed", json, StringComparison.Ordinal);
            Assert.DoesNotContain("client_timeouts", json, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ASingleRun_CannotBeSpanned_AndIsUnavailable()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, OneRunId, "sqlserver", ct);
            await SeedRunsAsync(connection, OneRunId, now.AddSeconds(-20), 1, TimeSpan.FromMinutes(1), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(OneRunId), 1, cancellationToken: ct);

            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(json));
        });
    }

    [Fact]
    public async Task APostgresTarget_IsNotCollected_NotEmpty()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, PostgresTargetId, MonitoredEngineKind.Postgres, ct);
            await SeedRunsAsync(connection, PostgresTargetId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(PostgresTargetId), 1, cancellationToken: ct);

            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("attention", json, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ARangeThatCutsARunsSpan_AgreesWithAttentionEvaluatorReadSpan()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, CutId, "sqlserver", ct);

            /* 18 runs five minutes apart (85 minutes), a count row of 7 at each. hours_back = 1 cuts the oldest six runs off. */
            var runs = await SeedRunsAsync(connection, CutId, now.AddSeconds(-30), 18, TimeSpan.FromMinutes(5), ct);
            var counts = new List<AttentionCountRow>();
            foreach (var run in runs)
            {
                await RowAsync(connection, CutId, run.AddSeconds(2), "count", 7, null, null, null, null, ct);
                counts.Add(new AttentionCountRow(run.AddSeconds(2), 7));
            }

            /* hours_back = 1: the runs inside the hour. */
            var floor = now.AddHours(-1);
            var expected = Expected(runs, counts, floor, now, now);
            Assert.Equal(12, expected.RunCount + 1);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(CutId), 1, cancellationToken: ct);
            AssertAgrees(json, expected);
            Assert.Equal(77, expected.TotalCount);

            /* hours_back = 2: the whole history, a different span. */
            var wide = Expected(runs, counts, now.AddHours(-2), now, now);
            AssertAgrees(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(CutId), 2, cancellationToken: ct), wide);
            Assert.NotEqual(expected.TotalCount, wide.TotalCount);

            /* as_of 20 minutes ago: later runs fall off the end, and freshness is judged at as_of, not now. */
            var asOf = new DateTime(now.AddMinutes(-20).Ticks - (now.AddMinutes(-20).Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            var anchored = Expected(runs, counts, asOf.AddHours(-1), asOf, asOf);
            AssertAgrees(
                await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(CutId), 1, as_of: asOf.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture), cancellationToken: ct),
                anchored);
        });
    }

    [Fact]
    public async Task AnAsOfJustAfterARun_StillCountsThatRunsRow_LikeTheAlertDoes()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, AsOfEdgeId, "sqlserver", ct);

            /* The collection log stamps a run when it starts; its rows carry the time the runner took a moment later (a few
               seconds). An as_of that lands between the two must still count that run's row, as ReadSpan's skew allowance does. */
            var newest = new DateTime((now.AddMinutes(-5).Ticks / TimeSpan.TicksPerSecond) * TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
            var runs = await SeedRunsAsync(connection, AsOfEdgeId, newest, 6, TimeSpan.FromMinutes(1), ct);
            var counts = new List<AttentionCountRow>();
            foreach (var run in runs)
            {
                await RowAsync(connection, AsOfEdgeId, run.AddSeconds(3), "count", 4, null, null, null, null, ct);
                counts.Add(new AttentionCountRow(run.AddSeconds(3), 4));
            }

            var asOf = newest.AddSeconds(1);
            var expected = Expected(runs, counts, asOf.AddHours(-1), asOf, asOf);
            Assert.Equal(20, expected.TotalCount);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(
                postgres, Name(AsOfEdgeId), 1, as_of: asOf.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture), cancellationToken: ct);
            AssertAgrees(json, expected);
        });
    }

    /// <summary>What the alert's own rule says about the runs and count rows inside [floor, ceiling], judged at <paramref name="at"/>.</summary>
    private static AttentionReading Expected(
        IReadOnlyList<DateTime> runs, IReadOnlyList<AttentionCountRow> counts, DateTime floor, DateTime ceiling, DateTime at)
    {
        var inRuns = runs.Where(r => r >= floor && r <= ceiling).OrderByDescending(r => r).ToList();

        /* The alert's rule reads rows up to newest run + skew; the stored read is cut at the range end. The seeded rows sit within
           a few seconds of their run, so a row is in range when its RUN is. */
        var inCounts = counts.Where(c => runs.Any(r => r <= ceiling && r >= floor && c.CollectionTime >= r && c.CollectionTime < r.AddSeconds(10))).ToList();
        var reading = AttentionEvaluator.ReadSpan(
            new AttentionWindow(inRuns, inCounts, Array.Empty<AttentionSampleGroup>(), Array.Empty<AttentionTopItem>(),
                Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>()),
            at);
        return reading ?? throw new InvalidOperationException("the seeded range is not observed under the alert's own rule");
    }

    private static void AssertAgrees(string json, AttentionReading expected)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("client_timeouts", out var body), "expected data, got: " + json);
        Assert.Equal(expected.TotalCount, body.GetProperty("count").GetInt64());
        Assert.Equal(expected.RunCount, body.GetProperty("collector_runs").GetInt32());
        Assert.Equal(Math.Round(expected.CoveredMinutes, 1, MidpointRounding.AwayFromZero), body.GetProperty("minutes_covered").GetDouble());
        Assert.Equal(
            Math.Round(expected.TotalCount / expected.CoveredMinutes, 2, MidpointRounding.AwayFromZero),
            body.GetProperty("rate_per_minute").GetDouble());
    }

    private static DateTime Now() =>
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_client_timeouts test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            await body(postgres, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "attention_events", "collection_log", "servers" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = ANY($1)", connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, AllIds);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, int serverId, string engineKind, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, Name(serverId), ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $2, sql_engine_edition = $3 WHERE server_id = $1",
            serverId, engineKind, engineKind == MonitoredEngineKind.Postgres ? 0 : 3);
    }

    /// <summary>Seeds <paramref name="count"/> successful runs, the newest at <paramref name="newest"/>, each <paramref name="gap"/> older than the last. Newest first.</summary>
    private static async Task<List<DateTime>> SeedRunsAsync(
        NpgsqlConnection connection, int serverId, DateTime newest, int count, TimeSpan gap, CancellationToken ct)
    {
        var runs = new List<DateTime>();
        for (var i = 0; i < count; i++)
        {
            var time = newest - (gap * i);
            runs.Add(time);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1, $2, $3, $4, $5, 5, 'SUCCESS')",
                Math.Abs((long)serverId) * 1000 + i, serverId, Name(serverId), AttentionWindowSql.CollectorName, time);
        }

        return runs;
    }

    private static long _nextEventId = 5640000000L;

    private static async Task RowAsync(
        NpgsqlConnection connection, int serverId, DateTime time, string kind, long count,
        string? db, string? app, string? fingerprint, string? text, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, Interlocked.Increment(ref _nextEventId));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Name(serverId));
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
