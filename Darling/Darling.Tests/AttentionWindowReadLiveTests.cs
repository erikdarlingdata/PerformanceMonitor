/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts window read (#5640) on a real PostgreSQL store, the twin of Lite's <c>AttentionWindowReadTests</c>: only
/// this server's SUCCESS runs, the count rows with their times, sample groups, the top lists, other servers and out-of-lookback
/// rows left out, and a quiet server (runs but no rows). Gated on DARLING_TEST_PG; the SQL pins are not.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class AttentionWindowReadLiveTests
{
    private const int ServerId = -5640001;
    private const int OtherServerId = -5640002;
    private const int QuietServerId = -5640003;
    private const int Lookback = 10;

    [Fact]
    public void TheSharedSql_IsBoundNotClockDependent_AndNamesTheLogAndTheKinds()
    {
        foreach (var sql in new[]
        {
            AttentionWindowSql.Runs("collection_log"), AttentionWindowSql.Counts("attention_events"),
            AttentionWindowSql.SampleGroups("attention_events"), AttentionWindowSql.TopStatements("attention_events"),
            AttentionWindowSql.TopBy("attention_events", "database_name"),
        })
        {
            Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("now(", sql.ToLowerInvariant(), StringComparison.Ordinal);
        }

        Assert.Contains("collector_name = 'attention'", AttentionWindowSql.Runs("collection_log"), StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", AttentionWindowSql.Runs("collection_log"), StringComparison.Ordinal);
        Assert.Contains("event_kind = 'count'", AttentionWindowSql.Counts("attention_events"), StringComparison.Ordinal);
        Assert.Contains("event_kind = 'sample'", AttentionWindowSql.SampleGroups("attention_events"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWindow_HoldsOnlyThisServersSuccessfulRunsAndRowsInsideTheLookback()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 1;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await LogAsync(connection, id++, ServerId, now.AddMinutes(-3.5), "SESSION_MISSING", ct);
            await LogAsync(connection, id++, ServerId, now.AddMinutes(-Lookback - 2), "SUCCESS", ct);
            await LogAsync(connection, id++, OtherServerId, now.AddSeconds(-30), "SUCCESS", ct);

            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "count", 7, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-2).AddSeconds(-28), "count", 5, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "db1", "etl", "fp-a", "SELECT 1", ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "db1", "etl", "fp-a", "SELECT 1", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-1).AddSeconds(-28), "sample", 0, "db2", "web", "fp-b", "SELECT 2", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-Lookback - 2), "count", 999, null, null, null, null, ct);
            await RowAsync(connection, id++, OtherServerId, now.AddSeconds(-28), "count", 888, null, null, null, null, ct);

            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.Equal(6, window.RunTimes.Count);
            Assert.True(window.RunTimes.SequenceEqual(window.RunTimes.OrderByDescending(t => t)), "newest first");
            Assert.Equal(12, window.Counts.Sum(c => c.Count));
            Assert.Equal(2, window.Counts.Count);
            Assert.Equal(3, window.SampleGroups.Sum(g => g.Samples));
            Assert.Contains(window.SampleGroups, g => g.DatabaseName == "db1" && g.ClientAppName == "etl" && g.Samples == 2);
            Assert.Equal(new[] { "fp-a", "fp-b" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal("SELECT 1", window.TopStatements[0].SampleText);
            Assert.Equal(new[] { "db1", "db2" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "etl", "web" }, window.TopApplications.Select(t => t.Name).ToArray());
            Assert.Equal(2, window.TopApplications[0].Samples);
        });
    }

    [Fact]
    public async Task TheTopLists_CoverTheSpansSamples_NotTheWholeLookback()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 200;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            /* Round 2 of #5714 (L6): the span is the newest three minutes (the boundary about three minutes back); the old samples
               sit four and a half minutes back, outside it. */
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "dbNew", "appNew", "fp-new", "SELECT new", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);

            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.False(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-new" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbNew" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appNew" }, window.TopApplications.Select(t => t.Name).ToArray());
        });
    }

    [Fact]
    public async Task ASpanThatKeptNoSamples_FallsBackToTheLookbackLists_AndSaysSo()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 300;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, OtherServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await RowAsync(connection, id++, OtherServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);

            var window = await adapter.GetAttentionWindowAsync(OtherServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.True(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-old" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbOld" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appOld" }, window.TopApplications.Select(t => t.Name).ToArray());
        });
    }

    [Fact]
    public async Task ARowPastTheNewestRunPlusSkew_IsInNeitherTheCountNorTheCardsLists()
    {
        /* Final round of #5714 (L2): the next collector run has committed its rows but not yet its SUCCESS log row, so they sit past
           the newest logged run + RowSkewSeconds. The judged count (Take) never sees them; the card's lists must not either. */
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 400;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "count", 7, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "dbIn", "appIn", "fp-in", "SELECT in", ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(60), "count", 100, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late", ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late", ct);

            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.False(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-in" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbIn" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appIn" }, window.TopApplications.Select(t => t.Name).ToArray());
            Assert.Equal(7, AttentionEvaluator.Read(window, now)!.TotalCount);
        });
    }

    [Fact]
    public async Task ASpanWhoseOnlySamplesArePastTheCeiling_StillFallsBackToTheLookbackLists_WithoutThem()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 500;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, OtherServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await RowAsync(connection, id++, OtherServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);
            await RowAsync(connection, id++, OtherServerId, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late", ct);

            var window = await adapter.GetAttentionWindowAsync(OtherServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.True(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-old" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbOld" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appOld" }, window.TopApplications.Select(t => t.Name).ToArray());
        });
    }

    [Fact]
    public async Task AQuietServer_HasRuns_ButNoCountOrSampleRows()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            for (var i = 0; i < 5; i++)
            {
                await LogAsync(connection, 100 + i, QuietServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            var window = await adapter.GetAttentionWindowAsync(QuietServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.Equal(5, window.RunTimes.Count);
            Assert.Empty(window.Counts);
            Assert.Empty(window.SampleGroups);
            Assert.Empty(window.TopStatements);
            Assert.Equal(0, AttentionEvaluator.Read(window, now)!.TotalCount);
        });
    }

    private static async Task RunAsync(Func<DarlingAlertReadAdapter, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Client Timeouts read test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(postgres);

        var bodySucceeded = false;
        try
        {
            await body(adapter, connection, ct);
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
        foreach (var table in new[] { "attention_events", "collection_log" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ($1, $2, $3)", connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, OtherServerId);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, QuietServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task LogAsync(NpgsqlConnection connection, long logId, int serverId, DateTime time, string status, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1, $2, 'attention-live', $3, $4, 5, $5)",
            connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, logId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, AttentionWindowSql.CollectorName);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Text, status);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RowAsync(
        NpgsqlConnection connection, long id, int serverId, DateTime time, string kind, long count,
        string? db, string? app, string? fingerprint, string? text, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text)
              VALUES ($1, $2, $3, 'attention-live', $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
