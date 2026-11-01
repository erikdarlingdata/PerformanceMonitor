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

    /* #7061: the same plan, assertions and numbers as Lite's AttentionWindowReadTests twin, so the two stores leave out the same sampled events. */

    [Fact]
    public async Task TheDurationFloor_AndTheAzdataMetadataShape_SplitTheSampleGroupsTheSameWayInBothStores()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 700;
            var server = ServerId;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, server, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            var at = now.AddSeconds(-28);
            await RowAsync(connection, id++, server, at, "count", 100, null, null, null, null, ct);
            for (var i = 0; i < 6; i++)
            {
                await RowAsync(connection, id++, server, at, "sample", 0, "db1", "Core Microsoft SqlClient Data Provider", null, null, ct, durationUs: 30);
            }

            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-slow", "SELECT slow", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-slow", "SELECT slow", ct, durationUs: 999);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-slow", "SELECT slow", ct, durationUs: 1000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-none", "SELECT no duration", ct, durationUs: null);
            for (var i = 0; i < 3; i++)
            {
                await RowAsync(connection, id++, server, at, "sample", 0, "master", "azdata", "fp-ver", "create table #SVer(ID int, Name sysname)", ct, durationUs: 5_000_000);
            }

            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "azdata", "fp-col", "SELECT clmns.name AS [Name] FROM sys.all_columns AS clmns", ct, durationUs: 5_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "azdata", "fp-editor", "SELECT * FROM dbo.Orders", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "azdata", null, null, ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "master", "azdata", "fp-ver", "create table #SVer(ID int)", ct, durationUs: 20);

            var window = await adapter.GetAttentionWindowAsync(server.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            /* 6 + 4 + 3 + 1 + 3 = 17 samples. Short: the six SqlClient ones, the 999 us one and the azdata #SVer at 20 us = 8. Azdata metadata (not short): 3 #SVer + 1 column read = 4. */
            Assert.Equal(17, window.SampleGroups.Sum(g => g.Samples));
            Assert.Equal(8, window.SampleGroups.Sum(g => g.ShortSamples));
            Assert.Equal(4, window.SampleGroups.Sum(g => g.AzdataMetadataSamples));

            var reading = AttentionEvaluator.Read(window, now)!;
            Assert.Equal(8, reading.ShortSamples);
            Assert.Equal(4, reading.AzdataMetadataSamples);

            var finding = AttentionEvaluator.Evaluate(reading, 1, 10_000);
            Assert.NotNull(finding);
            Assert.Equal(100d * 5 / 17, finding!.JudgedCount, 6);
        });
    }

    /// <summary>The names the C# rule decides on: excluded and counted, edge shapes included (trailing and leading spaces, mixed case, a name that
    /// only starts with the SSMS prefix, the VS Code names without their suffix, a quote and LIKE wildcards).</summary>
    public static readonly string?[] ExcludedAppNames =
    {
        AttentionEvaluator.IntelliSenseClientApp,
        AttentionEvaluator.IntelliSenseClientApp + "   ",
        AttentionEvaluator.IntelliSenseClientApp.ToUpperInvariant(),
        AttentionEvaluator.IntelliSenseClientApp.ToLowerInvariant(),
        "Microsoft SQL Server Management Studio 21 - Transact-SQL IntelliSense",
        "Microsoft SQL Server Management Studio IntelliSense",
        "Microsoft SQL Server Management Studio - Query",
        "Microsoft SQL Server Management Studio",
        "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense - Query",
        "Contoso Management Studio - Transact-SQL IntelliSense",
        "IntelliSense",
        "Microsoft_SQL Server Management Studio - Transact-SQL IntelliSense",
        "Microsoft%SQL Server Management Studio - Transact-SQL IntelliSense",
        "vscode-mssql-GeneralConnection",
        "VSCODE-MSSQL-GENERALCONNECTION-LANGUAGESERVICE",
        "  vscode-mssql-ObjectExplorer  ",
        "vscode-mssql-ObjectExplorer-languageService",
        "vscode-mssql-languageService",
        "vscode-mssql",
        "vscode-mssql-Query",
        "vscode-mssql-ObjectExplorer-x",
        "vscode_mssql-ObjectExplorer",
        "vscode-mssql-%",
        "O'Brien",
        "azdata",
        "SQLAgent - Job Manager",
        "",
        " ",
        null,
    };

    [Fact]
    public async Task TheExcludedAppPredicate_GivesTheSameAnswerAsTheCSharpRule_ForEveryName()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var names = ExcludedAppNames.Concat(AttentionWindowSql.VsCodeMetadataApps).ToArray();
            for (var i = 0; i < names.Length; i++)
            {
                using var command = new NpgsqlCommand(
                    $"SELECT COALESCE({AttentionWindowSql.ExcludedAppPredicate}, FALSE) FROM (SELECT $1::text AS client_app_name) AS t", connection);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)names[i] ?? DBNull.Value });
                var sql = (bool)(await command.ExecuteScalarAsync(ct))!;
                Assert.True(
                    AttentionEvaluator.IsExcludedApp(names[i]) == sql,
                    $"'{names[i]}': C# says {AttentionEvaluator.IsExcludedApp(names[i])}, SQL says {sql}");
            }

            Assert.Contains(names, n => AttentionEvaluator.IsExcludedApp(n));
            Assert.Contains(names, n => !AttentionEvaluator.IsExcludedApp(n));
        });
    }

    [Fact]
    public async Task TheBaseline_IsMadeOfTheSameEvents_TheSampledShareThatPassesScalesEachMinute()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var server = ServerId;
            var t0 = DateTime.UtcNow.Date.AddDays(-2).AddHours(12);
            long id = 800;
            for (var m = 0; m < 6; m++)
            {
                await LogAsync(connection, id++, server, t0.AddMinutes(m), "SUCCESS", ct);
            }

            var rows = new (int Minute, long Count, int Short, int Passing)[]
            {
                (0, 100, 3, 1),   /* 100 x 1/4 = 25 */
                (1, 50, 0, 0),    /* no samples: the full 50 */
                (2, 80, 0, 4),    /* every sample passes: the full 80 */
                (3, 40, 2, 0),    /* every sample is short: 0 */
                (4, 10, 0, 1),    /* a sample with no recorded duration passes: the full 10 */
            };
            foreach (var (minute, count, shortOnes, passing) in rows)
            {
                var at = t0.AddMinutes(minute).AddSeconds(2);
                await RowAsync(connection, id++, server, at, "count", count, null, null, null, null, ct);
                for (var i = 0; i < shortOnes; i++)
                {
                    await RowAsync(connection, id++, server, at, "sample", 0, "db1", "app", null, null, ct, durationUs: 25);
                }

                for (var i = 0; i < passing; i++)
                {
                    await RowAsync(connection, id++, server, at, "sample", 0, "db1", "app", "fp", "SELECT 1", ct, durationUs: minute == 4 ? null : 30_000_000);
                }
            }

            /* Minute 5: the applications the evaluator leaves out by name (SSMS IntelliSense, a VS Code metadata connection) are left out of the
               baseline too, with a long duration so only the name tells them apart: 60 x 1/4 = 15. */
            var namedAt = t0.AddMinutes(5).AddSeconds(2);
            await RowAsync(connection, id++, server, namedAt, "count", 60, null, null, null, null, ct);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL IntelliSense", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL IntelliSense ", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", "VSCODE-mssql-ObjectExplorer", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", "app", "fp", "SELECT 1", ct, durationUs: 30_000_000);

            using var command = new NpgsqlCommand(AttentionWindowSql.BaselineMinutes("collection_log", "attention_events"), connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, server);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t0, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t0.AddMinutes(10), DateTimeKind.Unspecified));
            var got = new System.Collections.Generic.List<long>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    got.Add(reader.GetInt64(1));
                }
            }

            Assert.Equal(new long[] { 25, 50, 80, 0, 10, 15 }, got.ToArray());
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
        string? db, string? app, string? fingerprint, string? text, CancellationToken ct, long? durationUs = null)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us)
              VALUES ($1, $2, $3, 'attention-live', $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)durationUs ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
