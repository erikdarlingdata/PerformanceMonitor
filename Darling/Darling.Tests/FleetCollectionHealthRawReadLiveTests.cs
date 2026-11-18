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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet collection health raw read is two levels (#8570), and this is the one rule that holds it: the raw read, and
/// the read composed from the hourly aggregate plus the raw head and hole slices, give the same answer as the one-level
/// raw read they replaced. The one-level statement is kept below as <see cref="OneLevelRawSql"/>, the reference. Each fact
/// seeds every collector at a multiple of its default interval, with the abandon, partial-failure and other markers on a few percent of
/// the rows, compresses the chunks older than a day as the store does, and creates and refreshes the hourly aggregate.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here goes through
   CollectionHealthAggregateTests.OpenStoreAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database
   through ScratchPostgres and then works entirely inside it, so it cannot race live collection. */
public sealed class FleetCollectionHealthRawReadLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the fleet collection health raw read equality facts.";

    /// <summary>The raw read as it was before #8570: one level, every aggregate over every row of the window. Kept as the
    /// reference the two-level read must equal row for row.</summary>
    internal const string OneLevelRawSql = $@"
SELECT
    server_id,
    collector_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS'
              AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
             THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
    MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
    SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
    MAX(collection_time) AS last_run_time,
    SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
             THEN 1 ELSE 0 END) AS abandoned_count,
    SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
    MAX(CASE WHEN status IS NULL
              OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
             THEN collection_time END) AS last_non_skip_time,
    MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
    MAX(CASE WHEN NOT (status = 'SUCCESS'
                       AND COALESCE(rows_collected, 0) = 0
                       AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
             THEN collection_time END) AS last_zero_row_streak_break_time,
    CASE WHEN MAX(CASE WHEN {CollectionHealthRollupSupport.PartialFailureRunPredicateSql} THEN collection_time END) = MAX(collection_time)
         THEN SUBSTRING(MAX(CASE WHEN {CollectionHealthRollupSupport.PartialFailureRunPredicateSql}
                            THEN TO_CHAR(collection_time, 'YYYYMMDDHH24MISSUS') || error_message END) FROM 21)
    END AS latest_run_note
FROM v_collection_log
WHERE collection_time >= $1
AND   server_id <> 0
AND   {CollectionCycleRow.NotCycleRowSql}
GROUP BY server_id, collector_name";

    private const int Servers = 3;
    private const int Days = 8;

    /// <summary>The test store runs each collector at ten times its default interval, so a fact seeds in seconds. The
    /// spread of statuses, the compressed chunks and the aggregate are the production shape, and the read does not care
    /// how many rows a collector wrote.</summary>
    private const int IntervalMultiplier = 10;

    private static DateTime NaiveUtcNow() => LiveClock.Now();

    private static string PartialNote(int failed, int attempted) =>
        string.Format(CultureInfo.InvariantCulture, PartialDatabaseFailureNote.Format, failed, attempted, "db1, db2", "login failed");

    /// <summary>Seeds <paramref name="servers"/> servers, every collector the schedule defaults ship enabled, one run at its
    /// default interval times <paramref name="intervalMultiplier"/> for <paramref name="days"/> days, set-based. The status of a run follows a deterministic spread:
    /// most are SUCCESS with rows and no error text, and a few percent each carry the partial-failure note, the budget note
    /// (as an ABANDONED run and as a SUCCESS written before the status existed), or one of the other statuses the
    /// aggregates branch on, including a SUCCESS with NULL <c>rows_collected</c>. Two collectors hold a newest run with the
    /// partial-failure note and a partial-failure note cleared by a newer clean run, and the sentinel server's rows are in
    /// the table for the read to leave out.</summary>
    internal static async Task SeedAsync(NpgsqlConnection connection, int servers, int days, int intervalMultiplier, CancellationToken ct)
    {
        var collectors = new StringBuilder();
        foreach (var (name, entry) in CollectorScheduleDefaults.All.Where(c => c.Value is { DefaultEnabled: true, FrequencyMinutes: > 0 }).OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            collectors.Append(collectors.Length == 0 ? "" : ", ")
                .Append(CultureInfo.InvariantCulture, $"('{name}', {entry.FrequencyMinutes * intervalMultiplier})");
        }

        await using var plant = new NpgsqlCommand($@"
WITH n AS (SELECT (now() AT TIME ZONE 'UTC') AS u),
arms (lo, hi, status, rows_collected, error_message) AS
(
    VALUES (0, 2, 'ERROR'::text, NULL::int, 'boom'::text),
           (2, 4, 'SUCCESS', 5, @partial),
           (4, 6, '{EnumeratedCollectorDriver.AbandonedStatus}', 0, @budget120),
           (6, 7, 'SUCCESS', 0, @budget600),
           (7, 8, 'SKIPPED', 0, NULL),
           (8, 9, '{CollectorRuntimePrecondition.ExtensionMissingStatus}', 0, 'no extension'),
           (9, 10, '{CollectorRuntimePrecondition.DegradedStatus}', 0, 'denied'),
           (10, 11, 'PERMISSIONS', 0, 'no permission'),
           (11, 12, '{CollectorRuntimePrecondition.CaptureSessionMissingStatus}', 0, 'no session'),
           (12, 14, 'SUCCESS', 0, NULL),
           (14, 15, 'SUCCESS', NULL, NULL),
           (15, 16, 'SUCCESS', 0, '{EnumeratedCollectorDriver.EmptyEnumerationMessage}'),
           (16, 100, 'SUCCESS', 5, NULL)
),
runs AS
(
    SELECT s AS server_id, d.collector_name, gs.t, (gs.g * 7 + s * 13 + length(d.collector_name) * 3) % 100 AS m
    FROM n
    CROSS JOIN generate_series(1, {servers}) s
    CROSS JOIN (VALUES {collectors}) AS d (collector_name, freq)
    CROSS JOIN LATERAL generate_series(n.u - INTERVAL '{days} days', n.u - INTERVAL '1 minute', d.freq * INTERVAL '1 minute')
        WITH ORDINALITY AS gs (t, g)
)
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
SELECT row_number() OVER (), r.server_id, 'srv-' || r.server_id, r.collector_name, r.t, 1, a.status, a.rows_collected, a.error_message
FROM runs r
JOIN arms a ON r.m >= a.lo AND r.m < a.hi
UNION ALL
SELECT 100000000 + row_number() OVER (), 1, 'srv-1', x.collector_name, n.u - x.minutes_back * INTERVAL '1 minute', 1, 'SUCCESS', 5, x.error_message
FROM n
CROSS JOIN (VALUES ('collector_note_newest', 1, @partialNewest), ('collector_note_newest', 30, NULL),
                   ('collector_note_cleared', 90, @partialNewest), ('collector_note_cleared', 60, NULL)) AS x (collector_name, minutes_back, error_message)
UNION ALL
SELECT 200000000 + row_number() OVER (), 0, 'fleet', 'fleet_maintenance', t, 1, 'ERROR', 0, NULL
FROM n CROSS JOIN LATERAL generate_series(n.u - INTERVAL '{days - 1} days', n.u, INTERVAL '6 hours') t", connection);
        plant.Parameters.AddWithValue("partial", PartialNote(2, 4));
        plant.Parameters.AddWithValue("partialNewest", PartialNote(1, 4));
        plant.Parameters.AddWithValue("budget120", EnumeratedCollectorDriver.WholeCycleBudgetNote(120));
        plant.Parameters.AddWithValue("budget600", EnumeratedCollectorDriver.WholeCycleBudgetNote(600));
        plant.CommandTimeout = 600;
        await plant.ExecuteNonQueryAsync(ct);

        /* The chunks older than a day compressed, as the store's compression policy leaves them. */
        await using var compress = new NpgsqlCommand(
            "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.collection_log', older_than => ((now() AT TIME ZONE 'UTC') - INTERVAL '1 day')) AS c", connection)
        {
            CommandTimeout = 600,
        };
        Assert.True(Convert.ToInt64(await compress.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0, "no chunk was old enough to compress");

        await using var analyze = new NpgsqlCommand("ANALYZE collect.collection_log", connection) { CommandTimeout = 600 };
        await analyze.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One statement's result keyed (server, collector), every ordinal rendered with its name and its database
    /// type, timestamps to the tick, so a type that changed (a SUM left numeric) or a MAX that lost a microsecond cannot
    /// compare equal.</summary>
    private static async Task<SortedDictionary<string, string>> ReadRowsAsync(
        NpgsqlDataSource postgres, string sql, DateTime windowStart, DateTime? headEnd, IReadOnlyList<DateTime>? holeHours, CancellationToken ct)
    {
        /* Every read here goes through the scope the service and the viewer use (#8570), so the equality below holds with the
           larger work_mem the production read runs with. */
        await using var scope = await CollectionHealthRollupSupport.OpenWorkMemReadAsync(postgres, 120, ct);
        await using var command = scope.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
        if (headEnd is { } h)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = h });
        }
        if (holeHours is { Count: > 0 } holes)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = holes.ToArray() });
        }
        var rows = new SortedDictionary<string, string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.Equal(14, reader.FieldCount);
        while (await reader.ReadAsync(ct))
        {
            var fields = Enumerable.Range(0, 14).Select(i => reader.GetName(i) + ":" + reader.GetDataTypeName(i) + "=" + (reader.IsDBNull(i)
                ? "NULL"
                : reader.GetValue(i) switch
                {
                    DateTime d => d.Ticks.ToString(CultureInfo.InvariantCulture),
                    var v => Convert.ToString(v, CultureInfo.InvariantCulture),
                }));
            rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1), string.Join(" ", fields));
        }
        return rows;
    }

    private static void AssertSameRows(SortedDictionary<string, string> expected, SortedDictionary<string, string> actual)
    {
        Assert.Equal(expected.Keys, actual.Keys);
        foreach (var (key, row) in expected)
        {
            Assert.Equal(row, actual[key]);
        }
    }

    private static async Task RunLiveAsync(Func<NpgsqlConnection, NpgsqlDataSource, int, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, SkipText);
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, Servers, Days, IntervalMultiplier, ct);
            await CollectionHealthAggregateTests.RunPolicyAsync(connection, jobId, ct);
            await body(connection, postgres, jobId, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The service's read, the viewer's read and the one-level reference return the same rows with the same
    /// column types over a seven-day window that starts mid-hour, and every marker the aggregates branch on is present in
    /// the answer, so the equality is not between empty columns.</summary>
    [Fact]
    public Task TwoLevelRawRead_EqualsTheOneLevelRead_ForTheServiceAndTheViewer_AgainstDevPostgres() =>
        RunLiveAsync(async (_, postgres, _, ct) =>
        {
            var windowStart = NaiveUtcNow().AddDays(-7).AddMinutes(-23);
            var reference = await ReadRowsAsync(postgres, OneLevelRawSql, windowStart, null, null, ct);
            var service = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthSql, windowStart, null, null, ct);
            var viewer = await ReadRowsAsync(postgres, ViewerDataService.FleetCollectionHealthByServerSql, windowStart, null, null, ct);

            Assert.True(reference.Count > 50, $"expected a few dozen collectors on each of {Servers} servers, got {reference.Count} rows");
            AssertSameRows(reference, service);
            AssertSameRows(reference, viewer);

            var all = string.Join("\n", reference.Values);
            Assert.Contains("latest_run_note:text=1 of 4", all, StringComparison.Ordinal);
            Assert.Contains(reference.Keys, k => k == "1|collector_note_newest");
            Assert.DoesNotContain(reference.Keys, k => k.StartsWith("0|", StringComparison.Ordinal));
            Assert.Contains("latest_run_note:text=NULL", reference["1|collector_note_cleared"], StringComparison.Ordinal);
        });

    /// <summary>The read composed from the hourly aggregate plus the raw head slice equals the one-level raw read over the
    /// same window, the read the aggregate must give the same answer as. The window starts mid-hour, so the head is a
    /// partial hour, and the newest hour sits above the aggregate's watermark.</summary>
    [Fact]
    public Task ComposedRead_EqualsAPureRawRead_WithAPartialHeadHour_AgainstDevPostgres() =>
        RunLiveAsync(async (_, postgres, _, ct) =>
        {
            var windowStart = NaiveUtcNow().AddDays(-7).AddMinutes(-23);
            var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);
            Assert.NotEqual(windowStart, headEnd);
            var plan = await CollectionHealthRollupSupport.RollupPlanAsync(postgres, headEnd, ct);
            Assert.True(plan.Usable, "the aggregate must be usable for this to compare composed against raw");
            Assert.Empty(plan.HoleHours);

            var reference = await ReadRowsAsync(postgres, OneLevelRawSql, windowStart, null, null, ct);
            var serviceComposed = await ReadRowsAsync(postgres, CollectionHealthRollupSupport.ComposeFleetSql(DarlingFleetReader.FleetCollectionHealthSql), windowStart, headEnd, null, ct);
            var viewerComposed = await ReadRowsAsync(postgres, CollectionHealthRollupSupport.ComposeFleetSql(ViewerDataService.FleetCollectionHealthByServerSql), windowStart, headEnd, null, ct);
            AssertSameRows(reference, serviceComposed);
            AssertSameRows(reference, viewerComposed);
        });

    /// <summary>The same equality with a hole: one hour of the aggregate missing below its watermark is read from raw for
    /// that hour alone, and the composed read still equals the one-level raw read over the whole window. Without the hole
    /// slice the composed read is short, so the hole is real.</summary>
    [Fact]
    public Task ComposedRead_EqualsAPureRawRead_WithAHoleHour_AgainstDevPostgres() =>
        RunLiveAsync(async (connection, postgres, _, ct) =>
        {
            string materialization;
            await using (var find = new NpgsqlCommand(
                "SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) " +
                "FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = '" +
                TimescaleSupport.CollectionHealthHourlyView + "'", connection))
            {
                materialization = (string)(await find.ExecuteScalarAsync(ct))!;
            }
            await using (var hole = new NpgsqlCommand(
                $"DELETE FROM {materialization} WHERE bucket = date_trunc('hour', (now() AT TIME ZONE 'UTC') - INTERVAL '3 days')", connection))
            {
                Assert.True(await hole.ExecuteNonQueryAsync(ct) > 0);
            }

            var windowStart = NaiveUtcNow().AddDays(-7).AddMinutes(-23);
            var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);
            var reference = await ReadRowsAsync(postgres, OneLevelRawSql, windowStart, null, null, ct);
            var withoutHoles = await ReadRowsAsync(postgres, CollectionHealthRollupSupport.ComposeFleetSql(DarlingFleetReader.FleetCollectionHealthSql), windowStart, headEnd, null, ct);
            Assert.NotEqual(reference, withoutHoles);

            var plan = await CollectionHealthRollupSupport.RollupPlanAsync(postgres, headEnd, ct);
            Assert.True(plan.Usable);
            Assert.Single(plan.HoleHours);
            var serviceSql = CollectionHealthRollupSupport.ComposeFleetSql(DarlingFleetReader.FleetCollectionHealthSql, plan.HoleHours);
            var viewerSql = CollectionHealthRollupSupport.ComposeFleetSql(ViewerDataService.FleetCollectionHealthByServerSql, plan.HoleHours);
            AssertSameRows(reference, await ReadRowsAsync(postgres, serviceSql, windowStart, headEnd, plan.HoleHours, ct));
            AssertSameRows(reference, await ReadRowsAsync(postgres, viewerSql, windowStart, headEnd, plan.HoleHours, ct));
        });

    /// <summary>The read's larger work memory (#8570) holds inside its own transaction only: the statement runs with 64MB,
    /// and the pooled connection it used reports the server's own value again once the scope is disposed, so no later
    /// statement on that connection inherits it. The pool is one connection, so the read and the check share it.</summary>
    [Fact]
    public Task WorkMemRead_RunsWithTheLargerValue_AndLeavesThePooledConnectionAtTheServersValue_AgainstDevPostgres() =>
        RunLiveAsync(async (connection, _, _, ct) =>
        {
            var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString) { MaxPoolSize = 1, Pooling = true };
            await using var pooled = NpgsqlDataSource.Create(builder.ConnectionString);

            async Task<string> ShowWorkMemAsync()
            {
                await using var show = pooled.CreateCommand("SHOW work_mem");
                return (string)(await show.ExecuteScalarAsync(ct))!;
            }

            var serverValue = await ShowWorkMemAsync();
            Assert.NotEqual("64MB", serverValue);

            var windowStart = NaiveUtcNow().AddDays(-7).AddMinutes(-23);
            await using (var scope = await CollectionHealthRollupSupport.OpenWorkMemReadAsync(pooled, 120, ct))
            {
                await using var inside = scope.CreateCommand("SHOW work_mem");
                Assert.Equal("64MB", (string)(await inside.ExecuteScalarAsync(ct))!);

                await using var read = scope.CreateCommand(DarlingFleetReader.FleetCollectionHealthSql);
                read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
            }

            Assert.Equal(serverValue, await ShowWorkMemAsync());
        });
}
