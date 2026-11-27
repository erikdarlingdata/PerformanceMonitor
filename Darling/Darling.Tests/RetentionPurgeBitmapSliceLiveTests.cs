using System;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */
/// <summary>
/// #9796: the slice DELETE of the sightings purge on a real store. Every seeded time and the cutoff are fixed January dates,
/// so no row, bound or plan moves with the hour the test runs.
/// </summary>
public sealed class RetentionPurgeBitmapSliceLiveTests
{
    private const string Table = "collect.query_tag_sightings";
    private const string TimeColumn = "last_seen";
    private const int RowsPerDay = 2000;
    private const int Days = 10;
    private static readonly DateTime FirstDay = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Cutoff = FirstDay.AddDays(4);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithSeededStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the purge slice pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var store = new NpgsqlConnection(connectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        var bodySucceeded = false;
        try
        {
            /* Ten days of rows written in time order, so the heap follows the time index and the planner's own choice for one day is the
               plain Index Scan. The statistics are refreshed so both plans are costed from the seeded rows. */
            await ExecAsync(store,
                $"INSERT INTO {Table} (server_id, key_kind, key, first_seen, last_seen)"
                + $" SELECT 7301, 'hash', 'k' || g, t.at, t.at"
                + $" FROM generate_series(0, {(RowsPerDay * Days) - 1}) AS g"
                + $" CROSS JOIN LATERAL (SELECT timestamptz '2026-01-01 00:00:00+00' + g * interval '{(86400.0 / RowsPerDay).ToString(CultureInfo.InvariantCulture)} seconds' AS at) AS t"
                + $" ORDER BY g", ct);
            await ExecAsync(store, $"ANALYZE {Table}", ct);
            await body(store, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(DateTime Start, DateTime End)> ReadBoundsAsync(NpgsqlConnection store, CancellationToken ct)
    {
        await using var probe = new NpgsqlCommand(DarlingRetention.SliceBoundsProbeSql(Table, TimeColumn), store);
        probe.Parameters.AddWithValue(Cutoff);
        var bounds = await DarlingRetention.ReadSliceBoundsAsync(probe, ct);
        Assert.NotNull(bounds);
        return bounds.Value;
    }

    /* The scan node under the DELETE the purge sends for the slice, planned (not run) with the slice's own bounds as parameters. */
    private static async Task<string> SliceDeleteScanNodeAsync(
        NpgsqlConnection store, NpgsqlTransaction? transaction, (DateTime Start, DateTime End) slice, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + DarlingRetention.BoundedSliceDeleteSql(Table, TimeColumn), store, transaction);
        explain.Parameters.AddWithValue(Cutoff);
        explain.Parameters.AddWithValue(slice.Start);
        explain.Parameters.AddWithValue(slice.End);
        using var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
        return plan.RootElement[0].GetProperty("Plan").GetProperty("Plans")[0].GetProperty("Node Type").GetString()!;
    }

    [Fact]
    public async Task TheProbe_ReadsTheOldestExpiredDay_AndNothingOnceNoExpiredRowIsLeft()
    {
        await WithSeededStoreAsync(async (store, ct) =>
        {
            var slice = await ReadBoundsAsync(store, ct);
            Assert.Equal(FirstDay, slice.Start.ToUniversalTime());
            Assert.Equal(FirstDay.AddDays(1), slice.End.ToUniversalTime());

            await ExecAsync(store, $"DELETE FROM {Table} WHERE {TimeColumn} < timestamptz '2026-01-05 00:00:00+00'", ct);
            await using var probe = new NpgsqlCommand(DarlingRetention.SliceBoundsProbeSql(Table, TimeColumn), store);
            probe.Parameters.AddWithValue(Cutoff);
            Assert.Null(await DarlingRetention.ReadSliceBoundsAsync(probe, ct));
        });
    }

    [Fact]
    public async Task TheSliceDelete_IsAnIndexScanOnItsOwn_AndABitmapHeapScanUnderThePurgesSetting_AndTheSettingEndsWithItsTransaction()
    {
        await WithSeededStoreAsync(async (store, ct) =>
        {
            var slice = await ReadBoundsAsync(store, ct);

            Assert.Equal("Index Scan", await SliceDeleteScanNodeAsync(store, transaction: null, slice, ct));

            var underTheSetting = await DarlingRetention.WithSliceDeleteScanSettingAsync(
                store, (transaction, token) => SliceDeleteScanNodeAsync(store, transaction, slice, token), ct);
            Assert.Equal("Bitmap Heap Scan", underTheSetting);

            Assert.Equal("Index Scan", await SliceDeleteScanNodeAsync(store, transaction: null, slice, ct));
        });
    }

    [Fact]
    public async Task TheSliceDelete_UnderTheSetting_RemovesExactlyTheSlicesExpiredRows()
    {
        await WithSeededStoreAsync(async (store, ct) =>
        {
            var slice = await ReadBoundsAsync(store, ct);

            var deleted = await DarlingRetention.WithSliceDeleteScanSettingAsync(
                store,
                async (transaction, token) =>
                {
                    await using var delete = new NpgsqlCommand(DarlingRetention.BoundedSliceDeleteSql(Table, TimeColumn), store, transaction);
                    delete.Parameters.AddWithValue(Cutoff);
                    delete.Parameters.AddWithValue(slice.Start);
                    delete.Parameters.AddWithValue(slice.End);
                    return await delete.ExecuteNonQueryAsync(token);
                },
                ct);

            Assert.Equal(RowsPerDay, deleted);
            await using var left = new NpgsqlCommand($"SELECT count(*) || '|' || to_char(min({TimeColumn}) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') FROM {Table}", store);
            Assert.Equal($"{RowsPerDay * (Days - 1)}|2026-01-02 00:00:00", (string)(await left.ExecuteScalarAsync(ct))!);
        });
    }
}
