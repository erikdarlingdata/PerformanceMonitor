using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Queries tab's "Showing since" banner and the MCP tools' <c>window_truncated</c> read the raw window floor.
/// A floor found inside the window called a quiet start a cut: a server with older rows, quiet for the first day of
/// the window, was reported as holding less than the window. The floor is now the oldest row at or before the
/// window's end, the same way the Custom Views panels find where their data starts, and it is still null when the
/// window itself holds nothing, which the MCP tools report as an empty window.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class RawWindowFloorQuietStartLiveTests
{
    private const int QuietServerId = -497201;
    private const string QuietServerName = "raw-floor-quiet-start";
    private const int EndedServerId = -497202;
    private const string EndedServerName = "raw-floor-ended";
    private const int RecentServerId = -497203;
    private const string RecentServerName = "raw-floor-recent";

    [Fact]
    public async Task AQuietStartInsideTheWindow_IsNotACut_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var start = store.End.AddDays(-7);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.QueryStats, QuietServerId, start, store.End, cancellationToken: ct);

        Assert.Equal(store.End.AddDays(-8), floor);
        Assert.False(RawWindowFloor.IsTruncated(floor, start));
        Assert.Equal(start, RawWindowFloor.EffectiveStart(floor, start));
    }

    [Fact]
    public async Task AServerWhoseRowsEndBeforeTheWindow_ReadsAsNothing_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var start = store.End.AddDays(-7);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.QueryStats, EndedServerId, start, store.End, cancellationToken: ct);

        Assert.Null(floor);
    }

    [Fact]
    public async Task ARecentServer_IsCutAtItsFirstRow_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var start = store.End.AddDays(-7);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.QueryStats, RecentServerId, start, store.End, cancellationToken: ct);

        Assert.Equal(store.End.AddDays(-3), floor);
        Assert.True(RawWindowFloor.IsTruncated(floor, start));
        Assert.Equal(store.End.AddDays(-3), RawWindowFloor.EffectiveStart(floor, start));
    }

    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime end)
        {
            _scratch = scratch;
            DataSource = dataSource;
            End = end;
        }

        public NpgsqlDataSource DataSource { get; }

        public DateTime End { get; }

        public static async Task<SeededStore> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live raw window floor tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, QuietServerId, QuietServerName, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, EndedServerId, EndedServerName, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, RecentServerId, RecentServerName, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);

                /* One old row, then nothing until a day into a 7-day window. */
                await InsertEveryHalfHourAsync(connection, QuietServerId, QuietServerName, end.AddDays(-8), end.AddDays(-8), ct);
                await InsertEveryHalfHourAsync(connection, QuietServerId, QuietServerName, end.AddDays(-6), end, ct);
                /* Rows that all end before a 7-day window starts. */
                await InsertEveryHalfHourAsync(connection, EndedServerId, EndedServerName, end.AddDays(-12), end.AddDays(-9), ct);
                /* A server added 3 days ago. */
                await InsertEveryHalfHourAsync(connection, RecentServerId, RecentServerName, end.AddDays(-3), end, ct);

                return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        private static async Task InsertEveryHalfHourAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    row_number() OVER (),
    t,
    $1,
    $2,
    'FloorDb',
    'HASH1',
    '0x01',
    1000,
    1000,
    10,
    300
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(lastUtc, DateTimeKind.Unspecified));
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>
/// The served window never starts before the one asked for. A floor older than the window's start means the rows
/// reach back past it, so the window was served whole.
/// </summary>
public sealed class RawWindowFloorEffectiveStartTests
{
    private static readonly DateTime s_start = new(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void AFloorBeforeTheStart_ServesTheWholeWindow() =>
        Assert.Equal(s_start, RawWindowFloor.EffectiveStart(s_start.AddDays(-1), s_start));

    [Fact]
    public void AFloorAfterTheStart_IsWhereTheWindowWasServedFrom() =>
        Assert.Equal(s_start.AddHours(2), RawWindowFloor.EffectiveStart(s_start.AddHours(2), s_start));

    [Fact]
    public void NoFloor_ReportsTheRequestedStart() =>
        Assert.Equal(s_start, RawWindowFloor.EffectiveStart(null, s_start));

    /// <summary>The floor walks each table's index in time order, so the walk stops at the first row it meets.</summary>
    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    [InlineData("query_store_stats")]
    public void EveryRawFloorTable_HasTheIndexTheWalkUses(string table) =>
        Assert.True(DataWindowFloor.Source.TryForCollectorTable(table, out _));

    /// <summary>
    /// Every MCP tool takes its served start from <see cref="RawWindowFloor.EffectiveStart"/> and its truncation
    /// verdict from <see cref="RawWindowFloor.IsTruncated"/>, so none reports a start before the window it was asked
    /// about, and none keeps a tolerance of its own. The clutter tool once restated the 90 minutes as a private
    /// constant and compared the floor with <c>requestedStart +</c> by hand, a copy that went stale when the floor
    /// stopped being the first row inside the window. The lines below are the ways a tool computes either by hand.
    /// </summary>
    [Fact]
    public void NoMcpTool_ComputesTheServedStartByHand()
    {
        var byHand = new[]
        {
            "floor ?? requestedStart",
            "WindowFloorTolerance",
            "FromMinutes(90)",
            "requestedStart +",
            "requestedStart.Add(",
        };

        var found = new List<string>();
        foreach (var file in new[] { "DarlingMcpDataTools.cs", "DarlingMcpQueryStoreClutterTools.cs" })
        {
            var text = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
            foreach (var line in byHand)
            {
                if (text.Contains(line, StringComparison.Ordinal))
                {
                    found.Add(file + " has `" + line + "`");
                }
            }
        }

        Assert.True(found.Count == 0, "an MCP tool computes the served start or the truncation verdict by hand: " + string.Join("; ", found));
    }
}
