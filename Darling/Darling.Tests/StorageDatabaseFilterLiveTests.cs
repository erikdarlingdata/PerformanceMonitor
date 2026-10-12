/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244 PR5, lane R1: the database filter on the storage reads get_table_index_sizes, get_database_sizes and get_pvs_stats,
/// run against a store. Every read takes a <see cref="DatabaseFilter"/> through an internal overload (the public MCP method
/// passes <see cref="DatabaseFilter.All"/>). Each live test seeds databases A, B and C and chooses A and B, so a predicate
/// that is missing, or that matches one name only, shows as C (or a missing B) in the answer. The one-name consumers on each
/// read's empty path are covered: a filter that matches nothing is <c>empty</c> "for the chosen databases" when the server has
/// data, and stays <c>unavailable</c> or <c>not_collected</c> when it has none.
/// </summary>
[Collection("live-postgres")]
public sealed class StorageDatabaseFilterLiveTests
{
    internal const string Cleanup =
        "DELETE FROM index_object_stats WHERE server_id = {0}; DELETE FROM database_size_stats WHERE server_id = {0}; " +
        "DELETE FROM pvs_stats WHERE server_id = {0}";

    internal const string A = "StorageFilterA";
    internal const string B = "StorageFilterB";
    internal const string C = "StorageFilterC";

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static string Message(string json) => Parse(json).GetProperty("message").GetString()!;

    private static string[] Names(JsonElement root, string array) =>
        root.GetProperty(array).EnumerateArray().Select(e => e.GetProperty("database_name").GetString()!).ToArray();

    /* ───────────────────────────── seeding ───────────────────────────── */

    internal static Task InsertTableAsync(Npgsql.NpgsqlConnection connection, System.Threading.CancellationToken ct,
        int serverId, string serverName, DateTime t, string db, string table, decimal reservedMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), serverId, serverName, db, "dbo", 100, table, 1,
            "PK_" + table, "CLUSTERED", reservedMb, reservedMb / 2, 1_000L);

    internal static Task InsertFileAsync(Npgsql.NpgsqlConnection connection, System.Threading.CancellationToken ct,
        int serverId, string serverName, DateTime t, string db, decimal sizeMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), serverId, serverName, db, 5, 1, "ROWS", db + "_data",
            "C:\\data\\" + db + ".mdf", sizeMb, sizeMb / 2);

    internal static Task InsertPvsAsync(Npgsql.NpgsqlConnection connection, System.Threading.CancellationToken ct,
        int serverId, string serverName, DateTime t, string db, decimal pvsMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), serverId, serverName, db, 7, true, pvsMb, 1000m);

    /* ───────────────────────────── get_table_index_sizes ───────────────────────────── */

    /// <summary>
    /// Two of three databases: only the chosen databases' tables come back, the largest table (database C's) is not there,
    /// and the <c>history</c> block is the server's own span, equal to the unfiltered call's, though the oldest snapshot
    /// holds only database C (the snapshot anchors never move with the filter).
    /// </summary>
    [Fact]
    public Task TableIndexSizes_TwoOfThree_OnlyTheChosenDatabases_HistoryStaysTheServers() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-tables", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await InsertTableAsync(connection, ct, serverId, serverName, now.AddDays(-10), C, "TC", 800m);
            await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, "TA", 300m);
            await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, "TB", 200m);
            await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), C, "TC", 900m);

            var all = Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, DatabaseFilter.All, ct));
            Assert.Equal(new[] { C, A, B }, Names(all, "tables"));

            var chosen = Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, Of(A, B), ct));
            Assert.Equal(new[] { A, B }, Names(chosen, "tables"));
            Assert.Equal(2, chosen.GetProperty("tables_returned").GetInt32());
            Assert.False(chosen.GetProperty("truncated").GetBoolean());
            Assert.Equal(all.GetProperty("history").GetRawText(), chosen.GetProperty("history").GetRawText());

            var one = Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, DatabaseFilter.One(B), ct));
            Assert.Equal(new[] { B }, Names(one, "tables"));

            /* The public MCP method passes every database, and a whitespace-only name is every database too (M3). */
            Assert.Equal(all.GetRawText(), Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, cancellationToken: ct)).GetRawText());
            Assert.Equal(all.GetRawText(), Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, DatabaseFilter.One("   "), ct)).GetRawText());
        }, Cleanup);

    /// <summary>
    /// The cap of 100 applies after the filter: 105 big tables in database C and two small ones in A. Unfiltered, the page is
    /// 100 tables of C and truncated; filtered to A and B, the page holds both small tables and is not truncated (a cap taken
    /// before the filter would have returned nothing).
    /// </summary>
    [Fact]
    public Task TableIndexSizes_CapAppliesAfterTheFilter() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-tables-cap", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            for (var i = 0; i < 105; i++)
            {
                await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), C, "Big" + i, 1000m + i);
            }

            await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, "SmallA", 5m);
            await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, "SmallB", 4m);

            var all = Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, DatabaseFilter.All, ct));
            Assert.Equal(100, all.GetProperty("tables_returned").GetInt32());
            Assert.True(all.GetProperty("truncated").GetBoolean());
            Assert.DoesNotContain(A, Names(all, "tables"));

            var chosen = Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, Of(A, B), ct));
            Assert.Equal(new[] { A, B }, Names(chosen, "tables"));
            Assert.False(chosen.GetProperty("truncated").GetBoolean());
        }, Cleanup);

    /// <summary>
    /// The empty path. A filter that matches no table, on a server that has index rows, is <c>empty</c> and says so for the
    /// database (one name) or the chosen databases (two or more), never <c>unavailable</c>. A server with no index rows at
    /// all stays <c>unavailable</c> under a filter and without one, and a server whose engine cannot collect stays
    /// <c>not_collected</c>.
    /// </summary>
    [Fact]
    public Task TableIndexSizes_EmptyPaths_SayWhichKindOfNothing() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-tables-empty", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var none = await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, Of("Nope1", "Nope2"), ct);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(none));
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, DatabaseFilter.All, ct)));

            await InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, "TA", 300m);

            var one = await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, DatabaseFilter.One("Nope1"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(one));
            Assert.Contains("database 'Nope1'", Message(one), StringComparison.Ordinal);
            Assert.DoesNotContain("chosen databases", Message(one), StringComparison.Ordinal);

            var many = await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, Of("Nope1", "Nope2"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(many));
            Assert.Contains("the chosen databases", Message(many), StringComparison.Ordinal);
            Assert.DoesNotContain("Nope1", Message(many), StringComparison.Ordinal);

            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", serverId, MonitoredEngineKind.Postgres);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM index_object_stats WHERE server_id = $1", serverId);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(
                await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, Of("Nope1", "Nope2"), ct)));
        }, Cleanup);

    /* ───────────────────────────── get_database_sizes ───────────────────────────── */

    /// <summary>
    /// Two of three databases at the server's newest snapshot: only the chosen databases' files, the same
    /// <c>captured_at</c> as the unfiltered call. A database that exists only in an OLDER snapshot is not found (the snapshot
    /// never moves with the filter), and that is an <c>empty</c> answer for the database, not <c>unavailable</c>.
    /// </summary>
    [Fact]
    public Task DatabaseSizes_TwoOfThree_OnlyTheChosenDatabases_SnapshotStaysTheServers() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-sizes", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await InsertFileAsync(connection, ct, serverId, serverName, now.AddHours(-5), "OnlyOlder", 77m);
            await InsertFileAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, 100m);
            await InsertFileAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, 200m);
            await InsertFileAsync(connection, ct, serverId, serverName, now.AddHours(-1), C, 300m);

            var all = Parse(await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, DatabaseFilter.All, ct));
            Assert.Equal(new[] { A, B, C }, Names(all, "databases"));

            var chosen = Parse(await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, Of(A, B), ct));
            Assert.Equal(new[] { A, B }, Names(chosen, "databases"));
            Assert.Equal(2, chosen.GetProperty("file_count").GetInt32());
            Assert.Equal(all.GetProperty("captured_at").GetString(), chosen.GetProperty("captured_at").GetString());

            /* The exact spelling: a differently cased name is not the database (the SQL arm compares with =). */
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(
                await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, DatabaseFilter.One(A.ToUpperInvariant()), ct)));

            Assert.Equal(all.GetRawText(), Parse(await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, cancellationToken: ct)).GetRawText());
            Assert.Equal(all.GetRawText(), Parse(await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, DatabaseFilter.One(""), ct)).GetRawText());

            var older = await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, DatabaseFilter.One("OnlyOlder"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(older));
            Assert.Contains("database 'OnlyOlder'", Message(older), StringComparison.Ordinal);

            var olderMany = await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, Of("OnlyOlder", "Nope"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(olderMany));
            Assert.Contains("the chosen databases", Message(olderMany), StringComparison.Ordinal);
        }, Cleanup);

    /// <summary>The empty path with no snapshot at all stays <c>unavailable</c> (filtered or not), and a server whose engine
    /// cannot collect stays <c>not_collected</c>.</summary>
    [Fact]
    public Task DatabaseSizes_NoSnapshot_StaysUnavailableOrNotCollected() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-sizes-empty", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(
                await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, Of("Nope1", "Nope2"), ct)));
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(
                await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, DatabaseFilter.All, ct)));

            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", serverId, MonitoredEngineKind.Postgres);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(
                await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, DatabaseFilter.One("Nope1"), ct)));
        }, Cleanup);

    /* ───────────────────────────── get_pvs_stats ───────────────────────────── */

    /// <summary>
    /// Two of three databases at the server's newest snapshot: only the chosen databases' rows, the same <c>as_of</c> as the
    /// unfiltered call, and a trend over the chosen databases only.
    /// </summary>
    [Fact]
    public Task PvsStats_TwoOfThree_OnlyTheChosenDatabases_AsOfStaysTheServers() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-pvs", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, 50m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, 10m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), C, 900m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-2), A, 40m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-2), B, 8m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-2), C, 800m);

            var all = Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 24, DatabaseFilter.All, ct));
            Assert.Equal(new[] { C, A, B }, Names(all, "databases"));
            Assert.Equal(new[] { A, B, C }, Names(all, "trend"));

            var chosen = Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 24, Of(A, B), ct));
            Assert.Equal(new[] { A, B }, Names(chosen, "databases"));
            Assert.Equal(new[] { A, B }, Names(chosen, "trend"));
            Assert.Equal(all.GetProperty("as_of").GetString(), chosen.GetProperty("as_of").GetString());

            var one = Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, DatabaseFilter.One(B), ct));
            Assert.Equal(new[] { B }, Names(one, "databases"));

            Assert.Equal(all.GetRawText(), Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 24, cancellationToken: ct)).GetRawText());
            Assert.Equal(all.GetRawText(), Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 24, DatabaseFilter.One(" "), ct)).GetRawText());
        }, Cleanup);

    /// <summary>
    /// The trend's top five is the top five of the CHOSEN databases: seven big databases outrank A and B, so an unfiltered
    /// trend holds five of the big ones and none of A or B, while a trend filtered to A and B holds both.
    /// </summary>
    [Fact]
    public Task PvsStats_TrendTopFiveAppliesAfterTheFilter() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-pvs-top5", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            for (var i = 1; i <= 7; i++)
            {
                await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), "Big" + i, 100m * i);
                await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-2), "Big" + i, 90m * i);
            }

            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, 5m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, 4m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-2), A, 3m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-2), B, 2m);

            var all = Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 24, DatabaseFilter.All, ct));
            Assert.Equal(5, Names(all, "trend").Length);
            Assert.DoesNotContain(A, Names(all, "trend"));

            var chosen = Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 24, Of(A, B), ct));
            Assert.Equal(new[] { A, B }, Names(chosen, "trend"));
            Assert.All(chosen.GetProperty("trend").EnumerateArray(), t => Assert.Equal(2, t.GetProperty("points").GetArrayLength()));

            /* The reader-level calls the tool makes: the same sets. */
            var rows = await DarlingPvsReader.GetPvsStatsLatestAsync(postgres, serverId, Of(A, B), ct);
            Assert.Equal(new[] { A, B }, rows.Select(r => r.DatabaseName).ToArray());
            var points = await DarlingPvsReader.GetPvsTrendAsync(postgres, serverId, now.AddHours(-24), Of(A, B), ct);
            Assert.Equal(new[] { A, B }, points.Select(p => p.DatabaseName).Distinct().ToArray());
        }, Cleanup);

    /// <summary>
    /// The empty path. A filter that matches no database, on a server with a PVS snapshot, is <c>empty</c> for the database
    /// (one name) or the chosen databases (two or more) and does not say the server has no PVS data. A server with no PVS rows
    /// keeps its own sentence under a filter and without one, and a server whose engine cannot collect stays
    /// <c>not_collected</c>.
    /// </summary>
    [Fact]
    public Task PvsStats_EmptyPaths_SayWhichKindOfNothing() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5r1-pvs-empty", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var noData = await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, Of("Nope1", "Nope2"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(noData));
            Assert.Contains("No PVS data collected for this server", Message(noData), StringComparison.Ordinal);
            Assert.Equal(Message(noData), Message(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, DatabaseFilter.All, ct)));

            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, 50m);
            await InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), B, 10m);

            var one = await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, DatabaseFilter.One("Nope1"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(one));
            Assert.Contains("database 'Nope1'", Message(one), StringComparison.Ordinal);
            Assert.DoesNotContain("No PVS data collected for this server", Message(one), StringComparison.Ordinal);

            var many = await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, Of("Nope1", "Nope2"), ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(many));
            Assert.Contains("the chosen databases", Message(many), StringComparison.Ordinal);
            Assert.Contains("2 other database(s)", Message(many), StringComparison.Ordinal);

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pvs_stats WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", serverId, MonitoredEngineKind.Postgres);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(
                await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, DatabaseFilter.One("Nope1"), ct)));
        }, Cleanup);

    /* ───────────────────────────── the statements ───────────────────────────── */

    /// <summary>
    /// The list predicate is the desktop's, bound as ONE text[] at the index each statement documents: $5 on the four snapshot
    /// CTEs of the table-size read (and not on <c>boundaries</c>, which carries the snapshot anchors and the store's span), $3
    /// on the database-size read, $2 on the PVS snapshot read, $3 on the PVS trend's top-five CTE.
    /// </summary>
    [Fact]
    public void Statements_CarryTheListPredicate_OnTheRightRelations()
    {
        const string Predicate = "{0}::text[] IS NULL OR database_name = ANY({0})";

        var growth = DarlingObjectStatsReader.ObjectSizeGrowthSql;
        var predicate5 = string.Format(System.Globalization.CultureInfo.InvariantCulture, Predicate, "$5");
        Assert.Equal(4, growth.Split(predicate5).Length - 1);
        var boundaries = growth[growth.IndexOf("boundaries AS (", StringComparison.Ordinal)..growth.IndexOf("latest AS (", StringComparison.Ordinal)];
        Assert.DoesNotContain("$5", boundaries, StringComparison.Ordinal);

        Assert.Contains(string.Format(System.Globalization.CultureInfo.InvariantCulture, Predicate, "$3"), DarlingObjectStatsReader.DatabaseSizeLatestSql, StringComparison.Ordinal);
        Assert.Contains(string.Format(System.Globalization.CultureInfo.InvariantCulture, Predicate, "$2"), DarlingPvsReader.PvsStatsLatestSql, StringComparison.Ordinal);

        var trend = DarlingPvsReader.PvsTrendSql;
        var topDbs = trend[..trend.IndexOf("JOIN top_dbs", StringComparison.Ordinal)];
        Assert.Contains(string.Format(System.Globalization.CultureInfo.InvariantCulture, Predicate, "$3"), topDbs, StringComparison.Ordinal);
        Assert.Contains("LIMIT 5", topDbs, StringComparison.Ordinal);

        /* Byte-for-byte the predicate DatabaseFilter.Clause spells. */
        Assert.Contains(DatabaseFilter.All.Clause("database_name", 5).TrimStart().Substring("AND ".Length), growth, StringComparison.Ordinal);
    }
}
