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
using System.Text.Json;
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
/// #5373: a severe error is named by the database its id carried AT THE ERROR'S TIME. SQL Server reuses the id of a
/// dropped database, so the server's latest id-to-name map named the wrong database for an old error. Pure cases for
/// the shared <see cref="DatabaseNameHistory"/> rule; the live class below runs it through the store.
/// </summary>
public sealed class DatabaseNameHistoryTests
{
    private static readonly DateTime T1 = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T2 = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    private static DatabaseNameHistory IdSevenIsAThenB() => new(new[]
    {
        new DatabaseNameHistory.Change(7, "A", T1),
        new DatabaseNameHistory.Change(7, "B", T2),
        new DatabaseNameHistory.Change(1, "master", T1),
    });

    [Fact]
    public void ReusedId_ErrorBeforeTheDrop_ShowsTheOldName_AfterShowsTheNewOne()
    {
        var history = IdSevenIsAThenB();
        Assert.Equal("A", history.Resolve(7, T1.AddDays(2)));
        Assert.Equal("B", history.Resolve(7, T2.AddDays(2)));
        Assert.Equal("master", history.Resolve(1, T2.AddDays(2)));
    }

    [Fact]
    public void ASnapshotAtTheSameInstantCountsAsBefore()
    {
        var history = IdSevenIsAThenB();
        Assert.Equal("A", history.Resolve(7, T1));
        Assert.Equal("B", history.Resolve(7, T2));
        Assert.Equal("A", history.Resolve(7, T2.AddTicks(-1)));
    }

    [Fact]
    public void NoSnapshotBeforeTheError_UsesTheOldestSnapshotAfterIt()
    {
        var history = IdSevenIsAThenB();
        Assert.Equal("A", history.Resolve(7, T1.AddDays(-30)));
    }

    [Fact]
    public void AnIdNeverSeen_SurfacesTheRawId_AndNoContextIsBlank()
    {
        var history = IdSevenIsAThenB();
        Assert.Equal("database_id 99", history.Resolve(99, T1));
        Assert.Equal("", history.Resolve(0, T1));
        Assert.Equal("", history.Resolve(null, T1));
        Assert.Equal("database_id 7", DatabaseNameHistory.Empty.Resolve(7, T1));
    }

    [Fact]
    public void AnErrorWithNoTime_UsesTheNewestKnownName()
    {
        Assert.Equal("B", IdSevenIsAThenB().Resolve(7, null));
    }

    [Fact]
    public void TimesCompareInUtc_ARepeatedNameAndRowOrderChangeNothing()
    {
        /* The same instant spelled in local time resolves the same; duplicate rows and any row order do too. */
        var history = new DatabaseNameHistory(new[]
        {
            new DatabaseNameHistory.Change(7, "B", T2),
            new DatabaseNameHistory.Change(7, "A", T1),
            new DatabaseNameHistory.Change(7, "A", T1.AddDays(1)),
        });
        Assert.Equal("A", history.Resolve(7, T2.AddSeconds(-1).ToLocalTime()));
        Assert.Equal("B", history.Resolve(7, T2.ToLocalTime()));
        Assert.Equal("B", history.Resolve(7, DateTime.SpecifyKind(T2, DateTimeKind.Unspecified)));
    }

    [Fact]
    public void RangeOf_IsTheEarliestAndLatestTime_OrNullWithNone()
    {
        Assert.Equal((T1, T2), DatabaseNameHistory.RangeOf(new DateTime?[] { T2, null, T1 }));
        Assert.Null(DatabaseNameHistory.RangeOf(new DateTime?[] { null }));
    }

    [Fact]
    public void TwoNamesForOneIdAtOneInstant_ResolveTheSameWhicheverOrderTheyArriveIn_5373()
    {
        /* The same id, the same collection_time, two names (a rename caught between two files of one collection): the
           result must not depend on row order, so the later name in ordinal order wins. */
        var x = new DatabaseNameHistory.Change(7, "X", T1);
        var y = new DatabaseNameHistory.Change(7, "Y", T1);
        Assert.Equal("Y", new DatabaseNameHistory(new[] { x, y }).Resolve(7, T2));
        Assert.Equal("Y", new DatabaseNameHistory(new[] { y, x }).Resolve(7, T2));
        Assert.Equal("Y", new DatabaseNameHistory(new[] { y, x }).Resolve(7, null));
        Assert.Equal("Y", new DatabaseNameHistory(new[] { x, y }).Resolve(7, null));
    }

    [Fact]
    public void Plan_NamesTheRealIds_TheirTimeRange_AndWhetherAnErrorHasNoTime_5373()
    {
        var plan = DatabaseNameHistory.Plan(new (int?, DateTime?)[] { (7, T2), (7, null), (0, null), (null, null), (1, T1) });
        Assert.Equal(new[] { 1, 7 }, plan.Ids.OrderBy(i => i).ToArray());
        Assert.Equal((T1, T2), plan.Range);
        Assert.True(plan.NeedsNewest);

        /* An error with no database context needs nothing, even with no time. */
        var none = DatabaseNameHistory.Plan(new (int?, DateTime?)[] { (0, null), (null, T1) });
        Assert.Empty(none.Ids);
        Assert.Null(none.Range);
        Assert.False(none.NeedsNewest);

        var allUntimed = DatabaseNameHistory.Plan(new (int?, DateTime?)[] { (7, null) });
        Assert.Null(allUntimed.Range);
        Assert.True(allUntimed.NeedsNewest);
    }

    [Fact]
    public void TheHistoryQuery_IsOneQueryOnTheServerTimeIndex_WithPositionalParameters()
    {
        var sql = DatabaseNameHistoryReader.Sql;
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("database_id = ANY($4)", sql, StringComparison.Ordinal);
        /* #5373: each of the two extra arms is ONE pass over all its ids (DISTINCT ON), never a per-id LATERAL probe, and
           a tie on one instant breaks by name. */
        Assert.Contains("before_floor AS", sql, StringComparison.Ordinal);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "SELECT DISTINCT ON \\(d\\.database_id\\)").Count);
        Assert.DoesNotContain("LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time, database_name", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT ON (database_id)", DatabaseNameHistoryReader.NewestSql, StringComparison.Ordinal);
        Assert.DoesNotContain("LATERAL", DatabaseNameHistoryReader.NewestSql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
    }
}

/// <summary>
/// #5373 live: the history read, the Darling MCP tool and the viewer's Severe Errors read, against a test PostgreSQL.
/// Id 7 is database A until T1, A is dropped, and id 7 is database B from T2.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseNameHistoryLivePostgresTests
{
    private const string ServerName = "darling-dbname-history-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string ErrorXml(DateTime at, int databaseId, int errorNumber) =>
        $"<event name=\"error_reported\" package=\"sqlserver\" timestamp=\"{at:yyyy-MM-ddTHH:mm:ss.fffZ}\">" +
        $"<data name=\"error_number\"><value>{errorNumber}</value></data>" +
        "<data name=\"severity\"><value>20</value></data>" +
        "<data name=\"state\"><value>1</value></data>" +
        "<data name=\"message\"><value>history probe</value></data>" +
        $"<action name=\"database_id\"><value>{databaseId}</value></action>" +
        "</event>";

    [Fact]
    public async Task SevereErrors_AreNamedByTheDatabaseTheirIdWasAtTheErrorsTime()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database-name-history test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            async Task Snapshot(double hoursAgo, int id, string name) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id)
VALUES ($1,$2,$3,$4,$5,$6)", CollectionIdGenerator.Next(), now.AddHours(-hoursAgo), ServerId, ServerName, name, id);

            async Task Error(double hoursAgo, int id, int number)
            {
                var at = now.AddHours(-hoursAgo);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, at, SystemHealthParser.ErrorReportedEvent, ErrorXml(at, id, number));
            }

            /* id 7 is A (snapshots at 20h, 16h, 12h ago, two files each), is dropped, and is B from 6h ago. */
            foreach (var h in new[] { 20.0, 16.0, 12.0 }) { await Snapshot(h, 7, "A"); await Snapshot(h, 7, "A"); await Snapshot(h, 1, "master"); }
            foreach (var h in new[] { 6.0, 2.0 }) { await Snapshot(h, 7, "B"); await Snapshot(h, 1, "master"); }
            /* id 8 appears only AFTER the whole error range, as C (rule 2 past the range). */
            await Snapshot(0.5, 8, "C");

            await Error(22, 7, 50001);   /* before any snapshot: the oldest one after it, A */
            await Error(14, 7, 50002);   /* A */
            await Error(4, 7, 50003);    /* B */
            await Error(1, 7, 50004);    /* B, after the last snapshot */
            await Error(10, 8, 50005);   /* id 8 seen only after the range: C */
            await Error(3, 99, 50006);   /* never seen */

            var json = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, hours_back: 48, limit: 50)).RootElement;
            var names = json.GetProperty("errors").EnumerateArray()
                .ToDictionary(e => e.GetProperty("error_number").GetInt32(), e => e.GetProperty("database_name").GetString()!);
            Assert.Equal("A", names[50001]);
            Assert.Equal("A", names[50002]);
            Assert.Equal("B", names[50003]);
            Assert.Equal("B", names[50004]);
            Assert.Equal("C", names[50005]);
            Assert.Equal("database_id 99", names[50006]);

            /* The desktop viewer's Severe Errors read says the same, and its database filter matches the time-correct name. */
            await using var viewer = new ViewerDataService(cs!);
            var rows = await viewer.GetSevereErrorsAsync(ServerId, now.AddHours(-48), now, cancellationToken: ct);
            var viewerNames = rows.ToDictionary(r => r.ErrorNumber ?? 0, r => r.DatabaseName);
            Assert.Equal(names, viewerNames);
            var onlyA = await viewer.GetSevereErrorsAsync(ServerId, now.AddHours(-48), now, new[] { "A" }, ct);
            Assert.Equal(new[] { 50001, 50002 }, onlyA.Select(r => r.ErrorNumber ?? 0).OrderBy(n => n).ToArray());
            var onlyB = await viewer.GetSevereErrorsAsync(ServerId, now.AddHours(-48), now, new[] { "B" }, ct);
            Assert.Equal(new[] { 50003, 50004 }, onlyB.Select(r => r.ErrorNumber ?? 0).OrderBy(n => n).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task SnapshotAsync(NpgsqlConnection connection, DateTime at, int id, string name, System.Threading.CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id)
VALUES ($1,$2,$3,$4,$5,$6)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, id);

    private static async Task ErrorAtAsync(NpgsqlConnection connection, DateTime at, int id, int number, bool xmlHasTime, System.Threading.CancellationToken ct)
    {
        var xml = ErrorXml(at, id, number);
        if (!xmlHasTime)
            xml = System.Text.RegularExpressions.Regex.Replace(xml, " timestamp=\"[^\"]*\"", "", System.Text.RegularExpressions.RegexOptions.None, TimeSpan.FromSeconds(5));
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, at, SystemHealthParser.ErrorReportedEvent, xml);
    }

    /// <summary>Seeds a fresh store with <paramref name="seed"/> and returns the names the MCP tool gave, keyed by error
    /// number, after checking the viewer's Severe Errors read gives the same.</summary>
    private static async Task<Dictionary<int, string>> NamesAsync(
        string cs, Func<NpgsqlConnection, DateTime, System.Threading.CancellationToken, Task> seed)
    {
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await seed(connection, now, ct);

            var json = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, hours_back: 168, limit: 50)).RootElement;
            Assert.True(json.TryGetProperty("errors", out var errors), json.ToString());
            var names = errors.EnumerateArray()
                .ToDictionary(e => e.GetProperty("error_number").GetInt32(), e => e.GetProperty("database_name").GetString()!);

            await using var viewer = new ViewerDataService(cs);
            var rows = await viewer.GetSevereErrorsAsync(ServerId, now.AddHours(-200), now, cancellationToken: ct);
            Assert.Equal(names, rows.ToDictionary(r => r.ErrorNumber ?? 0, r => r.DatabaseName));
            bodySucceeded = true;
            return names;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AnIdAbsentFromTheFloorSnapshot_KeepsItsLastEarlierName_5373()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database-name-history test.");

        /* The floor snapshot is server-wide. Database 5 went offline (offline, suspect and excluded databases are not
           collected): its last snapshot is before the whole error range, so it is in no snapshot of the range at all. */
        var names = await NamesAsync(cs!, async (c, now, ct) =>
        {
            foreach (var h in new[] { 40.0, 30.0, 20.0, 10.0, 2.0 }) await SnapshotAsync(c, now.AddHours(-h), 1, "master", ct);
            foreach (var h in new[] { 40.0, 30.0 }) await SnapshotAsync(c, now.AddHours(-h), 5, "Offline1", ct);
            await ErrorAtAsync(c, now.AddHours(-10), 5, 50101, true, ct);   /* the last name it had: Offline1, not "database_id 5" */
            await ErrorAtAsync(c, now.AddHours(-3), 5, 50102, true, ct);
            await ErrorAtAsync(c, now.AddHours(-3), 1, 50103, true, ct);
        });
        Assert.Equal("Offline1", names[50101]);
        Assert.Equal("Offline1", names[50102]);
        Assert.Equal("master", names[50103]);
    }

    [Fact]
    public async Task AReusedIdWhoseFirstOwnerWentOfflineBeforeTheRange_StillNamesBothOwners_5373()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database-name-history test.");

        /* A has id 7 until 5 days ago, then goes offline (no more rows) and is dropped. B gets id 7 10 hours ago. */
        var names = await NamesAsync(cs!, async (c, now, ct) =>
        {
            foreach (var h in new[] { 130.0, 120.0, 100.0, 50.0, 30.0, 10.0, 4.0 }) await SnapshotAsync(c, now.AddHours(-h), 1, "master", ct);
            foreach (var h in new[] { 130.0, 125.0, 120.0 }) await SnapshotAsync(c, now.AddHours(-h), 7, "A", ct);
            foreach (var h in new[] { 10.0, 4.0 }) await SnapshotAsync(c, now.AddHours(-h), 7, "B", ct);
            await ErrorAtAsync(c, now.AddHours(-26), 7, 50111, true, ct);   /* day -1: A */
            await ErrorAtAsync(c, now.AddHours(-5), 7, 50112, true, ct);    /* hour -5: B */
        });
        Assert.Equal("A", names[50111]);
        Assert.Equal("B", names[50112]);
    }

    [Fact]
    public async Task AnErrorWithNoTime_ShowsTheNewestNameOfItsId_NotTheRawId_5373()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database-name-history test.");

        var names = await NamesAsync(cs!, async (c, now, ct) =>
        {
            foreach (var h in new[] { 20.0, 16.0 }) await SnapshotAsync(c, now.AddHours(-h), 7, "A", ct);
            foreach (var h in new[] { 12.0, 2.0 }) await SnapshotAsync(c, now.AddHours(-h), 7, "B", ct);
            await ErrorAtAsync(c, now.AddHours(-14), 7, 50121, true, ct);    /* timed: A */
            await ErrorAtAsync(c, now.AddHours(-3), 7, 50122, false, ct);    /* no time in its XML: the newest name, B */
        });
        Assert.Equal("A", names[50121]);
        Assert.Equal("B", names[50122]);

        /* Every shown error has no time: there is no time range at all, and the newest name is still found. */
        var onlyUntimed = await NamesAsync(cs!, async (c, now, ct) =>
        {
            await SnapshotAsync(c, now.AddHours(-20), 7, "A", ct);
            await SnapshotAsync(c, now.AddHours(-2), 7, "B", ct);
            await ErrorAtAsync(c, now.AddHours(-3), 7, 50123, false, ct);
        });
        Assert.Equal("B", onlyUntimed[50123]);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM system_health_events WHERE server_id = {ServerId}; DELETE FROM database_size_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
