using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348 (R4b): a deadlock graph, blocked process report or system_health event the statement filter withheld WHOLE
/// stores only the marker, so its identity is the event time plus the non-text columns the collector parsed from the
/// raw XML before judging it. The same rule drops re-read copies and keeps different events apart; a row with real
/// text keeps its identity unchanged.
/// </summary>
public sealed class StatementScrubWholeMarkerIdentityTests : IDisposable
{
    private const int Server = -445302;
    private static readonly DateTime EventTime = new(2026, 3, 10, 9, 30, 0);
    private const string Marker = SensitiveStatements.PlaceholderText;
    private const string Plain = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    public StatementScrubWholeMarkerIdentityTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private async Task<DuckDBConnection> OpenAsync()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");
        using (var initializer = new DuckDbInitializer(path))
        {
            await initializer.InitializeAsync();
        }

        var connection = new DuckDBConnection($"Data Source={path}");
        await connection.OpenAsync();
        return connection;
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<long>> IdsAsync(DuckDBConnection connection, string sql)
    {
        var ids = new List<long>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(Convert.ToInt64(reader.GetValue(0)));
        return ids;
    }

    private static string Ts(DateTime value) => $"TIMESTAMP '{value:yyyy-MM-dd HH:mm:ss}'";

    private static string Str(string? value) => value is null ? "NULL" : $"'{value.Replace("'", "''")}'";

    private static string Num(int? value) => value is null ? "NULL" : value.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static Task InsertDeadlockAsync(DuckDBConnection connection, long id, int collectedMinutes, DateTime deadlockTime,
        string? victim, string? database, string graph) =>
        ExecuteAsync(connection,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, "
            + $"database_name, deadlock_graph_xml) VALUES ({id}, {Ts(EventTime.AddMinutes(collectedMinutes))}, {Server}, 'S', "
            + $"{Ts(deadlockTime)}, {Str(victim)}, {Str(database)}, {Str(graph)})");

    private static string ShownDeadlocks() =>
        "SELECT deadlock_id FROM " + StoredEventCopies.Deadlocks("server_id = " + Server) + " AS d ORDER BY 1";

    /* Three re-reads of one whole-marker deadlock (same time, victim and database) are copies: the cleanup removes the
       two later ones and the stored-copy read shows one. A different victim at the same time, the same victim at a
       different time, and a whole-marker row that carries no victim id are all different deadlocks: all stay. */
    [Fact]
    public async Task WholeMarkerDeadlocks_ReReadsCollapse_ButDifferentVictimsAndTimesStay()
    {
        using var connection = await OpenAsync();

        await InsertDeadlockAsync(connection, 1, 1, EventTime, "process1", "db1", Marker);
        await InsertDeadlockAsync(connection, 2, 2, EventTime, "process1", "db1", Marker);
        await InsertDeadlockAsync(connection, 3, 3, EventTime, "process1", "db1", Marker);
        await InsertDeadlockAsync(connection, 4, 4, EventTime, "process9", "db1", Marker);
        await InsertDeadlockAsync(connection, 5, 5, EventTime.AddSeconds(30), "process1", "db1", Marker);
        await InsertDeadlockAsync(connection, 6, 6, EventTime, null, "db1", Marker);
        await InsertDeadlockAsync(connection, 7, 7, EventTime, null, "db1", Marker);

        Assert.Equal(new List<long> { 1, 4, 5, 6, 7 }, await IdsAsync(connection, ShownDeadlocks()));

        var count = await IdsAsync(connection,
            "SELECT " + StoredEventCopies.DeadlockDistinctCount + " FROM v_deadlocks AS dl WHERE server_id = " + Server);
        Assert.Equal(new List<long> { 5 }, count);

        var removed = await DeadlockDuplicateCleanup.RemoveAsync(connection, logger: null);

        Assert.Equal(2, removed);
        Assert.Equal(new List<long> { 1, 4, 5, 6, 7 }, await IdsAsync(connection, "SELECT deadlock_id FROM deadlocks ORDER BY 1"));
    }

    /* A row with real graph text keeps the identity it had: the victim and database play no part in it. */
    [Fact]
    public async Task ARealTextDeadlock_KeepsItsIdentity_VictimAndDatabaseAreNotPartOfIt()
    {
        using var connection = await OpenAsync();

        await InsertDeadlockAsync(connection, 1, 1, EventTime, "process1", "db1", Plain);
        await InsertDeadlockAsync(connection, 2, 2, EventTime, "process9", "db2", Plain);

        Assert.Equal(new List<long> { 1 }, await IdsAsync(connection, ShownDeadlocks()));
        Assert.Equal(1, await DeadlockDuplicateCleanup.RemoveAsync(connection, logger: null));
    }

    private static Task InsertReportAsync(DuckDBConnection connection, long id, int collectedMinutes, int? blocked, int? blocking, string xml) =>
        ExecuteAsync(connection,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, "
            + "database_name, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_resource, blocked_process_report_xml) "
            + $"VALUES ({id}, {Ts(EventTime.AddMinutes(collectedMinutes))}, {Server}, 'S', {Ts(EventTime)}, 'db1', "
            + $"{Num(blocked)}, 0, {Num(blocking)}, 0, 'KEY: 5:1', {Str(xml)})");

    private static string Reports() =>
        "SELECT blocked_report_id FROM " + StoredEventCopies.BlockedProcessReports("server_id = " + Server) + " AS r ORDER BY 1";

    /* Several blocked sessions often report at one monitor tick. Whole-marker reports at one event time that name
       different blocked sessions are different reports; a re-read of the same one is a copy. */
    [Fact]
    public async Task WholeMarkerBlockedProcessReports_DifferentSessionsAtOneTime_StayApart_AndAReReadCollapses()
    {
        using var connection = await OpenAsync();

        await InsertReportAsync(connection, 1, 1, 51, 60, Marker);
        await InsertReportAsync(connection, 2, 2, 52, 60, Marker);
        await InsertReportAsync(connection, 3, 3, 51, 60, Marker);
        await InsertReportAsync(connection, 4, 1, null, null, Marker);
        await InsertReportAsync(connection, 5, 2, null, null, Marker);

        Assert.Equal(new List<long> { 1, 2, 4, 5 }, await IdsAsync(connection, Reports()));
    }

    /* Real report text keeps its identity: the sessions are not part of it. */
    [Fact]
    public async Task ARealTextBlockedProcessReport_KeepsItsIdentity()
    {
        using var connection = await OpenAsync();

        await InsertReportAsync(connection, 1, 1, 51, 60, "<blocked-process-report a=\"1\"/>");
        await InsertReportAsync(connection, 2, 2, 52, 61, "<blocked-process-report a=\"1\"/>");
        await InsertReportAsync(connection, 3, 2, 51, 60, "<blocked-process-report a=\"2\"/>");

        Assert.Equal(new List<long> { 1, 3 }, await IdsAsync(connection, Reports()));
    }

    private static Task InsertHealthAsync(DuckDBConnection connection, long id, int collectedMinutes, string? type, string xml) =>
        ExecuteAsync(connection,
            "INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, "
            + $"event_type, event_xml) VALUES ({id}, {Ts(EventTime.AddMinutes(collectedMinutes))}, {Server}, 'S', {Ts(EventTime)}, "
            + $"{Str(type)}, {Str(xml)})");

    [Fact]
    public async Task WholeMarkerSystemHealthEvents_DifferentTypesAtOneTime_StayApart_AndAReReadCollapses()
    {
        using var connection = await OpenAsync();

        await InsertHealthAsync(connection, 1, 1, "error_reported", Marker);
        await InsertHealthAsync(connection, 2, 2, "xml_deadlock_report", Marker);
        await InsertHealthAsync(connection, 3, 3, "error_reported", Marker);
        await InsertHealthAsync(connection, 4, 1, null, Marker);
        await InsertHealthAsync(connection, 5, 2, null, Marker);
        await InsertHealthAsync(connection, 6, 1, "error_reported", "<event a=\"1\"/>");
        await InsertHealthAsync(connection, 7, 2, "xml_deadlock_report", "<event a=\"1\"/>");

        Assert.Equal(new List<long> { 1, 2, 4, 5, 6 }, await IdsAsync(connection,
            "SELECT system_health_event_id FROM " + StoredEventCopies.SystemHealthEvents("server_id = " + Server) + " AS r ORDER BY 1"));
    }
}
