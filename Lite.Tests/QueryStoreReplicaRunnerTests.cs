/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6254 and #6279 through Lite's real definition runner (<c>RunCollectorAsync</c>, the enumerated arm), with the primary
/// and the replica supplied as reader seams that apply the cutoff the way the SQL does (<c>last_execution_time &gt;
/// @cutoff_time</c>): a replica read starts at W - L and the ship guard keeps the re-read rows from being stored twice,
/// a late row below the watermark is stored, an empty setting opens nothing, and a database a capturing secondary feeds
/// gets the same look-back with no replica involved.
/// </summary>
public sealed class QueryStoreReplicaRunnerTests : IDisposable
{
    private const string Sales = "Sales";
    private static readonly Guid GroupDatabaseId = Guid.Parse("7a88cd1f-6fcf-444c-a320-feb92dfbd0d2");

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _configDir;

    public QueryStoreReplicaRunnerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task AReplicaRead_StartsAtWatermarkMinusLookBack_StoresTheLateRow_AndNotTheReReadOne()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        var a = Row(queryId: 1, lastExecution: now.AddMinutes(-20));
        var b = Row(queryId: 2, lastExecution: now.AddMinutes(-28));
        f.ReplicaRows = [a];

        await RunAsync(f);
        Assert.Equal(1, await CountAsync(f));

        /* Run 2: the replica now shows A again and B, a row that became visible late with an older execution time (below
           the watermark A set). A cutoff of W would never ask for B; W - L (the flush interval plus 5 minutes) does. */
        f.ReplicaRows = [a, b];
        await RunAsync(f);

        Assert.Equal(2, await CountAsync(f));
        Assert.Equal(1, await CountAsync(f, queryId: 1));
        Assert.Equal(1, await CountAsync(f, queryId: 2));
        var cutoffs = f.ReplicaItemCutoffs;
        Assert.Equal(2, cutoffs.Count);
        Assert.Equal(a.LastExecution - TimeSpan.FromMinutes(20), cutoffs[1]);
        Assert.Empty(f.PrimaryItemCutoffs);
        Assert.Equal(2, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.StartsWith("replica 1", await ReadSourceAsync(f), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASettingThatIsEmpty_OpensNoReplicaConnection_ReadsThePrimaryFromTheWatermark_AndWritesNoReadSource()
    {
        var f = await BuildAsync(replica: null);
        var now = Truncate(DateTime.UtcNow);
        var a = Row(queryId: 1, lastExecution: now.AddMinutes(-20));
        f.PrimaryRows = [a];

        await RunAsync(f);
        await RunAsync(f);

        Assert.Equal(0, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.Equal(0, f.ReplicaCalls);
        Assert.Equal(1, await CountAsync(f));
        Assert.Equal(a.LastExecution, f.PrimaryItemCutoffs[1]);
        Assert.Null(await ReadSourceAsync(f));
    }

    [Fact]
    public async Task ACapturingSecondary_OnAServerWithNoReplicaSet_GetsTheLookBack_AndOpensNoReplicaConnection()
    {
        var f = await BuildAsync(replica: null);
        var now = Truncate(DateTime.UtcNow);
        var a = Row(queryId: 1, lastExecution: now.AddMinutes(-20), role: "Secondary");
        var late = Row(queryId: 2, lastExecution: now.AddMinutes(-40), role: "Secondary");
        f.PrimaryRows = [a];
        await RunAsync(f);

        /* The first read showed a row of another replica group: the database is now known to have a capturing secondary,
           so the next read starts at W - (flush interval + 10 minutes) and finds the late row. */
        f.PrimaryRows = [a, late];
        await RunAsync(f);

        Assert.Equal(0, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.Equal(a.LastExecution - TimeSpan.FromMinutes(25), f.PrimaryItemCutoffs[1]);
        Assert.Equal(1, await CountAsync(f, queryId: 2));

        /* The first look-back read writes the window once more (the guard learned nothing before it); from then on the guard
           keeps every re-read row out, so a third run with the same rows adds nothing. */
        var storedAfterSecondRun = await CountAsync(f);
        await RunAsync(f);
        Assert.Equal(storedAfterSecondRun, await CountAsync(f));
        Assert.Equal(1, await CountAsync(f, queryId: 2));
        Assert.Null(await ReadSourceAsync(f));
    }

    [Fact]
    public async Task AReplicaThatCannotBeEnumerated_SendsEveryDatabaseToThePrimary_AndSaysSo()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        f.PrimaryRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-20))];
        f.ReplicaFails = true;

        await RunAsync(f);

        Assert.Equal(1, await CountAsync(f));
        Assert.Empty(f.ReplicaItemCutoffs);
        var source = await ReadSourceAsync(f);
        Assert.Contains("primary 1", source, StringComparison.Ordinal);
        Assert.Contains("replica_unavailable", source, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AzureSqlDatabase_IgnoresTheSetting_NoReplicaConnection_NoReadSource()
    {
        var f = await BuildAsync(replica: "replica-host", engineEdition: 5);
        f.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Sales });
        f.Service.AzureDatabaseReaderOverrideForTests = (_, _) => new DataTable().CreateDataReader();

        await RunAsync(f);

        Assert.Equal(0, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.Null(await ReadSourceAsync(f));
    }

    private sealed record QsRow(long QueryId, DateTime LastExecution, string Role);

    private static QsRow Row(long queryId, DateTime lastExecution, string role = "Primary") => new(queryId, lastExecution, role);

    private static DateTime Truncate(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

    private sealed class Fixture
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerConnection Server { get; init; }
        public required int ServerId { get; init; }
        public required DuckDbInitializer DuckDb { get; init; }
        public List<QsRow> PrimaryRows { get; set; } = [];
        public List<QsRow> ReplicaRows { get; set; } = [];
        public bool ReplicaFails { get; set; }
        public int ReplicaCalls;
        public List<DateTime?> PrimaryItemCutoffs { get; } = [];
        public List<DateTime?> ReplicaItemCutoffs { get; } = [];
    }

    private async Task<Fixture> BuildAsync(string? replica, int engineEdition = 3)
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { ServerName = "sql-test", DisplayName = "sql-test", QueryStoreReplica = replica };
        serverManager.AddServer(server);
        var status = serverManager.GetConnectionStatus(server.Id);
        status.SqlEngineEdition = engineEdition;
        status.SqlMajorVersion = 16;

        var service = new RemoteCollectorService(duckDb, serverManager, new ScheduleManager(_configDir));
        var fixture = new Fixture
        {
            Service = service,
            Server = server,
            ServerId = RemoteCollectorService.GetServerId(server),
            DuckDb = duckDb,
        };

        service.ServerReaderOverrideForTests = plan => Answer(fixture, plan, replica: false);
        service.ReplicaReaderOverrideForTests = plan => Answer(fixture, plan, replica: true);
        return fixture;
    }

    private static Task RunAsync(Fixture f) =>
        f.Service.RunCollectorAsync(f.Server, QueryStoreCollector.Instance.Name, CancellationToken.None);

    /// <summary>One side's reader for one statement. Item reads apply the cutoff as the SQL does, and note it.</summary>
    private static DbDataReader Answer(Fixture f, CollectorQuery plan, bool replica)
    {
        if (replica)
        {
            Interlocked.Increment(ref f.ReplicaCalls);
            if (f.ReplicaFails)
            {
                throw new InvalidOperationException("the replica is down");
            }
        }

        if (plan.Parameters?.Any(static p => p.Name == "@cutoff_time") == true)
        {
            var cutoff = plan.Parameters.First(static p => p.Name == "@cutoff_time").Value as DateTime?;
            (replica ? f.ReplicaItemCutoffs : f.PrimaryItemCutoffs).Add(cutoff);
            var rows = (replica ? f.ReplicaRows : f.PrimaryRows).Where(r => cutoff is null || r.LastExecution > cutoff).ToList();
            return ItemTable(rows).CreateDataReader();
        }

        if (plan.Text == QueryStoreCollector.ProductVersionProbeText)
        {
            var version = new DataTable();
            version.Columns.Add("v", typeof(int));
            version.Rows.Add(16);
            return version.CreateDataReader();
        }

        if (plan.Text == QueryStoreCollector.BuildRedoStateQuery().Text)
        {
            var redo = new DataTable();
            redo.Columns.Add("name", typeof(string));
            redo.Columns.Add("group_database_id", typeof(Guid));
            redo.Columns.Add("last_commit_time", typeof(DateTime));
            redo.Columns.Add("is_primary_replica", typeof(bool));
            redo.Rows.Add(Sales, GroupDatabaseId, DateTime.UtcNow, !replica);
            return redo.CreateDataReader();
        }

        /* An enumeration: the databases, then the (empty) probe failures, then, on the primary, the flush intervals. */
        var databases = new DataTable();
        var failures = new DataTable();
        failures.Columns.Add("item_name", typeof(string));
        failures.Columns.Add("error_text", typeof(string));
        if (replica)
        {
            databases.Columns.Add("name", typeof(string));
            databases.Columns.Add("actual_state", typeof(int));
            databases.Columns.Add("readonly_reason", typeof(int));
            databases.Columns.Add("flush", typeof(int));
            databases.Rows.Add(Sales, 0, 8, 900);
            return new DataTableReader([databases, failures]);
        }

        databases.Columns.Add("database_name", typeof(string));
        databases.Rows.Add(Sales);
        var facts = new DataTable();
        facts.Columns.Add("name", typeof(string));
        facts.Columns.Add("flush", typeof(int));
        facts.Rows.Add(Sales, 900);
        return new DataTableReader([databases, failures, facts]);
    }

    /// <summary>The 56-column runtime-stats read the collector's row reader expects, one row per entry.</summary>
    private static DataTable ItemTable(IEnumerable<QsRow> rows)
    {
        var table = new DataTable();
        for (var i = 0; i < 56; i++)
        {
            var type = i switch
            {
                2 or 5 or 6 or 7 or 44 or 45 or 48 or 50 or 51 or 52 => typeof(string),
                3 or 4 => typeof(DateTimeOffset),
                46 => typeof(bool),
                49 => typeof(int),
                54 or 55 => typeof(DateTime),
                _ => typeof(long),
            };
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), type);
        }

        foreach (var row in rows)
        {
            var values = new object?[56];
            for (var i = 0; i < 56; i++)
            {
                values[i] = table.Columns[i].DataType == typeof(long) ? 0L : DBNull.Value;
            }

            values[0] = row.QueryId;
            values[1] = row.QueryId;
            values[2] = "Regular";
            values[3] = new DateTimeOffset(row.LastExecution.AddMinutes(-1), TimeSpan.Zero);
            values[4] = new DateTimeOffset(row.LastExecution, TimeSpan.Zero);
            values[6] = "SELECT " + row.QueryId.ToString(CultureInfo.InvariantCulture);
            values[8] = 5L;
            values[52] = row.Role;
            values[53] = row.QueryId;
            table.Rows.Add(values.Select(v => v ?? DBNull.Value).ToArray());
        }

        return table;
    }

    private static async Task<long> CountAsync(Fixture f, long? queryId = null)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM query_store_stats WHERE server_id = $1 AND database_name = $2"
            + (queryId is null ? "" : " AND query_id = $3");
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = Sales });
        if (queryId is not null)
        {
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = queryId.Value });
        }

        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ReadSourceAsync(Fixture f)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT read_source FROM collection_log WHERE server_id = $1 AND collector_name = 'query_store' ORDER BY collection_time DESC LIMIT 1";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        var value = await command.ExecuteScalarAsync();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }
}
