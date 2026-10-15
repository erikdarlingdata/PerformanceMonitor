/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5813, Lite's behavioural twin of Darling's <c>QueryStoreReadBudgetLiveTests</c>: the real collector run
/// (<see cref="RemoteCollectorService.RunCollectorAsync(ServerConnection, string, CancellationToken)"/>, its ERROR arm and
/// its collection_log row) over the real per-database loop of the Azure SQL Database arm, with a database whose
/// runtime-stats read waits on its token and never answers. The read is cut at the budget, nothing is written, the
/// database's watermark (Lite's per-database maximum of <c>last_execution_time</c>) does not move, so the next pass asks
/// from the same cutoff, and <c>slowest_item</c> names the database with its marker and the budget. On a one-database
/// server that row is the ERROR row; with a sibling that answered it is the SUCCESS row with the partial-failure note.
///
/// <para>The enumerated arm (every engine but Azure SQL Database) is driven the same way through
/// <c>ServerReaderOverrideForTests</c>, which supplies its enumeration and per-database readers.</para>
/// </summary>
public sealed class QueryStoreReadBudgetLiteTests : IDisposable
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);
    private const string Alpha = "alpha";
    private const string Zeta = "zeta";

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _configDir;

    public QueryStoreReadBudgetLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
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
    public async Task OneDatabaseServer_ATimedOutRead_IsAnErrorRowThatNamesTheDatabase_AndKeepsTheWatermark()
    {
        var fixture = await BuildFixtureAsync();
        var watermark = await SeedAsync(fixture, Alpha);
        var reads = Serve(fixture, slow: Alpha, Alpha);

        await fixture.Service.RunCollectorAsync(fixture.Server, QueryStoreCollector.Instance.Name, CancellationToken.None);
        var cut = await NewestLogRowAsync(fixture);
        Assert.Equal("ERROR", cut.Status);
        Assert.Contains("timed out", cut.Error ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal(Alpha, cut.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, cut.SlowestItemMs);
        Assert.Equal(1, cut.ItemCount);

        /* Nothing written, and the next pass asks from the same watermark: the seeded row's last_execution_time. */
        await fixture.Service.RunCollectorAsync(fixture.Server, QueryStoreCollector.Instance.Name, CancellationToken.None);
        Assert.Equal(1, await CountAsync(fixture, Alpha));
        Assert.Equal(2, reads.Count);
        Assert.All(reads, r => Assert.Equal(Alpha, r.Database));
        Assert.All(reads, r => Assert.Equal(watermark, r.Cutoff));
        Assert.Equal("ERROR", (await NewestLogRowAsync(fixture)).Status);
    }

    [Fact]
    public async Task ASiblingThatAnswers_KeepsTheRunASuccess_WithTheTimedOutDatabaseAsItsSlowestItem()
    {
        var fixture = await BuildFixtureAsync();
        var watermark = await SeedAsync(fixture, Alpha);
        var reads = Serve(fixture, slow: Alpha, Alpha, Zeta);

        await fixture.Service.RunCollectorAsync(fixture.Server, QueryStoreCollector.Instance.Name, CancellationToken.None);
        var row = await NewestLogRowAsync(fixture);
        Assert.Equal("SUCCESS", row.Status);
        Assert.Equal(Alpha, row.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, row.SlowestItemMs);
        Assert.Equal(2, row.ItemCount);

        /* #5813 review: the timed-out database's SQL slice is in the run's sql_duration_ms. The slice starts before the
           read's budget, so it is at least the budget; before the fix the catch arm never added it. */
        Assert.True(row.SqlMs >= (int)Budget.TotalMilliseconds, $"sql_duration_ms {row.SqlMs} holds the timed-out slice");
        Assert.Contains("timed out", fixture.Service.TelemetryFor(fixture.ServerId).Note ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal(1, await CountAsync(fixture, Alpha));
        Assert.Equal(watermark, reads.Single(r => r.Database == Alpha).Cutoff);
    }

    /// <summary>
    /// #5813 review, Darling's twin: the run stores the timed-out database under its plain name, and get_collection_health
    /// adds the words from its budget-sized cost. The run here is cut at the test's 1 s budget, so its row is moved to the
    /// production budget's 30,000 ms (the one fact the renderer reads) before the tool reads it.
    /// </summary>
    [Fact]
    public async Task CollectionHealth_SaysTheTimedOutDatabaseTimedOut_WhileTheStoredNameStaysPlain()
    {
        var fixture = await BuildFixtureAsync();
        await SeedAsync(fixture, Alpha);
        Serve(fixture, slow: Alpha, Alpha, Zeta);

        await fixture.Service.RunCollectorAsync(fixture.Server, QueryStoreCollector.Instance.Name, CancellationToken.None);
        Assert.Equal(Alpha, (await NewestLogRowAsync(fixture)).SlowestItem);

        using (var connection = fixture.DuckDb.CreateConnection())
        {
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.CommandText = "UPDATE collection_log SET slowest_item_ms = 30000, duration_ms = 31000 "
                + "WHERE server_id = $1 AND collector_name = 'query_store'";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = fixture.ServerId });
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }

        var json = await McpHealthTools.GetCollectionHealth(
            new LocalDataService(fixture.DuckDb), fixture.Servers, fixture.Server.DisplayName, full_detail: true);
        using var document = JsonDocument.Parse(json);
        var queryStore = document.RootElement.GetProperty("collectors").EnumerateArray()
            .Single(r => r.GetProperty("collector").GetString() == QueryStoreCollector.Instance.Name);
        var fanout = queryStore.GetProperty("fanout");
        Assert.Equal(Alpha + QueryStoreReadBudget.TimedOutMarker, fanout.GetProperty("slowest").GetString());
        Assert.Equal(30_000, fanout.GetProperty("slowest_ms").GetInt32());
    }

    /// <summary>
    /// The enumerated arm: query_store lists the server's databases and reads each on the same connection. The only
    /// database's read never answers, so the read is cut at the budget and the run's ERROR row names that database with
    /// the budget, as the Azure loop's row does. Without the budget the read would wait out the guard and write no row.
    /// </summary>
    [Fact]
    public async Task TheEnumeratedArm_CutsATimedOutRead_AndItsErrorRowNamesTheDatabase()
    {
        var fixture = await BuildFixtureAsync(engineEdition: 3);
        var enumeration = new DataTable();
        enumeration.Columns.Add("database_name", typeof(string));
        enumeration.Rows.Add(Alpha);
        fixture.Service.ServerReaderOverrideForTests = plan =>
            plan.Parameters?.Any(static p => p.Name == "@cutoff_time") == true
                ? new NeverAnsweringReader()
                : enumeration.CreateDataReader();

        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await fixture.Service.RunCollectorAsync(fixture.Server, QueryStoreCollector.Instance.Name, guard.Token);

        var cut = await NewestLogRowAsync(fixture);
        Assert.Equal("ERROR", cut.Status);
        Assert.Contains("timed out", cut.Error ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal(Alpha, cut.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, cut.SlowestItemMs);
        Assert.Equal(1, cut.ItemCount);
    }

    private sealed record Fixture(RemoteCollectorService Service, ServerConnection Server, int ServerId, DuckDbInitializer DuckDb, ServerManager Servers);

    /// <summary>
    /// Lists <paramref name="databases"/>; each read records the database and the runtime-stats cutoff it was asked
    /// from. The <paramref name="slow"/> database's read waits on its token, the stand-in for a WAITFOR; every other
    /// database answers with no rows.
    /// </summary>
    private static List<(string Database, DateTime? Cutoff)> Serve(Fixture fixture, string slow, params string[] databases)
    {
        var reads = new List<(string, DateTime?)>();
        fixture.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(databases.ToList());
        fixture.Service.AzureDatabaseReaderOverrideForTests = (database, plan) =>
        {
            var cutoff = plan.Parameters?.FirstOrDefault(static p => p.Name == "@cutoff_time")?.Value as DateTime?;
            lock (reads)
            {
                reads.Add((database, cutoff));
            }

            return database == slow ? new NeverAnsweringReader() : new DataTable().CreateDataReader();
        };

        return reads;
    }

    /// <summary>One stored row for <paramref name="database"/>, collected a minute ago; returns its watermark.</summary>
    private static async Task<DateTime> SeedAsync(Fixture fixture, string database)
    {
        var now = DateTime.UtcNow;
        var watermark = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddMinutes(-8);
        using var connection = fixture.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText =
            "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, last_execution_time) "
            + "VALUES (1, $1, $2, 'azure-test', $3, $4)";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = now.AddMinutes(-1) });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = fixture.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = database });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = watermark });
        await command.ExecuteNonQueryAsync();
        return watermark;
    }

    private static async Task<long> CountAsync(Fixture fixture, string database)
    {
        using var connection = fixture.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM query_store_stats WHERE server_id = $1 AND database_name = $2";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = fixture.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = database });
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<(string Status, string? Error, string? SlowestItem, int? SlowestItemMs, int? ItemCount, int? SqlMs)> NewestLogRowAsync(Fixture fixture)
    {
        using var connection = fixture.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT status, error_message, slowest_item, slowest_item_ms, fanout_item_count, sql_duration_ms FROM collection_log "
            + "WHERE server_id = $1 AND collector_name = 'query_store' ORDER BY collection_time DESC LIMIT 1";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = fixture.ServerId });
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "the run wrote a collection_log row");
        return (reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3), System.Globalization.CultureInfo.InvariantCulture),
            reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4), System.Globalization.CultureInfo.InvariantCulture),
            reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5), System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>A logical-server registration on the Azure SQL Database engine edition, so query_store takes the
    /// per-database loop, with the read budget at one second. Any other <paramref name="engineEdition"/> takes the
    /// enumerated arm.</summary>
    private async Task<Fixture> BuildFixtureAsync(int engineEdition = 5)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { ServerName = "azure-test", DisplayName = "azure-test" };
        serverManager.AddServer(server);
        serverManager.GetConnectionStatus(server.Id).SqlEngineEdition = engineEdition;

        var service = new RemoteCollectorService(duckDb, serverManager, new ScheduleManager(_configDir))
        {
            QueryStoreReadBudgetSpan = Budget,
        };
        return new Fixture(service, server, RemoteCollectorService.GetServerId(server), duckDb, serverManager);
    }

    /// <summary>The runtime-stats read that never answers: its first row waits until its token is cancelled.</summary>
    private sealed class NeverAnsweringReader : DbDataReader
    {
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return false;
        }

        public override bool Read() => throw new NotSupportedException("the loop reads asynchronously");
        public override bool NextResult() => false;
        public override int FieldCount => 0;
        public override bool HasRows => true;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int ordinal] => throw new NotSupportedException();
        public override object this[string name] => throw new NotSupportedException();
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override string GetName(int ordinal) => throw new NotSupportedException();
        public override int GetOrdinal(string name) => throw new NotSupportedException();
        public override string GetString(int ordinal) => throw new NotSupportedException();
        public override object GetValue(int ordinal) => throw new NotSupportedException();
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override bool IsDBNull(int ordinal) => throw new NotSupportedException();
    }
}
