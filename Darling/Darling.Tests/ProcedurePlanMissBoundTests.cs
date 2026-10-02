/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4954: an absent sql_handle is answered by a module-map probe plus a scan bounded to the rows newer than the
/// map's last refresh, instead of a scan of the server's whole procedure_stats history. The live pins drive
/// <see cref="DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync"/> itself.
/// </summary>
[Collection("live-postgres")]
public sealed class ProcedurePlanMissBoundTests
{
    private const int ServerId = -974954;
    private const string ServerName = "plan-miss-bound";

    [Fact]
    public void BoundedSql_IsTheUnboundedSqlPlusExactlyOnePredicate()
    {
        var unbounded = DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql;
        var bounded = DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleBoundedSql;

        Assert.Equal(unbounded.Length + "AND   ps.collection_time > $3\n".Length, bounded.Length);
        Assert.Equal(unbounded, bounded.Replace("AND   ps.collection_time > $3\n", "", StringComparison.Ordinal));
        Assert.Equal(1, Count(bounded, "ps.collection_time > $3"));
        Assert.DoesNotContain(" OR ps.collection_time", bounded, StringComparison.Ordinal);
    }

    [Fact]
    public void Probe_ResolvesTheServerNameFromProcedureStats_TheSpellingTheRefreshWrites()
    {
        Assert.Contains("SELECT server_name FROM collect.procedure_stats", DarlingStoredPlanReader.ModuleMapProbeSql, StringComparison.Ordinal);
        Assert.Contains("server_name, sql_handle, database_name, schema_name, object_name, collection_time\nFROM collect.procedure_stats", DarlingModuleMap.RefreshSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromDays(2), DarlingStoredPlanReader.ModuleMapStaleAfter);
    }

    [Fact]
    public async Task EmptyMap_FallsBackToTheFullScan_StoredHandleIsFound()
    {
        await RunAsync(async (cs, ct) =>
        {
            var now = Now();
            await InsertProcAsync(cs, now.AddHours(-30), "0x4954OLD", ct);
            await InsertProcAsync(cs, now.AddHours(-1), "0x4954NEWEST", ct);
            await using var pg = NpgsqlDataSource.Create(cs);
            Assert.Equal(PlanFor("0x4954OLD"), await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954OLD", cancellationToken: ct));
        });
    }

    [Fact]
    public async Task FreshMap_BoundsTheMiss_FindsNewUnmappedAndMappedHandles_AbsentIsNotFound()
    {
        await RunAsync(async (cs, ct) =>
        {
            var now = Now();
            await InsertProcAsync(cs, now.AddHours(-30), "0x4954MAPPED", ct);
            await InsertProcAsync(cs, now.AddHours(-30), "0x4954UNMAPPEDOLD", ct);
            await InsertProcAsync(cs, now.AddHours(-1), "0x4954NEW", ct);
            await InsertMapAsync(cs, "0x4954MAPPED", now.AddHours(-3), ct);
            await using var pg = NpgsqlDataSource.Create(cs);

            /* mapped: the unbounded read, even though the row is far below the horizon */
            Assert.Equal(PlanFor("0x4954MAPPED"), await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954MAPPED", cancellationToken: ct));
            /* written after the horizon, not mapped: found by the bounded read */
            Assert.Equal(PlanFor("0x4954NEW"), await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954NEW", cancellationToken: ct));
            /* below horizon - 1h and unmapped: the bounded read does not reach it, which shows the bound is applied */
            Assert.Null(await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954UNMAPPEDOLD", cancellationToken: ct));
            /* absent */
            Assert.Null(await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954ABSENT", cancellationToken: ct));
        });
    }

    [Fact]
    public async Task StaleMap_FallsBackToTheFullScan_AndLogsOncePerServer()
    {
        await RunAsync(async (cs, ct) =>
        {
            DarlingStoredPlanReader.ResetStaleMapLogForTests();
            var now = Now();
            await InsertProcAsync(cs, now.AddHours(-30), "0x4954OLD", ct);
            await InsertProcAsync(cs, now.AddHours(-1), "0x4954NEWEST", ct);
            await InsertMapAsync(cs, "0x4954SOMETHING", now.AddDays(-3), ct);
            await using var pg = NpgsqlDataSource.Create(cs);
            var logger = new CapturingLogger();

            Assert.Equal(PlanFor("0x4954OLD"), await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954OLD", logger, ct));
            Assert.Equal(PlanFor("0x4954OLD"), await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(pg, ServerId, "0x4954OLD", logger, ct));
            Assert.Single(logger.Messages);
            Assert.Contains("looks stale", logger.Messages[0], StringComparison.Ordinal);
        });
    }

    private static int Count(string text, string needle)
    {
        var n = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + 1, StringComparison.Ordinal)) { n++; }
        return n;
    }

    private static DateTime Now() => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    private static string PlanFor(string handle) => $"<ShowPlanXML>{handle}</ShowPlanXML>";

    private static async Task RunAsync(Func<string, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4954 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var builder = new NpgsqlConnectionStringBuilder(cs) { SearchPath = PgSchemaGenerator.SearchPath };
        await using (var setup = new NpgsqlConnection(cs))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            Assert.True(await DarlingModuleMap.EnsureTableAsync(setup, null, ct));
            await DeleteAsync(setup, ct);
        }

        var ok = false;
        try
        {
            await body(builder.ConnectionString, ct);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, ok, async (c, cct) => await DeleteAsync(c, cct));
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"DELETE FROM collect.procedure_stats WHERE server_id = {ServerId}; DELETE FROM collect.module_map WHERE server_name = '{ServerName}';", c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertProcAsync(string cs, DateTime t, string handle, CancellationToken ct)
    {
        await using var c = new NpgsqlConnection(cs);
        await c.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, query_plan_xml)
VALUES (1, $1, $2, $3, 'PlanDb', 'dbo', 'usp_4954', $4, 100, 200, 1, $5)", c);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(handle);
        cmd.Parameters.AddWithValue(PlanFor(handle));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMapAsync(string cs, string handle, DateTime lastSeen, CancellationToken ct)
    {
        await using var c = new NpgsqlConnection(cs);
        await c.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO collect.module_map (server_name, sql_handle, database_name, schema_name, object_name, last_seen) VALUES ($1, $2, 'PlanDb', 'dbo', 'usp_4954', $3)", c);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(handle);
        cmd.Parameters.AddWithValue(lastSeen);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Messages { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
    }
}
