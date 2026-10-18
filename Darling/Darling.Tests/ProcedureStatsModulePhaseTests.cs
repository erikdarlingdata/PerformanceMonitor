/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The deploy collector's module phase (#6009, option C): it reads <c>modify_date</c> only for the modules a pending deploy
/// follow-up watches, and sends nothing when nothing is watched.
/// </summary>
public sealed class ProcedureStatsModulePhaseTests
{
    private static readonly DateTime s_modified = new(2026, 10, 17, 9, 30, 0, DateTimeKind.Utc);

    private static ProcedureStatsCollector.Row RowFor(string db, string schema, string name) =>
        default(ProcedureStatsCollector.Row) with { DatabaseName = db, SchemaName = schema, ObjectName = name, ObjectType = "P" };

    private static CollectorContext Context(bool azure = false) => new()
    {
        ServerId = 42,
        ServerName = "module-test",
        CollectionTime = s_modified,
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
    };

    private static ServerRuntime Server() => new()
    {
        Config = new MonitoredServer { Name = "module-test", Host = "module-test" },
        ConnectionString = "Server=module-test;Initial Catalog=master",
        Target = new CollectorTargetInfo(),
        StorageName = "module-test",
        ServerId = 42,
    };

    private static DarlingCollectorRunner Runner() => new(
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x"), new CollectorDeltaCalculator());

    /// <summary>A target that counts the queries sent and answers each with <paramref name="answer"/>'s rows.</summary>
    private sealed class CountingProvider(Func<DataTable> answer) : ITargetProvider
    {
        public List<CollectorQuery> Sent { get; } = new();

        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => throw new NotSupportedException();

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds)
        {
            Sent.Add(query);
            return new AnsweringCommand(answer());
        }

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class AnsweringCommand(DataTable table) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => table.CreateDataReader();
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult<DbDataReader>(table.CreateDataReader());
    }

    private static DataTable Answer(params (string Schema, string Name, DateTime Modified)[] objects)
    {
        var table = new DataTable();
        table.Columns.Add("schema_name", typeof(string));
        table.Columns.Add("object_name", typeof(string));
        table.Columns.Add("modify_date", typeof(DateTime));
        foreach (var (schema, name, modified) in objects)
        {
            table.Rows.Add(schema, name, modified);
        }

        return table;
    }

    [Fact]
    public async Task NothingWatched_SendsNoModuleQuery_AndLeavesEveryRowWithoutADate()
    {
        var provider = new CountingProvider(() => Answer(("dbo", "usp_Orders", s_modified)));
        var rows = new List<ProcedureStatsCollector.Row> { RowFor("Shop", "dbo", "usp_Orders"), RowFor("Shop", "dbo", "usp_Other") };

        var sent = await Runner().RunProcedureStatsModulePhaseAsync(
            provider, null, null!, Server(), Context(), rows, Array.Empty<ProcedureStatsCollector.WatchedModule>(), CancellationToken.None);

        Assert.Equal(0, sent);
        Assert.Empty(provider.Sent);
        Assert.All(rows, r => Assert.Null(r.ModuleModifyDate));
    }

    [Fact]
    public async Task AWatchedModuleThatDidNotRunInTheWindow_SendsNoModuleQueryEither()
    {
        var provider = new CountingProvider(() => Answer(("dbo", "usp_Orders", s_modified)));
        var rows = new List<ProcedureStatsCollector.Row> { RowFor("Shop", "dbo", "usp_Other") };

        var sent = await Runner().RunProcedureStatsModulePhaseAsync(
            provider, null, null!, Server(), Context(), rows,
            new[] { new ProcedureStatsCollector.WatchedModule("Shop", "dbo", "usp_Orders") }, CancellationToken.None);

        Assert.Equal(0, sent);
        Assert.Empty(provider.Sent);
    }

    [Fact]
    public async Task AWatchedModulesRowsCarryItsModifyDate_AndEveryOtherRowStaysNull()
    {
        var provider = new CountingProvider(() => Answer(("dbo", "usp_Orders", s_modified)));
        var rows = new List<ProcedureStatsCollector.Row>
        {
            RowFor("Shop", "dbo", "usp_Orders"),
            RowFor("Shop", "dbo", "usp_Other"),
            RowFor("Billing", "dbo", "usp_Orders"),
        };

        var sent = await Runner().RunProcedureStatsModulePhaseAsync(
            provider, null, null!, Server(), Context(), rows,
            new[] { new ProcedureStatsCollector.WatchedModule("Shop", "dbo", "usp_Orders") }, CancellationToken.None);

        Assert.Equal(1, sent);
        var query = Assert.Single(provider.Sent);
        Assert.Equal(s_modified, rows[0].ModuleModifyDate);
        Assert.Null(rows[1].ModuleModifyDate);
        Assert.Null(rows[2].ModuleModifyDate);

        /* The names travel as parameters and the database through QUOTENAME, never in the text. */
        Assert.DoesNotContain("usp_Orders", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("Shop", query.Text, StringComparison.Ordinal);
        Assert.Contains("QUOTENAME(@db)", query.Text, StringComparison.Ordinal);
        Assert.Contains(query.Parameters, p => p.Name == "@db" && (string?)p.Value == "Shop");
        Assert.Contains(query.Parameters, p => p.Name == "@o0" && (string?)p.Value == "usp_Orders");
    }

    [Fact]
    public async Task OnAzureSqlDatabase_TheModulePhaseReadsTheConnectedDatabase_WithNoCrossDatabaseName()
    {
        var provider = new CountingProvider(() => Answer(("dbo", "usp_Orders", s_modified)));
        var rows = new List<ProcedureStatsCollector.Row> { RowFor("Shop", "dbo", "usp_Orders") };

        await Runner().RunProcedureStatsModulePhaseAsync(
            provider, null, null!, Server(), Context(azure: true), rows,
            new[] { new ProcedureStatsCollector.WatchedModule("Shop", "dbo", "usp_Orders") }, CancellationToken.None);

        var query = Assert.Single(provider.Sent);
        Assert.DoesNotContain("QUOTENAME", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain(query.Parameters, p => p.Name == "@db");
        Assert.Equal(s_modified, rows[0].ModuleModifyDate);
    }
}
