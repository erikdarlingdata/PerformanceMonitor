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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6182: drives the host's REAL Azure SQL Database per-database loop for procedure_stats and checks that a procedure the
/// main read lists comes out of the store with its plan. #5449 moved the plan out of the main query into a second "plan
/// phase" query, and wired that phase into the server-scoped path only: the per-database loop Azure SQL Database takes
/// (<c>ProcedureStatsCollector.RunsPerDatabase</c> is <c>IsAzureSqlDb</c>) read the numbers, never asked for the plan, and
/// stored every procedure with a NULL plan. A plan query that fails for one database must not cost that database its
/// rows, and must not touch the next database's plan.
///
/// <para><b>#1776 own-store</b> - the test creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ProcedureStatsAzureLoopPlanPhaseLiveTests
{
    private const int LiveServerId = -618200;
    private const string Alpha = "alpha";
    private const string Zeta = "zeta";

    private const string PlanXml =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.6\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"SELECT 1\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AzureLoop_StoresEachDatabasesProcedurePlan_AndAFailedPlanQueryCostsOnlyThatDatabasesPlans()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6182 Azure loop plan pin.");
        var token = TestContext.Current.CancellationToken;

        await RunTwoDatabasePassAsync(failPlanQueryOn: Zeta, async (result, provider, source) =>
        {
            Assert.Equal(2, result.Rows);
            Assert.Equal(new[] { Alpha, Zeta }, provider.PlanQueriesAskedOf);

            /* The read get_procedure_plan_xml and analyze_procedure_plan make: by the sql_handle that get_top_procedures_by_cpu lists. */
            var alphaPlan = await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(
                source, LiveServerId, SqlHandleOf(Alpha), cancellationToken: token);
            var zetaPlan = await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(
                source, LiveServerId, SqlHandleOf(Zeta), cancellationToken: token);

            Assert.True(
                !string.IsNullOrEmpty(alphaPlan) && alphaPlan.Contains("ShowPlanXML", StringComparison.Ordinal),
                "alpha's procedure plan comes from the plan query for alpha");
            Assert.True(string.IsNullOrEmpty(zetaPlan), "zeta's plan query failed, so its row ships without a plan");

            /* The failed query still cost zeta its milliseconds (each database's phase is at least 1 ms) and attempted none of
               zeta's ids: the run's sums hold both databases' time and only alpha's one id. */
            var plan = result.FetchPhases?.Plan;
            Assert.NotNull(plan);
            Assert.True(plan.Value.TargetMs >= 2, "plan_fetch target time covers both databases, got " + plan.Value.TargetMs);
            Assert.Equal(1, plan.Value.IdsAttempted);
        });
    }

    [Fact]
    public async Task AzureLoop_PlanFetchSumsCoverEveryDatabaseOfThePass()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6182 Azure loop plan pin.");

        await RunTwoDatabasePassAsync(failPlanQueryOn: null, (result, provider, _) =>
        {
            Assert.Equal(new[] { Alpha, Zeta }, provider.PlanQueriesAskedOf);

            /* One plan query per database, each at least 1 ms and one id: a pass that kept only the last database's stamp would
               read 1 id here, not 2. */
            var plan = result.FetchPhases?.Plan;
            Assert.NotNull(plan);
            Assert.Equal(2, plan.Value.IdsAttempted);
            Assert.True(plan.Value.TargetMs >= 2, "plan_fetch target time covers both databases, got " + plan.Value.TargetMs);
            Assert.Contains(result.Measurements, m => m.Label == "plans_rendered" && m.Value == 2);
            return Task.CompletedTask;
        });
    }

    private static async Task RunTwoDatabasePassAsync(
        string? failPlanQueryOn,
        Func<CollectorRunResult, FakeTargetProvider, NpgsqlDataSource, Task> assertions)
    {
        var token = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, token);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(token);
            await PgMigrations.MigrateAsync(migrate, token);
        }

        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        /* compressPlanContent off: the plan lands as readable text in query_plan_xml, which is what the assertion reads. */
        var runner = new DarlingCollectorRunner(source, new CollectorDeltaCalculator(), compressPlanContent: () => false)
        {
            AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Alpha, Zeta }),
        };
        var provider = new FakeTargetProvider(failPlanQueryOn);
        runner.TargetProviderOverrideForTests = _ => provider;

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "t", Host = "h" },
            ConnectionString = "Server=azure;Database=master",
            Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
            StorageName = "h",
            ServerId = LiveServerId,
        };

        var result = await runner.RunAsync(ProcedureStatsCollector.Instance, server, token);

        await assertions(result, provider, source);
    }

    private static string SqlHandleOf(string database) => "0x03000500" + (database == Alpha ? "A1" : "B2") + "AABBCC";

    private static DataTable MainRows(string database)
    {
        var table = new DataTable();
        string[] text = ["database_name", "schema_name", "object_name", "object_type"];
        foreach (var name in text)
        {
            table.Columns.Add(name, typeof(string));
        }

        table.Columns.Add("cached_time", typeof(DateTime));
        table.Columns.Add("last_execution_time", typeof(DateTime));
        for (var i = 6; i <= 24; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), typeof(long));
        }

        table.Columns.Add("sql_handle", typeof(string));
        table.Columns.Add("plan_handle", typeof(string));

        var when = DateTime.UtcNow.AddMinutes(-1);
        var values = new List<object>
        {
            database, "dbo", "usp_work", "PROCEDURE", when.AddHours(-1), when,
        };
        for (var i = 6; i <= 24; i++)
        {
            values.Add(i == 6 ? 5L : 1000L);
        }

        values.Add(SqlHandleOf(database));
        values.Add("0x05000500" + (database == Alpha ? "A1" : "B2") + "AABBCCDD");
        table.Rows.Add(values.ToArray());
        return table;
    }

    private static DataTable PlanRows()
    {
        var table = new DataTable();
        table.Columns.Add("ord", typeof(int));
        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        table.Rows.Add(0, PlanXml, (long)PlanXml.Length);
        return table;
    }

    private sealed class FakeTargetProvider(string? failPlanQueryOn) : ITargetProvider
    {
        public List<string> PlanQueriesAskedOf { get; } = new();

        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand(this, ((FakeConnection)connection).DatabaseName, query.Text);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();

        public DbDataReader Execute(string database, string text)
        {
            /* The plan phase is the only query over a VALUES list of handles aliased `ranked`. */
            if (text.Contains(") AS ranked (ord, plan_handle)", StringComparison.Ordinal))
            {
                PlanQueriesAskedOf.Add(database);
                if (database == failPlanQueryOn)
                {
                    throw new InvalidOperationException("injected plan query failure for " + database);
                }

                return PlanRows().CreateDataReader();
            }

            Assert.Contains("FROM sys.dm_exec_procedure_stats", text, StringComparison.Ordinal);
            return MainRows(database).CreateDataReader();
        }
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;
        public string DatabaseName => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "master";

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeCommand(FakeTargetProvider owner, string database, string initialText) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = initialText;
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
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => owner.Execute(database, CommandText);
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult(owner.Execute(database, CommandText));
    }
}
