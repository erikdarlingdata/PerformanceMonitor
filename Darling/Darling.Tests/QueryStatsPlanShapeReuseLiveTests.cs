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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8994 against a real PostgreSQL (gated on DARLING_TEST_PG): ten deferred query_stats runs over one workload of ad hoc
/// statements that differ only in their literals, plus procedures, go through the runner's partition, the plan fetch's
/// bookkeeping and the collector's own <c>WritePayload</c>, <c>PgCollectorRowWriter</c> and <c>PayloadDimensionWriter</c>.
/// The store must hold one plan per statement shape, and every stored row must still resolve a plan.
///
/// <para>The inline capture (Azure SQL Database, and any store with <c>queryStatsDeferredPlanFetch</c> off) has its own cases
/// below. They run the runner's real <c>RunAsync</c> against a fake target that renders each plan in the main read, so both
/// write sites (the server-scoped write and the Azure per-database write) go through <c>LinkInlineQueryStatsPlansByShape</c>.</para>
/// </summary>
public sealed class QueryStatsPlanShapeReuseLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the plan shape store tests.";
    private const int ServerId = 8994;
    private const string ServerName = "pm8994";
    private const int Shapes = 5;
    private const int VariantsPerShape = 60;
    private const int Procedures = 20;
    private const int Runs = 10;

    private static readonly DateTime Anchor = new(2031, 4, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    private static CollectorContext ContextFor(DateTime collectionTime) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        CollectionTime = collectionTime,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
        CapturePlanXml = true,
        DeferPlanXmlFetch = true,
    };

    private int _nextHandle;

    private QueryStatsCollector.Row Statement(string handlePrefix, int handle, string queryHash, string planHash, string text) => new()
    {
        PlanHandle = handlePrefix + handle.ToString("X8", CultureInfo.InvariantCulture),
        SqlHandle = "0x02" + handle.ToString("X8", CultureInfo.InvariantCulture),
        DatabaseName = "app",
        CreationTime = new DateTime(2031, 4, 10, 11, 0, 0, DateTimeKind.Unspecified),
        LastExecutionTime = Anchor,
        PlanGenerationNum = 1,
        StatementStartOffset = 0,
        StatementEndOffset = -1,
        ExecutionCount = 1,
        TotalElapsedTime = 1000,
        QueryHash = queryHash,
        QueryPlanHash = planHash,
        QueryText = text,
    };

    /// <summary>One run's rows: every ad hoc execution is a new plan_handle with its own literal; procedures keep theirs.</summary>
    private List<QueryStatsCollector.Row> Workload()
    {
        var rows = new List<QueryStatsCollector.Row>();
        for (var v = 0; v < VariantsPerShape; v++)
        {
            for (var s = 0; s < Shapes; s++)
            {
                var n = ++_nextHandle;
                rows.Add(Statement(
                    "0x06", n,
                    "0xA" + s.ToString("X15", CultureInfo.InvariantCulture),
                    "0xB" + s.ToString("X15", CultureInfo.InvariantCulture),
                    "SELECT * FROM dbo.t" + s.ToString(CultureInfo.InvariantCulture) + " WHERE id = " + n.ToString(CultureInfo.InvariantCulture)));
            }
        }

        for (var p = 0; p < Procedures; p++)
        {
            rows.Add(Statement(
                "0x05", p,
                "0xC" + p.ToString("X15", CultureInfo.InvariantCulture),
                "0xD" + p.ToString("X15", CultureInfo.InvariantCulture),
                "SELECT * FROM dbo.p" + p.ToString(CultureInfo.InvariantCulture) + " WHERE id = @id"));
        }

        return rows;
    }

    /// <summary>What the target renders for a fetched row: a plan whose StatementText carries the row's own literal.</summary>
    private static string PlanFor(QueryStatsCollector.Row row) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + row.QueryText + "\" QueryHash=\"" + row.QueryHash + "\" QueryPlanHash=\"" + row.QueryPlanHash + "\">"
        + "<QueryPlan>" + string.Concat(Enumerable.Repeat("<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Seek\" />", 40)) + "</QueryPlan>"
        + "</StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static async Task WriteAsync(NpgsqlConnection connection, DateTime collectionTime, IReadOnlyList<QueryStatsCollector.Row> rows, CancellationToken ct)
    {
        var definition = QueryStatsCollector.Instance;
        var context = ContextFor(collectionTime);
        var writer = new PgCollectorRowWriter();
        var dimensions = new PayloadDimensionBatch();
        writer.UseDimensions(PayloadDimensions.DiversionPlanFor(definition), dimensions);

        await using var transaction = await connection.BeginTransactionAsync(ct);
        using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
        {
            writer.Importer = importer;
            foreach (var row in rows)
            {
                await importer.StartRowAsync(ct);
                writer.Value(CollectionIdGenerator.Next());
                writer.Value(collectionTime).Value(ServerId).Value(ServerName);
                writer.BeginPayload();
                definition.WritePayload(row, writer, context);
                writer.EndPayload(definition.PayloadColumns.Count);
            }

            await importer.CompleteAsync(ct);
        }

        Assert.Empty(await PayloadDimensionWriter.FlushAsync(connection, transaction, dimensions, collectionTime, ct));
        await transaction.CommitAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? 0L : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task TenRunsOfLiteralOnlyVariants_StoreOnePlanPerShape_AndEveryRowResolvesAPlan()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var identities = new PlanDigestCache<QueryStatsPlanKey>();
            var shapes = new PlanDigestCache<QueryStatsPlanShapeKey>();
            var fetchedPerRun = new List<int>();
            var plansPerRun = new List<long>();
            var rowsStored = 0;

            for (var run = 0; run < Runs; run++)
            {
                var now = new DateTime(2031, 4, 10, 12, run, 0, DateTimeKind.Utc);
                var collectionTime = Anchor.AddMinutes(run);
                var rows = Workload();
                var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsPlans(ServerId, rows, identities, now, shapes);

                var pending = new List<QueryStatsPlanKey>();
                var pendingShapes = new List<QueryStatsPlanShapeKey>();
                var fetchedByShape = new Dictionary<QueryStatsPlanShapeKey, (string? PlanXml, long? Bytes)>();
                foreach (var (key, row) in misses)
                {
                    row.QueryPlanXml = PlanFor(row);
                    row.QueryPlanXmlBytes = row.QueryPlanXml.Length * 2L;
                    var digest = Convert.ToHexString(PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(row.QueryPlanXml)));
                    identities.AddPending(key, digest, row.QueryPlanXmlBytes, now);
                    pending.Add(key);
                    DarlingCollectorRunner.RecordFetchedShape(ServerId, row, digest, shapes, pendingShapes, fetchedByShape, now);
                }

                DarlingCollectorRunner.ShareFetchedPlansByShape(ServerId, rows, fetchedByShape);

                var before = await ScalarAsync(connection, "SELECT count(*) FROM query_plan_dim", ct);
                await WriteAsync(connection, collectionTime, rows, ct);
                identities.Settle(pending, committed: true, now);
                shapes.Settle(pendingShapes, committed: true, now);

                fetchedPerRun.Add(misses.Count);
                plansPerRun.Add(await ScalarAsync(connection, "SELECT count(*) FROM query_plan_dim", ct) - before);
                rowsStored += rows.Count;
            }

            var planRows = await ScalarAsync(connection, "SELECT count(*) FROM query_plan_dim", ct);
            var planBytes = await ScalarAsync(connection, "SELECT coalesce(sum(pg_column_size(d.*)), 0) FROM query_plan_dim AS d", ct);
            var summary = "plans fetched per run " + string.Join(",", fetchedPerRun)
                + "; query_plan_dim rows added per run " + string.Join(",", plansPerRun)
                + "; query_plan_dim rows " + planRows.ToString(CultureInfo.InvariantCulture)
                + ", bytes " + planBytes.ToString(CultureInfo.InvariantCulture);
            TestContext.Current.TestOutputHelper?.WriteLine(summary);

            /* One plan per ad hoc shape and per procedure, all stored by the first run. */
            Assert.True(planRows == Shapes + Procedures, summary);
            Assert.True(plansPerRun.Skip(1).All(n => n == 0), summary);

            /* Every row of every run still resolves a plan through the view readers use. */
            Assert.Equal(rowsStored, await ScalarAsync(connection,
                "SELECT count(*) FROM v_query_stats WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture)
                + " AND (query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private const int InlineVariantsPerShape = 30;
    private const int InlineProcedures = 10;
    private const int InlineServerScopedId = -899401;
    private const int InlineAzureId = -899402;

    private static readonly string[] InlineAzureDatabases = ["alpha", "zeta"];

    [Fact]
    public async Task InlineCapture_ServerScopedWrite_TenRunsOfLiteralOnlyVariants_StoreOnePlanPerShape()
    {
        await RunInlineTenRunsAsync(azure: false);
    }

    [Fact]
    public async Task InlineCapture_AzurePerDatabaseWrite_TenRunsOfLiteralOnlyVariants_StoreOnePlanPerShapePerDatabase()
    {
        await RunInlineTenRunsAsync(azure: true);
    }

    private static async Task RunInlineTenRunsAsync(bool azure)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            /* The service signs in with the store's search path, which the unqualified COPY target needs. */
            await using var source = NpgsqlDataSource.Create(DarlingStoreConnection.EnsureStoreSearchPath(scratch.ConnectionString));
            var target = new InlineFakeTarget();
            var serverId = azure ? InlineAzureId : InlineServerScopedId;
            /* queryStatsDeferredPlanFetch off: the server-scoped path takes the inline capture, as Azure SQL Database always does. */
            var runner = new DarlingCollectorRunner(source, new CollectorDeltaCalculator(), queryStatsDeferredPlanFetch: () => false)
            {
                TargetProviderOverrideForTests = _ => new InlineFakeTargetProvider(target),
                AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(InlineAzureDatabases.ToList()),
            };
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "t8994", Host = "t8994" },
                ConnectionString = "Server=target;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = azure, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "t8994",
                ServerId = serverId,
            };

            var plansPerRun = new List<long>();
            var planRowsPerRun = new List<long>();
            var planBytesPerRun = new List<long>();
            var storedRows = 0L;
            for (var run = 0; run < Runs; run++)
            {
                target.Run = run;
                var plansBefore = await ScalarAsync(source, "SELECT count(*) FROM query_plan_dim", ct);
                var result = await runner.RunAsync(QueryStatsCollector.Instance, server, ct);

                var plansAfter = await ScalarAsync(source, "SELECT count(*) FROM query_plan_dim", ct);
                plansPerRun.Add(plansAfter - plansBefore);
                planRowsPerRun.Add(plansAfter);
                planBytesPerRun.Add(await ScalarAsync(source, "SELECT coalesce(sum(pg_column_size(d.*)), 0) FROM query_plan_dim AS d", ct));
                storedRows += result.Rows;
            }

            var summary = (azure ? "azure per-database write" : "server-scoped write")
                + "; plans added per run " + string.Join(",", plansPerRun)
                + "; query_plan_dim rows after each run " + string.Join(",", planRowsPerRun)
                + "; query_plan_dim bytes after each run " + string.Join(",", planBytesPerRun);
            TestContext.Current.TestOutputHelper?.WriteLine(summary);

            /* One plan per ad hoc shape and per procedure (per database on Azure), all stored by the first run. */
            var expected = (Shapes + InlineProcedures) * (azure ? InlineAzureDatabases.Length : 1);
            Assert.True(plansPerRun[0] == expected, summary);
            Assert.True(plansPerRun.Skip(1).All(n => n == 0), summary);

            /* Every row of every run still resolves a plan through the view readers use. */
            Assert.True(storedRows > 0, summary);
            Assert.Equal(storedRows, await ScalarAsync(source,
                "SELECT count(*) FROM v_query_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " AND (query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<long> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        return await ScalarAsync(connection, sql, ct);
    }

    /// <summary>What the fake target serves: the run the test is on, and a handle counter that never repeats.</summary>
    private sealed class InlineFakeTarget
    {
        public int Run { get; set; }

        private int _nextHandle;

        public int NextHandle() => Interlocked.Increment(ref _nextHandle);
    }

    private sealed class InlineFakeTargetProvider(InlineFakeTarget state) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new InlineFakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new InlineFakeCommand(query.Text, (InlineFakeConnection)connection, state);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class InlineFakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;

        public string DatabaseName => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "app";

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

    private sealed class InlineFakeCommand(string text, InlineFakeConnection owner, InlineFakeTarget state) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = text;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get => owner; set { } }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            /* The server-scoped path reads statement text in a second query. It gets none: the rows keep the text of the main read. */
            if (CommandText.Contains("sys.dm_exec_sql_text(k.plan_handle)", StringComparison.Ordinal))
            {
                var texts = new DataTable();
                texts.Columns.Add("ord", typeof(int));
                texts.Columns.Add("text", typeof(string));
                return Task.FromResult<DbDataReader>(texts.CreateDataReader());
            }

            return Task.FromResult<DbDataReader>(MainRead(owner.DatabaseName, state));
        }
    }

    /// <summary>
    /// The main read in the 46-column shape of the inline capture: 44 candidate columns, then the plan and its size. Every ad hoc
    /// execution is a new plan_handle whose plan carries its own literal. Procedures keep their handle and their plan.
    /// </summary>
    private static DbDataReader MainRead(string database, InlineFakeTarget state)
    {
        var table = new DataTable();
        var stringColumns = new HashSet<int> { 0, 1, 2, 36, 37, 38, 42, 44 };
        for (var i = 0; i < 46; i++)
        {
            var type = stringColumns.Contains(i) ? typeof(string)
                : i is 3 or 4 ? typeof(DateTime)
                : i is 40 or 41 or 43 ? typeof(int)
                : typeof(long);
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), type);
        }

        var now = DateTime.UtcNow;
        var created = new DateTime(2031, 4, 10, 11, 0, 0, DateTimeKind.Utc);
        var executions = state.Run + 1L;

        void AddRow(string handlePrefix, int handle, string queryHash, string planHash, string queryText)
        {
            var values = new object?[46];
            for (var i = 0; i < 46; i++)
            {
                values[i] = table.Columns[i].DataType == typeof(long) ? 0L : DBNull.Value;
            }

            var hex = handle.ToString("X8", CultureInfo.InvariantCulture);
            var plan = PlanFor(new QueryStatsCollector.Row { QueryHash = queryHash, QueryPlanHash = planHash, QueryText = queryText });
            values[0] = database;
            values[1] = queryHash;
            values[2] = planHash;
            values[3] = created;
            values[4] = now;
            values[5] = executions;
            values[6] = executions * 1000L;
            values[7] = executions * 2000L;
            values[36] = "0x02" + hex;
            values[37] = handlePrefix + hex;
            values[38] = queryText;
            values[39] = 1L;
            values[40] = 0;
            values[41] = -1;
            values[43] = 3600;
            values[44] = plan;
            values[45] = plan.Length * 2L;
            table.Rows.Add(values);
        }

        /* Handles are unique across databases, so the delta keys of two databases never meet. */
        var dbSalt = database == "alpha" ? 1 : database == "zeta" ? 2 : 0;
        for (var v = 0; v < InlineVariantsPerShape; v++)
        {
            for (var s = 0; s < Shapes; s++)
            {
                var n = state.NextHandle();
                AddRow(
                    "0x06", n,
                    "0xA" + s.ToString("X15", CultureInfo.InvariantCulture),
                    "0xB" + s.ToString("X15", CultureInfo.InvariantCulture),
                    "SELECT * FROM dbo.t" + s.ToString(CultureInfo.InvariantCulture) + " WHERE id = " + n.ToString(CultureInfo.InvariantCulture));
            }
        }

        for (var p = 0; p < InlineProcedures; p++)
        {
            AddRow(
                "0x05", dbSalt * 1000 + p,
                "0xC" + p.ToString("X15", CultureInfo.InvariantCulture),
                "0xD" + p.ToString("X15", CultureInfo.InvariantCulture),
                "SELECT * FROM " + database + ".dbo.p" + p.ToString(CultureInfo.InvariantCulture) + " WHERE id = @id");
        }

        return table.CreateDataReader();
    }
}
