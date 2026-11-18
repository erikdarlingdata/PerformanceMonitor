/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
                var fetchedByShape = new Dictionary<QueryStatsPlanShapeKey, (string PlanXml, long? Bytes)>();
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
}
