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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter), the end-to-end proof for the Darling store: a new row holds the marker, and an old row
/// reads back as the marker without being changed.
///
/// <para><b>Collect.</b> The canary goes in as the rows a monitored server would return (a fake target provider
/// hands the runner a reader over a table), and <see cref="DarlingCollectorRunner.RunAsync"/> runs the real
/// definition: read, filter, delta, dedupe, write. The stored row is then read straight from the rig store.
/// query_stats and deadlocks take the Azure per-database path of the runner, and query_snapshots the server-wide
/// path, so both loops are covered.</para>
///
/// <para><b>Read.</b> Rows are planted straight into the tables, as rows stored before the upgrade would be. The tool
/// reads RAW (the control: the canary must still be there, so the plant reached the output), then through the
/// host's real filter list (the MCP read) and through the web read's result writer (<c>/api/read/*</c>). Both must
/// show the marker and no secret needle. The planted table's bytes are hashed before the reads and after: a read
/// never rewrites a stored row.</para>
///
/// <para><b>#1776 own-store</b> - every row is keyed by this class's own fake server ids and removed in the cleanup.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StatementScrubEndToEndLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the statement filter end-to-end tests.";
    private const string Db = "SsfE2eDb";
    private static readonly DateTime Anchor = new(2031, 6, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private const int CollectStatsId = -495101;
    private const int CollectSnapshotsId = -495102;
    private const int CollectDeadlocksId = -495103;
    private const int ReadStatsId = -495111;
    private const int ReadSnapshotsId = -495112;
    private const int ReadDeadlocksId = -495113;

    private static readonly int[] AllIds =
    {
        CollectStatsId, CollectSnapshotsId, CollectDeadlocksId, ReadStatsId, ReadSnapshotsId, ReadDeadlocksId,
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string NameOf(int id) => "darling-ssf-e2e" + id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    // ── collect ──

    [Fact]
    public async Task QueryStats_TheCanaryThroughTheRunner_IsStoredAsTheMarker_ThePlainStatementIsKept_AndThePlanIsFiltered()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator(), null, capturePlans: () => true);
            runner.TargetProviderOverrideForTests = _ => new FakeTargetProvider(_ => QueryStatsReader());
            runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });

            await runner.RunAsync(QueryStatsCollector.Instance, Server(CollectStatsId, azure: true), ct);

            var texts = await ScalarListAsync(connection,
                "SELECT query_hash || '|' || COALESCE(query_text, '<null>') FROM v_query_stats WHERE server_id = " + CollectStatsId + " ORDER BY query_hash", ct);
            Assert.Equal(new[] { "0xC0|" + StatementFilterCensus.Marker, "0xP0|" + StatementScrubCanary.PlainStatement }, texts);

            string? plan = await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(postgres, CollectStatsId, "0xC0", null, ct);
            Assert.False(string.IsNullOrEmpty(plan), "the canary row's stored plan did not resolve, so the plan check proves nothing");
            StatementFilterCensus.AssertPlanFilteredKeepsTheRest(StatementScrubCanary.CanaryPlan(), plan!);
            AssertNoSecret(await WholeTableTextAsync(connection, "query_stats", CollectStatsId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task QuerySnapshots_TheCanaryThroughTheDefinitionAndTheHostWriter_IsStoredAsTheMarker_AndBothPlansAreFiltered()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            /* The runner's server-wide path opens a real SQL Server connection (CreateTargetConnection, no seam), so this
               case drives the same definition and the same writer the runner calls: ReadAsync over the rows the server
               would return, then the host's COPY with its payload dimensions, in one transaction. */
            var context = new CollectorContext
            {
                ServerId = CollectSnapshotsId,
                ServerName = NameOf(CollectSnapshotsId),
                CollectionTime = Anchor,
                Deltas = new NoDeltas(),
                Target = new CollectorTargetInfo(),
            };
            await using (var reader = SnapshotsReader())
            {
                var read = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, ct);
                await WriteBatchAsync(connection, QuerySnapshotsCollector.Instance, context, read, ct);
            }

            var rows = await ScalarListAsync(connection,
                "SELECT session_id::text || '|' || COALESCE(query_text, '<null>') FROM query_snapshots WHERE server_id = " + CollectSnapshotsId + " ORDER BY session_id", ct);
            Assert.Equal(new[] { "81|" + StatementFilterCensus.Marker, "82|" + StatementScrubCanary.PlainStatement }, rows);

            foreach (string column in new[] { "query_plan", "live_query_plan" })
            {
                var plans = await ScalarListAsync(connection,
                    "SELECT " + column + " FROM query_snapshots WHERE server_id = " + CollectSnapshotsId + " AND session_id = 81 AND " + column + " IS NOT NULL", ct);
                string plan = Assert.Single(plans);
                StatementFilterCensus.AssertPlanFilteredKeepsTheRest(StatementScrubCanary.CanaryPlan(), plan);
            }

            AssertNoSecret(await WholeTableTextAsync(connection, "query_snapshots", CollectSnapshotsId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Deadlocks_TheCanaryThroughTheRunner_IsStoredAsTheMarker_InTheVictimTextAndTheGraph()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
            bool withPlan = runner.ShouldCapturePlanXmlFor(DeadlocksCollector.Instance.Name, CollectDeadlocksId);
            runner.TargetProviderOverrideForTests = _ => new FakeTargetProvider(_ => DeadlocksReader(withPlan));
            runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });

            await runner.RunAsync(DeadlocksCollector.Instance, Server(CollectDeadlocksId, azure: true), ct);

            var victims = await ScalarListAsync(connection,
                "SELECT COALESCE(victim_sql_text, '<null>') FROM deadlocks WHERE server_id = " + CollectDeadlocksId + " ORDER BY deadlock_time", ct);
            Assert.Equal(new[] { StatementFilterCensus.Marker, StatementScrubCanary.PlainStatement }, victims);

            var graphs = await ScalarListAsync(connection,
                "SELECT deadlock_graph_xml FROM deadlocks WHERE server_id = " + CollectDeadlocksId + " ORDER BY deadlock_time", ct);
            foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, graphs[0], StringComparison.Ordinal);
            Assert.Contains(StatementFilterCensus.Marker, graphs[0], StringComparison.Ordinal);
            Assert.Contains(StatementScrubCanary.PlainStatement, graphs[1], StringComparison.Ordinal);

            AssertNoSecret(await WholeTableTextAsync(connection, "deadlocks", CollectDeadlocksId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    // ── read ──

    [Fact]
    public async Task QueryStats_ARowStoredBeforeTheUpgrade_ReadsBackAsTheMarker_OnTheMcpAndTheWebRead_AndTheStoredRowIsUnchanged()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        using var host = await StatementFilterCensus.BuildHostAsync();
        var bodySucceeded = false;
        try
        {
            string server = NameOf(ReadStatsId);
            await DarlingMcpTestData.RegisterServerAsync(connection, ReadStatsId, server, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var seeds = new[] { (StatementScrubCanary.CanaryStatement, "C", StatementScrubCanary.CanaryPlan()), (StatementScrubCanary.PlainStatement, "P", (string?)null) };
            for (int i = 0; i < seeds.Length; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, query_plan_xml,
                                       delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30 - i)), ReadStatsId, server, Db,
                    "0xQH" + seeds[i].Item2, "0xPLAN" + seeds[i].Item2, "0xSQLH" + seeds[i].Item2, "0xPLANH" + seeds[i].Item2,
                    seeds[i].Item1, seeds[i].Item3,
                    10L, 1_000_000L, 2_000_000L, 5_000L, 1, 1, 60);
            }

            string before = await TableHashAsync(connection, "query_stats", ReadStatsId, ct);

            string raw = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, server, cancellationToken: ct);
            Assert.Contains("S3cret-canary-ssf", raw, StringComparison.Ordinal);
            await AssertMcpAndWebAsync(host, "get_top_queries_by_cpu", raw);

            string rawPlan = await DarlingMcpPlanTools.GetPlanXml(postgres, "0xQHC", server, cancellationToken: ct);
            string filteredPlan = (await StatementFilterCensus.FilterThroughHostAsync(host, rawPlan)).Text;
            foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, filteredPlan, StringComparison.Ordinal);
            Assert.Contains(StatementFilterCensus.Marker, filteredPlan, StringComparison.Ordinal);
            Assert.Contains("canary_plain_ssf", filteredPlan, StringComparison.Ordinal);
            var (planStatus, planBody) = await WebReadAsync("get_plan_xml", rawPlan);
            Assert.Equal(200, planStatus);
            foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, planBody, StringComparison.Ordinal);
            Assert.Contains(StatementFilterCensus.Marker, planBody, StringComparison.Ordinal);

            Assert.Equal(before, await TableHashAsync(connection, "query_stats", ReadStatsId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task QuerySnapshots_ARowStoredBeforeTheUpgrade_ReadsBackAsTheMarker_OnTheMcpAndTheWebRead_AndTheStoredRowIsUnchanged()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        using var host = await StatementFilterCensus.BuildHostAsync();
        var bodySucceeded = false;
        try
        {
            string server = NameOf(ReadSnapshotsId);
            await DarlingMcpTestData.RegisterServerAsync(connection, ReadSnapshotsId, server, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var texts = new[] { StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement };
            for (int i = 0; i < texts.Length; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id, wait_resource, percent_complete, query_hash)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-5 - i)), ReadSnapshotsId, server, 100 + i, Db,
                    "00 00:00:05.125", texts[i], "running", 0, null, 0L, 1000L, 1500L, 10_000L, 50L, 7L, 0m,
                    "Read Committed", 1, 0, "app_svc", "APPSRV01", "MyOrderService.Worker", 0, 0,
                    "KEY: 5:72057594043432960 (a1b2c3d4e5f6)", 12.5m, "0x" + (0x1A2B3C4D5E6F7080L + i).ToString("X16"));
            }

            string before = await TableHashAsync(connection, "query_snapshots", ReadSnapshotsId, ct);

            string raw = await DarlingMcpSessionTools.GetActiveQueries(postgres, server, cancellationToken: ct);
            Assert.Contains("S3cret-canary-ssf", raw, StringComparison.Ordinal);
            await AssertMcpAndWebAsync(host, "get_active_queries", raw);

            Assert.Equal(before, await TableHashAsync(connection, "query_snapshots", ReadSnapshotsId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Deadlocks_ARowStoredBeforeTheUpgrade_ReadsBackAsTheMarker_OnTheMcpAndTheWebRead_AndTheStoredRowIsUnchanged()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        using var host = await StatementFilterCensus.BuildHostAsync();
        var bodySucceeded = false;
        try
        {
            string server = NameOf(ReadDeadlocksId);
            await DarlingMcpTestData.RegisterServerAsync(connection, ReadDeadlocksId, server, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, victim_process_id, victim_sql_text, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-5)), ReadDeadlocksId, server,
                DarlingMcpTestData.Naive(now.AddMinutes(-6)), Graph(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement),
                "p1", StatementScrubCanary.CanaryStatement, Db);

            string before = await TableHashAsync(connection, "deadlocks", ReadDeadlocksId, ct);

            string raw = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, server, full_graph: true, cancellationToken: ct);
            Assert.Contains("S3cret-canary-ssf", raw, StringComparison.Ordinal);
            await AssertMcpAndWebAsync(host, "get_deadlock_detail", raw);

            Assert.Equal(before, await TableHashAsync(connection, "deadlocks", ReadDeadlocksId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    // ── plumbing ──

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

    private static async Task WriteBatchAsync<TRow>(
        NpgsqlConnection connection, ICollectorDefinition<TRow> definition, CollectorContext context, IReadOnlyList<TRow> rows, CancellationToken ct)
    {
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
                writer.Value(context.CollectionTime).Value(context.ServerId).Value(context.ServerName);
                writer.BeginPayload();
                definition.WritePayload(row, writer, context);
                writer.EndPayload(definition.PayloadColumns.Count);
            }

            await importer.CompleteAsync(ct);
        }

        await PayloadDimensionWriter.FlushAsync(connection, transaction, dimensions, context.CollectionTime, ct);
        await transaction.CommitAsync(ct);
    }

    private static async Task AssertMcpAndWebAsync(Microsoft.AspNetCore.TestHost.TestServer host, string tool, string raw)
    {
        string filtered = (await StatementFilterCensus.FilterThroughHostAsync(host, raw)).Text;
        try
        {
            StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, filtered);
        }
        catch (Exception ex)
        {
            throw new Xunit.Sdk.XunitException(tool + " (MCP): " + ex.Message);
        }

        var (status, body) = await WebReadAsync(tool, raw);
        Assert.Equal(200, status);
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, body, StringComparison.Ordinal);
        Assert.Contains(StatementFilterCensus.Marker, body, StringComparison.Ordinal);
        Assert.Contains("canary_plain_ssf", body, StringComparison.Ordinal);
    }

    private static async Task<(int Status, string Body)> WebReadAsync(string tool, string raw)
    {
        var result = DarlingWebEndpoints.ToHttpResult(raw, "/api/read/" + tool, NullLogger.Instance, 1);
        var context = new DefaultHttpContext { RequestServices = new ServiceCollection().AddLogging().BuildServiceProvider() };
        context.Response.Body = new MemoryStream();
        await result.ExecuteAsync(context);
        context.Response.Body.Position = 0;
        return (context.Response.StatusCode, Encoding.UTF8.GetString(((MemoryStream)context.Response.Body).ToArray()));
    }

    private static void AssertNoSecret(string text)
    {
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
    }

    private static string Graph(string victimText, string otherText) =>
        "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
        + "<process id=\"p1\" spid=\"55\" waittime=\"100\" lockMode=\"X\" currentdbname=\"" + Db + "\"><executionStack/><inputbuf>"
        + System.Security.SecurityElement.Escape(victimText) + "</inputbuf></process>"
        + "<process id=\"p2\" spid=\"56\" waittime=\"200\" lockMode=\"S\" currentdbname=\"" + Db + "\"><executionStack/><inputbuf>"
        + System.Security.SecurityElement.Escape(otherText) + "</inputbuf></process>"
        + "</process-list><resource-list/></deadlock>";

    private static ServerRuntime Server(int id, bool azure) => new()
    {
        Config = new MonitoredServer { Name = NameOf(id), Host = "h" },
        ConnectionString = "Server=azure;Database=master",
        Target = azure
            ? new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer }
            : new CollectorTargetInfo { SqlMajorVersion = 15, Engine = CollectorTargetEngine.SqlServer },
        StorageName = NameOf(id),
        ServerId = id,
    };

    private static async Task<NpgsqlConnection> OpenAsync(string cs, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        return connection;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (int id in AllIds)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                $"DELETE FROM query_snapshots WHERE server_id = {id}; DELETE FROM query_stats WHERE server_id = {id}; " +
                $"DELETE FROM deadlocks WHERE server_id = {id}; DELETE FROM collector_state WHERE server_id = {id}; " +
                $"DELETE FROM servers WHERE server_id = {id}; DELETE FROM config_monitored_servers WHERE server_id = {id};");
        }
    }

    private static async Task<List<string>> ScalarListAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var list = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(reader.IsDBNull(0) ? "<null>" : reader.GetString(0));
        }

        return list;
    }

    /// <summary>Every column of every row of one server in a table, as one text, for a whole-row secret scan.</summary>
    private static async Task<string> WholeTableTextAsync(NpgsqlConnection connection, string table, int serverId, CancellationToken ct) =>
        string.Concat(await ScalarListAsync(connection, $"SELECT t::text FROM {table} t WHERE server_id = {serverId}", ct));

    /// <summary>A hash of the bytes of every row of one server in a table: equal before and after means no read rewrote a row.</summary>
    private static async Task<string> TableHashAsync(NpgsqlConnection connection, string table, int serverId, CancellationToken ct)
    {
        var hash = await ScalarListAsync(connection,
            $"SELECT md5(COALESCE(string_agg(t::text, E'\\n' ORDER BY t::text), '')) || '|' || count(*)::text FROM {table} t WHERE server_id = {serverId}", ct);
        return Assert.Single(hash);
    }

    // ── what the monitored server hands back ──

    private static Type QueryStatsColumnType(int ordinal) => ordinal switch
    {
        3 or 4 => typeof(DateTime),
        0 or 1 or 2 or 36 or 37 or 38 or 42 => typeof(string),
        40 or 41 or 43 => typeof(int),
        _ => typeof(long),
    };

    private static DbDataReader QueryStatsReader()
    {
        var table = new DataTable();
        for (var i = 0; i < 44; i++) table.Columns.Add("c" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), QueryStatsColumnType(i));
        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        foreach (var (hash, text, plan) in new[]
        {
            ("0xC0", StatementScrubCanary.CanaryStatement, StatementScrubCanary.CanaryPlan()),
            ("0xP0", StatementScrubCanary.PlainStatement, (string?)null),
        })
        {
            var values = new object[table.Columns.Count];
            for (var i = 0; i < values.Length; i++) values[i] = DBNull.Value;
            values[1] = hash;
            values[36] = hash + "A";
            values[37] = hash + "B";
            values[38] = text;
            values[44] = (object?)plan ?? DBNull.Value;
            values[45] = (long)(plan?.Length ?? 0);
            table.Rows.Add(values);
        }

        return table.CreateDataReader();
    }

    private static DbDataReader SnapshotsReader()
    {
        var table = new DataTable();
        var types = new Type[35];
        for (var i = 0; i < types.Length; i++) types[i] = typeof(string);
        foreach (int i in new[] { 0, 7, 18, 19, 23, 34 }) types[i] = typeof(int);
        foreach (int i in new[] { 9, 11, 12, 13, 14, 15 }) types[i] = typeof(long);
        foreach (int i in new[] { 16, 24 }) types[i] = typeof(decimal);
        types[25] = typeof(bool);
        foreach (int i in new[] { 27, 28, 29, 30, 31, 32 }) types[i] = typeof(double);
        types[33] = typeof(DateTime);
        for (var i = 0; i < types.Length; i++) table.Columns.Add("c" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), types[i]);

        foreach (var (session, text, plan) in new[]
        {
            (81, StatementScrubCanary.CanaryStatement, StatementScrubCanary.CanaryPlan()),
            (82, StatementScrubCanary.PlainStatement, (string?)null),
        })
        {
            var values = new object[types.Length];
            for (var i = 0; i < values.Length; i++) values[i] = DBNull.Value;
            values[0] = session;
            values[1] = Db;
            values[3] = text;
            values[4] = (object?)plan ?? DBNull.Value;
            values[5] = (object?)plan ?? DBNull.Value;
            values[6] = "running";
            values[26] = "0x" + session.ToString(System.Globalization.CultureInfo.InvariantCulture);
            table.Rows.Add(values);
        }

        return table.CreateDataReader();
    }

    private static DbDataReader DeadlocksReader(bool withPlan)
    {
        var data = new DataSet();
        var rows = new DataTable();
        rows.Columns.Add("time", typeof(DateTime));
        rows.Columns.Add("victim", typeof(string));
        rows.Columns.Add("graph", typeof(string));
        if (withPlan) rows.Columns.Add("plan", typeof(string));
        rows.Columns.Add("source", typeof(string));
        foreach (var (minute, victimText, otherText) in new[]
        {
            (1, StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement),
            (2, StatementScrubCanary.PlainStatement, StatementScrubCanary.PlainStatement),
        })
        {
            var values = new List<object> { Anchor.AddMinutes(minute), "p1", Graph(victimText, otherText) };
            if (withPlan) values.Add(minute == 1 ? StatementScrubCanary.CanaryPlan() : DBNull.Value);
            values.Add(DBNull.Value);
            rows.Rows.Add(values.ToArray());
        }

        var gate = new DataTable();
        gate.Columns.Add("count", typeof(long));
        gate.Columns.Add("gated", typeof(bool));
        gate.Rows.Add(100L, false);
        data.Tables.Add(rows);
        data.Tables.Add(gate);
        return data.CreateDataReader();
    }

    private sealed class FakeTargetProvider(Func<string, DbDataReader> reader) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand(((FakeConnection)connection).Database1, reader);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;
        public string Database1 => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "master";

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

    private sealed class FakeCommand(string database, Func<string, DbDataReader> reader) : DbCommand
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
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => reader(database);
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult(reader(database));
    }
}
