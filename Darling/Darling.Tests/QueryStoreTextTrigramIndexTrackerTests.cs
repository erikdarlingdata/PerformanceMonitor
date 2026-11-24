/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the Query Store text index's option and its process-wide tracker. The tracker is read by get_store_metrics and by the Query Store
/// source's stopped advice from process start, so until the option is known the answer says so, never "the option is off". In
/// "darling-config-env" because two tests start the real worker or set <see cref="QueryStoreTextTrigramIndex.SharedTracker"/>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The one live test creates its own scratch database through
   ScratchPostgres and reads and writes only inside it, so it cannot race live collection. The class is in "darling-config-env" for the
   process-wide tracker and DARLING_CONFIG, which the live test also sets. */
[Collection("darling-config-env")]
public sealed class QueryStoreTextTrigramIndexTrackerTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 store metrics live test.";

    private static readonly TrigramCatalogState ValidIndex = new(
        TableExists: true, IndexValid: true, IndexSizeBytes: 1048576, ExtensionSchema: "public", ExtensionAvailable: true, BuildRunning: false);

    [Fact]
    public void AnUnconfiguredTracker_WithAValidIndex_IsNotDropPending_AndOnceConfiguredItClassifiesByTheOption()
    {
        var tracker = new QueryTextTrigramIndexTracker();
        Assert.Equal(TrigramIndexStatus.Unconfigured, QueryStoreTextTrigramIndex.Classify(ValidIndex, tracker.Read()));

        tracker.Configure(true);
        Assert.Equal(TrigramIndexStatus.Ready, QueryStoreTextTrigramIndex.Classify(ValidIndex, tracker.Read()));

        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        Assert.Equal(TrigramIndexStatus.DropPending, QueryStoreTextTrigramIndex.Classify(ValidIndex, off.Read()));
        Assert.Equal(
            TrigramIndexStatus.Off,
            QueryStoreTextTrigramIndex.Classify(ValidIndex with { IndexValid = null, IndexSizeBytes = null }, off.Read()));
    }

    [Fact]
    public void TheUnconfiguredNote_NeverSaysTheOptionIsOff()
    {
        var health = new TrigramIndexHealth(
            TrigramIndexStatus.Unconfigured, false, true, true, 1048576, false, null, true, true, 0, null, null, null, null, null);

        var note = QueryStoreTextTrigramIndex.Note(health);

        Assert.DoesNotContain("is off", note, StringComparison.Ordinal);
        Assert.DoesNotContain("drops", note, StringComparison.Ordinal);
        Assert.Contains("not read darling.json yet", note, StringComparison.Ordinal);
        Assert.Contains(QueryStoreTextTrigramIndex.OptionName, note, StringComparison.Ordinal);
    }

    /// <summary>The Query Store source's stopped advice: the database advice alone while the option is unknown, where to look when it is on,
    /// and the option's name when it is off.</summary>
    [Fact]
    public void TheStoppedAdvice_FollowsTheOption()
    {
        var unconfigured = new QueryTextTrigramIndexTracker();
        var on = new QueryTextTrigramIndexTracker();
        on.Configure(true);
        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);

        var plain = DarlingMcpFindQueriesByTextTools.QueryStoreStoppedAdvice(null, unconfigured.Read());
        var plainNamed = DarlingMcpFindQueriesByTextTools.QueryStoreStoppedAdvice("db_1", unconfigured.Read());
        Assert.Equal("Name a database with database_name to narrow the search; one server and one database is much faster.", plain);
        Assert.Equal("The database named is large; use a longer, more specific pattern.", plainNamed);

        var whenOn = DarlingMcpFindQueriesByTextTools.QueryStoreStoppedAdvice(null, on.Read());
        Assert.StartsWith(plain, whenOn, StringComparison.Ordinal);
        Assert.Contains("get_store_metrics query_store_text_index", whenOn, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreTextTrigramIndex.OptionName, whenOn, StringComparison.Ordinal);

        var whenOff = DarlingMcpFindQueriesByTextTools.QueryStoreStoppedAdvice("db_1", off.Read());
        Assert.StartsWith(plainNamed, whenOff, StringComparison.Ordinal);
        Assert.Contains(QueryStoreTextTrigramIndex.OptionName, whenOff, StringComparison.Ordinal);
        Assert.DoesNotContain("get_store_metrics", whenOff, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOption_DefaultsOff_AndReadsFromItsOwnKey()
    {
        Assert.False(DarlingConfig.Parse("{ }").QueryStoreTextTrigramIndexEnabled);

        var on = DarlingConfig.Parse("{ \"" + QueryStoreTextTrigramIndex.OptionName + "\": true }");
        Assert.True(on.QueryStoreTextTrigramIndexEnabled);
        Assert.False(on.QueryTextTrigramIndexEnabled);

        var planCacheOnly = DarlingConfig.Parse("{ \"queryTextTrigramIndexEnabled\": true }");
        Assert.True(planCacheOnly.QueryTextTrigramIndexEnabled);
        Assert.False(planCacheOnly.QueryStoreTextTrigramIndexEnabled);
    }

    /// <summary>
    /// The real worker, started on a config file that sets only this option and is refused at validation (no postgres section), before any
    /// store work. The shared tracker is the one the MCP host reads, and it carries the option by then; the plan-cache option stays off.
    /// </summary>
    [Fact]
    public async Task TheWorker_ConfiguresTheSharedTracker_TheMomentTheFileLoads_BeforeAnyStoreWork()
    {
        var dir = Directory.CreateTempSubdirectory("darling-qs-trigram-configure-").FullName;
        var path = Path.Combine(dir, "darling.json");
        File.WriteAllText(path, "{ \"" + QueryStoreTextTrigramIndex.OptionName + "\": true }");
        var previous = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Environment.SetEnvironmentVariable("DARLING_CONFIG", path);
        try
        {
            var worker = new DarlingWorker(
                NullLogger<DarlingWorker>.Instance,
                NullLoggerFactory.Instance,
                new McpRuntimeState(),
                new WebRuntimeState(),
                new MonitoredServerRegistryState(),
                new CollectorRuntimeState(),
                new WebTlsCertificateState(),
                new BaselineCache(),
                new ReadLatencyAccumulator());
            var method = typeof(DarlingWorker).GetMethod("ExecuteAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

            await (Task)method.Invoke(worker, [CancellationToken.None])!;

            var snapshot = QueryStoreTextTrigramIndex.SharedTracker.Read();
            Assert.True(snapshot.Configured);
            Assert.True(snapshot.Enabled);
            var planCache = QueryTextTrigramIndexTracker.Shared.Read();
            Assert.True(planCache.Configured);
            Assert.False(planCache.Enabled);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previous);
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>The block sits beside query_text_index on the reply of a store that has no self-metrics yet (status empty) and on the full
    /// reply, names this index, and reads Ready once the shared tracker's pass has built it.</summary>
    [Fact]
    public async Task GetStoreMetrics_CarriesTheQueryStoreTextIndexBlock_OnTheEmptyReplyAndOnTheFullReply()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        QueryStoreTextTrigramIndex.SharedTracker.Configure(false);
        try
        {
            using (var empty = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, cancellationToken: ct)))
            {
                Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
                Assert.True(empty.RootElement.TryGetProperty("query_text_index", out _));
                var block = empty.RootElement.GetProperty("query_store_text_index");
                Assert.Equal("Off", block.GetProperty("status").GetString());
                Assert.False(block.GetProperty("enabled").GetBoolean());
                Assert.False(block.GetProperty("exists").GetBoolean());
                Assert.Equal(QueryStoreTextTrigramIndex.IndexName, block.GetProperty("index_name").GetString());
                Assert.Equal(QueryStoreTextTrigramIndex.TableName, block.GetProperty("table").GetString());
                Assert.Contains(QueryStoreTextTrigramIndex.OptionName, block.GetProperty("note").GetString(), StringComparison.Ordinal);
            }

            await using (var c = new NpgsqlConnection(scratch.ConnectionString))
            {
                await c.OpenAsync(ct);
                await DarlingMcpTestData.ExecAsync(c, ct,
                    "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, enabled_server_count) VALUES (timezone('UTC', now()) - interval '5 minutes', 'darling', $1, 1000000, 1)",
                    StoreSelfMetrics.StoreObjectKind);
            }

            using (var full = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, cancellationToken: ct)))
            {
                Assert.Equal("summary", full.RootElement.GetProperty("view").GetString());
                var block = full.RootElement.GetProperty("query_store_text_index");
                Assert.Equal("Off", block.GetProperty("status").GetString());
                Assert.False(block.GetProperty("exists").GetBoolean());
            }

            QueryStoreTextTrigramIndex.SharedTracker.Configure(true);
            Assert.False(await QueryStoreTextTrigramIndex.RunPassAsync(
                data, true, QueryStoreTextTrigramIndex.SharedTracker, DateTime.UtcNow, NullLogger.Instance, ct));

            using (var ready = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, cancellationToken: ct)))
            {
                var block = ready.RootElement.GetProperty("query_store_text_index");
                Assert.Equal("Ready", block.GetProperty("status").GetString());
                Assert.True(block.GetProperty("enabled").GetBoolean());
                Assert.True(block.GetProperty("exists").GetBoolean());
                Assert.True(block.GetProperty("valid").GetBoolean());
                Assert.Contains("find_queries_by_text", block.GetProperty("note").GetString(), StringComparison.Ordinal);
                Assert.Equal(QueryStoreTextTrigramIndex.IndexName, block.GetProperty("index_name").GetString());
            }
        }
        finally
        {
            QueryStoreTextTrigramIndex.SharedTracker.Configure(false);
        }
    }
}
