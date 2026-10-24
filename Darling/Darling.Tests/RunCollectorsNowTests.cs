/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #5941: run all collectors now, or one collector now. The worker half drives the real <c>RunSnapshotAsync</c> with
/// <see cref="DarlingWorker.RunOneBodyOverride"/> standing in for the collector run; the guard half drives
/// <see cref="SnapshotNowRequests"/> over a fake store and a test clock (no sleeps); the web half drives the real route in a
/// test host with a read-only and an edit seat; the MCP half drives the tool body and the write policy.
/// </summary>
public sealed class RunCollectorsNowTests
{
    private const string SqlServerCollector = "wait_stats";
    private const string PostgresCollector = "pg_buffer_usage";

    // ------------------------------------------------------------------ worker

    private static DarlingWorker MakeWorker() =>
        new(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());

    private static DarlingWorker.ServerLoopState MakeServer(int serverId, CollectorTargetEngine engine = CollectorTargetEngine.SqlServer)
    {
        var host = $"run-now-test-{serverId}";
        var config = new MonitoredServer { Name = host, Host = host };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { Engine = engine },
            StorageName = host,
            ServerId = serverId,
        };
        return new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
    }

    private static List<string> Record(DarlingWorker worker)
    {
        var ran = new List<string>();
        worker.RunOneBodyOverride = (_, _, collector, _) =>
        {
            lock (ran)
            {
                ran.Add(collector);
            }

            return Task.FromResult(1);
        };
        return ran;
    }

    private static JsonElement Json(CommandOutcome outcome) => JsonDocument.Parse(outcome.ResultJson!).RootElement.Clone();

    [Fact]
    public async Task AnUnknownCollectorName_RefusesTheWholeCommand_AndNamesIt()
    {
        var worker = MakeWorker();
        var ran = Record(worker);
        var server = MakeServer(5941_1);

        var outcome = await worker.RunSnapshotAsync(
            [server], null!, server.Config.ServerId, TestContext.Current.CancellationToken, [SqlServerCollector, "not_a_collector"]);

        Assert.False(outcome.Success);
        Assert.Contains("not_a_collector", outcome.ResultJson, StringComparison.Ordinal);
        Assert.Empty(ran);
    }

    [Fact]
    public async Task ADisabledCollector_RunsWhenNamed_AndNotInRunAll_AndItsOverrideIsLeftAlone()
    {
        var worker = MakeWorker();
        var ran = Record(worker);
        var server = MakeServer(5941_2);
        var overrides = new[] { new ScheduleOverride(server.Runtime!.ServerId, SqlServerCollector, null, null, Enabled: false) };
        worker.ScheduleOverridesForTest = overrides;
        var ct = TestContext.Current.CancellationToken;

        await worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct);
        Assert.DoesNotContain(SqlServerCollector, ran);
        Assert.NotEmpty(ran);

        ran.Clear();
        var one = await worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct, [SqlServerCollector]);
        Assert.True(one.Success);
        Assert.Equal([SqlServerCollector], ran);
        Assert.Equal(1, Json(one).GetProperty("collectorsRun").GetInt32());
        Assert.Same(overrides, worker.ScheduleOverridesForTest);
        Assert.False(overrides[0].Enabled);
    }

    [Fact]
    public async Task ACollectorForAnotherEngine_IsSkippedWithTheReason_AndNeverRun()
    {
        var worker = MakeWorker();
        var ran = Record(worker);
        var server = MakeServer(5941_3);

        var outcome = await worker.RunSnapshotAsync(
            [server], null!, server.Config.ServerId, TestContext.Current.CancellationToken, [PostgresCollector]);

        Assert.True(outcome.Success);
        Assert.Empty(ran);
        var json = Json(outcome);
        Assert.Equal(0, json.GetProperty("collectorsRun").GetInt32());
        var skipped = Assert.Single(json.GetProperty("skipped").EnumerateArray());
        Assert.Equal(PostgresCollector, skipped.GetProperty("collector").GetString());
        Assert.Contains("engine", skipped.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASnapshot_WaitsForAPermitFromTheSweepGate_AndGivesItBack()
    {
        var worker = MakeWorker();
        var ran = Record(worker);
        var server = MakeServer(5941_4);
        using var gate = new SemaphoreSlim(1, 1);
        worker.SweepGate = gate;
        var ct = TestContext.Current.CancellationToken;

        await gate.WaitAsync(ct);
        var snapshot = worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct, [SqlServerCollector]);
        var finishedFirst = await Task.WhenAny(snapshot, Task.Delay(300, ct));
        Assert.NotSame(snapshot, finishedFirst);
        Assert.Empty(ran);

        gate.Release();
        var outcome = await snapshot;
        Assert.True(outcome.Success);
        Assert.Equal([SqlServerCollector], ran);
        Assert.Equal(1, gate.CurrentCount);
    }

    [Fact]
    public async Task AWaitingSnapshot_HoldsNoCollectionGate_SoTheScheduledPassIsNotBlocked_AndStartsNothingUntilAPermitIsFree()
    {
        var worker = MakeWorker();
        var ran = Record(worker);
        var server = MakeServer(5941_6);
        using var gate = new SemaphoreSlim(1, 1);
        worker.SweepGate = gate;
        var ct = TestContext.Current.CancellationToken;

        /* No fleet sweep permit is free: a scheduled pass holds the only one. */
        await gate.WaitAsync(ct);
        var snapshot = worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct, [SqlServerCollector]);
        Assert.NotSame(snapshot, await Task.WhenAny(snapshot, Task.Delay(300, ct)));
        Assert.Empty(ran);
        Assert.Equal(1, server.CollectionGate.CurrentCount);

        /* The scheduled pass holds its permit and only tries the collection gate; the request that is waiting must not be what
           stops it. */
        server.NextDue["latch_stats"] = DateTime.UtcNow.AddMinutes(-5);
        var scheduled = worker.RunDueCollectorsAsync(server, null!, ct);
        Assert.Same(scheduled, await Task.WhenAny(scheduled, Task.Delay(TimeSpan.FromSeconds(20), ct)));
        await scheduled;
        Assert.Equal(["latch_stats"], ran);
        Assert.False(snapshot.IsCompleted);

        gate.Release();
        var outcome = await snapshot;
        Assert.True(outcome.Success);
        Assert.Equal(["latch_stats", SqlServerCollector], ran);
        Assert.Equal(1, gate.CurrentCount);
        Assert.Equal(1, server.CollectionGate.CurrentCount);
    }

    [Fact]
    public async Task ADifferentlyCasedName_IsStoredAsTheCatalogSpelling_AndAnotherEnginesCollectorIsSkippedNeverRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new FakeStore();
        var accepted = await new SnapshotNowRequests(store, new ManualTime()).RequestAsync(7, ["PG_Buffer_Usage", " pg_buffer_usage "], "t", ct);
        Assert.True(accepted.Accepted);
        var queued = Assert.Single(store.Enqueued);
        Assert.Equal("{\"collectors\":[\"" + PostgresCollector + "\"]}", queued.Args);

        /* A row queued by hand with the wrong case reads back as the catalog spelling too. */
        Assert.True(SnapshotNowArgs.TryParse("{\"collectors\":[\"Wait_Stats\",\"PG_Buffer_Usage\"]}", out var parsed, out _));
        Assert.Equal([SqlServerCollector, PostgresCollector], parsed);

        var worker = MakeWorker();
        var ran = Record(worker);
        var server = MakeServer(5941_5);
        SnapshotNowArgs.TryParse("{\"collectors\":[\"PG_Buffer_Usage\"]}", out var names, out _);
        var outcome = await worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct, names);

        Assert.True(outcome.Success);
        Assert.Empty(ran);
        var skipped = Assert.Single(Json(outcome).GetProperty("skipped").EnumerateArray());
        Assert.Equal(PostgresCollector, skipped.GetProperty("collector").GetString());
        Assert.Contains("engine", skipped.GetProperty("reason").GetString(), StringComparison.Ordinal);

        /* The reviewer's case: a SQL Server collector, wrongly cased, against a PostgreSQL target. */
        var postgresServer = MakeServer(5941_7, CollectorTargetEngine.PostgreSql);
        SnapshotNowArgs.TryParse("{\"collectors\":[\"Wait_Stats\"]}", out var sqlServerNames, out _);
        var onPostgres = await worker.RunSnapshotAsync([postgresServer], null!, postgresServer.Config.ServerId, ct, sqlServerNames);

        Assert.True(onPostgres.Success);
        Assert.Empty(ran);
        Assert.Equal(0, Json(onPostgres).GetProperty("collectorsRun").GetInt32());
        Assert.Equal(SqlServerCollector, Assert.Single(Json(onPostgres).GetProperty("skipped").EnumerateArray()).GetProperty("collector").GetString());
    }

    [Fact]
    public void TheCommandPlan_RefusesAnUnknownCollector_NamingIt_AndAcceptsAKnownList()
    {
        var bad = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(1, "snapshot_now", 7, "{\"collectors\":[\"nope_nope\"]}", "t"));
        Assert.Equal(CommandKind.Fail, bad.Kind);
        Assert.Contains("nope_nope", bad.FailReason, StringComparison.Ordinal);

        var good = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(2, "snapshot_now", 7, $"{{\"collectors\":[\"{SqlServerCollector}\"]}}", "t"));
        Assert.Equal(CommandKind.Snapshot, good.Kind);
        Assert.Equal(CommandKind.Snapshot, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(3, "snapshot_now", 7, null, "t")).Kind);
    }

    // ------------------------------------------------------------------ guards

    private sealed class ManualTime : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 10, 16, 12, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    private sealed class FakeStore : ISnapshotNowStore
    {
        public SnapshotCommandRow? Latest { get; set; }

        public SnapshotCommandState? State { get; set; }

        public List<(int ServerId, string? Args, string By)> Enqueued { get; } = [];

        public Task<SnapshotCommandRow?> ReadLatestAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(Latest);

        public Exception? EnqueueFails { get; set; }

        public Task<long> EnqueueAsync(int serverId, string? argsJson, string requestedBy, CancellationToken cancellationToken)
        {
            if (EnqueueFails is not null)
            {
                throw EnqueueFails;
            }

            Enqueued.Add((serverId, argsJson, requestedBy));
            return Task.FromResult(100L + Enqueued.Count);
        }

        public Task<SnapshotCommandState?> ReadStateAsync(long commandId, CancellationToken cancellationToken) => Task.FromResult(State);
    }

    [Fact]
    public async Task AStoreRefusalOfTheRequestItself_Dl400_IsARefusedOutcome_WithTheStoresSentence_NotAnException()
    {
        var store = new FakeStore
        {
            EnqueueFails = new Npgsql.PostgresException("This request must run at the READ COMMITTED isolation level.", "ERROR", "ERROR", "DL400"),
        };

        var outcome = await new SnapshotNowRequests(store, new ManualTime()).RequestAsync(7, null, "t", TestContext.Current.CancellationToken);

        Assert.False(outcome.Accepted);
        Assert.Equal(SnapshotRequestOutcome.BadRequestCode, outcome.Code);
        Assert.Contains("READ COMMITTED", outcome.Text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("in_progress")]
    public async Task ASecondRequest_IsRefused_WhileOneIsQueuedOrRunning(string status)
    {
        var clock = new ManualTime();
        var store = new FakeStore { Latest = new SnapshotCommandRow(9, clock.GetUtcNow().UtcDateTime.AddMinutes(-30), status) };

        var outcome = await new SnapshotNowRequests(store, clock).RequestAsync(7, null, "t", TestContext.Current.CancellationToken);

        Assert.False(outcome.Accepted);
        Assert.Equal(SnapshotRequestOutcome.InFlightCode, outcome.Code);
        Assert.Empty(store.Enqueued);
    }

    [Fact]
    public async Task ARequestWithin60Seconds_IsRefused_SayingWhenTheNextIsAllowed_AndAllowedAfter()
    {
        var clock = new ManualTime();
        var started = clock.GetUtcNow().UtcDateTime;
        var store = new FakeStore { Latest = new SnapshotCommandRow(9, started, "succeeded") };
        var requests = new SnapshotNowRequests(store, clock);
        var ct = TestContext.Current.CancellationToken;

        clock.Advance(TimeSpan.FromSeconds(59));
        var tooSoon = await requests.RequestAsync(7, null, "t", ct);
        Assert.False(tooSoon.Accepted);
        Assert.Equal(SnapshotRequestOutcome.RateLimitedCode, tooSoon.Code);
        Assert.Equal(started.AddSeconds(60), tooSoon.NextAllowedUtc);
        Assert.Contains("12:01:00 UTC", tooSoon.Text, StringComparison.Ordinal);
        Assert.Empty(store.Enqueued);

        clock.Advance(TimeSpan.FromSeconds(2));
        var allowed = await requests.RequestAsync(7, [SqlServerCollector], "t", ct);
        Assert.True(allowed.Accepted);
        var queued = Assert.Single(store.Enqueued);
        Assert.Equal(7, queued.ServerId);
        Assert.Contains(SqlServerCollector, queued.Args, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUnknownCollectorName_IsRefusedBeforeAnythingIsQueued()
    {
        var store = new FakeStore();

        var outcome = await new SnapshotNowRequests(store, new ManualTime()).RequestAsync(7, ["nope_nope"], "t", TestContext.Current.CancellationToken);

        Assert.False(outcome.Accepted);
        Assert.Equal(SnapshotRequestOutcome.UnknownCollectorCode, outcome.Code);
        Assert.Contains("nope_nope", outcome.Text, StringComparison.Ordinal);
        Assert.Empty(store.Enqueued);
    }

    // ------------------------------------------------------------------ web

    private static async Task<WebApplication> StartAsync(
        FakeStore store, bool canEdit, ManualTime clock, Func<string, CancellationToken, Task<(int ServerId, string? Error)>>? resolver = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var seat = new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat(canEdit ? "ed" : "vi", canEdit);
        app.Use((context, next) =>
        {
            context.Items[PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat.HttpContextItemKey] = seat;
            return next(context);
        });
        DarlingWebEndpoints.MapCollectorRuns(
            app, store, NullLogger.Instance,
            resolver ?? ((name, _) => Task.FromResult(name == "known" ? (7, (string?)null) : (0, "No server matches."))), clock);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }

    private static async Task<(HttpStatusCode Status, string Body)> PostAsync(HttpClient client, string body)
    {
        var ct = TestContext.Current.CancellationToken;
        using var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/api/collectors/run", content, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    [Fact]
    public async Task TheWebRoute_Gives403ToAReadOnlySeat_AndQueuesForAnEditSeat()
    {
        var clock = new ManualTime();
        var viewerStore = new FakeStore();
        await using (var viewerApp = await StartAsync(viewerStore, canEdit: false, clock))
        {
            using var client = viewerApp.GetTestClient();
            var refused = await PostAsync(client, "{\"server\":\"known\"}");
            Assert.Equal(HttpStatusCode.Forbidden, refused.Status);
            Assert.Empty(viewerStore.Enqueued);
        }

        var editorStore = new FakeStore();
        await using var editorApp = await StartAsync(editorStore, canEdit: true, clock);
        using var editor = editorApp.GetTestClient();

        var queued = await PostAsync(editor, "{\"server\":\"known\",\"collectors\":[\"" + SqlServerCollector + "\"]}");
        Assert.Equal(HttpStatusCode.Accepted, queued.Status);
        Assert.Equal(101, JsonDocument.Parse(queued.Body).RootElement.GetProperty("command_id").GetInt64());
        Assert.Equal(("ed"), Assert.Single(editorStore.Enqueued).By);

        var unknownCollector = await PostAsync(editor, "{\"server\":\"known\",\"collectors\":[\"nope_nope\"]}");
        Assert.Equal(HttpStatusCode.BadRequest, unknownCollector.Status);
        Assert.Contains("nope_nope", unknownCollector.Body, StringComparison.Ordinal);

        var unknownServer = await PostAsync(editor, "{\"server\":\"other\"}");
        Assert.Equal(HttpStatusCode.NotFound, unknownServer.Status);

        editorStore.Latest = new SnapshotCommandRow(101, clock.GetUtcNow().UtcDateTime, "succeeded");
        var tooSoon = await PostAsync(editor, "{\"server\":\"known\"}");
        Assert.Equal(HttpStatusCode.TooManyRequests, tooSoon.Status);

        editorStore.Latest = new SnapshotCommandRow(101, clock.GetUtcNow().UtcDateTime, "in_progress");
        var running = await PostAsync(editor, "{\"server\":\"known\"}");
        Assert.Equal(HttpStatusCode.Conflict, running.Status);
    }

    /// <summary>The route over the resolver's real answers: the registry listing through <c>ResolveOrError</c> for a miss or an
    /// ambiguous name, and a registry read that fails through <c>ResolveOrErrorAsync</c> on a store nothing listens on.</summary>
    [Fact]
    public async Task TheWebRoute_AnswersEachResolverOutcome_ItsOwnStatus_WithAPlainMessage()
    {
        var registry = new List<RegisteredServer>
        {
            new(1, "alpha-one", null),
            new(2, "alpha-two", null),
        };
        var store = new FakeStore();

        await using (var app = await StartAsync(
            store, canEdit: true, new ManualTime(),
            (name, _) =>
            {
                var (resolved, error) = DarlingServerResolver.ResolveOrError(registry, name);
                return Task.FromResult((resolved.ServerId, error));
            }))
        {
            using var client = app.GetTestClient();

            var miss = await PostAsync(client, "{\"server\":\"nothing-like-it\"}");
            Assert.Equal(HttpStatusCode.NotFound, miss.Status);
            Assert.StartsWith("{\"error\":\"Could not resolve server.", miss.Body, StringComparison.Ordinal);
            Assert.DoesNotContain("\"invalid\"", miss.Body, StringComparison.Ordinal);

            var ambiguous = await PostAsync(client, "{\"server\":\"alpha\"}");
            Assert.Equal(HttpStatusCode.BadRequest, ambiguous.Status);
            Assert.Contains("matches 2 monitored servers", ambiguous.Body, StringComparison.Ordinal);
            Assert.DoesNotContain("\"invalid\"", ambiguous.Body, StringComparison.Ordinal);

            Assert.Empty(store.Enqueued);
        }

        /* Nothing listens on this port, so the real resolver's registry read fails: a store fault, not an unknown server. */
        await using var unreachable = new NpgsqlDataSourceBuilder("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=3;Command Timeout=3").Build();
        await using (var faultApp = await StartAsync(
            store, canEdit: true, new ManualTime(),
            async (name, token) =>
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(unreachable, name, token);
                return (resolved.ServerId, error);
            }))
        {
            using var client = faultApp.GetTestClient();

            var fault = await PostAsync(client, "{\"server\":\"known\"}");
            Assert.True(fault.Status is HttpStatusCode.InternalServerError or HttpStatusCode.ServiceUnavailable, fault.Status.ToString());
            Assert.DoesNotContain("127.0.0.1", fault.Body, StringComparison.Ordinal);
            Assert.DoesNotContain("registry", fault.Body, StringComparison.OrdinalIgnoreCase);
            Assert.Empty(store.Enqueued);
        }
    }

    [Fact]
    public async Task TheWebRoute_ReturnsTheServicesResult_OnceTheCommandHasFinished()
    {
        var store = new FakeStore
        {
            State = new SnapshotCommandState("succeeded", "snapshot complete", "{\"success\":true,\"collectorsRun\":2,\"rows\":14,\"skipped\":[]}"),
        };
        await using var app = await StartAsync(store, canEdit: false, new ManualTime());
        using var client = app.GetTestClient();

        var ct = TestContext.Current.CancellationToken;
        using var response = await client.GetAsync("/api/collectors/run/101", ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var root = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct)).RootElement;
        Assert.True(root.GetProperty("finished").GetBoolean());
        Assert.Equal(14, root.GetProperty("result").GetProperty("rows").GetInt32());
    }

    // ------------------------------------------------------------------ MCP

    [Fact]
    public void TheMcpTool_IsAWriteTool_NotOfferedWithWritesOff_AndOfferedWithWritesOn()
    {
        Assert.True(DarlingMcpWritePolicy.IsWrite("run_collectors_now"));
        Assert.False(DarlingMcpWritePolicy.Effective.Off.Offers("run_collectors_now"));
        Assert.True(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false).Offers("run_collectors_now"));
    }

    [Fact]
    public async Task TheMcpTool_ReturnsRefused_ForARunWithinTheRateWindow_AndTheResultForAFinishedRun()
    {
        var clock = new ManualTime();
        var ct = TestContext.Current.CancellationToken;
        var store = new FakeStore { Latest = new SnapshotCommandRow(1, clock.GetUtcNow().UtcDateTime.AddSeconds(-10), "succeeded") };
        var requests = new SnapshotNowRequests(store, clock);

        var refused = JsonDocument.Parse(await DarlingMcpCollectorRunTools.RunCollectorsNowCore(
            requests, 7, "srv", null, "mcp", TimeSpan.Zero, TimeSpan.Zero, ct)).RootElement;
        Assert.Equal("refused", refused.GetProperty("status").GetString());
        Assert.Equal(SnapshotRequestOutcome.RateLimitedCode, refused.GetProperty("reason").GetString());
        Assert.True(refused.TryGetProperty("next_allowed_utc", out _));

        store.Latest = null;
        store.State = new SnapshotCommandState("succeeded", "snapshot complete", "{\"success\":true,\"collectorsRun\":1,\"rows\":3,\"skipped\":[]}");
        var done = JsonDocument.Parse(await DarlingMcpCollectorRunTools.RunCollectorsNowCore(
            requests, 7, "srv", [SqlServerCollector], "mcp", TimeSpan.FromSeconds(5), TimeSpan.Zero, ct)).RootElement;
        Assert.Equal("succeeded", done.GetProperty("status").GetString());
        Assert.Equal(3, done.GetProperty("result").GetProperty("rows").GetInt32());
        Assert.Equal("mcp", Assert.Single(store.Enqueued).By);
    }
}
