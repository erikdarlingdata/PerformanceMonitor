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
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// Behaviour of the collector schedule tools (#6227) on a real store, for the parts a tool call alone does not show: the caps on
/// values a store already holds, the log lines a write leaves, and the Viewer's read and save of a row the tools left on "inherit".
/// </summary>
public sealed class DarlingMcpCollectorScheduleBehaviourLiveTests
{
    private const int ServerA = 7601;
    private const int ServerB = 7602;
    private const string Daily = "index_object_stats";
    private const string Delta = "wait_stats";
    private const string ShipsOff = "long_query_completions";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the collector schedule behaviour pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await ExecAsync(owner, $"INSERT INTO servers (server_id, server_name) VALUES ({ServerA}, 'beh-alpha'), ({ServerB}, 'beh-beta')", ct);
        await using var source = NpgsqlDataSource.Create(connectionString);

        var bodySucceeded = false;
        try
        {
            await body(source, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    // ---------------------------------------------------------------------------------------------------------------------------
    // 2. Caps against rows the store already holds
    // ---------------------------------------------------------------------------------------------------------------------------

    /// <summary>Rows a store can hold from before the V17 CHECK constraints, from a hand-run statement, or from a restore: the
    /// constraints are dropped on the scratch store so the out-of-range values can be seeded as the owner.</summary>
    private static async Task DropValueChecksAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var names = new List<string>();
        await using (var list = new NpgsqlCommand(
            "SELECT conname FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND contype = 'c'", owner))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                names.Add(reader.GetString(0));
            }
        }

        foreach (var name in names)
        {
            await ExecAsync(owner, $"ALTER TABLE config.config_collector_schedules DROP CONSTRAINT \"{name}\"", ct);
        }
    }

    [Fact]
    public async Task RowsAlreadyOutOfRange_AreClampedByTheRetentionLookbackHorizonAndFloorReads_AndGetCollectorSchedulesStillReportsThem()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            await DropValueChecksAsync(owner, ct);
            var taken = new[] { Daily, Delta, ShipsOff };
            var signal = DailySummaryHorizon.SignalCollectors.Where(c => !taken.Contains(c, StringComparer.OrdinalIgnoreCase)).ToArray();
            var floorSource = DataWindowFloor.Source.ForCollectorTable(
                CollectorCatalog.All.First(c => !taken.Contains(c.Name, StringComparer.OrdinalIgnoreCase)
                    && !c.Name.Equals(signal[0], StringComparison.OrdinalIgnoreCase)
                    && DataWindowFloor.Source.TryForCollectorTable(c.TargetTable, out var s) && s.RetentionDefaultDays is not null).TargetTable);
            var floorCollector = floorSource.CollectorName!.ToLowerInvariant();

            await ExecAsync(owner,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, frequency_minutes, retention_days) VALUES " +
                $"(NULL, '{Daily}', TRUE, 2000000000, 2000000000), " +        // far above both caps
                $"(NULL, '{Delta}', TRUE, -7, -5), " +                         // negative
                $"(NULL, '{ShipsOff}', TRUE, 0, 0), " +                        // zero retention, zero (once on load) frequency
                $"(NULL, '{signal[0]}', TRUE, NULL, 2000000000), " +
                $"({ServerA}, '{Daily}', TRUE, 600000, 3651), " +              // a server row just past both caps
                $"(NULL, '{floorCollector}', TRUE, NULL, 2000000000)", ct);

            var view = await new StoreConfigProvider(source).LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);

            /* The purge: the schedule resolver already degrades a bad retention to the cap or the default, and the purge's own floor is a
               second guard on the number it is handed. It is called with the raw stored values too, so it is held to its rule alone: a
               retention above the cap is cut to it, a zero or negative one is never below a day. */
            var daily = StoreConfigProvider.ResolveSchedule(Daily, ServerB, view.ScheduleOverrides);
            Assert.Equal(CollectorScheduleDefaults.MaxRetentionDays, DarlingRetention.EffectivePurgeRetentionDays(Daily, daily.RetentionDays));
            foreach (var (collector, stored) in new[] { (Daily, 2000000000), (Delta, -5), (ShipsOff, 0) })
            {
                Assert.Equal(stored, Convert.ToInt32(await ScalarAsync(owner, $"SELECT retention_days FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = '{collector}'", ct), CultureInfo.InvariantCulture));
                var purge = DarlingRetention.EffectivePurgeRetentionDays(collector, stored);
                Assert.InRange(purge, 1, CollectorScheduleDefaults.MaxRetentionDays);
                Assert.Equal(stored > 0 ? CollectorScheduleDefaults.MaxRetentionDays : 1, purge);
            }

            foreach (var collector in new[] { Delta, ShipsOff })
            {
                var resolved = StoreConfigProvider.ResolveSchedule(collector, ServerB, view.ScheduleOverrides);
                var days = DarlingRetention.EffectivePurgeRetentionDays(collector, resolved.RetentionDays);
                Assert.InRange(days, 1, CollectorScheduleDefaults.MaxRetentionDays);
            }

            var serverRow = StoreConfigProvider.ResolveSchedule(Daily, ServerA, view.ScheduleOverrides);
            Assert.InRange(DarlingRetention.EffectivePurgeRetentionDays(Daily, serverRow.RetentionDays), 1, CollectorScheduleDefaults.MaxRetentionDays);

            /* The analysis look-back: a stored frequency of two billion minutes would take the look-back start before the first date a
               timestamp holds. The look-back is held to two cycles of the one-year cap, and the start is a date that exists. */
            var lookback = AnalysisContext.LatestValueLookbackFor(daily.FrequencyMinutes);
            Assert.NotNull(lookback);
            Assert.True(lookback!.Value <= TimeSpan.FromMinutes(2.0 * CollectorScheduleDefaults.MaxFrequencyMinutes), $"look-back was {lookback}");
            Assert.True(DateTime.UtcNow - lookback.Value > DateTime.MinValue);
            Assert.Equal(TimeSpan.FromMinutes(2.0 * CollectorScheduleDefaults.MaxFrequencyMinutes), AnalysisContext.LatestValueLookbackFor(int.MaxValue));
            Assert.Null(AnalysisContext.LatestValueLookbackFor(StoreConfigProvider.ResolveSchedule(ShipsOff, ServerB, view.ScheduleOverrides).FrequencyMinutes));
            Assert.Null(AnalysisContext.LatestValueLookbackFor(-7));

            /* The daily summary horizon: the fleet retention the horizon is measured from is the cap for a huge row (as a number and as
               the SQL the store evaluates), and the shipped default for a zero or negative one. The shortest signal retention stays a
               real number of days. */
            var overrides = await DailySummaryHorizon.ReadFleetRetentionOverrideDaysAsync(source, 30, ct);
            Assert.Equal(2000000000, overrides[signal[0]]);
            Assert.Equal(CollectorScheduleDefaults.MaxRetentionDays, DarlingRetentionHorizons.ResolveFleetRetentionDays(signal[0], overrides[signal[0]]));
            Assert.Equal(
                Convert.ToInt64(CollectorScheduleDefaults.MaxRetentionDays, CultureInfo.InvariantCulture),
                Convert.ToInt64(await ScalarAsync(owner, $"SELECT {DarlingRetentionHorizons.FleetRetentionDaysSql(signal[0])}", ct), CultureInfo.InvariantCulture));
            Assert.Equal(CollectorScheduleDefaults.All[ShipsOff].RetentionDays, DarlingRetentionHorizons.ResolveFleetRetentionDays(ShipsOff, 0));
            Assert.Equal(CollectorScheduleDefaults.All[Delta].RetentionDays, DarlingRetentionHorizons.ResolveFleetRetentionDays(Delta, -5));
            Assert.InRange(DailySummaryHorizon.ShortestSignalRetentionDays(overrides, 7), 1, CollectorScheduleDefaults.MaxRetentionDays);

            /* The data-window floor: the horizon it subtracts from the window end is the capped one, so the probe answers where an
               unclamped two billion days would be a timestamp out of range. A collection_log row in the window makes the server count. */
            await ExecAsync(owner,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES " +
                $"(990001, {ServerA}, 'beh-alpha', '{floorCollector}', now() AT TIME ZONE 'UTC' - interval '1 hour', 'SUCCESS')", ct);
            var end = DateTime.UtcNow.AddHours(1);
            var floor = await DataWindowFloor.GetAsync(source, [floorSource], null, end.AddDays(-1), end, 30, ct);
            Assert.NotNull(floor);
            Assert.True(floor!.Value > end.AddDays(-CollectorScheduleDefaults.MaxRetentionDays - 1), $"floor was {floor}");

            /* get_collector_schedules reports every stored value as it is stored, without failing. */
            var all = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, null, null, ct));
            Assert.Null(all["error"]);
            var rows = ((JsonArray)all["overrides"]!).Cast<JsonObject>().ToList();
            Assert.Contains(rows, r => (string?)r["collector"] == Daily && (int?)r["retention_days"] == 2000000000 && (int?)r["frequency_minutes"] == 2000000000);
            Assert.Contains(rows, r => (string?)r["collector"] == Delta && (int?)r["retention_days"] == -5 && (int?)r["frequency_minutes"] == -7);
            Assert.Contains(rows, r => (string?)r["collector"] == ShipsOff && (int?)r["retention_days"] == 0);

            var one = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, Daily, "beh-alpha", ct));
            Assert.Null(one["error"]);
            Assert.NotEmpty((JsonArray)one["overrides"]!);
        });
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // 3. Log lines
    // ---------------------------------------------------------------------------------------------------------------------------

    private const string Caller = "schedule-probe/3.1";

    [Fact]
    public async Task EachScheduleWriteTool_LogsOneInformationLineNamingToolCollectorServerAndCaller_AndARefusalLogsNone()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            ReadCaller.SetAmbientMcpCaller(Caller);

            var calls = new (string Tool, Func<ILogger, Task<string>> Run)[]
            {
                ("disable_collector", l => DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, "beh-alpha", logger: l, cancellationToken: ct)),
                ("enable_collector", l => DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "beh-alpha", logger: l, cancellationToken: ct)),
                ("enable_collector", l => DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "beh-alpha", inherit: true, logger: l, cancellationToken: ct)),
                ("set_collector_frequency", l => DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Delta, 10, false, "beh-alpha", l, ct)),
                ("set_collector_retention", l => DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Delta, 45, false, null, l, ct)),
                ("set_collector_run_at", l => DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Daily, "03:30", "beh-alpha", l, ct)),
                ("set_collector_databases", l => DarlingMcpCollectorScheduleTools.SetCollectorDatabases(source, Delta, "Sales", false, "beh-alpha", l, ct)),
                /* Last: it removes the rows the calls above made, so it has something to remove. */
                ("reset_collector_schedules", l => DarlingMcpCollectorScheduleTools.ResetCollectorSchedules(source, "beh-alpha", l, ct)),
            };

            foreach (var (tool, run) in calls)
            {
                var logger = new CapturingTestLogger();
                var answer = Parse(await run(logger));
                Assert.Equal("changed", (string?)answer["status"]);

                var line = Assert.Single(logger.Lines);
                Assert.StartsWith("Information:", line, StringComparison.Ordinal);
                Assert.Contains(tool, line, StringComparison.Ordinal);
                if (tool != "reset_collector_schedules")
                {
                    /* The reset names a scope, not a collector. */
                    Assert.Contains(tool == "set_collector_run_at" ? Daily : Delta, line, StringComparison.Ordinal);
                }

                /* Retention is fleet-wide, so its write names the fleet scope, not the server. */
                Assert.Contains(tool == "set_collector_retention" ? "fleet" : "beh-alpha", line, StringComparison.OrdinalIgnoreCase);
                Assert.Contains("client says: " + Caller, line, StringComparison.Ordinal);
            }

            /* A refused call changes nothing and so logs nothing. */
            var refused = new CapturingTestLogger();
            Assert.NotEqual("changed", (string?)Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(source, "no_such_collector", "beh-alpha", logger: refused, cancellationToken: ct))["status"]);
            Assert.Empty(refused.Lines);

            var refusedDatabases = new CapturingTestLogger();
            Assert.NotEqual("changed", (string?)Parse(await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(source, "no_such_collector", "Sales", false, "beh-alpha", refusedDatabases, ct))["status"]);
            Assert.Empty(refusedDatabases.Lines);

            var refusedReset = new CapturingTestLogger();
            Assert.NotEqual("changed", (string?)Parse(await DarlingMcpCollectorScheduleTools.ResetCollectorSchedules(source, "no_such_server", refusedReset, ct))["status"]);
            Assert.Empty(refusedReset.Lines);
        });
    }

    /// <summary>The write-tool filter on the real host pipeline: the real schedule tool, served as it is with <c>mcp.allowWrites</c>, called
    /// over JSON-RPC with a User-Agent that names the caller. The filter's own logger and the tool's logger are separate sinks, so what
    /// each wrote can be told apart.</summary>
    private static async Task<TestServer> BuildWriteServerAsync(NpgsqlDataSource postgres, ILogger filterLogger, ILogger toolLogger)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, baselineCache: new BaselineCache()));
        builder.Services.AddSingleton(toolLogger);

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(), filterLogger,
            writes: new DarlingMcpWritePolicy.Effective(true, false));

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
        host.ConfigurePipeline(app, networkMode: false, networkListenIp: null, allowedCidr: System.Net.IPNetwork.Parse("127.0.0.1/32"), bearerToken: "write-filter-token");
        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<string> ToolsCallAsync(TestServer server, string toolName, string arguments)
    {
        var body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + toolName + "\",\"arguments\":" + arguments + "}}";
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(body);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Request.Headers.UserAgent = Caller;
            c.Request.Headers.Authorization = "Bearer write-filter-token";
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });
        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task TheWriteToolFilter_LogsToolCallerAndOutcomeForEveryWriteCall_AndNeverTheArguments()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var filterLog = new CapturingTestLogger();
            var toolLog = new CapturingTestLogger();
            using var server = await BuildWriteServerAsync(source, filterLog, toolLog);

            /* A write that lands: the arguments name a collector and a server, which only the tool's own line may carry. */
            var reply = await ToolsCallAsync(server, "disable_collector", "{\"collector_name\":\"wait_stats\",\"server_name\":\"beh-alpha\"}");
            Assert.Contains("changed", reply, StringComparison.Ordinal);
            var landed = Assert.Single(filterLog.Lines, l => l.Contains("MCP write tool", StringComparison.Ordinal));
            Assert.Contains("disable_collector", landed, StringComparison.Ordinal);
            Assert.Contains("client says: ua:" + Caller, landed, StringComparison.Ordinal);
            Assert.Contains("completed", landed, StringComparison.Ordinal);
            Assert.DoesNotContain("wait_stats", landed, StringComparison.Ordinal);
            Assert.DoesNotContain("beh-alpha", landed, StringComparison.Ordinal);
            Assert.Equal(1, Convert.ToInt32(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE collector_name = 'wait_stats' AND server_id = " + ServerA + " AND enabled = FALSE", ct), CultureInfo.InvariantCulture));

            /* The tool's own line, on its own sink, names the collector and the server. */
            var own = Assert.Single(toolLog.Lines, l => l.Contains("MCP schedule write", StringComparison.Ordinal));
            Assert.Contains("wait_stats", own, StringComparison.Ordinal);
            Assert.Contains("beh-alpha", own, StringComparison.Ordinal);
            Assert.Contains("client says: ua:" + Caller, own, StringComparison.Ordinal);

            /* A call the tool refuses is still a write call: the filter logs it, with an outcome, and still not the arguments. */
            filterLog = new CapturingTestLogger();
            toolLog = new CapturingTestLogger();
            using var second = await BuildWriteServerAsync(source, filterLog, toolLog);
            var refusedReply = await ToolsCallAsync(second, "set_collector_frequency", "{\"collector_name\":\"secret_collector_name\",\"frequency_minutes\":5,\"server_name\":\"beh-beta\"}");
            Assert.Contains("refused", refusedReply, StringComparison.Ordinal);
            var refused = Assert.Single(filterLog.Lines, l => l.Contains("MCP write tool", StringComparison.Ordinal));
            Assert.Contains("set_collector_frequency", refused, StringComparison.Ordinal);
            Assert.EndsWith(": refused", refused, StringComparison.Ordinal);
            Assert.DoesNotContain("completed", refused, StringComparison.Ordinal);
            Assert.Contains("client says: ua:" + Caller, refused, StringComparison.Ordinal);
            Assert.DoesNotContain("secret_collector_name", refused, StringComparison.Ordinal);
            Assert.DoesNotContain("beh-beta", refused, StringComparison.Ordinal);
            Assert.Empty(toolLog.Lines);

            /* A call the tool catches an error for (the schedule table is not there) answers an error envelope as text, not an exception:
               the filter logs it as failed, not completed, and still not the arguments. */
            await ExecAsync(owner, "ALTER TABLE config.config_collector_schedules RENAME TO config_collector_schedules_gone", ct);
            filterLog = new CapturingTestLogger();
            toolLog = new CapturingTestLogger();
            using var third = await BuildWriteServerAsync(source, filterLog, toolLog);
            var failedReply = await ToolsCallAsync(third, "disable_collector", "{\"collector_name\":\"wait_stats\",\"server_name\":\"beh-alpha\"}");
            Assert.Contains("Error during disable_collector", failedReply, StringComparison.Ordinal);
            var failed = Assert.Single(filterLog.Lines, l => l.Contains("MCP write tool", StringComparison.Ordinal));
            Assert.Contains("disable_collector", failed, StringComparison.Ordinal);
            Assert.EndsWith(": failed", failed, StringComparison.Ordinal);
            Assert.DoesNotContain("completed", failed, StringComparison.Ordinal);
            Assert.DoesNotContain("wait_stats", failed, StringComparison.Ordinal);
        });
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // 4. Viewer round trip of an inherited enabled state
    // ---------------------------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task ARowTheToolsLeftOnInherit_ReadsInTheViewerAsInherit_AndASaveFromTheViewerKeepsItNull()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            /* A fleet-wide disable, then a server row that only sets a frequency: it stores no enabled state of its own. */
            await DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, null, cancellationToken: ct);
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Delta, 10, false, "beh-alpha", cancellationToken: ct);
            Assert.Null(await ScalarAsync(owner, $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = '{Delta}'", ct));

            var connectionString = new NpgsqlConnectionStringBuilder(owner.ConnectionString).ConnectionString;
            await using var viewer = new ViewerDataService(connectionString);

            var rows = await viewer.GetCollectorSchedulesAsync(ct);
            var stored = rows.Single(r => r.ServerId == ServerA && r.CollectorName == Delta);
            Assert.Null(stored.Enabled);

            var runTimes = await viewer.GetCollectorRunTimesAsync(ct);
            var items = CollectorScheduleOverlay.BuildEffectiveSchedule(rows, runTimes, ServerA);
            var item = items.Single(i => i.Name == Delta);
            Assert.True(item.EnabledInherited);
            Assert.False(item.Enabled);           // what it inherits: the fleet-wide disable
            Assert.Equal(10, item.FrequencyMinutes);

            /* Saved unchanged, the row keeps NULL and its frequency: the Save does not freeze the inherited state into false. */
            var plan = CollectorScheduleOverlay.BuildSavePlan(items, runTimes, ServerA, usesDefault: false, resetToDefaults: false, schedulesRead: true, runTimesRead: true);
            Assert.Null(plan.Refusal);
            await viewer.SaveCollectorScheduleAsync(ServerA, plan.Rows, plan.RunTimeChanges, plan.ClearRunTimes, ct);
            Assert.Null(await ScalarAsync(owner, $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = '{Delta}'", ct));
            Assert.Equal(10, Convert.ToInt32(await ScalarAsync(owner, $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = '{Delta}'", ct), CultureInfo.InvariantCulture));

            /* The fleet-wide row is untouched by the server's Save, and a later fleet-wide enable still reaches the inherited row. */
            await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, null, cancellationToken: ct);
            var after = CollectorScheduleOverlay.BuildEffectiveSchedule(await viewer.GetCollectorSchedulesAsync(ct), runTimes, ServerA).Single(i => i.Name == Delta);
            Assert.True(after.EnabledInherited);
            Assert.True(after.Enabled);

            /* A choice made in the grid is an explicit state and is saved as one. */
            after.Enabled = false;
            Assert.False(after.EnabledInherited);
            var explicitPlan = CollectorScheduleOverlay.BuildSavePlan(
                new[] { after }, runTimes, ServerA, usesDefault: false, resetToDefaults: false, schedulesRead: true, runTimesRead: true);
            await viewer.SaveCollectorScheduleAsync(ServerA, explicitPlan.Rows, explicitPlan.RunTimeChanges, explicitPlan.ClearRunTimes, ct);
            Assert.False((bool)(await ScalarAsync(owner, $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = '{Delta}'", ct))!);

            /* An MCP write of an explicit enabled state, then inherit=true, reads back as on, then as inherit. */
            await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "beh-alpha", cancellationToken: ct);
            var explicitOn = CollectorScheduleOverlay.BuildEffectiveSchedule(await viewer.GetCollectorSchedulesAsync(ct), runTimes, ServerA).Single(i => i.Name == Delta);
            Assert.False(explicitOn.EnabledInherited);
            Assert.True(explicitOn.Enabled);
            await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "beh-alpha", inherit: true, cancellationToken: ct);
            Assert.True(CollectorScheduleOverlay.BuildEffectiveSchedule(await viewer.GetCollectorSchedulesAsync(ct), runTimes, ServerA).Single(i => i.Name == Delta).EnabledInherited);
        });
    }
}
