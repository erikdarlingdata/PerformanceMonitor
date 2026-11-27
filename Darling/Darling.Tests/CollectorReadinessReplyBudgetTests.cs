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
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9880: <c>get_collector_readiness</c> over a fleet whose monitored servers lack a setup step. Many servers each hold <c>pg_plan_capture</c> and
/// <c>pg_buffer_usage</c> behind a named skip, plus a few that are only waiting. The reply stays inside the response budget or says where it was cut and
/// the offset that reads on, no failure text ends inside a word, the repeated step text is carried once, and the verdict counts a collector held by a
/// setup step apart from one waiting for its first cycle. The fleet is built in memory: the read's own reply composition is what runs.
/// </summary>
public sealed class CollectorReadinessReplyBudgetTests
{
    private static readonly DateTime Start = new(2026, 11, 27, 6, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Now = Start.AddHours(7);

    private const string AutoExplainText =
        "auto_explain must be listed in shared_preload_libraries. On Aurora/RDS this is a custom CLUSTER parameter group setting (not DB instance level) "
        + "and needs a reboot; self-managed PostgreSQL needs the line in postgresql.conf and a restart. Once it is loaded the collector reads the plans on the next cycle "
        + "and the capture readiness read, get_pg_plan_capture_readiness, names which database and which setting still lack it.";

    private const string BufferCacheText =
        "the pg_buffercache extension is not installed: run CREATE EXTENSION pg_buffercache in database 'postgres', which is the one this collector connects to, "
        + "as a role that may create extensions. The first cycle after that returns the buffer pool rows with no restart.";

    private static List<CollectorFleetLiveness.ServerFacts> Servers(int count) =>
        Enumerable.Range(1, count)
            .Select(i => new CollectorFleetLiveness.ServerFacts(i, $"pg-target-{i:000}", null, 0, "postgres", AwsRdsKind.None))
            .ToList();

    private sealed record Fleet(
        List<CollectorFleetLiveness.ServerFacts> Servers,
        List<DarlingMcpCollectorReadinessTools.Pair> Pairs,
        Dictionary<(int, string), DateTime> FirstGood,
        Dictionary<(int, string), DarlingMcpCollectorReadinessTools.Failure> Failures);

    /// <summary>
    /// Every server holds both collectors behind a named skip and has one ordinary collector done. The first <paramref name="stillWaiting"/> servers
    /// also have a collector that never ran, with an ordinary error.
    /// </summary>
    private static Fleet BuildFleet(int serverCount, int stillWaiting)
    {
        var servers = Servers(serverCount);
        var pairs = new List<DarlingMcpCollectorReadinessTools.Pair>();
        var firstGood = new Dictionary<(int, string), DateTime>();
        var failures = new Dictionary<(int, string), DarlingMcpCollectorReadinessTools.Failure>();
        foreach (var server in servers)
        {
            pairs.Add(new(server.ServerId, "memory_stats", 5, false));
            firstGood[(server.ServerId, "memory_stats")] = Start.AddMinutes(2);

            pairs.Add(new(server.ServerId, "pg_plan_capture", 5, false));
            failures[(server.ServerId, "pg_plan_capture")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-3), AutoExplainText);

            pairs.Add(new(server.ServerId, "pg_buffer_usage", 5, false));
            failures[(server.ServerId, "pg_buffer_usage")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-4), BufferCacheText);

            if (server.ServerId <= stillWaiting)
            {
                pairs.Add(new(server.ServerId, "wait_stats", 5, false));
                failures[(server.ServerId, "wait_stats")] = new("ERROR", Now.AddMinutes(-2), $"connection to {server.ServerName} was refused by the server");
            }
        }

        return new Fleet(servers, pairs, firstGood, failures);
    }

    private static Task<string> Compose(Fleet fleet, int offset = 0, int budgetBytes = McpResponseBudget.DefaultBytes) =>
        DarlingMcpCollectorReadinessTools.ComposeAsync(
            fleet.Servers, fleet.Pairs,
            fleet.FirstGood.ToDictionary(kv => kv.Key, kv => kv.Value),
            new Dictionary<(int ServerId, string Collector), DateTime>(),
            Start, Now, notCollected: 0, disabled: 0, singleServer: false,
            readDetail: pairs => Task.FromResult(pairs.ToDictionary(p => (p.ServerId, p.Collector), _ => new DarlingMcpCollectorReadinessTools.Detail(1, 0))),
            readFailures: pairs => Task.FromResult(pairs
                .Where(p => fleet.Failures.ContainsKey((p.ServerId, p.Collector)))
                .ToDictionary(p => (p.ServerId, p.Collector), p => fleet.Failures[(p.ServerId, p.Collector)])),
            offset, budgetBytes);

    [Fact]
    public async Task FleetWithManyHeldCollectors_StaysInsideTheBudget_OrSaysWhereItWasCutAndWhereItReadsOn()
    {
        var fleet = BuildFleet(serverCount: 120, stillWaiting: 120);
        var reply = await Compose(fleet);

        using var doc = JsonDocument.Parse(reply);
        var root = doc.RootElement;
        var waiting = root.GetProperty("waiting").EnumerateArray().ToList();
        var bytes = Encoding.UTF8.GetByteCount(reply);

        Assert.Equal(120, root.GetProperty("waiting_count").GetInt32());
        if (waiting.Count < root.GetProperty("waiting_count").GetInt32())
        {
            Assert.True(root.GetProperty("budget_truncated").GetBoolean() || root.GetProperty("truncated").GetBoolean());
        }

        if (root.GetProperty("budget_truncated").GetBoolean())
        {
            Assert.NotEqual(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
            Assert.Contains($"offset = {waiting.Count}", root.GetProperty("budget_note").GetString());
        }
        else
        {
            Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"the reply is {bytes} bytes");
        }

        Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"the reply is {bytes} bytes");
    }

    [Fact]
    public async Task OffsetReadsOn_FromWhereTheBudgetCut_UntilEveryWaitingRowWasListOnce()
    {
        var fleet = BuildFleet(serverCount: 30, stillWaiting: 30);
        var seen = new List<string>();
        var offset = 0;
        var pages = 0;
        while (true)
        {
            var reply = await Compose(fleet, offset, budgetBytes: 10 * 1024);
            using var doc = JsonDocument.Parse(reply);
            var root = doc.RootElement;
            Assert.True(Encoding.UTF8.GetByteCount(reply) <= 10 * 1024);
            var rows = root.GetProperty("waiting").EnumerateArray().Select(w => w.GetProperty("server").GetString()!).ToList();
            seen.AddRange(rows);
            pages++;
            if (!root.GetProperty("truncated").GetBoolean())
            {
                break;
            }

            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            offset += rows.Count;
            Assert.True(pages < 200, "the pages never ended");
        }

        Assert.True(pages > 1);
        Assert.Equal(30, seen.Count);
        Assert.Equal(30, seen.Distinct().Count());
    }

    [Fact]
    public async Task NoFailureTextEndsInsideAWord_AndACutRowSaysSo()
    {
        var fleet = BuildFleet(serverCount: 40, stillWaiting: 40);
        /* A text with no break in range, and one whose limit falls inside "listed in sha". */
        fleet.Failures[(1, "wait_stats")] = new("ERROR", Now, new string('x', 900));
        fleet.Failures[(2, "wait_stats")] = new("ERROR", Now, "auto_explain must be listed in shared_preload_libraries and then the cluster has to restart " + string.Concat(Enumerable.Repeat("remedy words follow ", 40)));

        using var doc = JsonDocument.Parse(await Compose(fleet));
        var root = doc.RootElement;
        var texts = new List<(string Text, bool Cut)>();
        foreach (var row in root.GetProperty("waiting").EnumerateArray())
        {
            if (row.GetProperty("last_error").ValueKind == JsonValueKind.String)
            {
                texts.Add((row.GetProperty("last_error").GetString()!, row.GetProperty("last_error_cut").GetBoolean()));
            }
        }

        foreach (var group in root.GetProperty("precondition_held").EnumerateArray())
        {
            texts.Add((group.GetProperty("last_error").GetString()!, group.GetProperty("last_error_cut").GetBoolean()));
        }

        Assert.Contains(texts, t => t.Cut);
        var sources = new[] { AutoExplainText, BufferCacheText, "connection to pg-target-001 was refused by the server" };
        foreach (var (text, cut) in texts)
        {
            if (!cut)
            {
                continue;
            }

            /* Whatever is kept ends where the source text has a word break, or is the whole of an unbreakable run. */
            var source = fleet.Failures.Values.Select(f => f.Text!).Concat(sources).First(s => s.StartsWith(text, StringComparison.Ordinal));
            Assert.True(text.Length < source.Length);
            var next = source[text.Length];
            Assert.True(!char.IsLetterOrDigit(next) || !text.Contains(' '), $"'{text}' ends inside a word");
            Assert.False(text.EndsWith(' '));
        }
    }

    [Fact]
    public async Task TheRepeatedStepText_IsCarriedOnce_WithTheServerCountAndFewExamples()
    {
        var fleet = BuildFleet(serverCount: 120, stillWaiting: 0);
        var reply = await Compose(fleet);

        using var doc = JsonDocument.Parse(reply);
        var groups = doc.RootElement.GetProperty("precondition_held").EnumerateArray().ToList();
        Assert.Equal(2, groups.Count);
        Assert.Equal(new[] { "pg_buffer_usage", "pg_plan_capture" }, groups.Select(g => g.GetProperty("collector").GetString()).OrderBy(c => c, StringComparer.Ordinal));
        foreach (var group in groups)
        {
            Assert.Equal(120, group.GetProperty("servers").GetInt32());
            Assert.Equal(DarlingMcpCollectorReadinessTools.ExampleServers, group.GetProperty("example_servers").GetArrayLength());
            Assert.Equal(CollectorRuntimePrecondition.ExtensionMissingStatus, group.GetProperty("status").GetString());
            Assert.False(group.GetProperty("last_error_cut").GetBoolean());
        }

        /* The whole remedy of the auto_explain step is in the reply, once. */
        Assert.Equal(1, CountOccurrences(reply, "get_pg_plan_capture_readiness, names which database"));
        Assert.Equal(1, CountOccurrences(reply, "which is the one this collector connects to"));
    }

    [Fact]
    public async Task TheVerdict_CountsHeldCollectorsApartFromWaitingOnes_AndTheWaitingCountReachesZero()
    {
        var finished = BuildFleet(serverCount: 30, stillWaiting: 0);
        using (var doc = JsonDocument.Parse(await Compose(finished)))
        {
            var root = doc.RootElement;
            Assert.Equal(0, root.GetProperty("waiting_count").GetInt32());
            Assert.Equal(60, root.GetProperty("precondition_count").GetInt32());
            Assert.Equal(30, root.GetProperty("done_count").GetInt32());
            Assert.Equal("reached", root.GetProperty("first_full_cycle").GetProperty("status").GetString());
            Assert.StartsWith("Every due collector that a setup step is not holding has a good cycle", root.GetProperty("verdict").GetString());
            Assert.All(root.GetProperty("done").EnumerateArray(), s =>
            {
                Assert.Equal(0, s.GetProperty("collectors_waiting").GetInt32());
                Assert.Equal(2, s.GetProperty("collectors_held_by_precondition").GetInt32());
            });
        }

        var unfinished = BuildFleet(serverCount: 30, stillWaiting: 7);
        using (var doc = JsonDocument.Parse(await Compose(unfinished)))
        {
            var root = doc.RootElement;
            Assert.Equal(7, root.GetProperty("waiting_count").GetInt32());
            Assert.Equal(60, root.GetProperty("precondition_count").GetInt32());
            Assert.Equal("not yet", root.GetProperty("first_full_cycle").GetProperty("status").GetString());
            Assert.StartsWith("7 of ", root.GetProperty("verdict").GetString());
            Assert.All(root.GetProperty("waiting").EnumerateArray(), w => Assert.Equal("wait_stats", w.GetProperty("collector").GetString()));
        }
    }

    [Fact]
    public async Task ASummaryTooBigForTheBudget_SaysSoAndNamesThePerServerRead()
    {
        var fleet = BuildFleet(serverCount: 400, stillWaiting: 400);
        using var doc = JsonDocument.Parse(await Compose(fleet));
        var root = doc.RootElement;
        Assert.True(root.GetProperty("over_budget").GetBoolean());
        Assert.Contains("OVER BUDGET", root.GetProperty("budget_note").GetString());
        Assert.Contains("server_name", root.GetProperty("budget_note").GetString());
        Assert.Equal(400, root.GetProperty("waiting_count").GetInt32());
    }

    [Fact]
    public async Task AnAbandonedOrErroredCollector_IsWaiting_NotHeldByASetupStep()
    {
        var fleet = BuildFleet(serverCount: 3, stillWaiting: 0);
        fleet.Failures[(1, "pg_plan_capture")] = new("ABANDONED", Now, "ran past its time budget");
        fleet.Failures[(2, "pg_buffer_usage")] = new("ERROR", Now, "could not connect");

        using var doc = JsonDocument.Parse(await Compose(fleet));
        var root = doc.RootElement;
        Assert.Equal(2, root.GetProperty("waiting_count").GetInt32());
        Assert.Equal(4, root.GetProperty("precondition_count").GetInt32());
    }

    private static int CountOccurrences(string text, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = text.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }
}
