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

    private static string BufferCacheTextIn(string database) =>
        $"the pg_buffercache extension is not installed: run CREATE EXTENSION pg_buffercache in database '{database}', which is the one this collector connects to, "
        + "as a role that may create extensions. The first cycle after that returns the buffer pool rows with no restart.";

    private static readonly string BufferCacheText = BufferCacheTextIn("postgres");

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
    private static Fleet BuildFleet(int serverCount, int stillWaiting, Func<int, string>? bufferTextOf = null)
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
            failures[(server.ServerId, "pg_buffer_usage")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-4), bufferTextOf?.Invoke(server.ServerId) ?? BufferCacheText);

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
            Assert.Equal(1, group.GetProperty("distinct_texts").GetInt32());
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

        /* The summaries alone are over the budget, so no precondition group fits either, and the counts still say how many collectors are held. */
        Assert.True(root.GetProperty("precondition_held_truncated").GetBoolean());
        Assert.Equal(0, root.GetProperty("precondition_held").GetArrayLength());
        Assert.Equal(800, root.GetProperty("precondition_count").GetInt32());
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

    [Fact]
    public async Task ServersConnectedToDifferentDatabases_StillCollapseToOneGroupPerCollectorAndStatus_InsideTheBudget()
    {
        /* The buffer cache step names the connected database, so no two servers write the same text. */
        var fleet = BuildFleet(serverCount: 120, stillWaiting: 0, bufferTextOf: id => BufferCacheTextIn($"tenant_db_{id:000}"));
        var reply = await Compose(fleet);

        using var doc = JsonDocument.Parse(reply);
        var root = doc.RootElement;
        var groups = root.GetProperty("precondition_held").EnumerateArray().ToList();
        Assert.Equal(2, groups.Count);

        var buffer = groups.Single(g => g.GetProperty("collector").GetString() == "pg_buffer_usage");
        Assert.Equal(120, buffer.GetProperty("servers").GetInt32());
        Assert.Equal(120, buffer.GetProperty("distinct_texts").GetInt32());
        Assert.Contains("CREATE EXTENSION pg_buffercache", buffer.GetProperty("last_error").GetString());
        var textServer = buffer.GetProperty("text_server").GetString()!;
        var databaseOfTextServer = $"tenant_db_{int.Parse(textServer[^3..], System.Globalization.CultureInfo.InvariantCulture):000}";
        Assert.Contains($"in database '{databaseOfTextServer}'", buffer.GetProperty("last_error").GetString());

        var autoExplain = groups.Single(g => g.GetProperty("collector").GetString() == "pg_plan_capture");
        Assert.Equal(1, autoExplain.GetProperty("distinct_texts").GetInt32());

        Assert.False(root.GetProperty("precondition_held_truncated").GetBoolean());
        Assert.False(root.GetProperty("budget_truncated").GetBoolean());
        Assert.False(root.GetProperty("over_budget").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
        Assert.Equal(240, root.GetProperty("precondition_count").GetInt32());
        Assert.True(Encoding.UTF8.GetByteCount(reply) <= McpResponseBudget.DefaultBytes, $"the reply is {Encoding.UTF8.GetByteCount(reply)} bytes");
    }

    [Fact]
    public async Task TheGroupCarriesTheNewestServersText_AndTheServerItCameFrom()
    {
        var fleet = BuildFleet(serverCount: 4, stillWaiting: 0);
        fleet.Failures[(1, "pg_buffer_usage")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-10), BufferCacheTextIn("older_db"));
        fleet.Failures[(2, "pg_buffer_usage")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-1), BufferCacheTextIn("newest_db"));
        fleet.Failures[(3, "pg_buffer_usage")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-5), BufferCacheTextIn("older_db"));
        fleet.Failures[(4, "pg_buffer_usage")] = new(CollectorRuntimePrecondition.ExtensionMissingStatus, Now.AddMinutes(-7), BufferCacheTextIn("other_db"));

        using var doc = JsonDocument.Parse(await Compose(fleet));
        var buffer = doc.RootElement.GetProperty("precondition_held").EnumerateArray().Single(g => g.GetProperty("collector").GetString() == "pg_buffer_usage");
        Assert.Equal("pg-target-002", buffer.GetProperty("text_server").GetString());
        Assert.Contains("in database 'newest_db'", buffer.GetProperty("last_error").GetString());
        Assert.Equal(3, buffer.GetProperty("distinct_texts").GetInt32());
        Assert.Equal(4, buffer.GetProperty("servers").GetInt32());
        Assert.Equal(Stamp(Now.AddMinutes(-1)), buffer.GetProperty("last_error_at").GetString());
        Assert.Equal(Stamp(Start), buffer.GetProperty("waiting_since").GetString());
    }

    /// <summary>
    /// Every one of <paramref name="collectors"/> collectors is held by a named skip on every server, with a status that alternates by server, so a
    /// collector has two groups. The text is long enough that a group carries the full group cut.
    /// </summary>
    private static Fleet BuildManyGroups(int serverCount, int collectors, int stillWaiting)
    {
        var servers = Servers(serverCount);
        var pairs = new List<DarlingMcpCollectorReadinessTools.Pair>();
        var failures = new Dictionary<(int, string), DarlingMcpCollectorReadinessTools.Failure>();
        var remedy = string.Concat(Enumerable.Repeat("then restart the cluster and run the grant again ", 40));
        foreach (var server in servers)
        {
            for (var c = 1; c <= collectors; c++)
            {
                var collector = $"pg_collector_{c:000}";
                pairs.Add(new(server.ServerId, collector, 5, false));
                var status = server.ServerId % 2 == 1 ? CollectorRuntimePrecondition.ExtensionMissingStatus : "PERMISSIONS";
                failures[(server.ServerId, collector)] = new(status, Now.AddMinutes(-3), $"{collector} on {server.ServerName}: {remedy}");
            }

            if (server.ServerId <= stillWaiting)
            {
                pairs.Add(new(server.ServerId, "wait_stats", 5, false));
                failures[(server.ServerId, "wait_stats")] = new("ERROR", Now.AddMinutes(-2), "connection was refused by the server");
            }
        }

        return new Fleet(servers, pairs, new Dictionary<(int, string), DateTime>(), failures);
    }

    [Fact]
    public async Task MoreGroupsThanFit_AreCutFromTheEnd_AndTheNoteSaysHowManyWereLeftOut()
    {
        var fleet = BuildManyGroups(serverCount: 4, collectors: 30, stillWaiting: 3);
        var reply = await Compose(fleet);

        using var doc = JsonDocument.Parse(reply);
        var root = doc.RootElement;
        var kept = root.GetProperty("precondition_held").EnumerateArray()
            .Select(g => (Collector: g.GetProperty("collector").GetString()!, Status: g.GetProperty("status").GetString()!))
            .ToList();
        var all = Enumerable.Range(1, 30)
            .SelectMany(c => new[] { CollectorRuntimePrecondition.ExtensionMissingStatus, "PERMISSIONS" }.Select(st => ($"pg_collector_{c:000}", st)))
            .OrderBy(g => g.Item1, StringComparer.Ordinal)
            .ThenBy(g => g.st, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(60, all.Count);
        Assert.InRange(kept.Count, 1, 59);
        Assert.Equal(all.Take(kept.Count), kept);
        Assert.True(root.GetProperty("precondition_held_truncated").GetBoolean());
        Assert.False(root.GetProperty("over_budget").GetBoolean());
        Assert.Contains($"{60 - kept.Count} of 60 precondition_held group(s)", root.GetProperty("budget_note").GetString());
        Assert.True(Encoding.UTF8.GetByteCount(reply) <= McpResponseBudget.DefaultBytes, $"the reply is {Encoding.UTF8.GetByteCount(reply)} bytes");

        /* The counts cover every held collector, and a waiting row still fits. */
        Assert.Equal(120, root.GetProperty("precondition_count").GetInt32());
        Assert.Equal(3, root.GetProperty("waiting_count").GetInt32());
        Assert.True(root.GetProperty("waiting").GetArrayLength() >= 1);
    }

    [Fact]
    public async Task GroupsThatFit_AreAllKept_AndNothingSaysTheyWereCut()
    {
        var fleet = BuildManyGroups(serverCount: 4, collectors: 30, stillWaiting: 3);
        using var doc = JsonDocument.Parse(await Compose(fleet, budgetBytes: 4 * 1024 * 1024));
        var root = doc.RootElement;
        Assert.Equal(60, root.GetProperty("precondition_held").GetArrayLength());
        Assert.False(root.GetProperty("precondition_held_truncated").GetBoolean());
        Assert.False(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
        Assert.Equal(3, root.GetProperty("waiting").GetArrayLength());
    }

    private static string Stamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc).ToString("o");

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
