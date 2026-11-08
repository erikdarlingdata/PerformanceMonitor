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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8318: <c>get_sweep_reports</c> at its default arguments answers within <see cref="McpResponseBudget.DefaultBytes"/>
/// on a store with a standing worklist, and its <c>budget_note</c> describes the reply the caller gets. The worklist grows
/// with the fleet (one item per server that is or was off its band), so cutting the timeline alone cannot hold the budget:
/// the items are cut too, kept by standing (open before carried, newest first) and shipped in the worklist's shared order,
/// <c>watch_items_returned</c> rides beside the worklist's <c>count</c>, and when the reply at its floor is still over the
/// budget the note says so rather than "fit". Each case seeds its own scratch store (<see cref="ScratchPostgres"/>) and
/// reads it through the tool itself, so the bytes measured are the bytes a client receives.
/// </summary>
[Collection("live-postgres")]
public sealed class McpSweepReportsWorklistBudgetLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>One sweep with a small document and a 40-item worklist (32 open, 8 carried, about 900 bytes each): the
    /// worklist alone is over the budget. The carried items are seeded NEWER than the open ones, so the store's order
    /// (last seen first) would keep them and drop open items; the cut instead keeps the open items, newest first, and ships
    /// the survivors in the store's order. The reply is under the budget, says so, and names the worklist and the
    /// <c>watch_state</c> read in its note.</summary>
    [Fact]
    public async Task GetSweepReports_AStandingWorklist_IsCutToTheBudget_KeepingOpenItemsNewestFirst_AndTheNoteNamesIt()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report worklist budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        await SeedSweepsAsync(postgres, sweepCount: 1, serversPerReport: 6, openItems: 32, carriedItems: 8, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, hours_back: 1, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the default-shaped reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.True(root.GetProperty("truncated").GetBoolean(), "a reply whose worklist was cut says truncated: true");

        var worklist = root.GetProperty("watch_items");
        Assert.Equal(40, worklist.GetProperty("count").GetInt32());
        var returned = root.GetProperty("watch_items_returned").GetInt32();
        Assert.InRange(returned, 1, 32);
        Assert.Equal(returned, worklist.GetProperty("items").GetArrayLength());
        Assert.Equal(1, root.GetProperty("sweeps_returned").GetInt32());

        var note = root.GetProperty("budget_note").GetString();
        Assert.NotNull(note);
        Assert.Contains("watch item", note, StringComparison.Ordinal);
        Assert.Contains("watch_state", note, StringComparison.Ordinal);
        Assert.Contains("response budget", note, StringComparison.Ordinal);

        /* The survivors: every one open (the newer carried items were cut first), and the newest of the open ones, which
           the seed numbers 1..32 newest first - so the kept set is exactly servers 1..returned. */
        var items = worklist.GetProperty("items").EnumerateArray().ToList();
        Assert.All(items, item => Assert.Equal(FleetSweepWatchStateMachine.Open, item.GetProperty("state").GetString()));
        Assert.Equal(
            Enumerable.Range(1, returned),
            items.Select(item => item.GetProperty("server_id").GetInt32()).OrderBy(id => id));
    }

    /// <summary>Two sweeps in the window, each with a 150-server document, so <c>latest</c> alone is over the budget, and
    /// a 20-item worklist. The reply cannot be brought under the budget (latest is never cut), so the note says it is over
    /// and does not claim a fit, while <c>latest</c> still carries the full document.</summary>
    [Fact]
    public async Task GetSweepReports_ALatestOverTheBudgetOnItsOwn_SaysTheReplyIsOver_NotThatItFit()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report over-budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        await SeedSweepsAsync(postgres, sweepCount: 2, serversPerReport: 150, openItems: 20, carriedItems: 0, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) > McpResponseBudget.DefaultBytes,
            $"the seed must leave the floor reply over the budget; it is {Encoding.UTF8.GetByteCount(json):#,0} bytes");
        Assert.True(root.GetProperty("truncated").GetBoolean(), "a reply over the budget says truncated: true");
        Assert.Equal(2, root.GetProperty("count").GetInt32());
        Assert.Equal(1, root.GetProperty("sweeps_returned").GetInt32());

        var note = root.GetProperty("budget_note").GetString();
        Assert.NotNull(note);
        Assert.DoesNotContain("fit", note, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("response budget", note, StringComparison.Ordinal);
        Assert.Contains("latest", note, StringComparison.Ordinal);

        Assert.Equal(JsonValueKind.Object, root.GetProperty("latest").GetProperty("report").ValueKind);
        Assert.Equal(150, root.GetProperty("latest").GetProperty("report").GetProperty("servers").GetArrayLength());
    }

    /* ---------------- seeding ---------------- */

    /// <summary>Sweeps at a 15-minute cadence ending a minute ago, newest id highest, each with a <paramref name="serversPerReport"/>-card
    /// document (about 290 bytes a card); the worklist is written with the newest sweep: <paramref name="openItems"/> open items
    /// (servers 1.., last seen 1.. seconds BEFORE the sweep, so server 1 is the newest open item) then <paramref name="carriedItems"/>
    /// carried ones (last seen seconds AFTER the sweep, newer than every open item), each about 900 bytes on the wire.</summary>
    private static async Task<List<long>> SeedSweepsAsync(
        NpgsqlDataSource postgres, int sweepCount, int serversPerReport, int openItems, int carriedItems, CancellationToken ct)
    {
        var newest = DateTime.UtcNow.AddMinutes(-1);
        var cadence = TimeSpan.FromMinutes(15);
        var ids = new List<long>(sweepCount);
        const long idBase = 8_318_000_000_000_000_000L;
        for (var i = 0; i < sweepCount; i++)
        {
            var sweptAt = newest - (cadence * i);
            var id = idBase + (sweepCount - i);
            ids.Add(id);
            var cards = string.Join(",", Enumerable.Range(1, serversPerReport).Select(n =>
                $"{{\"server\":\"server-{n}\",\"band\":\"healthy\",\"reason\":\"{new string('x', 240)}\"}}"));
            var run = new FleetSweepRun(
                SweepId: id,
                SweptAtUtc: sweptAt,
                SpanStartUtc: sweptAt - cadence,
                SpanEndUtc: sweptAt,
                PreviousSweepId: i + 1 < sweepCount ? idBase + (sweepCount - i - 1) : null,
                AlertsEnabled: true,
                ServersExpected: serversPerReport,
                ServersReported: serversPerReport,
                InstrumentsAlive: true,
                InstrumentLivenessJson: """{"alert_pass_counter":{"read":true,"advancing":true}}""",
                ReportJson: $"{{\"sweep_id\":{id},\"servers\":[{cards}]}}");

            var items = i == 0
                ? Enumerable.Range(1, openItems)
                    .Select(n => WatchItem(n, FleetSweepWatchStateMachine.Open, id, lastSeen: sweptAt.AddSeconds(-n)))
                    .Concat(Enumerable.Range(openItems + 1, carriedItems)
                        .Select(n => WatchItem(n, FleetSweepWatchStateMachine.Carried, id, lastSeen: sweptAt.AddSeconds(n))))
                    .ToList()
                : new List<FleetSweepWatchItem>();

            await FleetSweepStore.RecordSweepAsync(
                postgres,
                run,
                new[]
                {
                    new FleetSweepServerVerdict(1, "server-1", "Healthy", "Healthy", null, null),
                    new FleetSweepServerVerdict(2, "server-2", "Warning", "Healthy", "cpu above the warning line", """{"cpu":82}"""),
                },
                Array.Empty<FleetSweepWouldHavePagedEntry>(),
                items,
                ct);
        }

        return ids;
    }

    private static FleetSweepWatchItem WatchItem(int serverId, string state, long sweepId, DateTime lastSeen) =>
        new(
            ServerId: serverId,
            ItemKey: string.Create(CultureInfo.InvariantCulture, $"cpu_percent:{serverId}"),
            Condition: "cpu_percent above the warning line on two consecutive sweeps",
            State: state,
            ConsecutiveHits: 2,
            ConsecutiveMisses: 0,
            FirstSeenSweepId: sweepId,
            LastSeenSweepId: sweepId,
            OpenedSweepId: sweepId,
            ClosedSweepId: null,
            FirstSeenAtUtc: lastSeen.AddMinutes(-15),
            LastSeenAtUtc: lastSeen,
            EvidenceJson: string.Create(CultureInfo.InvariantCulture,
                $"{{\"metric\":\"cpu_percent\",\"value\":{90 + (serverId % 9)},\"warning_line\":80,\"detail\":\"{new string('x', 520)}\"}}"));

    /// <summary>The scratch store's connection string with the store's search_path on every pooled session, so the tool's
    /// and the seeding's unqualified table names resolve (the <c>McpSweepAnalyzeResponseBudgetLiveTests</c> shape).</summary>
    private static string WithSearchPath(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
}
