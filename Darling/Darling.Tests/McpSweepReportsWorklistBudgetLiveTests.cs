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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
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
           the seed numbers 1..32 newest first - so the items are exactly servers 1..returned, and they ride in the store's
           order (last seen first), which for these items is that same order. */
        var items = worklist.GetProperty("items").EnumerateArray().ToList();
        Assert.All(items, item => Assert.Equal(FleetSweepWatchStateMachine.Open, item.GetProperty("state").GetString()));
        Assert.Equal(
            Enumerable.Range(1, returned),
            items.Select(item => item.GetProperty("server_id").GetInt32()));
        Assert.False(root.GetProperty("over_budget").GetBoolean(), "a reply brought under the budget says over_budget: false");
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
        Assert.True(root.GetProperty("truncated").GetBoolean(), "a reply whose timeline and worklist were cut says truncated: true");
        Assert.True(root.GetProperty("over_budget").GetBoolean(), "a reply still over the budget at its floor says over_budget: true");
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

    /// <summary>One sweep in the window with a 150-server document and one open item: nothing can be cut (one sweep, one
    /// item, both at their floors) and the reply is still over the budget because <c>latest</c> alone is. <c>truncated</c> is
    /// false, because nothing was left out; <c>over_budget</c> is true and the note says OVER BUDGET.</summary>
    [Fact]
    public async Task GetSweepReports_ALatestOverTheBudgetWithNothingToCut_SaysOverBudget_AndTruncatedIsFalse()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report over-budget flag test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        await SeedSweepsAsync(postgres, sweepCount: 1, serversPerReport: 150, openItems: 1, carriedItems: 0, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) > McpResponseBudget.DefaultBytes,
            $"the seed must leave the reply over the budget; it is {Encoding.UTF8.GetByteCount(json):#,0} bytes");
        Assert.Equal(1, root.GetProperty("count").GetInt32());
        Assert.Equal(1, root.GetProperty("sweeps_returned").GetInt32());
        Assert.Equal(1, root.GetProperty("watch_items").GetProperty("count").GetInt32());
        Assert.Equal(1, root.GetProperty("watch_items_returned").GetInt32());

        Assert.False(root.GetProperty("truncated").GetBoolean(), "nothing was cut, so truncated is false");
        Assert.True(root.GetProperty("over_budget").GetBoolean(), "the reply is over the budget, so over_budget is true");
        var note = root.GetProperty("budget_note").GetString();
        Assert.NotNull(note);
        Assert.StartsWith("OVER BUDGET", note, StringComparison.Ordinal);
        Assert.DoesNotContain("fit", note, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Two small sweeps in the window: the newest sweep's document ships once. <c>latest</c> carries it in full; the
    /// newest timeline entry keeps its header facts and a slim <c>report</c> with exactly the fields the sweep rows draw from
    /// (<c>fleet.bands</c>, <c>watch</c>, and <c>would_have_paged</c> when the sweep ran muted), equal to latest's, and names
    /// latest with <c>report_under</c>. The older entry keeps its whole document.</summary>
    [Fact]
    public async Task GetSweepReports_TheNewestSweepsDocument_ShipsOnce_AndItsEntryKeepsTheRowCounts()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report single-document test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        await SeedSweepsAsync(postgres, sweepCount: 2, serversPerReport: 6, openItems: 0, carriedItems: 0, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var latest = root.GetProperty("latest");
        var latestId = latest.GetProperty("sweep_id").GetString();
        var entries = root.GetProperty("sweeps").EnumerateArray().ToList();
        Assert.Equal(2, entries.Count);

        var newest = Assert.Single(entries, entry => entry.GetProperty("sweep_id").GetString() == latestId);
        Assert.Equal("latest", newest.GetProperty("report_under").GetString());
        var slim = newest.GetProperty("report");
        Assert.Equal(new[] { "fleet", "watch" }, slim.EnumerateObject().Select(p => p.Name).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(new[] { "bands" }, slim.GetProperty("fleet").EnumerateObject().Select(p => p.Name));
        var full = latest.GetProperty("report");
        Assert.Equal(full.GetProperty("fleet").GetProperty("bands").GetRawText(), slim.GetProperty("fleet").GetProperty("bands").GetRawText());
        Assert.Equal(full.GetProperty("watch").GetRawText(), slim.GetProperty("watch").GetRawText());
        Assert.Equal(6, full.GetProperty("servers").GetArrayLength());

        var older = Assert.Single(entries, entry => entry.GetProperty("sweep_id").GetString() != latestId);
        Assert.False(older.TryGetProperty("report_under", out _), "an older entry carries its own document and names nothing else");
        Assert.Equal(6, older.GetProperty("report").GetProperty("servers").GetArrayLength());

        Assert.False(root.GetProperty("truncated").GetBoolean());
        Assert.False(root.GetProperty("over_budget").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
    }

    /// <summary>The measured shape: one sweep in the window whose document leaves <c>latest</c> under the budget on its own,
    /// while latest plus a second copy of that document in the timeline entry is over it. With the document shipped once the
    /// reply is under the budget, nothing is cut, and the reply says so.</summary>
    [Fact]
    public async Task GetSweepReports_ALatestUnderTheBudget_WhoseEntryWouldPushTheReplyOver_Fits()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report measured-shape test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        await SeedSweepsAsync(postgres, sweepCount: 1, serversPerReport: 95, openItems: 2, carriedItems: 0, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var latestBytes = Encoding.UTF8.GetByteCount(root.GetProperty("latest").GetRawText());
        Assert.InRange(latestBytes, McpResponseBudget.DefaultBytes / 2, McpResponseBudget.DefaultBytes);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes with latest at {latestBytes:#,0}, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.False(root.GetProperty("truncated").GetBoolean());
        Assert.False(root.GetProperty("over_budget").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
        Assert.Equal(1, root.GetProperty("sweeps_returned").GetInt32());
        Assert.Equal(2, root.GetProperty("watch_items_returned").GetInt32());
    }

    /// <summary>A past <c>as_of</c>: two sweeps two hours apart, both with 150-server documents, and a window that holds only the
    /// older one. <c>latest</c> (the newer sweep) lies outside the window, so the window's newest entry keeps its full document
    /// and is never cut. The reply is over the budget at its floor, and the OVER BUDGET note names that entry with its size and
    /// says why it is whole, beside latest.</summary>
    [Fact]
    public async Task GetSweepReports_APastAsOf_NamesTheWindowsNewestEntry_WithItsFullDocument_InTheOverBudgetNote()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report past-window test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        var ids = await SeedSweepsAsync(postgres, sweepCount: 2, serversPerReport: 150, openItems: 0, carriedItems: 0, ct, cadence: TimeSpan.FromHours(2));
        var asOf = DateTime.UtcNow.AddMinutes(-91).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, hours_back: 1, as_of: asOf, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal(1, root.GetProperty("count").GetInt32());
        var entry = Assert.Single(root.GetProperty("sweeps").EnumerateArray());
        Assert.Equal(ids[1].ToString(CultureInfo.InvariantCulture), entry.GetProperty("sweep_id").GetString());
        Assert.Equal(ids[0].ToString(CultureInfo.InvariantCulture), root.GetProperty("latest").GetProperty("sweep_id").GetString());
        Assert.False(entry.TryGetProperty("report_under", out _), "the window's newest entry is not the newest sweep, so it carries its own document");
        Assert.Equal(150, entry.GetProperty("report").GetProperty("servers").GetArrayLength());

        Assert.True(root.GetProperty("over_budget").GetBoolean());
        var note = root.GetProperty("budget_note").GetString();
        Assert.NotNull(note);
        Assert.StartsWith("OVER BUDGET", note, StringComparison.Ordinal);
        var entryBytes = Encoding.UTF8.GetByteCount(entry.GetRawText());
        Assert.Contains(string.Create(CultureInfo.InvariantCulture, $"{entryBytes:#,0} bytes, its full document"), note, StringComparison.Ordinal);
        Assert.Contains("lies outside the window", note, StringComparison.Ordinal);
        Assert.DoesNotContain("fit", note, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>A muted newest sweep whose document carries a would_have_paged array: the slim entry keeps it, equal to latest's,
    /// and the Viewer's row builder over the reply reads the newest row's would-have-paged count from it.</summary>
    [Fact]
    public async Task GetSweepReports_AMutedNewestSweep_KeepsWouldHavePagedOnTheSlimEntry_AndTheRowCountsIt()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report muted-row test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        await SeedSweepsAsync(postgres, sweepCount: 2, serversPerReport: 6, openItems: 0, carriedItems: 0, ct, newestMuted: true);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var latest = root.GetProperty("latest");
        Assert.False(latest.GetProperty("alerts_enabled").GetBoolean());
        var newest = Assert.Single(root.GetProperty("sweeps").EnumerateArray(), e => e.GetProperty("sweep_id").GetString() == latest.GetProperty("sweep_id").GetString());
        Assert.Equal("latest", newest.GetProperty("report_under").GetString());
        var slim = newest.GetProperty("report");
        Assert.Equal(new[] { "fleet", "watch", "would_have_paged" }, slim.EnumerateObject().Select(p => p.Name).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(latest.GetProperty("report").GetProperty("would_have_paged").GetRawText(), slim.GetProperty("would_have_paged").GetRawText());
        Assert.Equal(1, slim.GetProperty("would_have_paged").GetArrayLength());

        var rows = FleetSweepsRows.Runs(JsonNode.Parse(json)!.AsObject());
        Assert.Equal(latest.GetProperty("sweep_id").GetString(), rows[0].SweepId);
        Assert.Equal("1 would have paged", rows[0].WouldHavePagedText);
    }

    /* ---------------- seeding ---------------- */

    /// <summary>Sweeps at a 15-minute cadence ending a minute ago, newest id highest, each with a <paramref name="serversPerReport"/>-card
    /// document (about 290 bytes a card); the worklist is written with the newest sweep: <paramref name="openItems"/> open items
    /// (servers 1.., last seen 1.. seconds BEFORE the sweep, so server 1 is the newest open item) then <paramref name="carriedItems"/>
    /// carried ones (last seen seconds AFTER the sweep, newer than every open item), each about 900 bytes on the wire.</summary>
    private static async Task<List<long>> SeedSweepsAsync(
        NpgsqlDataSource postgres, int sweepCount, int serversPerReport, int openItems, int carriedItems, CancellationToken ct,
        TimeSpan? cadence = null, bool newestMuted = false)
    {
        var newest = DateTime.UtcNow.AddMinutes(-1);
        var step = cadence ?? TimeSpan.FromMinutes(15);
        var ids = new List<long>(sweepCount);
        const long idBase = 8_318_000_000_000_000_000L;
        for (var i = 0; i < sweepCount; i++)
        {
            var sweptAt = newest - (step * i);
            var id = idBase + (sweepCount - i);
            ids.Add(id);
            var cards = string.Join(",", Enumerable.Range(1, serversPerReport).Select(n =>
                $"{{\"server\":\"server-{n}\",\"band\":\"healthy\",\"reason\":\"{new string('x', 240)}\"}}"));
            var run = new FleetSweepRun(
                SweepId: id,
                SweptAtUtc: sweptAt,
                SpanStartUtc: sweptAt - step,
                SpanEndUtc: sweptAt,
                PreviousSweepId: i + 1 < sweepCount ? idBase + (sweepCount - i - 1) : null,
                AlertsEnabled: !(i == 0 && newestMuted),
                ServersExpected: serversPerReport,
                ServersReported: serversPerReport,
                InstrumentsAlive: true,
                InstrumentLivenessJson: """{"alert_pass_counter":{"read":true,"advancing":true}}""",
                ReportJson: string.Create(CultureInfo.InvariantCulture,
                    $"{{\"sweep_id\":{id},\"fleet\":{{\"bands\":{{\"Healthy\":{serversPerReport - 1},\"Warning\":1}}}},\"watch\":{{\"opened\":{(i == 0 ? openItems : 0)},\"carried\":{(i == 0 ? carriedItems : 0)},\"closed\":0}},{(i == 0 && newestMuted ? "\"would_have_paged\":[{\"server_id\":2,\"family\":\"cpu\"}]," : string.Empty)}\"servers\":[{cards}]}}"));

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
