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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ChangeWatchDetectionLiveTests;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #6581 slice 2: Change Watch records a Query Store hint set, changed or cleared (<c>query_store_hint</c>), read from two consecutive complete captures
/// of a database in <c>collect.query_store_hints</c> the way forced-plan changes are read, and a watch of hint changes alone measures those queries.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchHintLiveTests
{
    /* The collector writes ONE all-NULL marker row per captured database per pass, then one row per hint. */
    private static Task MarkerAsync(NpgsqlConnection c, DateTime at, string database, CancellationToken ct) =>
        SeedAsync(c, "query_store_hints", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(at), ["database_name"] = database,
        }, ct);

    private static Task HintAsync(NpgsqlConnection c, DateTime at, string database, long hintId, long queryId, string text, CancellationToken ct) =>
        SeedAsync(c, "query_store_hints", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(at), ["database_name"] = database,
            ["query_hint_id"] = hintId, ["query_id"] = queryId, ["query_hint_text"] = text, ["source_desc"] = "USER",
        }, ct);

    /// <summary>One capture of one database exactly as the collector writes it: the marker row, then each hint.</summary>
    private static async Task CaptureAsync(NpgsqlConnection c, DateTime at, string database, CancellationToken ct, params (long Hint, long Query, string Text)[] hints)
    {
        await MarkerAsync(c, at, database, ct);
        foreach (var (hint, query, text) in hints)
        {
            await HintAsync(c, at, database, hint, query, text, ct);
        }
    }

    private static bool IsHint(ChangeWatchChange c) => c.Kind == "query_store_hint";

    [Fact]
    public async Task ANewHintInTheLaterCapture_IsOneSetChange_WithTheQueryDatabaseAndText()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-45);
            await CaptureAsync(h.Connection, first, "Sales", ct);
            await CaptureAsync(h.Connection, second, "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-40), "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));   /* still there: no second change */

            var found = (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Where(IsHint).ToList();

            var change = Assert.Single(found);
            Assert.Equal("query_store_hint:set:Sales:42:1:" + Stamp(second), change.Key);
            Assert.Equal("Sales", change.DatabaseName);
            Assert.Equal(42, change.QueryId);
            Assert.Null(change.PlanId);
            Assert.Contains("query 42", change.Description, StringComparison.Ordinal);
            Assert.Contains("Sales", change.Description, StringComparison.Ordinal);
            Assert.Contains("OPTION (MAXDOP 1)", change.Description, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ADifferentHintText_ForTheSameHintId_IsOneChangedChange_WithBothTexts()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-45);
            await CaptureAsync(h.Connection, first, "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));
            await CaptureAsync(h.Connection, second, "Sales", ct, (1, 42, "OPTION (MAXDOP 4)"));

            var found = (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Where(IsHint).ToList();

            var change = Assert.Single(found);
            Assert.Equal("query_store_hint:changed:Sales:42:1:" + Stamp(second), change.Key);
            Assert.Contains("OPTION (MAXDOP 1)", change.Description, StringComparison.Ordinal);
            Assert.Contains("OPTION (MAXDOP 4)", change.Description, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AHintGoneFromTheLaterCapture_IsOneClearedChange_ButAnUnchangedHintIsNothing()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-45);
            await CaptureAsync(h.Connection, first, "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"), (2, 43, "OPTION (RECOMPILE)"));
            await CaptureAsync(h.Connection, second, "Sales", ct, (2, 43, "OPTION (RECOMPILE)"));

            var found = (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Where(IsHint).ToList();

            var change = Assert.Single(found);
            Assert.Equal("query_store_hint:cleared:Sales:42:1:" + Stamp(second), change.Key);
            Assert.Equal(42, change.QueryId);
            Assert.Contains("OPTION (MAXDOP 1)", change.Description, StringComparison.Ordinal);
        });
    }

    /* The plan diff skips a force without a query_id; a hint row with a hint id and no query_id is skipped the same way, and the other hints are still read. */
    [Fact]
    public async Task AHintRowWithoutAQueryId_IsNotAChange_AndTheOtherHintsAreStillRead()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-45);
            await CaptureAsync(h.Connection, first, "Sales", ct);
            await CaptureAsync(h.Connection, second, "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));
            await SeedAsync(h.Connection, "query_store_hints", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(second), ["database_name"] = "Sales",
                ["query_hint_id"] = 9L, ["query_hint_text"] = "OPTION (RECOMPILE)", ["source_desc"] = "USER",
            }, ct);

            var found = (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Where(IsHint).ToList();

            var change = Assert.Single(found);
            Assert.Equal("query_store_hint:set:Sales:42:1:" + Stamp(second), change.Key);
        });
    }

    /* A capture with no marker row is not a complete capture: it is never the side of a diff, and the change it seems to hold is not read. */
    [Fact]
    public async Task ACaptureWithoutAMarkerRow_IsNotDiffed()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            await CaptureAsync(h.Connection, first, "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));
            await HintAsync(h.Connection, h.Now.AddMinutes(-45), "Sales", 1, 42, "OPTION (MAXDOP 4)", ct);      /* no marker: the pass died part way */
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-40), "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));

            Assert.DoesNotContain(await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct), IsHint);
        });
    }

    /* A previous capture more than the gap limit before this one (30 minutes at the 5-minute default) is a new baseline: the change cannot be dated. */
    [Fact]
    public async Task ACaptureAfterALongGap_IsABaseline_NotAChange()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-150);
            await CaptureAsync(h.Connection, first, "Sales", ct);
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-45), "Sales", ct, (1, 42, "OPTION (MAXDOP 1)"));

            Assert.DoesNotContain(await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct), IsHint);
        });
    }

    /* A readable secondary holds the primary's hints, so a hint change in its replicated database is the primary's; the server's other databases still count. */
    [Fact]
    public async Task AHintChange_InADatabaseThisServerHoldsAsASecondary_IsNotRecorded()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-50);
            var second = h.Now.AddMinutes(-45);
            foreach (var database in new[] { "Replicated", "Local" })
            {
                await CaptureAsync(h.Connection, first, database, ct);
                await CaptureAsync(h.Connection, second, database, ct, (1, 42, "OPTION (MAXDOP 1)"));
            }

            Assert.Equal(2, (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Count(IsHint));

            await SeedAsync(h.Connection, "ag_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddMinutes(-5)), ["ag_name"] = "AG1",
                ["replica_server_name"] = ServerName, ["role_desc"] = "SECONDARY", ["is_local"] = true,
            }, ct);
            await SeedAsync(h.Connection, "ag_database_replica_states", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(h.Now.AddMinutes(-5)), ["ag_name"] = "AG1",
                ["database_name"] = "Replicated", ["replica_server_name"] = ServerName, ["is_local"] = true,
            }, ct);

            var found = (await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct)).Where(IsHint).ToList();
            Assert.Equal(new[] { "query_store_hint:set:Local:42:1:" + Stamp(second) }, Keys(found));
        });
    }

    /* A watch of hint changes alone measures the changed query from Query Store, as a watch of plan changes alone does. */
    [Fact]
    public async Task AHintOnlyWatch_MeasuresTheChangedQuery()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            var before = change.AddMinutes(-65);        /* ends 5 minutes before the change: the baseline, 1 ms */
            var after = change.AddMinutes(55);          /* starts inside the watch: 9 ms */
            await SeedIntervalsAsync(h.Connection, new[] { before }, "Sales", 42, 1_000, ct);
            await SeedIntervalsAsync(h.Connection, new[] { after }, "Sales", 42, 9_000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("query_store_hint:set:Sales:42:1:x", "query_store_hint", change, "Query Store hint 1 set on query 42 in Sales: OPTION (MAXDOP 1)", "Sales", 42),
            }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var json = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!;
            var measure = json["measures"]!.AsArray().Single(m => m!["measure"]!.GetValue<string>() == "query_avg_duration_ms:Sales:42")!;
            Assert.Equal(1.0, measure["before"]!.GetValue<double>(), 3);
            Assert.Equal(9.0, measure["after"]!.GetValue<double>(), 3);
        });
    }

    private static async Task SeedIntervalsAsync(NpgsqlConnection c, IReadOnlyList<DateTime> starts, string database, long queryId, long avgUs, CancellationToken ct)
    {
        var id = avgUs;
        foreach (var start in starts)
        {
            await SeedAsync(c, "query_store_stats", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(start.AddMinutes(60)), ["database_name"] = database,
                ["query_id"] = queryId, ["plan_id"] = 1L, ["runtime_stats_interval_id"] = id++,
                ["first_execution_time"] = Naive(start), ["execution_type_desc"] = "Regular", ["replica_role"] = "PRIMARY",
                ["interval_start_time_utc"] = Naive(start), ["interval_end_time_utc"] = Naive(start.AddMinutes(60)),
                ["execution_count"] = 30L, ["avg_duration_us"] = (double)avgUs,
            }, ct);
        }
    }
}
