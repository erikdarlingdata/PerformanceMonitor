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
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: reads only the scratch database McpEveryReadStore mints. */
/// <summary>
/// Two windows read back to back count every row once. A window is split where its two halves meet, and the three windows (the
/// whole, the first half, the second half) are read for servers that hold one row each at an edge: exactly at the whole window's
/// start, exactly where the halves meet, exactly at the whole window's end. Whichever way a read places a row against its window, a
/// row the whole window counts is counted by one half and only one, and a row the whole window leaves out is counted by neither. A
/// read that sums its rows then answers the same total for the whole window as its two halves add up to. This runs that rule across
/// every read the MCP server lists that takes a server, <c>hours_back</c> and <c>as_of</c> (<see cref="McpEveryReadStore"/>).
///
/// <para>Which edge a window keeps follows from what the read places a row by. A read that places a row by an instant (when it was
/// collected, when the event happened) counts the rows after its start up to and including its end, so the row at the whole
/// window's start is left out, the row where the halves meet belongs to the first half and the row at the end to the second. A read
/// that places a row by the start of an aligned interval (<see cref="s_placedByIntervalStart"/>) counts from its start up to and not
/// including its end, so the row at the start is counted, the row where the halves meet belongs to the second half and the row at
/// the end is left out. Each of the three windows is held to the rows its kind counts.</para>
///
/// <para>A window counts a row when its answer for the server that holds the row differs from its answer for the server that holds
/// nothing in the window; the two servers are otherwise the same. A read whose whole window counts the row where the halves meet
/// reads the seeded rows, so it is held to the rule.</para>
///
/// <para>A read may stamp its answer with when its source was last seen, which no window bounds. When a row outside a window changes
/// the answer for it, the rows are counted again with every date and time in the answers set aside. A read whose answer then shows no
/// row counted is reported as not held, with that reason; a read whose rows are still counted, and by their own window alone, is held
/// on that count; and a read where a row outside a window still changes more than a time stamp keeps its finding.</para>
///
/// <para>A read built by design from rows before its window too (<see cref="s_readsBackBeforeItsWindow"/>) is reported as not held, with
/// what it takes from before, and is held only to a row after a window's end not changing the answer for it. So is a read whose
/// window is judged whole (<see cref="s_judgedWhole"/>). No other read is left out by name. A read that differences a counter as it reads
/// (<see cref="s_differencedAtReadTime"/>) is held to the rows it answers from, the row on its start among them. A read the seed does not reach is reported with the reason, taken from its own answer, and
/// <see cref="HeldShare"/> holds a floor under the share of windowed reads the rule was applied to, so the test cannot go quiet by
/// reads dropping out of it. The output lists, for every read held, which of the three rows each window counts.</para>
/// </summary>
[Collection("gap-cache-serial")]
public sealed class McpEveryReadWindowPartitionLiveTests : IClassFixture<McpEveryReadStore>
{
    /// <summary>The least share of the windowed reads the rule is applied to.</summary>
    private const double HeldShare = 0.8;

    /// <summary>The servers that hold one row at an edge, with the letter the output names the row by and the words a finding does.</summary>
    private static readonly (string Placement, char Letter, string Where)[] s_rows =
    [
        (McpEveryReadStore.Start, 'a', "at the whole window's start"),
        (McpEveryReadStore.Mid, 'b', "where the two halves meet"),
        (McpEveryReadStore.End, 'c', "at the whole window's end"),
    ];

    private const string ChangeIsADifference = "a change is the difference between two captures, so the read takes the capture before the window as its baseline";
    private const string JudgedWhole = "an analysis window is judged whole, never summed with its neighbour";
    private const string ByIntervalStart = "a Query Store row is placed by the start of its runtime-statistics interval";
    private const string RegressionJudgedWhole = "a regression window is judged whole against the baseline before it, never summed with its neighbour";
    private const string BaselineOnTheStart = "the row stamped on a window's start is the baseline its first delta is measured from";

    /// <summary>The rows a window counts, by what the read places a row by: (whole, first half, second half).</summary>
    private static readonly (string Whole, string First, string Second) s_byInstant = ("bc", "b", "c");
    private static readonly (string Whole, string First, string Second) s_byIntervalStart = ("ab", "a", "b");
    private static readonly (string Whole, string First, string Second) s_byBaseline = ("abc", "ab", "bc");

    /// <summary>
    /// The reads that place a row by the start of an aligned interval, each with what the interval is. Every other read places a
    /// row by an instant. A name here is held to the rows an interval read counts, so it fails once its read counts otherwise.
    /// </summary>
    private static readonly Dictionary<string, string> s_placedByIntervalStart = new(StringComparer.Ordinal)
    {
        ["get_query_store_aborts"] = ByIntervalStart,
        ["get_query_store_duration_trend"] = ByIntervalStart,
        ["get_query_store_top"] = ByIntervalStart,
    };

    /// <summary>
    /// The reads whose window is judged as one piece: its answer is a verdict on the whole window, set against the history before
    /// it, and no caller adds it to the answer for the window next to it. Such a read is reported as not held, with this reason, and
    /// is still held to one thing: a row after a window's end does not change the answer for it.
    /// </summary>
    private static readonly Dictionary<string, string> s_judgedWhole = new(StringComparer.Ordinal)
    {
        ["analyze_server"] = JudgedWhole,
        ["compare_analysis"] = JudgedWhole,
        ["get_analysis_facts"] = JudgedWhole,
        ["get_query_store_regressions"] = RegressionJudgedWhole,
    };

    /// <summary>
    /// The reads that difference a counter as they read it. The row stamped exactly on a window's start is the baseline the first
    /// delta is measured from, so it changes the answer and no delta of its own is counted. A name here is held to the rows such a
    /// read answers from: the row on its start, the rows inside it and the row on its end, and none before its start or after its
    /// end. It fails once its read answers from other rows.
    /// </summary>
    private static readonly Dictionary<string, string> s_differencedAtReadTime = new(StringComparer.Ordinal)
    {
        ["get_pg_write_stats"] = BaselineOnTheStart,
    };

    /// <summary>
    /// The reads whose answer is built, by design, from rows before the window asked for as well as the rows in it, each with what
    /// its reader takes from before. Such a read is reported as not held, with this reason, and is still held to one thing: a row
    /// after a window's end does not change the answer for it. A name stays here only while a row before a window still changes the
    /// read's answer: when none does, the test fails and says to take the name out.
    /// </summary>
    private static readonly Dictionary<string, string> s_readsBackBeforeItsWindow = new(StringComparer.Ordinal)
    {
        ["get_database_config_changes"] = ChangeIsADifference,
        ["get_server_config_changes"] = ChangeIsADifference,
        ["get_trace_flag_changes"] = ChangeIsADifference,
        ["get_plan_corrections"] = "its automatic_tuning list is the newest capture of each database's setting, which may be older than the window; its rows are read by the window",
    };

    private readonly McpEveryReadStore _store;

    public McpEveryReadWindowPartitionLiveTests(McpEveryReadStore store) => _store = store;

    [Fact]
    public async Task TwoHalvesOfAWindow_CountEachRowTheWholeWindowCounts_ExactlyOnce_OnEveryWindowedRead()
    {
        Assert.SkipWhen(!_store.Available, McpEveryReadStore.SkipReason);

        const int half = McpEveryReadStore.WindowHours / 2;
        (int Hours, DateTime AsOf) whole = (McpEveryReadStore.WindowHours, _store.WindowEnd);
        (int Hours, DateTime AsOf) first = (half, _store.WindowMiddle);
        (int Hours, DateTime AsOf) second = (half, _store.WindowEnd);

        var windowed = _store.Tools.Where(t => t.Takes("server_name") && t.Takes("hours_back") && t.Takes("as_of")).ToList();
        var broken = new List<string>();
        var notHeld = new List<string>();
        var placements = new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
        var held = 0;

        foreach (var tool in windowed)
        {
            string? reason = null;
            var reached = false;
            foreach (var engine in McpEveryReadStore.Engines)
            {
                async Task<McpEveryReadAnswer> ReadAsync((int Hours, DateTime AsOf) window, string placement) => await _store.CallAsync(
                    tool.Name, _store.Arguments(tool, McpEveryReadStore.ServerName(engine, placement), window.Hours, window.AsOf));

                var answers = new Dictionary<(int Hours, DateTime AsOf, string Placement), string>();
                async Task<string> AnswerAsync((int Hours, DateTime AsOf) window, string placement)
                {
                    if (!answers.TryGetValue((window.Hours, window.AsOf, placement), out var text))
                    {
                        answers[(window.Hours, window.AsOf, placement)] = text = McpEveryReadStore.Comparable((await ReadAsync(window, placement)).Text);
                    }

                    return text;
                }

                /* The rows a window counts: its answer for the server that holds the row is not its answer for the server that holds nothing
                   there, as each answer is seen (whole, or with its dates and times set aside). */
                async Task<string> CountedAsync((int Hours, DateTime AsOf) window, Func<string, string> seen)
                {
                    var nothing = seen(await AnswerAsync(window, McpEveryReadStore.Prior));
                    var counted = "";
                    foreach (var (placement, letter, _) in s_rows)
                    {
                        if (seen(await AnswerAsync(window, placement)) != nothing) counted += letter;
                    }

                    return counted;
                }

                var empty = await ReadAsync(whole, McpEveryReadStore.Prior);
                if (empty.IsError || empty.Status is "invalid" or "error" or "not_collected")
                {
                    reason ??= empty.NotDataReason;
                    continue;
                }

                var nothingInWhole = McpEveryReadStore.Comparable(empty.Text);
                if (McpEveryReadStore.Comparable((await ReadAsync(whole, McpEveryReadStore.Prior)).Text) != nothingInWhole)
                {
                    reason = "two identical calls answer differently, so its answers cannot be compared";
                    continue;
                }

                if (McpEveryReadStore.Comparable((await ReadAsync(whole, McpEveryReadStore.Mid)).Text) == nothingInWhole)
                {
                    reason ??= "a seeded row in the middle of the window does not change its answer" + (empty.NotDataReason is { } said ? " (" + said + ")" : "");
                    continue;
                }

                var inWhole = await CountedAsync(whole, AsWritten);
                var inFirst = await CountedAsync(first, AsWritten);
                var inSecond = await CountedAsync(second, AsWritten);
                var timesSetAside = false;
                if (inFirst.Contains('c') || inSecond.Contains('a'))
                {
                    /* A row outside a window changes the answer for it: count again on what the answers say apart from when. */
                    var wholeApartFromWhen = await CountedAsync(whole, McpEveryReadStore.WithoutTimes);
                    if (!wholeApartFromWhen.Contains('b'))
                    {
                        reason ??= "a seeded row changes only a date or time in its answer, in the window asked for or outside it, so its answer shows no row being counted";
                        continue;
                    }

                    var firstApartFromWhen = await CountedAsync(first, McpEveryReadStore.WithoutTimes);
                    var secondApartFromWhen = await CountedAsync(second, McpEveryReadStore.WithoutTimes);
                    if (!firstApartFromWhen.Contains('c') && !secondApartFromWhen.Contains('a'))
                    {
                        (inWhole, inFirst, inSecond, timesSetAside) = (wholeApartFromWhen, firstApartFromWhen, secondApartFromWhen, true);
                    }
                }

                if (s_judgedWhole.TryGetValue(tool.Name, out var whyWhole))
                {
                    reason = whyWhole;
                    if (inFirst.Contains('c'))
                    {
                        broken.Add($"{tool.Name} ({engine}): the row at the whole window's end changes its answer for the first half, which ends before that row [first half {Shown(inFirst)}]: "
                            + FirstDifference(await AnswerAsync(first, McpEveryReadStore.End), await AnswerAsync(first, McpEveryReadStore.Prior)));
                    }

                    break;
                }

                if (s_readsBackBeforeItsWindow.TryGetValue(tool.Name, out var fromBefore))
                {
                    if (inSecond.Contains('a'))
                    {
                        reason = "it reads back before its window by design: " + fromBefore;
                        if (inFirst.Contains('c'))
                        {
                            broken.Add($"{tool.Name} ({engine}): the row at the whole window's end changes its answer for the first half, which ends before that row [first half {Shown(inFirst)}]: "
                            + FirstDifference(await AnswerAsync(first, McpEveryReadStore.End), await AnswerAsync(first, McpEveryReadStore.Prior)));
                        }

                        break;
                    }

                    broken.Add($"{tool.Name} ({engine}): is listed as reading back before its window, and no row before a window changes its answer: take its name out of the list");
                }

                reached = true;
                var placement = $"whole {Shown(inWhole)} | first half {Shown(inFirst)} | second half {Shown(inSecond)}" + (timesSetAside ? " (dates and times set aside)" : "");
                if (!placements.TryGetValue(placement, out var reads)) placements[placement] = reads = [];
                reads.Add(tool.Name);

                var byBaseline = s_differencedAtReadTime.TryGetValue(tool.Name, out var baseline);
                foreach (var (_, letter, where) in s_rows)
                {
                    /* A baseline row changes the answer of the window it starts and is counted by the window it ends. */
                    if (byBaseline) break;
                    var halves = (inFirst.Contains(letter) ? 1 : 0) + (inSecond.Contains(letter) ? 1 : 0);
                    var problem = (inWhole.Contains(letter), halves) switch
                    {
                        (true, 2) => "is counted by both halves",
                        (true, 0) => "is counted by the whole window and by neither half",
                        (false, > 0) => "is counted by a half and not by the whole window",
                        _ => null,
                    };
                    if (problem is not null) broken.Add($"{tool.Name} ({engine}): the row {where} {problem} [{placement}]");
                }

                var byInterval = s_placedByIntervalStart.TryGetValue(tool.Name, out var interval);
                var counts = byInterval ? s_byIntervalStart : byBaseline ? s_byBaseline : s_byInstant;
                var kind = byInterval ? "the start of an interval (" + interval + ")" : byBaseline ? "an instant and differences a counter as it reads (" + baseline + ")" : "an instant";
                foreach (var (window, counted, expected) in new[] { ("the whole window", inWhole, counts.Whole), ("the first half", inFirst, counts.First), ("the second half", inSecond, counts.Second) })
                {
                    if (counted != expected)
                    {
                        broken.Add($"{tool.Name} ({engine}): {window} counts {Shown(counted)}, and a read that places a row by {kind} counts {expected} [{placement}]");
                    }
                }

                break;
            }

            if (reached) held++;
            else notHeld.Add($"{tool.Name}: {reason}");
        }

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{_store.Tools.Count} tools listed, {windowed.Count} take a server, hours_back and as_of, the rule was applied to {held}; "
            + $"{_store.SeededTableCount} tables seeded, {_store.UnseededTables.Count} not."
            + Environment.NewLine + "Rows each window counts (a at the whole window's start, b where the halves meet, c at its end), by the reads that count them so:"
            + Environment.NewLine + string.Join(Environment.NewLine, placements.Select(p => $"{p.Key}: {p.Value.Count} read(s): {string.Join(", ", p.Value)}"))
            + Environment.NewLine + "Not held to the rule:" + Environment.NewLine + string.Join(Environment.NewLine, notHeld)
            + Environment.NewLine + "Tables the seed could not fill:" + Environment.NewLine
            + string.Join(Environment.NewLine, _store.UnseededTables.Select(t => t.Key + ": " + t.Value)));

        Assert.True(broken.Count == 0,
            $"{broken.Count} finding(s) on {broken.Select(b => b[..b.IndexOf(' ')]).Distinct().Count()} read(s): the two halves of a window do not count each row the whole window counts exactly once, or a window does not count the rows its kind of read counts:"
            + Environment.NewLine + string.Join(Environment.NewLine, broken));
        Assert.True(windowed.Count > 0 && held >= windowed.Count * HeldShare,
            $"The rule was applied to only {held} of the {windowed.Count} windowed reads, under the floor of {HeldShare:P0}. Not held:"
            + Environment.NewLine + string.Join(Environment.NewLine, notHeld));
    }

    private static string Shown(string counted) => counted.Length == 0 ? "none" : counted;

    /// <summary>Where the answer with a row first parts from the answer without it, as a short excerpt of each.</summary>
    /// <summary>
    /// A caller that hands a stored row's own time back as its window keeps that row. <c>get_active_queries</c> lists a snapshot with its
    /// <c>collection_time</c>, and asked for that <c>collection_time</c> it answers the snapshot's rows: the window it reads starts one
    /// microsecond before the instant, because a window leaves out the row at its own start.
    /// </summary>
    [Fact]
    public async Task ASnapshotAskedForByTheCollectionTimeTheReadReturned_IsAnswered()
    {
        Assert.SkipWhen(!_store.Available, McpEveryReadStore.SkipReason);

        var server = McpEveryReadStore.ServerName(McpEveryReadStore.SqlServer, McpEveryReadStore.Mid);
        var listed = await _store.CallAsync("get_active_queries", new Dictionary<string, object?>
        {
            ["server_name"] = server,
            ["hours_back"] = McpEveryReadStore.WindowHours,
            ["as_of"] = McpEveryReadStore.AsOfText(_store.WindowEnd),
        });
        Assert.False(listed.IsError, listed.Text);
        var stamps = CollectionTimes(listed.Text);
        Assert.NotEmpty(stamps);

        var asked = stamps[0];
        var one = await _store.CallAsync("get_active_queries", new Dictionary<string, object?>
        {
            ["server_name"] = server,
            ["collection_time"] = asked,
        });
        Assert.False(one.IsError, one.Text);
        var answered = CollectionTimes(one.Text);
        Assert.NotEmpty(answered);
        Assert.All(answered, stamp => Assert.Equal(asked, stamp));
    }

    /// <summary>The <c>collection_time</c> of each row under <c>queries</c> in a <c>get_active_queries</c> reply, as the reply printed it.</summary>
    private static List<string> CollectionTimes(string reply)
    {
        using var document = JsonDocument.Parse(reply);
        if (document.RootElement.ValueKind != JsonValueKind.Object
            || !document.RootElement.TryGetProperty("queries", out var queries)
            || queries.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        return queries.EnumerateArray()
            .Select(row => row.TryGetProperty("collection_time", out var stamp) ? stamp.GetString() : null)
            .OfType<string>()
            .ToList();
    }

    private static string FirstDifference(string with, string without)
    {
        var at = 0;
        while (at < with.Length && at < without.Length && with[at] == without[at]) at++;
        var from = Math.Max(0, at - 80);
        return $"with the row ...{with[from..Math.Min(with.Length, at + 80)]}... and without it ...{without[from..Math.Min(without.Length, at + 80)]}...";
    }

    private static string AsWritten(string answer) => answer;
}
