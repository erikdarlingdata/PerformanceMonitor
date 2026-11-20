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
/// what it takes from before, and is held only to a row after a window's end not changing the answer for it. No other read is left out by name. A read the seed does not reach is reported with the reason, taken from its own answer, and
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
    private const string ComparesWithHistory = "it sets the window against a baseline of the history before it and against the newest capture of each setting";

    /// <summary>
    /// The reads whose answer is built, by design, from rows before the window asked for as well as the rows in it, each with what
    /// its reader takes from before. Such a read is reported as not held, with this reason, and is still held to one thing: a row
    /// after a window's end does not change the answer for it. A name stays here only while a row before a window still changes the
    /// read's answer: when none does, the test fails and says to take the name out.
    /// </summary>
    private static readonly Dictionary<string, string> s_readsBackBeforeItsWindow = new(StringComparer.Ordinal)
    {
        ["analyze_server"] = ComparesWithHistory,
        ["compare_analysis"] = ComparesWithHistory,
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

                if (s_readsBackBeforeItsWindow.TryGetValue(tool.Name, out var fromBefore))
                {
                    if (inSecond.Contains('a'))
                    {
                        reason = "it reads back before its window by design: " + fromBefore;
                        if (inFirst.Contains('c'))
                        {
                            broken.Add($"{tool.Name} ({engine}): the row at the whole window's end changes its answer for the first half, which ends before that row [first half {Shown(inFirst)}]");
                        }

                        break;
                    }

                    broken.Add($"{tool.Name} ({engine}): is listed as reading back before its window, and no row before a window changes its answer: take its name out of the list");
                }

                reached = true;
                var placement = $"whole {Shown(inWhole)} | first half {Shown(inFirst)} | second half {Shown(inSecond)}" + (timesSetAside ? " (dates and times set aside)" : "");
                if (!placements.TryGetValue(placement, out var reads)) placements[placement] = reads = [];
                reads.Add(tool.Name);

                foreach (var (_, letter, where) in s_rows)
                {
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
            $"{broken.Count} finding(s) on {broken.Select(b => b[..b.IndexOf(' ')]).Distinct().Count()} read(s): the two halves of a window do not count each row the whole window counts exactly once:"
            + Environment.NewLine + string.Join(Environment.NewLine, broken));
        Assert.True(windowed.Count > 0 && held >= windowed.Count * HeldShare,
            $"The rule was applied to only {held} of the {windowed.Count} windowed reads, under the floor of {HeldShare:P0}. Not held:"
            + Environment.NewLine + string.Join(Environment.NewLine, notHeld));
    }

    private static string Shown(string counted) => counted.Length == 0 ? "none" : counted;

    private static string AsWritten(string answer) => answer;
}
