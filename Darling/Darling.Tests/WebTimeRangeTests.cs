/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web time range picker (#5562) under Node, skipped where Node is not installed. The grammar module
/// (<c>wwwroot/js/time-range.js</c>) runs every row of the shared fixture the C# parser runs
/// (<c>Fixtures/time-range-cases.json</c>), so the two cannot drift. The picker's DOM
/// (<c>time-range-picker.js</c>) runs in a stand-in DOM, and the server page's use of it runs through the kept-history harness.
/// </summary>
public sealed class WebTimeRangeTests
{
    private static readonly string s_jsDir = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");

    /// <summary>Runs a harness script and parses the one line of JSON it prints; skips when Node is missing.</summary>
    private static bool TryRunNode(string script, out JsonElement result, params string[] args)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", script));
        foreach (var arg in args)
        {
            psi.ArgumentList.Add(arg);
        }

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the harness " + script + " did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the harness " + script + " failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    /// <summary>Every row of the shared fixture, through the web module: start, end, live, the echo text and the error code.</summary>
    [Fact]
    public void TheWebModule_RunsEveryRowOfTheSharedFixture()
    {
        var fixturePath = PathTo("Darling", "Darling.Tests", "Fixtures", "time-range-cases.json");
        if (!TryRunNode("time-range-harness.mjs", out var r, s_jsDir, fixturePath)) return;

        using var fixture = JsonDocument.Parse(System.IO.File.ReadAllText(fixturePath));
        var rows = fixture.RootElement.GetProperty("cases").GetArrayLength();
        Assert.True(rows >= 100, "The shared fixture has " + rows + " rows; it held 100 when the web module was written.");
        Assert.Equal(rows, r.GetProperty("total").GetInt32());
        Assert.Equal(fixture.RootElement.GetProperty("minimumSpanMinutes").GetInt32(), r.GetProperty("minimumSpanMinutes").GetInt32());

        var failures = r.GetProperty("failures").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.True(failures.Length == 0, failures.Length + " fixture rows differ in the web module:\n" + string.Join("\n", failures.Take(20)));
        Assert.Equal(rows, r.GetProperty("passed").GetInt32());
    }

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    /// <summary>The closed picker: the button names the range, the resolved start, end and zone sit beside it, and the reads get a
    /// whole-hours window.</summary>
    [Fact]
    public void TheClosedPicker_NamesTheRange_AndShowsTheResolvedStartEndAndZone()
    {
        if (!TryRunNode("time-range-picker-harness.mjs", out var r, s_jsDir)) return;

        var closed = r.GetProperty("closed");
        Assert.Equal("Past day", closed.GetProperty("button").GetString());
        Assert.Equal("Oct 7, 7:01 am - Oct 8, 7:01 am (UTC-04:00)", closed.GetProperty("detail").GetString());
        Assert.Equal("Time range: Past day, Oct 7, 7:01 am - Oct 8, 7:01 am (UTC-04:00)", closed.GetProperty("ariaLabel").GetString());
        Assert.Equal("false", closed.GetProperty("expanded").GetString());
        Assert.Equal("dialog", closed.GetProperty("haspopup").GetString());
        Assert.Equal(24, closed.GetProperty("windowHours").GetInt32());
        Assert.Equal(JsonValueKind.Null, closed.GetProperty("windowAsOf").ValueKind);
    }

    /// <summary>The popup lists the nine short presets and the eight calendar periods with their current lengths, greys out what is
    /// longer than the reach with the reason, and carries accessible names.</summary>
    [Fact]
    public void TheOpenPopup_ListsEveryPresetAndPeriod_AndGreysOutWhatTheReadsCannotReach()
    {
        if (!TryRunNode("time-range-picker-harness.mjs", out var r, s_jsDir)) return;

        var open = r.GetProperty("open");
        Assert.Equal("true", open.GetProperty("expanded").GetString());
        Assert.Equal("dialog", open.GetProperty("role").GetString());
        Assert.Equal("Time range options", open.GetProperty("dialogLabel").GetString());
        Assert.Equal("Type a time range", open.GetProperty("textLabel").GetString());
        Assert.Equal(new[] { "Range start", "Range end" }, Strings(open, "pickLabels"));
        Assert.True(open.GetProperty("focusedText").GetBoolean());
        Assert.Equal(1, open.GetProperty("documentMousedownListeners").GetInt32());
        Assert.Equal("2026-10-07T07:01", open.GetProperty("from").GetString());
        Assert.Equal("2026-10-08T07:01", open.GetProperty("to").GetString());

        var names = open.GetProperty("items").EnumerateArray().Select(i => i.GetProperty("name").GetString()).ToArray();
        Assert.Equal(
            new[]
            {
                "Past 5 minutes", "Past 15 minutes", "Past 30 minutes", "Past hour", "Past 4 hours", "Past day", "Past 2 days", "Past week", "Past 30 days",
                "Today", "Yesterday", "Week to Date", "Previous Week", "Month to Date", "Previous Month", "Year to Date", "Previous Year",
            },
            names);

        /* At Oct 8 7:01 am the week so far is 3d, last week is exactly the 7 days the reads reach, and everything longer is greyed out. */
        var lengths = open.GetProperty("items").EnumerateArray().ToDictionary(i => i.GetProperty("name").GetString()!, i => i.GetProperty("length").GetString());
        Assert.Equal("7h 1m", lengths["Today"]);
        Assert.Equal("1d", lengths["Yesterday"]);
        Assert.Equal("3d", lengths["Week to Date"]);
        Assert.Equal("7d", lengths["Previous Week"]);
        var disabled = Strings(open, "disabled").Select(d => d.Split(" | ")[0]).ToArray();
        Assert.Equal(new[] { "Past 30 days", "Month to Date", "Previous Month", "Year to Date", "Previous Year" }, disabled);
        Assert.All(Strings(open, "disabled"), d => Assert.Contains("reach at most 7 days", d));
        Assert.Contains(open.GetProperty("items").EnumerateArray(), i => i.GetProperty("name").GetString() == "Past day" && i.GetProperty("pressed").GetString() == "true");
    }

    /// <summary>Typing shows the parsed range before Apply; Enter applies; a range under five minutes, past the reach, or nonsense is
    /// refused with the reason, Apply is disabled and Enter does nothing.</summary>
    [Fact]
    public void TheTextBox_PreviewsBeforeApply_AppliesOnEnter_AndRefusesWithTheReason()
    {
        if (!TryRunNode("time-range-picker-harness.mjs", out var r, s_jsDir)) return;

        var ok = r.GetProperty("typedOk");
        Assert.Equal("45m  Oct 8, 6:16 am - Oct 8, 7:01 am (UTC-04:00)", ok.GetProperty("preview").GetString());
        Assert.Contains("trp-ok", ok.GetProperty("previewClass").GetString());
        Assert.False(ok.GetProperty("applyDisabled").GetBoolean());

        var applied = r.GetProperty("typedApplied");
        var change = Assert.Single(applied.GetProperty("changes").EnumerateArray());
        Assert.Equal("45m", change.GetProperty("id").GetString());
        Assert.Equal("Past 45 minutes", applied.GetProperty("button").GetString());
        Assert.False(applied.GetProperty("open").GetBoolean());
        Assert.Equal(0, applied.GetProperty("docListeners").GetInt32());
        /* A sub-hour range is read as the whole hour that holds it, live (no end time), and trimmed by the page. */
        Assert.Equal(1, applied.GetProperty("window").GetProperty("hours").GetInt32());
        Assert.Equal(JsonValueKind.Null, applied.GetProperty("window").GetProperty("asOf").ValueKind);

        Assert.Equal("That range is only 3m long. The shortest range is 5 minutes.", r.GetProperty("tooShort").GetProperty("preview").GetString());
        Assert.True(r.GetProperty("tooShort").GetProperty("applyDisabled").GetBoolean());
        Assert.Equal("This page's reads reach at most 7 days back.", r.GetProperty("tooLong").GetProperty("preview").GetString());
        Assert.Contains("trp-error", r.GetProperty("nonsense").GetProperty("previewClass").GetString());
        Assert.True(r.GetProperty("nonsense").GetProperty("applyDisabled").GetBoolean());
        Assert.True(r.GetProperty("empty").GetProperty("applyDisabled").GetBoolean());
        Assert.Equal(0, r.GetProperty("refusedChanges").GetInt32());
        Assert.True(r.GetProperty("stillOpen").GetBoolean());
    }

    /// <summary>Esc closes the popup from the text box and returns focus to the button; a press outside closes it without a change,
    /// one inside does not; the document listener exists only while the popup is open.</summary>
    [Fact]
    public void TheKeyboardAndFocus_EscClosesAndReturnsFocus_AndTheDocumentListenerGoesWithThePopup()
    {
        if (!TryRunNode("time-range-picker-harness.mjs", out var r, s_jsDir)) return;

        var esc = r.GetProperty("escape");
        Assert.False(esc.GetProperty("open").GetBoolean());
        Assert.True(esc.GetProperty("focusOnButton").GetBoolean());
        Assert.Equal("false", esc.GetProperty("expanded").GetString());
        Assert.Equal(0, esc.GetProperty("popups").GetInt32());
        Assert.Equal(0, esc.GetProperty("docListeners").GetInt32());

        var outside = r.GetProperty("outside");
        Assert.True(outside.GetProperty("stillOpenAfterInside").GetBoolean());
        Assert.False(outside.GetProperty("openAfterOutside").GetBoolean());
        Assert.Equal(0, outside.GetProperty("changes").GetInt32());
        Assert.Equal(0, outside.GetProperty("docListeners").GetInt32());
    }

    /// <summary>A preset applies at once, a greyed-out one does nothing, a calendar period holds its spec; the date boxes write the
    /// typed form and preview it.</summary>
    [Fact]
    public void ThePresetsAndThePick_ApplyHoldAndPreview()
    {
        if (!TryRunNode("time-range-picker-harness.mjs", out var r, s_jsDir)) return;

        var presets = r.GetProperty("presets");
        Assert.Equal("Past 15 minutes", presets.GetProperty("afterPreset").GetProperty("button").GetString());
        Assert.Equal("15m", Assert.Single(presets.GetProperty("afterPreset").GetProperty("changes").EnumerateArray()).GetProperty("id").GetString());
        Assert.Equal(1, presets.GetProperty("afterDisabled").GetProperty("changes").GetInt32());
        Assert.True(presets.GetProperty("afterDisabled").GetProperty("open").GetBoolean());
        var period = presets.GetProperty("afterPeriod");
        Assert.Equal("Previous Week", period.GetProperty("button").GetString());
        Assert.Equal("Sep 28, 12:00 am - Oct 5, 12:00 am (UTC-04:00)", period.GetProperty("detail").GetString());
        Assert.Equal(168, period.GetProperty("window").GetProperty("hours").GetInt32());
        Assert.Equal("2026-10-05T04:00:00.000Z", period.GetProperty("window").GetProperty("asOf").GetString());

        var picked = r.GetProperty("picked");
        Assert.Equal("2026-10-01 13:00 - 2026-10-01 15:00", picked.GetProperty("text").GetString());
        Assert.Equal("2h  Oct 1, 1:00 pm - Oct 1, 3:00 pm (UTC-04:00)", picked.GetProperty("preview").GetString());
    }

    /// <summary>Compact moves the detail into the tooltip; the sample-interval note shows when the span holds fewer than three samples
    /// and the data-start note when the range starts well before the data; select() refuses a range the page cannot take; the catalog's
    /// max_hours is the reach; a live range slides on refresh.</summary>
    [Fact]
    public void TheNotesTheReachAndTheClock_FollowTheirInputs()
    {
        if (!TryRunNode("time-range-picker-harness.mjs", out var r, s_jsDir)) return;

        Assert.Equal(string.Empty, r.GetProperty("compact").GetProperty("detail").GetString());
        Assert.Equal("Past day, Oct 7, 7:01 am - Oct 8, 7:01 am (UTC-04:00)", r.GetProperty("compact").GetProperty("title").GetString());
        Assert.Equal("Data here is collected every 5 minutes.", r.GetProperty("notes").GetProperty("short").GetString());
        Assert.Equal("Data starts Oct 8, 5:01 am", r.GetProperty("notes").GetProperty("wide").GetString());

        var select = r.GetProperty("select");
        Assert.Contains("7 days", select.GetProperty("tooLong").GetString());
        Assert.Contains("shortest range is 5 minutes", select.GetProperty("tooShort").GetString());
        Assert.Equal(JsonValueKind.Null, select.GetProperty("ok").ValueKind);
        Assert.Equal(new[] { "1h" }, Strings(select, "changes"));

        var reach = r.GetProperty("reach");
        Assert.Equal(2160, reach.GetProperty("reach").GetInt32());
        Assert.Equal(168, reach.GetProperty("none").GetInt32());
        Assert.Equal(168, reach.GetProperty("absent").GetInt32());
        Assert.Equal(new[] { "Year to Date", "Previous Year" }, Strings(reach, "items"));

        var slides = r.GetProperty("slides");
        Assert.Equal("Oct 8, 6:01 am - Oct 8, 7:01 am (UTC-04:00)", slides.GetProperty("first").GetString());
        Assert.Equal("Oct 8, 7:01 am - Oct 8, 8:01 am (UTC-04:00)", slides.GetProperty("second").GetString());
        Assert.Equal("Previous Month", slides.GetProperty("afterSetSpec").GetString());
        Assert.Equal("previous-month", slides.GetProperty("specId").GetString());
    }

    /// <summary>On the server page a range under an hour is fetched as the whole hour that holds it (the reads take integer hours) with
    /// the range's end as <c>as_of</c>, then trimmed to the exact pair; the label says where the hour's totals begin.</summary>
    [Fact]
    public void ARangeUnderAnHour_IsFetchedAsTheWholeHourAndTrimmedToTheExactPair()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("pickerSubHour", out var r)) return;

        var found = r.GetProperty("found");
        Assert.Equal(JsonValueKind.Null, found.GetProperty("err").ValueKind);
        Assert.Equal(1, found.GetProperty("hours").GetInt32());
        Assert.True(found.GetProperty("custom").GetBoolean());
        Assert.Equal("/api/read/get_cpu_utilization?server=SRV1&hours=1&as_of=2026-01-02T10%3A30%3A00.000Z", Assert.Single(Strings(found, "reads")));
        /* The five rows run 06:00 to 11:00; the picked 10:00 to 10:30 holds the 10:00 row only. */
        Assert.Equal(1, found.GetProperty("rows").GetInt32());
        Assert.Contains("totals and rankings aggregate from", found.GetProperty("label").GetString());
    }

    /// <summary>Every preset and calendar period, held for a server: the whole-hour presets within the reach are the page's own range
    /// (no trimming), the sub-hour presets and the periods are custom ranges, and what is longer than 7 days is refused and leaves the
    /// range as it was.</summary>
    [Fact]
    public void EveryPresetAndPeriod_IsHeldOrRefusedByTheServerPage()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("pickerPresetsLocal:America/New_York", out var r)) return;

        var found = r.GetProperty("found");
        foreach (var id in new[] { "1h", "4h", "1d", "2d", "1w" })
        {
            Assert.Equal(JsonValueKind.Null, found.GetProperty(id).GetProperty("err").ValueKind);
            Assert.False(found.GetProperty(id).GetProperty("custom").GetBoolean(), id + " is a whole-hour preset and needs no trimming.");
        }

        Assert.Equal(48, found.GetProperty("2d").GetProperty("hours").GetInt32());
        Assert.Equal("last 7 days", found.GetProperty("1w").GetProperty("label").GetString());
        foreach (var id in new[] { "5m", "15m", "30m", "today", "yesterday", "week-to-date", "previous-week" })
        {
            Assert.Equal(JsonValueKind.Null, found.GetProperty(id).GetProperty("err").ValueKind);
            Assert.True(found.GetProperty(id).GetProperty("custom").GetBoolean(), id + " is fetched as whole hours and trimmed.");
        }

        Assert.Equal(1, found.GetProperty("5m").GetProperty("hours").GetInt32());
        Assert.Equal(1, found.GetProperty("30m").GetProperty("hours").GetInt32());
        Assert.Equal(168, found.GetProperty("previous-week").GetProperty("hours").GetInt32());
        foreach (var id in new[] { "1mo", "month-to-date", "previous-month", "year-to-date", "previous-year" })
        {
            Assert.Contains("7 days", found.GetProperty(id).GetProperty("err").GetString());
        }
    }
}
