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
/// The web stored-plan viewer (#4843), from the shipped <c>plan-viewer.js</c> run under Node
/// (<c>web-plan-viewer-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class PlanViewerBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plan-viewer-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the plan viewer harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the plan viewer harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    [Fact]
    public void ThePlanButton_ReadsTheStoredPlan_AndShowsItIndented()
    {
        var r = Run("open");
        Assert.True(r.GetProperty("buttonBefore").GetBoolean());
        Assert.Equal("/api/read/get_plan_xml", Str(r, "path"));
        var q = r.GetProperty("query");
        Assert.Equal("srv-a", q.GetProperty("server").GetString());
        Assert.Equal("0xABCDEF0123456789", q.GetProperty("query_hash").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Contains("Loading", Str(r, "loading"));
        Assert.Equal("pre", Str(r, "preTag"));
        Assert.StartsWith("<ShowPlanXML", Str(r, "pre"));
        Assert.Contains("\n  <BatchSequence>\n    <Batch>", Str(r, "pre"));
        Assert.True(r.GetProperty("hasCopy").GetBoolean());
        Assert.True(r.GetProperty("hasDownload").GetBoolean());
        Assert.Equal("\u2014", Str(r, "noHashCell"));
        Assert.True(r.GetProperty("closed").GetBoolean());
    }

    [Fact]
    public void ThePrettyPrint_IsText_NotMarkup()
    {
        var r = Run("textOnly");
        Assert.Equal(0, r.GetProperty("preChildren").GetInt32());
        Assert.Equal(0, r.GetProperty("scriptElements").GetInt32());
        // The entity-escaped script text in the statement attribute stays escaped text, character for character.
        Assert.Contains("&lt;script&gt;", Str(r, "text"));
        Assert.True(r.GetProperty("lines").GetInt32() > 5);
        Assert.Equal("<a>\n  <b x=\"1>2\">t</b>\n  <c/>\n  <d>\n    <e/>\n  </d>\n</a>", Str(r, "pretty"));
        Assert.Equal("not xml <at all", Str(r, "notXml"));
    }

    [Fact]
    public void Download_IsAnXmlBlob_NamedForTheQueryHash_HoldingTheStoredXml()
    {
        var r = Run("download");
        Assert.Equal("0xABCDEF0123456789.sqlplan", Str(r, "name"));
        Assert.Equal("application/xml", Str(r, "type"));
        Assert.True(r.GetProperty("bodyIsStored").GetBoolean());
        Assert.Equal("a_b_c.sqlplan", Str(r, "safeName"));
    }

    [Fact]
    public void Copy_PutsTheShownTextOnTheClipboard()
    {
        var r = Run("copy");
        Assert.True(r.GetProperty("matchesShown").GetBoolean());
        Assert.Contains("\n  <BatchSequence>", Str(r, "copied"));
    }

    [Fact]
    public void ATruncatedPlan_SaysSo_AndTurnsDownloadOff()
    {
        var r = Run("truncated");
        Assert.True(r.GetProperty("disabled").GetBoolean());
        Assert.Equal(0, r.GetProperty("downloads").GetInt32());
        Assert.Contains("500 KB", Str(r, "notice"));
        Assert.Contains("Download is turned off", Str(r, "notice"));
        Assert.False(r.GetProperty("shown").GetBoolean());
        Assert.True(r.GetProperty("copyOn").GetBoolean());
    }

    [Fact]
    public void AQueryWithNoStoredPlan_SaysSo_WithNoDownload()
    {
        var r = Run("noPlan");
        Assert.Contains("No stored plan found", Str(r, "text"));
        Assert.False(r.GetProperty("hasPre").GetBoolean());
        Assert.False(r.GetProperty("hasDownload").GetBoolean());
    }

    [Fact]
    public void AnOpenPanel_SurvivesTheRebuild_WithoutAnotherRead()
    {
        var r = Run("rebuild");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsPlan").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
        Assert.False(r.GetProperty("otherServerOpen").GetBoolean());
        Assert.True(r.GetProperty("loadingInNew").GetBoolean());
        Assert.True(r.GetProperty("filledNew").GetBoolean());
    }

    private static string[] Strs(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void EachRowKind_ReadsItsOwnTool_WithTheRowsKeyExactlyAsHeld()
    {
        var r = Run("kinds");

        Assert.Equal(new[] { "has_query_plan", "has_live_query_plan" }, Strs(r, "activeKeys"));
        Assert.Equal(new[] { "Plan", "Live plan" }, Strs(r, "activeLabels"));
        Assert.True(r.GetProperty("activeHide").GetBoolean());
        Assert.True(r.GetProperty("planColsNoFilter").GetBoolean());
        Assert.True(r.GetProperty("planColsNoFilterNoCopy").GetBoolean(), "every plan column, planColumn and the blocking and deadlock columns included, sets filter, copy and csv to false: an open panel's XML is the cell's text");

        Assert.Equal("/api/read/get_active_query_plan_xml", Str(r, "activePath"));
        var est = r.GetProperty("activeQuery");
        Assert.Equal("srv-a", Str(est, "server"));
        /* The timestamp is the row's own string, all seven fractional digits, untouched. */
        Assert.Equal("2026-03-04T05:06:07.1234560", Str(est, "collection_time"));
        Assert.Equal("57", Str(est, "session_id"));
        Assert.Equal("3", Str(est, "request_id"));
        Assert.False(est.TryGetProperty("live", out _));

        var live = r.GetProperty("liveQuery");
        Assert.Equal("true", Str(live, "live"));
        Assert.Equal("0", Str(live, "request_id"));       /* a row with no request_id reads as 0 */
        Assert.Equal("\u2014", Str(r, "noFlagCell"));
        Assert.Equal("\u2014", Str(r, "noLiveFlagCell"));

        Assert.Equal("query_id", Str(r, "qsKey"));
        Assert.Equal("/api/read/get_query_store_plan_xml", Str(r, "qsPath"));
        var qs = r.GetProperty("qsQuery");
        Assert.Equal("Orders", Str(qs, "database_name"));
        Assert.Equal("42", Str(qs, "query_id"));
        Assert.Equal("7", Str(qs, "plan_id"));
        Assert.False(r.GetProperty("qsNoPlanIdQuery").TryGetProperty("plan_id", out _));
        Assert.Equal("\u2014", Str(r, "qsNoKey"));

        Assert.Equal("sql_handle", Str(r, "procKey"));
        Assert.True(r.GetProperty("procHide").GetBoolean());
        Assert.Equal("/api/read/get_procedure_plan_xml", Str(r, "procPath"));
        Assert.Equal("0x0300050011223344", Str(r.GetProperty("procQuery"), "sql_handle"));
        Assert.Equal("\u2014", Str(r, "procNoHandle"));

        // Blocking (#5236): two columns, keyed on their own flags. The query string is the row's strings in the read's
        // documented order; the blocked side sends no side, the blocking side sends side=blocking, and a missing ecid reads as 0.
        Assert.Equal(new[] { "has_blocked_plan", "has_blocking_plan" }, Strs(r, "blockingKeys"));
        Assert.Equal(new[] { "Blocked plan", "Blocking plan" }, Strs(r, "blockingLabels"));
        Assert.True(r.GetProperty("blockingHide").GetBoolean());
        Assert.Equal("/api/read/get_blocking_plan_xml", Str(r, "blockedPath"));
        Assert.Equal("?server=srv-a&event_time=2026-03-04T05%3A06%3A07.1234567&blocked_spid=57&blocked_ecid=1&blocking_spid=61&blocking_ecid=2", Str(r, "blockedSearch"));
        Assert.Equal("?server=srv-a&event_time=2026-03-04T05%3A06%3A07.1234567&blocked_spid=57&blocked_ecid=1&blocking_spid=61&blocking_ecid=2&side=blocking", Str(r, "blockingSearch"));
        /* The fraction keeps all seven digits, trailing zero included: the store matches the stamp for equality. */
        Assert.Equal("?server=srv-a&event_time=2026-03-04T05%3A06%3A07.1234560&blocked_spid=57&blocked_ecid=0&blocking_spid=61&blocking_ecid=0", Str(r, "blockedNoEcidSearch"));
        /* The row's database_name is sent last when the row has one; the searches above, from rows without one, carry none. */
        Assert.Equal("?server=srv-a&event_time=2026-03-04T05%3A06%3A07.1234567&blocked_spid=57&blocked_ecid=1&blocking_spid=61&blocking_ecid=2&database_name=Orders", Str(r, "blockedDbSearch"));

        // Deadlocks (#5236): the victim's plan by the two stamps, with the victim's process id when the row has one.
        Assert.Equal("has_victim_plan", Str(r, "deadlockKey"));
        Assert.Equal("Victim plan", Str(r, "deadlockLabel"));
        Assert.True(r.GetProperty("deadlockHide").GetBoolean());
        Assert.Equal("/api/read/get_deadlock_plan_xml", Str(r, "deadlockPath"));
        Assert.Equal("?server=srv-a&collection_time=2026-03-04T05%3A06%3A08.5000000&deadlock_time=2026-03-04T05%3A06%3A07.1230000&victim_process_id=process1a2b", Str(r, "deadlockSearch"));
        Assert.Equal("?server=srv-a&collection_time=2026-03-04T05%3A06%3A08.5000000&deadlock_time=2026-03-04T05%3A06%3A07.1230000", Str(r, "deadlockNoVictimSearch"));
        Assert.Equal("?server=srv-a&collection_time=2026-03-04T05%3A06%3A08.5000000&deadlock_time=2026-03-04T05%3A06%3A07.1230000&victim_process_id=process1a2b&database_name=Orders", Str(r, "deadlockDbSearch"));

        Assert.True(r.GetProperty("keysUnique").GetBoolean());
    }

    [Fact]
    public void TwoSources_NeverShareAPanel_AndTheQueryHashKeyKeepsItsOldShape()
    {
        var r = Run("kindsDoNotShareAPanel");
        Assert.True(r.GetProperty("aOpen").GetBoolean());
        Assert.False(r.GetProperty("bOpenAfterA").GetBoolean());
        Assert.False(r.GetProperty("liveOpenAfterEst").GetBoolean());
        Assert.Equal(2, r.GetProperty("keys").GetArrayLength());
        Assert.Equal("srv-a|Orders|0xABC", Str(r, "hashKey"));

        // #5236: a blocked process report row's blocked and blocking plans are two panels, not one; two deadlocks with
        // the same stamps and different victims are two panels as well.
        Assert.False(r.GetProperty("blockingOpenAfterBlocked").GetBoolean());
        Assert.True(r.GetProperty("bothSidesOpen").GetBoolean());
        Assert.Equal(new[] { "srv-a|@blocking||T|1|0|2|0|blocked", "srv-a|@blocking||T|1|0|2|0|blocking" }, Strs(r, "sideKeys"));
        Assert.False(r.GetProperty("victim2OpenAfterVictim1").GetBoolean());
        Assert.True(r.GetProperty("bothVictimsOpen").GetBoolean());
        Assert.Equal(new[] { "srv-a|@deadlock_victim||C|D|process1", "srv-a|@deadlock_victim||C|D|process2" }, Strs(r, "victimKeys"));

        // The row's database is in the key too: the same report key (and the same deadlock stamps and victim) in two databases are
        // two panels, never one.
        Assert.False(r.GetProperty("dbTwoOpenAfterDbOne").GetBoolean());
        Assert.True(r.GetProperty("bothDatabasesOpen").GetBoolean());
        Assert.Equal(new[] { "srv-a|@blocking|DbOne|T|1|0|2|0|blocked", "srv-a|@blocking|DbTwo|T|1|0|2|0|blocked" }, Strs(r, "databaseKeys"));
        Assert.False(r.GetProperty("victimDbTwoOpenAfterDbOne").GetBoolean());
    }

    [Fact]
    public void EachKind_DownloadsUnderAFileNameOfItsOwn()
    {
        var r = Run("stems");
        var names = Strs(r, "names");
        Assert.Equal(6, names.Distinct().Count());
        Assert.All(names, n => Assert.EndsWith(".sqlplan", n));
        Assert.Equal("active-57-2026-03-04T05_06_07.1234560-live.sqlplan", names[0]);
        Assert.Equal("qs-Orders-42-7.sqlplan", names[1]);
        Assert.Equal("0x03000500AA.sqlplan", names[2]);
        // #5236: the blocked and the blocking side of one report download under their own names, then the deadlock victim.
        Assert.Equal("blocked-57-2026-03-04T05_06_07.1234567.sqlplan", names[3]);
        Assert.Equal("blocking-61-2026-03-04T05_06_07.1234567.sqlplan", names[4]);
        Assert.Equal("deadlock-victim-2026-03-04T05_06_07.1230000-process1a2b.sqlplan", names[5]);
    }

    [Fact]
    public void ARowKindPanel_SurvivesTheRebuild_WithoutAnotherRead()
    {
        var r = Run("rebuildKinds");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsPlan").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());

        // #5236: a blocking side and a deadlock victim keep their open panels across the rebuild too.
        Assert.True(r.GetProperty("blockingOpen").GetBoolean());
        Assert.True(r.GetProperty("blockingShowsPlan").GetBoolean());
        Assert.True(r.GetProperty("victimOpen").GetBoolean());
        Assert.True(r.GetProperty("victimShowsPlan").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetchedRows").GetInt32());
    }

    [Fact]
    public void AQueryStoreRowWithNoStoredPlan_SaysSo()
    {
        var r = Run("noPlanKind");
        Assert.Contains("No stored Query Store plan found", Str(r, "text"));
        Assert.False(r.GetProperty("hasPre").GetBoolean());
    }

    /// <summary>#5236: a Deadlocks row with no victim id reads by its two times alone. When the read refuses (the deadlocks
    /// with those times name more than one victim), the panel shows the refusal's message in its error strip, not a blank pane.</summary>
    [Fact]
    public void ADeadlockRowWithNoVictim_ShowsTheReadsRefusalMessage_NotABlankPane()
    {
        var r = Run("deadlockRefusal");
        Assert.Equal("/api/read/get_deadlock_plan_xml", Str(r, "path"));
        Assert.False(r.GetProperty("hasVictimParam").GetBoolean());
        Assert.Contains("Pass the row's victim_process_id to pick one", Str(r, "text"));
        Assert.Equal(1, r.GetProperty("strip").GetInt32());
        Assert.False(r.GetProperty("hasPre").GetBoolean());
        Assert.False(r.GetProperty("hasDownload").GetBoolean());
    }

    [Fact]
    public void ANullSource_IsADash()
    {
        var r = Run("nullSource");
        Assert.Equal("\u2014", Str(r, "cell"));
        // #5236: a DMV row (no flag, no event time), a report or deadlock without its flag, a missing row, and a flagged row
        // that lacks a value the read keys on are all dashes, and none of them reads anything.
        var dash = "\u2014";
        Assert.Equal(new[] { dash, dash }, Strs(r, "dmvCells"));
        Assert.Equal(new[] { dash, dash }, Strs(r, "unflaggedBlockingCells"));
        Assert.Equal(dash, Str(r, "unflaggedVictimCell"));
        Assert.Equal(new[] { dash, dash, dash }, Strs(r, "noRowCells"));
        Assert.Equal(new[] { dash, dash, dash, dash, dash, dash }, Strs(r, "noKeyCells"));
        Assert.Equal(0, r.GetProperty("fetched").GetInt32());
    }

    /// <summary>A blocked process report row (#5236) gets a Blocked plan and a Blocking plan button, each only where its own
    /// flag is exactly true; each opens its own plan, so the two sides of one row are two reads and two panels.</summary>
    [Fact]
    public void ABlockingRow_DrawsEachButton_OnlyWhereItsFlagIs()
    {
        var r = Run("blockingButtons");
        var dash = "\u2014";
        Assert.Equal(new[] { "Blocked plan", "Blocking plan" }, Strs(r, "both"));
        Assert.Equal(new[] { "Blocked plan", dash }, Strs(r, "blockedOnly"));
        Assert.Equal(new[] { dash, "Blocking plan" }, Strs(r, "blockingOnly"));
        Assert.Equal(new[] { dash, dash }, Strs(r, "neither"));
        // false, null, 0, 1, "true", "yes", {} and [] are not true: no button for any of them.
        Assert.Equal(8, r.GetProperty("looseFlags").GetArrayLength());
        Assert.All(Strs(r, "looseFlags"), cells => Assert.Equal(dash + "|" + dash, cells));
        // The flags alone decide: an XE row with flags has both buttons, a DMV row without them has none.
        Assert.Equal(new[] { "Blocked plan", "Blocking plan" }, Strs(r, "xeFlagged"));
        Assert.Equal(new[] { dash, dash }, Strs(r, "dmvFlagless"));

        Assert.Equal("?server=srv-a&event_time=2026-03-04T05%3A06%3A07.1234567&blocked_spid=57&blocked_ecid=0&blocking_spid=61&blocking_ecid=0", Str(r, "blockedSearch"));
        Assert.True(r.GetProperty("blockingClosed").GetBoolean(), "opening the blocked plan must not open the blocking one");
        Assert.True(r.GetProperty("blockedShows").GetBoolean());
        Assert.Equal("?server=srv-a&event_time=2026-03-04T05%3A06%3A07.1234567&blocked_spid=57&blocked_ecid=0&blocking_spid=61&blocking_ecid=0&side=blocking", Str(r, "blockingSearch"));
        Assert.True(r.GetProperty("bothShow").GetBoolean());
        Assert.Equal(2, r.GetProperty("reads").GetInt32());
        Assert.Equal(2, r.GetProperty("panels").GetInt32());
        Assert.True(r.GetProperty("otherPairClosed").GetBoolean(), "another session pair at the same time is another panel");
    }

    /// <summary>A deadlock row (#5236) gets a Victim plan button only where its flag is exactly true; two deadlocks that
    /// carry the same stamps and different victims each open a plan of their own.</summary>
    [Fact]
    public void ADeadlockRow_DrawsTheVictimButton_OnlyWhereItsFlagIs()
    {
        var r = Run("victimButton");
        var dash = "\u2014";
        Assert.Equal("Victim plan", Str(r, "flagged"));
        Assert.Equal(dash, Str(r, "unflagged"));
        Assert.Equal(8, r.GetProperty("looseFlags").GetArrayLength());
        Assert.All(Strs(r, "looseFlags"), cell => Assert.Equal(dash, cell));
        Assert.EndsWith("&victim_process_id=process1a2b", Str(r, "firstSearch"));
        Assert.True(r.GetProperty("secondClosed").GetBoolean());
        Assert.EndsWith("&victim_process_id=process9z9z", Str(r, "secondSearch"));
        Assert.True(r.GetProperty("bothShow").GetBoolean());
        Assert.Equal(2, r.GetProperty("panels").GetInt32());
    }
}
