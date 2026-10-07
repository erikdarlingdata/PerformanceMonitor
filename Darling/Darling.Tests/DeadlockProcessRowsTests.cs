/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The per-process rows <c>get_deadlock_detail</c> serves in <c>processes[]</c> (the web Deadlocks sub-grid) come from
/// the same shared graph walk as the desktop viewer's grid, so the browser never parses the XML. Pure (no store).
/// </summary>
public sealed class DeadlockProcessRowsTests
{
    private const string Graph = """
        <deadlock>
          <victim-list><victimProcess id="process1" /></victim-list>
          <process-list>
            <process id="process1" spid="55" currentdbname="AppDb" waitresource="KEY: 5:72" waittime="1500" lockMode="X" isolationlevel="read committed (2)" logused="200" trancount="1" clientapp="TestApp" hostname="HOST1" loginname="app_login" status="suspended" transactionname="user_transaction" priority="0">
              <executionStack><frame procname="AppDb.dbo.usp_Update" line="1">UPDATE dbo.t SET c = 1</frame></executionStack>
              <inputbuf>UPDATE dbo.t SET c = 1</inputbuf>
            </process>
            <process id="process2" spid="60" currentdbname="AppDb" waittime="1200">
              <inputbuf>UPDATE dbo.t SET c = 2</inputbuf>
            </process>
          </process-list>
          <resource-list>
            <keylock hobtid="72" dbid="5" objectname="AppDb.dbo.t" indexname="pk_t" mode="X">
              <owner-list><owner id="process2" mode="X" /></owner-list>
              <waiter-list><waiter id="process1" mode="X" requestType="wait" /></waiter-list>
            </keylock>
          </resource-list>
        </deadlock>
        """;

    private static readonly DateTime When = new(2026, 7, 1, 10, 0, 5, DateTimeKind.Unspecified);

    /// <summary>The same XML gives the same rows through the viewer's grid path and the service path.</summary>
    [Fact]
    public void TheServicePathAndTheViewerPath_GiveTheSameRowsForTheSameGraph() => AssertSameRows(Graph);

    /// <summary>A graph naming two victims: both rows carry the victim flag, on both paths.</summary>
    [Fact]
    public void TheServicePathAndTheViewerPath_AgreeOnAGraphWithTwoVictims()
    {
        var twoVictims = Graph.Replace("<victimProcess id=\"process1\" />", "<victimProcess id=\"process1\" /><victimProcess id=\"process2\" />", StringComparison.Ordinal);
        AssertSameRows(twoVictims);
        Assert.All(DarlingDeadlockProcessRows.Parse(twoVictims, When), r => Assert.True(r.IsVictim));
    }

    private static void AssertSameRows(string graph)
    {
        var viewer = DeadlockProcessDetail.ParseFromRows(new List<ViewerDeadlockRow>
        {
            new() { DeadlockTime = When, VictimProcessId = "process1", DeadlockGraphXml = graph },
        });
        var service = DarlingDeadlockProcessRows.Parse(graph, When);

        Assert.Equal(viewer.Count, service.Count);
        for (var i = 0; i < viewer.Count; i++)
        {
            var v = viewer[i];
            var s = service[i];
            Assert.Equal(v.IsVictim, s.IsVictim);
            Assert.Equal(v.ProcessId, s.ProcessId);
            Assert.Equal(v.Spid, s.Spid);
            Assert.Equal(v.DatabaseName, s.DatabaseName);
            Assert.Equal(v.SqlText, s.SqlText);
            Assert.Equal(v.WaitResource, s.WaitResource);
            Assert.Equal(v.WaitTime, s.WaitTime);
            Assert.Equal(v.LockMode, s.LockMode);
            Assert.Equal(v.IsolationLevel, s.IsolationLevel);
            Assert.Equal(v.LogUsed, s.LogUsed);
            Assert.Equal(v.TransactionCount, s.TransactionCount);
            Assert.Equal(v.ClientApp, s.ClientApp);
            Assert.Equal(v.HostName, s.HostName);
            Assert.Equal(v.LoginName, s.LoginName);
            Assert.Equal(v.Status, s.Status);
            Assert.Equal(v.DeadlockType, s.DeadlockType);
            Assert.Equal(v.ObjectNames, s.ObjectNames);
            Assert.Equal(v.ProcName, s.ProcName);
            Assert.Equal(v.OwnerMode, s.OwnerMode);
            Assert.Equal(v.WaiterMode, s.WaiterMode);
            Assert.Equal(v.TransactionName, s.TransactionName);
            Assert.Equal(v.Priority, s.Priority);
        }
    }

    [Fact]
    public void ARow_CarriesTheVictimFlag_AndOmitsWhatTheGraphDidNotSay()
    {
        var (rows, truncated) = DarlingDeadlockProcessRows.Build(Graph, When);

        Assert.Equal(0, truncated);
        Assert.Equal(2, rows.Count);
        var victim = rows.Single(r => (int)r["spid"] == 55);
        var other = rows.Single(r => (int)r["spid"] == 60);

        Assert.True((bool)victim["victim"]);
        Assert.False((bool)other["victim"]);
        Assert.Equal("AppDb.dbo.usp_Update", victim["proc_name"]);
        Assert.Equal("read committed (2)", victim["isolation_level"]);
        Assert.Equal("app_login", victim["login_name"]);
        Assert.Equal(1500L, victim["wait_time_ms"]);

        /* process2 names no wait resource, login, host, application, isolation level or transaction. */
        foreach (var key in new[] { "wait_resource", "login_name", "host_name", "client_app", "isolation_level", "transaction_name", "proc_name", "waiter_mode", "status" })
            Assert.False(other.ContainsKey(key), key + " should be left off a row that has no value for it");
        Assert.All(rows.SelectMany(r => r.Values), v => Assert.NotNull(v));
        Assert.All(rows.SelectMany(r => r.Values.OfType<string>()), v => Assert.NotEqual("", v));
    }

    [Fact]
    public void AGraphThatDoesNotParse_GivesNoRows_NotAFabricatedSessionZero()
    {
        var (rows, truncated) = DarlingDeadlockProcessRows.Build("<deadlock><process-list>", When);
        Assert.Empty(rows);
        Assert.Equal(0, truncated);
        Assert.Empty(DarlingDeadlockProcessRows.Build(null, When).Rows);
    }

    [Fact]
    public void AGraphWithMoreProcessesThanTheCap_KeepsTheCap_AndCountsTheRest()
    {
        var sb = new StringBuilder("<deadlock><process-list>");
        var total = DarlingDeadlockProcessRows.MaxProcessesPerDeadlock + 5;
        for (var i = 0; i < total; i++)
            sb.Append($"<process id=\"p{i}\" spid=\"{100 + i}\"><inputbuf>{new string('x', 900)}</inputbuf></process>");
        sb.Append("</process-list></deadlock>");

        var (rows, truncated) = DarlingDeadlockProcessRows.Build(sb.ToString(), When);

        Assert.Equal(DarlingDeadlockProcessRows.MaxProcessesPerDeadlock, rows.Count);
        Assert.Equal(5, truncated);
        Assert.All(rows, r => Assert.True(((string)r["sql_text"]).Length < 900));
    }

    [Fact]
    public void TheTool_ServesProcessesBesideThePreviewedGraph()
    {
        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpBlockingTools.cs");
        tool = tool.ReplaceLineEndings("\n");
        var region = tool[tool.IndexOf("Name = \"get_deadlock_detail\"", StringComparison.Ordinal)..];
        region = region[..region.IndexOf("get_blocked_process_xml", StringComparison.Ordinal)];

        Assert.Contains("DarlingDeadlockProcessRows.Build(", region, StringComparison.Ordinal);
        Assert.Contains("processes,", region, StringComparison.Ordinal);
        Assert.Contains("processes_truncated = processesTruncated", region, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePage_KeepsTheFiveColumnSummary_AndAddsTheSubGrid_KeyedAtModuleScope()
    {
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        Assert.Contains("DEADLOCK_COLUMNS,", js, StringComparison.Ordinal);
        Assert.Equal(5, System.Text.RegularExpressions.Regex.Matches(
            js[js.IndexOf("const DEADLOCK_COLUMNS = [", StringComparison.Ordinal)..].Split("];")[0], @"\{ key: ").Count);
        Assert.Contains("const deadlockProcessesOpen = new Set();", js, StringComparison.Ordinal);
        Assert.Contains("server + \"\\u0001\" + (row.dedup_key", js, StringComparison.Ordinal);
        /* #5244: the panel passes its hidden-row scope to the columns, so the notice under it can count what the filter hid. */
        Assert.Contains("deadlockXmlColumns(server, scope)", js, StringComparison.Ordinal);
        Assert.DoesNotContain("DOMParser", js, StringComparison.Ordinal);
        foreach (var key in new[] { "victim", "spid", "login_name", "host_name", "client_app", "database_name", "wait_resource", "lock_mode", "isolation_level", "transaction_name", "log_used", "sql_text" })
            Assert.Contains("{ key: \"" + key + "\"", js, StringComparison.Ordinal);
    }

    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-deadlock-processes-harness.mjs"));
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
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the deadlock harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the deadlock harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return true;
        }
    }

    private static bool[] Bools(JsonElement node) => node.EnumerateArray().Select(e => e.GetBoolean()).ToArray();

    [Fact]
    public void EachDeadlock_GetsACollapsedSubGrid_WithTheDesktopColumnsPlusLogUsedAndStatus()
    {
        if (!TryRun("closed", out var r)) return;

        Assert.Equal(new[] { "2 processes", "1 process" }, r.GetProperty("summaries").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal(new[] { false, false }, Bools(r.GetProperty("open")));
        var headers = r.GetProperty("subHeaders")[0].EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal(21, headers.Length);
        Assert.Contains("Victim", headers);
        foreach (var h in new[] { "Object(s)", "Wait (ms)", "Tran Name", "Tran Count", "App", "Log Used", "Status" })
            Assert.Contains(h, headers);
        Assert.Contains("Statement", headers);
        var first = r.GetProperty("subRows")[0][0].EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Contains("Victim", first);
        Assert.Contains("app_login", first);
    }

    [Fact]
    public void AnExpandedSubGrid_StaysOpenAcrossARebuild()
    {
        if (!TryRun("expandSurvivesRebuild", out var r)) return;

        Assert.Equal(new[] { true, false }, Bools(r.GetProperty("afterToggle")));
        Assert.Equal(new[] { true, false }, Bools(r.GetProperty("afterRebuild")));
    }

    [Fact]
    public void ACollapsedSubGrid_StaysClosedAcrossARebuild()
    {
        if (!TryRun("collapseSurvivesRebuild", out var r)) return;

        Assert.Equal(new[] { false, false }, Bools(r.GetProperty("afterRebuild")));
    }

    [Fact]
    public void TheOpenState_IsKeyedByServer()
    {
        if (!TryRun("otherServerKeyed", out var r)) return;

        Assert.Equal(new[] { false, false }, Bools(r.GetProperty("otherServerOpen")));
    }

    [Fact]
    public void ADeadlockWithNoProcessRows_DrawsNoSubGrid()
    {
        if (!TryRun("noprocesses", out var r)) return;

        Assert.Equal(0, r.GetProperty("subgrids").GetInt32());
    }

    [Fact]
    public void ADeadlockWhoseEveryRowThePageBudgetCut_SaysSo_AndHowToGetThem()
    {
        if (!TryRun("allcut", out var r)) return;

        Assert.Equal(1, r.GetProperty("subgrids").GetInt32());
        var text = r.GetProperty("text").GetString()!;
        Assert.Contains("6 processes not sent (page row limit)", text, StringComparison.Ordinal);
        Assert.Contains("pick Custom… in the time range and narrow it to this deadlock", text, StringComparison.Ordinal);
        Assert.DoesNotContain("dedup_key", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ACappedDeadlock_SaysHowManyProcessesWereLeftOff()
    {
        if (!TryRun("capped", out var r)) return;

        Assert.Contains("(+3 more in the graph)", r.GetProperty("summaries")[0].GetString(), StringComparison.Ordinal);
    }

    private static string[] Strings(JsonElement node) => node.EnumerateArray().Select(e => e.GetString()!).ToArray();

    /// <summary>#5244: with A and B chosen, the C process row is hidden, a process with no database is hidden (the desktop's
    /// rule), and the graphs, which are not filtered, still count every process; the notice line and the chip say so.</summary>
    [Fact]
    public void WithAFilter_TheProcessRowsOutsideTheChosenDatabasesAreHidden_AndTheGraphsStayWhole()
    {
        if (!TryRun("spread:A|B", out var r)) return;

        Assert.Equal(new[] { "2 processes", "1 process" }, Strings(r.GetProperty("summaries")));
        Assert.Equal(new[] { "A", "B" }, Strings(r.GetProperty("subDatabases")[0]));
        Assert.Equal(new[] { "A" }, Strings(r.GetProperty("subDatabases")[1]));
        Assert.Equal(new[] { "Graph (3 processes)", "Graph (1 process)", "Graph (1 process)" }, Strings(r.GetProperty("graphs")));
        Assert.Equal(new[] { "1 process hidden by the database filter" }, Strings(r.GetProperty("cellTexts")));
        Assert.Equal(
            new[] { "The database filter hides 2 process rows outside the chosen databases. Each graph is whole." },
            Strings(r.GetProperty("notes")));
        var chip = Assert.Single(r.GetProperty("chips").EnumerateArray());
        Assert.Equal("process-rows", chip.GetProperty("state").GetString());
        Assert.Equal("Graphs: all; process rows: 2 databases", chip.GetProperty("text").GetString());
        Assert.Equal("Each graph is whole; process rows outside the chosen databases are hidden.", chip.GetProperty("title").GetString());
    }

    [Fact]
    public void WithNoFilter_EveryProcessRowShows_AndThereIsNoNoticeOrChip()
    {
        if (!TryRun("spread:none", out var r)) return;

        Assert.Equal(new[] { "3 processes", "1 process", "1 process" }, Strings(r.GetProperty("summaries")));
        Assert.Empty(r.GetProperty("cellTexts").EnumerateArray());
        Assert.Equal(0, Strings(r.GetProperty("notes")).Count(n => n.Length > 0));
        Assert.Empty(r.GetProperty("chips").EnumerateArray());
    }

    /// <summary>A filter naming A, B and C hides only the process that has no database, so the notice counts that one; the chip
    /// pluralises by the number of chosen databases.</summary>
    [Fact]
    public void AProcessWithNoDatabase_IsHiddenWhileAFilterIsActive_AndTheChipCountsTheChosenDatabases()
    {
        if (!TryRun("spread:A|B|C", out var r)) return;

        Assert.Equal(new[] { "3 processes", "1 process" }, Strings(r.GetProperty("summaries")));
        Assert.Equal(
            new[] { "The database filter hides 1 process row outside the chosen databases. Each graph is whole." },
            Strings(r.GetProperty("notes")));
        Assert.Equal("Graphs: all; process rows: 3 databases", r.GetProperty("chips")[0].GetProperty("text").GetString());

        if (!TryRun("spread:A", out var one)) return;
        Assert.Equal("Graphs: all; process rows: 1 database", one.GetProperty("chips")[0].GetProperty("text").GetString());
    }

    /// <summary>A filter that hides no process row draws no notice (the chip still says the rows follow the filter).</summary>
    [Fact]
    public void AFilterThatHidesNothing_DrawsNoNotice()
    {
        if (!TryRun("clean:A|B", out var r)) return;

        Assert.Equal(new[] { "1 process", "2 processes" }, Strings(r.GetProperty("summaries")));
        Assert.Equal(0, Strings(r.GetProperty("notes")).Count(n => n.Length > 0));
        Assert.Equal("process-rows", Assert.Single(r.GetProperty("chips").EnumerateArray()).GetProperty("state").GetString());
    }

    /// <summary>#5244: the page row budget runs before the browser filter, so when every sent row is outside the chosen database the
    /// cell still says how many rows were not sent (their database is unknown, so they are not counted as hidden).</summary>
    [Fact]
    public void WhenTheFilterHidesEverySentRow_TheCellStillCountsTheRowsNotSent()
    {
        if (!TryRun("cut:A", out var r)) return;

        Assert.Equal(new[] { "1 process hidden by the database filter (+2 more in the graph)" }, Strings(r.GetProperty("cellTexts")));
    }

    [Fact]
    public void TheProcessRowFilter_IsExactAboutDatabaseNames()
    {
        if (!TryRun("spread:a|b", out var r)) return;

        /* No trim and no case folding: "a" and "b" name no database of the data, so every process row is hidden. */
        Assert.Equal(
            new[] { "3 processes hidden by the database filter", "1 process hidden by the database filter", "1 process hidden by the database filter" },
            Strings(r.GetProperty("cellTexts")));
    }
}
