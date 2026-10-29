/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Web click-through fixes (text): the shipped <c>plain-text.js</c> run under Node, which puts the internal names the
/// service writes for an MCP client (<c>hints.captures</c>, <c>hours_back</c>, <c>denied_since_last_success</c>, collector
/// measurement counts, the engine-gate sentence) into words for the web page. Node is skipped when it is not installed.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class WebPlainTextTests
{
    private static string[] Run(string[] inputs, out string notCollectedLine) => RunDoc(inputs, out notCollectedLine).Out;

    private static (string[] Out, string[] Named, string[] Tools, string[] Labels, string[] Actions, string[] Counts) RunDoc(string[] inputs, out string notCollectedLine)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plain-text-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(inputs);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            notCollectedLine = "";
            return ([], [], [], [], [], []);
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the plain text harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the plain text harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            notCollectedLine = doc.RootElement.GetProperty("line").GetString()!;
            string[] Strs(string name) => doc.RootElement.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();
            return (Strs("out"), Strs("named"), Strs("tools"), Strs("labels"), Strs("actions"), Strs("counts"));
        }
    }

    /* #6764: the page and the two desktop apps say the same words for a collector that never ran or has gone dark. The page's copy
       is runtimePreconditionText in plain-text.js, run here under Node on the real MCP answers; the desktop copy is
       CollectorRuntimePrecondition.GatedOffReaderMessage. A word changed in either one fails this. */
    [Fact]
    public void ThePagesRuntimePreconditionText_IsTheDesktopReaderMessage_ForNeverRanAndGoneDark()
    {
        var serverLastCollected = DateTime.UtcNow.AddMinutes(-3);
        var causes = CollectorRuntimePrecondition.RunningJobsPossibleCauses;
        DateTime?[] collectorLastRuns = [null, serverLastCollected.AddDays(-20)];

        foreach (var last in collectorLastRuns)
        {
            var mcp = CollectorRuntimePrecondition.GatedOffMessage("srv", "running_jobs", causes, last, serverLastCollected, null)!;
            var reader = CollectorRuntimePrecondition.GatedOffReaderMessage("srv", "running_jobs", causes, last, serverLastCollected, null)!;

            var page = Run([mcp], out _)[0];

            Assert.Equal(reader, page);
            Assert.DoesNotContain("runtime PRECONDITION", page, StringComparison.Ordinal);
            Assert.EndsWith("Restart the service, or change this server's settings.", page, StringComparison.Ordinal);
        }
    }

    /* #6869: a collector that cannot read its source (the DEGRADED status) says the same words on the page as in the two desktop apps. */
    [Theory]
    [InlineData(CollectorRuntimePrecondition.DegradedStatus)]
    [InlineData(CollectorRuntimePrecondition.CaptureSessionMissingStatus)]
    [InlineData(CollectorRuntimePrecondition.ExtensionMissingStatus)]
    public void ThePagesRuntimePreconditionText_IsTheDesktopReaderMessage_ForACollectorOutcome(string status)
    {
        const string refused = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";
        var ran = new DateTime(2026, 10, 25, 12, 0, 0, DateTimeKind.Utc);
        var mcp = CollectorRuntimePrecondition.CollectionOutcomeMessage("srv", "tempdb_stats", status, refused, ran)!;
        var reader = CollectorRuntimePrecondition.CollectionOutcomeReaderMessage("srv", "tempdb_stats", status, refused, ran)!;

        var page = Run([mcp], out _)[0];

        /* The page also turns the collector's key into its display name, which the desktop text does not. */
        Assert.Equal(reader.Replace("tempdb_stats", "Tempdb Stats", StringComparison.Ordinal), page);
        Assert.DoesNotContain("Act on that sentence", page, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHintsPointer_AndHoursBack_AreDroppedOrSaidInWords()
    {
        var o = Run(
        [
            "No blocking was recorded in the last 24 hour(s). 5 collector run(s) DID execute, so this is a genuine all-clear - see hints.captures for which collectors ran and when.",
            "This is a gap rather than a dead collector: widen hours_back, or use get_collection_health to find where it stopped.",
        ], out _);
        Assert.DoesNotContain("hints.captures", o[0]);
        Assert.EndsWith("genuine all-clear.", o[0]);
        Assert.DoesNotContain("hours_back", o[1]);
        Assert.Contains("widen the time range, or use Collection Health to find", o[1]);
    }

    [Fact]
    public void TheHealthOutputSentence_NamesNoField()
    {
        var o = Run(
        [
            "Stored 0 rows across 96 runs with no current denial (denied_since_last_success is false), so this collector read and found nothing.",
            "Stored 0 rows across 96 runs, and denied_since_last_success is true - the newest denial postdates the newest success, so this collector is refused.",
        ], out _);
        Assert.Equal("Stored 0 rows across 96 runs with no current denial, so this collector read and found nothing.", o[0]);
        Assert.DoesNotContain("denied_since_last_success", o[1]);
        Assert.Contains("the newest denial is newer than the newest success", o[1]);
    }

    [Fact]
    public void CollectorMeasurementCounts_ReadAsWords()
    {
        var o = Run(["latest run: shred_gated=1 events_read=0 report_xml_empty=0"], out _);
        Assert.Equal("latest run: shred gated: 1, events read: 0, report xml empty: 0", o[0]);
    }

    /* #6649: the left-out databases pointer, the baseline window sentence and the memory clerk note's four arms reached the page
       with the MCP answer's field names in them. The baseline sentence and the clerk notes are read from the C# that owns them
       (QueryStoreRegressionRules.BaselineWindowTail, DarlingMcpQueryStoreClutterTools.ClerkNote*), so a reword there breaks this
       test instead of leaving a field name on the page (#3684). The left-out pointer is a generic suffix strip, so its one
       sentence is typed here. */
    [Fact]
    public void FieldNamesInTheServicesOwnSentences_ReadAsThePagesWords()
    {
        var o = Run(
        [
            "No Query Store rows in the 24-hour window for a database this server still has. 29 database(s) the server no longer lists were left out of this read (see databases_left_out).",
            QueryStoreRegressionRules.BaselineWindowTail,
            DarlingMcpQueryStoreClutterTools.ClerkNoteInNewestCapture,
            DarlingMcpQueryStoreClutterTools.ClerkNoteDroppedOut,
            DarlingMcpQueryStoreClutterTools.ClerkNoteNoCapture,
            DarlingMcpQueryStoreClutterTools.ClerkNoteNeverAppeared,
        ], out _);
        Assert.Equal("No Query Store rows in the 24-hour window for a database this server still has. 29 databases the server no longer lists were left out of this read.", o[0]);
        Assert.Equal("Here the window starts before the baseline period, so the baseline holds only the part the store has.", o[1]);
        Assert.Equal("The clerk is in the newest capture's top 25; Latest MB is its current footprint.", o[2]);
        Assert.Contains("Latest MB is its last observed footprint", o[3]);
        Assert.StartsWith("No memory clerk capture in the window: the collector wrote nothing", o[4]);
        Assert.Contains("The memory clerks collector stores the top 25 clerks over 1 MB per capture", o[5]);
        foreach (var line in o)
        {
            Assert.DoesNotContain("_left_out", line);
            Assert.DoesNotContain("baseline_start", line);
            Assert.DoesNotContain("effective_start", line);
            Assert.DoesNotContain("_mb is", line);
            Assert.DoesNotContain("memory_clerks", line);
            Assert.DoesNotContain("(s)", line);
        }
    }

    /* #6750: the empty-baseline sentence and the cut-recent-window sentences, read from the C# that owns them, reach the page without
       the MCP answer's field names, as the baseline sentence above does. */
    [Fact]
    public void TheEmptyBaselineAndCutRecentWindowSentences_ReadAsThePagesWords()
    {
        var o = Run(
        [
            QueryStoreRegressionRules.EmptyBaselineTail,
            QueryStoreRegressionRules.TailFor(new DateTime(2026, 1, 5), new DateTime(2026, 1, 1)),
        ], out _);
        Assert.Equal("Here the store starts at or after the end of the baseline period, so the baseline holds no hours: timeouts and errors are flagged on recent runs alone, and a cost flag can come only from a run before the baseline period ended that was collected later.", o[0]);
        Assert.StartsWith(o[0], o[1], StringComparison.Ordinal);
        Assert.Contains("The store also starts after the recent window began, so the recent window holds only the part the store has.", o[1], StringComparison.Ordinal);
        Assert.EndsWith("Its per-hour rates divide by the hours it holds.", o[1], StringComparison.Ordinal);
        foreach (var line in o)
        {
            Assert.DoesNotContain("baseline_end", line);
            Assert.DoesNotContain("effective_start", line);
            Assert.DoesNotContain("recent_window_start", line);
        }
    }

    /* #6649: a count can be negative or have a fraction, and either ended the run of pairs, so the whole note stayed raw. */
    [Fact]
    public void ACountRunWithANegativeOrFractionalValue_StillReadsAsWords()
    {
        var o = Run(
        [
            "latest run: plans_rendered=0 plans_rendered_bytes=-12 plan_fetch_ms=0.5 deferred_hit=0",
            "latest run: ring_buffer_truncated=1 ring_buffer_events_hidden=167 newest_visible_event_age_s=4300 statement_scrub_named=0",
        ], out _);
        Assert.Equal("latest run: plans rendered: 0, plans rendered bytes: -12, plan fetch ms: 0.5, deferred hit: 0", o[0]);
        Assert.DoesNotContain("=", o[0]);
        Assert.DoesNotContain("=0 ", o[1] + " ");
        Assert.DoesNotContain("=", o[1]);
    }

    /* #6649: the collection log's Error cell holds a failed run's error and a successful run's note. The note's counts are said in
       words; a real error message keeps everything else, a lone key=value pair and any tool-looking word included. */
    [Fact]
    public void TheCollectionLogMessage_SaysItsCountsInWords_AndLeavesAnErrorAlone()
    {
        var (_, _, _, _, _, counts) = RunDoc(
        [
            "ring_buffer_truncated=1 ring_buffer_events_hidden=167 newest_visible_event_age_s=4300",
            "Connection failed (timeout=30): relation \"get_orders\" is not available; widen hours_back",
        ], out _);
        Assert.Equal("ring buffer truncated: 1, ring buffer events hidden: 167, newest visible event age s: 4300", counts[0]);
        Assert.Equal("Connection failed (timeout=30): relation \"get_orders\" is not available; widen the time range", counts[1]);
    }

    /* #5644 W8 (live check): the CPU and Memory pages have no bucket control, the Waits and Activity notes print raw field names, and a
       window count printed "24 hour(s)". */
    [Fact]
    public void TrendNotes_NameTheTimeRange_TheColumnLabels_AndAgreeInNumber()
    {
        var o = Run(
        [
            "Each point averages the samples in one 5-minute bucket and is stamped at the bucket's start. A level is averaged, never summed, so a spike inside a bucket is smoothed into its average; narrow the bucket to see it. The width was chosen to keep this answer near 1500 points; pass bucket_minutes (1-1440) for another width, or narrow hours_back for finer points.",
            "wait_time_ms_per_second is recomputed from the summed wait time over the seconds the collections covered.",
            "collisions_per_second is recomputed from the summed delta.",
            "The session counts are levels, averaged, never summed; top_application_name, top_application_connections, top_host_name and top_host_connections are the newest collection's own values in the bucket.",
            "No query-stats group on SRV in the last 24 hour(s) has a cached plan.",
            "No blocking was recorded in the last 1 hour(s). 288 collector run(s) DID execute; 1 collector run(s) failed.",
            "3 query(s) failed, 1 query(s) retried, 2 policy(s) skipped.",
        ], out _);
        Assert.DoesNotContain("narrow the bucket", o[0]);
        Assert.Contains("narrow the time range to see it", o[0]);
        Assert.DoesNotContain("wait_time_ms_per_second", o[1]);
        Assert.StartsWith("Wait ms/s is recomputed", o[1]);
        Assert.StartsWith("Coll/s is recomputed", o[2]);
        Assert.DoesNotContain("top_application_name", o[3]);
        Assert.Contains("the top application, its connection count, the top host and its connection count are", o[3]);
        Assert.Equal("No query-stats group on SRV in the last 24 hours has a cached plan.", o[4]);
        Assert.Equal("No blocking was recorded in the last 1 hour. 288 collector runs did execute; 1 collector run failed.", o[5]);
        /* a noun ending in a consonant plus y takes "ies", not a bare "s" */
        Assert.Equal("3 queries failed, 1 query retried, 2 policies skipped.", o[6]);
    }

    /* #6831: System Events says what was captured in words, with the count's own plural, for every event type the health parser reads. */
    [Fact]
    public void TheSystemHealthEventNames_AreSaidInWords_WithTheCountsPlural()
    {
        /* Every event type the parser names, read from its public constants, so a seventh type fails here until the page says it in words. */
        var types = typeof(SystemHealthParser)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Event", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();
        Assert.Equal(6, types.Length);
        var inputs = types.SelectMany(t => new[]
        {
            $"1431 {t} event(s) were captured for SQL2025 in the last 24 hour(s) and none qualified.",
            $"1 {t} event(s) were captured for SQL2025 in the last 1 hour(s) and none qualified.",
            $"No {t} events were captured for SQL2025 in the last 24 hour(s).",
        }).ToArray();
        var o = Run(inputs, out _);
        for (var i = 0; i < types.Length; i++)
        {
            var word = types[i] switch
            {
                SystemHealthParser.SchedulerMonitorEvent => "scheduler monitor",
                SystemHealthParser.SpServerDiagnosticsEvent => "server diagnostics",
                SystemHealthParser.MemoryBrokerEvent => "memory broker",
                SystemHealthParser.MemoryNodeOomEvent => "memory node out-of-memory",
                SystemHealthParser.ErrorReportedEvent => "error",
                SystemHealthParser.WaitInfoEvent => "wait",
                _ => throw new InvalidOperationException("an event type the page has no words for: " + types[i]),
            };
            Assert.StartsWith($"1431 {word} events were captured for SQL2025 in the last 24 hours", o[i * 3]);
            Assert.StartsWith($"1 {word} event was captured", o[i * 3 + 1]);
            Assert.StartsWith($"No {word} events were captured", o[i * 3 + 2]);
        }
        foreach (var line in o)
        {
            Assert.DoesNotContain("_", line);
            Assert.DoesNotContain("event(s)", line);
        }
    }

    /* #6831: the cut-window sentence with no instant to name (util.js puts the time in when the answer carries one). */
    [Fact]
    public void TheCutWindowSentence_WithNoInstantToName_DoesNotPointAtHints()
    {
        var o = Run([McpHelpers.CutWindowNothingMessage], out _);
        Assert.DoesNotContain("effective_start", o[0]);
        Assert.DoesNotContain("see hints", o[0]);
        Assert.Contains("starts partway into the window, so", o[0]);
    }

    /* #5644 W8: the clauses that point an MCP caller at a parameter the page has no control for. */
    [Fact]
    public void ParameterClauses_ForAnMcpCaller_AreDroppedOrSaidInThePagesWords()
    {
        var o = Run(
        [
            "The width was chosen to keep this answer near 1500 points; pass bucket_minutes (1-1440) for another width, or narrow hours_back for finer points.",
            "TRUNCATED: 300 indexes match and 200 were returned. Rows are ordered by total wait time descending; raise limit to see more.",
            "TRUNCATED: 9 indexes match database 'x' and 5 were returned. This says nothing about databases outside the filter — raise limit to see more.",
            "TRUNCATED: 9 indexes match and 5 were returned. This is not evidence that a database was not collected — pass database_name to ask about one, or raise limit.",
            " truncated: 5 of 9 returned; raise limit (now 5) for more.",
            "The row limit of 50 was REACHED: databases[] holds the 50 biggest spillers and the totals above still cover every database - raise limit to see the rest of the rows.",
            "At least one database's statistics WERE reset in this window - see stats_reset_count per database, and raise limit if none of the databases returned shows one - so its totals are lower bounds.",
            "No active query snapshots on SQL2025 in the last 1 hour(s) matched blocking_only. The filters were applied in SQL over the whole window.",
            "No active query snapshots on SQL2025 in the last 1 hour(s) matched database_name 'Sales' with blocking_only with wait_type 'LCK_M_X'.",
            "3 sp_server_diagnostics event(s) were captured for SQL2025 in the last 1 hour(s) and none qualified. Events ARE being captured, so this is the healthy answer for this read rather than missing data.",
            "TRUNCATED: 40 groups were in scope; only the top 25 (by name) are returned. Scope by server_name, or raise limit, to see the rest.",
        ], out _);

        Assert.Equal("The width was chosen to keep this answer near 1500 points; narrow the time range for finer points.", o[0]);
        Assert.Equal("TRUNCATED: 300 indexes match and 200 were returned. Rows are ordered by total wait time descending.", o[1]);
        Assert.EndsWith("outside the filter.", o[2]);
        Assert.EndsWith("filter by database to ask about one.", o[3]);
        Assert.Equal(" truncated: 5 of 9 returned.", o[4]);
        Assert.EndsWith("still cover every database.", o[5]);
        Assert.Contains("per database - so its totals", o[6]);
        Assert.Equal("No active query snapshots on SQL2025 in the last 1 hour matched blocking queries only. The filters were applied in SQL over the whole window.", o[7]);
        Assert.Contains("matched database 'Sales' with blocking queries only with wait type 'LCK_M_X'.", o[8]);
        Assert.Contains("so this is a normal result, not missing data.", o[9]);
        Assert.EndsWith("Select a single server to see the rest.", o[10]);
        foreach (var line in o)
        {
            Assert.DoesNotContain("raise limit", line);
            Assert.DoesNotContain("bucket_minutes", line);
            Assert.DoesNotContain("blocking_only", line);
            Assert.DoesNotContain("healthy answer for this read", line);
        }
    }

    [Fact]
    public void AParameterNamedInRealText_IsLeftAlone()
    {
        string[] same =
        [
            "Column limit_value was reset",
            "relation database_name_idx does not exist",
            "blocking_only_tbl",
        ];
        Assert.Equal(same, Run(same, out _));
    }

    [Fact]
    public void UserDataAndRealErrors_ReachThePageAsWritten()
    {
        /* Round-1 M4: only known internal tokens are rewritten. */
        string[] same =
        [
            "Could not find stored procedure 'dbo.get_orders'",
            "Server 'get_prod' not found",
            "timeout=30",
            "A driver said: timeout=30 while connecting",
            "sales_2024",
            "pg_stat_statements_1 was reset",
            "Could not find get_orders_by_day",
        ];
        var o = Run(same, out _);
        Assert.Equal(same, o);
    }

    [Fact]
    public void TheClickThroughCases_StillReadInWords()
    {
        var o = Run(
        [
            "use get_collection_health to find where it stopped",
            "latest run: shred_gated=1 events_read=0 report_xml_empty=0",
            "latest run: events_read=4",
            "latest noted run: shred_gated_1 events_read_0 (3 of 5 runs)",
            "pg_wraparound_stats failed; last_error has the text",
        ], out _);
        Assert.Equal("use Collection Health to find where it stopped", o[0]);
        Assert.Equal("latest run: shred gated: 1, events read: 0, report xml empty: 0", o[1]);
        Assert.Equal("latest run: events read: 4", o[2]);
        Assert.Equal("latest noted run: shred gated: 1, events read: 0 (3 of 5 runs)", o[3]);
        Assert.Equal("the freeze-headroom collector failed; Last Error has the text", o[4]);
    }

    [Fact]
    public void TheNamedRules_TouchOnlyTheNamedTokens()
    {
        string[] inputs =
        [
            "Could not find 'dbo.get_orders' (timeout=30); widen hours_back or use get_collection_health",
            "latest run: events_read=4 shred_gated=1 and last_error shows it",
            "relation \"pg_wraparound_stats\" does not exist",
            "column \"last_error\" of relation \"x\" does not exist",
            "denied_since_last_success is true; move as_of or widen days_back",
        ];
        var (_, named, _, _, _, _) = RunDoc(inputs, out _);
        Assert.Equal("Could not find 'dbo.get_orders' (timeout=30); widen the time range or use get_collection_health", named[0]);
        /* Round-2 L1: a Last Error can quote a real relation or column, so field and table names are not rewritten here. */
        Assert.Equal("latest run: events_read=4 shred_gated=1 and last_error shows it", named[1]);
        Assert.Equal("relation \"pg_wraparound_stats\" does not exist", named[2]);
        Assert.Equal("column \"last_error\" of relation \"x\" does not exist", named[3]);
        Assert.Equal("a denial is newer than the last success; move the end date or widen the date range", named[4]);
    }

    [Fact]
    public void AToolReference_BecomesThePagesWords_OrTheSentenceIsDropped()
    {
        var o = Run(
        [
            /* DarlingMcpTools.cs: the empty Analysis Findings answer. The page has no analysis action, so the sentence goes. */
            "No findings in the requested time range. Run analyze_server to generate new findings.",
            /* A read a tab shows becomes that tab. */
            "If a replica is SUPPOSED to be attached, check get_pg_replication_slots.",
            /* CollectorRuntimePrecondition: a collector becomes its display name. */
            "The trace_flags collector has never run against this server, while the server itself is up.",
            "No rows. Check the trace_flags collector's latest successful run.",
            /* A tool no tab shows: the trailing clause goes, the sentence before it stays. */
            "3 point-in-time fact(s) could still be read here; audit_config reports those.",
            /* A user's own get_ word is no tool and stays. */
            "Could not find dbo.get_orders; ask for get_pg_xmin_horizon again.",
            "Nothing to report.",
        ], out _);
        Assert.Equal("No findings in the requested time range.", o[0]);
        Assert.Equal("If a replica is supposed to be attached, check the Replication tab.", o[1]);
        Assert.Equal("The Trace Flags collector has never run against this server, while the server itself is up.", o[2]);
        Assert.Equal("No rows. Check the Trace Flags collector's latest successful run.", o[3]);
        Assert.Equal("3 point-in-time fact(s) could still be read here.", o[4]);
        Assert.Equal("Could not find dbo.get_orders; ask for the Vacuum tab again.", o[5]);
        Assert.Equal("Nothing to report.", o[6]);
        foreach (var line in o)
        {
            Assert.DoesNotMatch(@"(analyze_server|audit_config|trace_flags|get_pg_replication_slots)", line);
        }
    }

    [Fact]
    public void CapitalsForEmphasis_AreLowerCased_AndAcronymsWaitTypesAndNamesAreNot()
    {
        var o = Run(
        [
            /* Real messages: DarlingMcpBlockingTools, DarlingMcpPgReplicationStatsTools, DarlingMcpPgXminTools. */
            "5 collector run(s) DID execute, so this is a genuine all-clear. Collection HAS run for this server, NOT the trailing 7 days.",
            "No holder rows AND no successful captures. This is MEASURED rather than estimated; a replica SUPPOSED to be attached is not.",
            "NOT an all-clear: nothing was captured.",
            /* Acronyms, units, wait types, settings and SQL stay. */
            "Gave 4 MB of WAL to the CPU, XID 12 and LSN 0/16B, over TLS in UTC: I/O on the AG via the DMV, OK per MCP and PG, RPO and RTO, TDE, OS, RAM, NUMA, CU, RTM.",
            "Waits were PAGEIOLATCH_SH and CXPACKET, with track_io_timing OFF and READ_ONLY on PG18 as ERROR 42P01.",
            /* A capital next to another capital word is a statement or permission name. */
            "Grant ALTER ANY EVENT SESSION; the column IS NOT NULL.",
            /* A name the reader typed. */
            "No rows for PRODSQL or database SALESDB.",
        ], out _);
        Assert.Equal("5 collector runs did execute, so this is a genuine all-clear. Collection has run for this server, not the trailing 7 days.", o[0]);
        Assert.Equal("No holder rows and no successful captures. This is measured rather than estimated; a replica supposed to be attached is not.", o[1]);
        Assert.Equal("Not an all-clear: nothing was captured.", o[2]);
        Assert.Equal("Gave 4 MB of WAL to the CPU, XID 12 and LSN 0/16B, over TLS in UTC: I/O on the AG via the DMV, OK per MCP and PG, RPO and RTO, TDE, OS, RAM, NUMA, CU, RTM.", o[3]);
        Assert.Equal("Waits were PAGEIOLATCH_SH and CXPACKET, with track_io_timing OFF and READ_ONLY on PG18 as ERROR 42P01.", o[4]);
        Assert.Equal("Grant ALTER ANY EVENT SESSION; the column IS NOT NULL.", o[5]);
        Assert.Equal("No rows for PRODSQL or database SALESDB.", o[6]);
    }

    [Fact]
    public void ThePlainNamedPath_NeverLowerCasesOrDropsAnything()
    {
        var (_, named, _, _, _, _) = RunDoc(["Last error: DID NOT connect; try analyze_server"], out _);
        Assert.Equal("Last error: DID NOT connect; try analyze_server", named[0]);
    }

    [Fact]
    public void TheToolNameList_IsExactlyTheRegisteredGetTools()
    {
        /* The get_ rule rewrites only these names, so the list must be the MCP tool registry's get_ tools, both ways. */
        var (_, _, tools, _, actions, _) = RunDoc([], out _);
        if (tools.Length == 0)
        {
            return;
        }

        var all = typeof(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpAlertTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .Select(n => n!)
            .ToList();
        var registry = all.Where(n => n.StartsWith("get_", StringComparison.Ordinal)).ToHashSet(StringComparer.Ordinal);
        var others = all.Where(n => !n.StartsWith("get_", StringComparison.Ordinal)).ToHashSet(StringComparer.Ordinal);

        Assert.True(registry.Count > 100, $"the registry walk found only {registry.Count} get_ tools");
        Assert.Empty(registry.Except(tools).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Empty(tools.Except(registry).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(tools.Length, tools.Distinct(StringComparer.Ordinal).Count());

        /* The sentence-dropping rule needs every other tool too (analyze_server, list_servers, the write tools). */
        Assert.Empty(others.Except(actions).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Empty(actions.Except(others).OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public void TheMeasurementLabelList_CoversEveryLabelTheCollectorsWrite()
    {
        var (_, _, tools, labels, _, _) = RunDoc([], out _);
        if (tools.Length == 0)
        {
            return;
        }

        var found = new System.Collections.Generic.SortedSet<string>(StringComparer.Ordinal);
        var dir = PathTo("PerformanceMonitor.Collectors");
        foreach (var file in System.IO.Directory.EnumerateFiles(dir, "*.cs", System.IO.SearchOption.TopDirectoryOnly))
        {
            var text = System.IO.File.ReadAllText(file);
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(
                         text, "const string \\w*Measurement\\w*\\s*=\\s*\"([a-z0-9_]+)\"|\\.Measure\\(\"([a-z0-9_]+)\""))
            {
                found.Add(m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value);
            }
        }

        Assert.True(found.Count > 20, $"the label scan found only {found.Count} labels");
        Assert.Empty(found.Except(labels).ToList());
        Assert.Empty(labels.Except(found).ToList());
    }

    [Fact]
    public void TheEngineGateSentence_BecomesOneShortLine_AndOtherTextIsKept()
    {
        var o = Run(
        [
            "example-pg-01 runs PostgreSQL. The pg_cpu_utilization collector does not run on that engine — its own AppliesTo gate excludes it — so this server does not collect the data, and never will. This is a permanent engine capability gap, not a collection outage.",
            "No stored plan for this query. pg_stat_statements has none.",
        ], out var line);
        Assert.Equal(line, o[0]);
        Assert.DoesNotContain("AppliesTo", o[0]);
        Assert.Equal("No stored plan for this query. pg_stat_statements has none.", o[1]);
    }

    /// <summary>#6178: the sentence the service writes for an Azure SQL Database names the engine on the page, from the real gate text; the
    /// PostgreSQL sentence keeps the generic line.</summary>
    [Fact]
    public void TheEngineGateSentence_NamesAzureSqlDatabase_WhenThatIsTheEngine()
    {
        var azure = PerformanceMonitor.Collectors.CollectorEngineCapability.NotCollectedMessage("example-azure-db", 5, null, "cpu_scheduler_stats", awsRdsKind: AwsRdsKind.None)!;
        var o = Run([azure], out var generic);
        Assert.Equal("Azure SQL Database does not have this data.", o[0]);
        Assert.NotEqual(generic, o[0]);
    }

    /// <summary>The Running Jobs panel showed the 1222-character "never run" answer written for an MCP client (it labels itself a runtime
    /// PRECONDITION and says to retry). From the real builder's text: the page says which data is missing and keeps the possible causes
    /// the server names, as written.</summary>
    [Fact]
    public void TheNeverRanAnswer_BecomesTwoShortSentences_KeepingThePossibleCauses()
    {
        var now = new DateTime(2026, 10, 25, 12, 0, 0, DateTimeKind.Utc);
        var real = PerformanceMonitor.Collectors.CollectorRuntimePrecondition.GatedOffMessage(
            "ui-sql", "running_jobs", PerformanceMonitor.Collectors.CollectorRuntimePrecondition.RunningJobsPossibleCauses,
            null, now, now.AddDays(-3))!;
        Assert.Contains("has never run against", real);
        Assert.True(real.Length > 1000, "the builder's text is the long one");

        var o = Run([real], out _);
        Assert.Equal(
            "The Running Jobs collector has never run for this server, so there is nothing to show. " +
            PerformanceMonitor.Collectors.CollectorRuntimePrecondition.RunningJobsPossibleCauses +
            " After you fix the cause, the service must reconnect to this server before collection starts again. " +
            "Restart the service, or change this server's settings.", o[0]);
        Assert.Contains("must reconnect to this server", o[0]);
        Assert.DoesNotContain("PRECONDITION", o[0]);
        Assert.DoesNotContain("Retrying", o[0]);
    }

    [Fact]
    public void TheGoneDarkAnswer_BecomesOneShortLine_AndAPreconditionWithAStatementKeepsIt()
    {
        var now = new DateTime(2026, 10, 25, 12, 0, 0, DateTimeKind.Utc);
        var dark = PerformanceMonitor.Collectors.CollectorRuntimePrecondition.GatedOffMessage(
            "ui-sql", "running_jobs", PerformanceMonitor.Collectors.CollectorRuntimePrecondition.RunningJobsPossibleCauses,
            now.AddDays(-5), now, now.AddDays(-9))!;
        Assert.Contains("no longer being invoked", dark);

        var session = PerformanceMonitor.Collectors.CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "ui-sql", "system_health", PerformanceMonitor.Collectors.CollectorRuntimePrecondition.CaptureSessionMissingStatus, null, now)!;
        Assert.Contains("runtime PRECONDITION", session);

        var o = Run([dark, session], out _);
        Assert.StartsWith("The Running Jobs collector has not run for this server for over 24 hours, so there is nothing current to show. Possible cause:", o[0]);
        Assert.EndsWith("After you fix the cause, the service must reconnect to this server before collection starts again. " +
            "Restart the service, or change this server's settings.", o[0]);
        Assert.DoesNotContain("PRECONDITION", o[0]);
        Assert.Contains("ALTER ANY EVENT SESSION", o[1]);
        Assert.DoesNotContain("PRECONDITION", o[1]);
        Assert.DoesNotContain("nothing to restart", o[1]);
    }

    /// <summary>The three Query Store answers (error, read-only, off) end on the shared precondition sentence, which the page drops. What
    /// each one names that the reader needs (the ALTER statement, the consistency check) stays, built from the real builder.</summary>
    [Theory]
    [InlineData("ERROR", "sys.sp_query_store_consistency_check")]
    [InlineData("READ_ONLY", "OPERATION_MODE = READ_WRITE")]
    [InlineData("OFF", "SET QUERY_STORE = ON")]
    public void TheQueryStoreAnswers_KeepTheStatementTheyName_AndDropThePreconditionSentence(string state, string named)
    {
        var real = PerformanceMonitor.Collectors.CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            "ui-sql", new[] { new PerformanceMonitor.Collectors.CollectorRuntimePrecondition.QueryStoreDatabaseState("AppDb", state) },
            new DateTime(2026, 10, 25, 12, 0, 0, DateTimeKind.Utc))!;
        Assert.Contains("runtime PRECONDITION", real);
        Assert.Contains("ALTER DATABASE", real);
        Assert.Contains(named, real);

        var o = Run([real], out _)[0];
        Assert.Contains("ALTER DATABASE", o);
        Assert.Contains(named, o);
        Assert.DoesNotContain("PRECONDITION", o);
        Assert.DoesNotContain("nothing to restart", o);
    }

    /// <summary>Text without the precondition closing sentence is not touched by that rule.</summary>
    [Fact]
    public void ATextWithoutThePreconditionMarks_IsUnchanged()
    {
        const string plain = "The running jobs table has no rows for this window. Try a wider range.";
        Assert.Equal(plain, Run([plain], out _)[0]);
    }

    /// <summary>#6439: the tempdb File I/O Latency panel on an Azure SQL Database says the same short line as the other Azure panels, not "check
    /// that collection is running"; any other engine gets no such answer and keeps its own miss.</summary>
    [Fact]
    public void TheTempDbFileIoAzureAnswer_IsNotCollected_AndBecomesTheAzureLine()
    {
        var json = PerformanceMonitor.Darling.Service.Mcp.DarlingEngineCapability.TempDbFileIoNotCollectedStatus("example-azure-db", 5)!;
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString()!;
        Assert.DoesNotContain("Check that collection is running", message);

        var o = Run([message], out _);
        Assert.Equal("Azure SQL Database does not have this data.", o[0]);

        Assert.Null(PerformanceMonitor.Darling.Service.Mcp.DarlingEngineCapability.TempDbFileIoNotCollectedStatus("example-sql", 3));
        Assert.Null(PerformanceMonitor.Darling.Service.Mcp.DarlingEngineCapability.TempDbFileIoNotCollectedStatus("example-mi", 8));
        Assert.Null(PerformanceMonitor.Darling.Service.Mcp.DarlingEngineCapability.TempDbFileIoNotCollectedStatus("unknown", 0));
    }
}

/// <summary>Collection Health on a server without Always On lists the two AG collectors as not applicable.</summary>
public sealed class CollectionHealthAlwaysOnRowsTests
{
    [Theory]
    [InlineData("ag_replica_states", true)]
    [InlineData("ag_database_replica_states", true)]
    [InlineData("wait_stats", false)]
    public void OnlyTheTwoAlwaysOnCollectors_AreAlwaysOnCollectors(string name, bool expected) =>
        Assert.Equal(expected, PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.IsAlwaysOnCollector(name));

    [Fact]
    public void ANotApplicableRow_SaysWhy_WithoutAGateName()
    {
        Assert.Contains("not enabled on this server", PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffMessage);
        Assert.DoesNotContain("needs a look", PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffMessage);
        Assert.Empty(PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffRows([]));
    }

    private static PerformanceMonitor.Darling.Service.Mcp.CollectorHealth AgRow(string name, bool failing)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new PerformanceMonitor.Darling.Service.Mcp.CollectorHealth
        {
            CollectorName = name,
            TotalRuns = 10,
            SuccessCount = failing ? 0 : 10,
            ErrorCount = failing ? 10 : 0,
            LastSuccessTime = failing ? null : last,
            LastRunTime = last,
            LastError = failing ? "Msg 1234: the AG DMV read failed" : null,
        };
    }

    [Fact]
    public void AHealthyAgCollector_IsReplacedByTheNotApplicableRow()
    {
        var healthy = AgRow("ag_replica_states", failing: false);
        var rows = new[] { healthy, AgRow("wait_stats", failing: false) };

        Assert.Equal("HEALTHY", healthy.HealthStatus);
        Assert.Equal(new[] { "wait_stats" }, PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.RowsShownWhenAlwaysOnOff(rows).Select(r => r.CollectorName));
        Assert.Single(PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffRows(rows));
    }

    [Fact]
    public void AFailingAgCollector_KeepsItsLogRowAndItsLastError()
    {
        /* Round-1 L5: a HADR-off server whose AG collector is actually failing used to lose the row, and with it the
           Last Error, behind the not-applicable sentence. */
        var failing = AgRow("ag_database_replica_states", failing: true);
        var rows = new[] { failing, AgRow("ag_replica_states", failing: false) };

        Assert.NotEqual("HEALTHY", failing.HealthStatus);
        var shown = PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.RowsShownWhenAlwaysOnOff(rows);
        Assert.Equal(new[] { "ag_database_replica_states" }, shown.Select(r => r.CollectorName));
        Assert.Equal("Msg 1234: the AG DMV read failed", shown[0].LastError);
        var notApplicable = PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffRows(rows);
        Assert.Single(notApplicable);
        Assert.Contains("ag_replica_states", System.Text.Json.JsonSerializer.Serialize(notApplicable));
        Assert.DoesNotContain("ag_database_replica_states", System.Text.Json.JsonSerializer.Serialize(notApplicable));
    }
}
