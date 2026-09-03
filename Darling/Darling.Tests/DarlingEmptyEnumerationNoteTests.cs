/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1837 (minimal core), Darling half: an enumerated collector whose enumeration query returns NO items
/// recorded a bare SUCCESS/0-rows row, indistinguishable from a healthy collector whose databases were
/// simply quiet. The status deliberately STAYS SUCCESS; the fix is a fixed, greppable message on the
/// collection_log row, carried out of the runner on <see cref="CollectorRunResult.Note"/> — the Darling
/// twin of Lite's per-run telemetry slot. Lite.Tests (<c>EmptyEnumerationNoteTests</c>) pins
/// the same contract on the same shared constant; parity is the point.
///
/// The zero-items branch needs a live SQL Server, so its wiring is pinned at source (the #1805
/// DarlingLockTimeoutYieldTests idiom). The record's carrying behavior is pinned for real.
/// </summary>
public sealed class DarlingEmptyEnumerationNoteTests
{
    [Fact]
    public void The_Message_Is_Fixed_And_Shared_With_Lite()
    {
        /* The identical literal Lite.Tests pins, asserted independently here: if either app's copy of
           this expectation is edited alone, one suite fails. The value lives once, on the shared
           EnumeratedCollectorDriver, so the runners cannot drift on what an operator greps for. */
        Assert.Equal(
            "enumeration yielded 0 items - nothing to collect this cycle",
            EnumeratedCollectorDriver.EmptyEnumerationMessage);
    }

    [Fact]
    public void An_Ordinary_Run_Result_Carries_No_Note()
    {
        /* The default keeps every other collector's row exactly as it was — message column null. */
        Assert.Null(new CollectorRunResult(12, 34, 56).Note);
    }

    [Fact]
    public void A_Run_Result_Round_Trips_The_Note()
    {
        var result = new CollectorRunResult(0, 5, 0, EnumeratedCollectorDriver.EmptyEnumerationMessage);

        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, result.Note);
        Assert.Equal(0, result.Rows);
    }

    [Fact]
    public void Runner_Takes_Its_Note_From_The_Shared_Enumeration_Read()
    {
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        /* Strengthened from the original "constructs the result with the shared constant" pin when
           #1837's probe-failure contract landed: the note can now be the empty-enumeration message, the
           probe-failure summary, or both, so pinning ONE of those literals would no longer prove the host
           cannot drift. Routing the whole enumeration read — items, probe failures, and the composed
           note — through the shared driver does, because there is then no host-side text at all. Lite's
           twin pin asserts the identical routing on its runner. */
        Assert.Contains("EnumeratedCollectorDriver.ReadEnumerationAsync(enumerationReader, cancellationToken)", source);
        Assert.Contains("new CollectorRunResult(0, sqlMs, 0, enumeration.Note)", source);

        /* Via the shared driver, never a copy of the text — a literal here is exactly the drift this
           fix exists to prevent. */
        Assert.DoesNotContain("\"enumeration yielded 0 items", source);
        Assert.DoesNotContain("failed their enumeration probe", source);
    }

    [Fact]
    public void Runner_Carries_The_Note_Onto_The_Success_Return_Too()
    {
        /* The partial case: items WERE enumerated but some of their probes failed, so the run collects
           normally and returns through the success path at the bottom of the method — which built a
           note-less result until #1837's contract. Without this the probe summary would reach the store
           only when the enumeration came back completely empty. */
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        Assert.Contains("collectionNote = enumeration.Note;", source);

        /* The success return gained the fan-out rollup at #2472 and the server-scoped phase split at #2851,
           and this pin moved with each of them rather than being loosened to a substring. Naming the whole
           argument list is the point: it is what makes an argument DROPPED from this call — the note
           included — fail here instead of silently reaching the store as a null.

           #2851 wrapped the call across lines, which broke the single-line literal this used to be. Collapsing
           runs of whitespace before matching makes the pin survive REFORMATTING while still failing on a
           dropped argument, which is the property it exists for — the previous form conflated the two, so a
           pure line-wrap failed it exactly as loudly as a real regression would have. */
        var collapsed = Regex.Replace(source, @"\s+", " ");

        Assert.Contains(
            "return new CollectorRunResult( rowsWritten, sqlMs, storageMs, collectionNote, fanout.Result, " +
            "ServerPhasesMeasured: serverPhasesMeasured, ServerOpenMs: context.ServerScopeOpenMs, " +
            "ServerDrainMs: context.ServerScopeDrainMs, ServerWatermarkMs: serverWatermarkMs);",
            collapsed);
    }

    [Fact]
    public void Worker_Passes_The_Note_To_The_Collection_Log_Write()
    {
        /* The note reaches error_message through LogCollectionAsync's message parameter, on the write for
           a run that RETURNED rather than threw. That write's status was a hardcoded "SUCCESS" until #2801,
           which is how a cycle abandoned by the #2673 wall-clock budget — storing nothing, advancing no
           watermark — inherited a success status. Pinned as the shared classifier rather than a literal so
           it cannot quietly go back to one; the note still rides this same write, which is the original
           claim and is unchanged. */
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        Assert.Contains("status, result.Rows, result.SqlMs, result.StorageMs, result.Note", source);
        Assert.Contains("EnumeratedCollectorDriver.ClassifyReturnedRun(result.Abandoned)", source);
    }

    [Fact]
    public void Last_Error_Stays_Gated_On_Failure_Statuses_Not_On_Message_Presence()
    {
        /* The read-side guard that keeps this note out of the Collection Health "last error" surface, in
           both readers. A broadening to error_message IS NOT NULL would turn every quiet enumeration
           cycle into a fake last-error.

           #1855 replaced the value-MAX with a newest-first rank, so the gate now reads as the status
           re-check on the rank-1 row. It is the SAME claim: the column can only ever be filled from a
           failing run. Without the re-check the rank falls through to the newest row of any class when
           no failure carried text, and a SUCCESS row's note would land here. Lite's twin pin asserts the
           identical expression against its DuckDB read. */
        foreach (var relative in new[]
        {
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.CollectionHealth.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs"),
        })
        {
            var source = ReadRepoFile(relative);
            Assert.Contains("MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS') THEN error_message END) AS last_error", source);
            Assert.DoesNotContain("error_message IS NOT NULL", source);
        }
    }

    /* ── #1837 health visibility: the note gets its own column, and it is NOT an error ── */

    [Fact]
    public void Health_Reads_Surface_The_Note_Gated_On_SUCCESS()
    {
        /* Both Darling readers, matching Lite. Gated on SUCCESS, not on "not a failure status": the
           runners attach a note only to the SUCCESS write, and the looser complement of last_error would
           drag Darling's SESSION_MISSING rows — a real capture fault with its own self-alert — into a
           column whose tooltip promises it is NOT an error. Every gate on this surface is still a STATUS
           gate — #1855's rank orders on whether the status-gated CASE came back empty, never on message
           presence alone (the pin above). */
        foreach (var relative in new[]
        {
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.CollectionHealth.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs"),
        })
        {
            var source = ReadRepoFile(relative);
            Assert.Contains("MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note", source);
            Assert.Contains("COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count", source);
        }
    }

    [Fact]
    public void The_Viewers_Fleet_Read_Projects_The_Same_Columns_As_The_Per_Server_One()
    {
        /* Both viewer health reads feed ONE mapper (MapHealthRow), so a column added to the per-server
           projection alone would make the fleet read throw on the new ordinal at runtime — a defect no
           per-SQL test would catch, because each SQL string is individually valid. */
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.CollectionHealth.cs"));

        var perServer = ViewerDataService.CollectionHealthSql;
        var fleet = ViewerDataService.FleetCollectionHealthSql;
        foreach (var column in new[] { "AS last_note", "AS note_count", "AS has_user_databases" })
        {
            Assert.Contains(column, perServer);
            Assert.Contains(column, fleet);
        }

        Assert.Contains("LastNote = reader.IsDBNull(11)", source);
        Assert.Contains("NoteCount = reader.IsDBNull(12)", source);
        Assert.Contains("TargetHasUserDatabases = !reader.IsDBNull(13)", source);

        /* #1855: the fleet read holds the ORDINALS but deliberately not the exemplar TEXT. Its only
           caller bands and counts; no surface renders a fleet row's message, and ranking them turns a
           parallel hash aggregate into a serial sort of the whole window (measured 0.84s -> 13.9s over
           200 servers / 4M rows on PG 18.4). NULL rather than the pre-#1855 lexicographic MAX, because a
           blank is honestly nothing while that MAX was quietly the wrong run's text. Pinned so a future
           edit that "completes the parity" has to meet the cost first — the note_count aggregate, which
           is order-independent and cheap, deliberately stays real on both. */
        Assert.Contains("CAST(NULL AS text) AS last_note", fleet);
        Assert.Contains("CAST(NULL AS text) AS last_error", fleet);
        Assert.Contains("COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count", fleet);
        Assert.DoesNotContain("CAST(NULL AS text)", perServer);

        /* #1852 holds the same line for the same reasons plus one of its own: the fleet read groups
           server_id INTO the result, so there is no single server to probe an inventory for. A truthful
           fleet version would be a cross-collector join across every enabled server on a query the status
           bar re-runs on every aggregate-tab refresh. The per-server read owns the subquery outright. */
        Assert.Contains("CAST(NULL AS integer) AS has_user_databases", fleet);
        Assert.Contains("FROM v_database_size_stats", perServer);
        Assert.DoesNotContain("v_database_size_stats", fleet);
    }

    [Fact]
    public void The_Web_Dashboards_Collection_Health_Table_Shows_The_Note_Too()
    {
        /* Darling has THREE Collection Health surfaces, not two: the WPF Viewer grid, the MCP tool, and
           the web dashboard's table, which renders whatever COLLECTOR_COLUMNS lists from that same tool's
           payload. A field added to the tool but not to that list is silently dropped, leaving the
           browser as the one surface still hiding what #1837 exists to show.

           The array moved from pages/server.js to pages/server-tabs.js when the web server page grew
           sub-tabs (#2475) - server.js is the shell now and the tab registry owns every column array. */
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

        /* The DEFINITION, not the earlier `columns: COLLECTOR_COLUMNS` use site. */
        var start = source.IndexOf("const COLLECTOR_COLUMNS", System.StringComparison.Ordinal);
        Assert.True(start >= 0, "server-tabs.js must still define COLLECTOR_COLUMNS");
        var columns = source[start..];
        columns = columns[..columns.IndexOf("];", System.StringComparison.Ordinal)];

        Assert.Contains("last_error", columns);

        /* note_summary, not the raw last_note: the table must carry the SAME qualified text the two WPF
           grids render, or a collector empty all week and one empty once read identically here - the
           exact ambiguity this feature exists to resolve. Composed server-side from the shared
           formatter, so the browser never re-derives it. */
        Assert.Contains("note_summary", columns);
        Assert.DoesNotContain("\"last_note\"", columns);
    }

    [Fact]
    public void Both_Apps_Mcp_Tools_Emit_The_Composed_Note_Summary()
    {
        /* The MCP response shape is deliberately identical across the SKUs, and this field is what the
           web table binds to - added to one app only, the web surface would break or drift. */
        foreach (var relative in new[]
        {
            Path.Combine("Lite", "Mcp", "McpHealthTools.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"),
        })
        {
            /* Whitespace-collapsed so the pin survives the line wrapping the argument list needs, rather
               than pinning one formatting of it. */
            var source = Regex.Replace(ReadRepoFile(relative), @"\s+", " ");
            Assert.Contains(
                "note_summary = CollectorHealthClassifier.FormatCollectionNote( r.LastNote, r.NoteCount, r.TotalRuns, r.CollectorName, r.TargetHasUserDatabases)",
                source,
                StringComparison.Ordinal);

            /* #1852: the raw flag rides the payload too, so an MCP caller diagnosing an empty collector
               gets a boolean rather than having to parse the qualifier out of note_summary's sentence. */
            Assert.Contains("target_has_user_databases = r.TargetHasUserDatabases", source, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void The_Note_Never_Reaches_The_Banding()
    {
        /* Constraint (a)/(b) of #1837's design: the band order and its inputs are untouched, so a target
           that is legitimately empty — no user databases, no AGs, nothing matching a filter — keeps
           reading HEALTHY. Two collectors identical except for the note must band identically. */
        var quiet = new CollectorHealthRow
        {
            CollectorName = "query_store",
            TotalRuns = 96,
            SuccessCount = 96,
            LastSuccessTime = System.DateTime.UtcNow.AddMinutes(-5),
            LastRunTime = System.DateTime.UtcNow.AddMinutes(-5),
        };
        var annotated = new CollectorHealthRow
        {
            CollectorName = "query_store",
            TotalRuns = 96,
            SuccessCount = 96,
            LastSuccessTime = quiet.LastSuccessTime,
            LastRunTime = quiet.LastRunTime,
            LastNote = EnumeratedCollectorDriver.EmptyEnumerationMessage,
            NoteCount = 96,
        };

        Assert.Equal(CollectorHealthClassifier.Healthy, quiet.HealthStatus);
        Assert.Equal(quiet.HealthStatus, annotated.HealthStatus);
    }

    [Theory]
    /* Nothing to say — the overwhelmingly common row — stays blank rather than shouting "OK". */
    [InlineData(null, 0L, 96L, "")]
    [InlineData("", 0L, 96L, "")]
    /* A note counted zero times is incoherent input; blank beats a "(0 of N)" that reads like a defect. */
    [InlineData("note", 0L, 96L, "")]
    /* The distinction the issue asks for: sometimes-empty is normal, always-empty is the signal. */
    [InlineData("note", 3L, 96L, "note (3 of 96 runs)")]
    [InlineData("note", 96L, 96L, "note (all 96 runs)")]
    public void Note_Qualifier_Says_How_Much_Of_The_Window_Was_Empty(string? note, long noteCount, long totalRuns, string expected)
    {
        /* The identical expectations Lite.Tests pins, asserted independently here against the identical
           shared helper — Erik's parity rule in test form. */
        Assert.Equal(expected, CollectorHealthClassifier.FormatCollectionNote(note, noteCount, totalRuns));
    }

    [Fact]
    public void Note_Qualifier_Is_The_Shared_One_Both_Apps_Render()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "query_store",
            TotalRuns = 96,
            LastNote = EnumeratedCollectorDriver.EmptyEnumerationMessage,
            NoteCount = 96,
        };

        Assert.Equal(
            CollectorHealthClassifier.FormatCollectionNote(row.LastNote, row.NoteCount, row.TotalRuns),
            row.NoteFormatted);
        Assert.Contains("(all 96 runs)", row.NoteFormatted);

        /* #1852 gave the formatter two more inputs, so "the property delegates" is only still a real
           claim if the property passes them. Asserted on a row where they CHANGE the answer: with the
           inventory flag set, the three-argument rendering and the property must now DIFFER. */
        row.TargetHasUserDatabases = true;

        Assert.Equal(
            CollectorHealthClassifier.FormatCollectionNote(
                row.LastNote, row.NoteCount, row.TotalRuns, row.CollectorName, row.TargetHasUserDatabases),
            row.NoteFormatted);
        Assert.NotEqual(
            CollectorHealthClassifier.FormatCollectionNote(row.LastNote, row.NoteCount, row.TotalRuns),
            row.NoteFormatted);
    }

    /* Locate the repo from this file — the DarlingLockTimeoutYieldTests idiom; no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }

    /// <summary>
    /// #2801: the Darling half of the abandonment wiring. The runner must MARK the budget-expiry return
    /// as abandoned, because the worker classifies from that flag rather than from the note text -- the
    /// two are separately editable, and a note reworded while the flag went unset would put the status
    /// silently back to SUCCESS with nothing failing.
    ///
    /// <para>Source-text pinned, matching every other pin in this class: the abandonment sits inside a
    /// catch filter on a live provider cancellation deep in RunOneAsync, which no unit test here can
    /// reach. Lite.Tests covers the same path end-to-end and pins ClassifyReturnedRun as a pure
    /// function; this asserts the one thing neither of those can see, that THIS host sets the flag.</para>
    /// </summary>
    [Fact]
    public void Runner_Marks_The_BudgetExpiry_Return_As_Abandoned()
    {
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        Assert.Contains("Abandoned: true", source, StringComparison.Ordinal);
        Assert.Contains("EnumeratedCollectorDriver.WholeCycleBudgetNote(budgetSeconds)", source, StringComparison.Ordinal);
    }
}
