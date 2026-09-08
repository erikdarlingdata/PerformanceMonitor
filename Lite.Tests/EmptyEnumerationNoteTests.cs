/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1837 (minimal core), Lite half: an enumerated collector whose enumeration query returns NO items
/// used to record a bare SUCCESS/0-rows row — byte-identical to a healthy collector whose databases
/// were simply quiet, and to query_store finding no Query-Store-enabled database at all. The status
/// deliberately STAYS SUCCESS (nothing failed; the health-banding design is the rest of #1837); the fix
/// is a fixed, greppable message on the collection_log row that normally leaves that column null.
///
/// The zero-items branch itself needs a live SQL Server (it is the tail of a real enumeration read), so
/// the wiring is pinned at source where it lives — the #1805 LockTimeoutYieldTests idiom. What IS
/// reachable, and is the actual regression risk this fix introduces, is pinned for real: a non-null
/// message on a SUCCESS row must stay inert everywhere health is computed.
/// </summary>
public class EmptyEnumerationNoteTests
{
    private const int ServerId = 4242;

    /* ── the shared message (the cross-app contract) ── */

    [Fact]
    public void The_Message_Is_Fixed_And_Shared_By_Both_Runners()
    {
        /* Fixed text, because its whole job is to be greppable in a support log. It lives on the shared
           EnumeratedCollectorDriver — the one owner of the enumerated path — so Lite and Darling cannot
           drift on the wording an operator searches for. Darling.Tests pins the identical literal. */
        Assert.Equal(
            "enumeration yielded 0 items - nothing to collect this cycle",
            EnumeratedCollectorDriver.EmptyEnumerationMessage);
    }

    /* ── the runner wiring (source pins — the branch is the tail of a live enumeration read) ── */

    [Fact]
    public void Runner_Takes_Its_Note_From_The_Shared_Enumeration_Read()
    {
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs")));

        /* Strengthened from the original "assigns the shared constant" pin when #1837's probe-failure
           contract landed: the note can now be the empty-enumeration message, the probe-failure summary,
           or both, so pinning ONE of those literals would no longer prove the host cannot drift. Routing
           the whole enumeration read — items, probe failures, and the composed note — through the shared
           driver does, because there is then no host-side text at all. */
        Assert.Contains("EnumeratedCollectorDriver.ReadEnumerationAsync(enumerationReader, cancellationToken)", source);
        Assert.Contains("telemetry.HostNote = enumeration.Note;", source);

        /* Via the shared driver, never a copy of the text — a literal here is exactly the drift this
           fix exists to prevent. */
        Assert.DoesNotContain("\"enumeration yielded 0 items", source);
        Assert.DoesNotContain("failed their enumeration probe", source);
    }

    [Fact]
    public void The_Note_Is_Assigned_Before_The_Zero_Item_Early_Return()
    {
        /* The ordering that makes the whole fix work: the zero-item branch RETURNS, so a note assigned
           after it would annotate every cycle except the one that needed it. Pinned by position because
           the branch itself is the tail of a live enumeration read. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs")));

        var assignment = source.IndexOf("telemetry.HostNote = enumeration.Note;", StringComparison.Ordinal);
        var earlyReturn = source.IndexOf("if (items.Count == 0)", StringComparison.Ordinal);

        Assert.True(assignment >= 0 && earlyReturn >= 0, "both the note assignment and the zero-item branch must exist");
        Assert.True(assignment < earlyReturn, "the note must be assigned before the zero-item early return");
    }

    [Fact]
    public void Runner_Resets_The_Note_With_The_Timing_Fields_Every_Run()
    {
        /* The note rides the same per-run telemetry slot as the sql/storage timings, so it must be
           cleared at the top of every definition run — otherwise one empty enumeration would annotate
           the NEXT collector's row too. The slot is keyed by SERVER because servers collect in parallel;
           a plain field would let one server's note land on another's row. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs")));

        Assert.Contains("telemetry.ResetNote();", source);

        var service = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.cs")));
        Assert.Contains("ConcurrentDictionary<int, RunTelemetry> _runTelemetry", service);
    }

    [Fact]
    public void One_Servers_Note_Cannot_Land_On_Another_Servers_Row()
    {
        /* A collection cycle runs the monitored servers in PARALLEL on this one service instance, so
           while the note lived in a plain field, server B's reset at the top of its run could blank
           server A's pending note, and A's "enumeration yielded 0 items" could be read onto B's row for
           a collector that does not even enumerate. The slot is keyed by server; the cycle's other rule
           (collectors within one server run sequentially) is what makes that key sufficient. */
        var service = CreateService();

        var serverA = service.TelemetryFor(1);
        var serverB = service.TelemetryFor(2);
        Assert.NotSame(serverA, serverB);

        serverA.HostNote = EnumeratedCollectorDriver.EmptyEnumerationMessage;
        serverA.SqlMs = 1234;

        /* Server B starting its own run resets ITS slot — the shape RunCollectorAsync uses. */
        serverB.ResetNote();
        serverB.SqlMs = 0;

        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, serverA.Note);
        Assert.Equal(1234, serverA.SqlMs);

        /* And the same server always gets the same slot back, or the reset and the read would target
           different objects within one run. */
        Assert.Same(serverA, service.TelemetryFor(1));
    }

    [Fact]
    public void RunCollectorAsync_Carries_The_Note_Onto_The_Collection_Log_Row()
    {
        /* The note reaches the row through errorMessage — the parameter LogCollectionAsync writes to the
           error_message column — assigned on the success path only, where errorMessage is provably null
           (only the catch blocks assign it). */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.cs")));

        Assert.Contains("errorMessage = telemetry.Note;", source);
    }

    /* ── the neutrality this fix depends on (real assertions) ── */

    [Fact]
    public void A_Message_On_A_SUCCESS_Row_Is_Not_An_Erroring_Collector()
    {
        /* The regression this fix could have caused: SUCCESS rows never carried a message before, so
           anything that treated "has a message" as "failed" would now mark every quiet enumerated
           collector unhealthy. Health tracking keys on STATUS — a SUCCESS resets the streak and ignores
           the message entirely — and that must stay true. */
        var service = CreateService();

        service.RecordCollectorResult(ServerId, "query_store", "SUCCESS",
            EnumeratedCollectorDriver.EmptyEnumerationMessage);

        var summary = service.GetHealthSummary(ServerId);
        Assert.Equal(0, summary.ErroringCollectors);
        Assert.Empty(summary.Errors);
    }

    [Fact]
    public void A_Message_On_A_SUCCESS_Row_Still_Clears_A_Real_Error_Streak()
    {
        /* An annotated success is still a success: the collector demonstrably ran, so it must clear a
           prior FAILING streak exactly as an unannotated one does. */
        var service = CreateService();

        service.RecordCollectorResult(ServerId, "query_store", "ERROR", "genuine failure");
        Assert.Equal(1, service.GetHealthSummary(ServerId).ErroringCollectors);

        service.RecordCollectorResult(ServerId, "query_store", "SUCCESS",
            EnumeratedCollectorDriver.EmptyEnumerationMessage);
        Assert.Equal(0, service.GetHealthSummary(ServerId).ErroringCollectors);
    }

    [Fact]
    public void Last_Error_Stays_Gated_On_Failure_Statuses_Not_On_Message_Presence()
    {
        /* The read-side guard that keeps this note out of the Collection Health "last error" surface. A
           broadening to error_message IS NOT NULL would turn every quiet enumeration cycle into a fake
           last-error — the note is deliberately visible ONLY in the raw collection-log detail grid. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "LocalDataService.CollectionHealth.cs")));

        /* #1855 replaced the value-MAX with a newest-first rank, so the gate now reads as the status
           re-check on the rank-1 row. It is the SAME claim: the column can only ever be filled from a
           failing run. Without the re-check the rank falls through to the newest row of any class when
           no failure carried text, and a SUCCESS row's note would land here. */
        Assert.Contains("MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS') THEN error_message END) AS last_error", source);
        Assert.DoesNotContain("error_message IS NOT NULL", source);
    }

    /* ── #1837 health visibility: the note gets its own column, and it is NOT an error ── */

    [Fact]
    public void Health_Read_Surfaces_The_Note_Gated_On_SUCCESS()
    {
        /* Gated on SUCCESS, not on "not a failure status": the runners attach a note only to the SUCCESS
           write, and the looser complement of last_error would drag SESSION_MISSING and CANCELLED
           messages into a column whose tooltip promises it is NOT an error. Every gate on this surface
           is still a STATUS gate — #1855's rank orders on whether the status-gated CASE came back empty,
           never on message presence alone (the pin above), so no read here can key on the fact that a
           row has text without first asking what kind of row it is. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "LocalDataService.CollectionHealth.cs")));

        Assert.Contains("MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note", source);
        Assert.Contains("COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count", source);
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
            LastSuccessTime = DateTime.UtcNow.AddMinutes(-5),
            LastRunTime = DateTime.UtcNow.AddMinutes(-5),
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
        Assert.Equal(expected, CollectorHealthClassifier.FormatCollectionNote(note, noteCount, totalRuns));
    }

    [Fact]
    public void Note_Qualifier_Is_The_Shared_One_Both_Apps_Render()
    {
        /* Erik's parity rule in test form: the grid text lives in PerformanceMonitor.Common, so Lite and
           the Darling Viewer cannot render the same store row two different ways. Darling.Tests pins the
           identical expectations against the identical helper. */
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

    /* ── helpers ── */

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
