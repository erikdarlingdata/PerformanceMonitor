/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3754: a collector whose per-item work threw must not land as SUCCESS, and the health surface must not
/// then say it read and found nothing.
///
/// <para>On an Azure SQL DB target two collectors failed on EVERY monitored database every sweep. The
/// service log carried the failures; the store recorded <c>SUCCESS, rows = 0</c> for both; and
/// <c>get_collection_health</c> reported both HEALTHY with <c>errors: 0</c> and an output finding asserting
/// that the collector had "read and found nothing rather than being unable to read", that it "needs no
/// action", that it "stores a row only when an event occurs" (one of the two is a configuration snapshot),
/// and that "no run recorded a note" - the last being the bug described as its own evidence.</para>
///
/// <para>Three places produced that, and this suite pins the Lite half of each. The ENUMERATED fan-out's
/// per-item catch handed the exception to a host closure that only logged it, so the driver now keeps the
/// account (<c>EnumeratedCollectorDriverTests</c> pins the driver) and both runners compose the #2623 partial
/// note and rethrow on all-failed from it. The long-query collector's XE session is reconciled OUTSIDE its
/// run, so a refused CREATE never reached the run record; the reconcile's failure is now kept per server and
/// rethrown into the run's classification. And the shared output finding took no fault count and no
/// category, so it offered the event-collector resting-state sentence to every zero-row window that left no
/// note; it now takes both.</para>
/// </summary>
public sealed class SwallowedItemFailureTests
{
    /* ── the runners consume the driver's account, both of them ── */

    /// <summary>
    /// Both hosts' enumerated paths must do the two things their Azure per-database loops already did
    /// (#2623): merge the partial note onto the SUCCESS row, and rethrow the first failure when every item
    /// failed. Anchored on the RESULT members rather than on a message string, because the members are the
    /// shared account and a host that re-derived its own count inside onItemError is the drift this exists
    /// to refuse.
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs")]
    [InlineData("Lite/Services/RemoteCollectorService.DefinitionRunner.cs")]
    public void BothRunners_MergeThePartialNote_AndRethrowWhenEveryItemFailed(string relativePath)
    {
        var source = ReadLf(relativePath);

        Assert.Contains("driverResult.PartialFailureNote", source, StringComparison.Ordinal);
        Assert.Contains("if (driverResult.AllItemsFailed)", source, StringComparison.Ordinal);
        Assert.Contains(
            "System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(driverResult.FirstError!).Throw();",
            source,
            StringComparison.Ordinal);

        /* The rethrow sits BETWEEN the driver call and the assignment of its rows, so a run that read
           nothing never reports rows or slices it does not have. */
        var driverCall = source.IndexOf("await EnumeratedCollectorDriver.RunAsync<TRow>(", StringComparison.Ordinal);
        var rethrow = source.IndexOf("if (driverResult.AllItemsFailed)", StringComparison.Ordinal);
        var rowsAssigned = source.IndexOf("rowsWritten = driverResult.Rows;", StringComparison.Ordinal);
        Assert.True(driverCall > 0 && rethrow > driverCall && rowsAssigned > rethrow,
            $"{relativePath}: the all-failed rethrow must sit between the driver call and the rows assignment");

        /* And the hosts do NOT keep a second count for this path: the driver's is the only one. The
           per-database loop's `failed++` arms are that loop's own (#2623) and stay; what must not appear
           is a count keyed on the enumerated path's onItemError. */
        Assert.DoesNotContain("onItemError: (item, ex) =>\n                    {\n                        failed++", source, StringComparison.Ordinal);
    }

    /* ── the long-query reconcile reaches the run, on this SKU ── */

    /// <summary>
    /// Lite's long-query reconcile runs from the collection loop, outside the run, and swallowed every
    /// failure; the blocked-process and deadlock ensures run INSIDE RunCollectorAsync and throw into its
    /// classification. The fix carries the reconcile's ENABLE failure to the run through a per-server slot
    /// and rethrows it before the read - the same exception object, so the classification arms see the
    /// type they know (XeSessionEnsureException for an all-databases refusal, its inner number deciding
    /// PERMISSIONS against ERROR).
    /// </summary>
    [Fact]
    public void LiteLongQueryReconcile_KeepsItsEnableFailure_AndTheRunRethrowsItBeforeReading()
    {
        var source = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.LongQueryCompletions.cs"));

        /* The slot, keyed per server, holding the EXCEPTION rather than its message. */
        Assert.Contains("ConcurrentDictionary<string, Exception> _longQueryTraceFault", source, StringComparison.Ordinal);

        /* Set in the reconcile's catch, gated on ENABLED - a failed DROP while disabled has no run to be
           honest on. */
        var catchArm = source.IndexOf("Failed to reconcile long-query completion XE session", StringComparison.Ordinal);
        var set = source.IndexOf("_longQueryTraceFault[server.Id] = ex;", StringComparison.Ordinal);
        Assert.True(catchArm > 0 && set > catchArm, "the fault is recorded in the reconcile's own catch");
        Assert.Contains("if (enabled)\n            {\n                _longQueryTraceFault[server.Id] = ex;", source, StringComparison.Ordinal);

        /* Cleared when a later reconcile succeeds, on both arms. */
        Assert.Equal(2, CountOccurrences(source, "_longQueryTraceFault.TryRemove(server.Id, out _);"));

        /* Rethrown in the collector's run BEFORE the definition read, with its original stack. */
        var collect = source.IndexOf("private async Task<int> CollectLongQueryCompletionsAsync(", StringComparison.Ordinal);
        var rethrow = source.IndexOf("System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ensureFailure).Throw();", StringComparison.Ordinal);
        var read = source.IndexOf("return await RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, server, cancellationToken);", StringComparison.Ordinal);
        Assert.True(collect > 0 && rethrow > collect && read > rethrow,
            "the recorded ensure failure is rethrown inside CollectLongQueryCompletionsAsync, ahead of the read");
    }

    /* ── the shared output finding reads the fault count and the category ── */

    private static CollectorHealthRow Row(
        string collectorName,
        long totalRuns,
        long successes,
        long errors = 0,
        long sessionMissing = 0,
        long rowsStored = 0,
        long noteCount = 0) => new()
    {
        CollectorName = collectorName,
        TotalRuns = totalRuns,
        SuccessCount = successes,
        ErrorCount = errors,
        SessionMissingCount = sessionMissing,
        RowsStored = rowsStored,
        RunsWithRows = rowsStored > 0 ? 1 : 0,
        NoteCount = noteCount,
        LastNote = noteCount > 0 ? "1 of 2 database(s) failed and were skipped (xedb1) - any rows this cycle are from the survivors ONLY, so a low or zero row count here is not evidence the server is quiet; first error: boom" : null,
        LastError = errors > 0 ? "Reference to database and/or server name in 'xedb1.sys.sp_executesql' is not supported in this version of SQL Server." : null,
        LastSuccessTime = successes > 0 ? DateTime.UtcNow.AddMinutes(-1) : null,
        LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        LastErrorTime = errors > 0 ? DateTime.UtcNow.AddMinutes(-1) : null,
    };

    /// <summary>
    /// THE ISSUE'S SHAPE, once the runner records the fault: one run, one ERROR, zero rows, a configuration
    /// collector. The finding must not say the collector read and found nothing, must not say it needs no
    /// action, must not offer the event-collector category, and must say the run could not read.
    /// </summary>
    [Fact]
    public void AnAllFaultedWindow_SaysUnableToRead_AndOffersNoRestingStateReading()
    {
        var row = Row("database_scoped_config", totalRuns: 1, successes: 0, errors: 1);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);

        Assert.Contains("UNABLE to read", finding, StringComparison.Ordinal);
        Assert.Contains("Every run in the window faulted", finding, StringComparison.Ordinal);
        Assert.Contains("needs action", finding, StringComparison.Ordinal);
        Assert.Contains("last_error", finding, StringComparison.Ordinal);

        Assert.DoesNotContain("read and found nothing", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("correct resting state", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("event occurs", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("No run recorded a note", finding, StringComparison.Ordinal);

        /* And the band is no longer HEALTHY: the on-load collector's 100% failure rate bands WARNING. */
        Assert.NotEqual(CollectorHealthClassifier.Healthy, row.HealthStatus);
    }

    /// <summary>
    /// SESSION_MISSING is the second "could not read" status, and until #3754 it reached this surface as
    /// total_runs and nothing else - so the long-query collector, once its refused session classifies it,
    /// would have banded FAILING beside errors 0 and the resting-state sentence. The count is its own
    /// column and the finding reads it with the errors.
    /// </summary>
    [Fact]
    public void ASessionMissingWindow_CountsAsCouldNotRead()
    {
        var row = Row("long_query_completions", totalRuns: 3, successes: 0, sessionMissing: 3);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("UNABLE to read", finding, StringComparison.Ordinal);
        Assert.Contains("3 of those runs recorded a fault", finding, StringComparison.Ordinal);
        Assert.Contains("SESSION_MISSING", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("read and found nothing", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);

        /* No success in the window: FAILING on the never-succeeded clock, as before - the count feeds the
           finding, never the band. */
        Assert.Equal(CollectorHealthClassifier.Failing, row.HealthStatus);
        Assert.Equal(0, row.ErrorCount);
    }

    /// <summary>
    /// SOME runs faulted: the resting-state reading is withheld for the whole window (it is a claim about
    /// every run), the survivors are described without the reassuring token, and the count is exact.
    /// </summary>
    [Fact]
    public void APartlyFaultedWindow_NamesTheSplit_AndStillWithholdsTheRestingStateReading()
    {
        var row = Row("deadlocks", totalRuns: 10, successes: 8, errors: 2);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("2 of those runs recorded a fault", finding, StringComparison.Ordinal);
        Assert.Contains("The other 8 runs completed and stored nothing", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("Every run in the window faulted", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("correct resting state", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// Faults and notes in one window - some cycles lost every database (ERROR), others lost some (a
    /// SUCCESS row with the #2623 note). The fault is the louder fact and leads; the note is named beside it
    /// rather than dropped, and neither is restated - the finding points at last_error and last_note.
    /// </summary>
    [Fact]
    public void AWindowWithFaultsAndNotes_LeadsWithTheFault_AndPointsAtBoth()
    {
        var row = Row("database_scoped_config", totalRuns: 4, successes: 2, errors: 2, noteCount: 2);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("2 of those runs recorded a fault", finding, StringComparison.Ordinal);
        Assert.Contains("2 of the runs also recorded a note", finding, StringComparison.Ordinal);
        Assert.Contains("last_error", finding, StringComparison.Ordinal);
        Assert.Contains("last_note", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("xedb1", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("sp_executesql", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// A configuration collector that stored nothing with NO fault, NO note and NO denial gets the snapshot
    /// sentence, never the event framing: its source came back empty on every run, and that is not a
    /// resting state.
    /// </summary>
    [Fact]
    public void ASnapshotCollectorsCleanZero_GetsTheSnapshotSentence_NotTheEventFraming()
    {
        var row = Row("database_scoped_config", totalRuns: 5, successes: 5);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("not an event-triggered collector", finding, StringComparison.Ordinal);
        Assert.Contains("not a resting state", finding, StringComparison.Ordinal);
        Assert.Contains("needs a look", finding, StringComparison.Ordinal);

        Assert.DoesNotContain("event occurs", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("correct resting state", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("read and found nothing", finding, StringComparison.Ordinal);

        /* Not a band: a legitimately empty configuration collector stays HEALTHY. */
        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
    }

    /// <summary>
    /// The positive control for every DoesNotContain above: an EVENT collector's clean zero keeps the
    /// reading operators have learned, token for token - and states the wider precondition it now rests
    /// on.
    /// </summary>
    [Theory]
    [InlineData("deadlocks")]
    [InlineData("blocked_process_report")]
    [InlineData("long_query_completions")]
    public void AnEventCollectorsCleanZero_KeepsTheRestingStateReading(string collector)
    {
        var row = Row(collector, totalRuns: 5, successes: 5);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("read and found nothing rather than being unable to read", finding, StringComparison.Ordinal);
        Assert.Contains("correct resting state", finding, StringComparison.Ordinal);
        Assert.Contains("needs no action", finding, StringComparison.Ordinal);
        Assert.Contains("No run recorded a note or a fault", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("not an event-triggered collector", finding, StringComparison.Ordinal);
    }

    /// <summary>The finding is about ZERO output: a productive window says nothing, faults or not.</summary>
    [Fact]
    public void AProductiveWindow_HasNoFinding_EvenWithFaults()
    {
        Assert.Null(Row("database_scoped_config", totalRuns: 4, successes: 3, errors: 1, rowsStored: 120).OutputFinding);
    }

    /* ── the event-collector set is closed, real, and correctly polarised ── */

    /// <summary>
    /// Every name on the set must be a collector that exists, or a rename silently drops a real event
    /// capture into the non-event sentence; and the set must carry the four collectors the tool
    /// description has always named as its examples plus the long-query capture the issue is about. The
    /// XE-session collectors are in by construction - a session name IS an event capture.
    /// </summary>
    [Fact]
    public void TheEventCollectorSet_NamesOnlyRealCollectors_AndTheDescribedOnes()
    {
        var catalog = new HashSet<string>(CollectorCatalog.All.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
        var set = CollectorHealthClassifier.EventCollectorNamesForPinning;

        Assert.NotEmpty(set);
        foreach (var name in set)
        {
            Assert.Contains(name, catalog);
            Assert.True(CollectorHealthClassifier.IsEventCollector(name));
        }

        /* The four the get_collection_health description names as examples, and the issue's collector. */
        foreach (var described in new[] { "deadlocks", "blocked_process_report", "pg_blocking", "pg_xmin_horizon", "long_query_completions" })
        {
            Assert.True(CollectorHealthClassifier.IsEventCollector(described), described);
        }

        /* The XE ring-buffer captures declare a session name; every one of them is an event capture. */
        foreach (var xe in new[] { DeadlocksCollector.XeSessionName, BlockedProcessReportCollector.XeSessionName, LongQueryCompletionsCollector.XeSessionName })
        {
            Assert.False(string.IsNullOrWhiteSpace(xe));
        }

        Assert.False(CollectorHealthClassifier.IsEventCollector(null));
    }

    /// <summary>
    /// The polarity that makes a name list acceptable here where #2511 refused one: it names the collectors
    /// to OFFER the reassuring sentence to, so the configuration and enumeration collectors - the ones the
    /// issue's false sentence landed on - must not be on it, and neither may any on-load snapshot.
    /// </summary>
    [Fact]
    public void TheEventCollectorSet_ExcludesEveryConfigurationAndEnumerationCollector()
    {
        foreach (var name in CollectorCatalog.All.Select(c => c.Name))
        {
            if (CollectorHealthClassifier.IsOnLoadCollector(name) || CollectorHealthClassifier.ExpectsUserDatabases(name))
            {
                Assert.False(CollectorHealthClassifier.IsEventCollector(name), $"{name} is a configuration or enumeration collector and must not get the resting-state sentence");
            }
        }

        Assert.False(CollectorHealthClassifier.IsEventCollector("database_scoped_config"));
        Assert.False(CollectorHealthClassifier.IsEventCollector("wait_stats"));
        Assert.False(CollectorHealthClassifier.IsEventCollector("database_size_stats"));
    }

    /* ── the read and the tool carry the new count, on both SKUs ── */

    [Fact]
    public void TheLiteHealthRead_SelectsTheSessionMissingCount_SoTheSharedFindingIsFullyFed()
    {
        var sql = LocalDataService.CollectionHealthSql;

        Assert.Contains("SUM(CASE WHEN status = 'SESSION_MISSING' THEN 1 ELSE 0 END) AS session_missing_count", sql, StringComparison.Ordinal);

        /* APPENDED: it is the last projected aggregate, after #3240's, because both MCP readers read this
           result set positionally. */
        Assert.True(
            sql.IndexOf("AS session_missing_count", StringComparison.Ordinal) > sql.IndexOf("AS extension_missing_count", StringComparison.Ordinal),
            "session_missing_count must be appended after extension_missing_count");
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs")]
    [InlineData("Lite/Mcp/McpHealthTools.cs")]
    public void BothSkusTools_PutTheSessionMissingCount_BesideTheErrors(string relativePath)
    {
        var source = ReadLf(relativePath);

        Assert.Contains("errors = r.ErrorCount,", source, StringComparison.Ordinal);
        Assert.Contains("session_missing = r.SessionMissingCount,", source, StringComparison.Ordinal);

        /* And the description teaches the reading: faulted runs withhold the resting-state sentence, and
           the sentence is offered only to the closed event-collector set. */
        Assert.Contains("errors plus session_missing above zero", source, StringComparison.Ordinal);
        Assert.Contains("closed list on the shared classifier", source, StringComparison.Ordinal);
        Assert.Contains("database_scoped_config, which returns a row per setting per database", source, StringComparison.Ordinal);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /// <summary>The source with line endings normalised to LF, so a multi-line pin reads the same on a
    /// CRLF checkout (this repo's .gitattributes) and on any other.</summary>
    private static string ReadLf(string relativePath) =>
        File.ReadAllText(FindRepoFile(relativePath)).Replace("\r\n", "\n", StringComparison.Ordinal);

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
