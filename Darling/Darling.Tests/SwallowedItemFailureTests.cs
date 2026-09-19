/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3754, the Darling half: a collector whose per-item work threw must not land as SUCCESS, and
/// <c>get_collection_health</c> must not then say it read and found nothing. The Lite suite of the same
/// name pins the shared driver's consumers and the Lite reconcile; this one pins the three Darling-only
/// mechanisms and the tool's own row.
///
/// <para>The issue's server: an Azure SQL DB logical server with two user databases. <c>database_scoped_config</c>
/// took the ENUMERATED fan-out and its per-item query was rejected in every database; the driver handed
/// each exception to the runner's <c>onItemError</c>, which logged it, and the run recorded SUCCESS with
/// zero rows. <c>long_query_completions</c>'s XE session was refused in every database by the per-sweep
/// reconcile, which caught each refusal, logged it and returned - so the worker latched the state as
/// applied, and the collector's per-database read of an absent session returned zero rows and recorded
/// SUCCESS. Both rows then reached the health tool as HEALTHY, <c>errors 0</c>, and an output finding
/// asserting the opposite of what happened.</para>
/// </summary>
public sealed class SwallowedItemFailureTests
{
    /* ── the runner: the driver's account becomes the run record ── */

    /// <summary>
    /// The enumerated path composes the #2623 partial note from the driver's account and rethrows the
    /// FIRST failure - stamped with the database it came from (#2997), because the fault handler upstream
    /// otherwise names the runtime's connected database, which for an enumerated collector is never the
    /// one that failed.
    /// </summary>
    [Fact]
    public void TheRunner_StampsTheDatabase_OnTheAllFailedRethrow()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        Assert.Contains(
            "collectionNote = EnumeratedCollectorDriver.MergeNotes(collectionNote, driverResult.PartialFailureNote);",
            source,
            StringComparison.Ordinal);
        Assert.Contains("CollectorFaultDatabase.Stamp(driverResult.FirstError, driverResult.FailedItems?[0]);", source, StringComparison.Ordinal);

        /* The stamp precedes the throw, inside the all-failed arm. */
        var arm = source.IndexOf("if (driverResult.AllItemsFailed)", StringComparison.Ordinal);
        var stamp = source.IndexOf("CollectorFaultDatabase.Stamp(driverResult.FirstError", StringComparison.Ordinal);
        var rethrow = source.IndexOf("ExceptionDispatchInfo.Capture(driverResult.FirstError!).Throw();", StringComparison.Ordinal);
        Assert.True(arm > 0 && stamp > arm && rethrow > stamp, "stamp, then throw, inside the all-failed arm");
    }

    /* ── the reconcile: a refused session reaches the run ── */

    /// <summary>
    /// The Azure arm of the long-query reconcile keeps the #2623 account over the databases it tried to
    /// ENABLE in, throws the first refusal when every database refused (so the worker does not latch and
    /// the run classifies SESSION_MISSING), and returns the partial note when some did. The DROP arm is
    /// deliberately not scored: no run is dispatched while disabled.
    /// </summary>
    [Fact]
    public void TheAzureReconcile_ThrowsWhenEveryDatabaseRefused_AndReturnsThePartialNoteWhenSomeDid()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");

        Assert.Contains("public static async Task<string?> ReconcileLongQueryCompletionsAsync(", source, StringComparison.Ordinal);
        Assert.Contains("private static async Task<string?> ReconcileLongQueryCompletionsAzureAsync(", source, StringComparison.Ordinal);

        /* Scored only while enabling. */
        Assert.Contains("if (enabled)\n            {\n                attempted++;\n            }", source, StringComparison.Ordinal);
        Assert.Contains("if (enabled)\n                {\n                    failed++;\n                    failedDatabases.Add(databaseName);\n                    CollectorFaultDatabase.Stamp(ex, databaseName);\n                    firstFailure ??= ex;\n                }", source, StringComparison.Ordinal);

        /* All refused: the first failure, raw, after the summary line. Same predicate as the runners'. */
        Assert.Contains("if (attempted > 0 && failed == attempted && firstFailure is not null)", source, StringComparison.Ordinal);
        Assert.Contains("System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstFailure).Throw();", source, StringComparison.Ordinal);

        /* Some refused: the shared composer, so the wording is the runners'. */
        Assert.Contains(
            "return EnumeratedCollectorDriver.BuildPartialFailureNote(failed, attempted, failedDatabases, firstFailure?.Message);",
            source,
            StringComparison.Ordinal);

        /* The per-database warning that names the refusing database is KEPT - the account rides beside
           it, it does not replace it. */
        Assert.Contains("Failed to reconcile the long-query completion XE session", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The worker carries what the reconcile could not do to the run: a throw while enabling fills
    /// <c>LongQueryTraceFault</c> (and leaves the latch unset, so the next sweep retries - the existing
    /// behaviour); a partial note fills <c>LongQueryTracePartialNote</c>; a clean reconcile clears both;
    /// a (re)connect resets both with the latch. Then <c>RunOneAsync</c> reads them for this collector
    /// alone: a fault becomes SESSION_MISSING before any read is dispatched, a partial note is merged
    /// onto the result's HostNote after the read.
    /// </summary>
    [Fact]
    public void TheWorker_ClassifiesARefusedSessionAsSessionMissing_BeforeDispatchingTheRead()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        /* The two slots on the loop state. */
        Assert.Contains("public string? LongQueryTraceFault { get; set; }", source, StringComparison.Ordinal);
        Assert.Contains("public string? LongQueryTracePartialNote { get; set; }", source, StringComparison.Ordinal);

        /* The reconcile fills them: partial note on success (enabled only), fault on a throw (enabled only). */
        Assert.Contains("var partialNote = await DarlingXeSessions.ReconcileLongQueryCompletionsAsync(", source, StringComparison.Ordinal);
        Assert.Contains("server.LongQueryTracePartialNote = enabled ? partialNote : null;", source, StringComparison.Ordinal);
        var reconcileCatch = source.IndexOf("Failed to reconcile the long-query completion XE session: {Message}", StringComparison.Ordinal);
        var faultSet = source.IndexOf("server.LongQueryTraceFault = refusedIn is null", StringComparison.Ordinal);
        Assert.True(reconcileCatch > 0 && faultSet > reconcileCatch, "the fault is recorded in the reconcile's catch");
        Assert.Contains("var refusedIn = CollectorFaultDatabase.For(ex, fallback: null);", source, StringComparison.Ordinal);

        /* Reset with the latch on every (re)connect. */
        var latchReset = source.IndexOf("server.LongQueryTraceApplied = null;", StringComparison.Ordinal);
        Assert.True(latchReset > 0);
        var slice = source.Substring(latchReset, 900);
        Assert.Contains("server.LongQueryTraceFault = null;", slice, StringComparison.Ordinal);
        Assert.Contains("server.LongQueryTracePartialNote = null;", slice, StringComparison.Ordinal);

        /* RunOneAsync: the fault throws the SESSION_MISSING type BEFORE the dispatch; the partial note is
           merged onto HostNote AFTER it. Both gated on the collector's own declared name. */
        var runOne = source.IndexOf("private async Task<int> RunOneAsync(", StringComparison.Ordinal);
        var faultThrow = source.IndexOf(
            "if (IsLongQueryCompletionsCollector(collectorName) && server.LongQueryTraceFault is { } traceFault)\n            {\n                throw new DarlingXeSessionMissingException(traceFault);\n            }",
            StringComparison.Ordinal);
        var dispatch = source.IndexOf("var result = await run(runner, runtime, cancellationToken);", StringComparison.Ordinal);
        var noteMerge = source.IndexOf(
            "result = result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, partialNote) };",
            StringComparison.Ordinal);
        Assert.True(runOne > 0 && faultThrow > runOne && dispatch > faultThrow && noteMerge > dispatch,
            "fault check, then dispatch, then partial-note merge, all inside RunOneAsync");

        /* The message-only constructor the pre-dispatch throw needs, beside the wrapping one. */
        Assert.Contains("public DarlingXeSessionMissingException(string message) : base(message) { }", source, StringComparison.Ordinal);

        /* Name-guarded on the collector's OWN name, not a literal. */
        Assert.Contains(
            "internal static bool IsLongQueryCompletionsCollector(string collectorName) =>\n        string.Equals(collectorName, LongQueryCompletionsCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);",
            source,
            StringComparison.Ordinal);
    }

    /* ── the health tool's row, on the issue's exact shape ── */

    private static CollectorHealth Row(
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
    /// <c>database_scoped_config</c>, one run, every item refused, recorded ERROR by the runner: the row must
    /// not be HEALTHY, must not carry <c>errors 0</c>, and its finding must say the run could not read -
    /// none of the four false clauses the issue quoted.
    /// </summary>
    [Fact]
    public void TheIssuesConfigCollector_OnceRecordedError_IsNotHealthy_AndTheFindingSaysUnableToRead()
    {
        var row = Row("database_scoped_config", totalRuns: 1, successes: 0, errors: 1);

        Assert.NotEqual(CollectorHealthClassifier.Healthy, row.HealthStatus);
        Assert.Equal(1, row.ErrorCount);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("UNABLE to read", finding, StringComparison.Ordinal);
        Assert.Contains("Every run in the window faulted", finding, StringComparison.Ordinal);

        Assert.DoesNotContain("read and found nothing", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("event occurs", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("No run recorded a note", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>long_query_completions</c>, one run, its session refused everywhere, recorded SESSION_MISSING by
    /// the worker: FAILING on the never-succeeded clock (as SESSION_MISSING always banded), and NOW the
    /// finding reads the status - before #3754 the same row said <c>errors 0</c> and offered the
    /// resting-state sentence beside a FAILING band.
    /// </summary>
    [Fact]
    public void TheIssuesXeCollector_OnceRecordedSessionMissing_IsFailing_AndTheFindingCountsIt()
    {
        var row = Row("long_query_completions", totalRuns: 1, successes: 0, sessionMissing: 1);

        Assert.Equal(CollectorHealthClassifier.Failing, row.HealthStatus);
        Assert.Equal(0, row.ErrorCount);
        Assert.Equal(1, row.SessionMissingCount);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("1 of those runs recorded a fault", finding, StringComparison.Ordinal);
        Assert.Contains("SESSION_MISSING", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("read and found nothing", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same collector with its session PRESENT and quiet - the positive control: an event collector's
    /// clean zero keeps the resting-state reading it always had.
    /// </summary>
    [Fact]
    public void TheIssuesXeCollector_WithItsSessionPresentAndQuiet_KeepsTheRestingStateReading()
    {
        var row = Row("long_query_completions", totalRuns: 60, successes: 60);

        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("read and found nothing rather than being unable to read", finding, StringComparison.Ordinal);
        Assert.Contains("correct resting state", finding, StringComparison.Ordinal);
        Assert.Contains("needs no action", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// The partial shape the Azure reconcile now produces - the session created in one database, refused
    /// in another - reaches the row as a SUCCESS with the #2623 note, and the finding defers to it.
    /// </summary>
    [Fact]
    public void APartiallyRefusedSession_ReachesTheRow_AsASuccessWithTheNote()
    {
        var row = Row("long_query_completions", totalRuns: 4, successes: 4, noteCount: 4);

        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);

        var finding = row.OutputFinding;
        Assert.NotNull(finding);
        Assert.Contains("last_note", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("correct resting state", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", finding, StringComparison.Ordinal);

        /* And the note itself, as the tool renders it, names the refused database. */
        var rendered = CollectorHealthClassifier.FormatCollectionNote(row.LastNote, row.NoteCount, row.TotalRuns, row.CollectorName, targetHasUserDatabases: true);
        Assert.Contains("xedb1", rendered, StringComparison.Ordinal);
        Assert.Contains("all 4 runs", rendered, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Darling health read selects the count the finding now reads, appended after #3240's column
    /// because both MCP readers read this result set positionally (the ordinal twin pin lives in
    /// <c>CollectionOutputBesideCostTests</c>).
    /// </summary>
    [Fact]
    public void TheDarlingHealthRead_SelectsTheSessionMissingCount_Appended()
    {
        var sql = DarlingDataReader.CollectionHealthSql;

        Assert.Contains("SUM(CASE WHEN status = 'SESSION_MISSING' THEN 1 ELSE 0 END) AS session_missing_count", sql, StringComparison.Ordinal);
        Assert.True(
            sql.IndexOf("AS session_missing_count", StringComparison.Ordinal) > sql.IndexOf("AS extension_missing_count", StringComparison.Ordinal),
            "session_missing_count must be appended after extension_missing_count");

        /* Not fed to the band: Classify's signature is pinned elsewhere; here, the count must not have been
           folded into error_count, whose definition is unchanged. */
        Assert.Contains("SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The event-collector set, against the catalog, from this SKU's suite too: every name real, the
    /// described examples present, and no configuration or enumeration collector on it.
    /// </summary>
    [Fact]
    public void TheEventCollectorSet_IsRealAndCorrectlyPolarised()
    {
        var catalog = new HashSet<string>(CollectorCatalog.All.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);

        foreach (var name in CollectorHealthClassifier.EventCollectorNamesForPinning)
        {
            Assert.Contains(name, catalog);
        }

        foreach (var described in new[] { "deadlocks", "blocked_process_report", "pg_blocking", "pg_xmin_horizon", "long_query_completions", "pg_deadlocks" })
        {
            Assert.True(CollectorHealthClassifier.IsEventCollector(described), described);
        }

        foreach (var name in catalog)
        {
            if (CollectorHealthClassifier.IsOnLoadCollector(name) || CollectorHealthClassifier.ExpectsUserDatabases(name))
            {
                Assert.False(CollectorHealthClassifier.IsEventCollector(name), name);
            }
        }
    }
}
