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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2546, the vocabulary half: what makes an absence a runtime PRECONDITION rather than one of the three
/// kinds of nothing that already existed.
///
/// <para>The assertions are about the DISTINCTIONS the vocabulary has to draw, not about today's wording. A
/// test that pinned the sentences would fail on every improvement to them and would say nothing about the
/// only property that matters: that a state somebody can fix is never reported as one they cannot, and the
/// reverse.</para>
/// </summary>
public sealed class CollectorRuntimePreconditionTests
{
    private const string Server = "precondition-probe";

    /// <summary>
    /// The state the vocabulary exists for. A missing capture session yields zero rows for as long as it is
    /// missing, which is byte-identical to a server that simply did not deadlock — so the message has to say
    /// which of those two it is, and it has to say it about the collector the read is actually served by.
    /// </summary>
    [Fact]
    public void ASessionMissingRun_IsReportedAsAPrecondition_NamingTheCollectorAndTheCapture()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "deadlocks", CollectorRuntimePrecondition.CaptureSessionMissingStatus,
            "The specified object 'deadlock_capture' could not be found.", new DateTime(2026, 8, 20, 14, 0, 0, DateTimeKind.Utc));

        Assert.NotNull(message);
        Assert.Contains(Server, message, StringComparison.Ordinal);
        Assert.Contains("deadlocks", message, StringComparison.Ordinal);
        Assert.Contains("the captured deadlock graphs", message, StringComparison.Ordinal);
        Assert.Contains("deadlock_capture", message, StringComparison.Ordinal);
        Assert.Contains("ALTER ANY EVENT SESSION", message, StringComparison.Ordinal);
        Assert.Contains("2026-08-20 14:00:00Z", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The remedy is the one the SERVER gave, quoted rather than re-authored. The runners already write the
    /// actionable sentence — the SQLSTATE 42P01 case says CREATE EXTENSION in so many words — and a second
    /// copy of that prose here is the drift this repo has been bitten by before.
    /// </summary>
    [Fact]
    public void ADegradedRun_QuotesWhatTheServerSaid_RatherThanRewritingIt()
    {
        const string stored =
            "relation \"pg_stat_statements\" does not exist (SQLSTATE 42P01) — the source object does not "
            + "exist on this target. This is NOT a missing grant: it is normally an extension that was never "
            + "created in the connected database (CREATE EXTENSION pg_stat_statements).";

        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "pg_statement_stats", CollectorRuntimePrecondition.DegradedStatus, stored, DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains("CREATE EXTENSION pg_stat_statements", message, StringComparison.Ordinal);
        Assert.Contains("aurora_stat_statements() per-statement history", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3240: an EXTENSION_MISSING run is a precondition too — the same absent-extension state that used to
    /// ride the PERMISSIONS bucket, now under its own status — and losing it from this vocabulary would
    /// regress the MCP miss for exactly the five sources the issue names. The stored sentence (which names
    /// the extension) is quoted, never re-authored, and the standard epilogue holds: these collectors retry
    /// every cycle, so satisfying the precondition needs nothing restarted on the monitoring side.
    /// </summary>
    [Fact]
    public void AnExtensionMissingRun_IsReportedAsAPrecondition_QuotingTheStoredSentence()
    {
        const string stored =
            "relation \"public.pg_buffercache\" does not exist (SQLSTATE 42P01) — the pg_buffercache "
            + "extension this collector reads is not installed on this target.";

        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "pg_buffer_usage", CollectorRuntimePrecondition.ExtensionMissingStatus, stored,
            new DateTime(2026, 9, 8, 9, 30, 0, DateTimeKind.Utc));

        Assert.NotNull(message);
        Assert.Contains(Server, message, StringComparison.Ordinal);
        Assert.Contains("pg_buffer_usage", message, StringComparison.Ordinal);
        Assert.Contains("pg_buffercache", message, StringComparison.Ordinal);
        Assert.Contains("extension", message, StringComparison.Ordinal);
        Assert.Contains("no grant", message, StringComparison.Ordinal);
        Assert.Contains("2026-09-08 09:30:00Z", message, StringComparison.Ordinal);

        /* The re-derived epilogue, not the connect-scoped one: CREATE EXTENSION needs no monitoring-side
           restart to be picked up. */
        Assert.Contains("re-derives it on EVERY call", message, StringComparison.Ordinal);
        Assert.DoesNotContain("read once when the service connects", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every OTHER status answers null and means it. This is the non-greediness that keeps the new word from
    /// swallowing the three that already work: SUCCESS is the read's <c>empty</c>, ERROR is collection
    /// health's business, and YIELDED is the lock-timeout guard doing its job on a server that is contended
    /// right now — a transient skip, not a state anybody sets or clears.
    /// </summary>
    [Theory]
    [InlineData("SUCCESS")]
    [InlineData("ERROR")]
    [InlineData("YIELDED")]
    [InlineData("CANCELLED")]
    [InlineData("")]
    [InlineData(null)]
    public void AnyOtherStatus_IsNotAPrecondition(string? status)
    {
        Assert.Null(CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "deadlocks", status, "something happened", DateTime.UtcNow));
    }

    /// <summary>
    /// The epilogue is what tells the reader this one is worth acting on, and it must say the thing an
    /// <c>AppliesTo</c> gate could not: the answer is re-derived per read, so doing what it asks is enough.
    /// Pinned on both message shapes because a divergence there is a divergence about whether to bother.
    /// </summary>
    [Fact]
    public void BothMessageShapes_SayTheAnswerIsReDerivedOnEveryRead()
    {
        var outcome = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "running_jobs", CollectorRuntimePrecondition.DegradedStatus, "denied", DateTime.UtcNow);
        var queryStore = CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server, new[] { new CollectorRuntimePrecondition.QueryStoreDatabaseState("AppDb", "OFF") }, DateTime.UtcNow);

        foreach (var message in new[] { outcome, queryStore })
        {
            Assert.NotNull(message);
            Assert.Contains("re-derives it on EVERY call", message, StringComparison.Ordinal);
            Assert.Contains("not a permanent engine capability gap", message, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Query Store OFF is a setup step with one statement behind it, and the message has to carry that
    /// statement — "Query Store may not be enabled", which is what both SKUs said before this, is equally
    /// true of a server where it IS enabled and the window simply reached past retention.
    /// </summary>
    [Fact]
    public void QueryStoreOff_NamesTheDatabasesAndTheAlterThatFixesIt()
    {
        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server,
            new[]
            {
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("AppDb", "OFF"),
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("ReportsDb", "OFF"),
            },
            new DateTime(2026, 8, 20, 9, 30, 0, DateTimeKind.Utc));

        Assert.NotNull(message);
        Assert.Contains("AppDb", message, StringComparison.Ordinal);
        Assert.Contains("ReportsDb", message, StringComparison.Ordinal);
        Assert.Contains("SET QUERY_STORE = ON", message, StringComparison.Ordinal);
        Assert.Contains("2026-08-20 09:30:00Z", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// READ_ONLY is a DIFFERENT precondition from OFF and must not be described as one: the database was
    /// configured, Query Store answers questions about what it already holds, and it silently stopped
    /// recording — usually because the storage cap was reached. Telling that operator to run
    /// SET QUERY_STORE = ON sends them to re-issue an ALTER that is already in effect.
    /// </summary>
    [Fact]
    public void QueryStoreReadOnly_IsDescribedAsTheCapHit_NotAsOff()
    {
        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server,
            new[]
            {
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("AppDb", "READ_ONLY"),
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("ReportsDb", "OFF"),
            },
            DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains("READ_ONLY", message, StringComparison.Ordinal);
        Assert.Contains("readonly_reason", message, StringComparison.Ordinal);
        Assert.Contains("OPERATION_MODE = READ_WRITE", message, StringComparison.Ordinal);
        Assert.DoesNotContain("SET QUERY_STORE = ON", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// ERROR is the THIRD not-collecting state, and the one the OFF wording misleads worst about (review
    /// catch). It is a database whose Query Store was configured and then broke: desired_state is typically
    /// still READ_WRITE, so "turn it on with SET QUERY_STORE = ON" is an instruction to re-issue an ALTER
    /// that is already in effect and will silently no-op, while the actual repair — the consistency check,
    /// then CLEAR — goes unmentioned.
    /// </summary>
    [Fact]
    public void QueryStoreInErrorState_IsDescribedAsBroken_NotAsOff()
    {
        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server,
            new[]
            {
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("BrokenDb", "ERROR"),
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("OffDb", "OFF"),
            },
            DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains("BrokenDb", message, StringComparison.Ordinal);
        Assert.Contains("ERROR is not OFF", message, StringComparison.Ordinal);
        Assert.Contains("sys.sp_query_store_consistency_check", message, StringComparison.Ordinal);
        Assert.Contains("SET QUERY_STORE CLEAR", message, StringComparison.Ordinal);

        /* The specific wrong instruction this branch exists to avoid. */
        Assert.DoesNotContain("Turn it on with ALTER", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// ERROR outranks READ_ONLY when both are present, because it is the state nobody would guess from
    /// either of the other two sentences. Pinned so the ordering cannot be reshuffled by accident.
    /// </summary>
    [Fact]
    public void QueryStoreErrorOutranksReadOnly()
    {
        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server,
            new[]
            {
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("ReadOnlyDb", "READ_ONLY"),
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("BrokenDb", "ERROR"),
            },
            DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains("ERROR is not OFF", message, StringComparison.Ordinal);
        Assert.DoesNotContain("readonly_reason", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two directions this must NOT fire in. A snapshot with any READ_WRITE database in scope means
    /// Query Store is collecting somewhere the read could have looked, so the emptiness has another cause;
    /// and an EMPTY snapshot is no evidence at all, which is not the same as evidence of absence — the
    /// health collector runs hourly and a server that predates Query Store has nothing to record.
    /// </summary>
    [Fact]
    public void QueryStoreCollectingOrUnknown_MakesNoPreconditionClaim()
    {
        Assert.Null(CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server,
            new[]
            {
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("AppDb", "READ_WRITE"),
                new CollectorRuntimePrecondition.QueryStoreDatabaseState("ReportsDb", "OFF"),
            },
            DateTime.UtcNow));

        Assert.Null(CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server, Array.Empty<CollectorRuntimePrecondition.QueryStoreDatabaseState>(), DateTime.UtcNow));

        /* An actual_state this repo has never met counts as collecting: a value we cannot interpret must not
           manufacture a precondition claim about a server that is working. */
        Assert.Null(CollectorRuntimePrecondition.QueryStoreDisabledMessage(
            Server,
            new[] { new CollectorRuntimePrecondition.QueryStoreDatabaseState("AppDb", "SOME_FUTURE_STATE") },
            DateTime.UtcNow));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    /* ---- #2559: the gated-off collector, which records nothing to read ---- */

    /// <summary>
    /// The candidate text the shipped tool passes, so these assertions exercise what <c>get_running_jobs</c>
    /// actually says rather than a string invented here. Since the dispatch half of #2559 removed
    /// HasMsdbAccess from the gate, AWS RDS is the sole candidate — a login without msdb access now
    /// dispatches and is denied, so it never reaches this arm.
    /// </summary>
    private const string GateCandidates = "this is an AWS RDS instance.";

    /// <summary>
    /// The case the recorded-outcome reader structurally cannot see. A collector whose <c>AppliesTo</c> gate
    /// is off is never dispatched, and the runner returns before writing any <c>collection_log</c> row — so
    /// there is no status to report, the outcome reader answers null, and the read falls through to its
    /// <c>empty</c> miss. For <c>get_running_jobs</c> that miss asserts "No running SQL Agent jobs found"
    /// about a server nobody was permitted to look at, which is the exact claim this vocabulary exists to
    /// stop.
    /// </summary>
    [Fact]
    public void AGatedOffCollector_IsReportedAsAPrecondition_NotAsNothingToReport()
    {
        var message = CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: null, serverLastCollectedUtc: DateTime.UtcNow.AddMinutes(-3));

        Assert.NotNull(message);
        Assert.Contains("never run", message, StringComparison.Ordinal);
        Assert.Contains("never permitted to look", message, StringComparison.Ordinal);
        Assert.Contains("AWS RDS", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The arm has to DISCRIMINATE, which is the only property worth pinning: a guard that answered the same
    /// thing for both populations would be the defect rather than the check on it. Driven twice over one
    /// function — a collector the dispatcher has stopped reaching for, and a collector that ran in the very
    /// sweep that collected the server and stored no rows because the Agent is genuinely idle.
    ///
    /// <para>The idle case must answer NULL specifically, not merely something different: null is what lets
    /// the caller fall through to its own <c>empty</c> miss, which is the honest answer there. A non-null
    /// "different" message would be a gate claim about a working collector.</para>
    /// </summary>
    [Fact]
    public void TheGatedOffArm_TellsADarkCollectorApartFromAGenuinelyIdleOne()
    {
        var serverLastCollected = DateTime.UtcNow.AddMinutes(-2);

        var dark = CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: serverLastCollected.AddDays(-20),
            serverLastCollectedUtc: serverLastCollected);

        var idle = CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: serverLastCollected,
            serverLastCollectedUtc: serverLastCollected);

        Assert.NotNull(dark);
        Assert.Null(idle);
        Assert.NotEqual(idle, dark);
    }

    /// <summary>
    /// The inference is RECENCY, and this is the population that proves it has to be. A store written before
    /// #2580 holds one zero-duration, zero-row <c>SUCCESS</c> per cycle for exactly this collector — the
    /// pre-dispatch skip recording itself as a success — and <c>collection_log</c> keeps
    /// <c>DarlingRetention.CollectionLogRetentionDays</c> of rows. A PRESENCE probe reads those as runs and
    /// answers "this collector has run", silencing the declaration for the whole retention window on
    /// precisely the fleet the declaration was built for. Measured live: rows 20 days old suppressing the
    /// answer on a server whose newest collection was two minutes old.
    ///
    /// <para>A genuine gate FLIP reaches the same state by a different route and does not age out of it, so
    /// this is not a one-off.</para>
    /// </summary>
    [Fact]
    public void RowsThatRecordASkipRatherThanARun_DoNotSilenceTheDeclaration()
    {
        var serverLastCollected = DateTime.UtcNow.AddMinutes(-2);

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: serverLastCollected.AddDays(-20),
            serverLastCollectedUtc: serverLastCollected);

        Assert.NotNull(message);
        Assert.Contains("no longer being invoked", message, StringComparison.Ordinal);
        Assert.Contains("AWS RDS", message, StringComparison.Ordinal);

        /* Those rows are real and the reader can query them, so denying them would be a falsifiable claim
           in operator-facing text. The message reports the instant instead. */
        Assert.DoesNotContain("never run", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A collector still being dispatched is at most one cadence behind the sweep that collected the server,
    /// so an ordinary gap must stand aside and let the recorded-outcome reader or the read's own miss answer.
    /// Pinned at the cutoff from both sides, because a comparison with the wrong sense passes one of them.
    /// </summary>
    [Fact]
    public void ACollectorStillBeingDispatched_IsNeverCalledGatedOff()
    {
        var serverLastCollected = DateTime.UtcNow.AddMinutes(-3);

        Assert.Null(CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: serverLastCollected.AddHours(-(CollectorRuntimePrecondition.GoneDarkHours - 1)),
            serverLastCollectedUtc: serverLastCollected));

        Assert.NotNull(CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: serverLastCollected.AddHours(-(CollectorRuntimePrecondition.GoneDarkHours + 1)),
            serverLastCollectedUtc: serverLastCollected));
    }

    /// <summary>
    /// The gap is measured between two STORED instants, never against the wall clock. Both halves age
    /// together when the service stops visiting a server, so a wall-clock comparison would call every
    /// collector on it gated off at once — a whole server's worth of confident structural claims produced by
    /// an outage. Measured against the server's own last collection the gap stays small, which is the
    /// <c>unavailable</c> answer collection health owns.
    /// </summary>
    [Fact]
    public void AServerTheServiceStoppedVisiting_IsAnOutage_NotAFleetOfGates()
    {
        var stopped = DateTime.UtcNow.AddDays(-3);

        Assert.Null(CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: stopped, serverLastCollectedUtc: stopped.AddMinutes(1)));
    }

    /// <summary>
    /// The other half, and the one that would turn an outage into a lie. A server that has collected NOTHING
    /// has no working collectors to contrast against, so "this collector never ran" says nothing about a
    /// gate — that is <c>unavailable</c>, and collection health's business.
    /// </summary>
    [Fact]
    public void AServerCollectingNothingAtAll_IsAnOutage_NotAGate()
        => Assert.Null(CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: null, serverLastCollectedUtc: null));

    /// <summary>
    /// One cutoff, two surfaces. <c>STOPPED</c> bands a collector that has attempted nothing for longer than
    /// the FAILING cutoff and names this exact RDS case in its own comment; this arm says the same thing in a
    /// sentence. Held as a separate constant because <c>PerformanceMonitor.Collectors</c> references nothing,
    /// so this equality is what stops the copy drifting — without it the two surfaces could disagree about
    /// every server sitting between their cutoffs, with nothing to tell the reader which to believe.
    /// </summary>
    [Fact]
    public void TheGoneDarkCutoff_IsTheOneTheStoppedBandAlreadyUses()
        => Assert.Equal(
            CollectorRuntimePrecondition.GoneDarkHours,
            CollectorHealthClassifier.FailingThresholdHours(
                CollectorScheduleDefaults.All["running_jobs"].FrequencyMinutes));

    /// <summary>
    /// A gated-off collector cannot re-derive its own precondition, so BOTH its messages must carry the
    /// connect-scoped epilogue rather than the general promise that nothing needs restarting. The deciding
    /// fact is read once at connect and cached for the connection's life — that is the whole of #2559 — and a
    /// message telling the operator to grant and retry sends them round a loop that never terminates. Pinned
    /// over both wordings, because an arm added without the epilogue reintroduces the contradiction in the
    /// half nobody re-read.
    /// </summary>
    [Fact]
    public void BothGatedOffMessages_SayAReconnectIsNeeded_NotThatNothingNeedsRestarting()
    {
        var serverLastCollected = DateTime.UtcNow.AddMinutes(-3);

        var neverRan = CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: null, serverLastCollectedUtc: serverLastCollected)!;

        var goneDark = CollectorRuntimePrecondition.GatedOffMessage(
            Server, "running_jobs", GateCandidates,
            collectorLastRunUtc: serverLastCollected.AddDays(-20),
            serverLastCollectedUtc: serverLastCollected)!;

        foreach (var message in new[] { neverRan, goneDark })
        {
            Assert.Contains("reconnect", message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("nothing to restart", message, StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// The contradiction this change also fixes, which predates #2559 and shipped for two issues. The
    /// SESSION_MISSING arm has always said in its own sentence that a dropped session "stays missing until
    /// the next connect", and then appended a shared epilogue promising "nothing to restart on the monitoring
    /// side". Both sentences were in the same operator-facing string.
    /// </summary>
    [Fact]
    public void TheCaptureSessionMessage_NoLongerContradictsItself()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "deadlocks", CollectorRuntimePrecondition.CaptureSessionMissingStatus,
            "The session was not found.", DateTime.UtcNow.AddMinutes(-3))!;

        Assert.Contains("next connect", message, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing to restart", message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// And the arm that CAN honour the promise keeps it. A PERMISSIONS skip is recorded by a collector that
    /// runs every cycle, so the denial really is re-derived and really does clear itself the cycle after the
    /// grant lands — losing that would make the common case worse to fix the rare one.
    /// </summary>
    [Fact]
    public void ThePermissionsMessage_StillPromisesNoRestart()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "running_jobs", CollectorRuntimePrecondition.DegradedStatus,
            "SELECT permission was denied.", DateTime.UtcNow.AddMinutes(-3))!;

        Assert.Contains("nothing to restart", message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// WIRING, parsed from the shipped source: the gated-off check must sit AFTER the recorded-outcome check
    /// and BEFORE the empty miss. Order is the whole correctness argument — recorded evidence from the
    /// monitored server beats an inference from an absence, and both beat asserting the Agent is idle.
    /// </summary>
    [Fact]
    public void GetRunningJobs_AsksAboutTheGate_AfterTheRecordedOutcome_AndBeforeTheEmptyMiss()
    {
        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpJobTools.cs"));

        /* Anchor on CODE, never on the sentence. The first draft of this pin looked for the string
           "No running SQL Agent jobs found" and matched the #2546 comment that QUOTES it, eleven lines above
           the call — so the pin failed against source whose order was correct. A source-parsed assertion has
           to key on something that cannot appear in prose about itself. */
        var recorded = source.IndexOf("DarlingRuntimePrecondition.StatusAsync", StringComparison.Ordinal);
        var gate = source.IndexOf("DarlingRuntimePrecondition.GatedOffStatusAsync", StringComparison.Ordinal);
        var empty = source.IndexOf("McpHelpers.Status(\"empty\"", StringComparison.Ordinal);

        Assert.True(recorded > 0 && gate > 0 && empty > 0, "get_running_jobs no longer has all three arms");
        Assert.True(gate > recorded, "the gate inference must not pre-empt the collector's own recorded denial");
        Assert.True(empty > gate, "the empty miss must remain the LAST resort");
    }

    /// <summary>
    /// The status word is not one of the three that already exist. Stated as an assertion because the whole
    /// design rests on it: reusing <c>unavailable</c> would send the reader to collection health, where they
    /// would find a collector that is running and doing its best, and reusing <c>not_collected</c> would tell
    /// them to stop looking at something they can fix.
    /// </summary>
    [Fact]
    public void ThePreconditionWord_IsDistinctFromTheThreeItSitsBeside()
    {
        Assert.Equal("precondition", CollectorRuntimePrecondition.StatusWord);
        Assert.DoesNotContain(
            CollectorRuntimePrecondition.StatusWord,
            new[] { "empty", "unavailable", "not_collected" },
            StringComparer.Ordinal);
    }

    /// <summary>
    /// The noun phrase comes from the capability vocabulary rather than a second table. Demonstrated on a
    /// collector both axes describe, so a divergence would be visible here rather than only in review.
    /// </summary>
    [Fact]
    public void TheCapturePathPhrase_IsBorrowedFromTheCapabilityVocabulary_NotRestated()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "running_jobs", CollectorRuntimePrecondition.DegradedStatus, "denied", DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains(
            CollectorEngineCapability.CapturePathByCollector["running_jobs"],
            message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A collector with no noun phrase still gets a correct sentence — the same fallback the capability
    /// message uses. Prose that falls out of date makes a message vaguer here, never wrong.
    /// </summary>
    [Fact]
    public void ACollectorWithNoNounPhrase_StillGetsACorrectSentence()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            Server, "a_collector_with_no_prose_entry", CollectorRuntimePrecondition.DegradedStatus, "denied", null);

        Assert.NotNull(message);
        Assert.Contains("the data this read is served from", message, StringComparison.Ordinal);

        /* No observation time recorded means no parenthetical claiming one. */
        Assert.DoesNotContain("as of", message, StringComparison.Ordinal);
    }
}

/// <summary>
/// #2546, the WIRING half. The failure mode is not a wrong answer, it is a read that never asks — it
/// compiles, every other test passes, and the caller keeps getting the old message. So the reads are scanned
/// as source, in both trees at once, exactly as <see cref="EngineCapabilityReadWiringTests"/> does for the
/// capability question.
///
/// <para>The ORDER is what this exists to hold. Capability is permanent and must win; a precondition message
/// on an engine that can never have the surface would re-introduce the defect #2511 closed, one layer down.
/// Nothing in the type system enforces that, and a <c>??</c> chain is trivially reorderable.</para>
/// </summary>
public sealed class RuntimePreconditionReadWiringTests
{
    private const string DarlingMcp = "Darling/PerformanceMonitor.Darling.Service/Mcp";
    private const string LiteMcp = "Lite/Mcp";

    private static readonly Regex ToolMark = new(@"McpServerTool\(Name = ""([a-z_0-9]+)""", RegexOptions.Compiled);

    /* The collector-name argument of a precondition call: quoted, or a const that names one. */
    private static readonly Regex PreconditionCall = new(
        @"RuntimePrecondition\.StatusAsync\([^)]*?,\s*(?:""([a-z_0-9]+)""|([A-Za-z_][A-Za-z0-9_]*))\)",
        RegexOptions.Compiled);

    private static readonly Regex CapabilityCall = new(@"NotCollectedStatusAsync\(", RegexOptions.Compiled);

    private static readonly Regex QueryStoreCall = new(
        @"RuntimePrecondition\.QueryStoreStatusAsync\(", RegexOptions.Compiled);

    /// <summary>tool name → the collector names its miss path asks the precondition question about.</summary>
    private static SortedDictionary<string, SortedSet<string>> WiredReads(string mcpDirectory)
    {
        var wired = new SortedDictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        foreach (var file in RepoFilesIn(mcpDirectory))
        {
            var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var marks = ToolMark.Matches(source);

            foreach (Match call in PreconditionCall.Matches(source))
            {
                var collector = call.Groups[1].Success ? call.Groups[1].Value : call.Groups[2].Value;
                var owner = marks.Where(m => m.Index < call.Index).LastOrDefault();
                Assert.True(owner is not null, $"{Path.GetFileName(file)}: a precondition call sits outside any MCP tool");

                if (!wired.TryGetValue(owner!.Groups[1].Value, out var collectors))
                {
                    wired[owner.Groups[1].Value] = collectors = new SortedSet<string>(StringComparer.Ordinal);
                }

                collectors.Add(collector);
            }
        }

        return wired;
    }

    /* The collector-name argument of a GATED-OFF call: the first lowercase quoted literal after the call
       opens. The gate-candidate sentence that follows it cannot be mistaken for one - it begins with a
       capital and contains spaces, neither of which this character class admits. */
    private static readonly Regex GatedOffCall = new(
        @"GatedOffStatusAsync\([^;]*?""([a-z_0-9]+)""",
        RegexOptions.Compiled);

    /// <summary>
    /// <c>CollectorRuntimePrecondition.GoneDarkHours</c> is a flat constant, and it equals the
    /// <c>STOPPED</c> cutoff only for collectors whose cadence sits at or below the FAILING floor. A
    /// collector on a daily cadence has a 48-hour STOPPED cutoff, so wiring one to this arm would have the
    /// read declare a gate at 24 hours while collection health still called it FAILING for another day —
    /// two surfaces disagreeing about the same server with nothing to tell a reader which to believe.
    ///
    /// <para>This asserts it of whatever is ACTUALLY wired, parsed from both SKUs' shipped sources, rather
    /// than of the one collector wired today. The gap is unreachable at present; a flat constant is the
    /// right shape while that holds, and this is what stops the day it stops holding from being silent.
    /// The sibling pin in <c>CollectorRuntimePreconditionTests</c> asks a different question — whether the
    /// constant still matches the band vocabulary at all — and neither covers the other.</para>
    /// </summary>
    [Theory]
    [InlineData(DarlingMcp)]
    [InlineData(LiteMcp)]
    public void EveryCollectorWiredToTheGatedOffArm_SharesTheStoppedCutoff(string mcpDirectory)
    {
        var wired = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var file in RepoFilesIn(mcpDirectory))
        {
            var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            foreach (Match call in GatedOffCall.Matches(source))
            {
                wired.Add(call.Groups[1].Value);
            }
        }

        /* A scan that parsed nothing passes for free, which is the one outcome this must not have. */
        Assert.True(
            wired.Count >= 1,
            $"no GatedOffStatusAsync call found under {mcpDirectory} - the scan is broken, or the arm was removed from this SKU");

        foreach (var collector in wired)
        {
            Assert.True(
                CollectorScheduleDefaults.All.TryGetValue(collector, out var schedule),
                $"the gated-off arm is wired for '{collector}', which has no CollectorScheduleDefaults entry");

            Assert.Equal(
                CollectorRuntimePrecondition.GoneDarkHours,
                CollectorHealthClassifier.FailingThresholdHours(schedule!.FrequencyMinutes));
        }
    }

    [Theory]
    [InlineData(DarlingMcp)]
    [InlineData(LiteMcp)]
    public void EveryWiredRead_NamesARealCollector(string mcpDirectory)
    {
        var wired = WiredReads(mcpDirectory);
        var catalog = CollectorCatalog.All.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);

        /* A scan that parsed nothing passes for free — the worst outcome a check like this can have. */
        Assert.True(wired.Count >= 5, $"only {wired.Count} precondition-wired reads found under {mcpDirectory} — the scan is broken, not the surface");

        foreach (var (tool, collectors) in wired)
        {
            foreach (var collector in collectors)
            {
                Assert.True(
                    catalog.Contains(collector),
                    $"{tool} asks the precondition question about '{collector}', which is not a CollectorCatalog name — " +
                    "the collection_log has no rows under that name, so the branch is dead and silently so");
            }
        }
    }

    /// <summary>
    /// The order pin. In every tool that asks both questions, the CAPABILITY call must come first: a
    /// permanent engine gap outranks a fixable precondition, and a <c>??</c> chain is one cut-and-paste away
    /// from telling an Azure SQL Database caller to start a session that engine cannot have.
    /// </summary>
    [Theory]
    [InlineData(DarlingMcp)]
    [InlineData(LiteMcp)]
    public void WhereAReadAsksBoth_CapabilityIsAskedFirst(string mcpDirectory)
    {
        var checkedTools = 0;

        foreach (var file in RepoFilesIn(mcpDirectory))
        {
            var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var marks = ToolMark.Matches(source).ToList();

            for (var i = 0; i < marks.Count; i++)
            {
                var start = marks[i].Index;
                var end = i + 1 < marks.Count ? marks[i + 1].Index : source.Length;
                var body = source[start..end];

                var precondition = PreconditionCall.Match(body);
                var queryStore = QueryStoreCall.Match(body);
                var first = precondition.Success && queryStore.Success
                    ? Math.Min(precondition.Index, queryStore.Index)
                    : precondition.Success ? precondition.Index
                    : queryStore.Success ? queryStore.Index
                    : -1;

                if (first < 0)
                {
                    continue;
                }

                var capability = CapabilityCall.Match(body);
                Assert.True(
                    capability.Success,
                    $"{marks[i].Groups[1].Value} asks the precondition question without asking the capability " +
                    "question at all — a PostgreSQL or Azure caller would be told to fix something their engine " +
                    "cannot have");

                Assert.True(
                    capability.Index < first,
                    $"{marks[i].Groups[1].Value} asks the precondition question BEFORE the capability question. " +
                    "Permanent outranks fixable: reversed, an engine that can never have the surface is told to " +
                    "go and start a session (#2511).");

                checkedTools++;
            }
        }

        Assert.True(checkedTools >= 5, $"only {checkedTools} tools under {mcpDirectory} were order-checked — the scan is broken");
    }

    /// <summary>
    /// The two SKUs wire the SAME reads to the SAME collectors, for the same reason the capability wiring is
    /// held to it: a caller must not get a different story depending on which SKU they are pointed at.
    /// Compared only across tools BOTH surfaces expose, so Darling's PostgreSQL reads are not reported as
    /// drift against a SKU that has no PostgreSQL target seam.
    /// </summary>
    [Fact]
    public void BothSkus_WireTheSameSharedReads()
    {
        var darling = WiredReads(DarlingMcp);
        var lite = WiredReads(LiteMcp);

        var shared = darling.Keys.Intersect(lite.Keys, StringComparer.Ordinal).ToArray();
        Assert.True(shared.Length >= 5, $"only {shared.Length} precondition-wired tools are common to both SKUs");

        foreach (var tool in shared)
        {
            Assert.Equal(darling[tool].ToArray(), lite[tool].ToArray());
        }

        /* And the reads this change was built on are wired on BOTH, named rather than counted, so dropping
           one is a failure that says which. */
        foreach (var tool in new[] { "get_deadlocks", "get_blocked_process_xml", "get_running_jobs", "get_long_query_completions", "get_query_store_top" })
        {
            Assert.True(darling.ContainsKey(tool), $"Darling's {tool} no longer asks the precondition question");
            Assert.True(lite.ContainsKey(tool), $"Lite's {tool} no longer asks the precondition question");
        }
    }

    private static string[] RepoFilesIn(string relativeDirectory, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relativeDirectory);
            if (Directory.Exists(candidate))
            {
                var files = Directory.GetFiles(candidate, "*.cs");
                Assert.NotEmpty(files);
                return files;
            }
        }

        Assert.Fail($"could not find {relativeDirectory} above {thisFile}");
        return Array.Empty<string>();
    }
}

/// <summary>
/// #2546 end to end against a live store: the SAME read, on servers whose stores differ only in what
/// collection recorded, must answer differently — and the difference must MOVE when the recorded state moves,
/// which is the property an <c>AppliesTo</c> gate could not give and the whole reason this is not one.
///
/// <para>Both directions, deliberately. A pin that only asserted the precondition branch would pass just as
/// well if the read had stopped distinguishing anything and started saying <c>precondition</c> to
/// everyone.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class RuntimePreconditionMissLivePostgresTests
{
    private const string DeniedServerName = "darling-precondition-denied";
    private const string HealthyServerName = "darling-precondition-healthy";
    private const string QueryStoreServerName = "darling-precondition-qs";
    private const string GatedServerName = "darling-precondition-gated";
    private const string IdleServerName = "darling-precondition-idle";

    private static readonly int DeniedServerId = ServerIdHelper.GetDeterministicHashCode(DeniedServerName);
    private static readonly int HealthyServerId = ServerIdHelper.GetDeterministicHashCode(HealthyServerName);
    private static readonly int QueryStoreServerId = ServerIdHelper.GetDeterministicHashCode(QueryStoreServerName);
    private static readonly int GatedServerId = ServerIdHelper.GetDeterministicHashCode(GatedServerName);
    private static readonly int IdleServerId = ServerIdHelper.GetDeterministicHashCode(IdleServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Two Enterprise servers, identical empty collector tables, differing only in what <c>collection_log</c>
    /// recorded for the collector each read is served by. That single difference is what has to turn "no
    /// deadlocks found" into "the capture session is missing" and "no running jobs" into "the login was
    /// denied msdb".
    /// </summary>
    [Fact]
    public async Task ARecordedPrecondition_ReplacesTheEmptyMiss_AndAHealthyServerKeepsIt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live precondition test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, ct, DeniedServerId, DeniedServerName);
            await RegisterAsync(connection, ct, HealthyServerId, HealthyServerName);

            await LogAsync(connection, ct, DeniedServerId, DeniedServerName, "deadlocks", "SESSION_MISSING",
                "The specified object 'deadlock_capture' could not be found.");
            await LogAsync(connection, ct, DeniedServerId, DeniedServerName, "running_jobs", "PERMISSIONS",
                "The server principal is not able to access the database \"msdb\" under the current security context.");

            /* The healthy server has run the same two collectors and found nothing, which is the state the
               precondition answer must NOT claim. */
            await LogAsync(connection, ct, HealthyServerId, HealthyServerName, "deadlocks", "SUCCESS", null);
            await LogAsync(connection, ct, HealthyServerId, HealthyServerName, "running_jobs", "SUCCESS", null);

            var deadlocks = await DarlingMcpBlockingTools.GetDeadlocks(postgres, DeniedServerName);
            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(deadlocks));
            Assert.Contains("deadlock_capture", deadlocks, StringComparison.Ordinal);
            Assert.Contains("ALTER ANY EVENT SESSION", deadlocks, StringComparison.Ordinal);

            var jobs = await DarlingMcpJobTools.GetRunningJobs(postgres, DeniedServerName);
            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(jobs));
            Assert.Contains("msdb", jobs, StringComparison.Ordinal);
            Assert.Contains("the SQL Agent running-job snapshot", jobs, StringComparison.Ordinal);

            /* Same reads, same empty tables, healthy log: the old miss, unchanged. */
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpBlockingTools.GetDeadlocks(postgres, HealthyServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpJobTools.GetRunningJobs(postgres, HealthyServerName)));

            /* And the property a gate could not give: the SAME read on the SAME server stops saying it the
               moment collection records a healthy run, with no reconnect and no restart. */
            await LogAsync(connection, ct, DeniedServerId, DeniedServerName, "deadlocks", "SUCCESS", null);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpBlockingTools.GetDeadlocks(postgres, DeniedServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The Query Store half, whose evidence is a collected SNAPSHOT rather than a log status: the hourly
    /// <c>query_store_health</c> collector has recorded actual_state per database all along, and the read was
    /// guessing at it in prose. Scope is the point of the second half — a read narrowed to one database must
    /// get that database's answer, not the server's most flattering one.
    /// </summary>
    [Fact]
    public async Task QueryStoreOff_IsStatedFromTheCollectedSnapshot_RatherThanGuessedAt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live precondition test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, ct, QueryStoreServerId, QueryStoreServerName);

            var captured = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
            await QueryStoreHealthAsync(connection, ct, captured, "OffDb", "OFF");
            await QueryStoreHealthAsync(connection, ct, captured, "OnDb", "READ_WRITE");

            /* Server-wide: one database IS collecting, so the emptiness has some other cause and the read
               keeps its own miss. Claiming a precondition here would be the false-alarm direction. */
            var wide = await DarlingMcpDataTools.GetQueryStoreTop(postgres, QueryStoreServerName);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(wide));

            /* Narrowed to the database that is off: now the evidence is unambiguous and so is the answer. */
            var scoped = await DarlingMcpDataTools.GetQueryStoreTop(postgres, QueryStoreServerName, database_name: "OffDb");
            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(scoped));
            Assert.Contains("OffDb", scoped, StringComparison.Ordinal);
            Assert.Contains("SET QUERY_STORE = ON", scoped, StringComparison.Ordinal);

            /* And the database that is on keeps the old miss, so the branch is not a blanket rule. */
            Assert.Equal(
                "unavailable",
                DarlingMcpTestData.StatusOf(await DarlingMcpDataTools.GetQueryStoreTop(postgres, QueryStoreServerName, database_name: "OnDb")));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The store half of #3423, driven through the real <c>collection_log</c> query rather than the decision
    /// function alone — the populations differ only in WHEN <c>running_jobs</c> last appeared, which is a fact
    /// the SQL has to fetch before anything can reason about it.
    ///
    /// <para><b>The gated server is seeded as the fleet actually holds it:</b> <c>running_jobs</c> rows that
    /// are gate SKIPS recorded as zero-duration, zero-row <c>SUCCESS</c>, sitting inside
    /// <c>collection_log</c> retention, beside a sibling Agent collector that is running right now. A
    /// presence probe answers "it has run" from those rows and the read goes back to asserting the Agent is
    /// idle on a server it was never permitted to look at.</para>
    ///
    /// <para><b>Both directions, and they must differ.</b> The idle server is the same read over the same
    /// empty snapshot table, distinguished only by <c>running_jobs</c> having run in the sweep that collected
    /// the server. A read answering <c>precondition</c> to both would be exactly as wrong as one answering
    /// <c>empty</c> to both, and only a paired assertion can fail on either.</para>
    /// </summary>
    [Fact]
    public async Task AGateSkipRecordedAsASuccess_DoesNotMakeTheReadCallTheAgentIdle()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live precondition test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, ct, GatedServerId, GatedServerName);
            await RegisterAsync(connection, ct, IdleServerId, IdleServerName);

            var now = DateTime.UtcNow;
            var thisSweep = now.AddMinutes(-2);

            await LogAtAsync(connection, ct, GatedServerId, GatedServerName, "running_jobs", "SUCCESS", null, now.AddDays(-20));
            await LogAtAsync(connection, ct, GatedServerId, GatedServerName, "job_history", "SUCCESS", null, thisSweep);

            await LogAtAsync(connection, ct, IdleServerId, IdleServerName, "running_jobs", "SUCCESS", null, thisSweep);
            await LogAtAsync(connection, ct, IdleServerId, IdleServerName, "job_history", "SUCCESS", null, thisSweep);

            var gated = await DarlingMcpJobTools.GetRunningJobs(postgres, GatedServerName);
            var idle = await DarlingMcpJobTools.GetRunningJobs(postgres, IdleServerName);

            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(gated));
            Assert.Contains("AWS RDS", gated, StringComparison.Ordinal);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(idle));
            Assert.NotEqual(DarlingMcpTestData.StatusOf(idle), DarlingMcpTestData.StatusOf(gated));

            /* The property a gate decided at connect time could not give: the moment the collector is
               dispatched again, this read stops declaring the gate, with nothing restarted on either side. */
            await LogAtAsync(connection, ct, GatedServerId, GatedServerName, "running_jobs", "SUCCESS", null, now);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(
                await DarlingMcpJobTools.GetRunningJobs(postgres, GatedServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 3, 15, 'sqlserver', $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_engine_edition = 3, engine_kind = 'sqlserver';", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task LogAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName,
        string collectorName, string status, string? errorMessage) =>
        LogAtAsync(connection, ct, serverId, serverName, collectorName, status, errorMessage, DateTime.UtcNow);

    /// <summary>
    /// One <c>collection_log</c> row at a CHOSEN instant. The gated-off inference is a comparison between two
    /// stored timestamps, so a seeder that always writes "now" can only ever produce one of the populations
    /// it has to tell apart.
    /// </summary>
    private static Task LogAtAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName,
        string collectorName, string status, string? errorMessage, DateTime collectionTimeUtc) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1,$2,$3,$4,$5,0,$6,$7,0,0,0)",
            CollectionIdGenerator.Next(), serverId, serverName, collectorName,
            DarlingMcpTestData.Naive(collectionTimeUtc), status, errorMessage);

    private static Task QueryStoreHealthAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime captureTime, string databaseName, string actualState) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), captureTime, QueryStoreServerId, QueryStoreServerName, databaseName,
            actualState, "READ_WRITE", 0, 0L, 1000L, "AUTO", 30L, 200L, 60L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = $"{DeniedServerId}, {HealthyServerId}, {QueryStoreServerId}, {GatedServerId}, {IdleServerId}";
        foreach (var table in new[] { "collection_log", "query_store_health", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
