/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The THIRD kind of nothing (#2546): a read whose surface is absent because a RUNTIME PRECONDITION on the
/// monitored server is unsatisfied — a capture session that is not running, a source object or extension that
/// was never created, a grant the monitoring login was refused, Query Store switched off — together with the
/// one sentence both SKUs return for it.
///
/// <para><b>Why this is not a fourth capability axis.</b> Engine EDITION (#2511) and engine KIND (#2536) are
/// properties of the TARGET, fixed for the life of a connection, and both answer "this collector can never
/// run here". That fixedness is what makes the sweep-and-derive machinery behind
/// <see cref="CollectorEngineCapability"/> sound: every fact a gate reads is a scalar and the derivation
/// walks a finite cross-product of them. A precondition is neither. It is MUTABLE — somebody can start the
/// session, issue the GRANT or create the extension while the service is running — and it is ACTIONABLE,
/// where the other two are final. An <c>AppliesTo</c> gate decided at connect time would silently ignore the
/// fix we ourselves recommended, which is the worst possible direction to be wrong in: the operator does the
/// thing the message asked for and nothing changes, with no way to tell why.</para>
///
/// <para><b>So it is answered at READ time, from what collection already recorded.</b> Every miss re-derives
/// it, which is exactly the property a gate cannot give — a precondition satisfied five minutes ago is
/// picked up on the next call rather than on the next reconnect. There is no new probe and no live hit on a
/// monitored server: the evidence is <c>collection_log</c>'s own classified status (the runners already sort
/// a denied grant, a missing source object and a disabled feature out of the general ERROR bucket) and, for
/// Query Store, the <c>query_store_health</c> snapshot the hourly collector already stores.</para>
///
/// <para><b>The remedy text is FRAMED here, not authored here.</b> For a degraded run the runners already
/// write the actionable sentence into <c>collection_log.error_message</c> — the SQLSTATE 42P01 case says
/// <c>CREATE EXTENSION</c> in so many words, the Azure service-objective hint explains an error 300 that is
/// not a missing grant at all. Re-authoring any of that here would be a second copy of prose whose whole
/// value is being accurate about one server's answer, and the copy that drifts is never the one being read.
/// This class supplies the frame and quotes what the server said.</para>
///
/// <para><b>Order at a call site: capability, then precondition, then the read's own miss.</b> A permanent
/// engine gap must win — telling somebody to start a session on an engine that has none is the defect #2511
/// was filed about, and a precondition message would re-introduce it one layer down. And both are asked
/// AFTER the read, never before, for the reason <c>DarlingEngineCapability</c> documents: a server whose
/// recorded state disagrees with its collected rows still gets its DATA rather than a confident explanation
/// of why it cannot have any.</para>
/// </summary>
public static class CollectorRuntimePrecondition
{
    /// <summary>
    /// The miss word. Deliberately NOT one of the three that already exist: <c>empty</c> claims we looked and
    /// there was nothing, which is false here; <c>unavailable</c> sends someone to collection health, where
    /// they will find a collector that is running and doing its best; and <c>not_collected</c> is the
    /// permanent answer, which is the one thing this must not be confused with, because the whole point is
    /// that this one can be fixed.
    /// </summary>
    public const string StatusWord = "precondition";

    /// <summary>
    /// The <c>collection_log</c> status meaning the Extended Events session a collector reads is absent or
    /// stopped on the monitored server, so the tolerant reader came back with nothing to say.
    /// </summary>
    public const string CaptureSessionMissingStatus = "SESSION_MISSING";

    /// <summary>
    /// The <c>collection_log</c> status the runners use for every NON-FATAL degradation — a denied grant, a
    /// source object or extension that does not exist, a feature the platform does not implement or that a
    /// parameter group has switched off. One bucket with three meanings, which is why the message quotes the
    /// stored explanation instead of expanding the status word itself.
    /// </summary>
    public const string DegradedStatus = "PERMISSIONS";

    /// <summary>
    /// The closing sentence every precondition message ends on. One copy, for the same reason
    /// <c>CollectorEngineCapability.PermanentGapEpilogue</c> is one copy: the messages have to agree about
    /// what a precondition IS, and a second wording would eventually disagree about whether it is worth
    /// chasing — which is the only thing the reader is trying to decide.
    /// </summary>
    private const string PreconditionEpilogue =
        "This is a runtime PRECONDITION, not a permanent engine capability gap and not a collection outage: " +
        "it can be satisfied on the monitored server, and this read re-derives it on EVERY call rather than " +
        "deciding it once at connect time — so the first collection cycle after it is satisfied is the one " +
        "this read starts answering from, with nothing to restart on the monitoring side.";

    /// <summary>
    /// The epilogue for a precondition whose collector CANNOT re-derive it every cycle, because the fact that
    /// gates it is read once when the server connects and cached for that connection's life.
    ///
    /// <para>The shared <see cref="PreconditionEpilogue"/> promises "nothing to restart on the monitoring
    /// side", and for these arms that promise is false — a user who satisfies the precondition and retries
    /// gets the identical answer forever. The <c>SESSION_MISSING</c> arm has always said in its own sentence
    /// that a dropped session "stays missing until the next connect" and then appended an epilogue denying
    /// it; the two have contradicted each other in shipped operator-facing text. A blanket claim is the wrong
    /// shape here, so the claim is now made only by the arms that can honour it.</para>
    /// </summary>
    private const string ConnectScopedEpilogue =
        "This is a runtime PRECONDITION, not a permanent engine capability gap and not a collection outage: " +
        "it can be satisfied on the monitored server. But unlike most preconditions this one is NOT " +
        "re-derived every cycle — it is read once when the service connects to this server and cached for " +
        "that connection's life, so satisfying it is not enough on its own: the service has to reconnect " +
        "(restart it, or change this server's configuration, which drops the cached connection) before " +
        "collection resumes. Retrying this read before that will return exactly this answer again.";

    /// <summary>
    /// The precondition explanation for a read whose collector's most recent run recorded one, or
    /// <c>null</c> when it did not — in which case the caller falls through to its own miss vocabulary,
    /// unchanged.
    ///
    /// <para>Only the LATEST run is consulted, deliberately. A precondition is a current state, and a
    /// <c>SESSION_MISSING</c> from three days ago on a collector that has succeeded since describes a
    /// condition somebody already fixed. Reporting it would be the stale-gate defect this whole shape exists
    /// to avoid, arriving by a different route.</para>
    ///
    /// <para>Every other status answers null and means it. <c>SUCCESS</c> is a collector that looked and
    /// found nothing, which is the read's <c>empty</c>. <c>ERROR</c> is a monitoring fault, which is
    /// collection health's business and belongs in <c>unavailable</c>. <c>YIELDED</c> is the lock-timeout
    /// guard working as designed on a server that is contended right now — a transient skip, not a state
    /// anybody sets or clears.</para>
    /// </summary>
    /// <param name="serverName">The server as the caller named it.</param>
    /// <param name="collectorName">The collector serving the read that missed.</param>
    /// <param name="status">The <c>collection_log</c> status of that collector's most recent run.</param>
    /// <param name="errorMessage">That run's stored explanation — the sentence the runner already wrote.</param>
    /// <param name="observedUtc">When that run happened, so the reader can judge how current this is.</param>
    public static string? CollectionOutcomeMessage(
        string serverName,
        string collectorName,
        string? status,
        string? errorMessage,
        DateTime? observedUtc)
    {
        if (string.IsNullOrWhiteSpace(status))
        {
            return null;
        }

        var observed = DescribeObserved(observedUtc);
        var said = DescribeServerAnswer(errorMessage);

        if (string.Equals(status, CaptureSessionMissingStatus, StringComparison.OrdinalIgnoreCase))
        {
            return $"{serverName} does not currently have the Extended Events session the {collectorName} " +
                   $"collector reads: its last run{observed} recorded {CaptureSessionMissingStatus}, so " +
                   $"{CapturePathOf(collectorName)} is not being captured at all.{said} The service creates " +
                   "its capture sessions when it CONNECTS to a server, so a session dropped or stopped " +
                   "afterwards stays missing until the next connect — check that the monitoring login holds " +
                   "ALTER ANY EVENT SESSION, then let the server reconnect. " +
                   ConnectScopedEpilogue;
        }

        if (string.Equals(status, DegradedStatus, StringComparison.OrdinalIgnoreCase))
        {
            return $"The {collectorName} collector is running against {serverName} but cannot read its " +
                   $"source: its last run{observed} was recorded as a non-fatal skip rather than a failure, " +
                   $"so {CapturePathOf(collectorName)} is not being stored.{said} Act on that sentence — it " +
                   "is what the monitored server itself answered, and it distinguishes the three states this " +
                   "one status carries: a grant the login was refused, a source object or extension that was " +
                   "never created, and a feature the platform does not implement or has switched off. " +
                   PreconditionEpilogue;
        }

        return null;
    }

    /// <summary>
    /// The precondition explanation for a read whose collector has produced <b>no <c>collection_log</c> row
    /// at all</b> on a server that is demonstrably collecting other things — or null when that is not the
    /// case, in which case the caller falls through to its own miss vocabulary.
    ///
    /// <para><b>Why this arm has to exist, and why <see cref="CollectionOutcomeMessage"/> cannot cover it.</b>
    /// That method reports what the collector's last run RECORDED. A collector whose <c>AppliesTo</c> gate is
    /// off never runs, and the runner returns before writing anything — deliberately, because a per-cycle
    /// fake row was thousands of rows a day of noise. So the gated-off case records nothing, the outcome
    /// reader finds nothing, and the read falls through to its <c>empty</c> miss, which for
    /// <c>get_running_jobs</c> means asserting "No running SQL Agent jobs found" about a server we were never
    /// permitted to look at. That is precisely the affirmative claim the precondition vocabulary was built to
    /// stop, surviving in the one case that produces no evidence to read.</para>
    ///
    /// <para><b>The inference.</b> No row EVER for this collector, while the server has collected something
    /// recently, means the gate is off — there is no third way to get here. Retention cannot fake it: a
    /// collector whose rows have all aged out has not run inside the retention window either, which is the
    /// same conclusion. Both halves are required; a server that has collected nothing at all is a collection
    /// outage and belongs in <c>unavailable</c>, not here.</para>
    ///
    /// <para><b>What it must not claim.</b> It cannot say WHICH gate is off, because the facts that decide
    /// are not persisted — <c>HAS_DBACCESS('msdb')</c> and the RDS flag live on the cached connection, not on
    /// the registry. So it names the candidates rather than picking one, and carries the connect-scoped
    /// epilogue, because the actionable candidate is a grant whose probe is cached for the connection's
    /// life.</para>
    /// </summary>
    /// <param name="serverName">The server as the caller named it.</param>
    /// <param name="collectorName">The collector serving the read that missed.</param>
    /// <param name="gateCandidates">Operator-facing description of what could switch this collector off.</param>
    /// <param name="collectorEverRan">Whether this collector has any recorded run against this server.</param>
    /// <param name="serverLastCollectedUtc">The server's most recent run by ANY collector, or null if none.</param>
    public static string? GatedOffMessage(
        string serverName,
        string collectorName,
        string gateCandidates,
        bool collectorEverRan,
        DateTime? serverLastCollectedUtc)
    {
        if (collectorEverRan || serverLastCollectedUtc is null)
        {
            return null;
        }

        return $"The {collectorName} collector has never run against {serverName}, while the server itself " +
               $"is collecting normally{DescribeObserved(serverLastCollectedUtc)}. That combination means the " +
               $"collector's gate is switched off for this server rather than that it has nothing to report, " +
               $"so this read cannot tell you the state it describes — it can only tell you it was never " +
               $"permitted to look. {gateCandidates} " +
               ConnectScopedEpilogue;
    }

    /// <summary>
    /// One database's Query Store configuration as the hourly <c>query_store_health</c> collector recorded
    /// it. <paramref name="ActualState"/> is what <c>sys.database_query_store_options</c> reports, which is
    /// the fact that matters: DESIRED READ_WRITE with ACTUAL READ_ONLY is the classic silent failure, and a
    /// message that read the desired state would call that database healthy.
    /// </summary>
    public readonly record struct QueryStoreDatabaseState(string DatabaseName, string? ActualState);

    /// <summary>
    /// The precondition explanation for a Query Store read that came back empty on a server whose recorded
    /// per-database configuration says Query Store is not collecting, or <c>null</c> when it says nothing of
    /// the kind.
    ///
    /// <para><b>This replaces a GUESS with a fact.</b> Both SKUs already told the caller "Query Store may not
    /// be enabled on the target databases" — hedged, because the read had no way to find out, and unhelpful
    /// for exactly that reason: it is equally true on a server where Query Store is fully on and the window
    /// simply reached past what the raw tier retains. The store has known the answer all along, in a table
    /// collected hourly for this purpose.</para>
    ///
    /// <para>Null when the snapshot is EMPTY (no evidence is not evidence — the health collector may not have
    /// completed a cycle, and a server that predates Query Store has nothing to record) and null when ANY
    /// database in scope reads READ_WRITE (Query Store is collecting somewhere the read could have looked, so
    /// the emptiness has some other cause and the read's own miss is the honest one). Scope matters: the
    /// caller passes the states for the databases its read actually covered, so a per-database read gets the
    /// answer for THAT database rather than for the server's most flattering one.</para>
    /// </summary>
    public static string? QueryStoreDisabledMessage(
        string serverName,
        IReadOnlyList<QueryStoreDatabaseState> states,
        DateTime? observedUtc)
    {
        if (states is null || states.Count == 0)
        {
            return null;
        }

        if (states.Any(s => IsCollecting(s.ActualState)))
        {
            return null;
        }

        var observed = DescribeObserved(observedUtc);
        var errored = states.Where(s => IsErrored(s.ActualState)).ToList();
        var readOnly = states.Where(s => IsReadOnly(s.ActualState)).ToList();
        var off = states.Where(s => !IsErrored(s.ActualState) && !IsReadOnly(s.ActualState)).ToList();

        /* THREE not-collecting states, and each of them has a different remedy. ERROR and READ_ONLY are both
           states a database reaches AFTER being configured, so the OFF wording — "turn it on" — is not just
           unhelpful there, it is an instruction to re-issue an ALTER that is already in effect and will
           silently no-op. The ordering is by how badly the OFF wording would mislead: ERROR is a database
           whose Query Store broke, which is the one nobody would guess from "not enabled". */
        if (errored.Count > 0)
        {
            return $"Query Store is in the ERROR state on {Describe(errored)} of {serverName}{observed}, and " +
                   "no database this read covers is recording. ERROR is not OFF: Query Store WAS configured " +
                   "and stopped on its own after an internal failure, so its desired state is typically still " +
                   "READ_WRITE and ALTER DATABASE [name] SET QUERY_STORE = ON is already in effect and will " +
                   "not repair it. Run sys.sp_query_store_consistency_check inside the affected database " +
                   "first; if that does not clear it, ALTER DATABASE [name] SET QUERY_STORE CLEAR, or cycle " +
                   "it OFF and back ON — both of which discard the Query Store data already held. " +
                   PreconditionEpilogue;
        }

        /* READ_ONLY before OFF for the same reason: the database WAS configured and stopped collecting on
           its own, and saying "Query Store is off" about a database whose desired state is READ_WRITE sends
           the operator to re-run an ALTER that is already in effect. */
        if (readOnly.Count > 0)
        {
            return $"Query Store is READ_ONLY and recording nothing new on {Describe(readOnly)} of " +
                   $"{serverName}{observed}, and no database this read covers is in READ_WRITE. A READ_ONLY " +
                   "Query Store still answers queries about what it already holds, which is why this looks " +
                   "like a quiet server rather than a stopped one. It is almost always the storage cap: " +
                   "check readonly_reason with get_query_store_health, raise MAX_STORAGE_SIZE_MB or clear " +
                   "space, then ALTER DATABASE [name] SET QUERY_STORE (OPERATION_MODE = READ_WRITE). " +
                   PreconditionEpilogue;
        }

        return $"Query Store is not enabled on {Describe(off)} of {serverName}{observed}, so there is nothing " +
               "for this read to return and no amount of waiting will change that. Turn it on with ALTER " +
               "DATABASE [name] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE), and the query_store " +
               "collector starts storing runtime statistics on its next cycle. " +
               PreconditionEpilogue;
    }

    /// <summary>
    /// READ_WRITE is the only <c>actual_state_desc</c> that records new runtime statistics. The other three
    /// the catalog view can report — OFF, READ_ONLY and ERROR — each get their own sentence above, because
    /// each has its own remedy. ALL_TRANSACTIONS and friends are capture MODES rather than states and never
    /// appear here; an unrecognised value counts as collecting, so a state this repo has not met cannot
    /// manufacture a precondition claim about a working server.
    /// </summary>
    private static bool IsCollecting(string? actualState) =>
        !string.IsNullOrWhiteSpace(actualState)
        && !IsReadOnly(actualState)
        && !IsErrored(actualState)
        && !actualState.Equals("OFF", StringComparison.OrdinalIgnoreCase);

    private static bool IsReadOnly(string? actualState) =>
        actualState is not null && actualState.Equals("READ_ONLY", StringComparison.OrdinalIgnoreCase);

    private static bool IsErrored(string? actualState) =>
        actualState is not null && actualState.Equals("ERROR", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The databases, named rather than counted, up to three — a count alone leaves the reader with another
    /// query to run before they can act, and three is where the sentence stops being readable.
    /// </summary>
    private static string Describe(List<QueryStoreDatabaseState> states)
    {
        var named = string.Join(", ", states.Take(3).Select(s => s.DatabaseName));
        var noun = states.Count == 1 ? "database" : "databases";

        return states.Count <= 3
            ? $"the {noun} this read covers ({named})"
            : $"all {states.Count} databases this read covers ({named} and {states.Count - 3} more)";
    }

    /// <summary>
    /// The observation instant as a parenthetical, or nothing at all when it is unknown. A precondition is a
    /// claim about NOW made from a stored measurement, so how old the measurement is belongs in the sentence
    /// — the reader is the only one who can judge whether an hour-old snapshot still describes their server.
    /// </summary>
    private static string DescribeObserved(DateTime? observedUtc) =>
        observedUtc is { } when
            ? $" (as of {DateTime.SpecifyKind(when, DateTimeKind.Utc).ToString("u", CultureInfo.InvariantCulture)})"
            : string.Empty;

    /// <summary>The stored explanation, quoted. Empty when the runner recorded none.</summary>
    private static string DescribeServerAnswer(string? errorMessage) =>
        string.IsNullOrWhiteSpace(errorMessage)
            ? string.Empty
            : $" The server said: {errorMessage.Trim()}";

    /// <summary>
    /// What the collector would have captured, borrowed from the capability vocabulary rather than restated.
    /// The two messages describe the same absence for different reasons, and a second noun-phrase table would
    /// be the drift <see cref="CollectorEngineCapability.CapturePathByCollector"/>'s own doc-comment warns
    /// about, doubled.
    /// </summary>
    private static string CapturePathOf(string collectorName) =>
        CollectorEngineCapability.CapturePathByCollector.TryGetValue(collectorName, out var described)
            ? described
            : "the data this read is served from";
}
