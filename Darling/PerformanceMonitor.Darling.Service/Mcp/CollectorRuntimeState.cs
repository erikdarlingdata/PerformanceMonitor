/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The collector's own startup verdict, published by the WORKER and readable WITHOUT touching the store
/// (#2953). The third seam of the <see cref="McpRuntimeState"/> / <see cref="WebRuntimeState"/> family, and
/// the one whose whole reason for existing is that it answers when the store cannot.
///
/// <para><b>The signal this replaces was a literal.</b> Every surface that could tell an operator
/// "collection is not running" reached the store to find out — <c>get_fleet_overview</c>, <c>/api/fleet</c>
/// and the Viewer's fleet grid all funnel into <c>DarlingFleetReader</c>'s reads — so with the store
/// unreachable they fail or come back empty, which reads as a UI or permissions fault rather than as the
/// store being down. The one surface that did not read the store, <c>/api/ping</c>, answered a hardcoded
/// <c>"ok"</c>, so it could not report anything either. That mattered more than it sounds: the
/// stand-down at the end of <c>DarlingWorker</c>'s startup steps <c>return</c>s, which completes the
/// worker task SUCCESSFULLY — the host stays up, both Kestrel hosts keep serving, and on Windows the
/// service keeps reporting Running, so SCM recovery never fires because nothing crashed. The only
/// diagnosis was one <c>LogCritical</c> line in a rolling file log, and the service registers no console
/// log provider, so that line prints nowhere visible even when run interactively.</para>
///
/// <para><b>Why an in-process field is the right store-free read.</b> The alternative — a row, a file, a
/// heartbeat table — is a thing that can itself be unavailable, and the failure being reported is
/// precisely "the durable store is unavailable". A volatile reference in the same process as the worker
/// and both hosts has no failure mode of its own, and both existing seams already prove the shape carries
/// worker state to a Kestrel host correctly.</para>
///
/// <para><b>A STARTUP verdict, and deliberately not a liveness heartbeat.</b>
/// <see cref="CollectorPhase.Collecting"/> means the collection loop STARTED, not that the current sweep
/// succeeded — the phase is set once, where the loop logs that it began, and is not re-published per
/// cycle. A store outage that begins after that point leaves this reading
/// <see cref="CollectorPhase.Collecting"/>, and that is the honest division of labour: once the store has
/// been reachable at least once, <c>collection_log</c> exists and the self-alert engine polls it, so the
/// later outage has a surface that can already report it. What had no surface at all is the window before
/// the first successful store interaction, which is exactly the window this covers.</para>
///
/// <para><b>Null until the worker publishes, matching both siblings rather than carrying an explicit
/// starting value.</b> An unpublished read means the worker has not yet reached a verdict — ordinary for
/// the first seconds of a start, and the state a reader should render as "starting" rather than as either
/// healthy or broken. Every collection-blocking exit publishes
/// <see cref="CollectorPhase.Stopped"/> before returning, so a terminal stand-down can never be mistaken
/// for a slow start; the one exit that stays silent is cancellation, which means the service is stopping
/// and is not a collector fault.</para>
///
/// <para>Thread-safety: one writer (the worker's startup path, then nothing), many readers (the web host's
/// request threads). State is swapped as one immutable record reference, so a reader always sees a
/// coherent snapshot — never a phase from one publish with an attempt count from another.</para>
/// </summary>
public sealed class CollectorRuntimeState
{
    /// <summary>
    /// Where the collector is relative to having started collecting. Four values because that is what a
    /// reader has to be able to tell apart: fine, not yet, failing but recovering on its own, and failed
    /// until somebody intervenes. Collapsing the last two would put a two-second store restart and a
    /// migration rung that can never apply behind the same word.
    /// </summary>
    public enum CollectorPhase
    {
        /// <summary>A collection-blocking startup step failed with something
        /// <c>StartupFailureTriage.IsRetryable</c> accepted, and is being retried on its budget. Transient
        /// by classification: this becomes <see cref="Collecting"/> or <see cref="Stopped"/> within
        /// <c>StartupFailureTriage.RetryBudget</c>.</summary>
        Retrying,

        /// <summary>A collection-blocking startup step failed terminally. Collection does not start for
        /// the life of this process and no amount of waiting changes that — the process must be restarted
        /// after fixing whatever <see cref="Snapshot.Detail"/> names.</summary>
        Stopped,

        /// <summary>The collection loop started. See the class remarks for why this is not a claim about
        /// the current sweep.</summary>
        Collecting,
    }

    /// <summary>
    /// Which of the collection-blocking startup steps a <see cref="CollectorPhase.Retrying"/> or
    /// <see cref="CollectorPhase.Stopped"/> phase is about — the same three sites
    /// <c>StartupFailureTriage</c> classifies for, named so a reader can say WHERE the start stopped
    /// without parsing the message.
    /// </summary>
    public enum StartupStep
    {
        /// <summary>Loading or validating <c>darling.json</c>.</summary>
        Configuration,

        /// <summary>Bootstrapping the bundled managed PostgreSQL (Windows, <c>postgres.managed = true</c>).</summary>
        ManagedStore,

        /// <summary>Opening the store connection and applying the migration ladder.</summary>
        Store,
    }

    /// <summary>
    /// A coherent published snapshot; null until the worker first publishes.
    /// </summary>
    /// <param name="Phase">Where the collector is relative to having started collecting.</param>
    /// <param name="Step">The startup step the phase is about; null for
    /// <see cref="CollectorPhase.Collecting"/>, which is not about a step.</param>
    /// <param name="Detail">The failure's message, as the critical/warning log line reports it; null for
    /// <see cref="CollectorPhase.Collecting"/>.</param>
    /// <param name="Attempt">Which attempt is in flight, and how many the budget allows — both zero
    /// outside <see cref="CollectorPhase.Retrying"/>, where an attempt number is the only one of the two
    /// caps a reader can be shown (the wall-clock budget can end the retrying earlier).</param>
    /// <param name="Attempts">The attempt cap the retry budget allows.</param>
    /// <param name="AsOfUtc">When this phase was published — for
    /// <see cref="CollectorPhase.Collecting"/> that is when collection started, and for the two failure
    /// phases it is when the failure was last observed.</param>
    public sealed record Snapshot(
        CollectorPhase Phase,
        StartupStep? Step,
        string? Detail,
        int Attempt,
        int Attempts,
        DateTime AsOfUtc);

    private volatile Snapshot? _current;

    /// <summary>Publishes a classified-transient failure of <paramref name="step"/> that is being retried
    /// (worker only; called from each retry arm alongside its warning line).</summary>
    public void PublishRetrying(StartupStep step, string detail, int attempt, int attempts)
        => _current = new Snapshot(CollectorPhase.Retrying, step, detail, attempt, attempts, DateTime.UtcNow);

    /// <summary>Publishes a terminal failure of <paramref name="step"/> (worker only; called from each
    /// collection-blocking exit, before the <c>return</c> — after the critical line, so a throw here could
    /// never cost the operator the log line).</summary>
    public void PublishStopped(StartupStep step, string detail)
        => _current = new Snapshot(CollectorPhase.Stopped, step, detail, 0, 0, DateTime.UtcNow);

    /// <summary>Publishes that the collection loop started (worker only; called once, where the loop logs
    /// that it began).</summary>
    public void PublishCollecting()
        => _current = new Snapshot(CollectorPhase.Collecting, null, null, 0, 0, DateTime.UtcNow);

    /// <summary>The latest published snapshot, or null when the worker has not reached a verdict yet.</summary>
    public Snapshot? Read() => _current;
}
