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
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;

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
///
/// <para><b>One file, for the one reader that cannot make a request (#4733).</b> A compose healthcheck runs
/// inside an image that has no <c>curl</c>, so it cannot ask <c>/api/ping</c>, and a container whose process
/// stayed up after a terminal verdict shows as running. Given a path (the <c>DARLING_STOPPED_MARKER</c>
/// environment variable, read in <c>Program.cs</c>), this class keeps a marker file present exactly while the
/// phase is <see cref="CollectorPhase.Stopped"/>. The in-process field stays the source of truth; the file
/// is a copy of the terminal verdict alone, and losing it costs the container's health signal and nothing
/// else. Without a path, nothing here touches the file system.</para>
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
    /// The fixed <see cref="Snapshot.Detail"/> text for each step's failure (#4316). The worker used to pass
    /// <c>ex.Message</c> straight through, but <c>Detail</c> leaves the process in <c>/api/ping</c>'s response
    /// body. In loopback mode that reaches any local process — the whole surface needs no token there; in
    /// network mode ping sits behind the same CIDR and sign-in gate as every other route (only logout and the
    /// OIDC routes are exempt, <c>IsAuthFlowPath</c>) — but a signed-in caller is still not the service log,
    /// and an exception's text was never vetted for that audience: a connection string, a file path or a
    /// driver's inner-exception chain can all land in <c>ex.Message</c>. The LogWarning/LogCritical line beside every
    /// <see cref="PublishRetrying"/>/<see cref="PublishStopped"/> call still logs the exception's full text to
    /// the service log, which is where that detail belongs.
    /// </summary>
    private static readonly Dictionary<StartupStep, string> FailureDetailByStep =
        new()
        {
            [StartupStep.Configuration] =
                "Loading or validating the configuration failed. The service log has the full error.",
            [StartupStep.ManagedStore] =
                "Bootstrapping the managed PostgreSQL store failed. The service log has the full error.",
            [StartupStep.Store] =
                "The store connection or migration failed. The service log has the full error.",
        };

    /// <summary>The fixed, non-exception <see cref="Snapshot.Detail"/> text to publish for
    /// <paramref name="step"/>'s failure. See <see cref="FailureDetailByStep"/> for why this replaces
    /// <c>ex.Message</c> at every <see cref="PublishRetrying"/>/<see cref="PublishStopped"/> call site.</summary>
    public static string FailureDetailFor(StartupStep step) => FailureDetailByStep[step];

    /// <summary>Appends the sustained-retry phrase to <paramref name="detail"/> (#4508), so every reader of
    /// <see cref="Snapshot.Detail"/> — the MCP/Viewer surfaces and <c>/api/ping</c> alike — sees the same
    /// text once a step has spent its fast budget: <c>StartupFailureTriage.RetryBudget</c> seconds at the
    /// original cadence, and is now retrying every <c>StartupFailureTriage.SustainedRetryDelay</c> seconds
    /// with no cap. Built from those two constants and <paramref name="attempt"/>, never a literal.</summary>
    internal static string SustainedRetryDetail(string detail, int attempt)
        => $"{detail} \u2014 retrying every {(int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds}s, "
            + $"attempt {attempt} (past the {(int)StartupFailureTriage.RetryBudget.TotalSeconds}s fast budget)";

    /// <summary>
    /// The fixed <see cref="Snapshot.Detail"/> for the one ManagedStore stand-down that is not on an
    /// exception path (#4316 round 1 B1): <c>postgres.managed = true</c> asked for the bundled runtime and
    /// its DPAPI-protected credential on a host that can have neither. Published by
    /// <see cref="PublishManagedStoreNeedsWindows"/>.
    /// </summary>
    public const string ManagedStoreNeedsWindowsDetail =
        "postgres.managed = true requires Windows; set postgres.managed = false and point postgres.connectionString "
        + "at your own PostgreSQL instead.";

    /// <summary>
    /// A coherent published snapshot; null until the worker first publishes.
    /// </summary>
    /// <param name="Phase">Where the collector is relative to having started collecting.</param>
    /// <param name="Step">The startup step the phase is about; null for
    /// <see cref="CollectorPhase.Collecting"/>, which is not about a step.</param>
    /// <param name="Detail">The step's fixed failure sentence (<see cref="FailureDetailFor"/>), the joined
    /// configuration problems, or the not-Windows sentence, never exception text (#4316); null for
    /// <see cref="CollectorPhase.Collecting"/>. Once <see cref="Sustained"/> is true, the sentence has the
    /// <see cref="SustainedRetryDetail"/> phrase appended, so every reader of this field — the MCP/Viewer
    /// surfaces and <c>/api/ping</c>'s <c>detail</c> alike — sees the same text (#4508).</param>
    /// <param name="Attempt">Which attempt is in flight, and how many the budget allows — both zero
    /// outside <see cref="CollectorPhase.Retrying"/>, where an attempt number is the only one of the two
    /// caps a reader can be shown (the wall-clock budget can end the retrying earlier).</param>
    /// <param name="Attempts">The attempt cap the retry budget allows — zero once <see cref="Sustained"/> is
    /// true (#4508), rather than a spent cap a reader would otherwise read as "attempt 30 of 25".</param>
    /// <param name="AsOfUtc">When this phase was published — for
    /// <see cref="CollectorPhase.Collecting"/> that is when collection started, and for the two failure
    /// phases it is when the failure was last observed.</param>
    /// <param name="Sustained">True once a <see cref="CollectorPhase.Retrying"/> step has spent its fast
    /// budget and moved to the slower, unbounded retry (#4508) — see
    /// <see cref="DarlingWebEndpoints.DescribePing"/> for how the ping body renders it. Always false outside
    /// <see cref="CollectorPhase.Retrying"/>. Defaults to false so the existing terminal/collecting publishes,
    /// which never pass it, are unaffected.</param>
    public sealed record Snapshot(
        CollectorPhase Phase,
        StartupStep? Step,
        string? Detail,
        int Attempt,
        int Attempts,
        DateTime AsOfUtc,
        bool Sustained = false);

    private volatile Snapshot? _current;

    private readonly string? _stoppedMarkerPath;
    private readonly ILogger? _logger;
    private int _markerFaultLogged;

    /// <summary>
    /// Creates the state, optionally with the path of the stopped-collection marker (#4733).
    ///
    /// <para><b>Why a file.</b> After a terminal startup verdict the process stays up on purpose (see the class
    /// remarks), so a container never notices that collection stopped: it keeps running. The compose file's
    /// healthcheck for the <c>darling</c> service tests for this file, so the container reports unhealthy
    /// while the phase is <see cref="CollectorPhase.Stopped"/> and healthy otherwise. It is a file and not a
    /// call to <c>/api/ping</c> because the image has no <c>curl</c>; <c>/api/ping</c> stays the richer
    /// answer, and the marker only carries the same <see cref="Snapshot.Detail"/> and the time.</para>
    ///
    /// <para><b>Off unless asked.</b> <paramref name="stoppedMarkerPath"/> is null, empty or whitespace for the
    /// Windows service and every non-compose deployment, and then nothing is written, nothing is removed and
    /// nothing is logged. When it is set, a marker left by the previous run is removed HERE, because a
    /// restarted container keeps its <c>/tmp</c> and would otherwise report the previous run's verdict until
    /// this run reached its own.</para>
    /// </summary>
    /// <param name="stoppedMarkerPath">Where to write the marker, or null for none; read from the
    /// <c>DARLING_STOPPED_MARKER</c> environment variable once, in <c>Program.cs</c>.</param>
    /// <param name="logger">Where a fault writing or removing the marker is logged (once, at Warning).</param>
    public CollectorRuntimeState(string? stoppedMarkerPath = null, ILogger? logger = null)
    {
        _stoppedMarkerPath = string.IsNullOrWhiteSpace(stoppedMarkerPath) ? null : stoppedMarkerPath.Trim();
        _logger = logger;

        RemoveStoppedMarker();
    }

    /// <summary>Publishes a classified-transient failure of <paramref name="step"/> that is being retried
    /// (worker only; called from each retry arm alongside its warning line). The detail is always
    /// <see cref="FailureDetailFor"/> — an exception-path retry has no other text to publish, and (#4316
    /// round 1 B1) there is no longer a <c>string</c> parameter here for a caller to put <c>ex.Message</c>
    /// in instead. <paramref name="sustained"/> is true once the fast retry budget is spent and the loop
    /// has moved to the slower, unbounded retry (#4508); <paramref name="attempts"/> is published as zero
    /// in that case — the cap the fast arm counted against no longer bounds anything, and publishing it
    /// past its own value is what rendered as "attempt 30 of 25" before this. When sustained, the published
    /// <see cref="Snapshot.Detail"/> also gets the <see cref="SustainedRetryDetail"/> phrase appended, so a
    /// reader of the detail text — not just the structured <see cref="Snapshot.Sustained"/> flag — can tell
    /// the retry is now unbounded.</summary>
    public void PublishRetrying(StartupStep step, int attempt, int attempts, bool sustained = false)
    {
        var wasStopped = _current?.Phase == CollectorPhase.Stopped;

        _current = new Snapshot(
            CollectorPhase.Retrying, step,
            sustained
                ? SustainedRetryDetail(FirstLineOf(FailureDetailFor(step)), attempt)
                : FirstLineOf(FailureDetailFor(step)),
            attempt, sustained ? 0 : attempts, DateTime.UtcNow, sustained);

        if (wasStopped)
        {
            RemoveStoppedMarker();
        }
    }

    /// <summary>Publishes a terminal failure of <paramref name="step"/> (worker only; called from each
    /// EXCEPTION-path collection-blocking exit, before the <c>return</c> — after the critical line, so a
    /// throw here could never cost the operator the log line). The detail is always
    /// <see cref="FailureDetailFor"/>; <see cref="PublishConfigurationProblems"/> and
    /// <see cref="PublishManagedStoreNeedsWindows"/> are the two terminal stand-downs that are NOT exception
    /// paths, and publish their own detail through the shared <see cref="PublishStoppedCore"/>.</summary>
    public void PublishStopped(StartupStep step) => PublishStoppedCore(step, FailureDetailFor(step));

    /// <summary>Publishes a terminal Configuration stand-down for a validated-but-rejected
    /// <paramref name="config"/> (worker only; #2953): every problem <see cref="DarlingConfig.Validate"/>
    /// finds against it, joined with <c>"; "</c> — all of them, not just the first, because
    /// <c>Validate</c> is all-fatal and a ping body naming only one of several problems would send an
    /// operator to fix a config that still would not start. Sanitized and length-capped exactly like every
    /// other <see cref="Snapshot.Detail"/>, by <see cref="PublishStoppedCore"/>. Takes the config itself,
    /// not a problem list (#4316 round 2 L1-r2 a): a <c>string</c>- or <c>IReadOnlyList&lt;string&gt;</c>-typed
    /// parameter here is exactly the free-text seat a future caller could put exception text into.</summary>
    public void PublishConfigurationProblems(DarlingConfig config)
        => PublishStoppedCore(StartupStep.Configuration, ConfigurationProblemsDetail(config.Validate()));

    /// <summary>Every problem <see cref="DarlingConfig.Validate"/> found, joined with <c>"; "</c> (#2953).
    /// Pure, so the join is testable on its own; only <see cref="PublishConfigurationProblems"/>
    /// publishes it.</summary>
    internal static string ConfigurationProblemsDetail(IReadOnlyList<string> problems) => string.Join("; ", problems);

    /// <summary>Publishes the terminal ManagedStore stand-down for the one config combination that reaches
    /// neither a retry nor an exception: <c>postgres.managed = true</c> on a non-Windows host (worker
    /// only).</summary>
    public void PublishManagedStoreNeedsWindows()
        => PublishStoppedCore(StartupStep.ManagedStore, ManagedStoreNeedsWindowsDetail);

    /// <summary>The shared body every terminal stand-down publishes through, whatever its detail's source —
    /// an exception's fixed sentence (<see cref="PublishStopped"/>), the joined config problems
    /// (<see cref="PublishConfigurationProblems"/>), or the not-Windows sentence
    /// (<see cref="PublishManagedStoreNeedsWindows"/>).</summary>
    private void PublishStoppedCore(StartupStep step, string detail)
    {
        var stopped = new Snapshot(CollectorPhase.Stopped, step, FirstLineOf(detail), 0, 0, DateTime.UtcNow);
        _current = stopped;

        /* #4733: the marker the compose healthcheck tests for, written after the snapshot is published and
           on the one path every route to Stopped shares. It never throws, so it cannot cost the settle call
           below or the worker's return its turn. */
        WriteStoppedMarker(stopped);

        /* #3914: every one of these stand-downs comes before role provisioning, so a web or MCP host in a
           container waiting to hear whether the compose store's roles were provisioned would otherwise wait for
           a verdict that never comes. It connects with its role's credential from an earlier start instead, or as
           the owner when the service holds none, and its warning says which and why. */
        DarlingStoreLogins.SettleComposeStoreVerdict(
            $"The collector stopped before it could provision the store's roles ({step}: {FirstLineOf(detail)}).");
    }

    /// <summary>Publishes that the collection loop started (worker only; called once, where the loop logs
    /// that it began).</summary>
    public void PublishCollecting()
    {
        var wasStopped = _current?.Phase == CollectorPhase.Stopped;

        _current = new Snapshot(CollectorPhase.Collecting, null, null, 0, 0, DateTime.UtcNow);

        if (wasStopped)
        {
            RemoveStoppedMarker();
        }
    }

    /// <summary>What the marker holds: the moment collection stopped, then the snapshot's
    /// <see cref="Snapshot.Detail"/> — the text <c>/api/ping</c> already serves, so it is as safe to show
    /// here as there (#4733). Two lines, so <c>cat</c> on the file answers "since when, and why".</summary>
    internal static string StoppedMarkerText(Snapshot stopped)
        => stopped.AsOfUtc.ToString("O", CultureInfo.InvariantCulture) + Environment.NewLine
           + stopped.Detail + Environment.NewLine;

    /// <summary>Writes the stopped-collection marker (#4733). Nothing without a path; a fault is logged once
    /// and swallowed, because this runs on the worker's stand-down path and a marker that could throw would
    /// cost the operator the verdict it exists to report.</summary>
    private void WriteStoppedMarker(Snapshot stopped)
    {
        if (_stoppedMarkerPath is null)
        {
            return;
        }

        try
        {
            File.WriteAllText(_stoppedMarkerPath, StoppedMarkerText(stopped));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogMarkerFaultOnce("write", ex);
        }
    }

    /// <summary>Removes the stopped-collection marker (#4733): on creation, for the previous run's leftover,
    /// and when the phase leaves <see cref="CollectorPhase.Stopped"/>. Nothing without a path, and nothing
    /// to do when the file is not there. A fault is logged once and swallowed.</summary>
    private void RemoveStoppedMarker()
    {
        if (_stoppedMarkerPath is null)
        {
            return;
        }

        try
        {
            File.Delete(_stoppedMarkerPath);
        }
        catch (DirectoryNotFoundException)
        {
            /* No directory, so no marker. Windows reports it where Unix reports nothing; either way there is
               nothing to remove. */
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogMarkerFaultOnce("remove", ex);
        }
    }

    /// <summary>One Warning for the process, whichever direction faulted first: a path that cannot be written
    /// or removed will fault again on every publish, and the point is to say so once, not per publish.</summary>
    private void LogMarkerFaultOnce(string action, Exception ex)
    {
        if (Interlocked.Exchange(ref _markerFaultLogged, 1) != 0)
        {
            return;
        }

        _logger?.LogWarning(
            "Could not {Action} the stopped-collection marker {Path} ({Message}). The compose healthcheck tests for that file, so the container's health may not show whether collection has stopped (#4733).",
            action, _stoppedMarkerPath, ex.Message);
    }

    /// <summary>The latest published snapshot, or null when the worker has not reached a verdict yet.</summary>
    public Snapshot? Read() => _current;

    /// <summary>
    /// The first line of a failure message, CR-trimmed and length-capped — the same reduction
    /// <c>DarlingCliCommands.FirstLineOf</c> and <c>ViewerStoreUnreachableException</c> apply, for the same
    /// reason and one more.
    ///
    /// <para>Since #4316's L3 fix, <paramref name="detail"/> is never a driver or server exception's own
    /// message: it is one of <see cref="FailureDetailFor"/>'s fixed sentences, the not-Windows sentence, or
    /// <see cref="ConfigurationProblemsDetail"/>'s joined configuration problems. This reduction stays a
    /// BACKSTOP rather than dead code, because the problem list <c>DarlingConfig.Validate</c> returns has
    /// no bound — a config with many problems, or one long problem sentence, is still possible, and either
    /// could still be multi-line or run past the cap below.</para>
    ///
    /// <para>The cap is about the destination: this text is the only part of the published snapshot that
    /// leaves the process and travels over HTTP, in the ping body, so what an unbounded problem list can put
    /// there is worth bounding. Truncation is MARKED, so a reader can tell a shortened message from a
    /// complete one and go to the log for the rest.</para>
    /// </summary>
    internal static string FirstLineOf(string detail)
    {
        var line = (detail ?? string.Empty).Split('\n')[0].TrimEnd('\r');

        return line.Length <= MaxDetailLength ? line : line[..MaxDetailLength] + "…";
    }

    /// <summary>How much of a failure message travels in the ping body. Generous enough for the shapes that
    /// actually arrive — a fixed failure sentence, or a handful of joined configuration problems — and
    /// finite because the configuration-problem list <c>DarlingConfig.Validate</c> returns has no bound, so
    /// the cap stays a backstop rather than a limit this process would otherwise never reach.</summary>
    internal const int MaxDetailLength = 400;
}
