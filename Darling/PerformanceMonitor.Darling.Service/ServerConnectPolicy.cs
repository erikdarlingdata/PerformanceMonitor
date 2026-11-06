/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The delay before the next connect attempt against a server that keeps failing to connect (#4710).
/// A fixed 60 s retry meant a server that stayed down cost a full connect timeout every minute forever.
/// The delay now doubles from 60 s to a 240 s cap, with +/-20% jitter, so the longest wait is 288 s: a
/// recovered server is noticed within five minutes, and a fleet of servers that failed together does not
/// retry together.
/// </summary>
internal static class ServerConnectBackoff
{
    internal const int BaseSeconds = 60;

    internal const int CapSeconds = 240;

    internal const double JitterFraction = 0.2;

    /// <param name="consecutiveFailures">Failed attempts in a row, counting the one that just failed (1 = the first).</param>
    /// <param name="jitterUnit">A value in [0, 1]; 0.5 is no jitter. Production passes a random draw, tests pin it.</param>
    internal static TimeSpan NextDelay(int consecutiveFailures, double jitterUnit)
    {
        var exponent = Math.Clamp(consecutiveFailures - 1, 0, 10);
        var seconds = Math.Min((double)CapSeconds, BaseSeconds * Math.Pow(2, exponent));
        var unit = Math.Clamp(jitterUnit, 0.0, 1.0);
        return TimeSpan.FromSeconds(seconds * (1.0 + JitterFraction * (2.0 * unit - 1.0)));
    }
}

/// <summary>
/// The outcome of one connect attempt: a runtime, or the exception that stopped it. <c>Config</c> is the
/// definition the attempt was made WITH (#4710): a reload can replace the server's definition while the attempt
/// runs or while the body waits for the fleet permit, and the install step needs to know whether what it is
/// about to install (or back off from) still describes the server.
/// </summary>
internal readonly record struct ConnectAttempt(MonitoredServer Config, ServerRuntime? Runtime, Exception? Failure);

/// <summary>
/// Runs a server's connect attempt in its own small gate instead of the fleet collection gate (#4710).
/// SqlClient takes about 15 s to fail for every kind of dead server, so an attempt that ran inside the
/// fleet gate held a collection slot for those 15 s: about 22 down servers filled the default gate and the
/// healthy servers queued behind them. The attempt touches only the monitored server, never the store,
/// so this gate is not bound by the store's connection pool the way the fleet gate is, and it is a fixed
/// width that never scales with the core count.
/// </summary>
internal static class ServerConnectProbe
{
    internal const int GateWidth = 8;

    /// <summary>
    /// Never throws except <see cref="OperationCanceledException"/> (shutdown): a failed connect is returned
    /// as <see cref="ConnectAttempt.Failure"/> so the caller handles it under the fleet permit, where its
    /// store writes belong.
    /// </summary>
    internal static async Task<ConnectAttempt> AttemptAsync(
        MonitoredServer config,
        Func<MonitoredServer, CancellationToken, Task<ServerRuntime>> connect,
        SemaphoreSlim probeGate,
        CancellationToken cancellationToken)
    {
        await probeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return new ConnectAttempt(config, await connect(config, cancellationToken).ConfigureAwait(false), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new ConnectAttempt(config, null, ex);
        }
        finally
        {
            probeGate.Release();
        }
    }
}

/// <summary>
/// The words the in-flight check uses for a collection body that is still in flight (#4710), split out of the
/// worker's sweep loop so the wording is pinned by tests. The wording is what an operator reads to decide
/// whether to raise the fleet's slot count or to look at a server, so it has to name the right cause.
/// </summary>
internal static class SweepInFlightWording
{
    /// <summary>
    /// The Info line for a body that has waited past the in-flight threshold for a fleet collection slot. The
    /// holes are Server, Elapsed (seconds since launch) and Limit (the effective fleet concurrency limit).
    /// </summary>
    internal const string QueuedInfoTemplate =
        "[{Server}] collection body has waited {Elapsed:F0}s for a free slot (fleet concurrency limit {Limit}) \u2014 queued, not stalled; it has not started yet";

    /// <summary>
    /// The Info line for a body that has waited past the in-flight threshold inside its connect attempt. That
    /// attempt runs before the body asks for a fleet slot, so the queued line above would blame the fleet
    /// concurrency limit for what is a slow or unreachable server. The holes are Server, Elapsed (seconds
    /// since the connect stage began, which includes any wait for one of the connect gate's slots) and Gate
    /// (the connect gate's width).
    /// </summary>
    internal const string ConnectingInfoTemplate =
        "[{Server}] collection body has been connecting for {Elapsed:F0}s \u2014 connect attempts run outside the collection slots (at most {Gate} at once), so this is not the fleet concurrency limit; the server is slow to answer or down, or the attempt is waiting behind other connect attempts; the body has not started yet";

    /// <summary>
    /// The state shown in the Debug line: how long the body has been running, that it is inside its connect
    /// attempt, or that it is queued for a fleet slot. A running body reports its execution clock whatever
    /// the connect stamp says.
    /// </summary>
    internal static string DebugState(bool running, double runningSeconds, bool connecting, double connectSeconds)
    {
        if (running)
        {
            return FormattableString.Invariant($"running {runningSeconds:F0}s");
        }

        return connecting
            ? FormattableString.Invariant($"connecting {connectSeconds:F0}s")
            : "queued for a slot";
    }

    /// <summary>
    /// The Info line for a body that has not started running and has waited past the threshold: the connect
    /// stage's wording when it is inside its connect attempt, otherwise the queued wording. The queued line
    /// takes (Server, Elapsed, Limit) and the connecting line (Server, Elapsed, Gate).
    /// </summary>
    internal static string NotStartedInfoTemplate(bool connecting)
        => connecting ? ConnectingInfoTemplate : QueuedInfoTemplate;
}

/// <summary>
/// Decides whether a collector fault means the server's runtime should be dropped and reconnected (#4710).
/// A command timeout (SqlException number -2, class 11) does NOT mean the connection is dead: the
/// connection stays open and runs SELECT 1. Dropping on every timeout made a stressed server lose the
/// reconnect's Extended Events setup and on-load snapshots (75 s or more of collection) on top of the
/// slowness that caused the timeout, the very reconnect storm the PostgreSQL arm was written to avoid. A
/// pre-login timeout on a hung server is also number -2, so a timeout is settled by a probe on a fresh
/// connection with a short timeout: the runtime is dropped only when the probe fails. One verdict serves every
/// timeout the server raises within <see cref="LivenessProbeMemo.ReuseSeconds"/> of it (<see cref="LivenessProbeMemo"/>).
/// A probe whose own login times out (<see cref="LivenessProbeOutcome.TimedOut"/>) counts the server alive, but only
/// <see cref="LivenessProbeMemo.MaxTimedOutProbesInARow"/> times in a row: the next one drops the runtime.
/// </summary>
internal static class ConnectionFaultDisposition
{
    internal const int ProbeSeconds = 5;

    internal static async Task<bool> ShouldDropRuntimeAsync(
        Exception exception,
        ServerRuntime? runtime,
        Func<ServerRuntime, CancellationToken, Task<bool>> probe,
        CancellationToken cancellationToken)
    {
        if (exception is SqlException sql)
        {
            if (sql.Class >= 20)
            {
                return true;
            }

            if (sql.Number == -2 && runtime is not null)
            {
                try
                {
                    return !await probe(runtime, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    /* Shutdown mid-probe: nothing is worth dropping on the way out. */
                    return false;
                }
            }

            return false;
        }

        return runtime?.Target.Engine == CollectorTargetEngine.PostgreSql
            && PostgresTargetProvider.Instance.Classify(exception, yieldsOnLockTimeout: false)
               == CollectorTargetFault.ConnectionFatal;
    }

    /// <summary>
    /// What a brand-new, unpooled login to the runtime's server finds inside <see cref="ProbeSeconds"/> seconds: it
    /// logs in and SELECT 1 answers, it times out, or it fails some other way. Unpooled so a hung server cannot answer
    /// from a pooled session that was already open.
    /// </summary>
    internal static Task<LivenessProbeOutcome> ProbeFreshConnectionAsync(ServerRuntime runtime, CancellationToken cancellationToken) =>
        ProbeFreshConnectionAsync(runtime, OpenAndSelectOneAsync, cancellationToken);

    /// <summary>
    /// The probe with its login injected: <paramref name="openAndSelectOne"/> logs in on a fresh connection and runs
    /// SELECT 1, or throws. What a throw means is decided here, so a test can hand in the driver's own faults: a
    /// timeout (number -2) from the login or from SELECT 1 is <see cref="LivenessProbeOutcome.TimedOut"/>, and every
    /// other fault, a refused port, an unknown host, a login the server rejected, or one outside the driver, is
    /// <see cref="LivenessProbeOutcome.Dead"/>. A target that is not SQL Server is not probed and counts as logged in.
    /// </summary>
    internal static async Task<LivenessProbeOutcome> ProbeFreshConnectionAsync(
        ServerRuntime runtime,
        Func<ServerRuntime, CancellationToken, Task> openAndSelectOne,
        CancellationToken cancellationToken)
    {
        if (runtime.Target.Engine != CollectorTargetEngine.SqlServer)
        {
            return LivenessProbeOutcome.LoggedIn;
        }

        try
        {
            await openAndSelectOne(runtime, cancellationToken).ConfigureAwait(false);
            return LivenessProbeOutcome.LoggedIn;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (SqlException sql) when (sql.Number == -2)
        {
            return LivenessProbeOutcome.TimedOut;
        }
        catch (Exception)
        {
            return LivenessProbeOutcome.Dead;
        }
    }

    private static async Task OpenAndSelectOneAsync(ServerRuntime runtime, CancellationToken cancellationToken)
    {
        var builder = new SqlConnectionStringBuilder(runtime.ConnectionString)
        {
            Pooling = false,
            ConnectTimeout = ProbeSeconds,
            MultipleActiveResultSets = false,
        };

        using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand("SELECT 1;", connection) { CommandTimeout = ProbeSeconds };
        await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>What a liveness probe's fresh login found (#4710).</summary>
internal enum LivenessProbeOutcome
{
    /// <summary>The login completed and SELECT 1 answered.</summary>
    LoggedIn,

    /// <summary>
    /// The login, or SELECT 1, timed out (number -2) inside <see cref="ConnectionFaultDisposition.ProbeSeconds"/>. A server
    /// under CPU or thread-pool pressure does this while the sessions already open on it keep answering, so one timed-out
    /// probe is not evidence that the server is gone, and dropping the runtime over it would add the reconnect's Extended
    /// Events setup and on-load snapshots to the pressure that made it slow. A server that is hung, or whose network drops
    /// every packet, times out the same way, and never reaches a transport fault: every collector run opens a fresh login,
    /// so each run times out like the probe did. What tells the two apart is the memo's bound
    /// (<see cref="LivenessProbeMemo.MaxTimedOutProbesInARow"/>): a server under pressure completes a run, or answers a
    /// probe, between its timeouts, and a dead one does not.
    /// </summary>
    TimedOut,

    /// <summary>Any other fault: a refused port or an unknown host (class 20), a rejected login (18456), a fault outside the driver.</summary>
    Dead,
}

/// <summary>The verdict of one liveness probe: when it ran, what its login found, and whether the server counts as alive.</summary>
internal sealed record LivenessVerdict(DateTime ProbedUtc, LivenessProbeOutcome Outcome, bool Alive);

/// <summary>
/// One server's memory of its last liveness probe. A server under CPU or thread-pool pressure times out many
/// collectors close together, the sequential body's and the detached runs', and each timeout is settled by
/// <see cref="ConnectionFaultDisposition.ShouldDropRuntimeAsync"/> with a probe. Each probe is a fresh unpooled
/// login against the server that is already struggling, so one verdict serves every timeout that arrives within
/// <see cref="ReuseSeconds"/> of it: two sweep ticks, enough for every timeout one tick's dispatch raises to share a
/// probe, and short enough that a server that died is probed again on the first timeout past it. It also counts the
/// probes in a row whose own login timed out, and past <see cref="MaxTimedOutProbesInARow"/> of them a timed-out probe
/// is a dead verdict. Lives on the server's loop state and is cleared when the runtime is dropped, so a reconnected
/// server starts with no memory. The verdict is a reference, so a reader on another thread sees a whole one or none.
/// </summary>
internal sealed class LivenessProbeMemo
{
    internal const int ReuseSeconds = 30;

    /// <summary>
    /// How many probes in a row may time out and still count the server alive. The next one drops the runtime, so a hung
    /// or black-holed server, whose every fresh login times out, is dropped on its fourth probe (at least three memo windows
    /// into the outage) instead of being kept for all of it with every collector waiting out its full connect timeout on
    /// every tick and the reconnect never armed. The streak ends when a run on the connection completes
    /// (<see cref="RunCompleted"/>) or a probe logs in: a server under pressure does one or the other between its timeouts.
    /// </summary>
    internal const int MaxTimedOutProbesInARow = 3;

    private LivenessVerdict? _last;
    private int _timedOutInARow;

    /// <summary>The last verdict, or null when none is held.</summary>
    internal LivenessVerdict? Last => _last;

    /// <summary>Probes in a row whose own login timed out, with no completed run and no login between them.</summary>
    internal int TimedOutProbesInARow => _timedOutInARow;

    /// <summary>Forgets the verdict and the streak: the runtime was dropped, and a reconnected server starts afresh.</summary>
    internal void Clear()
    {
        _last = null;
        _timedOutInARow = 0;
    }

    /// <summary>A run on the server's connection completed, so the server answers: the timed-out streak ends. The held
    /// verdict stays, since it is about fresh logins, not about the open session.</summary>
    internal void RunCompleted() => _timedOutInARow = 0;

    /// <summary>Whether a verdict stamped at <paramref name="verdict"/>.ProbedUtc still serves at <paramref name="nowUtc"/>.
    /// A clock that stepped back does not.</summary>
    internal static bool IsFresh(LivenessVerdict verdict, DateTime nowUtc) =>
        nowUtc >= verdict.ProbedUtc && nowUtc - verdict.ProbedUtc < TimeSpan.FromSeconds(ReuseSeconds);

    /// <summary>
    /// The held verdict when it is fresh, else <paramref name="probe"/>'s answer, remembered as of <paramref name="nowUtc"/>:
    /// a login counts the server alive and ends the timed-out streak, a timeout counts it alive while the streak is within
    /// <see cref="MaxTimedOutProbesInARow"/> and dead past it, and any other fault counts it dead. Two timeouts that arrive
    /// together with no verdict held may both probe; the later answer is the one kept.
    /// </summary>
    internal async Task<bool> ProbeAsync(
        ServerRuntime runtime,
        Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        if (_last is { } last && IsFresh(last, nowUtc))
        {
            return last.Alive;
        }

        var outcome = await probe(runtime, cancellationToken).ConfigureAwait(false);
        var alive = false;
        switch (outcome)
        {
            case LivenessProbeOutcome.LoggedIn:
                _timedOutInARow = 0;
                alive = true;
                break;
            case LivenessProbeOutcome.TimedOut:
                alive = Interlocked.Increment(ref _timedOutInARow) <= MaxTimedOutProbesInARow;
                break;
        }

        _last = new LivenessVerdict(nowUtc, outcome, alive);
        return alive;
    }
}
