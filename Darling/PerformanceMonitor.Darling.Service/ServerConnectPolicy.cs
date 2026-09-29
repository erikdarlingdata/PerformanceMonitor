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
/// connection with a short timeout: the runtime is dropped only when the probe fails.
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
    /// True when a brand-new, unpooled connection to the runtime's server logs in and answers SELECT 1
    /// inside <see cref="ProbeSeconds"/> seconds. Unpooled so a hung server cannot answer from a pooled
    /// session that was already open.
    /// </summary>
    internal static async Task<bool> ProbeFreshConnectionAsync(ServerRuntime runtime, CancellationToken cancellationToken)
    {
        if (runtime.Target.Engine != CollectorTargetEngine.SqlServer)
        {
            return true;
        }

        try
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
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
