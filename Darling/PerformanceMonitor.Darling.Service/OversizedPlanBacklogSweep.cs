/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Targets;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Drains <c>collect.oversized_plan_backlog</c> (#3392): the low-frequency, deliberately
/// one-plan-at-a-time errand that goes back for the cached plans
/// <see cref="QueryPlanXmlCaptureLimits"/> declined to capture.
///
/// <para><b>ONE PLAN PER CONNECTION PER FETCH, and that is the whole design, not a starting cadence.</b>
/// Several large plan fetches in one result set is precisely the shape the cap exists to prevent: the cost is
/// not the target's execution and not the wire, it is the CLIENT materializing each plan as one managed
/// string, whose Large-Object-Heap churn produces process-wide GC pauses that stall unrelated collectors on
/// unrelated connections (the mechanism <see cref="QueryPlanXmlCaptureLimits"/> documents and measured). So
/// "make it faster by fetching a few at once" is not an optimization here, it is the regression. The
/// structure enforces it rather than asking: <see cref="FetchSql"/> is a single-row
/// <c>sys.dm_exec_text_query_plan</c> call taking three SCALAR parameters — there is no set-valued input to
/// widen — and <see cref="FetchOnePlanAsync"/> is the only method here that touches a monitored server, takes
/// exactly one plan, and opens and disposes its OWN connection, so N plans cost N connections by
/// construction. <c>OversizedPlanBacklogPins</c> holds each of those properties.</para>
///
/// <para><b>Its own cadence, off the host loop, never the collector rotation.</b> Invoked from
/// <c>DarlingWorker</c>'s fleet-level cadence checks beside the daily purge — the
/// <see cref="DarlingRetention"/> shape — so it cannot compete with, queue behind, or extend a live
/// collection cycle's wall-clock budget. <see cref="SweepInterval"/> is generously below every compile age
/// measured on the population it drains (13.6 hours to 144.6 days on the fleet's outlier server), so a
/// pass every fifteen minutes is not racing cache eviction.</para>
///
/// <para><b>An empty backlog costs one indexed SELECT per server per tick.</b> A deployment that captures no
/// plans at all never records a sighting, so its backlog is permanently empty and its claim returns nothing —
/// there is no knob to turn off, because there is nothing to turn off.</para>
///
/// <para><b>Every outcome is a recorded state, never an exception.</b> Zero rows back means the handle no
/// longer renders a plan: <c>expired_at</c> is stamped and the sweep moves on, which the issue calls a benign
/// expected outcome for a best-effort backlog. A fetch that could not COMPLETE is deliberately not that — it
/// stamps only the attempt, so the row stays claimable. Conflating the two would retire a row over a
/// transient connect failure and recreate the blind spot this exists to close.</para>
///
/// <para><b>And every TICK is a recorded state too, including the empty ones (#3399).</b> The per-plan
/// verdicts above describe rows; they cannot describe a pass that claimed nothing, and a pass that claimed
/// nothing is the ordinary case on a deployment whose backlog is drained. So one
/// <c>collection_log</c> run-record lands per tick under the fleet sentinel
/// (<see cref="DarlingObservability.LogOversizedPlanSweepRunAsync"/>), whatever the tick found — which is
/// what makes an ABSENT row past <see cref="SweepInterval"/> mean the pass did not run, rather than meaning
/// nothing needed fetching. The write is in a <c>finally</c> and takes
/// <see cref="CancellationToken.None"/>, the <see cref="RecordOutcomeAsync"/> reasoning one level up: a tick
/// interrupted by shutdown still measured everything it did, and a run-record that is skipped on the paths
/// that end a pass early is a run-record whose absence no longer means anything.</para>
/// </summary>
internal static class OversizedPlanBacklogSweep
{
    /// <summary>
    /// How often one pass runs. Every fifteen minutes: far below the shortest compile age measured on the
    /// plans this drains (13.6 hours, which is fifty-four passes), so the handles are still resolvable many
    /// passes over, and with <see cref="MaxPlansPerServerPerTick"/> it is at most forty single-row DMV reads
    /// per server per hour — 1,680 across a 42-server fleet, against the ~1,400 new over-cap identities an
    /// hour measured on the store class whose arrival outruns its drain.
    ///
    /// <para><b>It is also the sentinel run-record rate</b>, since every tick writes one
    /// <c>collection_log</c> row under the fleet sentinel whatever it found (#3399): four an hour, 96 a day,
    /// held for <see cref="DarlingRetention.CollectionLogRetentionDays"/> days. Shortening it buys drain with
    /// rows in the one table retention has to prune, and eats into the connect-wait budget below, which only
    /// shortens anything while <see cref="ConnectWaitDelay"/> times <see cref="MaxConnectWaitAttempts"/>
    /// stays a minority of one interval.</para>
    /// </summary>
    internal static readonly TimeSpan SweepInterval = TimeSpan.FromMinutes(15);

    /// <summary>
    /// How long the gate waits before RE-ATTEMPTING a tick whose target list came back empty on a host that
    /// HAS sweepable targets — the cold-start case, where the registrations exist and simply have not
    /// finished connecting yet.
    ///
    /// <para>One minute is four passes of the fleet loop's own 15-second cadence, and that ratio is the
    /// property that matters rather than the absolute value: a re-attempt scheduled at or below the loop's
    /// cadence re-fires the gate on every pass until the first connect, and every one of those passes writes
    /// a run-record. So this is deliberately coarse relative to the loop instead of as small as it could be.</para>
    ///
    /// <para>Sized from the measured cold start — the first tick landed 1m50s after service start and the
    /// fleet's servers began connecting 30 seconds after that — so one minute clears the observed connect
    /// latency with room, and the FIRST re-attempt is the one that finds a runtime.</para>
    /// </summary>
    internal static readonly TimeSpan ConnectWaitDelay = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How many consecutive <see cref="ConnectWaitDelay"/> re-attempts the gate may spend waiting for a
    /// first runtime before falling back to <see cref="SweepInterval"/>. Five, so the wait covers five
    /// minutes — about ten times the measured connect latency — for at most five extra run-records, and a
    /// third of one <see cref="SweepInterval"/>. That ratio is the bound worth watching rather than the
    /// count: a budget covering a whole interval is that interval under another name, and the gate would
    /// have no short path left to take.
    ///
    /// <para><b>Spent, not reset, when it runs out.</b> <see cref="NextSweepDelay"/> HOLDS the count at this
    /// maximum rather than clearing it, so a host whose SQL Server targets never connect at all pays the
    /// burst once per process and then writes exactly one run-record per interval forever. Clearing it there
    /// would turn a permanently unreachable fleet into a fresh burst every interval, which is the row-growth
    /// objection to a plain short retry interval.</para>
    /// </summary>
    internal const int MaxConnectWaitAttempts = 5;

    /// <summary>
    /// Plans one pass may fetch for one server. Ten, and the number is the RATE bound; the per-fetch
    /// isolation above is the MEMORY bound, and the two are independent — raising this would not batch
    /// anything, it would take longer.
    ///
    /// <para><b>A ceiling on a LIMIT, not a quota.</b> <c>OversizedPlanBacklog.ClaimSql</c> takes at most
    /// this many rows, so a server whose backlog is drained claims what is there and its tick costs one
    /// indexed store SELECT and no target work at all. That is why the two measured store classes — one
    /// arriving at a handful of long-lived cache residents per server, one at ~1,400 new identities an hour
    /// fleet-wide — need no separate values: the same ceiling produces the rate each one's own backlog depth
    /// asks for.</para>
    ///
    /// <para><b>What it costs at the top of the range.</b> One server's pass is serial under
    /// <see cref="PerPlanBudget"/>, so the worst case for one monitored server is ten times fifteen seconds,
    /// 150 seconds; the fleet pass is serial too, so 42 servers all timing out at the cap is 105 minutes.
    /// Two passes never overlap whatever the interval — <c>DarlingWorker</c> tracks the pass and its gate
    /// will not launch on top of an incomplete one — so a pass that outruns <see cref="SweepInterval"/> runs
    /// back-to-back with its successor at the SAME one-plan-at-a-time load rather than at two passes' worth
    /// of concurrent fetches. That is the bound which matters, and it is the per-fetch isolation above that
    /// provides it. The measured pass is nowhere near any of this: 126 plans in 6,155 ms, ~49 ms each.</para>
    /// </summary>
    internal const int MaxPlansPerServerPerTick = 10;

    /// <summary>
    /// WALL-CLOCK ceiling for one plan's fetch — connect, execute AND drain. It is the binding constraint,
    /// for the reason <c>ICollectorSchemaInfo.PerItemWallClockBudget</c> records: a command timeout bounds the
    /// wait for a network read and SqlClient RESETS it on every read that arrives, so a large result that
    /// trickles never trips it. Fifteen seconds is the top of the issue's range, and a plan that cannot be
    /// read in fifteen seconds on the collector's own same-region path is one this pass should abandon and
    /// re-attempt next hour rather than hold a connection for.
    /// </summary>
    internal static readonly TimeSpan PerPlanBudget = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The provider-level twin of <see cref="PerPlanBudget"/>, set rather than inherited so a stalled EXECUTE
    /// surfaces as a driver error naming the timeout instead of as a cancellation. Equal to the budget on
    /// purpose: it can only ever fire at or before the wall clock, so it never widens the bound.
    /// </summary>
    internal const int FetchCommandTimeoutSeconds = 15;

    /// <summary>
    /// The fetch: ONE row, ONE plan, three scalar parameters.
    ///
    /// <para>The parameters are the values the collector's own plan apply passed — the statement offsets for
    /// <c>query_stats</c>, the module-grain literals for <c>procedure_stats</c> — so this is the same call
    /// that produced the measurement, not a re-derivation of it. A <c>plan_handle</c> with the wrong offsets
    /// is a different statement's plan.</para>
    ///
    /// <para>The handle round-trips through <c>CONVERT(varbinary(64), @plan_handle, 1)</c> from the
    /// <c>varchar(130)</c> hex form the backlog stores, the same conversion <c>procedure_stats</c>' own apply
    /// makes. It binds as <c>NVarChar260</c> and NOT <c>NVarChar128</c>: the hex form is 130 characters, and a
    /// truncated handle converts cleanly and then matches nothing — which this code would read as an evicted
    /// plan and stamp as an expiry, silently, on every row.</para>
    ///
    /// <para><c>query_plan</c> is NULL rather than absent for some plans the DMV can find but not render, so
    /// the predicate collapses both misses into "no rows" and there is one outcome to handle instead of
    /// two.</para>
    ///
    /// <para>No <c>OPTION(RECOMPILE)</c>, unlike the collectors: there is no parameter-sensitive ranking here
    /// to protect, and the cached plan for a three-scalar TVF call is the one this wants reused.</para>
    /// </summary>
    internal const string FetchSql = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    tqp.query_plan
FROM sys.dm_exec_text_query_plan
     (
         CONVERT(varbinary(64), @plan_handle, 1),
         @statement_start_offset,
         @statement_end_offset
     ) AS tqp
WHERE tqp.query_plan IS NOT NULL;";

    /// <summary>What one fetch established.</summary>
    internal enum PlanFetchVerdict
    {
        /// <summary>The plan came back and is stored.</summary>
        Captured,

        /// <summary>The handle no longer renders a plan — a benign, expected end state.</summary>
        Expired,

        /// <summary>The fetch could not complete. Nothing was learned; the row stays claimable.</summary>
        Failed,
    }

    /// <summary>
    /// What one tick did, accumulated across the fleet pass so the run-record can report it (#3399).
    ///
    /// <para>Mutable and passed down rather than returned up, because the per-server step is failure-isolated:
    /// a server that faults mid-pass has already contributed captures and expiries, and a return value would
    /// discard them on exactly the tick whose counts matter most. Not thread-safe and does not need to be —
    /// the pass is a serial <c>foreach</c> by design, since concurrent plan fetches are the regression the
    /// whole class is shaped to prevent.</para>
    /// </summary>
    internal sealed class SweepTally
    {
        /// <summary>Servers the pass REACHED — incremented before the attempt, so it is the denominator
        /// <see cref="ServersFaulted"/> is read against rather than a count of clean ones.</summary>
        internal int ServersSwept { get; set; }

        /// <summary>Servers whose pass ended on an unexpected fault (logged and isolated per server).</summary>
        internal int ServersFaulted { get; set; }

        /// <summary>Backlog rows the claims returned across the fleet.</summary>
        internal int PlansClaimed { get; set; }

        /// <summary>Plans fetched and stored — what <c>rows_collected</c> reports.</summary>
        internal int PlansCaptured { get; set; }

        /// <summary>Handles that no longer render a plan, stamped <c>expired_at</c>.</summary>
        internal int PlansExpired { get; set; }

        /// <summary>Fetches that could not complete. The rows stay claimable, so this is a retry count.</summary>
        internal int FetchFailures { get; set; }

        /// <summary>Milliseconds inside <see cref="FetchOnePlanAsync"/> — what <c>sql_duration_ms</c> means.</summary>
        internal long TargetMs { get; set; }

        /// <summary>Whether shutdown ended the pass before it reached every server.</summary>
        internal bool Interrupted { get; set; }
    }

    /// <summary>
    /// The status and the human summary for one tick's run-record. SUCCESS when every fetch and every
    /// server's pass completed; WARNING when a fetch failed, a server faulted, or shutdown cut the pass
    /// short — the <see cref="DarlingRetention.BuildRunRecordSummary"/> vocabulary, so the sentinel
    /// population reads with one set of words rather than two.
    ///
    /// <para>The four counts the outcome is made of are emitted UNCONDITIONALLY, empty tick included, so
    /// every row of this collector says the same things in the same order and a zero is a measurement rather
    /// than an omission. The fault and interrupt clauses are appended only when they happened, which is where
    /// this follows the retention summary rather than leading with noise.</para>
    ///
    /// <para>Pure, so the branch and the text are pinnable without a store or a fleet.</para>
    /// </summary>
    internal static (string Status, string Message) BuildRunRecordSummary(SweepTally tally)
    {
        ArgumentNullException.ThrowIfNull(tally);

        var status = tally.FetchFailures == 0 && tally.ServersFaulted == 0 && !tally.Interrupted
            ? "SUCCESS"
            : "WARNING";

        var message =
            $"Swept {tally.ServersSwept.ToString(CultureInfo.InvariantCulture)} server(s): "
            + $"{tally.PlansClaimed.ToString(CultureInfo.InvariantCulture)} plan(s) claimed, "
            + $"{tally.PlansCaptured.ToString(CultureInfo.InvariantCulture)} captured, "
            + $"{tally.PlansExpired.ToString(CultureInfo.InvariantCulture)} expired, "
            + $"{tally.FetchFailures.ToString(CultureInfo.InvariantCulture)} fetch failure(s)";

        if (tally.ServersFaulted > 0)
        {
            message += $", {tally.ServersFaulted.ToString(CultureInfo.InvariantCulture)} server(s) faulted (see prior warnings)";
        }

        if (tally.Interrupted)
        {
            message += "; interrupted by service shutdown";
        }

        return (status, message);
    }

    /// <summary>
    /// Whether one monitored-server registration could EVER carry a runtime this sweep would visit — the
    /// pre-connect half of the per-server skip inside <see cref="RunAsync"/>, which asks the same question
    /// of a runtime that already exists.
    ///
    /// <para><b>It reads the registration, not the runtime, and that is the whole point.</b> The gate has to
    /// separate "no target has connected yet" from "this host has no sweepable target at all", and a runtime
    /// answers neither — <c>ServerLoopState.Runtime</c> is null in both cases. <c>MonitoredServer.IsPostgres</c>
    /// is the only fact available before a connect that settles it, and it settles it exactly:
    /// <c>DarlingServerConnector.ConnectAsync</c> branches on that same property and only its PostgreSQL arm
    /// stamps <c>CollectorTargetInfo.Engine</c>, so a registration this admits produces a runtime whose
    /// engine is SQL Server, and one it rejects produces a runtime <see cref="RunAsync"/> skips.</para>
    ///
    /// <para>Total, with no third state: <c>MonitoredServer.TargetEngine</c> folds an absent or misspelled
    /// engine token to SQL Server rather than throwing, so a typo counts as sweepable here — the direction
    /// that matches what the connector will then actually do with it.</para>
    /// </summary>
    internal static bool IsSweepableTarget(MonitoredServer config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return !config.IsPostgres;
    }

    /// <summary>
    /// When the next tick is due and what is left of the connect-wait budget — the whole schedule decision,
    /// pure, so the branch is pinnable without a worker, a store or a fleet.
    ///
    /// <para><b>Two causes of an empty target list, meaning opposite things.</b> A tick with sweepable
    /// targets and no connected runtime among them is TRANSIENT: those registrations are mid-connect and
    /// will carry a runtime within a minute, so a full <see cref="SweepInterval"/> spends a whole slot of
    /// draining on a pass that reached nothing. A tick with NO sweepable target is PERMANENT — a
    /// PostgreSQL-only monitoring host, where the cap, the backlog and this sweep all live in the SQL Server
    /// plan-XML collectors, so there is nothing to sweep and never will be. That host takes the full
    /// interval and writes exactly one run-record per interval, because a re-attempt there is a retry loop
    /// with no terminating condition.</para>
    ///
    /// <para>Any connected target at all ends the wait and clears the budget, so what a restart costs is
    /// bounded by how fast the FIRST server connects rather than by the whole fleet: on the measured fleet
    /// that is one extra run-record, not <see cref="MaxConnectWaitAttempts"/> of them.</para>
    /// </summary>
    /// <param name="sweepableTargets">Registrations <see cref="IsSweepableTarget"/> admits, connected or not.</param>
    /// <param name="connectedTargets">Those of them carrying a runtime now — what this pass will visit.</param>
    /// <param name="connectWaitAttempts">Re-attempts already spent waiting for a first runtime.</param>
    internal static (TimeSpan Delay, int ConnectWaitAttempts) NextSweepDelay(
        int sweepableTargets, int connectedTargets, int connectWaitAttempts)
    {
        if (sweepableTargets <= 0 || connectedTargets > 0)
        {
            return (SweepInterval, 0);
        }

        return connectWaitAttempts < MaxConnectWaitAttempts
            ? (ConnectWaitDelay, connectWaitAttempts + 1)
            : (SweepInterval, connectWaitAttempts);
    }

    /// <summary>One pass over the fleet. Never throws; a server that fails is logged and the pass continues.</summary>
    /// <param name="servers">
    /// The connected runtimes from the tick's stable server snapshot. A registration with no live runtime
    /// has no connection to borrow, so the gate leaves it out; whether it is worth waiting a minute for one
    /// is <see cref="NextSweepDelay"/>'s question, not this pass's.
    /// </param>
    internal static async Task RunAsync(
        NpgsqlDataSource postgres,
        IReadOnlyList<ServerRuntime> servers,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(servers);

        var tally = new SweepTally();
        var sw = Stopwatch.StartNew();

        try
        {
            foreach (var server in servers)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    tally.Interrupted = true;
                    return;
                }

                /* The engine gate lives HERE, at the boundary a query would cross, rather than resting on the
                   backlog never holding a PostgreSQL server's rows — the PlanForceBot's reasoning. Only the two
                   SQL Server collectors describe observations, so a PostgreSQL row is unreachable today, and this
                   is what keeps it unreachable if that ever stops being true. */
                if (server.Config.IsPostgres || server.Target.Engine != CollectorTargetEngine.SqlServer)
                {
                    continue;
                }

                /* Counted BEFORE the attempt, so it is the number of servers this tick REACHED. Counting only
                   clean passes would make the run-record's fault clause a fraction of itself. */
                tally.ServersSwept++;

                try
                {
                    await SweepServerAsync(postgres, server, tally, logger, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    /* Service shutdown ends the pass quietly. The caller tracks this task without awaiting it, so
                       a fault escaping here would be an UNOBSERVED exception rather than one anybody handles —
                       and a shutdown is not a fault. Outcomes already fetched were written on
                       CancellationToken.None, so nothing measured is discarded. */
                    tally.Interrupted = true;
                    return;
                }
                catch (Exception ex)
                {
                    /* Failure-isolated per server: this pass is launched and not awaited, so an escaping fault
                       would be an UNOBSERVED exception rather than one anybody handles — and the fleet would
                       lose the whole tick's sweep with nothing logged.

                       Unfiltered on purpose, unlike the usual `ex is not OperationCanceledException` arm. That
                       filter exists so a shutdown is never swallowed as a fault, and the arm ABOVE already
                       answers the shutdown case on the token. What is left is a cancellation whose token is not
                       ours, which is not a shutdown and so IS a per-server fault — filtered out, it would be
                       the one exception shape that escapes. */
                    tally.ServersFaulted++;

                    logger?.LogDebug(
                        "Oversized-plan backlog sweep on '{Server}' ended early: {Message}",
                        server.Config.DisplayName, ex.Message);
                }
            }
        }
        finally
        {
            /* ONE run-record per tick, on every exit path (#3399) — the early returns above are the
               interesting ones, because a record written only after a complete pass is a record whose
               absence means either "the sweep is dead" or "the last pass was interrupted", and those need
               opposite responses. CancellationToken.None for the RecordOutcomeAsync reason: the tick's
               measurements are already paid for, so a shutdown arriving here must not discard them.
               Failure-isolated inside the writer, so this cannot fault the task the worker tracks. */
            sw.Stop();

            var (status, message) = BuildRunRecordSummary(tally);
            await DarlingObservability.LogOversizedPlanSweepRunAsync(
                postgres, status, tally.PlansCaptured, sw.ElapsedMilliseconds, tally.TargetMs, message,
                logger, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// One server's pass: claim a small set, then fetch each claimed plan SEPARATELY. The loop is the only
    /// thing that iterates; every step inside it is single-plan.
    /// </summary>
    private static async Task SweepServerAsync(
        NpgsqlDataSource postgres,
        ServerRuntime server,
        SweepTally tally,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        var pending = await ClaimAsync(postgres, server.ServerId, cancellationToken).ConfigureAwait(false);
        tally.PlansClaimed += pending.Count;

        if (pending.Count == 0)
        {
            return;
        }

        foreach (var plan in pending)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                tally.Interrupted = true;
                return;
            }

            /* Timed around the fetch ALONE, which is the only step here that touches a monitored server —
               so the sum is what sql_duration_ms claims to be on the run-record, and the claim and the
               outcome write fall to the storage phase where they belong. */
            var fetchStart = Stopwatch.GetTimestamp();

            var (verdict, planXml, error) = await FetchOnePlanAsync(server, plan, logger, cancellationToken)
                .ConfigureAwait(false);

            tally.TargetMs += (long)Stopwatch.GetElapsedTime(fetchStart).TotalMilliseconds;

            await RecordOutcomeAsync(postgres, server, plan, verdict, planXml, logger).ConfigureAwait(false);

            switch (verdict)
            {
                case PlanFetchVerdict.Captured:
                    tally.PlansCaptured++;
                    logger?.LogInformation(
                        "Oversized-plan backlog: captured a {Bytes}-byte {Collector} plan on '{Server}' that exceeded the {Cap}-byte capture cap (#3392)",
                        plan.Observation.ObservedBytes, plan.CollectorName, server.Config.DisplayName,
                        QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes);
                    break;

                case PlanFetchVerdict.Expired:
                    tally.PlansExpired++;
                    logger?.LogDebug(
                        "Oversized-plan backlog: the {Collector} plan handle for a {Bytes}-byte plan on '{Server}' no longer renders a plan — recorded as expired (#3392)",
                        plan.CollectorName, plan.Observation.ObservedBytes, server.Config.DisplayName);
                    break;

                default:
                    tally.FetchFailures++;
                    logger?.LogDebug(
                        "Oversized-plan backlog: the {Collector} fetch for a {Bytes}-byte plan on '{Server}' did not complete ({Message}) — the row stays claimable (#3392)",
                        plan.CollectorName, plan.Observation.ObservedBytes, server.Config.DisplayName, error);
                    break;
            }
        }
    }

    /// <summary>
    /// This server's next few un-captured rows, oldest ATTEMPT first. Reads only; the claim takes no lock and
    /// marks nothing, because the sweep is the single writer on this path and a lock held across a
    /// target fetch is the one shape a bound must never take.
    /// </summary>
    private static async Task<List<OversizedPlanBacklog.PendingPlan>> ClaimAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken)
    {
        var claimed = new List<OversizedPlanBacklog.PendingPlan>(MaxPlansPerServerPerTick);

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            OversizedPlanBacklog.ClaimSql(MaxPlansPerServerPerTick), connection)
        {
            CommandTimeout = OversizedPlanBacklog.CommandTimeoutSeconds,
        };

        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            claimed.Add(new OversizedPlanBacklog.PendingPlan(
                reader.GetString(0),
                new OversizedPlanObservation(
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetInt32(3),
                    reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    QueryHash: null,
                    reader.GetInt64(6))));
        }

        return claimed;
    }

    /// <summary>
    /// ONE plan, on a connection of its own, under its own budget. This is the only method here that reaches a
    /// monitored server, and the connection is opened and disposed inside it — so the per-fetch isolation the
    /// class doc rests on is a property of this signature, not of how carefully a caller loops.
    /// </summary>
    private static async Task<(PlanFetchVerdict Verdict, string? PlanXml, string? Error)> FetchOnePlanAsync(
        ServerRuntime server,
        OversizedPlanBacklog.PendingPlan plan,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(PerPlanBudget);

        try
        {
            var provider = TargetProviders.For(server.Target);

            await using var connection = provider.CreateConnection(server.ConnectionString);
            await connection.OpenAsync(budget.Token).ConfigureAwait(false);

            using var command = provider.CreateCommand(
                new CollectorQuery(FetchSql, new[]
                {
                    /* NVarChar260, never NVarChar128 — see FetchSql: the hex handle is 130 characters and a
                       truncated one matches nothing, which would read as an expiry. */
                    new CollectorParameter("@plan_handle", plan.Observation.PlanHandle, CollectorParameterType.NVarChar260),
                    new CollectorParameter("@statement_start_offset", plan.Observation.StatementStartOffset, CollectorParameterType.Int32),
                    new CollectorParameter("@statement_end_offset", plan.Observation.StatementEndOffset, CollectorParameterType.Int32),
                }),
                connection,
                FetchCommandTimeoutSeconds);

            await using var reader = await command.ExecuteReaderAsync(budget.Token).ConfigureAwait(false);

            if (!await reader.ReadAsync(budget.Token).ConfigureAwait(false) || await reader.IsDBNullAsync(0, budget.Token).ConfigureAwait(false))
            {
                return (PlanFetchVerdict.Expired, null, null);
            }

            /* One managed string, up to a few megabytes, once per fetch. That allocation is the thing the
               capture cap exists to bound — and it is bounded here by the shape rather than by a size: one
               plan at a time, on a connection nothing else is using, off the collection path, at most
               MaxPlansPerServerPerTick per server per pass. The cap's problem was two hundred of these inside
               a cycle's drain, not one of them. */
            var planXml = reader.GetString(0);

            return (PlanFetchVerdict.Captured, planXml, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || budget.IsCancellationRequested)
        {
            /* Classified on the TOKEN, not the exception type, the StallWaitProbeRunner reasoning: cancelling
               a SqlClient operation does not reliably surface as OperationCanceledException, and a service
               shutdown must never be recorded as this fetch's own outcome. A shutdown re-throws. */
            if (cancellationToken.IsCancellationRequested)
            {
                throw;
            }

            logger?.LogDebug(
                "Oversized-plan backlog: a {Collector} plan fetch on '{Server}' failed after at most {Budget}s: {Message} (#3392)",
                plan.CollectorName, server.Config.DisplayName,
                PerPlanBudget.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture), ex.Message);

            return (PlanFetchVerdict.Failed, null, ex.Message);
        }
    }

    /// <summary>
    /// Stamps one row's outcome. Runs on <see cref="CancellationToken.None"/> for the reason the stall probe's
    /// store write does: the target query is already paid for, so a shutdown arriving between the fetch and
    /// the write would discard content that cost a real DMV read. Never throws.
    /// </summary>
    private static async Task RecordOutcomeAsync(
        NpgsqlDataSource postgres,
        ServerRuntime server,
        OversizedPlanBacklog.PendingPlan plan,
        PlanFetchVerdict verdict,
        string? planXml,
        ILogger? logger)
    {
        var sql = verdict switch
        {
            PlanFetchVerdict.Captured => OversizedPlanBacklog.RecordCaptureSql,
            PlanFetchVerdict.Expired => OversizedPlanBacklog.RecordExpirySql,
            _ => OversizedPlanBacklog.RecordAttemptSql,
        };

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(sql, connection)
            {
                CommandTimeout = OversizedPlanBacklog.CommandTimeoutSeconds,
            };

            /* $1..$6 are the primary key, $7 the attempt stamp, $8 the content when there is any. Naive-UTC
               storage — see PgCollectorRowWriter. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server.ServerId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = plan.CollectorName });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = plan.Observation.PlanHandle });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = plan.Observation.SqlHandle });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = plan.Observation.StatementStartOffset });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = plan.Observation.StatementEndOffset });
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Timestamp,
                Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
            });

            if (verdict == PlanFetchVerdict.Captured)
            {
                command.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlDbType.Text,
                    Value = PgCollectorRowWriter.StripEmbeddedNuls(planXml ?? string.Empty),
                });
            }

            await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogDebug(
                "Oversized-plan backlog: the {Verdict} outcome for a {Collector} plan on '{Server}' was not stored: {Message}",
                verdict, plan.CollectorName, server.Config.DisplayName, ex.Message);
        }
    }
}
