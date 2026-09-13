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
/// measured on the population it drains (13.6 hours to 144.6 days on the fleet's outlier server), so an
/// hourly pass is not racing cache eviction.</para>
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
/// </summary>
internal static class OversizedPlanBacklogSweep
{
    /// <summary>
    /// How often one pass runs. Hourly: far below the shortest compile age measured on the plans this drains
    /// (13.6 hours), so the handles are still resolvable, and far above any cadence that would make the pass
    /// worth thinking about as load — at <see cref="MaxPlansPerServerPerTick"/> it is at most three single-row
    /// DMV reads per server per hour.
    /// </summary>
    internal static readonly TimeSpan SweepInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// Plans one pass may fetch for one server. Three, and the number is the RATE bound; the per-fetch
    /// isolation above is the MEMORY bound, and the two are independent — raising this would not batch
    /// anything, it would take longer. The measured population is 13 over-cap plans at once on the fleet's
    /// worst server, so three per hour clears a worst-case backlog in a few hours and a normal one in one
    /// pass.
    /// </summary>
    internal const int MaxPlansPerServerPerTick = 3;

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

    /// <summary>One pass over the fleet. Never throws; a server that fails is logged and the pass continues.</summary>
    /// <param name="servers">
    /// The tick's stable server snapshot. Entries with no live runtime are skipped — a server that is not
    /// connected has no connection to borrow and will be here again next hour.
    /// </param>
    internal static async Task RunAsync(
        NpgsqlDataSource postgres,
        IReadOnlyList<ServerRuntime> servers,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(servers);

        foreach (var server in servers)
        {
            if (cancellationToken.IsCancellationRequested)
            {
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

            try
            {
                await SweepServerAsync(postgres, server, logger, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                /* Service shutdown ends the pass quietly. The caller tracks this task without awaiting it, so
                   a fault escaping here would be an UNOBSERVED exception rather than one anybody handles —
                   and a shutdown is not a fault. Outcomes already fetched were written on
                   CancellationToken.None, so nothing measured is discarded. */
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
                logger?.LogDebug(
                    "Oversized-plan backlog sweep on '{Server}' ended early: {Message}",
                    server.Config.DisplayName, ex.Message);
            }
        }
    }

    /// <summary>
    /// One server's pass: claim a small set, then fetch each claimed plan SEPARATELY. The loop is the only
    /// thing that iterates; every step inside it is single-plan.
    /// </summary>
    private static async Task SweepServerAsync(
        NpgsqlDataSource postgres,
        ServerRuntime server,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        var pending = await ClaimAsync(postgres, server.ServerId, cancellationToken).ConfigureAwait(false);
        if (pending.Count == 0)
        {
            return;
        }

        foreach (var plan in pending)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var (verdict, planXml, error) = await FetchOnePlanAsync(server, plan, logger, cancellationToken)
                .ConfigureAwait(false);

            await RecordOutcomeAsync(postgres, server, plan, verdict, planXml, logger).ConfigureAwait(false);

            switch (verdict)
            {
                case PlanFetchVerdict.Captured:
                    logger?.LogInformation(
                        "Oversized-plan backlog: captured a {Bytes}-byte {Collector} plan on '{Server}' that exceeded the {Cap}-byte capture cap (#3392)",
                        plan.Observation.ObservedBytes, plan.CollectorName, server.Config.DisplayName,
                        QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes);
                    break;

                case PlanFetchVerdict.Expired:
                    logger?.LogDebug(
                        "Oversized-plan backlog: the {Collector} plan handle for a {Bytes}-byte plan on '{Server}' no longer renders a plan — recorded as expired (#3392)",
                        plan.CollectorName, plan.Observation.ObservedBytes, server.Config.DisplayName);
                    break;

                default:
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
               MaxPlansPerServerPerTick per server per hour. The cap's problem was two hundred of these inside
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
