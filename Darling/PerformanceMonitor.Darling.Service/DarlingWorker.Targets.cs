/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using ServerClock = PerformanceMonitor.Analysis.Baselines.ServerClock;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingWorker
{
    /// <summary>
    /// The engine's live-msdb failed-jobs feed: runs the shared <see cref="FailedJobsQuery"/> on
    /// the monitored server's own connection. Gated !IsAzureSqlDb (the engine also gates on the
    /// snapshot; there is deliberately NO msdb-access probe — Phase-5 review F11) and degrades
    /// exactly like the Dashboard's caller: a login that cannot SELECT the msdb job tables
    /// raises SqlException 229/297/300/916 → Info + empty list; any other failure → Warning +
    /// empty list — a permission gap or transient error never fails the alert cycle.
    /// </summary>
    private async Task<List<FailedJobInfo>> FetchFailedJobsAsync(
        List<ServerLoopState> servers, string serverKey, int lookbackMinutes, CancellationToken cancellationToken)
    {
        /* Lookup only — held briefly under the lock (the command loop reconciles the list concurrently),
           then the connection I/O runs outside it. */
        ServerRuntime? runtime;
        lock (_serversLock)
        {
            runtime = servers
                .Select(s => s.Runtime)
                .FirstOrDefault(r => r is not null
                    && string.Equals(r.ServerId.ToString(CultureInfo.InvariantCulture), serverKey, StringComparison.Ordinal));
        }

        /* Engine first: msdb, SQL Agent and the whole FailedJobsQuery are SQL Server concepts, and this
           opens a SqlConnection below. On a PostgreSQL target it threw "Keyword not supported: 'host'" once
           per alert cycle. The IsAzureSqlDb arm stays for the same reason it always did — Azure SQL DB has
           no msdb either. */
        if (runtime is null
            || runtime.Target.Engine != CollectorTargetEngine.SqlServer
            || runtime.Target.IsAzureSqlDb)
        {
            return new List<FailedJobInfo>();
        }

        try
        {
            using var connection = new SqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            using var command = new SqlCommand(FailedJobsQuery.Sql, connection) { CommandTimeout = 10 };
            command.Parameters.Add(new SqlParameter(FailedJobsQuery.LookbackMinutesParameter, SqlDbType.Int) { Value = lookbackMinutes });
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            return await FailedJobsQuery.ReadAsync(reader, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* #2512: routed through the shared set rather than a fourth copy of it — this one had
               916 but neither 262 nor 8189, which is the drift the set exists to end. Widening is safe
               in the only direction that matters here: every number in it means the login cannot read
               what it asked for, and the response is to return no jobs rather than fail the alert
               cycle. A 262 naming msdb is exactly this case and used to fall through to the warning.
               Expected for read-only monitoring accounts; hit every alert cycle, so Info. The named
               remedy is direct table SELECTs, NOT SQLAgentReaderRole: that role gates the sp_help_job*
               interface only and confers nothing on the base tables this query reads — a #1823 field
               box had the role and still landed here every cycle. */
            _logger.LogInformation("[{Server}] Skipping recently-failed-job check (needs SELECT on msdb.dbo.sysjobs and sysjobhistory — SQLAgentReaderRole alone is not enough; see the monitoring-login grants in the README): {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: this reads the MONITORED SERVER's msdb over its own
               connection and its own timeout, not the store over the alert pass's deadline. It is a swallowed alert
               read, but not one #3013's mechanism can produce, and pooling the two would put a target-side outage
               in a number the operator reads as store contention. */
            _logger.LogWarning("[{Server}] Recently-failed-job check errored: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
        }
    }

    /// <summary>
    /// The engine's #3497 Agent-job name lookup: one <see cref="AgentJobStepQuery"/> round trip against
    /// the monitored server's msdb for the (job, step) pairs a firing Long-Running Query card is about to
    /// show. Gated and degraded exactly like <see cref="FetchFailedJobsAsync"/> one method up, because it
    /// is the same kind of read against the same tables' neighborhood: engine-gated first (msdb, SQL Agent
    /// and the job-step program_name are SQL Server concepts — a PostgreSQL target must not pay for a
    /// SqlConnection it can never open), no Azure SQL DB (no msdb there), permission-denied → Info + empty
    /// map (expected for read-only monitoring logins; the alert cycle proceeds and the card renders the
    /// unresolved form with the raw job-id marker — ANNOTATION, NEVER SUPPRESSION: the card itself already
    /// fired before this read ran), anything else → Warning + empty map.
    /// </summary>
    private async Task<IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames>> FetchAgentJobStepNamesAsync(
        List<ServerLoopState> servers, string serverKey, List<AgentJobStepKey> keys, CancellationToken cancellationToken)
    {
        /* Lookup only — held briefly under the lock (the command loop reconciles the list concurrently),
           then the connection I/O runs outside it. */
        ServerRuntime? runtime;
        lock (_serversLock)
        {
            runtime = servers
                .Select(s => s.Runtime)
                .FirstOrDefault(r => r is not null
                    && string.Equals(r.ServerId.ToString(CultureInfo.InvariantCulture), serverKey, StringComparison.Ordinal));
        }

        if (keys.Count == 0
            || runtime is null
            || runtime.Target.Engine != CollectorTargetEngine.SqlServer
            || runtime.Target.IsAzureSqlDb)
        {
            return new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        }

        try
        {
            using var connection = new SqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            using var command = new SqlCommand(AgentJobStepQuery.BuildSql(keys.Count), connection) { CommandTimeout = 10 };
            for (int i = 0; i < keys.Count; i++)
            {
                /* Bound as uniqueidentifier so the ENGINE's type system does the compare — the byte-order
                   conversion already happened once, in AgentJobStepQuery.TryParseProgramName, and binding a
                   string here would invite a second, divergent spelling of it. */
                command.Parameters.Add(new SqlParameter(AgentJobStepQuery.JobIdParameter(i), SqlDbType.UniqueIdentifier) { Value = keys[i].JobId });
                command.Parameters.Add(new SqlParameter(AgentJobStepQuery.StepIdParameter(i), SqlDbType.Int) { Value = keys[i].StepId });
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await AgentJobStepQuery.ReadAsync(reader, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* Same shared set, same reasoning, same remedy naming as the failed-jobs arm: every number in
               it means the login cannot read what it asked for, the response is to answer nothing rather
               than fail the alert cycle, and SQLAgentReaderRole is NOT the remedy (it gates sp_help_job*
               only). Info because a read-only monitoring login hits this on every Agent-annotated fire. */
            _logger.LogInformation("[{Server}] Skipping the Agent job-name lookup for the Long-Running Query card (needs SELECT on msdb.dbo.sysjobs and sysjobsteps — SQLAgentReaderRole alone is not enough; see the monitoring-login grants in the README). The card renders the unresolved form: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter, for FetchFailedJobsAsync's exact reason: this
               reads the MONITORED SERVER's msdb, not the store, and pooling the two would put a target-side
               outage in a number the operator reads as store contention. */
            _logger.LogWarning("[{Server}] Agent job-name lookup for the Long-Running Query card errored — the card renders the unresolved form: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        }
    }

    /// <summary>How old a cached failed-jobs list may be and still be served (#5642). Older than this the body has
    /// stopped refreshing it (a stuck body, a dropped server), and an old list must not read as the current one.</summary>
    internal static readonly TimeSpan s_msdbCacheMaxAge = TimeSpan.FromMinutes(10);

    private static ServerLoopState? FindStateByServerKey(List<ServerLoopState> servers, string serverKey, object gate)
    {
        lock (gate)
        {
            return servers.FirstOrDefault(s => s.AlertRuntime is { } r
                && string.Equals(r.ServerId.ToString(CultureInfo.InvariantCulture), serverKey, StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// The engine's failed-jobs feed (#5642): the list the collection body last read from msdb, no I/O. Records the
    /// lookback the engine asked for so the body reads with it. Empty when nothing has been read yet or the list is
    /// older than <see cref="s_msdbCacheMaxAge"/>, the same degrade a denied or failed msdb read always gave.
    /// </summary>
    internal List<FailedJobInfo> ServeFailedJobsFromCache(List<ServerLoopState> servers, string serverKey, int lookbackMinutes)
    {
        var state = FindStateByServerKey(servers, serverKey, _serversLock);
        if (state is null)
        {
            return new List<FailedJobInfo>();
        }

        state.FailedJobsLookbackWanted = lookbackMinutes;
        var entry = state.FailedJobsCache;
        if (entry is null || DateTime.UtcNow - entry.FetchedUtc > s_msdbCacheMaxAge)
        {
            return new List<FailedJobInfo>();
        }

        return new List<FailedJobInfo>(entry.Jobs);
    }

    /// <summary>
    /// The engine's Agent-job name lookup (#5642), from the cache the body fills. A key not resolved yet is recorded as
    /// wanted for the body's next refresh and left out of the answer, so that card renders the unresolved form once
    /// (annotation, never suppression) and the next one carries the name.
    /// </summary>
    internal IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames> ServeStepNamesFromCache(
        List<ServerLoopState> servers, string serverKey, IReadOnlyList<AgentJobStepKey> keys)
    {
        var found = new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        var state = FindStateByServerKey(servers, serverKey, _serversLock);
        if (state is null)
        {
            return found;
        }

        foreach (var key in keys)
        {
            if (state.StepNamesCache.TryGetValue(key, out var names))
            {
                found[key] = names;
            }
            else
            {
                state.StepNamesWanted[key] = 0;
            }
        }

        return found;
    }

    /// <summary>
    /// Reads the two msdb facts the alert engine wants (#5642) into the server's cache: the recently failed jobs, with the
    /// lookback the engine last asked for, and the Agent job step names the cards asked for. Runs in the collection body
    /// after the collectors, at the alert cadence, so the fleet pass stays store reads only. Both reads keep their own
    /// degrade (permission denied or any failure gives an empty answer).
    /// </summary>
    private async Task RefreshMsdbCacheAsync(ServerLoopState server, CancellationToken cancellationToken)
    {
        var runtime = server.Runtime;
        if (runtime is null
            || runtime.Target.Engine != CollectorTargetEngine.SqlServer
            || runtime.Target.IsAzureSqlDb
            || !StampIsDue(server.NextMsdbRefresh, s_alertSweepInterval, DateTime.UtcNow))
        {
            return;
        }

        server.NextMsdbRefresh = DateTime.UtcNow.Add(s_alertSweepInterval);
        var one = new List<ServerLoopState> { server };
        var serverKey = runtime.ServerId.ToString(CultureInfo.InvariantCulture);

        if (server.FailedJobsLookbackWanted > 0)
        {
            var jobs = await FetchFailedJobsAsync(one, serverKey, server.FailedJobsLookbackWanted, cancellationToken);
            server.FailedJobsCache = new MsdbCacheEntry(DateTime.UtcNow, jobs);
        }

        if (!server.StepNamesWanted.IsEmpty)
        {
            var wanted = server.StepNamesWanted.Keys.Take(16).ToList();
            var names = await FetchAgentJobStepNamesAsync(one, serverKey, wanted, cancellationToken);
            if (server.StepNamesCache.Count > 1000)
            {
                server.StepNamesCache.Clear();
            }

            foreach (var pair in names)
            {
                server.StepNamesCache[pair.Key] = pair.Value;
            }

            foreach (var key in wanted)
            {
                server.StepNamesWanted.TryRemove(key, out _);
            }
        }
    }

    private async Task TryConnectAsync(ServerLoopState server, DarlingCollectorRunner runner, DarlingConfig config, ConnectAttempt? attempt, CancellationToken cancellationToken)
    {
        /* #4710: the attempt was made before the fleet gate (see ProcessServerSweepAsync), and the due check
           with it. No attempt means the server was not due for one, or was retired. */
        if (attempt is null)
        {
            return;
        }

        /* #4710: the attempt ran before the fleet permit, and a reload can replace the definition while it
           ran or while this body waited for the permit. A runtime built from the OLD definition must not be
           installed on an edited server: ReconcileServers already cleared Runtime and NextConnectAttempt for
           the edit, so nothing would reconnect and the server would keep collecting from the old target. A
           failed OLD attempt must not put the NEW definition into backoff either. ServerDefinitionEquals, not
           reference equality: a cost or alert-delivery edit replaces the held definition too, and that attempt
           is still good. The failure count is left alone (the edit's reconcile already reset it). */
        if (!ServerDefinitionEquals(server.Config, attempt.Value.Config))
        {
            _logger.LogInformation(
                "[{Server}] Definition changed during the connect attempt - connecting again with the new one",
                server.Config.DisplayName);
            server.NextConnectAttempt = DateTime.MinValue;
            return;
        }

        try
        {
            if (attempt.Value.Failure is { } connectFailure)
            {
                /* Rethrown into the catch below so a failed attempt takes the same path it always did. */
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(connectFailure).Throw();
            }

            var runtime = attempt.Value.Runtime!;

            /* #6771: the plan-capture setting is not part of ServerDefinitionEquals (an edit must not reconnect), so the
               attempt's runtime can carry the value from before an edit that landed while this body waited for the permit,
               and ReconcileServers had no runtime to re-stamp. The held definition is the current one: copy from it. */
            runtime.CapturePlans = server.Config.CapturePlans;
            server.Runtime = runtime;
            server.LastKnownRuntime = runtime;
            server.WatchingSinceUtc = DateTime.UtcNow;

            /* #4732: the definition comparison again, now that the runtime is on the state. ReconcileServers runs on
               the loop thread and clears Runtime when it replaces a definition, so an edit that lands between the check
               above and this assignment finds nothing to clear: this runtime, built from the old definition, would stay
               installed and nothing would reconnect it. An edit that lands later clears it, and the checks below (before
               the store writes, before the extended events DDL, before the first collector pass) each ask again so none
               of them runs against a connection string the server no longer has. */
            bool ConnectionIsStale() => !ConnectedRuntimeIsCurrent(server.Config, attempt.Value.Config, server.Runtime, runtime);
            if (ConnectionIsStale())
            {
                DiscardStaleConnection(server, runtime);
                return;
            }

            server.ConsecutiveConnectFailures = 0;

            /* #2255: cleared on success so a LATER failure prints in full even when it carries the same
               message as one from before this connect. Without this, a fixed-then-broken-again cause would be
               suppressed as a repeat of something the operator had already scrolled past. */
            server.LastConnectFailureLogged = null;

            /* #7067: how long the server was down and how many attempts it took, for the "Connected" line below. */
            var recoveryNote = TakeConnectRecoveryNote(server);

            /* #2228: the tripwire. The connection just told us which database it actually landed in, so this
               is the one moment the registration's claim can be checked against the server's own answer.
               Identity is registration-derived and never verified against the connection, so without this a
               registration pointing somewhere else collects that other database's rows under its own id,
               indefinitely and silently — and if a sibling registration names that database too, both collect
               it and the history is duplicated under two identities (#2220's byte-identical graphs).

               ERROR, not Warning: nothing clears this on its own and every sweep in the meantime stores
               mis-attributed rows. Logged on the TRANSITION so a standing misconfiguration does not bury
               itself. */
            var mismatch = DarlingServerConnector.DescribeDatabaseMismatch(
                server.Config.Database, runtime.ConnectedDatabase, server.Config.DisplayName);

            if (!string.Equals(server.LastDatabaseMismatchLogged, mismatch, StringComparison.Ordinal))
            {
                server.LastDatabaseMismatchLogged = mismatch;
                if (mismatch is not null)
                {
                    _logger.LogError("[{Server}] {Mismatch}", server.Config.DisplayName, mismatch);
                }
                else
                {
                    /* The transition BACK is worth one line too: it is the confirmation that an operator's
                       edit actually took, which otherwise requires trusting silence. */
                    _logger.LogInformation(
                        "[{Server}] now connected to the database it is registered for ('{Database}') — the " +
                        "earlier mismatch (#2228) is resolved.",
                        server.Config.DisplayName, runtime.ConnectedDatabase);
                }
            }
            /* Force the long-query trace (#1496) to re-reconcile on the next sweep after every (re)connect:
               an Azure database-scoped session can stop on reconnect, so a still-"applied" flag would
               otherwise skip restarting it. Cheap — the reconcile no-ops unless the desired state differs
               from what actually exists (ensure is IF-NOT-running START; drop is IF EXISTS). */
            server.LongQueryTraceApplied = null;
            /* #3754: and the two run-record slots the reconcile fills, for the same reason - they describe
               the sessions as of the LAST reconcile, and the next one is about to run against a fresh
               connection. A fault left standing here would classify the first post-reconnect run
               SESSION_MISSING before the reconcile had a chance to succeed; a stale partial note would
               name databases the reconnect may have just fixed. */
            server.LongQueryTraceFault = null;
            server.LongQueryTracePartialNote = null;
            server.LongQueryTraceAppliedKey = null;
            server.LongQueryTraceAppliedAtUtc = null;
            server.LongQueryTraceDropRetry.Reset();
            /* #4964: and the create side's "already warned" state, so a failure after the reconnect is a new one. */
            server.LongQueryTraceCreateWarned = false;
            /* #4961: and the always-on sessions' clock, so the connect path ensures them and stamps it again. */
            server.XeSessionsEnsuredAtUtc = null;
            /* Capture the id once, while the connection is freshly established and non-null: an on-load
               RunOneAsync below can drop server.Runtime on a mid-collection connection-level failure, so any
               later read of server.Runtime.ServerId (the schedule resolve, the connection edge) would NRE. */
            var serverId = runtime.ServerId;

            /* Retired containment (#1553 D1), the CONNECT-path re-check: a reconcile-remove may have retired this
               server while this connect body sat QUEUED on the fleet gate or ran the connect I/O (3+ min under
               distress). The connect SUCCEEDED, but a just-disabled server must incur ZERO durable side-effects —
               so bail BEFORE the registry upsert, the XE CREATE SESSION DDL, and the Server-Restored edge,
               dropping the runtime we just took. The entry check in ProcessServerSweepAsync covers the queued-
               dequeue case; this closes the window where removal lands DURING the connect itself. */
            if (server.Retired)
            {
                server.Runtime = null;
                server.LastKnownRuntime = null;
                return;
            }

            _logger.LogInformation("[{Server}] Connected (major {Major}, edition {Edition}, server_id {ServerId}){Recovery}",
                server.Config.DisplayName,
                runtime.Target.SqlMajorVersion,
                runtime.Target.IsAzureSqlDb ? "AzureSqlDb" : runtime.Target.IsAzureManagedInstance ? "ManagedInstance" : "Box",
                serverId,
                recoveryNote);

            /* Stage 4: the offline->online connection edge (Server Restored) — fired HERE, right after the
               connection is established and BEFORE the on-load collectors run, using the captured serverId.
               The connect succeeded regardless of what the on-load collectors do next, so this is the correct
               point to record "online" (and it can't NRE on a Runtime the on-load loop might drop). Fires only
               if the server was previously seen offline; the first-ever connect is a silent baseline (the
               state machine mirrors the Dashboard's skip-first-check). */
            await _selfAlerts!.ApplyConnectionOutcomeAsync(
                serverId, server.Config.DisplayName, online: true, error: null, cancellationToken);

            /* Same edge, second consumer: discard anything we concluded about this server while it was
               unreachable. An Azure SQL DB firewall rejection or failover is reported with the same error
               numbers as "this login may not read master", so a verdict formed during the outage can be
               wrong — and used to persist until the service restarted, quietly degrading database-scoped
               collection (#1506). */
            runner.OnServerReconnected(serverId);

            /* #4732: an edit that landed during the store writes above has already cleared Runtime; do not write the old
               target's facts into the registry or create its extended events sessions. */
            if (ConnectionIsStale())
            {
                DiscardStaleConnection(server, runtime);
                return;
            }

            await DarlingObservability.UpsertServerAsync(_postgres!, runtime, _logger, cancellationToken);

            /* Extended Events are a SQL Server feature. Ungated, this ran SqlClient against a PostgreSQL
               target on every connect and logged "Failed to ensure XE sessions: Keyword not supported:
               'host'. - deadlock/blocked-process collection will read zero rows until resolved" — a warning
               that is both alarming and meaningless on an engine that has no XE, on a target whose deadlock
               collectors are engine-gated off anyway. Confirmed on a live PostgreSQL target. */
            if (runtime.Target.Engine == CollectorTargetEngine.SqlServer)
            {
                var attentionEnabled = StoreConfigProvider.ResolveSchedule("attention", server.Config.ServerId, _scheduleOverrides).Enabled;
                var captureOutcome = new DarlingXeSessions.CaptureEnsureOutcome();
                var attentionFault = await DarlingXeSessions.EnsureAllAsync(runtime, runner, _logger, cancellationToken, attentionEnabled, captureOutcome);
                server.AttentionXePermissionFault = attentionFault?.Message;
                server.CaptureEnsure = captureOutcome;
                server.AttentionXeEnsuredWhileEnabled = attentionEnabled;
                server.XeSessionsEnsuredAtUtc = DateTime.UtcNow;
            }

            /* On-load config snapshots (effective FrequencyMinutes 0) run once per connect, then every
               scheduled collector becomes immediately due — mirrors Lite's server-open behavior. The
               effective schedule layers config_collector_schedules overrides on CollectorScheduleDefaults;
               a collector disabled by an override is neither run on-load nor scheduled.

               These on-load runs are NOT under the per-server CollectionGate (unlike the scheduled sweep and
               snapshot_now). Safe today: this path only runs while Runtime is null, and a snapshot_now needs
               a non-null Runtime, so the two never overlap for one server. If TryConnect's timing ever changes
               so a connect can race a snapshot, gate this loop too.

               #4999: they ARE under the per-(server, collector) slot a detached daily run holds, which the
               CollectionGate never covered. The scheduled run of each on-load collector is detached and outlives its
               pass, so a fault and a reconnect while that run is still going would start a second run beside it.
               RunOnLoadAsync takes the slot for the run's length and leaves out a collector whose slot is held;
               the next-due seeding below still happens for it. */
            var now = DateTime.UtcNow;
            /* #1575: seed each scheduled collector's first post-connect due time from its persisted last-run
               watermark so a restart RESUMES the real cadence instead of re-phasing it up to a full interval
               forward (which starved long-frequency collectors like the daily index_object_stats across a
               restart-heavy window). ONE batched round-trip reads every collector's MAX(collection_time) for this
               server; ComputeSeededNextDue turns each into a due time — a recently-run collector waits out the
               remaining interval, an overdue / never-run one runs promptly under a small per-server jitter that
               de-clusters the fleet WITHOUT the old full-interval defer (#1553's anti-herd intent, capped). The
               steady-state advance in RunDueCollectorsAsync stays on the exact interval. */
            var watermarks = await ReadCollectorWatermarksAsync(_postgres!, serverId, _logger, cancellationToken);

            /* #4938: the server's own clock, read once here and held on the server's state, so a collector that has a run
               time is seeded on this server's wall clock and no tick ever queries for it. A failed read keeps the clock
               already held (UTC for a server seen the first time); the post-run refresh of server_properties and
               pg_server_config (RunOneAsync) updates it as those rows arrive. */
            if (await TryReadServerClockAsync(_postgres!, serverId, runtime.Target.Engine, _logger, cancellationToken) is { } connectClock)
            {
                server.Clock = connectClock;
            }

            /* #4732: the first collector pass on this connection is the one the edit check exists for. */
            if (ConnectionIsStale())
            {
                DiscardStaleConnection(server, runtime);
                return;
            }

            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                /* The SAME pre-dispatch engine gate the scheduled sweep applies (see RunDueCollectorsAsync),
                   which this loop never got. Without it, the on-load pass dispatches every foreign-engine
                   collector once per connect: a PostgreSQL target ran server_config, database_config,
                   database_scoped_config, trace_flags and server_properties as T-SQL and logged five fake
                   SUCCESS rows with zero rows collected — confirmed on a live PostgreSQL target. Those rows
                   feed the health bands and analysis, which key on status, so a fake success is worse than an
                   error. Re-read from server.Runtime because a preceding RunOneAsync in this loop can have
                   nulled it on a connection-level failure. */
                if (server.Runtime is null
                    || !CollectorCatalog.EngineMatches(name, server.Runtime.Target)
                    /* And the within-engine gate, on EVERY engine — same reasoning as the scheduled sweep,
                       and extended to SQL Server with it (#2579). This loop runs on every connect and
                       reconnect, so leaving it PostgreSQL-scoped would keep landing fake SUCCESS rows for
                       gated-off SQL Server collectors at exactly the moments an operator is watching. */
                    || !CollectorCatalog.AppliesTo(name, server.Runtime.Target))
                {
                    continue;
                }

                /* Captured serverId, not server.Runtime.ServerId: an earlier on-load RunOneAsync in this loop
                   can null server.Runtime on a connection-level failure, which would otherwise NRE here. */
                var effective = StoreConfigProvider.ResolveSchedule(name, serverId, _scheduleOverrides);
                if (!effective.Enabled)
                {
                    continue;
                }

                if (effective.FrequencyMinutes == 0)
                {
                    var ranAtConnect = await RunOnLoadAsync(server, runner, name, serverId, effective, cancellationToken);

                    /* #3929/#3930: ALSO becomes due again on CollectorScheduleDefaults.OnLoadRecaptureMinutes,
                       seeded from the SAME pre-dispatch watermark used below - the run just above updates it
                       moments from now, but seeding from the watermark read at the top of this method means a
                       collector that already ran recently (a quick reconnect) is not re-phased a full day
                       forward. Without this, a long-lived connection never re-captures: its on-load config
                       snapshot ages out of retention (#3930) and a cleared trace flag has nothing to overwrite
                       its stale ON row (#3929) until the next reconnect. */
                    var onLoadInterval = CollectorScheduleDefaults.OnLoadRecaptureMinutes;
                    var onLoadLastRun = watermarks.TryGetValue(name, out var w0) ? w0 : (DateTime?)null;
                    var onLoadJitter = SeedJitter(serverId, onLoadInterval * 60);
                    if (effective.RunAtMinute is int onLoadRunAt)
                    {
                        SeedOnLoadRunTime(server, name, serverId, onLoadRunAt, onLoadInterval, ranAtConnect, onLoadLastRun);
                    }
                    else
                    {
                        lock (server.ScheduleLock)
                        {
                            server.RunTimeSlots.TryRemove(name, out _);
                            server.NextDue[name] = ComputeSeededNextDue(onLoadLastRun, onLoadInterval, now, onLoadJitter);
                        }
                    }
                }
                else
                {
                    var lastRun = watermarks.TryGetValue(name, out var w) ? w : (DateTime?)null;
                    var jitter = SeedJitter(serverId, effective.FrequencyMinutes * 60);

                    /* #4938: with a run time the seed is the slot (ComputeSeededNextDue holds the rules), on this
                       server's clock as read above; the slot's inputs are kept beside the stamp so a reload or a clock
                       change computes it again from the same last run. */
                    if (effective.RunAtMinute is int runAt)
                    {
                        var clock = server.Clock;
                        lock (server.ScheduleLock)
                        {
                            /* #5033: a daily run of the previous connection can still be recording, so its last run counts. */
                            server.RunTimeSlots.TryGetValue(name, out var heldSlot);
                            var seedLastRun = Newer(heldSlot?.LastRunUtc, lastRun);
                            server.NextDue[name] = ComputeSeededNextDue(
                                seedLastRun, effective.FrequencyMinutes, now, jitter, new RunTimeRule(runAt, serverId, clock.ToUtc));
                            server.RunTimeSlots[name] = new RunTimeSlot(runAt, effective.FrequencyMinutes, clock.Id, seedLastRun);
                        }
                    }
                    else
                    {
                        lock (server.ScheduleLock)
                        {
                            server.RunTimeSlots.TryRemove(name, out _);
                            server.NextDue[name] = ComputeSeededNextDue(lastRun, effective.FrequencyMinutes, now, jitter);
                        }
                    }
                }
            }

            /* #5597: every due stamp above was seeded from the clock read before the on-load runs, and this body is the
               server's only body, so a slot that came due while it ran could not have run. Slots count as skipped from
               here on (RunDueCollectorsAsync). The stamps themselves are not moved: when the first rows land is unchanged. */
            Interlocked.Exchange(ref server.SeedFinishedTicks, DateTime.UtcNow.Ticks);

            /* Phase the first scheduled analysis over a SMALL fixed sub-2.5-minute window (#1553 jitter site 3):
               at a fleet restart every freshly connected server would otherwise become analysis-due in the same
               sweep, and with N=4 concurrency that clusters 4 analysis passes at once. A deterministic per-server
               offset de-clusters them while keeping post-restart analysis prompt (state-M3). The window is a
               fixed 150s, NOT the full analysis interval — full-interval phasing would break the "already-
               populated store analyzes promptly" promise by up to the interval, and is redundant since analysis
               already runs inside the N=4-bounded body. Left at MinValue when analysis is disabled so re-enabling
               still runs immediately (the sweep gates on config.Analysis.Enabled). */
            server.NextAnalysisDue = config.Analysis.Enabled
                ? DateTime.UtcNow.Add(CadencePhaseOffset(serverId, 150))
                : DateTime.MinValue;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            server.Runtime = null;

            /* Retired containment (#4795), mirroring the CONNECT-path re-check on the online path above: a removal
               (Retired, then Forget) that lands during a failed connect must leave no alert state and send nothing.
               Applying the offline outcome below would write Offline under the removed server's key AFTER Forget
               cleared it, and a re-added server (same storage name, same server_id) would inherit that Offline.
               The check sits ahead of the failure count, the backoff and the retry log for the same reason: a
               removed server is never retried, so counting the failure, scheduling a retry or logging "retrying
               in Ns" for it would be false. server.Runtime = null above still runs first. */
            if (server.Retired)
            {
                return;
            }

            /* #4710: back off while the server stays down (60 s doubling to a 240 s cap, jittered), rather
               than a fixed 60 s. A definition edit or a successful connect resets the count. */
            server.ConsecutiveConnectFailures++;
            var retryDelay = ServerConnectBackoff.NextDelay(server.ConsecutiveConnectFailures, Random.Shared.NextDouble());
            server.NextConnectAttempt = DateTime.UtcNow.Add(retryDelay);
            var retrySeconds = (int)Math.Round(retryDelay.TotalSeconds);
            var failure = LogConnectFailure(server, ex, retrySeconds);

            /* Stage 4: the online->offline connection edge (Server Unreachable) — fires once when a
               previously-connected server can no longer be reached; a repeated failed reconnect does NOT
               re-fire (the state machine dedups). server_id comes from the CONFIG rather than the runtime,
               because Runtime is null here by definition -- and post-#2218 the config carries the STORED id,
               so an alert on a server that has never once connected keys on the same identity its collected
               history does. */
            await _selfAlerts!.ApplyConnectionOutcomeAsync(
                server.Config.ServerId,
                server.Config.DisplayName, online: false, error: failure, cancellationToken);
        }
    }

    /// <summary>
    /// Writes the log line for one failed connect attempt and returns the failure text. #2255: full text on a NEW cause.
    /// #7067: while the cause stays the same the "still failing" line is written at most once an hour, with the attempts
    /// and how long the server has been down, instead of on every retry (about every 5 minutes for a server that is
    /// switched off, 1,982 lines in one day on a host with seven). Timed on <see cref="RepeatLog"/>'s clock.
    /// </summary>
    internal string LogConnectFailure(ServerLoopState server, Exception ex, int retrySeconds)
    {
        var now = RepeatLog.Clock();
        server.ConnectDownSinceUtc ??= now;
        server.ConnectAttemptsSinceDown++;

        /* #2255: full text on a NEW cause, one line while it persists. A credential that cannot be
           decrypted on this host is not a transient connect failure, so its explanation is worth Error
           once and worth almost nothing on the 1,440th repeat. */
        /* #6183: SqlClient's native network library not loading breaks every SQL Server connection from this
           install, and "The type initializer for 'TdsParser' threw an exception" does not say why. The shared
           explanation replaces the raw text, and it is logged once for the whole process (it is the same cause
           for every server), not once per server with a retry line. The retry loop itself is unchanged. */
        var nativeLoadFailure = SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory);
        var failure = nativeLoadFailure ?? ex.Message;
        if (nativeLoadFailure is not null)
        {
            server.LastConnectFailureLogged = failure;
            server.LastConnectFailureLineUtc = now;
            if (Interlocked.Exchange(ref _nativeLoadFailureLogged, 1) == 0)
            {
                _logger.LogError("Connect failed and will keep failing until fixed: {Message}", failure);
            }
        }
        else if (!string.Equals(server.LastConnectFailureLogged, failure, StringComparison.Ordinal))
        {
            server.LastConnectFailureLogged = failure;
            server.LastConnectFailureLineUtc = now;
            if (ex is InvalidOperationException && failure.Contains("DPAPI-decrypt", StringComparison.Ordinal))
            {
                /* Error, not Warning: nothing about this clears on its own, so it needs an operator. */
                _logger.LogError("[{Server}] Connect failed and will keep failing until fixed: {Message}",
                    server.Config.DisplayName, failure);
            }
            else
            {
                _logger.LogWarning("[{Server}] Connect failed, retrying in {Delay}s: {Message}",
                    server.Config.DisplayName, retrySeconds, failure);
            }
        }
        else if (now - server.LastConnectFailureLineUtc >= StillFailingInterval)
        {
            server.LastConnectFailureLineUtc = now;
            _logger.LogWarning(
                "[{Server}] Connect still failing after {Attempts} attempts over {Down}, retrying in {Delay}s (same cause as logged above)",
                server.Config.DisplayName, server.ConnectAttemptsSinceDown, RepeatLogGate.FormatSpan(now - server.ConnectDownSinceUtc.Value), retrySeconds);
        }

        return failure;
    }

    /// <summary>
    /// The warning for a classified PostgreSQL fault. #7067: a precondition that does not change (an extension that is not
    /// created) said the same thing on every cycle. It is logged when first seen and when the text changes, then as a
    /// reminder once a day; the collection_log row is written on every run as before, and a success clears the memory.
    /// </summary>
    internal void LogPostgresFault(string serverName, string collectorName, string status, string? sqlState, string explanation)
    {
        if (RepeatLog.ShouldLog(serverName, collectorName, $"{status}|{sqlState}|{explanation}"))
        {
            _logger.LogWarning("  [{Server}] {Collector} => {Status} ({SqlState}): {Message}",
                serverName, collectorName, status, sqlState, explanation);
        }
    }

    /// <summary>#7067: how long the "still failing" connect line stays quiet.</summary>
    internal static readonly TimeSpan StillFailingInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// #7067: the tail of the "Connected" line when the server had been failing to connect, and the reset of the
    /// failure bookkeeping. Empty when the server connected first time. One line carries the recovery, so a server that
    /// was down for a day logs its first failure, at most 24 reminders and this.
    /// </summary>
    internal string TakeConnectRecoveryNote(ServerLoopState server)
    {
        var downSince = server.ConnectDownSinceUtc;
        var attempts = server.ConnectAttemptsSinceDown;
        server.ConnectDownSinceUtc = null;
        server.ConnectAttemptsSinceDown = 0;
        server.LastConnectFailureLineUtc = default;
        return downSince is null
            ? string.Empty
            : $", after {attempts} failed attempt(s) over {RepeatLogGate.FormatSpan(RepeatLog.Clock() - downSince.Value)} down";
    }

    /// <summary>1 once the native-library load failure (#6183) has been logged. It is one cause for every server.</summary>
    private int _nativeLoadFailureLogged;

    /// <summary>
    /// #4999: the at-connect run of ONE on-load collector (effective frequency 0), inline in
    /// <see cref="TryConnectAsync"/>. Each on-load collector's SCHEDULED run is detached (its recapture is daily), and
    /// a detached run outlives the pass that dispatched it: a fault and a reconnect while it is still going, or still
    /// queued for a permit, brings this loop to the same collector. So the run takes the (server, collector) slot the
    /// detached run holds (<see cref="TryTakeInlineDailySlot"/>), for its own length, and a collector whose slot is
    /// held is left out: the run that holds it is already capturing, and it reads the connection the server has when
    /// it starts. Left out costs nothing: the caller still seeds the collector's next due time. Internal so a test can
    /// drive it with <see cref="RunOneBodyOverride"/> standing in for the collector run, because the connect path
    /// itself needs a store.
    /// </summary>
    /// <param name="serverId">The id the connect captured, which stays valid after a run nulls the server's runtime.</param>
    /// <param name="effective">The collector's effective schedule, resolved by the caller.</param>
    /// <returns>True when the collector ran and succeeded; false when its slot was held or the run failed.</returns>
    internal async Task<bool> RunOnLoadAsync(
        ServerLoopState server, DarlingCollectorRunner runner, string collectorName, int serverId, EffectiveSchedule effective,
        CancellationToken cancellationToken)
    {
        using var slot = TryTakeInlineDailySlot(serverId, collectorName, effective);
        if (slot is null)
        {
            _logger.LogInformation(
                "  [{Server}] {Collector} not run at connect: its scheduled daily run is still going (#4999)",
                server.Config.DisplayName, collectorName);
            return false;
        }

        /* null, not the live mark: the on-load dispatch is not a scheduled sweep body and
           never resets it, so folding it in would mix a previous body's bookkeeping
           into these rows - the cross-body contamination the reset exists to prevent. */
        var runOutcome = new RunOutcome();
        await RunOneAsync(server, runner, collectorName, peerMaxAtDispatchMs: null, cancellationToken, runOutcome: runOutcome);
        return runOutcome.Succeeded;
    }

    /// <summary>
    /// #4938: seeds an on-load collector's daily re-run when it has a run time. A run at connect that succeeded is the last
    /// run: it counts for today when it fell at or after today's slot, and for the day before when it fell earlier, which
    /// leaves today's slot owed. Seeded from the older mark instead, a connect inside the hour after the slot would run the
    /// collector a second time within seconds. A run that failed, or never started because the scheduled run holds the slot,
    /// is not a run: the slot is seeded from the last run on record, so it stays due inside its grace.
    /// </summary>
    internal void SeedOnLoadRunTime(
        ServerLoopState server, string name, int serverId, int runAtMinute, int intervalMinutes, bool ranAtConnect, DateTime? watermark)
    {
        var jitter = SeedJitter(serverId, intervalMinutes * 60);
        var clock = server.Clock;
        var rule = new RunTimeRule(runAtMinute, serverId, clock.ToUtc);
        var now = DateTime.UtcNow;
        lock (server.ScheduleLock)
        {
            var lastRun = watermark;
            if (ranAtConnect)
            {
                lastRun = now;
            }
            else if (server.RunTimeSlots.TryGetValue(name, out var held) && held.LastRunUtc is { } heldRun && (lastRun is null || heldRun > lastRun))
            {
                lastRun = heldRun;
            }

            server.NextDue[name] = ComputeSeededNextDue(lastRun, intervalMinutes, now, jitter, rule);
            server.RunTimeSlots[name] = new RunTimeSlot(runAtMinute, intervalMinutes, clock.Id, lastRun);
        }
    }

    /// <summary>
    /// One server's scheduled collector pass: runs, or detaches, every collector that has come due. Internal
    /// (#4938) so a test can drive the pass with <see cref="RunOneBodyOverride"/> standing in for the collector
    /// runs; production calls it only from the per-server body.
    /// </summary>
    internal async Task RunDueCollectorsAsync(ServerLoopState server, DarlingCollectorRunner runner, CancellationToken cancellationToken)
    {
        var runtime = server.Runtime;
        if (runtime is null)
        {
            return;
        }

        /* Non-blocking: if an on-demand snapshot_now holds this server's gate, skip its scheduled sweep
           this pass (the due collectors run next sweep) — the main loop NEVER blocks on the gate, so a
           long snapshot cannot starve collection of the OTHER servers. */
        if (!await server.CollectionGate.WaitAsync(0, cancellationToken))
        {
            return;
        }

        try
        {
            var now = DateTime.UtcNow;

            /* #5735: what the PREVIOUS body measured is the cause of any slot this one steps over, so it is read before this
               body overwrites it. */
            var priorCauses = new CycleSkipCauses(
                server.LastBodyMs, server.LastBodySlowestName, server.LastBodySlowestMs, server.LastGateWaitMs,
                server.LastAnalysisMs, server.LastGateWaitAnalysisHolders);
            var bodyStarted = Stopwatch.GetTimestamp();
            var bodySlowestName = (string?)null;
            var bodySlowestMs = 0L;

            /* #2864: the peer high-water mark describes ONE body. Reset here rather than decayed, because
               the comparison it feeds is 'were this sweep's other collectors slow', and a mark carried
               across bodies would answer a different question with the same number. */
            server.SweepPeerMaxMs = -1;
            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                /* Wrong-engine collectors are dropped BEFORE dispatch, not gated inside the runner: a
                   definition whose dialect the target does not speak must leave no trace at all. The
                   runner's own CollectorCatalog.AppliesTo check returns 0 rows, which RunOneAsync would
                   record as SUCCESS — fine for the handful of Azure-gated collectors, but with a second
                   engine in the catalog every target would log a fake success per foreign collector per
                   cycle (most are 1-minute), flooding collection_log and feeding phantom successes to the
                   health bands and analysis, which key on status. This is Darling's equivalent of Lite's
                   pre-dispatch SKIPPED path: no dispatch, no log row, no NextDue churn. */
                if (!CollectorCatalog.EngineMatches(name, runtime.Target))
                {
                    continue;
                }

                /* WITHIN-engine gates get the same treatment, on EVERY engine.
                   EngineMatches above drops the wrong DIALECT; it says nothing about a collector that is
                   right-dialect but inapplicable to this particular target — pg_wait_stats reads Aurora's own
                   wait instrumentation, so on stock PostgreSQL it dispatched, came back with 0 rows, and
                   RunOneAsync recorded SUCCESS. At a 1-minute cadence that is ~1,440 fake successes a day per
                   server, and the PR promised "a graceful skip with an explanation" instead. (pg_statement_stats
                   was the second such collector until #2625 gave it a vanilla pg_stat_statements path; it now
                   applies to every PostgreSQL target and reaches this gate on none of them.)

                   #2579 EXTENDS THIS TO SQL SERVER, which the PostgreSQL change deliberately left alone as
                   "its own decision" because it changes a shipping SKU's log semantics for the Azure-gated
                   collectors. Here is that decision, and what settled it is that the cost turned out to be
                   the opposite of cosmetic. On an AWS RDS fleet the SQL Server gates are not a handful: 84
                   instances x agent_status and running_jobs x a 5-minute cadence is ~24,000 rows a day that
                   say SUCCESS about collectors deliberately not running. A gated-off run recorded as SUCCESS
                   is byte-identical to a real one — same status, zero rows, no note — so nothing downstream
                   can tell them apart. That is not merely noise: it is the shape the whole miss vocabulary
                   exists to prevent, and it read as evidence of working collection convincingly enough to
                   produce an issue and a PR built on it before the 0ms durations gave it away.

                   No log row is the honest outcome, and it is not silent: --test-connection names exactly
                   which collectors do not apply to a target, and why, before the service ever runs. */
                if (!CollectorCatalog.AppliesTo(name, runtime.Target))
                {
                    continue;
                }

                /* Effective schedule = config_collector_schedules override layered on the code default.
                   A disabled collector is skipped; the frequency the NextDue stamp advances by is the
                   EFFECTIVE one, so an override takes effect immediately. An on-load collector (freq 0) is
                   NOT skipped here (#3929/#3930): it also reruns on
                   CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes, in addition to the immediate
                   on-connect run TryConnectAsync's on-load loop still does — a long-lived connection would
                   otherwise never refresh it again, aging its config snapshot out of retention (#3930) and
                   leaving a cleared trace flag with no later row to overwrite its stale ON one (#3929). */
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled
                    || !server.NextDue.TryGetValue(name, out var due))
                {
                    continue;
                }

                var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes);
                var intervalSpan = TimeSpan.FromMinutes(interval);

                /* #4938: a collector that has a run time is scheduled on its slot, not on the interval grid below. The
                   slot is the run time on this server's clock plus the server's spread, one a day (RunTimeSlots holds what
                   the stamp was computed from). The pass hands the run off from the slot until an hour after it and never
                   later: a day it could not hand off is skipped, not replayed, like any missed slot (#4636). A run handed
                   off is detached below with every other daily collector, so it waits for the fleet cap and is never
                   dropped for waiting. The clock is read from memory (ServerLoopState.Clock); a clock that has changed
                   since the stamp was computed, such as a server's own zone arriving after its first
                   server_properties row, computes the slot again from the last run held beside the stamp. */
                RunTimeHandOff? handOff = null;
                if (effective.RunAtMinute is int runAtMinute)
                {
                    var clock = server.Clock;
                    var rule = new RunTimeRule(runAtMinute, runtime.ServerId, clock.ToUtc);
                    var jitter = SeedJitter(runtime.ServerId, interval * 60);
                    /* #5033: the slot is read, seeded or re-seeded and stepped under the server's schedule lock, so a
                       reload's recompute or a run's record never lands between the read and the write. The lock covers only
                       these in-memory steps; the log lines and the gate-stat count follow it. */
                    RunTimeSlot slot = null!;
                    RunTimeStep step = default;
                    var stampGone = false;
                    var floor = _skipCreditFloor.Floor;
                    lock (server.ScheduleLock)
                    {
                        if (!server.NextDue.TryGetValue(name, out due))
                        {
                            stampGone = true;
                        }
                        else
                        {
                            if (!server.RunTimeSlots.TryGetValue(name, out slot!))
                            {
                                slot = new RunTimeSlot(runAtMinute, interval, clock.Id, null);
                                server.RunTimeSlots[name] = slot;
                            }
                            else if (!string.Equals(slot.ClockId, clock.Id, StringComparison.Ordinal))
                            {
                                due = ComputeSeededNextDue(slot.LastRunUtc, interval, now, jitter, rule);
                                server.NextDue[name] = due;
                                slot = slot with { ClockId = clock.Id };
                                server.RunTimeSlots[name] = slot;
                            }

                            step = StepRunTimeCollector(due, now, floor, interval, rule, jitter);
                            if (step.Action == RunTimeAction.SkipDay)
                            {
                                server.NextDue[name] = step.NextDue;
                            }
                        }
                    }

                    if (stampGone || step.Action == RunTimeAction.NotDue)
                    {
                        continue;
                    }

                    if (step.Action == RunTimeAction.SkipDay)
                    {
                        _fleetGateStats?.RecordSkippedSlots(step.Skipped);
                        server.CycleSkips.AddDailyLost(name, step.Skipped, due);
                        _logger.LogInformation(
                            "  [{Server}] {Collector} skipped today's run: the pass could not hand it off within {Grace} minutes of its run time, so it waits for its next run time, {Next:u} (#4938)",
                            server.Config.DisplayName, name, CollectorRunTime.GraceMinutes, step.NextDue);
                        continue;
                    }

                    /* Yesterday's run still holds the (server, collector) slot, going or queued for a permit: today's run
                       would be refused at once, so it is not handed off, and the slot stays unserved with its stamp where it
                       is. The next tick asks again inside the grace. */
                    if (_detachedCollectorGates.TryGetValue((runtime.ServerId, name), out var heldGate) && heldGate.IsHeld)
                    {
                        _logger.LogDebug(
                            "  [{Server}] {Collector} not handed off this tick: its previous daily run still holds its slot (#4938)",
                            server.Config.DisplayName, name);
                        continue;
                    }

                    /* Today's slot is served only when today's run takes its (server, collector) slot, so the stamp, the
                       slot's last run and the count of a run wait for it (RunOneAsync records them through this). When
                       yesterday's run still holds the slot, nothing is recorded and the stamp stays where it is: the next
                       tick hands the run off again inside the grace, and past the grace it counts as one skipped slot. */
                    handOff = new RunTimeHandOff(slot, step.NextDue);
                }
                else
                {
                    /* #4732: a due time more than one interval ahead can only be a wall clock that stepped backwards after
                       the time was stamped; left alone it pauses this collector for as long as the step. Treated as due
                       now. (RecomputeNextDueAsync, on a schedule reload, does the milder thing: it caps a stored due time
                       at now plus the interval, so that stamp waits one interval instead of running at once.) */
                    due = CollectorCadence.ClampDue(due, now, intervalSpan);
                    if (now < due)
                    {
                        continue;
                    }

                    /* #4636: advance on a fixed grid from the previous due time, not from this body's start, so a
                       late start is not carried into the next slot; a slot missed in a stall is skipped, not replayed.
                       #4732: and the skipped slots are counted, next to the slot that ran, so a fleet that cannot keep
                       its cadence shows up as a share of skipped slots instead of one Info line per collector. Only the
                       slots that came due while the sweep loop was running count (SkipCreditFloor): a host that slept, a
                       clock that stepped forward or a pause leaves this stamp hours old, and none of those slots was
                       skipped by a gate that was too narrow. A body that starts late because the gate was full, while the
                       loop keeps ticking, still counts them all. */
                    var seeded = new DateTime(SkipCreditFloor.ClampSeedStamp(ref server.SeedFinishedTicks, now), DateTimeKind.Utc);
                    var gridSkipped = _skipCreditFloor.Skipped(due, now, intervalSpan, seeded);
                    _fleetGateStats?.RecordSlot(gridSkipped);
                    server.CycleSkips.AddGrid(name, gridSkipped, due);
                    server.NextDue[name] = CollectorCadence.NextDue(due, now, intervalSpan);

                    /* #6186: a collector the server refused with the same answer run after run waits out its backoff instead
                       of writing a PERMISSIONS row every interval. The slot is served like any other: counted above, with the
                       grid slots the loop stepped over, and the stamp moved on, because the fleet gate measures whether the
                       sweep loop keeps cadence and those slots are real whether or not this collector then runs. The backoff
                       only decides not to run it, so the first try after the wait is an ordinary slot, not a burst of missed
                       ones. The continue skips the whole run for this pass (the detached query_store and daily dispatches
                       below included), so no collection_log row is written. */
                    if (server.RefusedBackoff.IsBackedOff(name, now, intervalSpan))
                    {
                        _logger.LogDebug(
                            "  [{Server}] {Collector} not run this slot: the server has refused it {Streak} times in a row, so it is tried less often until that changes (#6186)",
                            server.Config.DisplayName, name, server.RefusedBackoff.Streak(name));
                        continue;
                    }
                }

                /* #2700: query_store is split off this sequential body rather than awaited inline. Its
                   run time is bimodal — a heavy batch runs 100-230+ seconds against a ~5-35s mean, on its
                   own 5-minute cadence — and every OTHER due collector in this foreach (query_stats,
                   procedure_stats, wait_stats, all 1-minute cadence) would otherwise queue behind that one
                   await for the rest of the body's duration. Worse, this body IS the unit the outer launch
                   loop will not relaunch while it is still running (INV-2, "one body per server"), so a
                   single heavy query_store run stalled the server's ENTIRE collection for its duration, not
                   just query_store's own row — confirmed via get_collection_health's BODY_OVERRUN
                   diagnostic as the mechanism that pushed several servers' last_collection stale enough to
                   false-trip the fleet's 15-minute Offline threshold while every collector was otherwise
                   healthy (zero failures, a pure scheduling overrun). Fire-and-forget is safe here
                   specifically because RunOneAsync already gates query_store through the per-server
                   QueryStoreServerGate (#2165) — a still-in-flight previous tick skips rather than
                   overlapping — and query_store's own window is watermark-driven (#1960), so a detached run
                   that outlives this sweep resumes correctly from its own gate rather than dropping rows.
                   RunOneAsync's catch-all already contains every fault but cancellation, so
                   RunDetachedAsync exists only to keep a shutdown-time OperationCanceledException from
                   surfacing as an unobserved task exception. */
                /* #2717: plan_correction gets the identical treatment for the identical reason. Its own
                   SQL is already correctly seek-based (#2687) and averages ~1 second, but on a server
                   whose Query Store carries the same workload-class distinct-plan-population signature
                   already root-caused for query_store on multi-03/OMEGA, it can spike to 20+ seconds — the
                   same bimodal shape, just a smaller worst case. Detached the same way, through the
                   generic DetachedCollectorGate (#2717) rather than query_store's own gate, which has an
                   orthogonal second job (excluding the backfill loop) this collector does not share.
                   plan_correction's recommendation-set read is DMV-driven with no persisted watermark, but
                   sys.dm_db_tuning_recommendations is re-read whole on every successful pass regardless —
                   a skipped tick simply re-reads the same (or since-refreshed) live set next time, the
                   same "defers, does not drop" property #1960 gives query_store's watermark. */
                /* #2864 review: snapshot the peer mark HERE, at dispatch, and hand it to the run. Reading it
                   at completion is correct only for the sequential arm; a detached run finishes 100-230s
                   later, by which time the 15s sweep has reset and rebuilt the mark from unrelated ticks. */
                var peerMaxAtDispatchMs = PeerMaxOrNull(server);
                /* #3604: pg_wait_sampling is the third, and the reason is different in kind — see
                   IsPgWaitSamplingCollector: a deliberate 30 s sampling window, not a bimodal tail. */
                /* #4999: ONE test decides whether a collector leaves this pass, RunsDetached: the three above by
                   name and every daily collector by cadence. get_collection_health asks the same test to leave
                   those collectors out of its sweep-body roll-up, so the dispatch and the reading cannot
                   disagree about what runs in the body. */
                if (!RunsDetached(name, interval))
                {
                    var collectorStarted = Stopwatch.GetTimestamp();
                    await RunOneAsync(server, runner, name, peerMaxAtDispatchMs, cancellationToken, runTimeHandOff: handOff);
                    var collectorMs = (long)Stopwatch.GetElapsedTime(collectorStarted).TotalMilliseconds;
                    if (collectorMs > bodySlowestMs)
                    {
                        bodySlowestMs = collectorMs;
                        bodySlowestName = name;
                    }
                }
                else if (IsDetachedByName(name))
                {
                    /* #4999: tracked like the daily runs below, so the shutdown drain waits for a run that is still
                       going. Dropped on the floor, as these three used to be, nothing held them once the pass that
                       dispatched them ended. */
                    TrackDetachedRun(RunDetachedAsync(server, runner, name, peerMaxAtDispatchMs, cancellationToken, runTimeHandOff: handOff));
                }
                /* #4938: and every DAILY collector, by cadence rather than by name. index_object_stats took 43
                   minutes across 72 databases in a field case, and awaited here that stalled the server's
                   1-minute collectors for the whole run: the earlier "amortised to nil" reading counted how often
                   it runs, not how long the body waits when it does. The three above stay on their own rules
                   (the by-name arm is asked first, so a daily override on one of them changes nothing). A daily
                   run goes through the same per-(server, collector) single-flight slot, taken in RunOneAsync, and
                   a fleet-wide cap on how many run at once; a run over the cap waits and is never dropped.
                   The pass gives its fleet permit back at once, as for the detached runs above. The interval is
                   the EFFECTIVE one resolved above, where an on-load collector's 0 is already its daily
                   recapture. Only this scheduled dispatch detaches: the at-connect run of an on-load collector
                   stays inline in TryConnectAsync, because it is a short config snapshot the first sweep and
                   the analysis read right after connecting, and it runs once per connect, outside any pass. */
                else
                {
                    TrackDetachedRun(RunDetachedAsync(server, runner, name, peerMaxAtDispatchMs, cancellationToken, detachedDaily: true, runTimeHandOff: handOff));
                }
            }

            /* #5735: the slots this server lost since its last row, written as one row by the body that noticed (at most one in
               DarlingCycleSkip.RowMinInterval), then this body's own length is kept for the next one's cause. */
            await FlushCycleSkipAsync(server, runtime, priorCauses, cancellationToken);
            server.LastBodyMs = (long)Stopwatch.GetElapsedTime(bodyStarted).TotalMilliseconds;
            server.LastBodySlowestName = bodySlowestName;
            server.LastBodySlowestMs = bodySlowestMs;
        }
        finally
        {
            server.CollectionGate.Release();
        }
    }

    /// <summary>
    /// #5735: writes the server's pending stepped-over slots as one <c>collection_cycle</c> row, or nothing when there are
    /// none, the last row is under <see cref="DarlingCycleSkip.RowMinInterval"/> old, or the server is retired. A failed
    /// write keeps the pending counts for the next body. A restart loses them (the in-memory gate stats do too).
    /// </summary>
    internal async Task FlushCycleSkipAsync(ServerLoopState server, ServerRuntime runtime, CycleSkipCauses causes, CancellationToken cancellationToken)
    {
        var postgres = _postgres;
        if (postgres is null || server.Retired)
        {
            return;
        }

        var now = DateTime.UtcNow;
        var taken = server.CycleSkips.TryTake(now, DarlingCycleSkip.RowMinInterval);
        if (taken is null)
        {
            return;
        }

        var report = DarlingCycleSkip.Build(taken, causes, now);
        if (report is null)
        {
            return;
        }

        if (await DarlingObservability.LogCycleSkipAsync(postgres, runtime, now, report, _logger, cancellationToken))
        {
            server.CycleSkips.Commit(taken, now);
        }
    }

    /// <summary>
    /// The <c>snapshot_now</c> command handler (Lite's Live Snapshot, control-plane form): runs EVERY
    /// enabled collector for one connected server immediately, bypassing the schedule, and reports the
    /// collectors run + total rows. Serialized against the scheduled sweep by the per-server
    /// <see cref="ServerLoopState.CollectionGate"/> so the two never double-collect. Waits its turn for the
    /// gate (unlike the main loop, which skips) because an explicit operator snapshot should not be dropped.
    /// </summary>
    internal async Task<CommandOutcome> RunSnapshotAsync(
        List<ServerLoopState> servers, DarlingCollectorRunner runner, int serverId, CancellationToken cancellationToken,
        IReadOnlyList<string>? collectors = null)
    {
        /* #5941: an optional collector list. Empty or absent = the run-all form (every ENABLED collector). A list is
           the per-collector form: only those collectors, each run even when disabled (its enabled state and its
           schedule are not touched; the override map is only read). Every name is checked against the catalog first,
           so a typo refuses the whole command and names the bad name instead of running part of it. */
        var requested = new List<string>();
        foreach (var raw in collectors ?? [])
        {
            var name = raw?.Trim() ?? string.Empty;
            if (!CollectorScheduleDefaults.All.ContainsKey(name))
            {
                return new CommandOutcome(false, "unknown collector",
                    JsonError($"unknown collector '{name}'. Use a name from the collector list."));
            }

            if (!requested.Contains(name, StringComparer.Ordinal))
            {
                requested.Add(name);
            }
        }

        /* #5941 fleet cap: the snapshot takes a permit from the same gate (width 4 by default) a scheduled pass takes, so
           clicks on several servers cannot run more collection than the sweeps do. The permit comes BEFORE the
           per-server collection gate; the scheduled pass holds a permit and only TRIES the collection gate, so the two
           cannot wait on each other. */
        var sweepGate = SweepGate;
        if (sweepGate is not null)
        {
            await sweepGate.WaitAsync(cancellationToken);
        }

        try
        {
            return await RunSnapshotUnderPermitAsync(servers, runner, serverId, requested, cancellationToken);
        }
        finally
        {
            sweepGate?.Release();
        }
    }

    private async Task<CommandOutcome> RunSnapshotUnderPermitAsync(
        List<ServerLoopState> servers, DarlingCollectorRunner runner, int serverId, List<string> requested,
        CancellationToken cancellationToken)
    {
        var explicitList = requested.Count > 0;

        ServerLoopState? server;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (server.Runtime is null)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{server.Config.DisplayName}' is not currently connected — snapshot skipped"));
        }

        await server.CollectionGate.WaitAsync(cancellationToken);
        try
        {
            /* Runtime can be dropped by a concurrent connection failure between the check above and here;
               re-read under the gate. */
            var runtime = server.Runtime;
            if (runtime is null)
            {
                return new CommandOutcome(false, "server not connected",
                    JsonError($"server '{server.Config.DisplayName}' disconnected before the snapshot ran"));
            }

            var collectorsRun = 0;
            var totalRows = 0;
            var skippedDaily = new List<string>();
            var skippedOther = new List<(string Collector, string Reason)>();
            foreach (var name in explicitList ? requested : CollectorScheduleDefaults.All.Keys.ToList())
            {
                cancellationToken.ThrowIfCancellationRequested();

                /* Honor a disabled-by-override collector (mirrors the on-load/scheduled gates); run every
                   enabled one NOW regardless of frequency or NextDue — that is what "snapshot" means. A collector the
                   operator NAMED is run even when disabled (#5941): that is an explicit choice, and nothing is written
                   to the override map. */
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled && !explicitList)
                {
                    continue;
                }

                /* The THIRD dispatch loop, and it got neither engine gate in the first round: an operator
                   snapshot against a PostgreSQL target dispatched every SQL Server collector, whose
                   AppliesTo early-return yields zero rows and lands a burst of fake SUCCESS in
                   collection_log — the phantom-success class the other two loops (on-load, scheduled
                   sweep) were gated against. Same predicate, and since #2579 the same every-engine scoping:
                   an operator-triggered snapshot against an RDS target would otherwise land its own burst of
                   fake successes for the msdb-gated collectors. */
                if (!CollectorCatalog.EngineMatches(name, runtime.Target)
                    || !CollectorCatalog.AppliesTo(name, runtime.Target))
                {
                    /* The run-all form leaves these out without a word (a PostgreSQL target has dozens of them); a
                       collector the operator named is reported, with the reason (#5941). */
                    if (explicitList)
                    {
                        skippedOther.Add((name, "it does not apply to this server's engine"));
                    }

                    continue;
                }

                /* #4999: a daily collector's scheduled run is detached and holds its (server, collector) slot until it
                   ends, which can be hours (it may still be waiting for one of the fleet's permits). An operator
                   snapshot of the same collector would run it a second time beside that run, so the snapshot takes
                   the same slot: held means the scheduled run is still going, and the snapshot leaves that
                   collector out and says so. Free means the snapshot runs it inline, as before, holds the slot for
                   that run only, and gives it back after, so a snapshot that is over never makes the next scheduled
                   run skip. The three collectors detached by name take their own slot inside RunOneAsync. */
                var snapshotSlot = TryTakeInlineDailySlot(runtime.ServerId, name, effective);
                if (snapshotSlot is null)
                {
                    skippedDaily.Add(name);
                    _logger.LogInformation(
                        "  [{Server}] snapshot_now skipped {Collector}: its scheduled daily run is still going (#4999)",
                        server.Config.DisplayName, name);
                    continue;
                }

                /* null for the same reason as the on-load loop: an operator snapshot is not a body. */
                using (snapshotSlot)
                {
                    totalRows += await RunOneAsync(server, runner, name, peerMaxAtDispatchMs: null, cancellationToken);
                }

                collectorsRun++;
            }

            _logger.LogInformation("[{Server}] snapshot_now ran {Collectors} collector(s), {Rows} row(s)",
                server.Config.DisplayName, collectorsRun, totalRows);
            var json = JsonSerializer.Serialize(new
            {
                success = true,
                server = server.Config.DisplayName,
                collectorsRun,
                rows = totalRows,
                /* #4999: the collectors the snapshot left out because their scheduled daily run is still going. */
                skipped = skippedDaily.Select(collector => new
                {
                    collector,
                    reason = "its scheduled daily run is still going",
                }).Concat(skippedOther.Select(o => new { collector = o.Collector, reason = o.Reason })).ToList(),
            });
            var skippedCount = skippedDaily.Count + skippedOther.Count;
            return new CommandOutcome(
                true,
                skippedCount == 0
                    ? "snapshot complete"
                    : $"snapshot complete; {skippedCount} collector(s) skipped",
                json);
        }
        finally
        {
            server.CollectionGate.Release();
        }
    }

    /// <summary>
    /// The <c>fetch_plan</c> command handler (headless-plan live-plan wave): reads an execution plan from one
    /// monitored server's LIVE plan cache and returns the plan XML — the mechanism the viewer uses to fetch a
    /// plan for ANY process in a deadlock graph / blocked-process report (by its sql_handle) or the
    /// currently-cached plan for a query-grid row (by its plan_handle), neither of which the store holds. The
    /// actual cache read is delegated to the SAME <see cref="PgPlanFetcher"/> the analysis pipeline already uses
    /// (it resolves the serverId to the connected runtime's connection string) — the plan_handle path is its
    /// existing <see cref="PgPlanFetcher.FetchPlanXmlAsync"/>, the sql_handle path its
    /// <see cref="PgPlanFetcher.FetchPlanBySqlHandleAsync"/>. Unlike snapshot_now it takes NO collection gate: a
    /// DMV plan read touches no collector state and writes nothing, so it can run concurrently with a scheduled
    /// sweep. The up-front lookup gives a precise "not monitored" / "not connected" outcome (mirroring
    /// RunSnapshotAsync); a plan that has aged out of the cache — or any fetch error the fetcher swallows to null
    /// per its analysis-safe contract — is reported as a clean "not in cache" (the command itself succeeded).
    /// </summary>
    private async Task<CommandOutcome> RunFetchPlanAsync(
        List<ServerLoopState> servers, PgPlanFetcher planFetcher, int serverId,
        PlanFetchRequest request, CancellationToken cancellationToken)
    {
        /* Lookup under the lock (the command loop reconciles the list concurrently); the fetcher re-resolves the
           serverId to the runtime connection string itself, so this only gates the precise not-monitored /
           not-connected outcomes. Held only for the microsecond lookup. */
        ServerLoopState? server;
        bool connected;
        string displayName;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
            connected = server?.Runtime is not null;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (!connected)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — the live plan cache can only be read from a connected server"));
        }

        /* #2443: both arms now take the SAME token. The by-sql_handle arm always had it; the
           by-plan_handle arm did not, so a cancelled fetch_plan command kept a session open on the
           monitored server for whichever key the caller happened to use. */
        var planXml = request.UsePlanHandle
            ? await planFetcher.FetchPlanXmlAsync(serverId, request.PlanHandle!, cancellationToken)
            : await planFetcher.FetchPlanBySqlHandleAsync(
                serverId, request.DatabaseName, request.SqlHandle!,
                request.StatementStartOffset, request.StatementEndOffset, cancellationToken);

        if (string.IsNullOrEmpty(planXml))
        {
            _logger.LogInformation("[{Server}] fetch_plan: the requested plan is not in the cache", displayName);
            /* Succeeded (the fetch ran) but the plan is gone — the viewer shows a "not in cache" info, distinct
               from a failure. planXml is null so the viewer's parse hits the not-in-cache branch. */
            return new CommandOutcome(true, "not in cache",
                JsonSerializer.Serialize(new { success = true, planXml = (string?)null }));
        }

        _logger.LogInformation("[{Server}] fetch_plan returned a {Length}-char plan", displayName, planXml.Length);
        return PlanResultOutcome("plan fetched", planXml);
    }

    /// <summary>
    /// #5320 (the Darling twin of Lite's <c>LivePlanDisplay.Filter</c>): the one place the <c>fetch_plan</c> and
    /// <c>execute_actual_plan</c> handlers turn a plan read live from the monitored server into the command's
    /// <c>result_json</c>. The plan is judged whole by the statement filter BEFORE it is serialized, so the stored
    /// result never holds the raw text (the viewer deletes the row after it reads it, but a row it never reads stays
    /// until the terminal-command purge). The same instance comes back when nothing is named; the whole-plan
    /// marker comes back when the plan cannot be judged, and the viewer shows that as withheld. Nothing cuts the
    /// plan here, so there is no second judge after a cut.
    /// </summary>
    internal static CommandOutcome PlanResultOutcome(string resultStatus, string planXml)
    {
        /* A block body, not an expression body: the statement-column census reads method bodies, and it only sees a
           block (#5367 review round 2, N1). */
        return new(true, resultStatus,
            JsonSerializer.Serialize(new { success = true, planXml = SensitiveStatements.Xml(planXml) }));
    }

    /// <summary>The SQL command timeout (seconds) for the live active-queries DMV read. A "what is running now"
    /// snapshot should return quickly; the shared collector query sets <c>LOCK_TIMEOUT 1000</c> and runs under
    /// READ UNCOMMITTED, so 30s is a wide margin. Bounded well under the viewer's
    /// <c>ActiveQueriesLiveTimeout</c> poll budget so the viewer sees a real "timed out" outcome rather than a
    /// poll miss, and so a wedged read cannot pin the single-threaded command loop past the stale-command reaper.</summary>
    public const int ActiveQueriesFetchTimeoutSeconds = 30;

    /// <summary>
    /// <c>test_hypothetical_index</c> (#2612): plan one stored statement with and without a candidate index.
    ///
    /// <para>The statement text is resolved HERE, from this product's own <c>pg_statement_text</c> store,
    /// keyed by the queryid the caller named. It is never taken from the caller — the request carries an
    /// identifier and a candidate, and nothing else reaches SQL.</para>
    ///
    /// <para>PostgreSQL only, and it says so rather than failing obscurely on a SQL Server target: hypopg
    /// and <c>EXPLAIN (GENERIC_PLAN)</c> have no SQL Server equivalent, and the candidate this answers about
    /// comes from a PostgreSQL-only collector.</para>
    /// </summary>
    private async Task<CommandOutcome> RunTestHypotheticalIndexAsync(
        List<ServerLoopState> servers, int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
    {
        ServerLoopState? server;
        ServerRuntime? runtime;
        MonitoredServer? config;
        string displayName;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
            runtime = server?.Runtime;
            /* #6170: the definition is read with the runtime, in one hold of the lock: the login pair below must be the one the
               address in the runtime was made from, not a later edit's. */
            config = server?.Config;
            displayName = config?.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (server is null || config is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (runtime is null)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — a hypothetical index has to be tested against the server's own statistics, so there is nothing to answer from while it is unreachable"));
        }

        if (!ServerDefinitionEquals(config, runtime.Config))
        {
            return new CommandOutcome(false, "server definition changed",
                JsonError($"server '{displayName}' was edited and is reconnecting; try again once it has reconnected"));
        }

        if (runtime.Target.Engine != CollectorTargetEngine.PostgreSql)
        {
            return new CommandOutcome(false, "not a PostgreSQL target",
                JsonError($"server '{displayName}' is not PostgreSQL. Hypothetical indexes come from the hypopg extension and the plan comparison needs EXPLAIN (GENERIC_PLAN); neither has a SQL Server equivalent, and the index candidate this answers about comes from a PostgreSQL-only collector."));
        }

        if (!request.TryGetQueryId(out var queryId))
        {
            return new CommandOutcome(false, "invalid queryid", JsonError("queryid must be a signed 64-bit integer sent as a STRING"));
        }

        string? statementText;
        await using (var lookup = _postgres!.CreateCommand(
            "SELECT query_text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2"))
        {
            lookup.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;
            lookup.Parameters.AddWithValue(serverId);
            lookup.Parameters.AddWithValue(queryId);
            statementText = (await lookup.ExecuteScalarAsync(cancellationToken)) as string;
        }

        if (string.IsNullOrWhiteSpace(statementText))
        {
            return new CommandOutcome(false, "statement text not captured",
                JsonError($"no statement text is stored for queryid {queryId} on '{displayName}'. Text is refreshed on its own cadence and a major-version upgrade re-keys every queryid, so a statement first seen minutes ago genuinely has none yet — there is nothing to re-plan until it does."));
        }

        /* #6170: the check runs under its own login, never the monitoring login. Planning can evaluate a function a database role created
           when its arguments are constants, and the monitoring login usually holds pg_monitor or pg_read_all_stats. With no login set up
           the check is refused with what to set up, and nothing is sent to the monitored server. */
        var (hypotheticalIndexConnectionString, loginRefusal) = Targets.HypotheticalIndexLogin.Resolve(
            config, runtime.ConnectionString);
        if (hypotheticalIndexConnectionString is null)
        {
            return new CommandOutcome(false, "hypothetical-index login not set up", JsonError(loginRefusal ?? "no login"));
        }

        try
        {
            /* #6143: pinned like every other monitoring connection. The experiment sets the database's own
               search_path only around the EXPLAIN and the hypothetical index it plans against. */
            await using var connection = PostgresTargetProvider.CreatePinnedConnection(hypotheticalIndexConnectionString);
            await connection.OpenAsync(cancellationToken);

            var result = await Targets.HypotheticalIndexExperiment.RunAsync(
                connection, statementText, request.BuildCreateIndexStatement(),
                runtime.Target.PostgresMajorVersion, cancellationToken);

            _logger.LogInformation(
                "[{Server}] test_hypothetical_index on {Schema}.{Table}: planner would {Verdict} it ({Before:N2} -> {After:N2})",
                displayName, request.SchemaName, request.TableName,
                result.PlannerWouldUseIt ? "USE" : "NOT use", result.CostBefore, result.CostAfter);

            return new CommandOutcome(true, "hypothetical index tested", JsonSerializer.Serialize(new
            {
                server = displayName,
                queryid = request.QueryId,
                candidate = request.BuildCreateIndexStatement(),
                planner_would_use_it = result.PlannerWouldUseIt,
                estimated_cost_before = result.CostBefore,
                estimated_cost_after = result.CostAfter,
                hypothetical_index_name = result.HypotheticalIndexName,
                explanation = result.Explanation,
                plan_before = result.PlanBeforeJson,
                plan_after = result.PlanAfterJson,
            }));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (NpgsqlException ex) when (Targets.HypotheticalIndexLogin.IsSignInRuleRefusal(ex))
        {
            /* #6170: the server asked for a sign-in the check does not accept (clear text, MD5, none, or SCRAM without channel binding). Refused
               before the password was sent, with what to change. */
            return new CommandOutcome(false, "hypothetical-index sign-in refused",
                JsonError($"the hypothetical-index connection to '{displayName}' did not sign in: "
                    + $"{CustomAlertEvaluator.SanitizeDisplayText(ex.Message, HypotheticalIndexErrorMaxLength).TrimEnd('.')}. "
                    + Targets.HypotheticalIndexLogin.SignInRule));
        }
        catch (PostgresException ex)
        {
            /* Reported rather than thrown, and the SQLSTATE travels: 42P01 here means the table named in the
               candidate does not exist, which is a caller mistake, and 42501 means the login cannot plan
               against it — two different conversations that a bare failure would merge. The server's text goes to the
               Viewer, so it is cleaned and capped like the pooler text is (#6165). */
            return new CommandOutcome(false, "experiment failed",
                JsonError($"planning on '{displayName}' failed: {CustomAlertEvaluator.SanitizeDisplayText(ex.MessageText, HypotheticalIndexErrorMaxLength)} (SQLSTATE {ex.SqlState})"));
        }
        catch (Exception ex)
        {
            return new CommandOutcome(false, "experiment failed",
                JsonError($"planning on '{displayName}' failed: {CustomAlertEvaluator.SanitizeDisplayText(ex.Message, HypotheticalIndexErrorMaxLength)}"));
        }
    }

    /// <summary>The longest server or driver text the hypothetical-index result carries to the Viewer (#6170).</summary>
    private const int HypotheticalIndexErrorMaxLength = 300;

    /// <summary>
    /// The <c>fetch_active_queries</c> command handler (headless-plan live-snapshot wave): reads the LIVE
    /// running-request DMV snapshot from one monitored server on demand and returns the rows — the on-demand,
    /// live counterpart of the collector's stored hourly snapshot, for the viewer's Current Active Queries tab.
    /// The query and shredding are REUSED from the shared <see cref="QuerySnapshotsCollector"/> (via
    /// <see cref="DarlingCollectorRunner.FetchRowsAsync"/>), so the live rows carry the SAME columns as the
    /// stored ones. Read-only — <c>sys.dm_exec_requests</c>/<c>sessions</c> + the sql_text / query_plan DMFs — so
    /// unlike <see cref="RunExecuteActualPlanAsync"/> it is not consent-class and takes NO collection gate (a DMV
    /// read touches no collector state and writes nothing, so it runs concurrently with a scheduled sweep). The
    /// up-front lookup gives a precise "not monitored" / "not connected" outcome (mirroring
    /// <see cref="RunFetchPlanAsync"/>); a timeout / permission gap / SQL error is caught and reported as a
    /// legible outcome rather than a raw exception (mirroring <see cref="RunExecuteActualPlanAsync"/>).
    /// </summary>
    private async Task<CommandOutcome> RunFetchActiveQueriesLiveAsync(
        List<ServerLoopState> servers, DarlingCollectorRunner runner, int serverId, CancellationToken cancellationToken)
    {
        /* Resolve the runtime under the lock (the command loop reconciles the list concurrently); ServerRuntime is
           immutable, so capturing the reference and using it after the lock is safe. Held only for the lookup. */
        ServerLoopState? server;
        ServerRuntime? runtime;
        string displayName;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
            runtime = server?.Runtime;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (runtime is null)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — the live active queries can only be read from a connected server"));
        }

        try
        {
            var rows = await runner.FetchRowsAsync(
                QuerySnapshotsCollector.Instance, runtime, ActiveQueriesFetchTimeoutSeconds, cancellationToken);
            _logger.LogInformation("[{Server}] fetch_active_queries returned {Count} running request(s)", displayName, rows.Count);
            return new CommandOutcome(true, "active queries fetched", ActiveQueriesLivePayload.Serialize(rows, DateTime.UtcNow));
        }
        catch (OperationCanceledException)
        {
            /* Service shutdown mid-read — let the poller's catch stop the loop cleanly. */
            throw;
        }
        catch (SqlException ex) when (ex.Number == -2)
        {
            return new CommandOutcome(false, "timed out",
                JsonError($"reading the live active queries on '{displayName}' did not finish within the {ActiveQueriesFetchTimeoutSeconds}s budget and was cancelled"));
        }
        catch (SqlException ex) when (IsPermissionError(ex))
        {
            return new CommandOutcome(false, "permission denied",
                JsonError($"the monitoring login for '{displayName}' lacks permission to read the active-query DMVs (VIEW SERVER STATE is required). (SQL error {ex.Number}: {ex.Message})"));
        }
        catch (SqlException ex)
        {
            return new CommandOutcome(false, "sql error",
                JsonError($"reading the live active queries on '{displayName}' failed (SQL error {ex.Number}): {ex.Message}"));
        }
        catch (Exception ex)
        {
            return new CommandOutcome(false, "error",
                JsonError($"reading the live active queries on '{displayName}' failed: {ex.Message}"));
        }
    }

    /// <summary>The store lookup that resolves the actual-plan command's <c>query_stats</c> IDENTIFIER (server_id +
    /// query_hash + database_name) to the latest captured query text + estimated plan XML — the SERVICE's own
    /// copy, so no SQL text ever rides on the command payload. Serves Top Queries, Query-Stats history, and the
    /// FinOps High Impact grid. The third column (isolation level) is NULL here (query_stats does not capture it).
    /// Public const so a test can pin its shape ($1 server_id, $2 query_hash, $3 database_name).</summary>
    public const string ResolveStoredQueryForActualPlanSql = ActualPlanStoredQuery.QueryStatsResolveSql; /* #5639: the viewer reads its prompt text with this same constant */

    /// <summary>The <c>query_store_stats</c> resolver — the Query Store history surface's identifier (server_id +
    /// database_name + query_id) to its captured query text + stored plan. $1 server_id, $2 database_name, $3
    /// query_id. Isolation is NULL (Query Store does not capture it).
    /// <para>
    /// The plan-presence tiebreak leads the sort (#1556): query_store now dedupes plan XML to ONE runtime-stats
    /// interval per plan_id per cycle (NULL on the others), so a plain <c>collection_time DESC</c> could land on
    /// a newer NULL-plan interval of the same query. <c>(query_plan_text IS NOT NULL) DESC</c> first prefers the
    /// row that actually carries the plan; <c>collection_time DESC</c> then breaks ties toward the newest. The
    /// query_text is the same query either way, so this is strictly more robust — the sibling stored-plan
    /// readers' semantics.
    /// </para></summary>
    /* #2150: the text now comes from collect.query_store_text and only FALLS BACK to the fact row's own
       column, which is where it lived before the cutover — pre-cutover rows keep working unchanged, and
       post-cutover rows (NULL inline) resolve from the side table. The lookup is keyed on exactly the
       identifier this resolver already has, and all three keys are the statement's own parameters, so it
       is an uncorrelated scalar subquery: resolved once, then named once by the derived table so the
       IS NOT NULL filter can test the RESOLVED text rather than the raw column. Testing the raw column
       is the trap — it would exclude every post-cutover row, the whole set this change exists to serve. */
    public const string ResolveStoredQueryStoreForActualPlanSql = @"
SELECT r.query_text,
       CASE WHEN m.plan_id IS NULL THEN r.query_plan_text ELSE d.query_plan_xml END AS query_plan_text,
       NULL::text AS transaction_isolation_level,
       d.query_plan_gz
FROM
(
    SELECT
        COALESCE
        (
            (
                SELECT x.query_sql_text
                FROM query_store_text AS x
                WHERE x.server_id = $1
                AND   x.database_name = $2
                AND   x.query_id = $3
            ),
            s.query_text
        ) AS query_text,
        s.query_plan_text,
        s.plan_id,
        s.collection_time
    FROM query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.database_name = $2
    AND   s.query_id = $3
) AS r
/* #5257: since #2210 the plan lives once in query_plan_dim, reached through collect.query_store_plan_map on
   its primary key (server_id, database_name, plan_id) — the fact row's own column is a NULL placeholder. The
   inline column answers ONLY for a plan_id the map has no row for (a pre-cutover plan): a map row with no
   content behind it (a NULL-digest marker, or a digest whose dimension row is gone) is an absent plan, and
   the inline column is not asked for it. */
LEFT JOIN collect.query_store_plan_map AS m
  ON  m.server_id = $1
  AND m.database_name = $2
  AND m.plan_id = r.plan_id
LEFT JOIN query_plan_dim AS d
  ON  d.digest = m.digest
  AND (d.query_plan_xml IS NOT NULL OR d.query_plan_gz IS NOT NULL)
WHERE r.query_text IS NOT NULL
ORDER BY ((m.plan_id IS NULL AND r.query_plan_text IS NOT NULL) OR d.digest IS NOT NULL) DESC, r.collection_time DESC
LIMIT 1";

    /// <summary>The <c>query_snapshots</c> resolver — the Wait drill-down surface's identifier (server_id +
    /// collection_time + session_id) to that captured request's query text, plan (live preferred), and isolation
    /// level. $1 server_id, $2 collection_time, $3 session_id, $4 database_name. The exact-timestamp match keys the
    /// one snapshot the row represents, and the row must also be in the database the request names (the other two
    /// resolvers match the database the same way; a row stored with no database matches a request with none), so
    /// the text that is re-executed is the text of the named database's session.</summary>
    public const string ResolveStoredSnapshotForActualPlanSql = @"
SELECT query_text, COALESCE(live_query_plan, query_plan), transaction_isolation_level, NULL::bytea AS query_plan_gz
FROM query_snapshots
WHERE server_id = $1
AND   collection_time = $2
AND   session_id = $3
AND   database_name IS NOT DISTINCT FROM $4
AND   query_text IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>The command timeout (seconds) for the actual-plan re-execution. Bounded so a runaway query
    /// cannot pin the single-threaded command loop or outlive the stale-command reaper; comfortably under the
    /// viewer's 3-minute poll budget so the viewer sees a real "timed out" outcome rather than a poll miss.</summary>
    public const int ActualPlanCaptureTimeoutSeconds = 120;

    /// <summary>
    /// The <c>execute_actual_plan</c> command handler: RE-EXECUTES a stored query against one monitored server
    /// with SET STATISTICS XML ON to capture its ACTUAL plan, and returns the plan XML. Unlike every other
    /// worker-delegated command this WRITES to the target — re-running an INSERT/UPDATE/DELETE/MERGE re-applies
    /// its changes (the viewer gates this behind informed consent). The command payload is an IDENTIFIER ONLY
    /// (<see cref="ActualPlanRequest"/>): the query text + estimated plan XML are resolved HERE from the service's
    /// OWN store (<see cref="ResolveStoredQueryForActualPlanSql"/>), never from the payload, so a command writer
    /// can never smuggle arbitrary SQL onto a target. The query runs as the server's stored monitoring credential;
    /// the least-privilege VIEW SERVER STATE login has no SELECT on user tables, so a permission failure is caught
    /// and surfaced legibly (as are timeouts), rather than a raw SQL exception.
    /// </summary>
    private async Task<CommandOutcome> RunExecuteActualPlanAsync(
        List<ServerLoopState> servers, int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
    {
        /* #5639: a query_stats request is refused BEFORE any work unless it carries the digest of the text the
           operator confirmed and a hash that names a particular query. */
        var refusedBeforeResolve = RefuseActualPlanRequest(request);
        if (refusedBeforeResolve is not null)
        {
            return new CommandOutcome(false, "request refused", JsonError(refusedBeforeResolve));
        }

        /* Resolve the connection under the lock (the command loop reconciles the list concurrently). Capture the
           immutable connection string + Azure flag as locals — never hold the runtime across the execute. */
        bool serverExists;
        string? connectionString;
        bool isAzureSqlDb;
        string displayName;
        lock (_serversLock)
        {
            var server = servers.Find(s => s.Config.ServerId == serverId);
            serverExists = server is not null;
            connectionString = server?.Runtime?.ConnectionString;
            isAzureSqlDb = server?.Runtime?.Target.IsAzureSqlDb ?? false;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (!serverExists)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (string.IsNullOrEmpty(connectionString))
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — the actual plan can only be captured from a connected server"));
        }

        if (_postgres is null)
        {
            return new CommandOutcome(false, "store unavailable", JsonError("the Postgres store is not available to resolve the query text"));
        }

        /* Resolve the query text + estimated plan + isolation level from the store BY THE IDENTIFIER (the
           collector's own capture) — the identifier-only contract: the payload named a stored row; the service
           supplies the text. The SQL + bound parameters are chosen by the identifier kind (query_stats /
           query_store_stats / query_snapshots). */
        string? queryText = null;
        string? estimatedPlanXml = null;
        string? isolationLevel = null;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(ResolveActualPlanSql(request.Source), connection);
            command.CommandTimeout = ServiceCommandDeadlines.ActualPlanResolveSeconds;
            BindActualPlanResolveParameters(command, serverId, request);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                queryText = reader.IsDBNull(0) ? null : reader.GetString(0);
                /* #2069: query_stats plans written since V54 ride as gzip bytes (column 3) with the
                   text column NULL; the other two resolvers bind NULL::bytea there, so text-else-gz
                   is the one rule for all three source kinds. */
                estimatedPlanXml = PayloadDimensions.ResolveContent(
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(3) ? null : reader.GetFieldValue<byte[]>(3));
                isolationLevel = reader.IsDBNull(2) ? null : reader.GetString(2);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CommandOutcome(false, "store error",
                JsonError($"could not resolve the stored query text for the actual-plan request: {ex.Message}"));
        }

        if (string.IsNullOrWhiteSpace(queryText))
        {
            return new CommandOutcome(false, "no stored query",
                JsonError($"no stored query text was found for this {DescribeActualPlanIdentifier(request)} on '{displayName}' — the actual plan is captured by re-executing the stored query text, which is not available"));
        }

        /* #5639: the store picks the latest text for a hash + database, which can differ from the text the
           operator confirmed in the viewer prompt. Run it only when it is the confirmed text. */
        var refusedChangedText = RefuseChangedActualPlanText(request, queryText);
        if (refusedChangedText is not null)
        {
            _logger.LogInformation("[{Server}] execute_actual_plan: the stored query text no longer matches the confirmed text; nothing was run", displayName);
            return new CommandOutcome(false, "stored query changed", JsonError(refusedChangedText));
        }

        /* RE-EXECUTE via the SHARED executor (SET STATISTICS XML ON), as the server's stored credential. */
        try
        {
            var planXml = await ActualPlanExecutor.ExecuteForActualPlanAsync(
                connectionString,
                request.DatabaseName ?? "",
                queryText,
                estimatedPlanXml,
                isolationLevel: isolationLevel,
                isAzureSqlDb: isAzureSqlDb,
                timeoutSeconds: ActualPlanCaptureTimeoutSeconds,
                cancellationToken,
                productName: "SQL Server Performance Monitor Darling");

            if (string.IsNullOrEmpty(planXml))
            {
                _logger.LogInformation("[{Server}] execute_actual_plan: the query ran but no plan was captured", displayName);
                return new CommandOutcome(true, "no plan captured",
                    JsonSerializer.Serialize(new { success = true, planXml = (string?)null }));
            }

            _logger.LogInformation("[{Server}] execute_actual_plan captured a {Length}-char actual plan", displayName, planXml.Length);
            return PlanResultOutcome("actual plan captured", planXml);
        }
        catch (OperationCanceledException)
        {
            /* Service shutdown mid-execute — let the poller's catch stop the loop cleanly. */
            throw;
        }
        catch (SqlException ex) when (ex.Number == -2)
        {
            return new CommandOutcome(false, "timed out",
                JsonError($"the query did not finish within the {ActualPlanCaptureTimeoutSeconds}s actual-plan capture budget on '{displayName}' and was cancelled"));
        }
        catch (SqlException ex) when (IsPermissionError(ex))
        {
            return new CommandOutcome(false, "permission denied",
                JsonError($"the monitoring login for '{displayName}' lacks permission to execute this query. The least-privilege monitoring login has VIEW SERVER STATE for DMV reads but no SELECT/DML on the queried objects, so the actual plan cannot be captured. (SQL error {ex.Number}: {ex.Message})"));
        }
        catch (SqlException ex)
        {
            return new CommandOutcome(false, "sql error",
                JsonError($"executing the query on '{displayName}' to capture the actual plan failed (SQL error {ex.Number}): {ex.Message}"));
        }
        catch (Exception ex)
        {
            return new CommandOutcome(false, "error",
                JsonError($"executing the query on '{displayName}' to capture the actual plan failed: {ex.Message}"));
        }
    }

    /// <summary>Picks the store-resolution SQL for the actual-plan request's identifier kind.</summary>
    private static string ResolveActualPlanSql(ActualPlanSource source) => source switch
    {
        ActualPlanSource.QueryStats => ResolveStoredQueryForActualPlanSql,
        ActualPlanSource.QueryStore => ResolveStoredQueryStoreForActualPlanSql,
        ActualPlanSource.QuerySnapshot => ResolveStoredSnapshotForActualPlanSql,
        _ => throw new InvalidOperationException($"no store resolver for actual-plan source {source}"),
    };

    /// <summary>
    /// #5639: the checks a <c>query_stats</c> actual-plan request must pass before the store is read. Returns the
    /// refusal message, or null when the request may proceed. The request has to carry the digest of the text the
    /// operator confirmed (a request from a viewer that predates the digest is refused rather than run unchecked)
    /// and a hash that names a particular query (an empty or all-zero hash would let the lookup pick any statement
    /// in the database). Query Store and snapshot requests name one stored row exactly and are not checked.
    /// </summary>
    internal static string? RefuseActualPlanRequest(ActualPlanRequest request)
    {
        if (request.Source != ActualPlanSource.QueryStats)
        {
            return null;
        }

        if (!QueryModificationDetector.IsUsableQueryHash(request.QueryHash))
        {
            return "This row has no usable query hash (it is empty or all zeros), so the stored text cannot be tied to "
                + "the statement you confirmed. Nothing was run.";
        }

        if (string.IsNullOrWhiteSpace(request.ConfirmedSha256))
        {
            return "This request did not include the digest of the query text you confirmed, so nothing was run. "
                + "Update the viewer to the same version as the service and try again.";
        }

        return null;
    }

    /// <summary>
    /// #5639: compares the digest of the text the operator confirmed with the digest of the text the store
    /// resolved. Returns the refusal message on a mismatch, or null when the texts are the same (or the request is
    /// not a <c>query_stats</c> one, which names one stored row exactly).
    /// </summary>
    internal static string? RefuseChangedActualPlanText(ActualPlanRequest request, string resolvedQueryText)
    {
        if (request.Source != ActualPlanSource.QueryStats)
        {
            return null;
        }

        return QueryTextDigest.Matches(request.ConfirmedSha256, resolvedQueryText)
            ? null
            : "The stored query text changed after you confirmed. Nothing was run. Reopen the plan and confirm again.";
    }

    /// <summary>Binds the store-resolution parameters ($1 server_id, then the identifier's $2/$3) for the request's
    /// identifier kind (a snapshot also binds $4, the database name). The snapshot's collection_time binds as a
    /// naive-UTC timestamp (Unspecified), matching how the collector stores it.</summary>
    internal static void BindActualPlanResolveParameters(NpgsqlCommand command, int serverId, ActualPlanRequest request)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        switch (request.Source)
        {
            case ActualPlanSource.QueryStats:
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request.QueryHash! });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request.DatabaseName ?? "" });
                break;
            case ActualPlanSource.QueryStore:
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request.DatabaseName ?? "" });
                command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = request.QueryId!.Value });
                break;
            case ActualPlanSource.QuerySnapshot:
                command.Parameters.Add(new NpgsqlParameter<DateTime>
                {
                    TypedValue = DateTime.SpecifyKind(request.SnapshotCollectionTime!.Value, DateTimeKind.Unspecified),
                });
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = request.SnapshotSessionId!.Value });
                // $4 is nullable text: a stored row with no database matches a request with none (IS NOT DISTINCT FROM).
                command.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text,
                    Value = request.DatabaseName is null ? DBNull.Value : request.DatabaseName,
                });
                break;
            default:
                throw new InvalidOperationException($"no store resolver for actual-plan source {request.Source}");
        }
    }

    /// <summary>A human description of the request's identifier for the "no stored query" message.</summary>
    private static string DescribeActualPlanIdentifier(ActualPlanRequest request) => request.Source switch
    {
        ActualPlanSource.QueryStats => $"query (query_hash {request.QueryHash})",
        ActualPlanSource.QueryStore => $"Query Store query (query_id {request.QueryId})",
        ActualPlanSource.QuerySnapshot => $"query snapshot (session {request.SnapshotSessionId})",
        _ => "row",
    };

    /// <summary>
    /// Where the missing extension has to be created, named when we know it (#2638).
    /// </summary>
    private static string WhereToCreateIt(string? connectedDatabase)
        => string.IsNullOrWhiteSpace(connectedDatabase)
            ? "in the connected database (CREATE EXTENSION ...). "
            : $"in database '{connectedDatabase}', which is the one this collector connects to — an "
              + "extension installed in a DIFFERENT database on the same cluster is invisible from here, "
              + $"so run CREATE EXTENSION in '{connectedDatabase}'. ";

    /// <summary>
    /// The self-hosted log readers (#3239; three since #3601). Their dispatch entries send Aurora and RDS to
    /// the log-API ingestors, so a target-side PostgresException under any of the names comes from the
    /// pg_read_file route.
    /// Consulted by two arms: the 42501 grant-pair sentence (#3239) and the 58P01 missing-file sentence
    /// (#3410), which is why it names the ROUTE rather than either fault.
    /// </summary>
    private static bool ReadsServerLogWithPgReadFile(string collectorName)
        => collectorName is "pg_deadlocks" or "pg_plan_capture" or "pg_log_events";

    /// <summary>
    /// Where the pg_read_file grants have to be issued, named when we know it (#3239) — function ACLs are
    /// per-database catalogs, the same fact <see cref="WhereToCreateIt"/> names for extensions. Measured
    /// on the 20260910 dogfood soak: the pair issued in the wrong database leaves a failure identical to
    /// no grant at all.
    /// </summary>
    private static string WhereToGrantIt(string? connectedDatabase)
        => string.IsNullOrWhiteSpace(connectedDatabase)
            ? "in the database this collector connects to."
            : $"in database '{connectedDatabase}', the one this collector connects to — issued in a "
              + "DIFFERENT database on the same cluster, they change nothing here.";

    /// <summary>
    /// The extensions <paramref name="collectorName"/> declares it cannot run without
    /// (<see cref="ICollectorSchemaInfo.RequiredPgExtensions"/>, #3191) — the seam #3240's classification
    /// consults. Empty for a collector the catalog does not know, which keeps an unknown name on the
    /// generic missing-object arm rather than inventing a dependency for it.
    /// </summary>
    private static IReadOnlyList<PgExtensionDependency> RequiredExtensionsOf(string collectorName) =>
        CollectorCatalog.Find(collectorName)?.RequiredPgExtensions ?? Array.Empty<PgExtensionDependency>();

    /// <summary>
    /// The sentence an <c>EXTENSION_MISSING</c> row carries (#3240): the raw error, the DECLARED extension
    /// by name, the exact <c>CREATE EXTENSION</c> to run and where (#2638's per-database caution kept — an
    /// extension installed in a different database on the same cluster is invisible from here), and the
    /// <c>shared_preload_libraries</c> restart when the declaration says installing costs one. Ends on the
    /// same retry promise the generic arm makes, because it is the same machinery: the collector retries
    /// every cycle and starts collecting on the first one after the extension exists.
    /// </summary>
    private static string ExtensionMissingExplanation(
        PostgresException ex, IReadOnlyList<PgExtensionDependency> required, string? connectedDatabase)
    {
        var names = string.Join(", ", required.Select(r => r.ExtensionName));
        var create = string.Join("; ", required.Select(r => $"CREATE EXTENSION {r.ExtensionName}"));
        var noun = required.Count == 1 ? "extension" : "extensions";

        var where = string.IsNullOrWhiteSpace(connectedDatabase)
            ? $"run {create} in the connected database (extensions are per-database). "
            : $"run {create} in database '{connectedDatabase}', which is the one this collector connects "
              + "to — an extension installed in a DIFFERENT database on the same cluster is invisible "
              + "from here. ";

        var preload = required.Any(r => r.InstallKind == PgExtensionInstallKind.SharedPreloadLibraries)
            ? $"The module also has to be in shared_preload_libraries first, which takes a server restart "
              + "(a parameter-group change plus a reboot on Aurora/RDS) and leaves it inert until then "
              + "whatever else is installed. "
            : string.Empty;

        return $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the {names} {noun} this collector reads is "
            + "not installed on this target. This is NOT a missing grant, and no GRANT will change it: "
            + where
            + preload
            + "Recorded as a named non-fatal skip rather than an error so it does not fill the log every "
            + "cycle; the collector retries every cycle and starts collecting on the first one after the "
            + $"{noun} exists.";
    }

    /// <summary>
    /// The object a 42P01 / 42883 names, as its bare last segment (#3818): <c>relation
    /// "public.pg_stat_statements_info" does not exist</c> yields <c>pg_stat_statements_info</c>;
    /// <c>function aurora_stat_statements(boolean) does not exist</c> yields <c>aurora_stat_statements</c>.
    /// Null when the message has neither shape - a non-English <c>lc_messages</c>, or a message this did not
    /// anticipate - which keeps the caller on the arm it would have taken before this existed rather than
    /// guessing. The relation and function fields Npgsql exposes are NOT consulted: PostgreSQL fills them for
    /// constraint and datatype errors, not for a name it could not resolve.
    /// </summary>
    internal static string? MissingObjectNamedBy(PostgresException ex)
    {
        var text = ex.MessageText ?? string.Empty;

        var relation = System.Text.RegularExpressions.Regex.Match(
            text, "^relation \"(?<name>[^\"]+)\" does not exist", System.Text.RegularExpressions.RegexOptions.CultureInvariant);
        var function = System.Text.RegularExpressions.Regex.Match(
            text, "^function (?<name>[^\\s(]+)\\(", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

        var qualified = relation.Success ? relation.Groups["name"].Value
            : function.Success ? function.Groups["name"].Value
            : null;

        if (qualified is null)
        {
            return null;
        }

        var lastDot = qualified.LastIndexOf('.');
        return lastDot >= 0 ? qualified[(lastDot + 1)..] : qualified;
    }

    /// <summary>
    /// The declared companion (<see cref="PgExtensionDependency.Companions"/>) a missing-object fault names,
    /// with the extension it belongs to - or null when the fault names the extension's base object, an
    /// undeclared object, or nothing this can read (#3818). Ordinal on the bare name, both sides lowercase
    /// by declaration and by PostgreSQL's own folding.
    /// </summary>
    internal static (PgExtensionDependency Extension, PgExtensionCompanionObject Companion)? MissingCompanionOf(
        PostgresException ex, IReadOnlyList<PgExtensionDependency> required)
    {
        var missing = MissingObjectNamedBy(ex);
        if (missing is null)
        {
            return null;
        }

        foreach (var extension in required)
        {
            foreach (var companion in extension.Companions)
            {
                if (string.Equals(companion.ObjectName, missing, StringComparison.Ordinal))
                {
                    return (extension, companion);
                }
            }
        }

        return null;
    }

    /// <summary>
    /// The sentence a missing COMPANION object carries (#3818): the raw error, the object the server actually
    /// named, what the catalog says about the extension, and the remedy that follows FROM THAT STATE. Not
    /// <see cref="ExtensionMissingExplanation"/>, whose sentence says the extension is not installed and gives
    /// <c>CREATE EXTENSION</c> plus a preload restart: on the upgraded-fleet case that motivated #3818 the
    /// extension was installed, preloaded and readable, and all three of those were false.
    ///
    /// <para><b>Four states, four sentences (#3830), because one remedy for all of them was wrong on the
    /// fleet it shipped for.</b> #3818's sentence had a single remedy, <c>ALTER EXTENSION ... UPDATE</c>,
    /// inferred from the ordering property alone - the base object resolved, therefore the extension is
    /// present, therefore it is present at an old version. The middle step does not survive the Aurora
    /// flavor: there the base object that resolved is <c>aurora_stat_statements()</c>, which is not the
    /// extension's at all and needs none, so "it resolved" licenses nothing about <c>pg_extension</c>. The 23
    /// clusters that produced #3818 were exactly that case - no row, never created, the collector productive
    /// throughout - and they were told to update an extension they did not have.</para>
    ///
    /// <para><b>So the state ARRIVES rather than being inferred.</b> <paramref name="extensionRow"/> is the
    /// <c>pg_extension</c> row the connect-time read observed, scoped to the database this fault came from,
    /// and <see cref="PgExtensionCompanionObject.VerdictFrom"/> draws the verdict from it here. When nothing
    /// was observed the sentence falls back to the two-possibilities form, which is the most #3818's
    /// inference could honestly claim - a hedge, kept only where there is no answer to prefer over it.</para>
    /// </summary>
    /// <param name="extensionRow">The <c>pg_extension</c> row for the declared extension, as observed at
    /// connect. <c>default</c> (not observed) is the safe value and produces the fallback sentence.</param>
    /// <param name="isAurora">Whether the target is Aurora, which decides only what an ABSENT extension
    /// costs - together with the collector's declared <c>AuroraNativeAlternative</c>.</param>
    private static string CompanionMissingExplanation(
        PostgresException ex,
        PgExtensionDependency extension,
        PgExtensionCompanionObject companion,
        string? connectedDatabase,
        PgExtensionRowObservation extensionRow,
        bool isAurora)
    {
        var where = string.IsNullOrWhiteSpace(connectedDatabase)
            ? "in the connected database"
            : $"in database '{connectedDatabase}'";

        var head = $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the missing object is {companion.ObjectName}, "
            + $"which is NOT the {extension.ExtensionName} extension's base object";

        const string tail = "This is "
            + "NOT a missing grant, and no GRANT will change it. Recorded as a non-fatal skip rather than an "
            + "error so it does not fill the log every cycle; the collector retries every cycle.";

        /* The row is only usable if it was read in the database this fault came from: pg_extension is per
           database, and a RunsPerDatabase collector faults in databases the connect-time read never saw. A
           mismatch degrades to "not observed", which is the fallback sentence, never a claim about the
           wrong catalog. */
        var verdict = companion.VerdictFrom(extensionRow.InDatabase(connectedDatabase));

        switch (verdict)
        {
            /* NO ROW. The extension was never created in that database, so there is no version to update:
               ALTER EXTENSION would raise, and CREATE EXTENSION is the only statement that changes
               anything. Whether it is WORTH running is the flavor's question, and the collector answers it
               by declaring what it reads on Aurora instead. */
            case PgExtensionCompanionVerdict.NoRow when isAurora && extension.AuroraNativeAlternative is { } auroraSource:
                return head + $". pg_extension has NO row for {extension.ExtensionName} {where}: the extension "
                    + "was never created there, so it is not present at any version and there is nothing for "
                    + $"ALTER EXTENSION {extension.ExtensionName} UPDATE to update. This target is Aurora, where "
                    + $"this collector's source is {auroraSource} — which needs no extension — so the collector "
                    + $"is not waiting on one: running CREATE EXTENSION {extension.ExtensionName} {where} is "
                    + $"OPTIONAL, and what it buys is {companion.ObjectName} and nothing else. " + tail;

            case PgExtensionCompanionVerdict.NoRow:
                return head + $". pg_extension has NO row for {extension.ExtensionName} {where}: the extension "
                    + "was never created there, so it is not present at any version and there is nothing for "
                    + $"ALTER EXTENSION {extension.ExtensionName} UPDATE to update. The remedy is CREATE EXTENSION "
                    + $"{extension.ExtensionName} {where}"
                    + (extension.InstallKind == PgExtensionInstallKind.SharedPreloadLibraries
                        ? $", and {extension.ExtensionName} has to be in shared_preload_libraries before that "
                          + "does anything — a parameter-group change and a restart. "
                        : ". ")
                    + tail;

            /* BELOW the companion's version: #3818's case, and now stated as a fact read out of the catalog
               rather than as one of two possibilities. */
            case PgExtensionCompanionVerdict.BelowCompanionVersion:
                return head + $": that resolved, so the extension IS installed {where} and this is not the "
                    + "extension missing. pg_extension says it is at catalog version "
                    + $"{extensionRow.ExtensionVersion}, below the {companion.SinceExtensionVersion} whose update "
                    + $"script creates {companion.ObjectName} — an engine upgraded in place or restored keeps the "
                    + "version the extension was created at until someone runs ALTER EXTENSION "
                    + $"{extension.ExtensionName} UPDATE {where} — a statement, with no restart and no "
                    + "shared_preload_libraries change; RDS and Aurora do not run it for you. " + tail;

            /* AT OR ABOVE: the update everything else would have recommended is already done, so recommending
               it again is the wrong-remedy defect in its third form. */
            case PgExtensionCompanionVerdict.AtOrAboveCompanionVersion:
                return head + $": that resolved, so the extension IS installed {where} and this is not the "
                    + "extension missing. pg_extension says it is at catalog version "
                    + $"{extensionRow.ExtensionVersion}, which already carries {companion.ObjectName} "
                    + $"({companion.SinceExtensionVersion} and later), so no ALTER EXTENSION "
                    + $"{extension.ExtensionName} UPDATE is owed and it would change nothing. The object is "
                    + "installed outside the schema the query text names (a relocatable extension lives in "
                    + "whatever schema it was created in; check pg_extension.extnamespace), or the error text "
                    + "above names a reason of its own. " + tail;

            /* NOTHING OBSERVED — or a version string nothing can rank. The two-possibilities sentence, which
               is everything the fault alone supports. */
            default:
                return head + ": that resolved, so the "
                    + $"extension IS installed {where} and this is not the extension missing. {companion.ObjectName} "
                    + $"is created by the extension's {companion.SinceExtensionVersion} update script, so either the "
                    + $"extension is present at a catalog version below {companion.SinceExtensionVersion} (an engine "
                    + "upgraded in place or restored keeps the version the extension was created at until someone "
                    + $"runs ALTER EXTENSION {extension.ExtensionName} UPDATE {where} — a statement, with no restart "
                    + "and no shared_preload_libraries change; RDS and Aurora do not run it for you) or the object "
                    + "is installed outside the schema the query text names (a relocatable extension lives in whatever "
                    + "schema it was created in; check pg_extension.extversion and extnamespace). " + tail;
        }
    }
    /// <summary>
    /// #4046 part 1c: the general handler's one call for a fault on a log-tail read (#4051 round-2 review). It
    /// returns <see cref="LogTailUndecodableByteExplanation"/>'s sentence, or null, and it drops a stale
    /// pg_read_binary_file verdict through <see cref="DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict"/>.
    /// The sentence is built first, because it reads the verdict that the second step can drop. Nothing here
    /// allocates on the way to null for a fault that is not a <see cref="PostgresException"/>, because the general
    /// handler is also the OutOfMemoryException landing pad.
    ///
    /// <para>#4251 round-1 review, M1: also drops a stale pg_file_settings verdict through
    /// <see cref="DarlingCollectorRunner.ForgetStaleFileSettingsVerdict"/> — a no-op here in practice, since a
    /// pg_server_config 42501 classifies PERMISSIONS and is caught before reaching this general handler (see
    /// the other call at the PERMISSIONS arm below), but called anyway so this stays the one place every
    /// stale-verdict check for a fault this handler sees is reached from, the same way the read-binary-file
    /// one already is.</para>
    /// </summary>
    internal static string? LogTailGeneralFault(Exception ex, string collectorName, ServerRuntime runtime)
    {
        var explanation = LogTailUndecodableByteExplanation(ex, collectorName, runtime);
        DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict(collectorName, ex, runtime);
        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict(collectorName, ex, runtime);
        return explanation;
    }

    /// <summary>
    /// #4046 part 1c: the sentence for a log-tail read that PostgreSQL refused because of one byte, or null for
    /// any other fault. <c>pg_read_file()</c> returns text, which PostgreSQL checks before this process sees a
    /// byte. A failed login can plant a bad byte in the FATAL message's unescaped %u/%d echo, under any
    /// log_line_prefix, and that byte fails the WHOLE tail read for as long as it sits inside the window. Every
    /// reader sharing PgServerLogTail's tail CTE goes blind, not just the one that read this cycle.
    ///
    /// <para><b>Two sentences.</b> On a UTF8 or SQL_ASCII database the fault is a 22021, and the sentence names
    /// the pg_read_binary_file grant that moves the reader to the byte route. On any other encoding the byte
    /// route does not apply (<see cref="PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding"/>), and the
    /// fault is a 22021 (EUC encodings) or a 22P05 (a WIN1252 byte with no UTF-8 equivalent). There the sentence
    /// says that the grant does not help and names #4062 (#4051 round-2 review, L-1).</para>
    ///
    /// <para><b>ERROR, not PERMISSIONS (#4051 review M1).</b> The general handler records this under ERROR with
    /// the sentence as its message, rather than PostgresFaultOutcome giving it a PERMISSIONS arm. Anyone who can
    /// attempt a login can plant the byte, so the blinding has to count in error_count and the daily error
    /// figures. A PERMISSIONS row drops out of both, and a reader an attacker blinds most of the week would
    /// still band HEALTHY whenever one cycle got through.</para>
    ///
    /// <para>Null for a proven write to the STORE (#3111's rule, #4051 review L3): a 22021 the store's COPY
    /// raises is not about the target's log. The checks run cheapest first and allocate nothing on the way to
    /// null, because the general handler is also the OutOfMemoryException landing pad.</para>
    /// </summary>
    internal static string? LogTailUndecodableByteExplanation(Exception ex, string collectorName, ServerRuntime runtime)
    {
        if (ex is not PostgresException { SqlState: "22021" or "22P05" } pg
            || !ReadsServerLogWithPgReadFile(collectorName)
            || CollectorFaultCopyPhase.IsProvenStoreWrite(ex))
        {
            return null;
        }

        /* #4735 item 1: every 22021 that reaches this handler has already been retried with the read start moved forward
           by 1, 2 and 3 bytes (DarlingCollectorRunner.RunWithSplitCharacterRetryAsync), either in this cycle or in the
           cycle that first met it, after which each cycle makes one read until a read succeeds. The sentence says what
           the attempts were and does not blame a planted byte alone. A 22P05 is a conversion fault, not a start offset. */
        var splitCharacter = pg.SqlState == PgServerLogTail.EncodingRefusalSqlState
            ? "A read with no saved position starts 4 MB before the end of the log, and that start can fall inside a "
              + "multi-byte character, which PostgreSQL refuses the same way. The collector retried the read from 1, 2 and 3 "
              + "bytes later and every attempt was refused. Until a read succeeds it reads once per cycle and does not retry. "
              + "Once a read succeeds, every later read resumes from the start of a full line, so a split character clears "
              + "by itself. "
            : string.Empty;

        const string Planted = " A client can plant such a byte with nothing more than a failed login. The role or "
            + "database name that the client sends lands unescaped in the FATAL message (#4046).";

        if (pg.SqlState == "22P05" || PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding(runtime.StorageName))
        {
            return $"{pg.MessageText} (SQLSTATE {pg.SqlState}). " + splitCharacter
                + "The log tail that this cycle read contains a byte "
                + "that this database's encoding cannot pass to this collector, so PostgreSQL refused the whole read."
                + Planted
                + " Granting pg_read_binary_file does not help on this database. The binary route decodes the log in "
                + "the database's own server_encoding, but this collector does not map this database's encoding "
                + "(EUC_TW, EUC_JIS_2004, LATIN6, LATIN8, LATIN10, MULE_INTERNAL, or one this runtime cannot resolve). "
                + "The read fails until the line with the byte leaves the 4 MB tail window.";
        }

        return $"{pg.MessageText} (SQLSTATE {pg.SqlState}). " + splitCharacter
            + "The log tail that this cycle read contains a byte that "
            + "is not valid UTF-8, so PostgreSQL refused the whole read. pg_read_file() returns text, and PostgreSQL "
            + "checks text before this collector sees a row, even when only one byte in the 4 MB window is bad."
            + Planted
            + " Grant EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint) to the monitoring role "
            + WhereToGrantIt(CollectorFaultDatabase.For(ex, runtime.ConnectedDatabase))
            + " From the next cycle, this collector reads the same bytes as bytea, which PostgreSQL does not check. "
            + "An invalid byte then shows as U+FFFD instead of stopping the read.";
    }

    /// <summary>
    /// Maps a PostgreSQL fault to a collection_log status plus the sentence an operator needs.
    /// <para>PERMISSIONS is the non-fatal-degradation bucket for the cases whose absent thing the code
    /// cannot name — a denied grant, an undeclared missing object, a disabled feature — and the MESSAGE
    /// distinguishes them, the same division the Azure service-objective hint already uses. A missing
    /// object on a collector that DECLARES the extension it reads is not one of those: since #3240 it gets
    /// its own <c>EXTENSION_MISSING</c> status, so the health surfaces can band it apart from a grant
    /// problem instead of hinting at <c>pg_monitor</c>. A missing FILE on one of the two log readers is
    /// not one of those either — there the absent thing IS nameable, because the path came from the
    /// server's own answers, so its arm names it (#3410). Returning "ERROR" means "let the general handler
    /// have it", which keeps the genuinely unexpected loud.</para>
    /// </summary>
    /// <param name="extensionRow">#3830: the <c>pg_extension</c> row a PostgreSQL connect observed for the
    /// declared extension, which the companion arm needs and the exception cannot carry. <c>default</c> means
    /// nothing was observed, and every arm treats that as "no answer" rather than as "no row".</param>
    /// <param name="isAurora">#3830: whether the target is Aurora, which decides what an ABSENT extension
    /// costs a collector that declares an Aurora-native alternative.</param>
    internal static (string Status, string Explanation) PostgresFaultOutcome(
        PostgresException ex,
        string collectorName,
        string? connectedDatabase = null,
        PgExtensionRowObservation extensionRow = default,
        bool isAurora = false)
    {
        var fault = PostgresTargetProvider.Instance.Classify(
            ex, CollectorCatalog.YieldsOnLockTimeout(collectorName));

        return fault switch
        {
            /* #3239: the two self-hosted log readers are the exception the general sentence below used to
               deny to their faces. Reading the log with pg_read_file needs the pg_read_server_files role
               AND an explicit EXECUTE grant — measured on #2566, the role alone does NOT carry it, because
               the function's ACL is postgres=X/postgres — and the EXECUTE half lives in each database's
               own catalog, so the grants only count in the database this collector connects to. The old
               hint said pg_monitor covers everything, which sent an operator in a circle: the product's
               own changelog knew better. */
            CollectorTargetFault.Permissions when ReadsServerLogWithPgReadFile(collectorName) => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — this collector reads the server log with "
                + "pg_read_file(), which pg_monitor does NOT cover. The monitoring login needs BOTH the "
                + "pg_read_server_files role AND an explicit GRANT EXECUTE ON FUNCTION pg_read_file(text), "
                + "pg_read_file(text, bigint, bigint), pg_read_file(text, bigint, bigint, boolean) — the "
                + "role alone does not carry EXECUTE, because the function's ACL is postgres=X/postgres. "
                + "EXECUTE grants live in each database's own catalog, so issue them "
                + WhereToGrantIt(connectedDatabase)
                + " While issuing that grant, also GRANT EXECUTE ON FUNCTION pg_read_binary_file(text, "
                + "bigint, bigint) to the same role (#4046): a byte that is not valid UTF-8 makes "
                + "pg_read_file() fail the whole tail read, pg_read_binary_file() does not, and the "
                + "collector switches to it on its own once it is granted."),

            CollectorTargetFault.Permissions => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the monitoring login lacks a grant this "
                + "source needs. pg_monitor covers every collector here; check that it is granted."),

            /* #3410, the half #3414 left open: 58P01 and 42501 were told apart in the classifier —
               Permissions versus Unclassified — and nowhere the operator looks. Both rendered as a failed
               collector, so the natural first move was the grant chase, which is right for 42501 and a
               dead end here: no GRANT changes a missing file. Scoped to the two log readers because for
               them a missing file is a nameable state of the source — the path was built from the server's
               own answers moments earlier — while a 58P01 anywhere else is genuinely unexpected and must
               stay loud, which the default arm below still is. Keyed on Unclassified deliberately: the
               classifier LEAVES 58P01 there so every other collector keeps the loud default, and this arm
               is the exception being made, not a reclassification. */
            CollectorTargetFault.Unclassified when ReadsServerLogWithPgReadFile(collectorName) && ex.SqlState == "58P01" =>
                ("PERMISSIONS",
                    $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the server named the exact file it could "
                    + "not OPEN, and that path is the fact to act on: 58P01 is a missing FILE, not a missing "
                    + "grant, and no GRANT changes it — a denial is 42501 and gets different advice. The "
                    + "path is built from the server's own answers, current_setting('log_directory') joined "
                    + "to a name pg_ls_logdir() returned in the same statement, and it resolves on the "
                    + "MONITORED server's filesystem, not the monitoring host's. A one-off is a file removed "
                    + "between the listing and the read and heals on the next cycle; one that repeats every "
                    + "cycle means the directory log_directory names no longer holds what the listing said, "
                    + "which is worth a look at that server's setting and what actually sits behind it. A "
                    + "server whose logging_collector is off does not land here — that is detected first "
                    + "and recorded as its own named state."),

            /* #4046 part 1c: a 22021 on a log-tail reader has no arm here on purpose. It keeps ERROR, and the
               general handler records it with LogTailUndecodableByteExplanation's sentence instead. */

            /* #3240: a missing source object on a collector that DECLARES its extension dependency
               (ICollectorSchemaInfo.RequiredPgExtensions, #3191) is not ambiguous — the absent thing is
               that extension, installing it is the remedy, and no grant changes anything. Recorded under
               its own status so Collection Health bands it EXTENSION_MISSING rather than NO_PERMISSIONS
               with a pg_monitor hint, and the sentence names the extension instead of describing the
               error's shape. Undeclared sources keep the arm below: a version-gated relation, or an
               extension-owned object nothing declares, still presents as 42P01/42883, and for those the
               generic sentence is the honest one. */
            /* #3818, the exception to #3240's inference, checked FIRST because it is the narrower claim: when the
               object the server could not find is a COMPANION the collector declared beside its base object,
               the extension is present - the base object resolved before the companion was looked up - and
               "not installed" would be false. The remedy is ALTER EXTENSION ... UPDATE, not CREATE EXTENSION,
               and the status is the general non-fatal-degradation bucket rather than EXTENSION_MISSING,
               because that status word says the extension is missing and every health surface reads it as an
               optional module left uninstalled - a legitimate resting state. A previously productive collector
               dark on a version-gated companion is not resting. The text is where the truth goes, as it is on
               the generic arm below. */
            CollectorTargetFault.ObjectMissing when MissingCompanionOf(ex, RequiredExtensionsOf(collectorName)) is { } found =>
                (CollectorRuntimePrecondition.DegradedStatus,
                    CompanionMissingExplanation(
                        ex, found.Extension, found.Companion, connectedDatabase, extensionRow, isAurora)),

            CollectorTargetFault.ObjectMissing when RequiredExtensionsOf(collectorName) is { Count: > 0 } required =>
                (CollectorRuntimePrecondition.ExtensionMissingStatus,
                    ExtensionMissingExplanation(ex, required, connectedDatabase)),

            /* 42P01 / 42883: the relation or function is not there. Overwhelmingly an extension that was
               never created in the connected database rather than anything to do with privileges. */
            /* #2638: the database is NAMED. Extensions are per-database, and on a real fleet one was
               installed in a different database on the same cluster from the one this collector connects
               to — so an operator who checked the obvious database found it already there and concluded
               the collector was broken. "Create it somewhere" is not an instruction; naming the database
               makes it one. Threaded from ServerRuntime.ConnectedDatabase; when that is unknown the
               sentence degrades to what it always said rather than inventing a name. */
            CollectorTargetFault.ObjectMissing => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the source object does not exist on this "
                + "target. This is NOT a missing grant: it is normally an extension that was never created "
                + WhereToCreateIt(connectedDatabase)
                + "The collector will keep degrading until it is. Recorded as a non-fatal skip rather than "
                + "an error so it does not fill the log every cycle."),

            /* 0A000 / 55000 / 55006: the server will not do this, permanently or by configuration —
               pg_stat_wal on Aurora, or an optimized-reads cache that is switched off. */
            CollectorTargetFault.FeatureDisabled => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — this source is unsupported or disabled on "
                + "this server. NOT a missing grant. Aurora does not implement some community sources at "
                + "all, and others are gated by a parameter group. Recorded as a non-fatal skip because it "
                + "will not change until the platform or the parameter group does."),

            CollectorTargetFault.LockTimeoutYield => ("YIELDED",
                $"Lock-timeout yield (SQLSTATE {ex.SqlState}): the collector's lock-timeout guard fired "
                + "rather than waiting in a blocking chain. One sweep skipped; evidence of lock contention "
                + "on the monitored server, not a monitoring failure."),

            /* Everything else belongs to the general handler, which logs ERROR and (for
               ConnectionFatal) forces the reprobe. A command timeout no longer arrives here as raw
               transport text — see PostgresTimeoutExplanation and the arm that calls it — but it
               still lands on ERROR, because a deadline that collected nothing IS a collection
               failure and must stay inside the error counts that feed collector health. */
            _ => ("ERROR", ex.Message),
        };
    }

    /// <summary>
    /// The sentence a PostgreSQL command timeout gets instead of the transport's seven words (#2997).
    ///
    /// <para><b>Why this is not folded into <see cref="PostgresFaultOutcome"/>.</b> Two reasons, and
    /// neither is that a client-side deadline cannot produce a <see cref="PostgresException"/> — it can,
    /// when the server's <c>57014</c> response beats the tearing stream. The first is the PARAMETER TYPE:
    /// that method takes a <see cref="PostgresException"/>, and the shape a client-side deadline
    /// normally arrives in is an <c>NpgsqlException</c> wrapping a <c>TimeoutException</c> with no
    /// SQLSTATE at all, so it cannot be reached through a SQLSTATE map however wide that map grew. The
    /// second is that the answer is not a function of the code: the split below turns on the message
    /// text, which a switch over SQLSTATE cannot express whatever it is handed.</para>
    ///
    /// <para><b>The status stays ERROR.</b> The store has five and none of them means "ran out of time";
    /// PERMISSIONS is the non-fatal-degradation bucket and would wrongly exclude this from the error
    /// counts, the health band and the collection-failure self-alerts, which is precisely where a
    /// collector that has never once succeeded belongs. What changes is that the row is now readable:
    /// the message names the collector, the database, the mechanism and the measured elapsed time, and
    /// the caller records that elapsed figure instead of a literal zero.</para>
    ///
    /// <para><paramref name="origin"/> splits the deadlines that all classify as
    /// <see cref="CollectorTargetFault.CommandTimeout"/>, because the remedy is on a different machine for
    /// each. <b>The SQLSTATE does not carry that split and the message text does</b>, which is the reverse
    /// of what the code alone suggests: 57014 is <c>query_canceled</c>, which PostgreSQL raises for the
    /// target's <c>statement_timeout</c>, for a <c>pg_cancel_backend()</c>, and for the client
    /// CancelRequest Npgsql sends when its own <c>CommandTimeout</c> expires — while
    /// <c>canceling statement due to statement timeout</c> and
    /// <c>canceling statement due to user request</c> are different strings. So the parameter is not a bool
    /// over the code: that expression is
    /// <see cref="PgBaselineProvider.IsCommandTimeout"/>'s first disjunct, where it correctly identifies
    /// OUR deadline, and one expression cannot mean both. <see cref="CollectorFaultCancelOrigin.For"/>
    /// reads the text instead, and reports <see cref="PostgresCancelSource.Unproven"/> for every wording it
    /// does not recognise so that a translated or reworded message costs the weaker sentence rather than
    /// the wrong machine.</para>
    /// </summary>
    internal static string PostgresTimeoutExplanation(
        string collectorName, string? connectedDatabase, long elapsedMs, CollectorFaultCancelOrigin origin)
    {
        var where = string.IsNullOrWhiteSpace(connectedDatabase)
            ? "the connected database"
            : $"database '{connectedDatabase}'";

        /* Invariant culture on the grouping separator: this string is read by operators and compared
           across rows, and a machine-dependent thousands separator makes two identical durations look
           like different ones. */
        var elapsed = elapsedMs.ToString("N0", CultureInfo.InvariantCulture);

        return origin.Source switch
        {
            PostgresCancelSource.TargetStatementTimeout =>
                $"{collectorName} on {where} was CANCELLED BY THE SERVER after {elapsed} ms (SQLSTATE "
                + $"{CollectorFaultCancelOrigin.QueryCanceled}) — the target's own statement_timeout "
                + "expired, so the deadline that fired is on the monitored server, not here. Nothing was "
                + "collected this cycle: this is NOT 'there was nothing to collect'.",

            /* "its command timeout", never the name of a knob. This arm fires for EVERY PostgreSQL
               collector classified as CommandTimeout, and only two of them set
               CommandTimeoutSecondsOverride at all - the rest fall back to
               DarlingCollectorRunner.CommandTimeoutSeconds. Naming the override would therefore be
               false for most collectors that can reach here, and would send an operator looking for a
               setting that collector does not have. The MEASURED elapsed time above says what the
               deadline actually was, which is the more useful number anyway: it is what applied,
               rather than what was configured somewhere.

               The no-SQLSTATE clause is why this sentence belongs to this arm alone and is not the
               fall-through for everything that is not the server's statement_timeout: it describes the
               shape Npgsql produces when its own deadline fires and it never reaches the server's error
               response. A cancel that DID arrive as SQLSTATE 57014 would be contradicted by its own
               explanation. */
            PostgresCancelSource.OurCommandDeadline =>
                $"{collectorName} on {where} hit its CLIENT-SIDE command deadline after {elapsed} ms — "
                + "its command timeout expired and Npgsql cancelled the read mid-stream, which is why the "
                + "transport reports 'Exception while reading from stream' with no SQLSTATE. The statement "
                + "was still running when it was cut off, so the work asked for does not fit the deadline; "
                + "raising the deadline is the wrong half of that and shrinking the work is the right one. "
                + "Nothing was collected this cycle: this is NOT 'there was nothing to collect'.",

            /* The honest arm, and the one this method exists to be able to reach. A 57014 whose wording is
               not the statement_timeout one was cancelled by SOMETHING, and the code names neither the
               machine nor the knob; the wording is quoted rather than interpreted, because a human reading
               a translated or reworded message can place it and this code cannot. It rules the target's
               statement_timeout OUT, which is the actionable half - the alternative on offer is a
               confident sentence about whichever machine the coin landed on.

               The code is DESCRIBED as PostgreSQL's behaviour, never attributed to this fault, and that
               distinction is load-bearing. Unproven is reachable for a fault carrying no SQLSTATE at all -
               CollectorFaultCancelOrigin.For is total on purpose, so that it does not rest on the arm's
               filter - and a sentence reading "(SQLSTATE 57014)" would then assert a code the fault never
               carried. Resting the SENTENCE on the filter instead of the classifier would be this same
               defect one layer up: a confident claim true only because of something a caller elsewhere
               happens to do. The two arms above assert nothing beyond the CommandTimeout classification
               this whole method is handed, which is the caller's to guarantee for all three. */
            _ => $"{collectorName} on {where} was CANCELLED after {elapsed} ms, and this fault does not "
                 + "say WHOSE deadline fired. PostgreSQL raises SQLSTATE "
                 + $"{CollectorFaultCancelOrigin.QueryCanceled} for the target's own statement_timeout, "
                 + "for a pg_cancel_backend() aimed at the backend, and for the client CancelRequest this "
                 + "service's own command deadline sends: one code, three producers, and only the wording "
                 + $"tells them apart. The server said {QuotedCancelWording(origin.ServerText)}, which is "
                 + "not its statement_timeout wording — so do not change statement_timeout on the target "
                 + "on the strength of this row. Nothing was collected this cycle: this is NOT 'there was "
                 + "nothing to collect'.",
        };
    }

    /// <summary>
    /// The server's own cancel wording, quoted for an operator to read, degrading to a phrase when the
    /// error carried none — the rule <c>PostgresTimeoutExplanation</c> already follows for an unknown
    /// database. Quoting an empty string would present "the server said nothing" as "the server said
    /// ''".
    /// </summary>
    private static string QuotedCancelWording(string? serverText)
        => string.IsNullOrWhiteSpace(serverText) ? "nothing at all" : $"'{serverText}'";

    /// <summary>
    /// True when a SqlException is a permission denial — the expected failure when the least-privilege monitoring
    /// login (VIEW SERVER STATE only) re-executes a query that reads/writes user objects. Detected by the known
    /// permission error numbers or a "permission was denied" / "does not have permission" message, so the handler
    /// can surface a clear cause instead of a raw exception.
    /// </summary>
    private static bool IsPermissionError(SqlException ex)
    {
        foreach (SqlError error in ex.Errors)
        {
            if (error.Number is 229 or 230 or 262 or 297 or 300 or 301 or 916 or 33044)
            {
                return true;
            }

            if (error.Message.Contains("permission was denied", StringComparison.OrdinalIgnoreCase)
                || error.Message.Contains("does not have permission", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// This body's peer high-water mark for the collection_log write (#2864), or null before any
    /// non-budgeted collector has run in it.
    ///
    /// <para>NULL rather than the -1 the field carries, because the column means 'no peer had run yet when
    /// this row was written' - true for the first collector of every body - and a stored -1 would be a
    /// magic number every reader had to know to filter. The sentinel is right in memory, where it must not
    /// collide with a real 0 ms peer; NULL is right in the store, where absence has its own value.</para>
    /// </summary>
    private static int? PeerMaxOrNull(ServerLoopState server) =>
        server.SweepPeerMaxMs >= 0 ? server.SweepPeerMaxMs : null;

    /// <summary>
    /// Records a run that took its slot: the stamp moves to the next slot and the slot's last run is now (#5033).
    /// Done under the server's schedule lock against the slot as it is now: a reload that changed the run time after
    /// the hand-off was built keeps its new run time, and gets the next slot computed from it; a reload that cleared
    /// the run time leaves no slot behind, only a stamp one interval out; a reload that disabled the collector leaves
    /// nothing at all.
    /// </summary>
    internal void RecordRunTimeHandOff(ServerLoopState server, int serverId, string name, RunTimeHandOff handOff)
    {
        var now = DateTime.UtcNow;
        lock (server.ScheduleLock)
        {
            if (!server.RunTimeSlots.TryGetValue(name, out var current))
            {
                /* No slot: a reload cleared the run time (or disabled the collector). When one edit clears the run time
                   and also changes the frequency, this branch stamps the hand-off's old interval for one cycle. That
                   cannot cause a second run: the reload has no handle on the in-flight hand-off, and the next reload
                   caps the stamp to the new interval. */
                if (server.NextDue.ContainsKey(name))
                {
                    var interval = handOff.Slot.IntervalMinutes;
                    server.NextDue[name] = ComputeSeededNextDue(now, interval, now, SeedJitter(serverId, interval * 60));
                }

                return;
            }

            if (current.RunAtMinute == handOff.Slot.RunAtMinute
                && current.IntervalMinutes == handOff.Slot.IntervalMinutes
                && string.Equals(current.ClockId, handOff.Slot.ClockId, StringComparison.Ordinal))
            {
                server.NextDue[name] = handOff.NextDue;
                server.RunTimeSlots[name] = handOff.Slot with { LastRunUtc = now };
                return;
            }

            server.RunTimeSlots[name] = current with { LastRunUtc = now };
            server.NextDue[name] = ComputeSeededNextDue(
                now, current.IntervalMinutes, now, SeedJitter(serverId, current.IntervalMinutes * 60),
                new RunTimeRule(current.RunAtMinute, serverId, server.Clock.ToUtc));
        }
    }

    /// <summary>The later of two optional last-run times; null only when both are.</summary>
    internal static DateTime? Newer(DateTime? a, DateTime? b) => a is null ? b : b is null ? a : (a > b ? a : b);

    /// <summary>The run-time slot held for a collector, read under the server's schedule lock (#5033).</summary>
    private static RunTimeSlot? ReadRunTimeSlot(ServerLoopState server, string name)
    {
        lock (server.ScheduleLock)
        {
            server.RunTimeSlots.TryGetValue(name, out var slot);
            return slot;
        }
    }



    /// <summary>
    /// #6186: the collection_log write every <see cref="RunOneAsync"/> outcome goes through, then the refusal backoff's record
    /// of that outcome, so no arm can write its row and forget the backoff.
    /// </summary>
    private async Task LogRunAsync(
        ServerLoopState server, ServerRuntime runtime, string collectorName, string status, int rowsCollected,
        long sqlMs, long storageMs, string? errorMessage, FanoutCost? fanout, ServerPhaseCost? phases,
        DrainForensics? drain, FetchPhaseCost? fetchPhases, int? sweepPeerMaxMs, ILogger? logger, CancellationToken cancellationToken,
        string? readSource = null)
    {
        await DarlingObservability.LogCollectionAsync(
            _postgres!, runtime, collectorName, status, rowsCollected, sqlMs, storageMs, errorMessage, fanout, phases, drain,
            fetchPhases, sweepPeerMaxMs, logger, cancellationToken, readSource);
        RecordRunForBackoff(server, runtime, collectorName, status, errorMessage);
    }

    /// <summary>
    /// #6186: feeds one run's status to the server's refusal backoff. A PERMISSIONS run with the same answer as the last one
    /// extends the streak; a success, an error or a different answer restarts from the normal interval. YIELDED, ABANDONED and
    /// SKIPPED runs say nothing about whether the grant changed, so they leave the streak alone.
    /// </summary>
    private void RecordRunForBackoff(ServerLoopState server, ServerRuntime runtime, string collectorName, string status, string? message)
    {
        if (status is "YIELDED" or "SKIPPED" || status == EnumeratedCollectorDriver.AbandonedStatus)
        {
            return;
        }

        var effective = StoreConfigProvider.ResolveSchedule(collectorName, runtime.ServerId, _scheduleOverrides);
        var interval = TimeSpan.FromMinutes(CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes));
        var streak = server.RefusedBackoff.Record(collectorName, status == "PERMISSIONS", message, DateTime.UtcNow, interval);
        if (streak == RefusedCollectorBackoff.FreeRefusals + 1)
        {
            _logger.LogInformation(
                "  [{Server}] {Collector} refused {Streak} times in a row with the same answer: it is tried less often (up to every {Max}) until the answer changes or it succeeds (#6186)",
                server.Config.DisplayName, collectorName, streak, RefusedCollectorBackoff.MaxDelay);
        }
    }

    /// <summary>
    /// Runs one collector for a server and logs its outcome to collection_log. Returns the rows written
    /// (0 on skip/permissions/error) so an on-demand snapshot can tally them; the scheduled/on-load callers
    /// simply discard the count.
    /// </summary>
    /// <param name="peerMaxAtDispatchMs">
    /// The sweep body's peer high-water mark AS AT DISPATCH (#2864 review), or null when this call is not
    /// part of a scheduled body. Passed in rather than re-read from <c>server.SweepPeerMaxMs</c> at
    /// completion because <c>query_store</c> and <c>plan_correction</c> are dispatched FIRE-AND-FORGET:
    /// their runs outlive the body by 100-230s while the 15s sweep resets and rebuilds the mark several
    /// times over, so a value read at completion describes some unrelated later tick. Those two are among
    /// the heavies this diagnostic exists to explain, so reading it late is wrong exactly where it matters.
    /// </param>
    /// <param name="detachedDaily">
    /// #4938: true for a daily collector's scheduled run, which RunDueCollectorsAsync detaches. Such a run takes
    /// the same per-(server, collector) single-flight slot as plan_correction, then one of the fleet's daily-run
    /// permits, and does not fold its duration into the sweep body's peer mark (see <see cref="FoldsIntoSweepPeerMark"/>).
    /// False for every other caller, including the at-connect run of an on-load collector and snapshot_now.
    /// </param>
    /// <param name="runTimeHandOff">
    /// #4938: set by the pass for a collector that has a run time. Once the run holds its slot, today's slot counts as served:
    /// the stamp moves to the next slot, the slot's last run is now, and the fleet's gate stats count one run. A run that
    /// finds the slot held records none of it, so the slot stays due and the next tick tries again.
    /// </param>
    /// <param name="runOutcome">#4938: when given, set to whether this run succeeded.</param>
    private async Task<int> RunOneAsync(
        ServerLoopState server, DarlingCollectorRunner runner, string collectorName, int? peerMaxAtDispatchMs, CancellationToken cancellationToken,
        bool detachedDaily = false, RunTimeHandOff? runTimeHandOff = null, RunOutcome? runOutcome = null)
    {
        var runtime = server.Runtime;
        if (runtime is null || !s_dispatch.TryGetValue(collectorName, out var run))
        {
            return 0;
        }

        if (DispatchOverrideForTests is { } dispatchOverride)
        {
            run = (_, overrideRuntime, overrideToken) => dispatchOverride(collectorName, overrideRuntime, overrideToken);
        }

        /* #2165: the tick's Query Store pass and the backfill slice both do heavy QS text extraction, and
           they used to be free to run against the SAME server at once — measured as ~128 MB in flight on a
           4-core box, because a big catalog arriving triggers BOTH loops. Gated HERE because this is the one
           funnel that has the runtime and the collector name together. Never waits: see QueryStoreServerGate
           for why blocking a shared fleet loop would recreate the #2148 wedge through a lock. Skipping is safe
           for this collector because its window is watermark-driven (#1960) — the next pass resumes from the
           same boundary, so a skipped pass defers rows rather than dropping them. */
        using var queryStoreGate = IsQueryStoreCollector(collectorName)
            ? _queryStoreGates.GetOrAdd(runtime.ServerId, static _ => new QueryStoreServerGate()).TryAcquire()
            : QueryStoreServerGate.NotGated;

        if (queryStoreGate is null)
        {
            _logger.LogInformation(
                "  [{Server}] query_store skipped this tick — its Query Store backfill slice is mid-flight (#2165). " +
                "Resumes next tick from the same watermark; no rows are lost.",
                server.Config.DisplayName);
            return 0;
        }

        /* #2717: the generic sibling of the gate above, for collectors detached from the sequential body
           for the same bimodal-cost reason as query_store but with no second loop to exclude — see
           DetachedCollectorGate's own doc comment. Keyed by (server, collector name), so a future third
           collector detached this way needs only its own IsXCollector check added to this condition; the
           dictionary already generalizes. A held gate here means a previous detached tick for THIS
           collector on THIS server has not finished — skip is safe because every collector detached this
           way is picked specifically for having no wall-clock-derived window (plan_correction re-reads
           the live DMV set whole on every pass, so a skip just re-reads it, possibly refreshed, next time).
           NotGated (mirroring QueryStoreServerGate's) collapses this to a single null check below — a
           future third collector needs only its own IsXCollector check added to this one condition,
           never a second one to keep in sync. */
        using var detachedGate = IsPlanCorrectionCollector(collectorName) || IsPgWaitSamplingCollector(collectorName) || detachedDaily
            ? _detachedCollectorGates.GetOrAdd((runtime.ServerId, collectorName), static _ => new DetachedCollectorGate()).TryAcquire()
            : DetachedCollectorGate.NotGated;

        if (detachedGate is null)
        {
            if (detachedDaily)
            {
                /* #4938: a daily run's skip costs a whole day for that collector on that server, not one tick, so
                   it says so rather than "re-reads next tick". It only happens when the previous run, which may
                   still be waiting for a permit, has not finished by the time this one comes due. */
                _logger.LogInformation(
                    "  [{Server}] {Collector} skipped this run — its previous daily run has not finished (#4938). " +
                    "It runs again at its next daily interval.",
                    server.Config.DisplayName, collectorName);
                return 0;
            }

            _logger.LogInformation(
                "  [{Server}] {Collector} skipped this tick — a previous detached run has not finished (#2717). " +
                "Re-reads the live set next tick; no rows are lost.",
                server.Config.DisplayName, collectorName);
            return 0;
        }

        if (runTimeHandOff is not null)
        {
            _fleetGateStats?.RecordSlot(0);
            RecordRunTimeHandOff(server, runtime.ServerId, collectorName, runTimeHandOff);
        }

        /* #4938: a daily run waits here for one of the fleet's permits, holding its single-flight slot while it
           waits, so a run that has not yet started still counts as unfinished. The permit is held to the end of
           the run, through the store writes. The wait can last hours behind other daily runs, so what the run
           works on is read again after it: a server removed or reconnected in the meantime must not be run
           against the connection it had when this run was dispatched. #4999: and everything the dispatch checked
           is checked again (ReReadAfterPermitWait): a pause, a collector turned off, a target it no longer
           applies to. */
        using var dailyPermit = detachedDaily ? await AcquireDailyRunPermitAsync(server, collectorName, cancellationToken) : null;
        if (detachedDaily)
        {
            if (ReReadAfterPermitWait(server, collectorName) is not { } currentRuntime)
            {
                return 0;
            }

            runtime = currentRuntime;
        }

        /* #4999: from here a detached run is executing, so the hang watchdog watches it (WatchDailyRuns), until it
           ends: the watch is disposed with this scope. A daily run is watched from the moment it holds its permit
           and has re-read its runtime, and starts after the wait for a permit on purpose, because a run that is
           queued behind the cap is capacity pressure and is not stuck. A run detached by name (query_store,
           plan_correction, pg_wait_sampling) takes no permit, so its watch starts as it starts executing, here, past
           the single-flight gates above. Both are RanDetached; an inline run (the at-connect run of an on-load
           collector, a collector run by snapshot_now) is not watched, unless it is one of the three by name. The
           gates are what keep the watch's (server, collector) key to one run: a second run of the collector is
           skipped above, before it is watched, and this watch ends before those gates are released. It sits before
           the test seam below so a run a test holds open is watched the way a real one is. */
        using var dailyRunWatch = RanDetached(collectorName, detachedDaily)
            ? BeginDailyRunWatch(server.Config.DisplayName, runtime.ServerId, collectorName, holdsPermit: detachedDaily)
            : null;

        if (RunOneBodyOverride is { } bodyOverride)
        {
            var overrideRows = await bodyOverride(server, runtime, collectorName, cancellationToken);
            if (RunOneRefusalOverride?.Invoke(collectorName) is { } overrideRefusal)
            {
                /* Stands in for the PERMISSIONS arm: the same recorder the real run goes through (LogRunAsync). */
                RecordRunForBackoff(server, runtime, collectorName, "PERMISSIONS", overrideRefusal);
            }
            else
            {
                RecordRunForBackoff(server, runtime, collectorName, overrideRows >= 0 ? "SUCCESS" : "ERROR", null);
            }

            if (runOutcome is not null)
            {
                runOutcome.Succeeded = overrideRows >= 0;
            }

            return Math.Max(overrideRows, 0);
        }

        /* #2997: wall clock for the whole run, read ONLY by the fault arms below. The success path
           keeps reporting the runner's own measured sql/storage split and never consults this — a
           wall clock that included dispatch overhead would quietly widen three published numbers
           (collection_log.duration_ms, sql_duration_ms and the collector_cost series) to satisfy a
           log line.

           The fault arms have no split to report, because a fault by definition interrupted whichever
           phase was running, and what they recorded instead was three literal zeros. That made a
           300-second death indistinguishable from an instant one and sent two separate investigations
           after a connection that dies at open. One honest total beats three precise zeros.

           WHICH SLOT each arm reads this into follows where the time was actually spent, not
           convenience: an arm whose statement reached the target and was refused THERE passes it as
           sqlMs, and an arm that never queried the target at all passes it as storageMs. The RDS/PI
           authorization arms are the second kind, and take the accounting their own success paths
           already use — see IngestRdsPlansAsync, IngestRdsDeadlocksAsync and IngestRdsCpuAsync, which
           file an HTTPS round trip as storage time so that one target's sql_duration_ms cannot mean
           something different from every other target's. duration_ms is sqlMs + storageMs either way,
           so the wall clock is honest under both; only the decomposition differs. */
        var runClock = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            /* #3754: the long-query completion session is ensured by the per-sweep reconcile, not by this
               run, and until now nothing carried the reconcile's failure here. So on a target where the
               CREATE was refused in every database the collector still ran, its tolerant ring-buffer read
               found no session and returned zero rows - which is also what a quiet session returns - and
               the run recorded SUCCESS, sweep after sweep, while the service log said why it could not
               work. When the reconcile recorded that the session exists nowhere this collector would read,
               the honest run is SESSION_MISSING with the reconcile's message, and the read is not worth
               its connections: throwing the same exception RunXeTolerantAsync raises for a permission-
               denied session lands in the same arm below, so the row, the log line and the classification
               are the ones an operator already knows from the deadlock and blocked-process collectors. */
            if (IsLongQueryCompletionsCollector(collectorName) && server.LongQueryTraceFault is { } traceFault)
            {
                /* #5378: a denied create is a permission state, not a capture that broke: PERMISSIONS, as in Lite. */
                if (server.LongQueryTraceFaultIsPermission)
                {
                    throw new DarlingXeSessionDeniedException(traceFault);
                }

                throw new DarlingXeSessionMissingException(traceFault);
            }

            /* #5640 (H2): the client-timeout session's create was refused for permission. The read of a session that cannot exist
               would throw "not running" and record SESSION_MISSING, and the Capture Down self-alert reads that: a permanent alert on
               every server whose login lacks ALTER ANY EVENT SESSION (or whose engine refuses it). The same arm as the long-query
               session's denial (#5378), so the row is PERMISSIONS. A session dropped by hand on a login that may create one is not
               here: the ensure recreates it, and until then the read records SESSION_MISSING. */
            if (AttentionPermissionFaultFor(collectorName, server) is { } attentionFault)
            {
                throw new DarlingXeSessionDeniedException(attentionFault);
            }

            /* #6893: the deadlock and blocked-process sessions, the same two outcomes as the two above. The ensure that could not create the
               session left its failure on the server: PERMISSIONS for a denied create, SESSION_MISSING for anything else. Without this
               the read joined an absent session and returned zero rows, and the run recorded SUCCESS. */
            if (CaptureEnsureFaultFor(collectorName, server) is { } captureFault)
            {
                if (captureFault.IsPermission)
                {
                    throw new DarlingXeSessionDeniedException(captureFault.Message);
                }

                throw new DarlingXeSessionMissingException(captureFault.Message);
            }

            var result = await run(runner, runtime, cancellationToken);

            /* #4964: a run of this collector succeeded, so its run of missing-session failures is over: the next failure
               logs its line at Warning again (XeSessionMissingWarnings). */
            server.XeSessionMissingWarnings.Clear(collectorName);

            /* #3754, the partial case: the Azure reconcile created the session in some databases and was
               refused in others. The run just read the survivors and its SUCCESS is a real success - but
               its row has to say that the refused databases are not in it, or a zero here reads as a quiet
               server. The #2623 partial note the reconcile composed rides the same channel every other
               partial loss uses (MergeNotes, so a probe-failure or per-database note the runner itself
               composed is kept beside it). HostNote rather than Note: Note is computed from HostNote plus
               the definition's measurements, and setting the composed value would lose the measurements. */
            if (IsLongQueryCompletionsCollector(collectorName) && server.LongQueryTracePartialNote is { } partialNote)
            {
                result = result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, partialNote) };
            }

            /* #6893: the same partial case for the deadlock and blocked-process sessions on Azure SQL Database. */
            if (CaptureEnsurePartialNoteFor(collectorName, server) is { } captureNote)
            {
                result = result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, captureNote) };
            }

            /* #4004 review, round 3: the first pg_log_events run after a start that replaced a discarded log-hash key
               carries that on its row, through the same HostNote channel; taking it clears it, so no later run does. */
            result = _logHashKeyRotation.ApplyTo(collectorName, result);

            /* #4046: a log read that skipped lines stamped outside the target's UTC log_timezone carries their count;
               this puts the sentence that names the setting and the issue beside it, on the same HostNote channel. */
            result = DarlingCollectorRunner.WithForeignZoneLinesNote(result);

            /* #4699: a log read that fell back or skipped bytes or files says so beside the count. An Aurora or RDS
               target reads its log through the AWS API, so its two measurements carry the RDS wording (#4708). */
            result = DarlingCollectorRunner.WithLogResumeNotes(result, runtime.Target.IsAurora || runtime.Target.IsAwsRds);

            /* #4058 L1: a plan-capture run that skipped forged captures (a NULL query id or duration out of
               the guarded CASE chain) carries their count; this puts the sentence that names the issue
               beside it, on the same HostNote channel, following the ForeignZoneLines pattern above. */
            result = DarlingCollectorRunner.WithForgedCaptureNote(result);

            /* #4058 item 1: a deadlock-capture run that skipped RAISE-shaped entries (not written by
               PostgreSQL's own DeadLockReport) carries their count; this puts the sentence that names the
               issue beside it, on the same HostNote channel, following the ForgedCapture pattern above. */
            result = DarlingCollectorRunner.WithRaiseShapedDeadlocksSkippedNote(result);

            /* #4046: the once-a-day nudge for a self-hosted target still on the text route, BEFORE it ever
               hits the 22021 byte — gated to pg_log_events alone so a target with all three log-tail
               collectors scheduled gets one note a day, not whichever of the three wins the 24-hour gate.
               Never fires for Aurora/RDS: they reach the log through the AWS API and never populate
               PgReadBinaryFileCapability's cache, so WithReadBinaryFileAdvisoryNote's cache read finds
               nothing for them. */
            if (string.Equals(collectorName, "pg_log_events", StringComparison.Ordinal))
            {
                result = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(result, runtime);
            }

            /* #3102: Debug, which is BELOW the default filter's Information, for the same reason the
               per-database fault split takes its arm's level — see LogPerDatabaseFaultSplit's remarks. A
               timing on the default log with no reason beside it is the shape to avoid, and a run that
               SUCCEEDED has no reason beside it at all. The level is the whole of the choice: the message,
               its gates and its numbers are untouched, so the parsers outside this repo see the identical
               text the moment the level is turned up.

               Not deleted and not aggregated, because the sample IS the artifact here: attributing one
               server's one cycle needs that cycle's own numbers, and a periodic distribution cannot answer
               "what did this collector do at 04:12". Reachable per-namespace rather than all-or-nothing, so
               measuring the store-write path does not also turn on every other Debug line in the process
               (Darling/README.md, "Logs"). */
            _logger.LogDebug("  [{Server}] {Collector} => {Rows} rows (sql:{SqlMs}ms, pg:{PgMs}ms)",
                server.Config.DisplayName, collectorName, result.Rows, result.SqlMs, result.StorageMs);

            /* #3653 A5: the identity-epoch account, if this run's definition saw one. The definition has the
               row (old and new start time, old and new name) and no logger; this loop has the logger and
               no row; the calculator both hold is where the sentence waits (CollectorDeltaCalculator
               .DrainDiscontinuities). Information, not Debug: a forgotten baseline set is a measurement
               discontinuity an operator reading a flat minute on the charts needs to be able to find, and
               it happens on the order of once per restart or failover, never per cycle. Drained on every
               run — a dictionary probe — so the line lands beside the run that observed it and is never
               re-logged by the next. The same run's collection_log row carries the count
               (identity_epoch_changes=1 / statements_epoch_changes=1) as the marker in the store; Lite's
               twin is the same drain in RemoteCollectorService.RunCollectorAsync. Worded as "drained after"
               rather than "observed by": a carrier run that observed the epoch and then failed at its store
               write leaves the sentence queued for the next successful run on the server, whichever
               collector that is. */
            if (_deltas is not null)
            {
                foreach (var discontinuity in _deltas.DrainDiscontinuities(runtime.ServerId))
                {
                    _logger.LogInformation("  [{Server}] {Discontinuity} (#3653 A5; drained after {Collector})",
                        server.Config.DisplayName, discontinuity, collectorName);
                }

                /* #4428: the wait-stats-clear account, queued by WaitStatsCollector.ReadAsync when it
                   detects a server-wide DBCC SQLPERF(..., CLEAR)-shaped pass. Drained the same way as the
                   identity-epoch discontinuities just above — a dictionary probe beside the run that
                   observed it — but the calculator throttles it to once per server per day itself, so no
                   collector-name framing is added here; the sentence is already complete. */
                foreach (var clearWarning in _deltas.DrainWaitStatsClearWarnings(runtime.ServerId))
                {
                    _logger.LogInformation("  [{Server}] {ClearWarning}", server.Config.DisplayName, clearWarning);
                }
            }

            /* #4961: the instance's identity moved (a restart, or a failover to another instance). A restart stops the
               long-query trace's session, which is created stopped at startup, so the latch is cleared and the next
               sweep's reconcile starts it again. Without this the gap lasts until the hourly create pass. */
            if (ForgetLongQueryTraceLatchOnRestart(server, result.Measurements))
            {
                _logger.LogInformation(
                    "[{Server}] The instance's identity moved (a restart or a failover): the long-query trace is checked again on the next sweep",
                    server.Config.DisplayName);
            }

            /* #2851: the server-scoped phase split rides its OWN line, for the same reason #2811's fetch
               sub-splits do — the line above is parsed by tooling outside this repo, and "don't break the
               parser" outranks "one line to grep". Gated on the MEASURED flag rather than on a value being
               non-zero: the enumerated path's `PerItemOpenMs > 0` gate cannot tell a genuinely instant open
               from a path that emits no split at all, and this one must.

               wm: sits OUTSIDE the sum and says so, because on this path it genuinely is outside: the
               watermark read runs before the sql: stopwatch starts. Printing it inside a `sql:N = ...`
               decomposition would have made it a permanent 0 and taught every future reader that a store
               read #2796 clocked at 50s cold is free. */
            if (result.ServerPhasesMeasured)
            {
                _logger.LogDebug(
                    "  [{Server}] {Collector} sql:{SqlMs}ms = open:{OpenMs}ms + drain:{DrainMs}ms + other:{OtherMs}ms (wm:{WatermarkMs}ms store-side, outside sql)",
                    server.Config.DisplayName, collectorName, result.SqlMs,
                    result.ServerOpenMs, result.ServerDrainMs, result.ServerOtherMs, result.ServerWatermarkMs);
            }

            /* result.Note is null for an ordinary run — the message column stays null as before. It is set
               only for a successful-but-empty run worth explaining (today: an enumeration that listed zero
               databases, #1837). The status stays SUCCESS, and every health/band read keys on status rather
               than on error_message, so the note is inert outside the Collection Log detail grid.

               The ONE exception is a cycle the #2673 whole-server wall-clock budget abandoned, which reaches
               here on the same path because it returns normally rather than throwing. It is not a successful
               empty run: it stored nothing and advanced no watermark, so recording it SUCCESS made it the
               newest success in ReadCollectionSignalsAsync's status IN ('SUCCESS', 'SKIPPED') — a collector
               abandoning every cycle read as perpetually fresh — and put its message in the #1837 note
               channel, whose whole claim is that the run succeeded. Same reasoning as the RDS/PI
               authorization arms below: nothing was read, so it must not be recorded as a successful empty
               read. */
            var status = EnumeratedCollectorDriver.ClassifyReturnedRun(result.Abandoned);

            await LogRunAsync(
                server, runtime, collectorName, status, result.Rows, result.SqlMs, result.StorageMs, result.Note,
                result.Fanout, result.ServerPhases, result.Drain, result.FetchPhases, peerMaxAtDispatchMs, _logger, cancellationToken,
                readSource: result.ReadSource);

            /* #2864 item 3: fold THIS run into the body's peer high-water mark, AFTER its own row is
               written so a collector is never its own peer. The mark answers the question that took
               manual cross-referencing of neighbouring rows to answer before: when a heavy collector
               blows its budget, were the ORDINARY collectors in that same body slow too?

               Population A - one genuinely large query - runs beside peers at or below their baseline;
               a measured sweep had wait_stats at 1ms and latch_stats at 1ms while query_store took
               71,977ms for 12,557 rows. Population B is sweep-wide degradation, where the same light
               collectors ran 34-47x their baseline BEFORE the heavy ones burned their budget. Same
               stored shape, opposite causes, and only the peers tell them apart.

               Budgeted collectors are excluded because they are the heavy ones being explained - a
               mark that included procedure_stats would be dominated by exactly the run in question.
               Asked of the catalog rather than a name list here: the list would be right until a fifth
               collector earned a budget and silently wrong after.

               #4999: and a run that ran DETACHED is excluded whatever its budget. It finishes after the body
               that dispatched it, so its duration would land in an unrelated body's mark; the budget rule
               alone left pg_wait_sampling's 30 s window folding in. */
            FoldIntoSweepPeerMark(server, collectorName, detachedDaily, result.SqlMs);

            /* #2674: record this run's cost for the hourly collector_cost aggregate — the same numbers that
               go to collection_log, kept as a compact per-(server, collector) series for the cost panel. */
            _collectorCost.Record(runtime.ServerId, collectorName, result.Rows, result.SqlMs, result.StorageMs);

            /* #4938: a run that has just written this server's clock to the store (server_properties on SQL Server,
               pg_server_config on PostgreSQL) refreshes the clock the worker holds for it: the first row of a new server
               replaces the UTC it started with, and a zone change replaces the old zone. The pass computes the slot of a
               collector that has a run time again when the clock's identity differs from the one its stamp was computed
               with. One read per run of these two collectors, none on a tick, and a failed read keeps the held clock. */
            if (CarriesServerClock(collectorName, runtime.Target.Engine)
                && await TryReadServerClockAsync(_postgres!, runtime.ServerId, runtime.Target.Engine, _logger, cancellationToken) is { } refreshedClock)
            {
                server.Clock = refreshedClock;
            }

            /* #2219: statement TEXT rides alongside the statement stats, on its own hourly cadence. Hung off the
               stats collector's success rather than given its own loop because it is meaningless without those
               rows and must never run against a server whose stats collection is failing — one less loop that
               can be independently wrong. Best-effort: a text fetch that fails leaves the statistics collected
               and logs, because unreadable text is a degraded read while a failed collection is lost data. */
            if (IsPgStatementStatsCollector(collectorName))
            {
                await TryRefreshPgStatementTextAsync(runtime, cancellationToken);
            }
            if (runOutcome is not null)
            {
                runOutcome.Succeeded = true;
            }

            /* #7067: the precondition cleared, so the next failure of this collector is a new sighting and is logged at once. */
            RepeatLog.Clear(server.Config.DisplayName, collectorName);

            return result.Rows;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (DarlingXeSessionDeniedException ex)
        {
            /* #5378: the long-query session's create was denied (the login lacks ALTER ANY EVENT SESSION). A least-privilege
               choice an operator is entitled to make (#1823), so PERMISSIONS, the status every other denied source records,
               and not the SESSION_MISSING of a session that broke. The row is written on every sweep, so collection health
               keeps reading it; the line logs at Warning once and at Debug after, like the missing-session line (#4964). */
            _logger.Log(server.XeSessionMissingWarnings.TryMarkWarned(collectorName) ? LogLevel.Warning : LogLevel.Debug,
                "  [{Server}] {Collector} => insufficient permissions: {Message}",
                server.Config.DisplayName, collectorName, ex.Message);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (DarlingXeSessionMissingException ex)
        {
            /* The blocking/deadlock XE session is missing and couldn't be created — log a distinct
               SESSION_MISSING status the Stage 4 Capture Down self-alert reads. Non-fatal, like a
               permissions skip: log it and continue the rest of the sweep. RunXeTolerantAsync already
               classified this (wrapping an XE 297/15151/'XE session' into the distinct type), so this
               catch and the SqlException permissions filter below match disjoint types — their relative
               order is not load-bearing; it only needs to precede the general Exception catch (which would
               otherwise mislabel it ERROR). A non-XE 297 arrives as a plain SqlException and correctly
               hits the permissions filter.

               #3754: the second producer is the pre-dispatch check at the top of the try, for
               long_query_completions alone - the reconcile recorded that its session could not be created
               anywhere this run would read, so the run is classified here without opening a connection.
               Same type, same arm, same row shape; only the message's origin differs.

               #4964: this arm runs on every sweep for as long as the session cannot be ensured, and the row below is
               written on every one of them, on purpose, so collection health keeps reading SESSION_MISSING. The log line is
               what would repeat without end: the first failing run of a collector on this server logs it at Warning, and the
               runs after it log the same line at Debug, until a run of that collector succeeds (the clear after its run
               above). */
            _logger.Log(server.XeSessionMissingWarnings.TryMarkWarned(collectorName) ? LogLevel.Warning : LogLevel.Debug,
                "  [{Server}] {Collector} => XE session missing (capture down): {Message}",
                server.Config.DisplayName, collectorName, ex.Message);

            await LogRunAsync(
                server, runtime, collectorName, "SESSION_MISSING", 0, runClock.ElapsedMilliseconds, 0, ex.Message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (Exception ex) when (AwsRoleConfigurationFault(ex) is { } assume)
        {
            /* #5452: the AWS role on this server cannot be used: the allow list does not list it, its partition is not the
               target's, or STS refused to hand it out. FIRST of the RDS arms, and ahead of every arm that reads the text of an
               AWS failure: an STS denial says "is not authorized to perform", which the log and Performance Insights arms
               below would read as the monitoring host's own IAM role missing a grant, and tell an operator to fix the wrong
               role. PERMISSIONS, like the other refused-source outcomes, with the exception's own message, which names the role
               ARN and whether an external ID is set and never the ID. Nothing was read this cycle. Written on every sweep so
               collection health keeps reading it. The other kinds (the host's credentials, no source identity, a transient
               STS failure) carry no operator-fixable setting and fall through to the general ERROR arm with the same message. */
            var roleNote = AwsRoleFaultNote(_logger, server.Config.DisplayName, collectorName, assume);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, 0, runClock.ElapsedMilliseconds, roleNote,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (RdsEndpointMismatchException ex)
        {
            /* The host is not the endpoint AWS reports for the id parsed from it, or the target did not accept a fresh
               login, so no RDS or Performance Insights call was made for this target. PERMISSIONS, like the other
               refused-source outcomes, and the message names the host and the id (or the login). Written on every sweep
               so collection health keeps reading it. */
            _logger.LogWarning(ex is RdsTargetLoginException
                    ? "  [{Server}] {Collector} => PERMISSIONS: the target did not accept a fresh login"
                    : "  [{Server}] {Collector} => PERMISSIONS: the host does not match the endpoint AWS reports",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, 0, runClock.ElapsedMilliseconds, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (RdsLogUnavailableException ex) when (ex.IsAuthorizationFailure)
        {
            /* #2633: the AWS call was DENIED, so nothing was read. Degraded to PERMISSIONS rather than
               ERROR for the same reason a 42501 from the pg_read_file route is — a least-privilege
               deployment is an expected state an operator can act on, and screaming every cycle about it
               would bury real faults — but it must NOT be recorded as a successful empty read, which is
               what returning zero rows used to make it.

               Only the authorization case lands here. A throttle, a failover or an endpoint that stopped
               resolving falls through to the general handler and stays loud, because a permanent-sounding
               status on a transient fault is how an outage gets read as a configuration choice. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: the RDS log API refused the call",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, 0, runClock.ElapsedMilliseconds,
                $"{ex.Message} — the MONITORING HOST's IAM role lacks a grant this source needs, which is "
                + "not a database grant: plan capture on managed PostgreSQL reads the server log through "
                + "the RDS API, so the role needs rds:DescribeDBInstances, rds:DescribeDBClusters, "
                + "rds:DescribeDBLogFiles and rds:DownloadDBLogFilePortion "
                + "on the target instance. Nothing was read this cycle — this is NOT 'no plans were "
                + "captured'.",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PiMetricsUnavailableException ex) when (ex.IsAuthorizationFailure)
        {
            /* #2719, same shape as the RdsLogUnavailableException handler above and for the same reason:
               the AWS call was DENIED, so no CPU reading was pulled this cycle, and that must read as
               PERMISSIONS rather than a SUCCESS row claiming PI was read and simply had nothing new. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: the RDS/PI API refused the call",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, 0, runClock.ElapsedMilliseconds,
                $"{ex.Message} — the MONITORING HOST's IAM role lacks a grant this source needs, which is "
                + "not a database grant: instance CPU on managed PostgreSQL reads AWS Performance Insights, "
                + "so the role needs rds:DescribeDBInstances, rds:DescribeDBClusters and "
                + "pi:GetResourceMetrics on the target instance. Nothing was read this cycle — this is NOT "
                + "'CPU is idle'.",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgLogTimezoneUnsupportedException ex)
        {
            /* #2993: this target's log_timezone is not UTC, so the lines in its server log are stamped in
               local time and cannot be stored against UTC. Both readers that assemble log entries through
               PgLogEntryAssembler (pg_log_events and pg_deadlocks) land here, which is why the message names
               the log, not deadlocks.

               PERMISSIONS for the same reason the PostgresException FeatureDisabled arm below is: none of
               the store's five statuses means "this target is configured in a way this source cannot be
               read under", so the non-fatal degradation bucket carries it and the MESSAGE is where the
               truth goes. It also earns that bucket on the merits — the cause is a setting on the
               monitored server, an operator can clear it, and CollectorRuntimePrecondition's PERMISSIONS
               arm already frames the condition as satisfiable and re-derives it on every read.

               Not ERROR: nothing is broken on the monitoring side, and a parameter group does not change
               because we shouted about it once a minute. Not a SUCCESS row with zero rows, which is what
               dropping the unusable blocks quietly would have produced — a target whose log is in the
               wrong zone reading as a target with no deadlocks.

               Both transports arrive here: the pg_read_file route throws out of the collector's ReadAsync,
               the RDS log-API route out of the ingestor's Extract.

               That shared arrival is also why this is the ONE fault arm that records zeros rather than
               runClock's wall clock. The slot rule at the stopwatch's declaration keys on whether the
               target was queried, and the two transports answer that oppositely: pg_read_file IS a target
               query and belongs in sqlMs, while the RDS log API is an HTTPS round trip that belongs in
               storageMs. The exception carries only ObservedZone, so this catch cannot tell which one it
               is, and either slot would be wrong for half the fleet. A zero here understates a duration;
               a guessed slot would misattribute one, and this store's readers decompose sql vs storage
               per target. Threading the transport onto PgLogTimezoneUnsupportedException is what would
               let this arm join the rule. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: the server log is stamped '{Zone}', not UTC",
                server.Config.DisplayName, collectorName, ex.ObservedZone);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, 0, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgLoggingCollectorOffException ex)
        {
            /* #3410's third state: logging_collector is off, so the target writes its log to stderr and
               there is no file for the pg_read_file route to read. Not 42501's missing grant and not
               58P01's missing file — neither a grant nor a path change can produce a file the server is
               not writing — so it takes the timezone arm's disposition for the timezone arm's reasons:
               PERMISSIONS, because it is a setting on the monitored server that an operator can change and
               CollectorRuntimePrecondition's arm already frames it as satisfiable and re-derived every
               cycle; not ERROR, because nothing is broken on the monitoring side and a deliberate logging
               destination does not change because we shouted about it once a cycle; not a SUCCESS row with
               zero rows, which would read as a server with no deadlocks and nothing slow. The message is
               the exception's own — it names the setting and the restart — which is the same
               not-collected-with-the-reason answer the store's own log read gives when its directory
               listing comes back empty.

               Unlike the timezone arm this one CAN name its slot: only the pg_read_file route returns the
               marker row — the RDS transport runs no SQL and its platform keeps the logging collector on —
               so the elapsed time is a target query and belongs in sqlMs. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: logging_collector is off, so the target writes no server log files",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgNoStderrLogFileException ex)
        {
            /* #3997's narrower sibling to the arm above: logging_collector is ON, but log_destination
               carries no stderr format, so the shared tail's newest CTE excluded every file it saw as a
               csvlog/jsonlog sibling and there is nothing left for the pg_read_file route to read. Same
               disposition as PgLoggingCollectorOffException and for the same reasons — a setting on the
               monitored server, satisfiable and re-derived every cycle, not broken on the monitoring side,
               and a SUCCESS row with zero rows would read as a target with nothing to report rather than
               one this route cannot read at all. Same slot rule too: only the pg_read_file route produces
               this marker, so the elapsed time is a target query and belongs in sqlMs. */

            /* #4053 review L1: the cache said stderr-only, but no stderr-format file is left — the far more
               likely explanation is csvlog/jsonlog was just added and the cache has not caught up, not that
               log_destination emptied entirely. Drop the verdict so the next cycle re-probes instead of
               reading no stderr file (and, on the csvlog route, no rows) for up to an hour.

               #4053 review L1 (round 2): this marker is also thrown by pg_deadlocks and pg_plan_capture when
               they read the stderr tail (csvlog off, or a jsonlog-only target, which they don't read yet).
               Their no-stderr-file fault says nothing about a csvlog or jsonlog verdict, so it must not drop
               one. Gate on RoutedCollectors, the same set the runner
               keys the probe itself on, so only a collector this cache actually informs can invalidate it.

               #4053 a2 review W1: and only when the cached verdict said stderr-only. A missing stderr file
               contradicts that verdict and nothing else. On a jsonlog-only target the deadlock and plan
               collectors (still stderr readers) hit this arm every cycle, and clearing a jsonlog verdict there
               would re-probe the target on every routed run. */
            var cacheKey = DarlingCollectorRunner.ReadBinaryFileCacheKey(runtime);
            if (PgLogFormatCapability.RoutedCollectors.Contains(collectorName)
                && PgLogFormatCapability.CachedVerdictIsStderrOnly(cacheKey))
            {
                PgLogFormatCapability.Invalidate(cacheKey);
            }

            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: log_destination carries no stderr-format file for this target",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgNoCsvlogFileException ex)
        {
            /* #4053 part a1b's own narrow state: pg_log_events is on the csvlog route (log_destination
               includes csvlog), logging_collector is on, but no .csv file has appeared under log_directory
               yet — most likely csvlog was only just added to the destinations and the syslogger has not
               rolled a file since the reload. Same disposition as PgNoStderrLogFileException and for the
               same reasons: a setting on the monitored server, satisfiable and re-derived every cycle, not
               broken on the monitoring side. Only the csvlog route produces this marker, so the elapsed
               time is a target query and belongs in sqlMs. */

            /* #4053 review L1: the cache said csvlog was one of the destinations, but no .csv file exists.
               The most likely explanation once the syslogger has had time to roll a file is that csvlog was
               REMOVED from log_destination since the cache checked — the exception's own message text
               ("was only just added") is aimed at the fresher case, but this fault is exactly the evidence a
               stale cache needs. Drop the verdict so the next cycle re-probes; a genuinely fresh csvlog
               addition simply gets the same true verdict back next time.

               #4053: every csvlog reader throws this marker (pg_log_events, pg_deadlocks and pg_plan_capture on
               the self-hosted route; the RDS ingestors on a stale csv listing). The RoutedCollectors gate is
               applied here too, for the same reason as the sibling arm above: one guard the cache trusts, not
               two that must agree by accident. */
            if (PgLogFormatCapability.RoutedCollectors.Contains(collectorName))
            {
                PgLogFormatCapability.Invalidate(DarlingCollectorRunner.ReadBinaryFileCacheKey(runtime));
            }

            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: no csvlog file has appeared for this target yet",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgNoJsonlogFileException ex)
        {
            /* #4053 part a2's own narrow state: pg_log_events is on the jsonlog route (log_destination
               includes jsonlog), logging_collector is on, but no .json file has appeared under log_directory
               yet — most likely jsonlog was only just added to the destinations and the syslogger has not
               rolled a file since the reload. Same disposition as PgNoCsvlogFileException and
               PgNoStderrLogFileException, and for the same reasons: a setting on the monitored server,
               satisfiable and re-derived every cycle, not broken on the monitoring side. Only the jsonlog
               route produces this marker, so the elapsed time is a target query and belongs in sqlMs. */

            /* Same #4053 review L1 reasoning as the csvlog arm above: the cache said jsonlog was one of the
               destinations, but no .json file exists — drop the verdict so the next cycle re-probes rather
               than reading no rows for up to an hour, gated on RoutedCollectors so only a collector this
               cache actually informs can invalidate it. */
            if (PgLogFormatCapability.RoutedCollectors.Contains(collectorName))
            {
                PgLogFormatCapability.Invalidate(DarlingCollectorRunner.ReadBinaryFileCacheKey(runtime));
            }

            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: no jsonlog file has appeared for this target yet",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (SqlException ex) when (ex.Number == 1222 && CollectorCatalog.YieldsOnLockTimeout(collectorName))
        {
            /* The 1-second LOCK_TIMEOUT guard doing its job (#1805): the snapshot sweep stepped aside
               instead of joining a blocking chain on the monitored server. Not a collection failure —
               the next sweep sees current state — so it records as YIELDED: its own status, excluded
               from the error counts that feed collector health, the daily health band, and the
               collection-failure self-alerts, and readable as evidence of lock contention on the
               TARGET rather than a monitoring fault. Same classification Lite applies — parity is the
               point. A 1222 from a collector without the guard flag falls through to the general
               catch below, unchanged. This filter and the permissions filter match disjoint
               conditions, so their relative order is not load-bearing; both only need to precede the
               general Exception catch. */
            _logger.LogInformation("  [{Server}] {Collector} => YIELDED - 1s lock-timeout guard fired (target lock contention)",
                server.Config.DisplayName, collectorName);

            await LogRunAsync(
                server, runtime, collectorName, "YIELDED", 0, runClock.ElapsedMilliseconds, 0,
                $"Lock-timeout yield (SQL error #{ex.Number}): the 1-second LOCK_TIMEOUT guard fired rather than waiting in a blocking chain. One snapshot sweep skipped; evidence of lock contention on the monitored server, not a monitoring failure.",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* Same Azure explanation Lite appends (#1631): error 300 on Azure SQL Database is a service
               objective limit phrased as a permission denied on 'master', which reads as a missing GRANT
               and sends people looking for one that cannot be issued. Appended, so the raw error stays
               searchable. Parity is the point — a Darling operator gets the identical sentence Lite gives.
               8189 is sys.traces' own denial ("You do not have permission to run 'SYS.TRACES'", ALTER
               TRACE missing): a legitimate least-privilege choice (#1823) — ALTER TRACE is not read-only —
               so default_trace_events must degrade as PERMISSIONS, not scream ERROR every cycle.
               #2512: the number set moved to SqlServerPermissionErrors, shared with Lite's catch and
               with SqlServerTargetProvider.Classify, and gained 262 — "permission denied in database
               'tempdb'", the #2150 denial that used to record ERROR every cycle and is the reason
               tempdb_stats was gated off Azure SQL Database at all. */
            var message = ex.Message + AzureDmvPermissionHint.For(
                ex.Number, server.Runtime?.Target.IsAzureSqlDb == true, ex.Message);

            _logger.LogWarning("  [{Server}] {Collector} => insufficient permissions ({Number}): {Message}",
                server.Config.DisplayName, collectorName, ex.Number, message);

            await LogRunAsync(
                server, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        /* #2997: the database is read off the EXCEPTION first, falling back to the runtime's own. #2638
           added the database name to the ObjectMissing sentence precisely so an operator would not go
           looking in the wrong one - and for a RunsPerDatabase collector the runtime's field is the wrong
           one, because it holds whatever database the initial probe landed on rather than the database
           this fault came from. See CollectorFaultDatabase. */
        catch (PostgresException ex) when (
            PostgresFaultOutcome(
                ex,
                collectorName,
                CollectorFaultDatabase.For(ex, runtime.ConnectedDatabase),
                runtime.PgStatStatementsExtension,
                runtime.Target.IsAurora)
                is { Status: not "ERROR" } outcome)
        {
            /* PostgreSQL faults classified by SQLSTATE through the same ITargetProvider.Classify the
               engine seam already exposes, so the runner and the provider cannot disagree about what an
               error means.

               Without this the general catch below claimed every one of them, and a PERSISTENT condition
               would log ERROR every single cycle forever: pg_statement_stats against a database where the
               extension was never created (42P01), a source Aurora does not implement at all (0A000), a
               feature switched off in the parameter group (55006). Those are the exact PostgreSQL analogue
               of the 8189 sys.traces denial above, which degrades to PERMISSIONS for the same reason —
               it is a real, operator-actionable state, not a monitoring fault, and burying it in a
               once-a-minute error is how it gets ignored.

               The message says WHICH kind it is rather than leaving "PERMISSIONS" to imply a missing
               GRANT, following the AzureDmvPermissionHint precedent: the status is the store's
               non-fatal-degradation bucket, the text is where the truth goes. */
            var (status, explanation) = (outcome.Status, outcome.Explanation);

            /* #4051 review L1: a 42501 while the binary route is in use means that grant is gone, so the
               cached verdict must not outlive it by up to an hour. */
            DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict(collectorName, ex, runtime);

            /* #4251 round-1 review, M1: the real landing spot for a pg_server_config 42501 — PostgresFaultOutcome
               classifies SqlState 42501 PERMISSIONS for every collector (PostgresTargetProvider.Classify), so
               this fault never reaches the general catch below, and a call only in LogTailGeneralFault would
               never fire for it. Dropping the stale verdict here is what lets the next cycle re-probe instead
               of losing the whole pg_settings snapshot until the hour runs out. */
            DarlingCollectorRunner.ForgetStaleFileSettingsVerdict(collectorName, ex, runtime);

            if (status == "YIELDED")
            {
                _logger.LogInformation("  [{Server}] {Collector} => YIELDED - {Explanation}",
                    server.Config.DisplayName, collectorName, explanation);
            }
            else
            {
                LogPostgresFault(server.Config.DisplayName, collectorName, status, ex.SqlState, explanation);
            }

            await LogRunAsync(
                server, runtime, collectorName, status, 0, runClock.ElapsedMilliseconds, 0, explanation, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        /* yieldsOnLockTimeout: false is deliberate, and matches the general catch below rather than the
           PostgresException arm above. That flag only ever decides the 55P03 branch inside Classify, so it
           cannot change a CommandTimeout answer - passing the collector's real value here would read as
           though the yield question were relevant to this filter, and it is not. Both of the plain-Exception
           filters in this method ask a single narrow question of the classifier and pass false for the same
           reason; the SQLSTATE arm passes the real value because it dispatches on the whole fault map,
           55P03 included. */
        catch (Exception ex) when (
            server.Runtime?.Target.Engine == CollectorTargetEngine.PostgreSql
            /* The arm may only claim what the filter guarantees. Classify's first branch answers
               CommandTimeout for ANY TimeoutException, and EnumeratedCollectorDriver.ItemBudgetException
               throws a BARE one for the in-process per-item wall-clock budget - a monitoring-side cut
               that never reached the database and has nothing to do with Npgsql. Without this term the
               authored sentence below would tell an operator that Npgsql cancelled a read mid-stream
               about a budget this service abandoned on its own. Latent today, because no PostgreSQL
               collector declares PerItemWallClockBudget, which is exactly the kind of "true when written"
               that stops being true without anyone revisiting the sentence.

               NpgsqlException covers BOTH real deadlines and only those: PostgresException derives from
               it (verified against Npgsql 10.0.3), so SQLSTATE 57014 still lands here, while the bare
               TimeoutException falls through to the general catch - which classifies it CommandTimeout
               too, so it still forces no reprobe, and now reports the budget's own message and the real
               elapsed time rather than a borrowed narrative. */
            && ex is NpgsqlException
            /* #3111: and NOT a write to the STORE. The engine term above was standing in for "this fault
               came from reading the target", and for a store write that proxy is simply false — the store
               connection is Npgsql whatever the target's engine is, so a COPY timeout satisfies both terms
               above and lands here. Observed on a real fault: a start-phase COPY timeout under store-side
               contention arrives as an NpgsqlException wrapping a TimeoutException, classifies
               CommandTimeout, and reached this arm.

               What it then got was the client-side sentence below, whose remedy is target-read-specific:
               "Npgsql cancelled the read mid-stream ... the work asked for does not fit the deadline ...
               shrinking the work is the right one." Shrinking a read is not the remedy for a COPY blocked
               on a store-side lock, and a confident wrong instruction is worse than a vague one because an
               operator acts on it.

               #3095's phase axis is the discriminator this filter lacked: a recorded phase is only ever
               written by a COPY into the store, so it identifies a store write BY CONSTRUCTION rather than
               by inference. Excluded here rather than given a store-side sentence of its own, so that one
               fault reports one way: the general arm below already renders the phase into both the app log
               and the collection_log row, and it is where this same fault already lands on a SQL Server
               target. A second authored sentence would make the identical store fault read differently
               depending on the monitored target's engine, which the store write has nothing to do with.

               Only the PROVEN population moves. An unstamped fault keeps #2997's sentence, so the post-COPY
               dimension flush and commit (#1767) are still described as target reads — see
               IsProvenStoreWrite for why that residual is the affordable direction. */
            && !CollectorFaultCopyPhase.IsProvenStoreWrite(ex)
            && PostgresTargetProvider.Instance.Classify(ex, yieldsOnLockTimeout: false)
               == CollectorTargetFault.CommandTimeout)
        {
            /* #2997: a PostgreSQL deadline, authored rather than left as transport text.
               pg_index_bloat died here eleven consecutive times reporting only "Exception while
               reading from stream" — seven words, no collector, no database, no duration — which is
               indistinguishable from a dropped socket and was read as one twice.

               Deliberately AFTER the PostgresException arm above and not merged into it: this arm has
               to catch a plain Exception, because a client-side deadline normally arrives as an
               NpgsqlException wrapping a TimeoutException, carrying no SQLSTATE for a SQLSTATE map to
               switch on. Normally, not always — Npgsql enforces its deadline by sending a CancelRequest,
               so the server's 57014 error response can arrive before the stream tears and OUR deadline
               then surfaces as a PostgresException. A 57014 reaches the arm above first, where
               PostgresFaultOutcome maps it to ERROR and so declines it, which is what lets both shapes
               land here. Which one arrives is a RACE, so the exception's type cannot be what tells the
               two deadlines apart, and neither can the SQLSTATE that three unrelated producers share:
               CollectorFaultCancelOrigin reads what the server actually said.

               NO reprobe, and that is the same care the general catch takes: the provider classifies
               this CommandTimeout rather than ConnectionFatal precisely so a slow statement cannot
               force a reconnect and turn a tuning problem into a reconnect storm. ConnectionFatal is
               left to the general catch below, which is where the reprobe lives; classifying it here
               would take the reprobe away and re-create the bug that arm exists to fix. */
            var elapsedMs = runClock.ElapsedMilliseconds;
            var explanation = PostgresTimeoutExplanation(
                collectorName,
                /* Off the exception, not the runtime: pg_index_bloat fans out per database and opens its
                   own connection for each, so the runtime's initial-probe database is not the one that
                   ran out of time. Naming the wrong database confidently is worse than naming none. */
                CollectorFaultDatabase.For(ex, runtime.ConnectedDatabase),
                elapsedMs,
                /* Off the exception for the same reason, one line on: the machine whose deadline fired is
                   read from what the server actually said, not inferred from the SQLSTATE - which is
                   shared by the target's statement_timeout, an external cancel and our own. Naming the
                   wrong MACHINE confidently is the same defect as naming the wrong database, and this
                   arm's whole job is to be the row an operator acts on. */
                CollectorFaultCancelOrigin.For(ex));

            _logger.LogError("  [{Server}] {Collector} => ERROR (timeout): {Message}",
                server.Config.DisplayName, collectorName, explanation);

            await LogRunAsync(
                server, runtime, collectorName, "ERROR", 0, elapsedMs, 0, explanation,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (Exception ex)
        {
            /* #3095: the COPY phase is named when the fault carries one. This arm is where a collector's
               binary COPY into the STORE lands, on EITHER engine, and that uniformity is deliberate: it
               reaches neither the SQLSTATE arm above (it is not a PostgresException) nor the
               PostgreSQL-target timeout arm (#3111 excludes a proven store write from it, since the store
               connection is Npgsql whatever the target's engine is and the fault would otherwise satisfy
               that arm's every term on a PostgreSQL target). "Exception while reading from stream" is all
               it said: the two COPY phases carry separate deadlines and produce that same string, so the
               message could not say which of them had been reached.

               Computed once and used for BOTH the app log and the collection_log row, because the stored
               row is the instrument any measurement of this population reads; naming the phase only in the
               app log would leave the store's own error rows as ambiguous as they are today.

               Total by construction: a fault with no phase — which is every fault that is not a COPY —
               gets the very string it has now, with nothing allocated. That matters here specifically,
               because this arm is also the OutOfMemoryException landing pad. */
            var message = CollectorFaultCopyPhase.Describe(ex);

            /* #4046 part 1c: a planted byte on a log-tail read keeps ERROR, with its own sentence, and drops a
               stale pg_read_binary_file verdict so a grant made in answer takes effect next cycle. */
            message = LogTailGeneralFault(ex, collectorName, runtime) ?? message;

            _logger.LogError("  [{Server}] {Collector} => ERROR: {Message}",
                server.Config.DisplayName, collectorName, message);

            /* #5003: the first failure of each kind from a collector since the service started is written once more
               with its full text, so the next "Collection was modified" (or any other fault in our own code) names the
               frame that threw. The line above stays as it is for every failure, and so does the collection_log row.

               The text goes into the message rather than only onto the exception object: the file log renders an
               exception it is handed as "Type: Message" and drops the stack (DarlingFileLoggerProvider.Log), and the
               file is the log an operator reads. An OutOfMemoryException is never given one - this arm is its landing
               pad, and building the text allocates.

               The ERROR (timeout) arm above is deliberately left alone. Its filter admits only an NpgsqlException that
               the provider classifies as a command timeout, so its cause is a deadline by construction, the authored
               sentence already names the collector, the database, the elapsed time and the side whose deadline fired,
               and a stack there would be Npgsql's read frames. A bug in our own code cannot reach it. */
            if (_collectorFaultStacks.TakeFirst(collectorName, ex) is { } faultText)
            {
                _logger.LogError(ex,
                    "  [{Server}] {Collector} => ERROR detail: first {ExceptionType} from this collector since the service "
                    + "started, with its full text so the frame that threw is on record. Later failures of this kind log "
                    + "the one line only.{FullText}",
                    server.Config.DisplayName, collectorName, ex.GetType().Name, Environment.NewLine + faultText);
            }

            /* A dead connection poisons every collector — force a reconnect + reprobe. The Postgres arm
               matters as much as the SQL Server one and is deliberately NARROWER than "any
               PostgresException": a cancelled statement (57014) is not a dead socket whichever side
               cancelled it, and dropping the connection over one would turn a tuning problem into a
               reconnect storm. Only the 08 class and the shutdown/unavailability codes qualify, which is
               exactly what the provider's ConnectionFatal means. */
            /* #4710: a SQL Server command timeout (number -2) no longer drops the runtime by itself. The
               connection is still good, and the reconnect re-ran the Extended Events setup and every on-load
               snapshot, so one slow statement cost a stressed server 75 s or more of collection, alerts and
               analysis. ShouldDropRuntimeAsync settles a timeout with a probe on a fresh connection and drops
               only when the probe fails (a pre-login timeout on a hung server is also -2). Class 20+ still
               drops at once. The PostgreSQL arm keeps its rule: ANY exception on a PostgreSQL target, not just
               a PostgresException, because a dead socket surfaces as a plain NpgsqlException with no SQLSTATE,
               and the provider already classifies that as ConnectionFatal. */
            if (await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
                    ex,
                    server.Runtime,
                    LivenessProbeOverride ?? ConnectionFaultDisposition.ProbeFreshConnectionAsync,
                    cancellationToken))
            {
                server.Runtime = null;
                server.NextConnectAttempt = DateTime.UtcNow.AddSeconds(60);
                _logger.LogWarning("[{Server}] Connection-level failure — will reconnect", server.Config.DisplayName);
            }
            else if (ex is SqlException { Number: -2 })
            {
                _logger.LogInformation(
                    "[{Server}] {Collector} timed out, but a fresh connection answered — keeping the connection (#4710)",
                    server.Config.DisplayName, collectorName);
            }

            /* Best-effort store write (#1556): this is also the OutOfMemoryException landing pad (OOM is an
               Exception and no earlier catch claims it), and under an OOM this handler's own LogCollectionAsync
               allocation can itself fail. A throw HERE would fault the fire-and-track body task instead of being
               the isolated, already-logged ERROR above — so swallow a secondary failure (nothing is allocated in
               the catch, to stay safe under the very condition it guards against). The LogError above already
               recorded the fault to the app log, so no signal is lost. */
            try
            {
                /* #2997 item 3: the run's real elapsed time, not the literal zero this arm used to
                   store. duration_ms is sqlMs + storageMs, so three zeros made every failure look
                   instantaneous — a 300-second timeout and a refused connection stored the identical
                   row, and the number that separates them is the only one an operator needs first.
                   Read from the stopwatch rather than from the exception, because most faults carry
                   no duration at all.

                   #5813: the fan-out comes off the exception, where the runner's all-failed rethrow put it, so a
                   one-database server whose Query Store read timed out names that database in slowest_item with
                   the budget as slowest_item_ms. Null for every fault that carries none, as before. */
                await LogRunAsync(
                    server, runtime, collectorName, "ERROR", 0, runClock.ElapsedMilliseconds, 0, message, fanout: CollectorFaultFanout.For(ex), phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            }
            catch
            {
                /* Intentionally empty — see the comment above. Do not add logging here; it must not throw. */
            }

            return 0;
        }
    }

    /// <summary>
    /// #2717 generalized this from query_store's own name (<c>RunDetachedQueryStoreAsync</c>): shared by
    /// every collector fired detached from <see cref="RunDueCollectorsAsync"/>'s sequential body — see
    /// the two call sites for why each one qualifies. <see cref="RunOneAsync"/>'s own catch-all already
    /// contains every fault but cancellation, so this wrapper exists only to keep a shutdown-time
    /// <see cref="OperationCanceledException"/> from surfacing as an unobserved task exception, the same
    /// containment every other fire-and-track body in this file gets.
    /// </summary>
    private async Task RunDetachedAsync(
        ServerLoopState server, DarlingCollectorRunner runner, string collectorName, int? peerMaxAtDispatchMs, CancellationToken cancellationToken,
        bool detachedDaily = false, RunTimeHandOff? runTimeHandOff = null)
    {
        try
        {
            await RunOneAsync(server, runner, collectorName, peerMaxAtDispatchMs, cancellationToken, detachedDaily, runTimeHandOff);
        }
        catch (OperationCanceledException)
        {
            /* Shutdown — expected, and safe to abandon: every collector detached this way is picked
               specifically for having no wall-clock-derived window (query_store's is watermark-driven,
               #1960; plan_correction re-reads the live DMV set whole on every pass), so a run dropped
               here resumes correctly — from the same watermark, or by re-reading the current set — on
               the next start.

               #4938: a daily run is the same. It reads the target as it is now (a delta-family collector
               cannot be scheduled this slowly, see CollectorDeltaCalculator.MaxDeltaFrequencyMinutes), and
               a run dropped here wrote no row, so the next start seeds it overdue from its last-run mark
               and runs it promptly. A run still waiting for a permit ends here too: the wait is on this
               same token. */
        }
    }

    private delegate Task<CollectorRunResult> DispatchEntry(DarlingCollectorRunner runner, ServerRuntime server, CancellationToken cancellationToken);

    /// <summary>Test hook: the collector names the worker can dispatch (pinned against the catalog).</summary>
    internal static IReadOnlyCollection<string> DispatchedCollectorNames => s_dispatch.Keys.ToArray();

    /// <summary>
    /// Collector-name dispatch — the Darling twin of Lite's RunCollectorAsync switch, one typed
    /// entry per shared definition, with Lite's forwarder tolerances mirrored: the XE readers
    /// treat a missing/inaccessible session as zero rows. trace_flags has no tolerance (#5633 item 19):
    /// a failed DBCC TRACESTATUS is a PERMISSIONS or ERROR run, because a SUCCESS run with 0 rows reads as "no flag is enabled".
    /// </summary>
    private static readonly IReadOnlyDictionary<string, DispatchEntry> s_dispatch = new Dictionary<string, DispatchEntry>(StringComparer.OrdinalIgnoreCase)
    {
        ["wait_stats"] = (r, s, ct) => r.RunAsync(WaitStatsCollector.Instance, s, ct),
        ["latch_stats"] = (r, s, ct) => r.RunAsync(LatchStatsCollector.Instance, s, ct),
        ["spinlock_stats"] = (r, s, ct) => r.RunAsync(SpinlockStatsCollector.Instance, s, ct),
        ["cpu_scheduler_stats"] = (r, s, ct) => r.RunAsync(CpuSchedulerStatsCollector.Instance, s, ct),
        ["plan_cache_stats"] = (r, s, ct) => r.RunAsync(PlanCacheStatsCollector.Instance, s, ct),
        ["tempdb_stats"] = (r, s, ct) => r.RunAsync(TempDbStatsCollector.Instance, s, ct),
        ["memory_grant_stats"] = (r, s, ct) => r.RunAsync(MemoryGrantsCollector.Instance, s, ct),
        ["cpu_utilization"] = (r, s, ct) => r.RunAsync(CpuUtilizationCollector.Instance, s, ct),
        ["memory_stats"] = (r, s, ct) => r.RunAsync(MemoryStatsCollector.Instance, s, ct),
        ["memory_clerks"] = (r, s, ct) => r.RunAsync(MemoryClerksCollector.Instance, s, ct),
        ["memory_pressure_events"] = (r, s, ct) => r.RunAsync(MemoryPressureEventsCollector.Instance, s, ct),
        ["file_io_stats"] = (r, s, ct) => r.RunAsync(FileIoStatsCollector.Instance, s, ct),
        ["server_properties"] = (r, s, ct) => r.RunAsync(ServerPropertiesCollector.Instance, s, ct),
        ["server_config"] = (r, s, ct) => r.RunAsync(ServerConfigCollector.Instance, s, ct),
        ["database_config"] = (r, s, ct) => r.RunAsync(DatabaseConfigCollector.Instance, s, ct),
        ["database_states"] = (r, s, ct) => r.RunAsync(DatabaseStateCollector.Instance, s, ct),
        ["trace_flags"] = (r, s, ct) => r.RunAsync(TraceFlagsCollector.Instance, s, ct),
        ["database_scoped_config"] = (r, s, ct) => r.RunAsync(DatabaseScopedConfigCollector.Instance, s, ct),
        ["query_store_health"] = (r, s, ct) => r.RunAsync(QueryStoreHealthCollector.Instance, s, ct),
        ["session_stats"] = (r, s, ct) => r.RunAsync(SessionStatsCollector.Instance, s, ct),
        ["session_summary_stats"] = (r, s, ct) => r.RunAsync(SessionSummaryStatsCollector.Instance, s, ct),
        ["waiting_tasks"] = (r, s, ct) => r.RunAsync(WaitingTasksCollector.Instance, s, ct),
        ["procedure_stats"] = (r, s, ct) => r.RunAsync(ProcedureStatsCollector.Instance, s, ct),
        ["running_jobs"] = (r, s, ct) => r.RunAsync(RunningJobsCollector.Instance, s, ct),
        ["perfmon_stats"] = (r, s, ct) => r.RunAsync(PerfmonStatsCollector.Instance, s, ct),
        ["dmv_blocking_snapshot"] = (r, s, ct) => r.RunAsync(DmvBlockingSnapshotCollector.Instance, s, ct),
        ["database_size_stats"] = (r, s, ct) => r.RunAsync(DatabaseSizeStatsCollector.Instance, s, ct),
        ["index_object_stats"] = (r, s, ct) => r.RunAsync(IndexObjectStatsCollector.Instance, s, ct),
        ["query_stats"] = (r, s, ct) => r.RunAsync(QueryStatsCollector.Instance, s, ct),
        ["query_snapshots"] = (r, s, ct) => r.RunAsync(QuerySnapshotsCollector.Instance, s, ct),
        ["query_store"] = (r, s, ct) => r.RunAsync(QueryStoreCollector.Instance, s, ct),
        ["deadlocks"] = (r, s, ct) => RunXeTolerantAsync(DeadlocksCollector.Instance, r, s, ct),
        ["blocked_process_report"] = (r, s, ct) => RunXeTolerantAsync(BlockedProcessReportCollector.Instance, r, s, ct),
        ["attention"] = (r, s, ct) => RunXeTolerantAsync(AttentionCollector.Instance, r, s, ct),
        ["long_query_completions"] = (r, s, ct) => RunXeTolerantAsync(LongQueryCompletionsCollector.Instance, r, s, ct),
        ["system_health_events"] = (r, s, ct) => r.RunAsync(SystemHealthEventsCollector.Instance, s, ct),
        ["default_trace_events"] = (r, s, ct) => r.RunAsync(DefaultTraceEventsCollector.Instance, s, ct),
        ["job_history"] = (r, s, ct) => r.RunAsync(JobHistoryCollector.Instance, s, ct),
        ["agent_status"] = (r, s, ct) => r.RunAsync(AgentStatusCollector.Instance, s, ct),
        ["ag_replica_states"] = (r, s, ct) => r.RunAsync(AgReplicaStatesCollector.Instance, s, ct),
        ["ag_database_replica_states"] = (r, s, ct) => r.RunAsync(AgDatabaseReplicaStatesCollector.Instance, s, ct),
        ["plan_correction"] = (r, s, ct) => r.RunAsync(PlanCorrectionCollector.Instance, s, ct),
        ["query_store_forced_plans"] = (r, s, ct) => r.RunAsync(QueryStoreForcedPlansCollector.Instance, s, ct),
        ["query_store_hints"] = (r, s, ct) => r.RunAsync(QueryStoreHintsCollector.Instance, s, ct),
        ["identity_capacity"] = (r, s, ct) => r.RunAsync(IdentityCapacityCollector.Instance, s, ct),
        ["agent_alerts"] = (r, s, ct) => r.RunAsync(AgentAlertsCollector.Instance, s, ct),
        ["database_protection"] = (r, s, ct) => r.RunAsync(DatabaseProtectionCollector.Instance, s, ct),
        ["pvs_stats"] = (r, s, ct) => r.RunAsync(PvsStatsCollector.Instance, s, ct),
        /* PostgreSQL. Dispatch is by name and engine-agnostic; the engine gate upstream in
           RunDueCollectorsAsync means this lambda is only ever reached for a Postgres target. */
        ["pg_wait_stats"] = (r, s, ct) => r.RunAsync(PgWaitStatsCollector.Instance, s, ct),
        ["pg_statement_stats"] = (r, s, ct) => r.RunAsync(PgStatementStatsCollector.Instance, s, ct),
        ["pg_wraparound_stats"] = (r, s, ct) => r.RunAsync(PgWraparoundStatsCollector.Instance, s, ct),
        ["pg_server_config"] = (r, s, ct) => r.RunAsync(PgServerConfigCollector.Instance, s, ct),
        /* TWO TRANSPORTS, one table, same reason as pg_plan_capture below: self-hosted reads the server
           log with pg_read_file; Aurora and RDS have no filesystem and pg_read_server_files is not
           grantable, so those go through the AWS log API instead. This branch was the missing half — the
           collector's own AppliesTo returns true for every target on the assumption the route is "chosen
           at dispatch", but before this there was no dispatch branch, so every Aurora target fell through
           to the pg_read_file route and failed PERMISSIONS 100% of the time (no grant fixes a filesystem
           that is not there). */
        ["pg_deadlocks"] = (r, s, ct) =>
            s.Target.IsAurora || s.Target.IsAwsRds
                ? r.IngestRdsDeadlocksAsync(s, ct)
                : r.RunAsync(PgDeadlocksCollector.Instance, s, ct),
        /* TWO TRANSPORTS, one table, the third time (#3601): the classified log-event pipeline reads the
           same server log by the same two roads as its two siblings above and below, and the classifier
           both roads feed is one instance, so the rows are the same whichever road the text took. */
        ["pg_log_events"] = (r, s, ct) =>
            s.Target.IsAurora || s.Target.IsAwsRds
                ? r.IngestRdsLogEventsAsync(s, ct)
                : r.RunAsync(PgLogEventsCollector.Instance, s, ct),
        ["pg_xmin_horizon"] = (r, s, ct) => r.RunAsync(PgXminHorizonCollector.Instance, s, ct),
        ["pg_replication_slots"] = (r, s, ct) => r.RunAsync(PgReplicationSlotsCollector.Instance, s, ct),
        ["pg_autovacuum_stats"] = (r, s, ct) => r.RunAsync(PgAutovacuumStatsCollector.Instance, s, ct),
        ["pg_io_stats"] = (r, s, ct) => r.RunAsync(PgIoStatsCollector.Instance, s, ct),
        ["pg_blocking"] = (r, s, ct) => r.RunAsync(PgBlockingCollector.Instance, s, ct),
        ["pg_database_stats"] = (r, s, ct) => r.RunAsync(PgDatabaseStatsCollector.Instance, s, ct),
        ["pg_index_usage_stats"] = (r, s, ct) => r.RunAsync(PgIndexUsageStatsCollector.Instance, s, ct),
        ["pg_table_bloat_stats"] = (r, s, ct) => r.RunAsync(PgTableBloatStatsCollector.Instance, s, ct),
        ["pg_session_states"] = (r, s, ct) => r.RunAsync(PgSessionStatesCollector.Instance, s, ct),
        ["pg_plan_capture_readiness"] = (r, s, ct) => r.RunAsync(PgPlanCaptureReadinessCollector.Instance, s, ct),
        ["pg_write_stats"] = (r, s, ct) => r.RunAsync(PgWriteStatsCollector.Instance, s, ct),
        ["pg_extension_availability"] = (r, s, ct) => r.RunAsync(PgExtensionAvailabilityCollector.Instance, s, ct),
        ["pg_lock_stats"] = (r, s, ct) => r.RunAsync(PgLockStatsCollector.Instance, s, ct),
        ["pg_wait_sampling"] = (r, s, ct) => r.RunAsync(PgWaitSamplingCollector.Instance, s, ct),
        ["pg_kernel_stats"] = (r, s, ct) => r.RunAsync(PgKernelStatsCollector.Instance, s, ct),
        ["pg_predicate_stats"] = (r, s, ct) => r.RunAsync(PgPredicateStatsCollector.Instance, s, ct),
        /* TWO TRANSPORTS, one table. Self-hosted reads the server log with pg_read_file; Aurora and RDS
           have no filesystem and pg_read_server_files is not grantable, so those go through the AWS log
           API instead (#2538). The collector's own AppliesTo excludes managed targets, so without this
           branch they would simply never capture a plan - and would look like they had nothing to say
           rather than like they were on a different road. */
        ["pg_plan_capture"] = (r, s, ct) =>
            s.Target.IsAurora || s.Target.IsAwsRds
                ? r.IngestRdsPlansAsync(s, ct)
                : r.RunAsync(PgPlanCaptureCollector.Instance, s, ct),
        ["pg_column_stats"] = (r, s, ct) => r.RunAsync(PgColumnStatsCollector.Instance, s, ct),
        ["pg_replication_stats"] = (r, s, ct) => r.RunAsync(PgReplicationStatsCollector.Instance, s, ct),
        ["pg_buffer_usage"] = (r, s, ct) => r.RunAsync(PgBufferUsageCollector.Instance, s, ct),
        ["pg_index_bloat"] = (r, s, ct) => r.RunAsync(PgIndexBloatCollector.Instance, s, ct),
        /* ONE TRANSPORT, unconditionally — unlike pg_deadlocks/pg_plan_capture above, there is no
           pg_read_file-shaped fallback for a self-hosted target, because PostgreSQL exposes no
           instance-level CPU signal at all (#2719, see PgCpuUtilizationCollector's doc comment). A
           self-hosted host resolves to nothing in IngestPgCpuAsync's RdsEndpoint.TryParse and the ingestor
           no-ops, the same "not this transport" answer RdsLogSource itself gives a non-RDS host. */
        ["pg_cpu_utilization"] = (r, s, ct) => r.IngestPgCpuAsync(s, ct),
        /* #3691 (V136): the per-database size series, an ordinary SQL collector over the shared catalog.
           (The rung's other series, host memory, has no entry here: it is six columns on the CPU row the
           pg_cpu_utilization entry above already writes from the same Performance Insights call.) */
        ["pg_database_size_stats"] = (r, s, ct) => r.RunAsync(PgDatabaseSizeStatsCollector.Instance, s, ct),
    };

    /// <summary>
    /// Signals that a blocking/deadlock XE session is missing or inaccessible so the reader returned no
    /// events. <see cref="RunXeTolerantAsync"/> throws it, <see cref="RunOneAsync"/> catches it and logs a
    /// distinct <c>SESSION_MISSING</c> collection_log status. Before Stage 4 this case swallowed to zero
    /// rows and logged SUCCESS — indistinguishable from a genuinely idle session, so the Capture Down
    /// self-alert had no signal to read.
    /// </summary>
    private sealed class DarlingXeSessionMissingException : Exception
    {
        public DarlingXeSessionMissingException(string message, Exception inner) : base(message, inner) { }

        /// <summary>
        /// #3754: the reconcile-side case, where the session could not be CREATED (rather than read) and the
        /// exception that said so was caught a sweep phase earlier in <c>ReconcileLongQueryTraceAsync</c>.
        /// Only its message survives to the run, on <see cref="ServerLoopState.LongQueryTraceFault"/>, so
        /// there is no inner exception to carry - and the arm that catches this reads the message alone.
        /// </summary>
        public DarlingXeSessionMissingException(string message) : base(message) { }
    }

    /// <summary>
    /// #5378: the reconcile's kept failure to create the long-query session was a permission denial. Only its message
    /// survives to the run, like <see cref="DarlingXeSessionMissingException"/>; the run records PERMISSIONS for it.
    /// </summary>
    private sealed class DarlingXeSessionDeniedException : Exception
    {
        public DarlingXeSessionDeniedException(string message) : base(message) { }
    }

    private static async Task<CollectorRunResult> RunXeTolerantAsync<TRow>(
        ICollectorDefinition<TRow> definition, DarlingCollectorRunner runner, ServerRuntime server, CancellationToken cancellationToken)
    {
        try
        {
            return await runner.RunAsync(definition, server, cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            /* XE session not found or not accessible. Lite swallows this to zero rows, but a headless
               service (like the Dashboard) must surface it: raise it as a distinct SESSION_MISSING
               collection_log status so the Stage 4 Capture Down self-alert can detect that blocking/
               deadlock capture is non-functional. RunOneAsync catches this and continues the sweep, so
               collection is still tolerant — only the logged status differs from a real zero-row success. */
            throw new DarlingXeSessionMissingException(ex.Message, ex);
        }
    }
}
