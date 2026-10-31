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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingCollectorRunner
{
    /// <summary>
    /// The choice this install has made for each deadlock and blocked-process session in each Azure SQL Database it monitors
    /// (#4961): the shared session, or its own. The ensure sets it and the per-database read takes its name from it. In
    /// memory only: a restart starts every database at the shared session, and the first ensure sets it again.
    /// </summary>
    internal AlwaysOnXeChoices AlwaysOnChoices { get; } = new();

    /// <summary>This install's own session of the capture, or null when the runner has no install id to make it from.</summary>
    internal string? AlwaysOnOwnSessionName(AlwaysOnXeSessionKind kind) =>
        AlwaysOnXeSessions.TryOwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallId, kind);

    /// <summary>
    /// The session a read of the capture names in one Azure SQL Database: this install's own when the ensure fell back to it
    /// there, else the shared name.
    /// </summary>
    internal string AlwaysOnReadSessionName(ServerRuntime server, string databaseName, AlwaysOnXeSessionKind kind) =>
        AlwaysOnChoices.NameFor(DarlingAlwaysOnXeSessions.ServerKey(server), databaseName, kind, AlwaysOnOwnSessionName(kind));

    /// <summary>
    /// Replaces the connection to one Azure SQL Database for the always-on sessions' ensure: the server and the database. The
    /// ensure's decisions then run against what it returns, in place of a server. Null in production.
    /// </summary>
    internal Func<ServerRuntime, string, CancellationToken, Task<IAlwaysOnXeDatabase>>? AlwaysOnXeDatabaseForTests { get; set; }

    /// <summary>
    /// A test replaces each connection the always-on sessions' ensure would open to one Azure SQL Database, below
    /// <see cref="AlwaysOnXeDatabaseForTests"/> (which wins when both are set): the server, the database, and the connection
    /// string the open would use, so a test sees whether each statement goes over a connection with read-only intent (#4961).
    /// Null in production.
    /// </summary>
    internal Func<ServerRuntime, string, string, CancellationToken, Task<IAlwaysOnXeDatabase>>? AlwaysOnXeConnectionForTests { get; set; }

    /// <summary>
    /// Replaces the whole always-on ensure of one server, which otherwise opens a connection to it: a test counts the calls.
    /// Null in production.
    /// </summary>
    internal Func<ServerRuntime, CancellationToken, Task>? XeEnsureOverrideForTests { get; set; }

    /// <summary>Replaces the engine target provider resolved for a run. Null in production.</summary>
    internal Func<CollectorTargetInfo, ITargetProvider>? TargetProviderOverrideForTests { get; set; }

    /// <summary>Replaces the Azure per-database list. Null in production.</summary>
    internal Func<ServerRuntime, CancellationToken, Task<List<string>>>? AzureDatabaseListOverrideForTests { get; set; }

    /// <summary>
    /// Replaces the database list the long-query trace reads on Azure SQL Database. Called with <c>allDatabases</c> true
    /// for every online database, false for the monitored ones narrowed by the given database scope. Null in production.
    /// </summary>
    internal Func<ServerRuntime, bool, IReadOnlyList<string>?, CancellationToken, Task<List<string>>>? LongQueryTraceListOverrideForTests { get; set; }

    /// <summary>
    /// Replaces the long-query trace's work in one Azure SQL Database database: called with <c>create</c> true to create
    /// the session there, false to drop it. Null in production.
    /// </summary>
    internal Func<ServerRuntime, string, bool, string, CancellationToken, Task>? LongQueryTraceDatabaseOverrideForTests { get; set; }

    /// <summary>
    /// Replaces where the one-time legacy drop keeps its record (<see cref="ILegacyLongQueryRecords"/>), so a test that never
    /// opens the store has one. Set before the first reconcile. Null in production.
    /// </summary>
    internal ILegacyLongQueryRecords? LegacyLongQueryRecordsForTests { get; set; }

    private DarlingLegacyLongQuerySession? _legacyLongQuery;

    /// <summary>
    /// The one-time drop of the long-query session older versions shared between installs, and what it remembers of its
    /// record for each registration (#4961). Built on first use, over the store.
    /// </summary>
    internal DarlingLegacyLongQuerySession LegacyLongQuery =>
        LazyInitializer.EnsureInitialized(ref _legacyLongQuery, () => new DarlingLegacyLongQuerySession(LegacyLongQueryRecordsForTests ?? new StoreLegacyLongQueryRecords(_postgres)))!;

    /// <summary>
    /// Replaces the question the create path asks in the same batch as its existence check: whether the legacy session is in
    /// the database (the empty name is the server). Null in production.
    /// </summary>
    internal Func<ServerRuntime, string, CancellationToken, Task<bool>>? LegacyLongQueryPresentForTests { get; set; }

    /// <summary>
    /// Replaces one open-and-act step of the long-query trace's create, below
    /// <see cref="LongQueryTraceDatabaseOverrideForTests"/>, which wins when both are set (#4961). Called with the server,
    /// the database (empty for the server's own session), the connection string the step would open, the step, and the
    /// session name. A test sees which connection each step uses, with or without read-only intent. The step's work is
    /// not done. Null in production.
    /// </summary>
    internal Func<ServerRuntime, string, string, LongQueryTraceStep, string, CancellationToken, Task>? LongQueryTraceStepOverrideForTests { get; set; }

    /// <summary>
    /// Replaces what the read-only-intent ensure's <see cref="LongQueryTraceStep.Check"/> reads on the replica, with
    /// <see cref="LongQueryTraceStepOverrideForTests"/> set (#4961): called with the server and the database, answers whether the
    /// session's definition is visible and whether it runs there. Unset, the stand-in finds neither, as in a database that has
    /// never had the session. Null in production.
    /// </summary>
    internal Func<ServerRuntime, string, LongQueryTraceReplicaState>? LongQueryTraceReplicaStateForTests { get; set; }

    /// <summary>
    /// The databases the long-query trace works in on Azure SQL Database. With <paramref name="allDatabases"/>, every
    /// online database with no exclusions and no scope; otherwise the monitored ones, narrowed by
    /// <paramref name="databaseScope"/>. A registration that names a database gets that database either way.
    /// </summary>
    internal async Task<List<string>> ListLongQueryTraceDatabasesAsync(ServerRuntime server, bool allDatabases, IReadOnlyList<string>? databaseScope, CancellationToken cancellationToken) =>
        LongQueryTraceListOverrideForTests is { } listOverride
            ? await listOverride(server, allDatabases, allDatabases ? null : databaseScope, cancellationToken)
            : allDatabases
                ? await GetAzureDatabaseListAsync(server, databaseScope: null, applyExclusions: false, cancellationToken)
                : await GetAzureDatabaseListAsync(server, databaseScope, cancellationToken);

    /// <summary>Called with the database name just before its batch is written, inside the loop's try. Null in production.</summary>
    internal Action<string>? PerDatabaseWriteFaultForTests { get; set; }

    /// <summary>
    /// The databases one Azure SQL DB registration's per-database sweep covers.
    ///
    /// <para><b>A registration that names a database sweeps that database, and nothing else</b> (#2220) —
    /// which is the common case, since <c>server_id</c> hashes <c>host[:database][:RO]</c> and registering
    /// each database separately is how you get separate identities. That path returns immediately and never
    /// touches <c>master</c>.</para>
    ///
    /// <para>Only a registration naming NO database — or naming <c>master</c>, where a catalog-less Azure
    /// connection lands — is a registration of the logical SERVER, and only that one enumerates: master
    /// first with the per-server exclusion filter, and on a master-access error a fallback that has nothing
    /// to fall back to and therefore throws (#857's shape, now the exceptional path rather than the default).
    /// The re-probe throttle is deliberately NOT consulted there; see the comment at the call site.</para>
    ///
    /// <para>It read master unconditionally before #2220, sweeping every online database on the logical
    /// server into whichever registration ran the sweep — N registrations of N databases meant N² collection
    /// with every registration's history contaminated by its siblings'.</para>
    /// </summary>
    internal Task<List<string>> GetAzureDatabaseListAsync(ServerRuntime server, IReadOnlyList<string>? databaseScope, CancellationToken cancellationToken) =>
        GetAzureDatabaseListAsync(server, databaseScope, applyExclusions: true, cancellationToken);

    /// <summary>
    /// <see cref="GetAzureDatabaseListAsync(ServerRuntime, IReadOnlyList{string}, CancellationToken)"/>, with a choice about the
    /// server's excluded databases. The long-query trace's drop passes <paramref name="applyExclusions"/> false: a session
    /// created before a database was excluded must still be dropped there.
    /// </summary>
    internal async Task<List<string>> GetAzureDatabaseListAsync(ServerRuntime server, IReadOnlyList<string>? databaseScope, bool applyExclusions, CancellationToken cancellationToken)
    {
        var targetDb = new SqlConnectionStringBuilder(server.ConnectionString).InitialCatalog;

        /* #2220: a registration that NAMES a database is a registration OF that database, so its sweep
           covers exactly that one and never touches master. Before this, EVERY database-scoped collector
           enumerated master and swept every online database on the logical server, storing all of it under
           the one server_id of whichever registration ran the sweep — N registrations of N databases on one
           server meant N² collection with every registration's history contaminated by its siblings'.

           This also subsumes the #857 case it looks like it bypasses, and improves on it: a login granted
           access to one user database but not to master HAS a named database, so it now returns here without
           probing master at all, rather than probing, failing, forming a verdict and falling back. Master is
           reached only by a registration that names no database — the logical-server registration, which has
           nothing else to enumerate from. */
        /* #3477: the database scope deliberately does NOT filter this named-database return, for the
           parity reason that governs the whole feature — ExcludedDatabases has never filtered it
           either. A registration that names a database IS that database; the way to stop collecting
           it is the collector's enabled flag for that server, not a scope that would have to be
           matched client-side with a comparer the engine never sees. */
        var ownDatabase = AzureSweepScope.OwnDatabaseOrEmpty(targetDb);
        if (ownDatabase.Count > 0)
        {
            return ownDatabase;
        }

        /* NO throttle check here, and that is deliberate rather than an omission — restoring what the
           `hasFallback &&` guard used to achieve. This branch is reached ONLY when the registration names no
           database, so there is nothing to fall back TO: honouring the throttle would return
           FallbackDatabaseList, which throws immediately without probing, and would keep throwing for the
           whole recheck interval while never attempting the one thing that could recover. Probing master
           every cycle is the cheaper failure. (Review caught me reintroducing exactly this: I read
           `hasFallback &&` as a redundant condition when it was there to DISABLE the throttle.)

           The throttle machinery itself is left alone. It is tested behaviour from #857/#1506, and it is now
           unreachable in production for a different reason than this one: its whole purpose was to stop
           re-probing master for a registration that HAS a fallback, and such a registration no longer probes
           master at all. Retiring it is its own change, with those tests. */

        /* The query and the hop to master both come from the provider, so the enumeration set is defined
           in exactly one place per engine. What stays here is the failure policy below, which is the
           part that is genuinely Azure-specific. */
        var (masterConnectionString, enumerationQuery) = AzureDatabaseListPlan(server, databaseScope, applyExclusions);

        var databases = new List<string>();
        try
        {
            using var connection = new SqlConnection(masterConnectionString);
            await connection.OpenAsync(cancellationToken);
            /* Azure master enumeration is SQL-Server-only, but it goes through the same parameter
               mapping as every other command so a type cannot be mapped two ways. */
            using var command = CreateCollectorCommand(enumerationQuery, connection, CommandTimeoutSeconds);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                databases.Add(reader.GetString(0));
            }

            _azureMasterInaccessibleSince.TryRemove(server.ServerId, out _);
            return databases;
        }
        catch (SqlException ex) when (ShouldFallBackToSingleDatabaseError(ex.Number))
        {
            _azureMasterInaccessibleSince[server.ServerId] = DateTime.UtcNow;

            return FallbackDatabaseList(
                server, targetDb, reason: $"master DB inaccessible (SQL error {ex.Number})",
                errorNumber: ex.Number, serverMessage: ex.Message);
        }
    }

    /// <summary>
    /// The master hop and the enumeration query behind <see cref="GetAzureDatabaseListAsync(ServerRuntime, IReadOnlyList{string}, bool, CancellationToken)"/>.
    /// The server's excluded databases are in the query only when <paramref name="applyExclusions"/> is true, so a drop that
    /// passes false lists the databases the registration excludes too. Its own member so a test reads the query, not a stand-in
    /// for the list.
    /// </summary>
    internal static (string ConnectionString, CollectorQuery Query) AzureDatabaseListPlan(
        ServerRuntime server, IReadOnlyList<string>? databaseScope, bool applyExclusions) =>
        SqlServerTargetProvider.Instance.BuildDatabaseListPlan(
            server.ConnectionString, applyExclusions ? server.Config.ExcludedDatabases : null, databaseScope);

    /// <summary>
    /// True while a recent master-inaccessible verdict still stands. It expires so a server whose
    /// access was restored recovers on its own rather than staying degraded until restart (#1506).
    /// </summary>
    private bool IsMasterProbeThrottled(int serverId)
    {
        if (!_azureMasterInaccessibleSince.TryGetValue(serverId, out var deniedAt))
        {
            return false;
        }

        if (!CollectorCadence.IntervalElapsed(deniedAt, DateTime.UtcNow, AzureMasterRecheckInterval))
        {
            return true;
        }

        _azureMasterInaccessibleSince.TryRemove(serverId, out _);
        return false;
    }

    /// <summary>
    /// The database list to use when master cannot be enumerated: the connection's own catalog.
    ///
    /// When there isn't one, database-scoped collectors have nowhere to read from. That used to be a
    /// warning and an empty list, which made every one of them report success having collected zero
    /// rows. Throwing puts the failure where it can actually be seen (#1506).
    /// </summary>
    /// <param name="quiet">
    /// Set on the throttled path, which runs for every database-scoped collector on every cycle. Only
    /// forming the verdict is worth an Information line; re-reading it is not.
    /// </param>
    internal List<string> FallbackDatabaseList(
        ServerRuntime server, string? targetDb, string reason, bool quiet = false,
        int? errorNumber = null, string? serverMessage = null)
    {
        var fallback = SingleDbOrEmpty(targetDb);

        if (fallback.Count == 0)
        {
            /* #5498: the firewall comes first. #5647: 40615 means the server's firewall did not allow the
               client's IP address, which a missing database setting never causes, so the message leads with the
               firewall and then offers the set-a-database fix for a client allowed only by a database-level rule
               in a user database. Text only: the fallback itself is unchanged. */
            throw new InvalidOperationException(AzureMasterFallbackMessage.Build(
                reason, errorNumber, serverMessage, server.Config.DisplayName, "database"));
        }

        if (quiet)
        {
            _logger?.LogDebug("[{Server}] {Reason} — collecting from '{Database}' only.",
                server.Config.DisplayName, reason, targetDb);
        }
        else
        {
            _logger?.LogInformation("[{Server}] {Reason} — collecting from '{Database}' only.",
                server.Config.DisplayName, reason, targetDb);
        }

        return fallback;
    }

    internal async Task<SqlConnection> OpenAzureDatabaseConnectionAsync(ServerRuntime server, string databaseName, CancellationToken cancellationToken)
        => (SqlConnection)await OpenDatabaseConnectionAsync(
            SqlServerTargetProvider.Instance, server, databaseName, cancellationToken);

    /// <summary>
    /// #5158: the second half of a deferred query_stats read. The main query returned stats rows with no plan; this
    /// gives each row its plan, rendering on the monitored server only the plans this host has not already committed.
    ///
    /// <para>A row whose identity (<see cref="QueryStatsPlanKey"/>) is in the host's cache carries the cached digest and
    /// measured size, so the writer stores the digest and no plan text crosses the wire. A plan over the capture cap
    /// is cached with a null digest: its rows carry the size and a NULL plan, and it is not rendered again. The
    /// rest go to <see cref="QueryStatsCollector.BuildPlanFetchQuery"/>, one call (split above
    /// <see cref="QueryStatsCollector.MaxPlanFetchKeys"/> keys), on the same connection as the main read. A plan
    /// whose handle aged out between the two queries comes back with no plan and no size, the pairing the inline form
    /// gives for the same case, and is not cached.</para>
    ///
    /// <para>A fetch that fails ships the rows without plans and does not fail the run: the plans not yet fetched
    /// are fetched again next cycle. A chunk that fails keeps what the chunks before it already rendered (those
    /// rows carry their plans, and their keys are returned as pending like any other); it caches nothing for the
    /// failed chunk or any after it. A stop or the item budget expiring is not a fetch failure and propagates.
    /// What this run rendered is returned as pending keys: they are confirmed after the batch commits, or discarded.</para>
    ///
    /// <para><b>Timing.</b> The fetch loop runs after the main read, inside the same <c>sql:</c> slice but outside
    /// the drain, so it has its own stopwatch: <see cref="CollectorContext.PerItemPlanFetchMs"/> and the
    /// <c>plan_fetch_ms</c> measurement carry it, beside <c>plans_rendered</c> and <c>plans_rendered_bytes</c>, which are
    /// rendered from <see cref="CollectorContext.PerItemPlanRenderedRows"/> and
    /// <see cref="CollectorContext.PerItemPlanRenderedBytes"/>. A run with misses records the loop's elapsed
    /// milliseconds; a run with none records 0, because it ran no fetch.</para>
    ///
    /// <para><b>Accepted race.</b> A statement recompile between the main query and this fetch can render the new
    /// plan and store its digest under the old identity (<c>creation_time</c>, <c>plan_generation_num</c>). That
    /// pairing is wrong for that one run's row only: the old identity leaves <c>dm_exec_query_stats</c> with the
    /// recompile, so nothing reads it again, and the cache prunes it after an hour. The inline form has the same
    /// race in a smaller window.</para>
    /// </summary>
    private async Task<List<QueryStatsPlanKey>> FetchDeferredQueryStatsPlansAsync(
        ITargetProvider provider,
        DbConnection targetConnection,
        ServerRuntime server,
        CollectorContext context,
        List<QueryStatsCollector.Row> rows,
        CancellationToken cancellationToken)
    {
        var cache = _queryStatsPlanCaches.GetOrAdd(server.ServerId, static _ => new PlanDigestCache<QueryStatsPlanKey>());
        var pending = new List<QueryStatsPlanKey>();
        var now = DateTime.UtcNow;

        cache.Prune(now - QueryStatsPlanCacheMaxAge);

        var misses = new List<(QueryStatsPlanKey Key, QueryStatsCollector.Row Row)>();
        foreach (var row in rows)
        {
            if (QueryStatsPlanKey.TryCreate(server.ServerId, row) is not { } key)
            {
                continue;
            }

            if (cache.TryGet(key, now, out var hit))
            {
                /* The cached size rides along with the digest, so a reused row is the row it always was. A null
                   digest is an over-cap plan: the size and no plan, the pairing the inline capture produces. */
                row.KnownPlanDigest = hit.Digest;
                row.QueryPlanXmlBytes = hit.Bytes;
            }
            else
            {
                misses.Add((key, row));
            }
        }

        if (BetweenPlanPhasesForTests is { } hook)
        {
            await hook(cancellationToken);
        }

        var renderedRows = 0;
        long renderedBytes = 0;
        var fetchWatch = Stopwatch.StartNew();
        if (misses.Count == 0)
        {
            /* Nothing to render: no fetch ran, so the stamps read 0 and the measurements say so. */
            fetchWatch.Stop();
            StampPlanFetch(context, 0, 0, 0);
            return pending;
        }

        try
        {
            for (var offset = 0; offset < misses.Count; offset += QueryStatsCollector.MaxPlanFetchKeys)
            {
                var chunk = misses.GetRange(offset, Math.Min(QueryStatsCollector.MaxPlanFetchKeys, misses.Count - offset));
                var query = QueryStatsCollector.BuildPlanFetchQuery(context, chunk.ConvertAll(m => m.Key.ToFetchKey()));

                Dictionary<int, (string? PlanXml, long? Bytes)> fetched;
                var unjudged = new HashSet<int>();
                using (var command = CreateCollectorCommand(provider, query, targetConnection, CommandTimeoutSeconds))
                using (var planReader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    /* #4348: one filter session per fetch call; the digest below is taken from the FILTERED plan. */
                    fetched = await QueryStatsCollector.ReadPlanFetchAsync(planReader, context.BeginStatementScrub(), cancellationToken, unjudged);
                }

                foreach (var (ord, result) in fetched)
                {
                    var (key, row) = chunk[ord];
                    if (result.PlanXml is null && result.Bytes is null)
                    {
                        /* Aged out between the two queries: no plan and no size, and nothing worth caching. */
                        continue;
                    }

                    row.QueryPlanXmlBytes = result.Bytes;
                    renderedRows++;
                    renderedBytes += result.Bytes ?? 0;

                    if (result.PlanXml is null)
                    {
                        /* Over the cap: the size, a NULL plan, and an entry that stops the next run rendering it again. */
                        cache.AddPending(key, null, result.Bytes, now);
                    }
                    else if (unjudged.Contains(ord))
                    {
                        /* #4348: the filter's budget ran out before or during this plan. This row stores the marker; nothing
                           is cached and nothing is pending, so the next cycle fetches and filters the plan again with a
                           fresh budget instead of answering from a cached marker digest. A plan withheld whole for its own
                           sake (its judge threw, it cannot be parsed) is not in this set: it fails the same way every
                           cycle, so it is recorded like any fetched plan and the next cycle's dedup sees it (A-L4). */
                        row.QueryPlanXml = result.PlanXml;
                        continue;
                    }
                    else
                    {
                        row.QueryPlanXml = result.PlanXml;
                        var digest = Convert.ToHexString(
                            PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(result.PlanXml)));
                        cache.AddPending(key, digest, result.Bytes, now);
                    }

                    pending.Add(key);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException
                                   && !cancellationToken.IsCancellationRequested)
        {
            _logger?.LogWarning(
                ex,
                "{Collector} on '{Server}': the plan fetch failed, so this run's rows ship without plans for the plans not yet stored; they are fetched again next cycle (#5158).",
                QueryStatsCollector.Instance.Name, server.Config.DisplayName);
        }
        finally
        {
            fetchWatch.Stop();
            StampPlanFetch(context, renderedRows, renderedBytes, fetchWatch.ElapsedMilliseconds);
        }

        return pending;
    }

    /// <summary>
    /// #5158: records one deferred fetch on the context, then renders the run's measurements FROM the context, so the
    /// stamp is the single source and the collection_log note only reads it. <c>PerItemPlanFetchMs</c> is "inside
    /// <c>sql:</c>, not drain"; a procedure_stats change that shares these names will stamp the same fields.
    /// </summary>
    internal static void StampPlanFetch(CollectorContext context, int renderedRows, long renderedBytes, long fetchMs)
    {
        context.PerItemPlanRenderedRows = renderedRows;
        context.PerItemPlanRenderedBytes = renderedBytes;
        context.PerItemPlanFetchMs = fetchMs;
        context.Measure("plans_rendered", context.PerItemPlanRenderedRows);
        context.Measure("plans_rendered_bytes", context.PerItemPlanRenderedBytes);
        context.Measure("plan_fetch_ms", context.PerItemPlanFetchMs);
    }

    /// <summary>
    /// #5449: procedure_stats' plan phase. Runs one second target query for the first
    /// <see cref="ProcedureStatsCollector.MaxPlansPerRun"/> kept rows with a parsable plan handle, on the main read's
    /// connection and inside its budget, and merges each row's plan and identity onto it. Closes the main reader first (one
    /// command at a time on the connection), as the deferred fetch does. A failed query ships every row without plans and
    /// marks them all skipped, so the reuse pass counts no miss and caches nothing; the plans are fetched again next
    /// cycle. A stop or the item budget expiring is not a phase failure and propagates. Returns the phase's milliseconds,
    /// 0 when no query ran. A null <paramref name="mainReader"/> is a caller whose main reader is already disposed (the
    /// Azure SQL Database per-database loop, see <see cref="RunProcedureStatsDatabasePlanPhaseAsync"/>).
    /// </summary>
    internal async Task<long> RunProcedureStatsPlanPhaseAsync(
        ITargetProvider provider,
        DbDataReader? mainReader,
        DbConnection targetConnection,
        ServerRuntime server,
        CollectorContext context,
        List<ProcedureStatsCollector.Row> rows,
        CancellationToken cancellationToken)
    {
        var indexes = ProcedureStatsCollector.SelectPlanPhaseRows(rows, out var handles);
        if (indexes.Count == 0)
        {
            ProcedureStatsCollector.ApplyPlanPhase(rows, indexes, new Dictionary<int, ProcedureStatsCollector.PlanPhaseResult>());
            return 0;
        }

        if (mainReader is not null)
        {
            await mainReader.CloseAsync();
        }

        var watch = Stopwatch.StartNew();
        Dictionary<int, ProcedureStatsCollector.PlanPhaseResult>? results = null;
        try
        {
            var query = ProcedureStatsCollector.BuildPlanPhaseQuery(context, handles);
            using var command = CreateCollectorCommand(provider, query, targetConnection, CommandTimeoutSeconds);
            using var phaseReader = await command.ExecuteReaderAsync(cancellationToken);
            results = await ProcedureStatsCollector.ReadPlanPhaseAsync(phaseReader, context, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException
                                   && !cancellationToken.IsCancellationRequested)
        {
            _logger?.LogWarning(
                ex,
                "{Collector} on '{Server}': the plan query failed, so this run's rows ship without plans; they are fetched again next cycle (#5449).",
                ProcedureStatsCollector.Instance.Name, server.Config.DisplayName);
        }

        ProcedureStatsCollector.ApplyPlanPhase(rows, indexes, results);
        return Math.Max(1L, watch.ElapsedMilliseconds);
    }

    /// <summary>
    /// #6182: procedure_stats' plan phase for ONE database of an Azure SQL Database collection. That target is read per
    /// database (<c>ProcedureStatsCollector.RunsPerDatabase</c>), through a loop that opens a connection per database and
    /// never reaches the server-scoped branch that runs <see cref="RunProcedureStatsPlanPhaseAsync"/>, so until now no
    /// procedure it read was stored with a plan. Azure always runs procedure_stats in Off mode (no identity columns, no deferred
    /// fetch), so the plan phase renders the plans itself and there is no reuse pass: this stamps what it rendered as the
    /// server-scoped Off mode does. The whole phase is one target query, so its time is also the run's target time for
    /// <c>plan_fetch</c>. A failed query ships this database's rows without plans and does not fail the run (the phase's own
    /// rule); a stop or the item budget expiring propagates. Sends nothing when the run captures no plans (a gated cycle) or
    /// the database returned no rows.
    /// </summary>
    internal async Task RunProcedureStatsDatabasePlanPhaseAsync(
        ITargetProvider provider,
        DbConnection databaseConnection,
        ServerRuntime server,
        CollectorContext context,
        List<ProcedureStatsCollector.Row> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || !ProcedureStatsCollector.PlanPhaseApplies(context))
        {
            return;
        }

        var planPhaseMs = await RunProcedureStatsPlanPhaseAsync(provider, null, databaseConnection, server, context, rows, cancellationToken);
        if (planPhaseMs <= 0)
        {
            return;
        }

        StampProcedureStatsOffPlanPhase(context, rows, planPhaseMs);
        context.PerItemPlanTargetMs = planPhaseMs;
        context.PerItemPlanIdsAttempted = rows.Count(row => !row.PlanPhaseSkipped);
    }

    /// <summary>#6009: a server's watch list and when it was read, so the databases of one Azure SQL Database collection share one store read.</summary>
    private readonly ConcurrentDictionary<int, (long ReadAtMs, IReadOnlyList<ProcedureStatsCollector.WatchedModule> Modules)> _watchedModules = new();

    /// <summary>#6009: how long a read watch list is reused: longer than one collection's fan-out over databases, far shorter than the collection interval.</summary>
    private const long WatchedModulesReuseMs = 30_000;

    private async Task<IReadOnlyList<ProcedureStatsCollector.WatchedModule>> WatchedModulesAsync(int serverId, CancellationToken cancellationToken)
    {
        var now = Environment.TickCount64;
        if (_watchedModules.TryGetValue(serverId, out var held) && now - held.ReadAtMs < WatchedModulesReuseMs)
        {
            return held.Modules;
        }

        var modules = await ProcedureStatsModuleDates.ReadWatchedAsync(_postgres, serverId, _logger, cancellationToken);
        _watchedModules[serverId] = (now, modules);
        return modules;
    }

    /// <summary>
    /// #6009: procedure_stats' module phase, option C of D16. Reads the watch list from the store (once per collection, unless
    /// <paramref name="watched"/> is given), and for each database that holds a watched module sends ONE small query on the target
    /// connection: <c>sys.objects</c> joined to <c>sys.schemas</c> for those names only. A row of a watched module takes the module's
    /// <c>modify_date</c>; every other row keeps null. With nothing watched, or no watched module among the rows, it sends nothing
    /// at all. Closes the main reader first when it sends (one command at a time on the connection). A failed query is a warning and
    /// the rows ship without a date; a stop or the budget expiring propagates. Returns how many queries it sent.
    /// </summary>
    internal async Task<int> RunProcedureStatsModulePhaseAsync(
        ITargetProvider provider,
        DbDataReader? mainReader,
        DbConnection targetConnection,
        ServerRuntime server,
        CollectorContext context,
        List<ProcedureStatsCollector.Row> rows,
        IReadOnlyList<ProcedureStatsCollector.WatchedModule>? watched,
        CancellationToken cancellationToken)
    {
        watched ??= await WatchedModulesAsync(server.ServerId, cancellationToken);
        var byDatabase = ProcedureStatsCollector.SelectWatchedModules(rows, watched.ToArray());
        if (byDatabase.Count == 0)
        {
            return 0;
        }

        if (mainReader is not null)
        {
            await mainReader.CloseAsync();
        }

        var sent = 0;
        foreach (var (databaseName, modules) in byDatabase)
        {
            sent++;
            try
            {
                var query = ProcedureStatsCollector.BuildModulePhaseQuery(context, databaseName, modules);
                using var command = CreateCollectorCommand(provider, query, targetConnection, CommandTimeoutSeconds);
                using var moduleReader = await command.ExecuteReaderAsync(cancellationToken);
                var results = await ProcedureStatsCollector.ReadModulePhaseAsync(moduleReader, cancellationToken);
                ProcedureStatsCollector.ApplyModulePhase(rows, databaseName, results);
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException
                                       && !cancellationToken.IsCancellationRequested)
            {
                _logger?.LogWarning(
                    ex,
                    "{Collector} on '{Server}': the module query for [{Database}] failed, so this run's rows ship without a module modify_date; it is read again next cycle (#6009).",
                    ProcedureStatsCollector.Instance.Name, server.Config.DisplayName, databaseName);
            }
        }

        return sent;
    }

    /// <summary>
    /// #5449: Off mode renders plans in the plan phase and has no reuse pass, so the run stamps what the phase rendered
    /// (the rows that carry a plan or a size) and how long it took. A phase that failed leaves no row with a plan, so the
    /// stamp reads zero rendered rows and still records the phase's milliseconds.
    /// </summary>
    internal static void StampProcedureStatsOffPlanPhase(
        CollectorContext context, List<ProcedureStatsCollector.Row> rows, long planPhaseMs)
    {
        var renderedRows = 0;
        long renderedBytes = 0;
        foreach (var row in rows)
        {
            if (row.QueryPlanXml is not null || row.QueryPlanXmlBytes is not null)
            {
                renderedRows++;
                renderedBytes += row.QueryPlanXmlBytes ?? 0;
            }
        }

        StampPlanFetch(context, renderedRows, renderedBytes, planPhaseMs);
    }

    /// <summary>The cache a server's procedure_stats runs have filled so far, for a test to confirm and inspect.</summary>
    internal PlanDigestCache<ProcedureStatsPlanKey>? ProcedureStatsPlanCacheForTests(int serverId) =>
        _procedureStatsPlanCaches.TryGetValue(serverId, out var cache) ? cache : null;

    /// <summary>
    /// #5158: procedure_stats' plan reuse, after the main read. In shadow the rows already carry their inline plans (from the #5449 plan phase), and
    /// this only measures whether each module plan's identity (<see cref="ProcedureStatsPlanKey"/>) would have been
    /// recognized and right; nothing written changes. In on the rows carry no plan: recognized identities get their stored
    /// digest, and on a cycle the cadence gate lets render, the others are rendered in one second query on this same
    /// connection, inside the same wall-clock budget, at most <see cref="ProcedureStatsPlanReuse.MaxMissesPerRun"/>.
    ///
    /// <para>What this pass counts goes into the run's collection-log note beside <c>plans_rendered</c>:
    /// <c>deferred_would_hit</c>, <c>deferred_false_hit</c> (the plan's shape changed, or could not be compared; split into <c>deferred_false_hit_shape</c>,
    /// <c>_no_shape</c>, <c>_over_cap</c> and <c>_unparsed</c>, which sum to it), <c>deferred_same_shape</c> (the shape held; in the field
    /// only the memory grant moved) and <c>deferred_miss</c> in shadow, <c>deferred_hit</c> and
    /// <c>deferred_miss</c> in on. A failed second query ships the rows it covered without plans, caches nothing, and does
    /// not fail the run. Returns the keys to confirm after the batch commits, or discard.</para>
    /// </summary>
    internal async Task<List<ProcedureStatsPlanKey>> ApplyProcedureStatsPlanReuseAsync(
        ITargetProvider provider,
        DbConnection targetConnection,
        ServerRuntime server,
        CollectorContext context,
        List<ProcedureStatsCollector.Row> rows,
        ProcedureStatsPlanFetchMode mode,
        CancellationToken cancellationToken,
        long planPhaseMs = 0)
    {
        var cache = _procedureStatsPlanCaches.GetOrAdd(server.ServerId, static _ => new PlanDigestCache<ProcedureStatsPlanKey>());
        if (_procedureStatsPlanCacheModes.TryGetValue(server.ServerId, out var cachedUnderMode) && cachedUnderMode != mode)
        {
            cache.Clear();
        }

        _procedureStatsPlanCacheModes[server.ServerId] = mode;

        var now = DateTime.UtcNow;
        cache.Prune(now - ProcedureStatsPlanCacheMaxAge);

        /* The mode the run was STAMPED with, passed in; never re-resolved from the live knob or capture setting. */
        var shadow = mode == ProcedureStatsPlanFetchMode.Shadow;
        var captureOrdinal = context.CapturePlanXml
            ? _procedureStatsCaptureOrdinals.AddOrUpdate(server.ServerId, 1L, static (_, previous) => previous + 1)
            : _procedureStatsCaptureOrdinals.GetOrAdd(server.ServerId, 0L);

        ProcedureStatsPlanReuse.Outcome outcome;
        /* #5449: the plan phase's own time is part of the run's plan time, in every mode. */
        long fetchMs = planPhaseMs;
        if (shadow)
        {
            outcome = ProcedureStatsPlanReuse.ApplyShadow(
                server.ServerId, cache, rows, captureOrdinal, now,
                (key, cause) => _logger?.LogWarning(
                    "procedure_stats on server {ServerId}: a plan identity the cache recognized rendered a different plan this run (a false hit): {Key}. Why: {Reason} (#5158).",
                    server.ServerId, key, ProcedureStatsPlanReuse.DescribeFalseHit(cause)));
        }
        else
        {
            if (BetweenPlanPhasesForTests is { } hook)
            {
                await hook(cancellationToken);
            }

            var fetchWatch = Stopwatch.StartNew();
            var unjudgedOrdinals = new HashSet<int>();
            outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
                server.ServerId, cache, rows, context.CapturePlanXml, captureOrdinal, now,
                async (handles, token) =>
                {
                    var query = ProcedureStatsCollector.BuildPlanFetchQuery(context, handles);
                    using var command = CreateCollectorCommand(provider, query, targetConnection, CommandTimeoutSeconds);
                    using var planReader = await command.ExecuteReaderAsync(token);
                    return await QueryStatsCollector.ReadPlanFetchAsync(planReader, context.BeginStatementScrub(), token, unjudgedOrdinals);
                },
                cancellationToken,
                unjudgedOrdinals);
            fetchWatch.Stop();
            fetchMs += fetchWatch.ElapsedMilliseconds;

            if (outcome.FetchFailure is { } failure)
            {
                _logger?.LogWarning(
                    failure,
                    "{Collector} on '{Server}': the plan fetch failed, so this run's rows ship without plans for the plans not yet stored; they are fetched again next cycle (#5158).",
                    ProcedureStatsCollector.Instance.Name, server.Config.DisplayName);
            }
        }

        /* The same seam query_stats uses, so plans_rendered, plans_rendered_bytes and plan_fetch_ms come from one stamp.
           Shadow issues no further query after the plan phase, so its fetch time is the plan phase's. */
        StampPlanFetch(context, outcome.Rendered, outcome.RenderedBytes, fetchMs);
        if (shadow)
        {
            context.Measure("deferred_would_hit", outcome.WouldHit);
            context.Measure("deferred_false_hit", outcome.FalseHit);
            context.Measure("deferred_false_hit_shape", outcome.FalseHitShape);
            context.Measure("deferred_false_hit_no_shape", outcome.FalseHitNoShape);
            context.Measure("deferred_false_hit_over_cap", outcome.FalseHitOverCap);
            context.Measure("deferred_false_hit_unparsed", outcome.FalseHitUnparsed);
            context.Measure("deferred_same_shape", outcome.SameShape);
        }
        else
        {
            context.Measure("deferred_hit", outcome.Hit);
            context.Measure("deferred_over_cap", outcome.OverCap);
        }

        context.Measure("deferred_miss", outcome.Miss);

        _logger?.LogInformation(
            "procedure_stats on server {ServerId} ({Mode}, {Cycle} cycle): deferred: {Counters} rendered={Rendered}.",
            server.ServerId,
            shadow ? "shadow" : "on",
            context.CapturePlanXml ? "capture" : "gated",
            shadow
                ? $"would_hit={outcome.WouldHit} false_hit={outcome.FalseHit} (shape={outcome.FalseHitShape} no_shape={outcome.FalseHitNoShape} over_cap={outcome.FalseHitOverCap} unparsed={outcome.FalseHitUnparsed}) same_shape={outcome.SameShape} miss={outcome.Miss}"
                : $"hit={outcome.Hit} miss={outcome.Miss} over_cap={outcome.OverCap}",
            outcome.Rendered);

        return outcome.Pending;
    }

    /// <summary>
    /// The connection for a collector that reads the server as a whole — engine-resolved from the probed
    /// target, never constructed directly.
    /// <para>Extracted so it can be PINNED by test. This is the exact seam that broke: the non-per-database
    /// branch built a <c>SqlConnection</c> literally, so six of the seven PostgreSQL collectors then
    /// shipping got a SQL Server connection and failed in the connection-string parser before running a query. Both engines'
    /// providers were already correct and individually tested — nothing asserted that the RUNNER asked them.
    /// A test that opens nothing and only checks the returned TYPE is enough to catch it, which is why it is
    /// worth having.</para>
    /// </summary>
    internal static DbConnection CreateTargetConnection(ServerRuntime server, ITargetProvider? provider = null)
    {
        ArgumentNullException.ThrowIfNull(server);

        return (provider ?? TargetProviders.For(server.Target)).CreateConnection(server.ConnectionString);
    }

    /// <summary>
    /// The engine-neutral per-database connection: same monitored server, one specific database.
    /// <para>PostgreSQL has no alternative to this. A SQL Server collector can reach another database
    /// without reconnecting (<c>EXECUTE [db].sys.sp_executesql</c>), but a PostgreSQL connection is
    /// bound to one database for its lifetime, so a per-database collector there is necessarily one
    /// connection per database per cycle. That is the cost of reading <c>pg_stat_user_tables</c> and
    /// friends at all, and it is why per-database PostgreSQL collectors get slow cadences.</para>
    /// </summary>
    internal static async Task<DbConnection> OpenDatabaseConnectionAsync(
        ITargetProvider provider, ServerRuntime server, string databaseName, CancellationToken cancellationToken)
    {
        var connection = provider.CreateConnection(
            provider.WithDatabase(server.ConnectionString, databaseName));

        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            /* The caller only disposes what it receives, so a connection that fails to open must be
               disposed HERE or it leaks — once per database per cycle, on exactly the unreachable
               database the per-database loop is designed to skip and keep going past. */
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Lists the databases to fan out over on a PostgreSQL target.
    /// <para>No master-inaccessible fallback and no re-probe throttle, unlike the Azure twin, because
    /// neither has a meaning here: <c>pg_database</c> is a shared catalog readable from the connected
    /// database, so a failure means the login or the server is broken rather than that one catalog is
    /// out of reach. Falling back to the connected database would convert a permissions problem into a
    /// quiet partial collection, which is the failure mode that fallback exists to avoid elsewhere.</para>
    /// </summary>
    internal async Task<List<string>> GetPostgresDatabaseListAsync(ServerRuntime server, IReadOnlyList<string>? databaseScope, CancellationToken cancellationToken)
    {
        var provider = TargetProviders.For(server.Target);
        var (connectionString, query) = provider.BuildDatabaseListPlan(
            server.ConnectionString, server.Config.ExcludedDatabases, databaseScope);

        var databases = new List<string>();

        using var connection = provider.CreateConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        using var command = CreateCollectorCommand(provider, query, connection, CommandTimeoutSeconds);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }

    /* #2220: delegates to the shared rule. Both runners carried their own copy of this predicate, and a
       sweep-scoping rule that disagrees between Lite and Darling is the same class of defect as the one
       #2220 fixes. */
    private static List<string> SingleDbOrEmpty(string? targetDb) =>
        AzureSweepScope.OwnDatabaseOrEmpty(targetDb);

    /// <summary>
    /// Whether master enumeration failed in a way that means database-scoped collectors should fall back
    /// to the connection's own catalog (#857). Deliberately broader than "this login cannot read master":
    /// a 40615 firewall rejection at the logical server says nothing about the login's rights, but the
    /// fallback still works, because Azure evaluates DATABASE-level firewall rules first and a user
    /// database can be reachable while master is not (#1631). The list — and the reason a reachability
    /// error must never be read as a rights verdict (#1506) — is owned by
    /// <see cref="SqlErrorClassification"/>, shared with Lite so the two cannot drift. This bug reached
    /// Darling because the list was duplicated here.
    /// </summary>
    internal static bool ShouldFallBackToSingleDatabaseError(int errorNumber) =>
        SqlErrorClassification.ShouldFallBackToSingleDatabase(errorNumber);

    /* Internal, not private: QueryStoreBackfill (#2022) builds its slice commands through the same
       parameter mapping so the two paths cannot drift on a type.

       Still SqlCommand-typed and still SQL-Server-only, because every caller of THIS overload is:
       Query Store backfill and the Azure per-database/master paths are SQL Server features by
       definition. The engine-neutral path goes through CreateCollectorCommand(ITargetProvider, ...)
       below, and both end up in the same parameter mapping inside SqlServerTargetProvider, so a
       parameter type cannot be mapped two ways. */
    internal static SqlCommand CreateCollectorCommand(CollectorQuery plan, SqlConnection connection, int commandTimeoutSeconds)
        => (SqlCommand)SqlServerTargetProvider.Instance.CreateCommand(plan, connection, commandTimeoutSeconds);

    /// <summary>
    /// The engine-neutral command factory: same collector query, whichever engine the target is.
    /// </summary>
    private static DbCommand CreateCollectorCommand(
        ITargetProvider provider, CollectorQuery plan, DbConnection connection, int commandTimeoutSeconds)
        => provider.CreateCommand(plan, connection, commandTimeoutSeconds);
}
