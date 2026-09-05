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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #2022 — Query Store phase 2 (of #1960): the newest-first backfill worker for the history the
/// live path never takes. Phase 1 made the LIVE path hole-free, but two bounded windows still
/// discard history by design: first contact takes only the trailing 60 minutes of a ~30-day
/// catalog, and post-outage catch-up is clamped to <see cref="WatermarkPolicy.MaxCatchup"/> (the
/// #1556 incident fix, tightened by #2102) as a bounded, logged hole. One mechanism fills
/// both, and every slice of it windows at most <see cref="QueryStoreBackfillState.MaxSliceSpan"/>
/// at a time (#2102 — the query's cost grows with window width, so an unchunked wide range on a
/// big database re-times-out forever instead of draining):
///
/// <para><b>The tail (first contact).</b> The backfill ceiling is DERIVED, exactly like the live
/// watermark: MIN(last_execution_time) over the rows already stored for a database. Everything at
/// or above that boundary shipped complete (both of phase 1's bounded cuts — TOP ... WITH TIES and
/// the byte budget — finish the boundary tie group), so each slice ships the newest missing chunk
/// strictly BELOW it, and the write itself advances the boundary downward. A pre-existing store
/// whose history already reaches the horizon marks itself done on the first look without shipping
/// a row. The live watermark — MAX() — cannot see backfilled (older) rows by construction, which
/// is the #1960 design constraint: the two paths can never race for the same boundary.</para>
///
/// <para><b>Clamp holes (post-outage).</b> An interior gap is invisible to MIN/MAX, so the runner
/// records it at the moment the catch-up clamp fires — (raw watermark, clamped floor), merged wider on
/// a repeat clamp — under this worker's own <c>collector_state</c> rows
/// (<see cref="StateCollectorName"/>; deliberately NOT the definition's StateKeys machinery, so
/// query_store itself still declares none). The worker services the hole newest-first and shrinks
/// its ceiling as slices land, deleting the record when the hole is filled, empty, or expired.</para>
///
/// <para><b>The horizon.</b> Slices carry a BACKDATED collection_time (the slice ceiling), so the
/// rows land in time buckets adjacent to their own activity — readers window on collection_time,
/// hourly CAGGs bucket on it, and retention drops on it. That is exactly why this stage refuses to
/// dig below <see cref="Horizon"/> (derived from the refresh window and the raw tier, not
/// hand-maintained — the #1937 rule): inside it, the hourly CAGGs' refresh window re-materializes
/// the touched buckets on their next scheduled run AND raw retention cannot immediately drop what
/// was just shipped; below it, at least one of those stops holding, and that deeper stage is a
/// separate decision (#2022's own staging note). Re-shipped interval rows are already deduped by
/// every reader (#1907/#1841), so a boundary overlap is waste at worst, never a double-count.</para>
///
/// <para><b>Pacing.</b> One slice per server per tick on the worker's OWN loop (the command-loop
/// precedent), each slice bounded by the same per-database byte budget as the live path, on its own
/// SQL and store connections, never touching the sweep gate — backfill can be slow forever without
/// delaying collection. Azure SQL DB targets ride the same state model on per-database connections
/// (#2058 — the window travels as command parameters, since Azure rejects the sp_executesql
/// nesting); Lite remains deferred scope with its own horizon decision (30-day raw + parquet, no
/// CAGG/retention tiers), tracked on #2058.</para>
/// </summary>
public sealed class QueryStoreBackfill
{
    /// <summary>The stored identity/codec is SHARED with Lite's worker (#2058) — see
    /// <see cref="QueryStoreBackfillState"/>; only the horizon and the host plumbing are per-SKU.</summary>
    public const string StateCollectorName = QueryStoreBackfillState.StateCollectorName;

    /// <summary>
    /// How deep the hourly refresh policy can still reach on its NEXT run — its start offset minus one
    /// schedule interval, because the window slides forward by exactly that much between runs. A row landed at
    /// the start-offset boundary itself is already outside the window by the time the policy next fires.
    /// </summary>
    private static readonly TimeSpan RefreshReachableDepth =
        TimescaleSupport.HourlyRefreshStartSpan - TimescaleSupport.HourlyRefreshScheduleSpan;

    /// <summary>
    /// How far below now a backfill slice may reach — the SMALLER of the two conditions that both have to
    /// hold, derived from each rather than hand-maintained.
    ///
    /// <para><b>Both, not either.</b> A slice lands rows at a BACKDATED <c>collection_time</c>, so for the
    /// slice to be worth shipping two things must be true of the depth it lands at: raw retention must not
    /// immediately drop it (<see cref="RetentionTierRouter.RawMaxAge"/>, raw retention minus the route
    /// margin), and the hourly continuous aggregates must still re-materialize the buckets it touched
    /// (<see cref="RefreshReachableDepth"/>). The rollups are materialized-only — the watermark is a hard
    /// partition, not a fallback — so a bucket the refresh window no longer covers keeps whatever it was
    /// materialized with, and the backfilled rows are invisible to every window that routes at hourly grain
    /// while still being visible at raw grain. That is a silent split between two tiers, not a delay.</para>
    ///
    /// <para><b>Why this is a <c>min</c> now and was a single term before (#3012).</b> The two conditions
    /// happened to coincide: the hourly refresh window was 3 days and <c>RawMaxAge</c> is 3 days, so taking
    /// only the retention term looked complete. It was not — it was the retention term acting as a PROXY for
    /// the refresh term, which is exactly the coupling that made the refresh expensive. The hourly window is
    /// now chosen against its own cadence (<see cref="TimescaleSupport.HourlyRefreshStartOffset"/>), so the
    /// two terms differ and the smaller one is the real horizon. The cost is stated rather than hidden: this
    /// horizon narrows from 3 days to 23 hours, so post-outage catch-up and first-contact tail recovery reach
    /// less far back per store. Digging deeper than the refresh window is a separate stage that would have to
    /// refresh the buckets it wrote — which is a decision about running
    /// <c>refresh_continuous_aggregate</c> off the backfill loop, not a constant.</para>
    /// </summary>
    public static readonly TimeSpan Horizon =
        RetentionTierRouter.RawMaxAge <= RefreshReachableDepth
            ? RetentionTierRouter.RawMaxAge
            : RefreshReachableDepth;

    /// <summary>Candidate databases come from rows this window fresh — a database that stopped
    /// shipping Query Store rows entirely ages out of the backfill scan with them.</summary>
    private static readonly TimeSpan CandidateWindow = TimeSpan.FromDays(7);

    private readonly NpgsqlDataSource _postgres;
    private readonly DarlingCollectorRunner _runner;
    private readonly CollectorDeltaCalculator _deltas;
    private readonly ILogger? _logger;
    private readonly Func<bool> _capturePlans;

    /* #2164: the per-database text budget override in MB, read live like _capturePlans. Backfill slices
       carry the SAME nvarchar(max) query-text/plan-XML payload over the same link as a live tick, so the
       operator knob has to reach here too — a knob that only bounds the tick would leave the heavier of
       the two paths at the compile-time 64 MB, which is precisely the drain the knob exists to shorten. */
    private readonly Func<int> _textBudgetMb;

    public QueryStoreBackfill(
        NpgsqlDataSource postgres,
        DarlingCollectorRunner runner,
        CollectorDeltaCalculator deltas,
        ILogger? logger,
        Func<bool>? capturePlans = null,
        Func<int>? textBudgetMb = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _runner = runner ?? throw new ArgumentNullException(nameof(runner));
        _deltas = deltas ?? throw new ArgumentNullException(nameof(deltas));
        _logger = logger;
        _capturePlans = capturePlans ?? (() => true);
        /* Null provider = keep the collector's compile-time budget (tests and any non-Darling host). */
        _textBudgetMb = textBudgetMb ?? (() => 0);
    }

    /// <summary>
    /// Runs AT MOST one backfill slice for one server: the first database found with a pending
    /// hole or an undrained first-contact tail gets one byte-budgeted slice; everything else waits
    /// for a later tick. Returns true when a slice (or an exhaustion probe) ran, false when the
    /// server had no backfill work — the common steady state, costing one candidate query and a
    /// few MIN() lookups.
    /// </summary>
    public async Task<bool> RunServerSliceAsync(ServerRuntime server, CancellationToken cancellationToken)
    {
        /* The COMPOSED gate, not the definition's own AppliesTo. Query Store is a SQL Server feature and this
           method opens SqlConnections, but the raw override never checks the engine — it reads
           SqlMajorVersion, and CollectorTargetInfo treats 0 as "assume newest" so a PostgreSQL target (which
           has no SqlMajorVersion at all) sails straight through. Latent today only because the caller happens
           to be reached from a SQL-Server-shaped path; one new call site and it becomes B1 again. */
        if (!CollectorCatalog.AppliesTo(QueryStoreCollector.Instance, server.Target))
        {
            return false;
        }

        /* #2111 yield-to-live: a backfill slice scans the same QS internal tables the live sweep
           reads, on a replica that is often MAXDOP-1 — when the live path is failing on this
           server, running a slice anyway is the contention that keeps it failing. Skip the server
           this tick (false = the tick is free for another server); the hole waits, live recovers,
           backfill resumes. Debug, not Warning: the live failure already logs loudly every cycle,
           and this is the designed response to it. */
        if (QueryStoreBackfillState.ShouldYieldToLive(
            _runner.LastQueryStoreItemFailureUtc(server.ServerId), DateTime.UtcNow))
        {
            _logger?.LogDebug(
                "query_store backfill on '{Server}': yielding to the live path (recent live query_store failure)",
                server.Config.DisplayName);
            return false;
        }

        var state = await _runner.GetCollectorStateAsync(server.ServerId, StateCollectorName, cancellationToken);
        var databases = await GetCandidateDatabasesAsync(server.ServerId, cancellationToken);

        var nowUtc = DateTime.UtcNow;
        var floorLimit = nowUtc - Horizon;

        foreach (var databaseName in databases)
        {
            cancellationToken.ThrowIfCancellationRequested();

            /* Holes before the tail: a recorded outage gap is the history closest to expiring. */
            if (state.TryGetValue(QueryStoreBackfillState.HoleKeyPrefix + databaseName, out var encoded)
                && QueryStoreBackfillState.TryDecodeHole(encoded, out var holeFrom, out var holeTo))
            {
                if (holeTo <= floorLimit)
                {
                    /* The whole hole sank below the horizon before it was serviced — expired, and
                       deliberately NOT dug after: the staging rule above. */
                    await _runner.DeleteCollectorStateKeyAsync(server.ServerId, StateCollectorName, QueryStoreBackfillState.HoleKeyPrefix + databaseName, cancellationToken);
                    continue;
                }

                var holeFloor = holeFrom > floorLimit ? holeFrom : floorLimit;
                await RunCountedSliceAsync(server, databaseName, holeFloor, holeTo, isHole: true, cancellationToken);
                return true;
            }

            if (state.ContainsKey(QueryStoreBackfillState.DoneKeyPrefix + databaseName))
            {
                continue;
            }

            /* The derived ceiling: everything at or above the stored MIN shipped complete. Null
               means the live path has not made first contact for this database yet — its 60-minute
               first window establishes the ceiling this worker digs below. */
            var storedFloor = await GetStoredFloorAsync(server.ServerId, databaseName, cancellationToken);
            if (storedFloor is null)
            {
                continue;
            }

            if (storedFloor <= floorLimit)
            {
                /* History already reaches the horizon — the pre-existing-store case, marked done
                   without shipping a row so the steady state never re-probes it. */
                await SaveStateAsync(server.ServerId, QueryStoreBackfillState.DoneKeyPrefix + databaseName, nowUtc.ToString("o", CultureInfo.InvariantCulture), cancellationToken);
                continue;
            }

            await RunCountedSliceAsync(server, databaseName, floorLimit, storedFloor.Value, isHole: false, cancellationToken);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Consecutive failed slices per server — the adaptive-shrink signal's backfill half (#2111
    /// promoted): a server whose hour-wide slices keep dying at the command timeout digs in
    /// progressively narrower chunks (<see cref="QueryStoreBackfillState.AdaptiveSpan"/>) until one
    /// fits. Reset by any completed slice; in-memory on purpose, like the live counters — a restart
    /// forgetting it costs one full-width slice. Concurrent for symmetry with the Lite twin — the
    /// worker is single-threaded today, but nothing pins that.
    /// </summary>
    private readonly ConcurrentDictionary<int, int> _consecutiveSliceFailures = new();

    /// <summary>Runs one slice with the failure accounting wrapped around it — the worker's outer
    /// catch still logs the throw exactly as before.</summary>
    private async Task RunCountedSliceAsync(
        ServerRuntime server, string databaseName, DateTime floorUtc, DateTime ceilingUtc, bool isHole, CancellationToken cancellationToken)
    {
        try
        {
            await RunSliceAsync(server, databaseName, floorUtc, ceilingUtc, isHole, cancellationToken);
            _consecutiveSliceFailures.TryRemove(server.ServerId, out _);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _consecutiveSliceFailures.AddOrUpdate(server.ServerId, 1, static (_, count) => count + 1);
            throw;
        }
    }

    /// <summary>
    /// One byte-budgeted, newest-first slice for one database: probe PRODUCTVERSION (the same
    /// version gates as the live path, so the reader ordinals cannot differ), run the backfill
    /// window query, read through the SAME budget machinery, COPY through the SAME writer with the
    /// slice ceiling as the backdated collection_time, then advance the boundary — derived for the
    /// tail, shrunk-and-saved for a hole. An empty slice means Query Store retains nothing in the
    /// window: the tail marks done, the hole record deletes.
    /// </summary>
    private async Task RunSliceAsync(
        ServerRuntime server, string databaseName, DateTime floorUtc, DateTime ceilingUtc, bool isHole, CancellationToken cancellationToken)
    {
        /* #2102: one slice queries at most the top MaxSliceSpan of the remaining range. The byte
           budget bounds what SHIPS, not what the query aggregates and sorts — an unchunked wide
           window on a big database times out at the command timeout every tick and the range never
           drains, the same row-cap-is-not-a-cost-cap flaw that wedged the live path. */
        /* #2111 adaptive shrink: after consecutive failed slices this server digs in narrower
           chunks until one fits its command timeout; a completed slice resets to full width. */
        var sliceSpan = QueryStoreBackfillState.AdaptiveSpan(
            QueryStoreBackfillState.MaxSliceSpan,
            _consecutiveSliceFailures.TryGetValue(server.ServerId, out var recentFailures) ? recentFailures : 0);
        var sliceFloor = QueryStoreBackfillState.BoundSliceFloor(floorUtc, ceilingUtc, sliceSpan);

        var definition = QueryStoreCollector.Instance;
        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = DateTime.UtcNow,
            Deltas = _deltas,
            Target = server.Target,
            ExcludedDatabases = server.Config.ExcludedDatabases?.ToArray() ?? Array.Empty<string>(),
            CapturePlanXml = _capturePlans(),
            /* #2164: 0 from the default provider means "no override" — the collector keeps its constant. */
            TextByteBudgetOverride = _textBudgetMb() > 0 ? _textBudgetMb() * 1024 * 1024 : null,
        };

        var timeout = definition.CommandTimeoutSecondsOverride ?? DarlingCollectorRunner.CommandTimeoutSeconds;
        var rows = new List<QueryStoreCollector.Row>();

        if (server.Target.IsAzureSqlDb)
        {
            /* Azure arm (#2058): the window travels as command parameters on a per-database
               connection — Azure SQL DB rejects the [db].sys.sp_executesql nesting (#1836). The
               version gates are forced on by the target flags, so no PRODUCTVERSION probe is
               needed; CurrentDatabaseName feeds ReadAsync's database attribution exactly as on
               the live Azure path. */
            context.CurrentDatabaseName = databaseName;
            var azurePlan = definition.BuildBackfillQuery(context, sliceFloor, ceilingUtc);
            using var dbConnection = await _runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
            using var dbCommand = DarlingCollectorRunner.CreateCollectorCommand(azurePlan, dbConnection, timeout);
            using var dbReader = await dbCommand.ExecuteReaderAsync(cancellationToken);
            rows = await definition.ReadAsync(dbReader, context, cancellationToken);
        }
        else
        {
            using var sqlConnection = new SqlConnection(server.ConnectionString);
            await sqlConnection.OpenAsync(cancellationToken);

            /* Same best-effort 10-second PRODUCTVERSION probe as the live enumeration path — the
               version gates shape the SELECT, and the fallback default is the conservative one. */
            var probePlan = definition.BuildEnumerationProbe(context);
            if (probePlan is not null)
            {
                try
                {
                    using var probeCommand = DarlingCollectorRunner.CreateCollectorCommand(probePlan, sqlConnection, 10);
                    var probeResult = await probeCommand.ExecuteScalarAsync(cancellationToken);
                    if (probeResult is not null && probeResult != DBNull.Value)
                    {
                        context.EnumerationProbeResult = probeResult;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger?.LogDebug("Backfill version probe on '{Server}' failed; using defaults: {Error}",
                        server.Config.DisplayName, ex.Message);
                }
            }

            var plan = definition.BuildBackfillPerItemQuery(databaseName, context, sliceFloor, ceilingUtc);
            using var command = DarlingCollectorRunner.CreateCollectorCommand(plan, sqlConnection, timeout);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await definition.ReadItemAsync(databaseName, reader, rows, context, cancellationToken);
        }

        if (rows.Count == 0)
        {
            if (sliceFloor > floorUtc)
            {
                /* Only this CHUNK is quiet — the range below it is unexplored, so this is an
                   advance, not a terminal verdict (#2102). The persisted hole ceiling shrinks past
                   the quiet chunk; a derived-boundary tail converts its remainder to a hole record,
                   because MIN over stored rows cannot walk through quiet space (an empty chunk
                   ships nothing, so the derived ceiling would re-ask the same chunk forever). The
                   tail marks done in the same breath — the hole owns the rest of the dig, and the
                   scan services holes first. */
                await SaveStateAsync(server.ServerId, QueryStoreBackfillState.HoleKeyPrefix + databaseName, QueryStoreBackfillState.EncodeHole(floorUtc, sliceFloor), cancellationToken);
                if (!isHole)
                {
                    await SaveStateAsync(server.ServerId, QueryStoreBackfillState.DoneKeyPrefix + databaseName, DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture), cancellationToken);
                }

                _logger?.LogInformation(
                    "query_store backfill on '{Server}' [{Database}]: quiet chunk {Floor:o}..{Ceiling:o}, continuing below ({Range}).",
                    server.Config.DisplayName, databaseName, sliceFloor, ceilingUtc, isHole ? "hole" : "tail");
                return;
            }

            /* Query Store retains nothing inside the window — the monitored catalog is shorter
               than the horizon (or the hole's span was never persisted at the source). Terminal
               for this range, and cheaper to record than to re-ask every tick. */
            if (isHole)
            {
                await _runner.DeleteCollectorStateKeyAsync(server.ServerId, StateCollectorName, QueryStoreBackfillState.HoleKeyPrefix + databaseName, cancellationToken);
            }
            else
            {
                await SaveStateAsync(server.ServerId, QueryStoreBackfillState.DoneKeyPrefix + databaseName, DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture), cancellationToken);
            }

            _logger?.LogInformation(
                "query_store backfill on '{Server}' [{Database}]: nothing retained below {Ceiling:o} — {Range} complete.",
                server.Config.DisplayName, databaseName, ceilingUtc, isHole ? "hole" : "tail");
            return;
        }

        /* Backdated so the rows land beside their own activity — see the class doc's horizon
           contract. The ceiling, not each row's interval, keeps the write one batch. */
        var written = await _runner.WriteBackfillBatchAsync(definition, rows, server, ceilingUtc, context, cancellationToken);

        var boundary = context.PerItemShippedBoundary;
        if (isHole)
        {
            /* A chunked slice's rows all sit at or above its own chunk floor, so a missing shipped
               boundary falls back to the chunk floor rather than deleting (#2102) — deletion under
               a bounded window would orphan the unexplored range below it. */
            var shippedTo = boundary ?? sliceFloor;
            if (shippedTo <= floorUtc)
            {
                await _runner.DeleteCollectorStateKeyAsync(server.ServerId, StateCollectorName, QueryStoreBackfillState.HoleKeyPrefix + databaseName, cancellationToken);
            }
            else
            {
                /* Shrink the ceiling to the oldest shipped row; the from-side stays at the floor we
                   actually used (anything below it is horizon-expired either way). */
                await SaveStateAsync(server.ServerId, QueryStoreBackfillState.HoleKeyPrefix + databaseName, QueryStoreBackfillState.EncodeHole(floorUtc, shippedTo), cancellationToken);
            }
        }
        else if (boundary is not null && boundary <= floorUtc)
        {
            await SaveStateAsync(server.ServerId, QueryStoreBackfillState.DoneKeyPrefix + databaseName, DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture), cancellationToken);
        }

        _logger?.LogInformation(
            "query_store backfill on '{Server}' [{Database}]: shipped {Rows} rows ({ShippedMB:F1}MB) down to {Boundary:o} ({Range}, ceiling {Ceiling:o}).",
            server.Config.DisplayName, databaseName, written,
            context.PerItemTextBytesShipped / (1024.0 * 1024.0),
            boundary ?? floorUtc, isHole ? "hole" : "tail", ceilingUtc);
    }

    private Task SaveStateAsync(int serverId, string key, string value, CancellationToken cancellationToken)
        => _runner.SaveCollectorStateAsync(
            serverId, StateCollectorName, new Dictionary<string, string>(StringComparer.Ordinal) { [key] = value }, cancellationToken);

    /// <summary>Databases that shipped query_store rows recently — the backfill universe. The
    /// store already knows them, so no live enumeration (and no probing of QS-ineligible
    /// databases) is needed.</summary>
    private async Task<List<string>> GetCandidateDatabasesAsync(int serverId, CancellationToken cancellationToken)
    {
        var databases = new List<string>();
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT DISTINCT database_name FROM query_store_stats WHERE server_id = $1 AND collection_time > $2 ORDER BY database_name", connection);
            /* #2874: the enclosing BackfillSliceDeadline ABANDONS rather than cancels, so this is the only
               bound that reaches the statement. CandidateWindow is wider than raw retention, which makes the
               collection_time predicate inert — this read is unbounded across every chunk that exists. */
            command.CommandTimeout = ServiceCommandDeadlines.QueryStoreBackfillReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow - CandidateWindow, DateTimeKind.Unspecified));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(0))
                {
                    databases.Add(reader.GetString(0));
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug(ex, "query_store backfill candidate read failed; skipping this tick");
        }

        return databases;
    }

    /// <summary>MIN(last_execution_time) stored for one database — the derived backfill ceiling,
    /// the mirror of the runner's MAX() watermark reads. Null (no rows / failure) skips the
    /// database this tick; failure never invents a boundary.</summary>
    private async Task<DateTime?> GetStoredFloorAsync(int serverId, string databaseName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT MIN(last_execution_time) FROM query_store_stats WHERE server_id = $1 AND database_name = $2", connection);
            /* #2874: unbounded by construction — the derived ceiling IS the oldest stored row, so the
               collection_time floor #2344 and #2795 gave the MAX siblings would hide what this looks for.
               The deadline is the whole bound, and the enclosing budget abandons rather than cancels. */
            command.CommandTimeout = ServiceCommandDeadlines.QueryStoreBackfillReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(databaseName);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
            {
                return dt;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug(ex, "query_store backfill floor read failed for [{Database}]; skipping this tick", databaseName);
        }

        return null;
    }
}
