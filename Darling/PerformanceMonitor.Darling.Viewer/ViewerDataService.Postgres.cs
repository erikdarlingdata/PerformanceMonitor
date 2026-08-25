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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's PostgreSQL reader layer (#2530) — what the six PostgreSQL inner tabs
/// (<see cref="ViewerPostgresTabs"/>) read, and the projections that make stored rows renderable.
///
/// <para><b>The queries are not here, deliberately.</b> Every PostgreSQL read runs the SAME SQL the MCP
/// surface runs, from <c>DarlingPg*Reader</c> in <c>PerformanceMonitor.Darling.Storage</c> — moved there
/// from the service's MCP folder by this change so both front ends can reach them. Copying them into the
/// viewer would have meant a second copy of, among others, a 200-line recursive blocking walk whose revisit
/// guard, root attribution and truncation flag were each a separate review finding; two copies of that
/// diverge, and the copy that diverges is never the one being read. Every other seam in this repo that both
/// SKUs answer follows the same rule (<c>CollectorEngineCapability</c>'s message, <c>DescribeEngineKind</c>,
/// the alert gates), and the reason is always this one.</para>
///
/// <para><b>What IS here:</b> the per-collector health read the Overview tab needs — which has no MCP twin
/// because the MCP answers it per-read rather than per-server — and the display projections. Those exist
/// because the stored rows carry sentinels and units a grid must not render raw: <c>-1</c> means "not
/// measured" rather than "zero", <c>pg_stat_io</c>'s write side is genuinely absent on Aurora rather than
/// zero, and every timestamp in the store is naive UTC that has to pass through
/// <see cref="ViewerTimeHelper.ForDisplay"/> like every other timestamp the viewer shows.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Per-collector collection facts for one server over the window — the Overview tab's grid.
    ///
    /// <para>Scoped by an explicit collector-name array rather than by a <c>LIKE 'pg\_%'</c> pattern: the
    /// caller passes the names <see cref="CollectorCatalog"/> says are PostgreSQL collectors, so the set
    /// tracks the catalog instead of a naming convention a future collector could break. Cast to
    /// <c>text[]</c> explicitly — Npgsql's inference through <c>= ANY($4)</c> is a runtime failure, not a
    /// compile one.</para>
    ///
    /// <para>A collector this engine can never run has NO row here at all, by design: dispatch filters it
    /// out before it runs, so it writes no <c>collection_log</c> entry (a fake SUCCESS/0-rows would be
    /// ~2,880 rows a day per server of noise). That absence is exactly why
    /// <see cref="BuildPostgresCollectorHealth"/> composes this against the catalog rather than rendering it
    /// directly — otherwise the one collector an operator most needs explained is the one row missing.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 collector names.</para>
    /// </summary>
    public const string PostgresCollectorHealthSql = """
        SELECT
            collector_name                                                        AS collector_name,
            MAX(collection_time)                                                  AS last_run_at,
            MAX(collection_time) FILTER (WHERE status = 'SUCCESS')                AS last_success_at,
            CAST(COUNT(*) AS bigint)                                              AS runs,
            CAST(COUNT(*) FILTER (WHERE status <> 'SUCCESS') AS bigint)           AS failed_runs,
            CAST(COALESCE(SUM(rows_collected), 0) AS bigint)                      AS rows_collected,
            (ARRAY_AGG(status ORDER BY collection_time DESC))[1]                  AS last_status,
            /* The newest NON-EMPTY message, not the newest message. A collector that failed an hour ago and
               has succeeded quietly since would otherwise show a blank explanation for a non-zero failure
               count, which reads as the grid having nothing to say about it. */
            (ARRAY_AGG(error_message ORDER BY (error_message IS NULL), collection_time DESC))[1]
                                                                                  AS last_message
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   collector_name = ANY($4::text[])
        GROUP BY collector_name
        """;

    /// <summary>One collector's raw <c>collection_log</c> facts over the window, before the catalog join.</summary>
    public sealed record PostgresCollectorLogFacts(
        string CollectorName,
        DateTime? LastRunAt,
        DateTime? LastSuccessAt,
        long Runs,
        long FailedRuns,
        long RowsCollected,
        string? LastStatus,
        string? LastMessage);

    /// <summary>Runs <see cref="PostgresCollectorHealthSql"/> for the PostgreSQL collectors the catalog ships.</summary>
    public async Task<List<PostgresCollectorLogFacts>> GetPostgresCollectorLogFactsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string> collectorNames,
        CancellationToken cancellationToken = default)
    {
        var facts = new List<PostgresCollectorLogFacts>();
        if (collectorNames.Count == 0)
        {
            return facts;
        }

        await using var command = _dataSource.CreateCommand(PostgresCollectorHealthSql);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        /* Kind-Unspecified at the bind, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then zone-shifts these naive columns to compare them, so
           east of UTC the window slides off the data and the read silently returns nothing. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = collectorNames.ToArray() });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            facts.Add(new PostgresCollectorLogFacts(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7)));
        }

        return facts;
    }

    /// <summary>One row of the Overview tab's grid: a PostgreSQL collector and what it did for this server.</summary>
    public sealed class PostgresCollectorHealthRow
    {
        public string Collector { get; init; } = "";
        public string StoreTable { get; init; } = "";
        public string Status { get; init; } = "";
        public string LastRun { get; init; } = "";
        public long Runs { get; init; }
        public long FailedRuns { get; init; }
        public long RowsCollected { get; init; }
        public string Explanation { get; init; } = "";

        /// <summary>Drives the grid's row highlight: true for a collector this engine can never run, which
        /// is a fact to read rather than a fault to chase.</summary>
        public bool IsPermanentGap { get; init; }

        /// <summary>Drives the grid's row highlight for a collector that IS supposed to run and is not.</summary>
        public bool IsFault { get; init; }
    }

    /// <summary>
    /// Composes the Overview grid from the catalog and the log — pure, so it is unit-tested without a store
    /// or a window.
    ///
    /// <para><b>Catalog-driven, not log-driven, and that is the whole design.</b> Rows come from the nine
    /// PostgreSQL collectors <see cref="CollectorCatalog"/> ships; the log only fills them in. A collector
    /// with no log row is therefore VISIBLE, with the reason: on stock PostgreSQL the two Aurora-only
    /// collectors read <c>aurora_stat_system_waits()</c> and
    /// <c>aurora_stat_statements()</c>, which core PostgreSQL has in no version, and
    /// <see cref="CollectorEngineCapability.NotCollectedMessage"/> says so in the one sentence the MCP
    /// surface and the web dashboard also print. The defect #2530 is about is unexplained emptiness; a
    /// grid that just dropped those two would have reproduced it one layer down.</para>
    ///
    /// <para>The three states are deliberately distinct, because they need three different responses: a
    /// permanent engine gap is nothing to do, a collector that should be running and has no rows in the
    /// window is something to chase, and a collector that ran and failed carries its own error text.</para>
    /// </summary>
    public static List<PostgresCollectorHealthRow> BuildPostgresCollectorHealth(
        DarlingServer server,
        IReadOnlyList<ICollectorSchemaInfo> postgresCollectors,
        IReadOnlyList<PostgresCollectorLogFacts> logFacts)
    {
        ArgumentNullException.ThrowIfNull(server);
        ArgumentNullException.ThrowIfNull(postgresCollectors);
        ArgumentNullException.ThrowIfNull(logFacts);

        var byName = logFacts.ToDictionary(f => f.CollectorName, StringComparer.Ordinal);
        var rows = new List<PostgresCollectorHealthRow>();

        foreach (var definition in postgresCollectors)
        {
            var gap = CollectorEngineCapability.NotCollectedMessage(
                server.ServerName, server.EngineEdition, server.EngineKind, definition.Name);

            byName.TryGetValue(definition.Name, out var facts);

            string status;
            var isFault = false;
            if (gap is not null)
            {
                status = "Not collected on this engine";
            }
            else if (facts is null)
            {
                status = "No runs in this window";
                isFault = true;
            }
            else if (facts.FailedRuns > 0)
            {
                status = string.Equals(facts.LastStatus, "SUCCESS", StringComparison.Ordinal)
                    ? "Recovered"
                    : facts.LastStatus ?? "Failing";
                isFault = !string.Equals(facts.LastStatus, "SUCCESS", StringComparison.Ordinal);
            }
            else
            {
                status = "Collecting";
            }

            rows.Add(new PostgresCollectorHealthRow
            {
                Collector = definition.Name,
                StoreTable = definition.TargetTable,
                Status = status,
                LastRun = facts?.LastRunAt is { } at
                    ? ViewerTimeHelper.ForDisplay(at).ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture)
                    : "",
                Runs = facts?.Runs ?? 0,
                FailedRuns = facts?.FailedRuns ?? 0,
                RowsCollected = facts?.RowsCollected ?? 0,
                /* The engine sentence wins over the last error when both exist: a collector that cannot run
                   here has no error worth showing, and a stale one from before a migration would be the more
                   confusing of the two. */
                Explanation = gap ?? facts?.LastMessage ?? "",
                IsPermanentGap = gap is not null,
                IsFault = isFault,
            });
        }

        return rows;
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // The nine shared reads, as the viewer calls them. Thin on purpose: the SQL, the parameter binding
    // and the ordinal mapping all live in PerformanceMonitor.Darling.Storage, shared byte-for-byte with
    // the MCP surface, so these add a window and nothing else.
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>Vacuum tab, panel 1 — what is holding the xmin horizon back, by cause.</summary>
    public Task<List<DarlingPgXminReader.PgXminRow>> GetPgXminHorizonAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgXminReader.GetPgXminHorizonAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Vacuum tab, panel 2 — per-table autovacuum backlog, ranked by ratio to the table's own threshold.</summary>
    public Task<List<DarlingPgAutovacuumReader.PgAutovacuumRow>> GetPgAutovacuumAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 100, CancellationToken cancellationToken = default) =>
        DarlingPgAutovacuumReader.GetPgAutovacuumAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Vacuum tab, panel 3 — per-database transaction-ID and multixact freeze headroom.</summary>
    public Task<List<DarlingPgWraparoundReader.PgWraparoundRow>> GetPgWraparoundAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgWraparoundReader.GetPgWraparoundAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Waits tab — Aurora's cumulative wait counters, differenced over the window.</summary>
    public Task<List<DarlingPgWaitReader.PgWaitRow>> GetPgWaitStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgWaitReader.GetPgWaitStatsAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>I/O tab — <c>pg_stat_io</c> per backend type / object / context, differenced over the window.</summary>
    public Task<List<DarlingPgIoReader.PgIoRow>> GetPgIoAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 100, CancellationToken cancellationToken = default) =>
        DarlingPgIoReader.GetPgIoAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Replication tab — slot WAL retention and the xmin each slot pins.</summary>
    public Task<List<DarlingPgSlotReader.PgSlotRow>> GetPgSlotsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgSlotReader.GetPgSlotsAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Activity tab, panel 1 — the sampling DENOMINATOR: how many captures ran, and how many saw blocking.</summary>
    public Task<DarlingPgBlockingReader.PgBlockingCaptureCounts> GetPgBlockingCaptureCountsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgBlockingReader.GetPgBlockingCaptureCountsAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Activity tab, panel 2 — blocking chains by root.</summary>
    public Task<List<DarlingPgBlockingReader.PgBlockingChainRow>> GetPgBlockingChainsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgBlockingReader.GetPgBlockingChainsAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Activity tab, panel 3 — lock cycles, which have no root and so appear on no chain.</summary>
    public Task<List<DarlingPgBlockingReader.PgBlockingCycleRow>> GetPgBlockingCyclesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgBlockingReader.GetPgBlockingCyclesAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Activity tab, panel 4 — top statement shapes by total execution time.</summary>
    public Task<List<DarlingPgStatementReader.PgStatementRow>> GetPgTopQueriesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgStatementReader.GetPgTopQueriesAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Activity tab, panel 5 — per-database counters: temp-file spills, cache hits, deadlocks, commit split.</summary>
    public Task<List<DarlingPgDatabaseReader.PgDatabaseRow>> GetPgDatabaseStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgDatabaseReader.GetPgDatabaseStatsAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Storage tab, panel 1 - the per-table bloat ESTIMATE with its measured sizes and the trust
    /// signals that decide whether the estimate may be rendered at all.</summary>
    public Task<List<DarlingPgTableBloatReader.PgTableBloatRow>> GetPgTableBloatAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgTableBloatReader.GetPgTableBloatAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Storage tab, panel 2 - per-index scan counts with the catalog facts that decide whether an
    /// unscanned index can actually go.</summary>
    public Task<List<DarlingPgIndexUsageReader.PgIndexUsageRow>> GetPgIndexUsageAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgIndexUsageReader.GetPgIndexUsageAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Vacuum tab, panel 4 - the sessions holding a transaction open, and which of them actually
    /// pins the xmin horizon the panels above it measure.</summary>
    public Task<List<DarlingPgSessionStatesReader.PgSessionStateRow>> GetPgSessionStatesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgSessionStatesReader.GetPgSessionStatesAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Vacuum tab, panel 5 - whether this target can capture execution plans at all, and if not,
    /// which step is missing. Latest state per facet rather than the history: every facet is a
    /// parameter-group setting, so the window holds the same rows repeated.</summary>
    public Task<List<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow>> GetPgPlanCaptureReadinessAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgPlanCaptureReadinessReader.GetPgPlanCaptureReadinessAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Waits tab, write-side panel - checkpoints, background writer and WAL over the window (#2544).
    /// Returns a SINGLE row or null: the three source views are cluster-wide singletons, and what is reported
    /// is the CHANGE across the window rather than the cumulative levels, so there is one answer rather than a
    /// series. Null means fewer than two samples, which is a real state on a freshly added server and is not
    /// the same as a quiet one.</summary>
    public Task<DarlingPgWriteStatsReader.PgWriteStatsRow?> GetPgWriteStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgWriteStatsReader.GetPgWriteStatsAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Overview tab - which extensions this target has, could have, or cannot have (#2545). Latest
    /// state per extension rather than the history: installing one is a rare deliberate act, so the window
    /// holds the same answer repeated daily. Monitoring-relevant extensions sort first, and within them the
    /// ACTIONABLE state (available but not installed) sorts above the rest.</summary>
    public Task<List<DarlingPgExtensionAvailabilityReader.PgExtensionRow>> GetPgExtensionAvailabilityAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 200, CancellationToken cancellationToken = default) =>
        DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Activity tab - lock state by mode, type and relation over the window (#2544). Every row
    /// carries the capture denominator, because these are SAMPLES: three ungranted rows means something
    /// different in 60 captures than in 4. Ungranted sorts first, then by the worst wait anyone served.</summary>
    public Task<List<DarlingPgLockStatsReader.PgLockStatRow>> GetPgLockStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgLockStatsReader.GetPgLockStatsAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Storage tab - per-column planner statistics (#2543), latest per column and ranked by
    /// suspicion rather than alphabetically: heavy skew first (the parameter-sensitivity signal), then low
    /// correlation (why an index scan was rejected). Zero rows has TWO causes - no qualifying table, or a
    /// monitoring role without SELECT, since pg_stats filters on has_column_privilege - and the caller must
    /// not report either as healthy statistics.</summary>
    public Task<List<DarlingPgColumnStatsReader.PgColumnStatRow>> GetPgColumnStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 100, CancellationToken cancellationToken = default) =>
        DarlingPgColumnStatsReader.GetPgColumnStatsAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Replication tab - connected standbys and how far behind each got (#2544). Returns the latest
    /// sample AND the window's worst, because a replica that drifts hundreds of MB behind and recovers reads
    /// as healthy in any single sample - and it is the one most likely to be useless when somebody needs to
    /// fail over to it. Ranked by worst REPLAY bytes, never by the time lag, which understates a stall.</summary>
    public Task<List<DarlingPgReplicationStatsReader.PgReplicationStatRow>> GetPgReplicationStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgReplicationStatsReader.GetPgReplicationStatsAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>I/O tab - what is resident in shared buffers (#2544). Latest snapshot only, because
    /// residency is a level rather than a counter and averaging it across a day answers nothing. A NULL
    /// relation name means another database's relation or a shared catalog, not a missing name.</summary>
    public Task<List<DarlingPgBufferUsageReader.PgBufferUsageRow>> GetPgBufferUsageAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgBufferUsageReader.GetPgBufferUsageAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>Storage tab - MEASURED b-tree index bloat (#2561), latest per index. Ranked by estimated
    /// reclaimable BYTES rather than by density, because a small index at 40% density is worth nothing next
    /// to a large one at 70%. Indexes too large to measure sort to the TOP: unknown is not zero, and they
    /// are the likeliest big win.</summary>
    public Task<List<DarlingPgIndexBloatReader.PgIndexBloatRow>> GetPgIndexBloatAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int limit = 50, CancellationToken cancellationToken = default) =>
        DarlingPgIndexBloatReader.GetPgIndexBloatAsync(_dataSource, serverId, startUtc, endUtc, limit, cancellationToken);
}
