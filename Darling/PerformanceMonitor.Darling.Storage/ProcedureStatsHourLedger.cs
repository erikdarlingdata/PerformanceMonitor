/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly row-count ledger of <c>collect.procedure_stats</c> (#9357): the same ledger <see cref="QueryStatsHourLedger"/>
/// keeps for <c>collect.query_stats</c> (#4605), for the same reason. The hourly-plus-raw-edges read of a procedure cell may
/// take its middle hours from <c>collect.procedure_stats_interval_hourly</c> only when that rollup holds every raw row those
/// hours have, and <see cref="IntervalRollupCountGuard"/> proves it by comparing, per (server, hour), this ledger with the
/// rollup's <c>sum(sample_count)</c> instead of counting raw. The collector adds each batch's count in the same transaction
/// as its COPY (<see cref="QueryStatsHourLedgerWriter.AddProcedureStatsBatchAsync"/>), so under one read snapshot the ledger
/// counts every row raw holds from this build's writer.
///
/// <para><b>What is counted.</b> Exactly the population the rollup counts
/// (<see cref="TimescaleSupport.CreateProcedureStatsIntervalHourlySql"/>): rows whose <c>sample_interval_seconds</c> is not 0.
/// A restart row (0) is not counted and a row with a NULL interval is, as in the rollup's own
/// <c>WHERE sample_interval_seconds IS DISTINCT FROM 0</c>. An hour with no counted rows has no ledger row.</para>
///
/// <para><b>Where counting starts.</b> The ledger holds nothing for rows written before the store reached V300. The one-row
/// state table's <c>counted_since</c> is the first whole hour from which every row is counted: the rung sets it to the next
/// whole hour after it ran, and a reader treats a window that starts before it as uncounted and stays on raw. An existing store
/// therefore takes the route only once the ledger covers the whole window (a 24 hour window, a day after the upgrade).</para>
///
/// <para><b>Plain tables, no grants.</b> Both tables are plain, not hypertables, and not collectors, like the query_stats pair.
/// They sit in the <c>collect</c> schema, so the blanket SELECT and the owner's default privileges that provisioning
/// re-asserts at every start cover them. The collector writes as the owner. The key leads with <c>bucket</c>, so the guard's
/// range of hours and the retention prune are both index range scans.</para>
/// </summary>
public static class ProcedureStatsHourLedger
{
    /// <summary>The ledger table: one row per (hour, server) with the count of non-restart <c>collect.procedure_stats</c> rows written into that hour.</summary>
    public const string LedgerTable = "collect.procedure_stats_hour_ledger";

    /// <summary>The one-row state table: <c>counted_since</c> is the first whole hour from which every row is in the ledger.</summary>
    public const string StateTable = "collect.procedure_stats_hour_ledger_state";

    /// <summary>
    /// The store schema version of the rung that creates both tables. A store below it has neither table, so the runner
    /// checks the store's schema version against this before it names them, and a store below the rung reads raw with no
    /// fault to log. <c>ProcedureStatsHourLedgerTests</c> pins it to the version the rung is registered under.
    /// </summary>
    public const int RungVersion = 300;

    /// <summary>
    /// Both tables and the state row, as the V300 rung runs them: the same shape as <see cref="QueryStatsHourLedger.CreateSql"/>.
    /// Idempotent. The rung embeds this text, so editing it edits a shipped rung: add a new rung instead.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.procedure_stats_hour_ledger
(
    server_id integer NOT NULL,
    server_name text NOT NULL,
    bucket timestamp NOT NULL,
    n bigint NOT NULL,
    CONSTRAINT pk_procedure_stats_hour_ledger PRIMARY KEY (bucket, server_id, server_name)
);

CREATE TABLE IF NOT EXISTS collect.procedure_stats_hour_ledger_state
(
    id integer NOT NULL,
    counted_since timestamp NOT NULL,
    CONSTRAINT pk_procedure_stats_hour_ledger_state PRIMARY KEY (id),
    CONSTRAINT ck_procedure_stats_hour_ledger_state_single_row CHECK (id = 1)
);

INSERT INTO collect.procedure_stats_hour_ledger_state (id, counted_since)
VALUES (1, date_trunc('hour', now() AT TIME ZONE 'UTC') + interval '1 hour')
ON CONFLICT (id) DO NOTHING;";

    /// <summary>
    /// Adds one batch's count to its hour, as <see cref="QueryStatsHourLedger.UpsertSql"/> does for query_stats: <c>$1</c> is
    /// the server id, <c>$2</c> the storage name, <c>$3</c> the batch's <c>collection_time</c> and <c>$4</c> the count of rows
    /// whose interval is not 0. A count of 0 or less writes nothing. Run it on the COPY's own transaction and after the COPY.
    /// </summary>
    public static readonly string UpsertSql = HourLedgerStatements.Upsert(LedgerTable);

    /// <summary>
    /// Recomputes the ledger from raw for a range of hours, as <see cref="QueryStatsHourLedger.RecountSql"/> does for
    /// query_stats: <c>$1</c> the start and <c>$2</c> the end (exclusive), widened to whole hours; a (server, hour) with
    /// raw rows is set to its count and a ledger row with none is deleted. It does not touch the state row. Tests use it to
    /// seed the ledger from raw rows they insert.
    /// </summary>
    public static readonly string RecountSql = HourLedgerStatements.Recount(LedgerTable, "collect.procedure_stats");

    /// <summary>
    /// The retention prune: deletes every ledger bucket below <c>$1</c> (naive UTC <c>timestamp</c>, truncated to its hour).
    /// The caller passes the successor hourly rollup's horizon, as <see cref="QueryStatsHourLedger.PruneSql"/>'s caller does,
    /// because the guard compares the ledger with the rollup and the rollup outlives raw.
    /// </summary>
    public static readonly string PruneSql = HourLedgerStatements.Prune(LedgerTable);
}
