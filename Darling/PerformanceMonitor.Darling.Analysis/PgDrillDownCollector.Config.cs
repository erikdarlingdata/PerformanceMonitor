/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgDrillDownCollector
{
    /* #7448: master, msdb, model and tempdb are left out, like the DB_CONFIG fact's own read. SQL Server does not
       allow READ_COMMITTED_SNAPSHOT on tempdb, master or msdb, so a row for one is not something to act on. */
    public const string ConfigIssuesSql = @"
SELECT database_name, recovery_model, is_auto_shrink_on, is_auto_close_on,
       is_read_committed_snapshot_on, page_verify_option, is_query_store_on
FROM v_database_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)
/*SEC*/
AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')
AND   (is_auto_shrink_on = true OR is_auto_close_on = true
       OR is_read_committed_snapshot_on = false OR page_verify_option != 'CHECKSUM')
ORDER BY database_name";

    /* #7448: the per-database RCSI inaction figures, read from the already-collected blocked_process_reports
       and deadlocks of the analysis window (nothing is asked of the monitored server).

       Blocking events are WAITS, counted through BlockingWaits.WaitsCteSql exactly as the BLOCKING fact counts
       them (SQL Server writes a blocked-process report for a still-blocked session every threshold seconds, so
       a report count would overstate a one-minute wait about elevenfold). The lock_mode is the longest-wait
       report's. The statement returns (database, lock_mode, waits) and RcsiFigureAccumulator classifies each
       lock_mode through RcsiLockModeClassifier, so the lock-mode vocabulary lives in one place.

       The window reads event_time with the EventWindowFloor ($4) beside it, like the fact: the table is
       partitioned on collection_time, which an event_time bound gives the planner nothing to exclude chunks on.
       Only the RCSI-off databases ($5, lowered) are read, so the wait chain works over their reports alone. The
       predicate shape is the fact's own, so it uses the same index. */
    public static readonly string RcsiWaitsSql = @"
WITH " + BlockingWaits.WaitsCteSql(
        BlockingWaits.SqlDialect.Postgres, "waits",
        @"SELECT blocked_spid, blocking_spid, event_time, wait_time_ms, database_name, lock_mode
    FROM blocked_process_reports
    WHERE server_id = $1
    AND   event_time >= $2
    AND   event_time <= $3
    AND   collection_time >= $4
    AND   lower(database_name) = ANY($5::text[])") + @"
SELECT lower(database_name) AS database_name, lock_mode, COUNT(*) AS wait_count
FROM waits
GROUP BY lower(database_name), lock_mode";

    /// <summary>Deadlocks per database over the same window. A deadlock row carries the victim process's database
    /// (<c>deadlocks.database_name</c>), and a row with no database cannot be attributed, so it counts for none.
    /// $4 is the event-window floor and $5 the lowered RCSI-off database names.</summary>
    public const string RcsiDeadlocksSql = @"
SELECT lower(database_name) AS database_name, COUNT(*) AS deadlock_count
FROM deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2
AND   deadlock_time <= $3
AND   collection_time >= $4
AND   lower(database_name) = ANY($5::text[])
GROUP BY lower(database_name)";

    private async Task CollectConfigIssues(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(ConfigIssuesSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        /* #5558: the same databases the fact skipped. */
        PgSecondaryReplicaScope.Apply(cmd, context, "DB_CONFIG", "database_name");

        /* Read the rows first: Npgsql runs one reader per connection, and the RCSI figures need the RCSI-off set
           before any row is written. */
        var rows = new List<ConfigIssueRow>();
        using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
        {
            while (await reader.ReadAsync(context.CancellationToken))
            {
                rows.Add(new ConfigIssueRow(
                    Database: reader.IsDBNull(0) ? "" : reader.GetString(0),
                    RecoveryModel: reader.IsDBNull(1) ? "" : reader.GetString(1),
                    AutoShrink: !reader.IsDBNull(2) && reader.GetBoolean(2),
                    AutoClose: !reader.IsDBNull(3) && reader.GetBoolean(3),
                    Rcsi: !reader.IsDBNull(4) && reader.GetBoolean(4),
                    PageVerify: reader.IsDBNull(5) ? "" : reader.GetString(5),
                    QueryStore: !reader.IsDBNull(6) && reader.GetBoolean(6)));
            }
        }

        var rcsiOff = rows.Where(r => !r.Rcsi && r.Database.Length > 0).Select(r => r.Database).ToList();
        var figures = rcsiOff.Count > 0
            ? await CollectRcsiInactionFigures(connection, rcsiOff, context)
            : new RcsiFigureAccumulator();

        var items = new List<object>();
        foreach (var r in rows)
        {
            var issues = new List<string>();
            if (r.AutoShrink) issues.Add("auto_shrink ON");
            if (r.AutoClose) issues.Add("auto_close ON");
            if (!r.Rcsi) issues.Add("RCSI OFF");
            if (!string.IsNullOrEmpty(r.PageVerify) && r.PageVerify != "CHECKSUM") issues.Add($"page_verify={r.PageVerify}");

            // RCSI-off rows carry the three structured inaction-risk fields (#7448): blocked-process waits,
            // deadlocks, and the reader share of the reader plus writer waits, over the analysis window. A
            // database with none reads 0, never null: null would mean not measured.
            if (!r.Rcsi)
            {
                var fig = figures.Get(r.Database);
                items.Add(new
                {
                    database = r.Database,
                    recovery_model = r.RecoveryModel,
                    rcsi = r.Rcsi,
                    query_store = r.QueryStore,
                    issues,
                    auto_shrink = r.AutoShrink,
                    auto_close = r.AutoClose,
                    page_verify = r.PageVerify,
                    rcsi_blocking_events = fig?.BlockingEvents,
                    rcsi_deadlocks = fig?.Deadlocks,
                    rcsi_reader_writer_pct = fig?.ReaderWriterPct
                });
            }
            else
            {
                items.Add(new
                {
                    database = r.Database,
                    recovery_model = r.RecoveryModel,
                    rcsi = r.Rcsi,
                    query_store = r.QueryStore,
                    issues,
                    // §4.1: structured, wording-independent fields the shared extractor
                    // (FactRemediation.ExtractDbConfigTargets) reads. Identical JSON names
                    // and types to the Lite collector (bool / bool / string).
                    auto_shrink = r.AutoShrink,
                    auto_close = r.AutoClose,
                    page_verify = r.PageVerify
                });
            }
        }

        if (items.Count > 0)
            finding.DrillDown!["config_issues"] = items;
    }

    private sealed record ConfigIssueRow(
        string Database, string RecoveryModel, bool AutoShrink, bool AutoClose, bool Rcsi, string PageVerify, bool QueryStore);

    private async Task<RcsiFigureAccumulator> CollectRcsiInactionFigures(
        NpgsqlConnection connection, List<string> databases, AnalysisContext context)
    {
        var figures = new RcsiFigureAccumulator();
        foreach (var d in databases)
            figures.Track(d);
        var lowered = databases.Select(d => d.ToLowerInvariant()).Distinct().ToArray();

        using (var cmd = new NpgsqlCommand(RcsiWaitsSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds })
        {
            AddRcsiWindow(cmd, context, lowered);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
                figures.AddWaits(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetInt64(2));
        }

        using (var cmd = new NpgsqlCommand(RcsiDeadlocksSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds })
        {
            AddRcsiWindow(cmd, context, lowered);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
                figures.AddDeadlocks(reader.GetString(0), reader.GetInt64(1));
        }

        return figures;
    }

    private static void AddRcsiWindow(NpgsqlCommand cmd, AnalysisContext context, string[] lowered)
    {
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(lowered);
    }
}
