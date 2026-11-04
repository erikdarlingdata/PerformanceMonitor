using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Analysis;

public partial class DrillDownCollector
{
    /* #7448: master, msdb, model and tempdb are left out, like the DB_CONFIG fact's own read. SQL Server does not
       allow READ_COMMITTED_SNAPSHOT on tempdb, master or msdb, so a row for one is not something to act on. */
    internal const string ConfigIssuesSql = @"
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

    private async Task CollectConfigIssues(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = ConfigIssuesSql;

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        /* #5558: the same databases the fact skipped. */
        cmd.CommandText = SecondaryReplicaScope.Apply(cmd.CommandText, cmd, context, "DB_CONFIG", "database_name", 2);

        /* Read the rows first: the RCSI figures need the RCSI-off set before any row is written. */
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
                    // and types to the Darling collector (bool / bool / string).
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

    /* #7448: the per-database RCSI inaction figures, read from the already-collected blocked_process_reports and
       deadlocks of the analysis window. Blocking events are WAITS, counted through BlockingWaits.WaitsCteSql
       exactly as the BLOCKING fact counts them, with the longest-wait report's lock_mode; the statement returns
       (database, lock_mode, waits) and RcsiFigureAccumulator classifies each lock_mode through
       RcsiLockModeClassifier. Only the RCSI-off databases are read ($4 and up, lowered), so the wait chain works
       over their reports alone. Deadlocks count each stored deadlock once (StoredEventCopies.DeadlockDistinctCount),
       attributed by the victim process's database (deadlocks.database_name); a row with no database counts for
       none. */
    private async Task<RcsiFigureAccumulator> CollectRcsiInactionFigures(
        DuckDBConnection connection, List<string> databases, AnalysisContext context)
    {
        var figures = new RcsiFigureAccumulator();
        foreach (var d in databases)
            figures.Track(d);
        var lowered = databases.Select(d => d.ToLowerInvariant()).Distinct().ToList();
        var names = string.Join(", ", lowered.Select((_, i) => "$" + (4 + i)));

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
WITH " + BlockingWaits.WaitsCteSql(
                BlockingWaits.SqlDialect.DuckDb, "waits",
                @"SELECT blocked_spid, blocking_spid, event_time, wait_time_ms, database_name, lock_mode
    FROM " + StoredEventCopies.BlockedProcessReports(
                    "server_id = $1 AND event_time >= $2 AND event_time <= $3 AND lower(database_name) IN (" + names + ")") + @" AS ev") + @"
SELECT lower(database_name) AS database_name, lock_mode, COUNT(*) AS wait_count
FROM waits
GROUP BY lower(database_name), lock_mode";
            AddWindow(cmd, context, lowered);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
                figures.AddWaits(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), Convert.ToInt64(reader.GetValue(2)));
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT lower(dl.database_name) AS database_name, " + StoredEventCopies.DeadlockDistinctCount + @" AS deadlock_count
FROM v_deadlocks AS dl
WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3
AND   lower(dl.database_name) IN (" + names + @")
GROUP BY lower(dl.database_name)";
            AddWindow(cmd, context, lowered);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
                figures.AddDeadlocks(reader.GetString(0), Convert.ToInt64(reader.GetValue(1)));
        }

        return figures;
    }

    private static void AddWindow(System.Data.Common.DbCommand cmd, AnalysisContext context, List<string> lowered)
    {
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
        foreach (var d in lowered)
            cmd.Parameters.Add(new DuckDBParameter { Value = d });
    }
}
