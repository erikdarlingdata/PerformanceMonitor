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
    private async Task CollectConfigIssues(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT database_name, recovery_model, is_auto_shrink_on, is_auto_close_on,
       is_read_committed_snapshot_on, page_verify_option, is_query_store_on
FROM v_database_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)
AND   (is_auto_shrink_on = true OR is_auto_close_on = true
       OR is_read_committed_snapshot_on = false OR page_verify_option != 'CHECKSUM')
ORDER BY database_name";

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var issues = new List<string>();
            if (!reader.IsDBNull(2) && reader.GetBoolean(2)) issues.Add("auto_shrink ON");
            if (!reader.IsDBNull(3) && reader.GetBoolean(3)) issues.Add("auto_close ON");
            var rcsiOn = !reader.IsDBNull(4) && reader.GetBoolean(4);
            if (!rcsiOn) issues.Add("RCSI OFF");
            var pageVerify = reader.IsDBNull(5) ? "" : reader.GetString(5);
            if (!string.IsNullOrEmpty(pageVerify) && pageVerify != "CHECKSUM") issues.Add($"page_verify={pageVerify}");

            // RCSI-off rows carry the three structured inaction-risk fields with the
            // SAME JSON names + types as the Dashboard collector (M-2): Lite emits them
            // null/0 because it has no collect.blocking_deadlock_stats aggregate and the
            // per-DB counts would not be parity-clean. Lite has no Apply path, so the
            // disclosure simply shows the weak-case baseline. The parity test asserts
            // TYPES only (int / int / nullable-int) for these three fields.
            if (!rcsiOn)
            {
                items.Add(new
                {
                    database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    recovery_model = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    rcsi = rcsiOn,
                    query_store = !reader.IsDBNull(6) && reader.GetBoolean(6),
                    issues,
                    auto_shrink = !reader.IsDBNull(2) && reader.GetBoolean(2),
                    auto_close = !reader.IsDBNull(3) && reader.GetBoolean(3),
                    page_verify = pageVerify,
                    rcsi_blocking_events = 0,
                    rcsi_deadlocks = 0,
                    rcsi_reader_writer_pct = (int?)null
                });
            }
            else
            {
                items.Add(new
                {
                    database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    recovery_model = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    rcsi = rcsiOn,
                    query_store = !reader.IsDBNull(6) && reader.GetBoolean(6),
                    issues,
                    // §4.1: structured, wording-independent fields the shared extractor
                    // (FactRemediation.ExtractDbConfigTargets) reads. Identical JSON names
                    // and types to the Dashboard collector (bool / bool / string).
                    auto_shrink = !reader.IsDBNull(2) && reader.GetBoolean(2),
                    auto_close = !reader.IsDBNull(3) && reader.GetBoolean(3),
                    page_verify = pageVerify
                });
            }
        }

        if (items.Count > 0)
            finding.DrillDown!["config_issues"] = items;
    }
}
