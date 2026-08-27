/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgDrillDownCollector
{
    public const string ConfigIssuesSql = @"
SELECT database_name, recovery_model, is_auto_shrink_on, is_auto_close_on,
       is_read_committed_snapshot_on, page_verify_option, is_query_store_on
FROM v_database_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)
AND   (is_auto_shrink_on = true OR is_auto_close_on = true
       OR is_read_committed_snapshot_on = false OR page_verify_option != 'CHECKSUM')
ORDER BY database_name";

    private async Task CollectConfigIssues(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(ConfigIssuesSql, connection);
        cmd.Parameters.AddWithValue(context.ServerId);

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
            // SAME JSON names + types as the Dashboard collector (M-2): like Lite, Darling
            // emits them null/0 because it has no collect.blocking_deadlock_stats aggregate
            // and the per-DB counts would not be parity-clean. Darling is headless — the
            // disclosure simply shows the weak-case baseline (Lite's posture verbatim).
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
                    // and types to the Lite/Dashboard collectors (bool / bool / string).
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
