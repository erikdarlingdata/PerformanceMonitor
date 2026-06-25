using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerDrillDownCollector
{
    private async Task CollectConfigIssues(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // The Dashboard uses config.database_configuration_history which stores
        // settings as rows (setting_type, setting_name, setting_value) not columns.
        // Pivot the latest snapshot into the format we need.
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT database_name, setting_name,
           CAST(setting_value AS NVARCHAR(256)) AS setting_value,
           ROW_NUMBER() OVER (PARTITION BY database_name, setting_name ORDER BY collection_time DESC) AS rn
    FROM config.database_configuration_history
    WHERE setting_name IN (
        'recovery_model_desc', 'is_auto_shrink_on', 'is_auto_close_on',
        'is_read_committed_snapshot_on', 'page_verify_option_desc', 'is_query_store_on'
    )
),
pivoted AS (
    SELECT
        database_name,
        MAX(CASE WHEN setting_name = 'recovery_model_desc' THEN setting_value END) AS recovery_model,
        MAX(CASE WHEN setting_name = 'is_auto_shrink_on' THEN setting_value END) AS is_auto_shrink_on,
        MAX(CASE WHEN setting_name = 'is_auto_close_on' THEN setting_value END) AS is_auto_close_on,
        MAX(CASE WHEN setting_name = 'is_read_committed_snapshot_on' THEN setting_value END) AS is_rcsi_on,
        MAX(CASE WHEN setting_name = 'page_verify_option_desc' THEN setting_value END) AS page_verify_option,
        MAX(CASE WHEN setting_name = 'is_query_store_on' THEN setting_value END) AS is_query_store_on
    FROM latest
    WHERE rn = 1
    GROUP BY database_name
)
SELECT database_name, recovery_model,
       is_auto_shrink_on, is_auto_close_on,
       is_rcsi_on, page_verify_option, is_query_store_on
FROM pivoted
WHERE is_auto_shrink_on = '1' OR is_auto_close_on = '1'
   OR is_rcsi_on = '0' OR page_verify_option != 'CHECKSUM'
ORDER BY database_name;";

        // Read the pivoted rows first into a typed buffer so we can determine the
        // RCSI-OFF set BEFORE building the emitted items — the §3.3 enrichment is
        // computed only for RCSI-off databases and injected into those rows.
        var rows = new List<ConfigIssueRow>();
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                rows.Add(new ConfigIssueRow
                {
                    Database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    RecoveryModel = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    AutoShrink = (reader.IsDBNull(2) ? "" : reader.GetString(2)) == "1",
                    AutoClose = (reader.IsDBNull(3) ? "" : reader.GetString(3)) == "1",
                    Rcsi = (reader.IsDBNull(4) ? "" : reader.GetString(4)) == "1",
                    PageVerify = reader.IsDBNull(5) ? "" : reader.GetString(5),
                    QueryStore = (reader.IsDBNull(6) ? "" : reader.GetString(6)) == "1"
                });
            }
        }

        // §3.3 enrichment: per-DB blocking/deadlock counts + reader/writer split, ONLY
        // for RCSI-off databases, over the analysis window, from already-collected
        // monitoring tables (no fresh probe of the target server).
        var rcsiOff = rows.Where(r => !r.Rcsi && !string.IsNullOrEmpty(r.Database))
                          .Select(r => r.Database)
                          .ToList();
        var enrichment = rcsiOff.Count > 0
            ? await CollectRcsiInactionFigures(connection, rcsiOff, context)
            : new Dictionary<string, RcsiInactionFigures>(StringComparer.Ordinal);

        var items = new List<object>();
        foreach (var r in rows)
        {
            var issues = new List<string>();
            if (r.AutoShrink) issues.Add("auto_shrink ON");
            if (r.AutoClose) issues.Add("auto_close ON");
            if (!r.Rcsi) issues.Add("RCSI OFF");
            if (!string.IsNullOrEmpty(r.PageVerify) && r.PageVerify != "CHECKSUM") issues.Add($"page_verify={r.PageVerify}");

            // RCSI-off rows carry the three structured inaction-risk fields (M-2:
            // identical JSON names + types to Lite, which emits them null/0). RCSI-on
            // rows omit them entirely (the affordance never applies there).
            if (!r.Rcsi)
            {
                enrichment.TryGetValue(r.Database, out var fig);
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
                    // §3.3: int / int / nullable-int. Counts from blocking_deadlock_stats
                    // (O-P3-F — NOT a blocking_BlockedProcessReport row count); the split
                    // is a separate pass over blocking_BlockedProcessReport.
                    rcsi_blocking_events = fig?.BlockingEvents ?? 0,
                    rcsi_deadlocks = fig?.Deadlocks ?? 0,
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

    /// <summary>
    /// Emits the <c>server_config</c> drill-down for a CONFIG_* finding (WS3): the single bad
    /// server-level setting that rooted this finding, with its latest <c>value_in_use</c> plus the
    /// server's <c>engine_edition</c> and <c>cores_per_socket</c> (needed to compute the
    /// edition-aware, core-capped MAXDOP recommendation in
    /// <see cref="FactRemediation.ExtractServerConfigTargets"/>). The structured fields
    /// (<c>setting</c>, <c>current_value</c>, <c>edition</c>, <c>cores_per_socket</c>) are what the
    /// shared extractor reads; nothing here is executed. The four CONFIG_* keys root SEPARATE
    /// findings, so each finding emits exactly the one row for its own setting.
    /// </summary>
    private async Task CollectServerConfig(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Latest value per requested config (ROW_NUMBER, not TOP N — same dedup correctness as the
        // fact collector), plus the latest server_properties row for edition + cores-per-socket.
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest_config AS (
    SELECT
        configuration_name,
        CAST(value_in_use AS BIGINT) AS value_in_use,
        ROW_NUMBER() OVER (PARTITION BY configuration_name ORDER BY collection_time DESC) AS rn
    FROM config.server_configuration_history
    WHERE configuration_name IN (
        'cost threshold for parallelism',
        'max degree of parallelism',
        'max server memory (MB)',
        'min server memory (MB)'
    )
)
SELECT
    configuration_name,
    value_in_use
FROM latest_config
WHERE rn = 1;

SELECT TOP (1)
    engine_edition,
    cores_per_socket
FROM collect.server_properties
ORDER BY collection_time DESC;";

        long? ctfp = null, maxdop = null, maxMem = null, minMem = null;
        int edition = 0, coresPerSocket = 0;

        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var name = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var value = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
                switch (name)
                {
                    case "cost threshold for parallelism": ctfp = value; break;
                    case "max degree of parallelism": maxdop = value; break;
                    case "max server memory (MB)": maxMem = value; break;
                    case "min server memory (MB)": minMem = value; break;
                }
            }

            if (await reader.NextResultAsync() && await reader.ReadAsync())
            {
                edition = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                coresPerSocket = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
            }
        }

        var items = new List<object>();

        // Emit ONLY the setting that rooted this finding (the engine roots one CONFIG_* per finding).
        // edition / cores_per_socket ride every row (the extractor uses them only for MAXDOP).
        if (pathKeys.Contains("CONFIG_MAXDOP") && maxdop is { } md)
            items.Add(new { setting = "maxdop", current_value = md, edition, cores_per_socket = coresPerSocket });

        if (pathKeys.Contains("CONFIG_CTFP") && ctfp is { } ct)
            items.Add(new { setting = "ctfp", current_value = ct, edition, cores_per_socket = coresPerSocket });

        if (pathKeys.Contains("CONFIG_MAX_MEMORY_MB") && maxMem is { } mx)
            items.Add(new { setting = "max_memory", current_value = mx, edition, cores_per_socket = coresPerSocket });

        // For the narrow-memory finding the bad value the operator acts on is MIN server memory
        // (lower it); carry it as the min_memory target.
        if (pathKeys.Contains("CONFIG_MIN_MAX_MEMORY_NARROW") && minMem is { } mn)
            items.Add(new { setting = "min_memory", current_value = mn, edition, cores_per_socket = coresPerSocket });

        if (items.Count > 0)
            finding.DrillDown!["server_config"] = items;
    }
}
