using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerFactCollector
{
    /// <summary>
    /// Collects server configuration settings relevant to analysis.
    /// These become facts that amplifiers and the config audit tool can reference
    /// to make recommendations specific (e.g., "your CTFP is 50" vs "check CTFP").
    /// </summary>
    private async Task CollectServerConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            // Latest value PER configuration_name (ROW_NUMBER, not TOP N): server_configuration_history
            // accumulates a row per collection, so a naive TOP-N-ORDER-BY-time returns the newest N
            // ROWS — which collapses to one config when collections are frequent, silently dropping
            // settings. Partition by name and take rn = 1 so each requested setting is its latest value.
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT
        configuration_name,
        CAST(value_in_use AS BIGINT) AS value_in_use,
        ROW_NUMBER() OVER (PARTITION BY configuration_name ORDER BY collection_time DESC) AS rn
    FROM config.server_configuration_history
    WHERE configuration_name IN (
        'cost threshold for parallelism',
        'max degree of parallelism',
        'max server memory (MB)',
        'min server memory (MB)',
        'max worker threads'
    )
)
SELECT
    configuration_name,
    value_in_use
FROM latest
WHERE rn = 1";

            // max/min server memory are read alongside the rooted CONFIG_* facts so the
            // narrow-memory derivation below can compare them without a second query.
            double? maxMemoryMb = null;
            double? minMemoryMb = null;

            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var configName = reader.GetString(0);
                    var value = Convert.ToDouble(reader.GetValue(1));

                    if (configName == "max server memory (MB)") maxMemoryMb = value;
                    if (configName == "min server memory (MB)") minMemoryMb = value;

                    var factKey = configName switch
                    {
                        "cost threshold for parallelism" => "CONFIG_CTFP",
                        "max degree of parallelism" => "CONFIG_MAXDOP",
                        "max server memory (MB)" => "CONFIG_MAX_MEMORY_MB",
                        "min server memory (MB)" => "CONFIG_MIN_MEMORY_MB",
                        "max worker threads" => "CONFIG_MAX_WORKER_THREADS",
                        _ => null
                    };

                    if (factKey == null) continue;

                    facts.Add(new Fact
                    {
                        Source = "config",
                        Key = factKey,
                        Value = value,
                        ServerId = context.ServerId,
                        Metadata = new Dictionary<string, double>
                        {
                            ["value_in_use"] = value
                        }
                    });
                }
            }

            // CONFIG_MIN_MAX_MEMORY_NARROW: emitted only when max is configured AND min is pinned
            // near it (shared rule so Dashboard/Lite agree).
            var narrow = FactRemediation.BuildNarrowMemoryFact(context.ServerId, maxMemoryMb, minMemoryMb);
            if (narrow is not null)
                facts.Add(narrow);
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectServerConfigFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects SQL Server edition and major version from the server_properties table.
    /// </summary>
    private async Task CollectServerMetadataFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1
    engine_edition,
    CAST(LEFT(product_version, CHARINDEX('.', product_version) - 1) AS INT) AS major_version
FROM collect.server_properties
ORDER BY collection_time DESC";

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var edition = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var majorVersion = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));

            if (edition > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_EDITION", Value = edition, ServerId = context.ServerId });
            if (majorVersion > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_MAJOR_VERSION", Value = majorVersion, ServerId = context.ServerId });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectServerMetadataFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects database configuration facts: RCSI status, auto_shrink, auto_close,
    /// recovery model. Aggregates counts across databases.
    /// Dashboard stores config as individual setting rows in config.database_configuration_history.
    /// We pivot from the per-setting rows into aggregated counts.
    /// </summary>
    private async Task CollectDatabaseConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT
        database_name,
        setting_name,
        setting_value,
        ROW_NUMBER() OVER (PARTITION BY database_name, setting_name ORDER BY collection_time DESC) AS rn
    FROM config.database_configuration_history
    WHERE setting_type = 'DATABASE_PROPERTY' /* collector writes 'DATABASE_PROPERTY' (install/39); 'database_option' matched 0 rows → DB_CONFIG fact never fired */
    AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')
),
pivoted AS (
    SELECT
        database_name,
        MAX(CASE WHEN setting_name = 'recovery_model_desc' THEN CAST(setting_value AS NVARCHAR(128)) END) AS recovery_model,
        MAX(CASE WHEN setting_name = 'is_auto_shrink_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_shrink_on,
        MAX(CASE WHEN setting_name = 'is_auto_close_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_close_on,
        MAX(CASE WHEN setting_name = 'is_read_committed_snapshot_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_read_committed_snapshot_on,
        MAX(CASE WHEN setting_name = 'is_auto_create_stats_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_create_stats_on,
        MAX(CASE WHEN setting_name = 'is_auto_update_stats_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_auto_update_stats_on,
        MAX(CASE WHEN setting_name = 'page_verify_option_desc' THEN CAST(setting_value AS NVARCHAR(128)) END) AS page_verify_option,
        MAX(CASE WHEN setting_name = 'is_query_store_on' THEN CAST(setting_value AS NVARCHAR(10)) END) AS is_query_store_on
    FROM latest
    WHERE rn = 1
    GROUP BY database_name
)
SELECT
    COUNT(*) AS database_count,
    COUNT(CASE WHEN is_auto_shrink_on = '1' OR is_auto_shrink_on = 'True' THEN 1 END) AS auto_shrink_count,
    COUNT(CASE WHEN is_auto_close_on = '1' OR is_auto_close_on = 'True' THEN 1 END) AS auto_close_count,
    COUNT(CASE WHEN is_read_committed_snapshot_on = '0' OR is_read_committed_snapshot_on = 'False' THEN 1 END) AS rcsi_off_count,
    COUNT(CASE WHEN is_auto_create_stats_on = '0' OR is_auto_create_stats_on = 'False' THEN 1 END) AS auto_create_stats_off_count,
    COUNT(CASE WHEN is_auto_update_stats_on = '0' OR is_auto_update_stats_on = 'False' THEN 1 END) AS auto_update_stats_off_count,
    COUNT(CASE WHEN page_verify_option IS NOT NULL AND page_verify_option != 'CHECKSUM' THEN 1 END) AS page_verify_not_checksum_count,
    COUNT(CASE WHEN recovery_model = 'FULL' THEN 1 END) AS full_recovery_count,
    COUNT(CASE WHEN recovery_model = 'SIMPLE' THEN 1 END) AS simple_recovery_count,
    COUNT(CASE WHEN is_query_store_on = '1' OR is_query_store_on = 'True' THEN 1 END) AS query_store_on_count
FROM pivoted";

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var dbCount = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            if (dbCount == 0) return;

            var autoShrink = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var autoClose = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var rcsiOff = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var autoCreateOff = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
            var autoUpdateOff = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));
            var pageVerifyBad = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6));
            var fullRecovery = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7));
            var simpleRecovery = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8));
            var queryStoreOn = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9));

            facts.Add(new Fact
            {
                Source = "database_config",
                Key = "DB_CONFIG",
                Value = dbCount,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["database_count"] = dbCount,
                    ["auto_shrink_on_count"] = autoShrink,
                    ["auto_close_on_count"] = autoClose,
                    ["rcsi_off_count"] = rcsiOff,
                    ["auto_create_stats_off_count"] = autoCreateOff,
                    ["auto_update_stats_off_count"] = autoUpdateOff,
                    ["page_verify_not_checksum_count"] = pageVerifyBad,
                    ["full_recovery_count"] = fullRecovery,
                    ["simple_recovery_count"] = simpleRecovery,
                    ["query_store_on_count"] = queryStoreOn
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectDatabaseConfigFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects active global trace flags. Context for the AI to factor into recommendations.
    /// </summary>
    private async Task CollectTraceFlagFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT
        trace_flag,
        status,
        ROW_NUMBER() OVER (PARTITION BY trace_flag ORDER BY collection_time DESC) AS rn
    FROM config.trace_flags_history
    WHERE is_global = 1
)
SELECT trace_flag
FROM latest WHERE rn = 1 AND status = 1
ORDER BY trace_flag";

            using var reader = await cmd.ExecuteReaderAsync();
            var metadata = new Dictionary<string, double>();
            var flagCount = 0;

            while (await reader.ReadAsync())
            {
                var flag = Convert.ToInt32(reader.GetValue(0));
                metadata[$"TF_{flag}"] = 1;
                flagCount++;
            }

            if (flagCount == 0) return;

            metadata["flag_count"] = flagCount;

            facts.Add(new Fact
            {
                Source = "config",
                Key = "TRACE_FLAGS",
                Value = flagCount,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectTraceFlagFactsAsync failed", ex);
        }
    }

    private async Task CollectServerPropertiesFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Version-skew resilience: a server whose PerformanceMonitor DB has not yet had the WS5
            // upgrade lacks some or all of the three server-health columns. Probe EACH independently
            // (COL_LENGTH returns NULL for an absent column) and reference only the present ones, so
            // the core SERVER_HARDWARE read never fails — and keeps flowing — regardless of which
            // columns a partially-upgraded or out-of-order schema happens to have.
            bool hasLpim, hasIfi, hasDumps;
            using (var probe = connection.CreateCommand())
            {
                probe.CommandText = $@"
SELECT
    COL_LENGTH('collect.server_properties', '{LpimColumn}'),
    COL_LENGTH('collect.server_properties', '{IfiColumn}'),
    COL_LENGTH('collect.server_properties', '{DumpsColumn}');";
                using var probeReader = await probe.ExecuteReaderAsync();
                await probeReader.ReadAsync();
                hasLpim = !probeReader.IsDBNull(0);
                hasIfi = !probeReader.IsDBNull(1);
                hasDumps = !probeReader.IsDBNull(2);
            }

            using var cmd = connection.CreateCommand();
            cmd.CommandText = BuildServerPropertiesQuery(hasLpim, hasIfi, hasDumps);

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var cpuCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var htRatio = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
            var physicalMemMb = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var socketCount = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3));
            var coresPerSocket = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4));
            var hadrEnabled = !reader.IsDBNull(5) && Convert.ToBoolean(reader.GetValue(5));
            var edition = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);

            // Read each PRESENT health column by name — the SELECT includes only the columns that
            // exist, so their ordinals shift with the subset; GetOrdinal resolves each regardless.
            // An absent column stays null, so EmitServerHealthFacts emits nothing for it.
            bool? lpim = null;
            bool? ifi = null;
            int? dumpCount = null;
            if (hasLpim)
            {
                var ord = reader.GetOrdinal(LpimColumn);
                lpim = reader.IsDBNull(ord) ? (bool?)null : Convert.ToBoolean(reader.GetValue(ord));
            }
            if (hasIfi)
            {
                var ord = reader.GetOrdinal(IfiColumn);
                ifi = reader.IsDBNull(ord) ? (bool?)null : Convert.ToBoolean(reader.GetValue(ord));
            }
            if (hasDumps)
            {
                var ord = reader.GetOrdinal(DumpsColumn);
                dumpCount = reader.IsDBNull(ord) ? (int?)null : Convert.ToInt32(reader.GetValue(ord));
            }

            if (cpuCount == 0) return;

            facts.Add(new Fact
            {
                Source = "config",
                Key = "SERVER_HARDWARE",
                Value = cpuCount,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["cpu_count"] = cpuCount,
                    ["hyperthread_ratio"] = htRatio,
                    ["physical_memory_mb"] = physicalMemMb,
                    ["socket_count"] = socketCount,
                    ["cores_per_socket"] = coresPerSocket,
                    ["hadr_enabled"] = hadrEnabled ? 1 : 0
                }
            });

            // WS5 server-health advisories (advise-only). Gating lives here so a fact that would
            // score 0 is simply never emitted (noise control); the scorer then scores the emitted
            // fact's Value. Shared with Lite — keep the rules identical (see DuckDbFactCollector).
            FactCollectorHelpers.EmitServerHealthFacts(context, facts, edition, physicalMemMb, lpim, ifi, dumpCount);
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectServerPropertiesFactsAsync failed", ex);
        }
    }
}
