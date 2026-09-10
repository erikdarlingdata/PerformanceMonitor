using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>
    /// Collects server configuration settings relevant to analysis.
    /// These become facts that amplifiers and the config audit tool can reference
    /// to make recommendations specific (e.g., "your CTFP is 50" vs "check CTFP").
    /// </summary>
    private async Task CollectServerConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        // Latest value PER configuration_name (ROW_NUMBER, not LIMIT N): server_config accumulates
        // a row per capture, so LIMIT-N-ORDER-BY-time returns the newest N ROWS — which collapses to
        // one config when captures are frequent, silently dropping settings. Partition by name and
        // take rn = 1 so each requested setting is its latest value.
        cmd.CommandText = @"
WITH latest AS (
    SELECT
        configuration_name,
        value_in_use,
        ROW_NUMBER() OVER (PARTITION BY configuration_name ORDER BY capture_time DESC) AS rn
    FROM v_server_config
    WHERE server_id = $1
    AND   configuration_name IN (
        'cost threshold for parallelism',
        'max degree of parallelism',
        'max server memory (MB)',
        'min server memory (MB)',
        'max worker threads',
        'priority boost',
        'lightweight pooling'
    )
)
SELECT configuration_name, value_in_use
FROM latest
WHERE rn = 1";

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

        // max/min server memory are read alongside the rooted CONFIG_* facts so the
        // narrow-memory derivation below can compare them without a second query.
        double? maxMemoryMb = null;
        double? minMemoryMb = null;

        using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
        {
            while (await reader.ReadAsync(context.CancellationToken))
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
                    "priority boost" => "CONFIG_PRIORITY_BOOST",
                    "lightweight pooling" => "CONFIG_LIGHTWEIGHT_POOLING",
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

    /// <summary>
    /// Collects SQL Server edition and major version from the servers table.
    /// These are persisted by RemoteCollectorService after connection check.
    /// </summary>
    private async Task CollectServerMetadataFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT engine_edition,
       CAST(SPLIT_PART(product_version, '.', 1) AS INTEGER) AS major_version
FROM v_server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var edition = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var majorVersion = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));

            if (edition > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_EDITION", Value = edition, ServerId = context.ServerId });
            if (majorVersion > 0)
                facts.Add(new Fact { Source = "config", Key = "SERVER_MAJOR_VERSION", Value = majorVersion, ServerId = context.ServerId });
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Columns may not exist yet (pre-migration). An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects database configuration facts: RCSI status, auto_shrink, auto_close,
    /// recovery model. Aggregates counts across databases.
    /// </summary>
    private async Task CollectDatabaseConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT database_name, recovery_model, is_auto_shrink_on, is_auto_close_on,
           is_read_committed_snapshot_on, is_auto_create_stats_on, is_auto_update_stats_on,
           is_query_store_on, compatibility_level, page_verify_option,
           is_accelerated_database_recovery_on,
           ROW_NUMBER() OVER (PARTITION BY database_name ORDER BY capture_time DESC) AS rn
    FROM v_database_config
    WHERE server_id = $1
)
SELECT
    COUNT(*) AS database_count,
    COUNT(CASE WHEN is_auto_shrink_on THEN 1 END) AS auto_shrink_count,
    COUNT(CASE WHEN is_auto_close_on THEN 1 END) AS auto_close_count,
    COUNT(CASE WHEN NOT is_read_committed_snapshot_on THEN 1 END) AS rcsi_off_count,
    COUNT(CASE WHEN NOT is_auto_create_stats_on THEN 1 END) AS auto_create_stats_off_count,
    COUNT(CASE WHEN NOT is_auto_update_stats_on THEN 1 END) AS auto_update_stats_off_count,
    COUNT(CASE WHEN page_verify_option != 'CHECKSUM' THEN 1 END) AS page_verify_not_checksum_count,
    COUNT(CASE WHEN recovery_model = 'FULL' THEN 1 END) AS full_recovery_count,
    COUNT(CASE WHEN recovery_model = 'SIMPLE' THEN 1 END) AS simple_recovery_count,
    COUNT(CASE WHEN is_query_store_on THEN 1 END) AS query_store_on_count
FROM latest WHERE rn = 1
AND database_name NOT IN ('master', 'msdb', 'model', 'tempdb')";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var dbCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (dbCount == 0) return;

            var autoShrink = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var autoClose = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var rcsiOff = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var autoCreateOff = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var autoUpdateOff = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var pageVerifyBad = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
            var fullRecovery = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
            var simpleRecovery = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
            var queryStoreOn = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects active global trace flags. Context for the AI to factor into recommendations.
    /// </summary>
    private async Task CollectTraceFlagFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH latest AS (
    SELECT trace_flag, status,
           ROW_NUMBER() OVER (PARTITION BY trace_flag ORDER BY capture_time DESC) AS rn
    FROM v_trace_flags
    WHERE server_id = $1
    AND   is_global = true
)
SELECT trace_flag
FROM latest WHERE rn = 1 AND status = true
ORDER BY trace_flag";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            var metadata = new Dictionary<string, double>();
            var flagCount = 0;

            while (await reader.ReadAsync(context.CancellationToken))
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects server hardware properties: CPU count, cores, sockets, memory.
    /// Critical context for MAXDOP and memory recommendations.
    /// </summary>
    private async Task CollectServerPropertiesFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT COALESCE(vcore_count, cpu_count) AS cpu_count, hyperthread_ratio, physical_memory_mb,
       socket_count, cores_per_socket, is_hadr_enabled, edition, product_version,
       lock_pages_in_memory, instant_file_initialization_enabled, memory_dump_count
FROM v_server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var cpuCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            var htRatio = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
            var physicalMemMb = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var socketCount = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3));
            var coresPerSocket = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4));
            var hadrEnabled = !reader.IsDBNull(5) && Convert.ToBoolean(reader.GetValue(5));
            var edition = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);
            bool? lpim = reader.IsDBNull(8) ? (bool?)null : Convert.ToBoolean(reader.GetValue(8));
            bool? ifi = reader.IsDBNull(9) ? (bool?)null : Convert.ToBoolean(reader.GetValue(9));
            int? dumpCount = reader.IsDBNull(10) ? (int?)null : Convert.ToInt32(reader.GetValue(10));

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

            // WS5 server-health advisories (advise-only). Gating mirrors the Dashboard collector so
            // both apps agree on what is worth flagging; a fact that would score 0 is simply never
            // emitted (noise control).
            FactCollectorHelpers.EmitServerHealthFacts(context, facts, edition, physicalMemMb, lpim, ifi, dumpCount);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

}
