/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Holds the connected server's UTC offset so model display properties
/// can convert UTC timestamps to server-local time without per-instance wiring.
/// Set by ServerTab on creation; defaults to local offset for backwards compatibility.
/// </summary>
public static class ServerTimeHelper
{
    private static int _utcOffsetMinutes = (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;

    public static int UtcOffsetMinutes
    {
        get => _utcOffsetMinutes;
        set => _utcOffsetMinutes = value;
    }

    public static DateTime ToServerTime(DateTime utcTime) => utcTime.AddMinutes(_utcOffsetMinutes);

    /// <summary>
    /// Converts a local DateTime (from date picker) to server time.
    /// Use when the user picks dates in their local timezone but the database stores server time.
    /// </summary>
    public static DateTime LocalToServerTime(DateTime localTime)
    {
        var utcTime = localTime.ToUniversalTime();
        return utcTime.AddMinutes(_utcOffsetMinutes);
    }

    /// <summary>
    /// Converts a server DateTime to local time.
    /// Use this when displaying server timestamps to the user in the UI.
    /// </summary>
    public static DateTime ToLocalTime(DateTime serverTime)
    {
        /* Convert server time to UTC, then to local */
        var utcTime = serverTime.AddMinutes(-_utcOffsetMinutes);
        return utcTime.ToLocalTime();
    }

    public static string FormatServerTime(DateTime utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => utcTime.AddMinutes(_utcOffsetMinutes).ToString(format);

    public static string FormatServerTime(DateTime? utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => utcTime.HasValue ? utcTime.Value.AddMinutes(_utcOffsetMinutes).ToString(format) : "";
}

public partial class LocalDataService
{
    /// <summary>
    /// Gets recent deadlock events for a server.
    /// </summary>
    public async Task<List<DeadlockRow>> GetRecentDeadlocksAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    deadlock_time,
    victim_process_id,
    victim_sql_text,
    deadlock_graph_xml
FROM deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY deadlock_time DESC
LIMIT 50";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<DeadlockRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DeadlockRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeadlockTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                VictimProcessId = reader.IsDBNull(2) ? "" : reader.GetString(2),
                VictimSqlText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                DeadlockGraphXml = reader.IsDBNull(4) ? "" : reader.GetString(4)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query snapshots (currently running queries) for a server.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetLatestQuerySnapshotsAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    session_id,
    database_name,
    elapsed_time_formatted,
    query_text,
    status,
    blocking_session_id,
    wait_type,
    wait_time_ms,
    wait_resource,
    cpu_time_ms,
    total_elapsed_time_ms,
    reads,
    writes,
    logical_reads,
    granted_query_memory_gb,
    transaction_isolation_level,
    dop,
    parallel_worker_count,
    query_plan,
    live_query_plan,
    collection_time
FROM query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_text NOT LIKE 'WAITFOR%'
ORDER BY collection_time DESC, cpu_time_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                QueryPlan = reader.IsDBNull(18) ? null : reader.GetString(18),
                LiveQueryPlan = reader.IsDBNull(19) ? null : reader.GetString(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets recent blocked process reports from the XE-based collector.
    /// </summary>
    public async Task<List<BlockedProcessReportRow>> GetRecentBlockedProcessReportsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    event_time,
    database_name,
    blocked_spid,
    blocked_ecid,
    blocking_spid,
    blocking_ecid,
    wait_time_ms,
    wait_resource,
    lock_mode,
    blocked_status,
    blocked_isolation_level,
    blocked_log_used,
    blocked_transaction_count,
    blocked_client_app,
    blocked_host_name,
    blocked_login_name,
    blocked_sql_text,
    blocking_status,
    blocking_isolation_level,
    blocking_client_app,
    blocking_host_name,
    blocking_login_name,
    blocking_sql_text,
    blocked_process_report_xml,
    blocked_transaction_name,
    blocking_transaction_name,
    blocked_last_tran_started,
    blocking_last_tran_started,
    blocked_last_batch_started,
    blocking_last_batch_started,
    blocked_last_batch_completed,
    blocking_last_batch_completed,
    blocked_priority,
    blocking_priority
FROM blocked_process_reports
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY event_time DESC
LIMIT 200";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<BlockedProcessReportRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new BlockedProcessReportRow
            {
                CollectionTime = reader.GetDateTime(0),
                EventTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                DatabaseName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                BlockedSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                BlockedEcid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                BlockingSpid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                BlockingEcid = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                LockMode = reader.IsDBNull(9) ? "" : reader.GetString(9),
                BlockedStatus = reader.IsDBNull(10) ? "" : reader.GetString(10),
                BlockedIsolationLevel = reader.IsDBNull(11) ? "" : reader.GetString(11),
                BlockedLogUsed = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                BlockedTransactionCount = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                BlockedClientApp = reader.IsDBNull(14) ? "" : reader.GetString(14),
                BlockedHostName = reader.IsDBNull(15) ? "" : reader.GetString(15),
                BlockedLoginName = reader.IsDBNull(16) ? "" : reader.GetString(16),
                BlockedSqlText = reader.IsDBNull(17) ? "" : reader.GetString(17),
                BlockingStatus = reader.IsDBNull(18) ? "" : reader.GetString(18),
                BlockingIsolationLevel = reader.IsDBNull(19) ? "" : reader.GetString(19),
                BlockingClientApp = reader.IsDBNull(20) ? "" : reader.GetString(20),
                BlockingHostName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                BlockingLoginName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                BlockingSqlText = reader.IsDBNull(23) ? "" : reader.GetString(23),
                BlockedProcessReportXml = reader.IsDBNull(24) ? "" : reader.GetString(24),
                BlockedTransactionName = reader.IsDBNull(25) ? "" : reader.GetString(25),
                BlockingTransactionName = reader.IsDBNull(26) ? "" : reader.GetString(26),
                BlockedLastTranStarted = reader.IsDBNull(27) ? null : reader.GetDateTime(27),
                BlockingLastTranStarted = reader.IsDBNull(28) ? null : reader.GetDateTime(28),
                BlockedLastBatchStarted = reader.IsDBNull(29) ? null : reader.GetDateTime(29),
                BlockingLastBatchStarted = reader.IsDBNull(30) ? null : reader.GetDateTime(30),
                BlockedLastBatchCompleted = reader.IsDBNull(31) ? null : reader.GetDateTime(31),
                BlockingLastBatchCompleted = reader.IsDBNull(32) ? null : reader.GetDateTime(32),
                BlockedPriority = reader.IsDBNull(33) ? 0 : reader.GetInt32(33),
                BlockingPriority = reader.IsDBNull(34) ? 0 : reader.GetInt32(34)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets blocking incident trend (count of distinct blocking events per time bucket).
    /// Uses blocked_process_reports from Extended Events for more reliable detection.
    /// Falls back to blocking_snapshots if no XE data available.
    /// </summary>
    public async Task<List<TrendPoint>> GetBlockingTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        /* Use blocked_process_reports from XE session - more reliable than point-in-time snapshots
           Group by event_time (when blocking actually occurred) rather than collection_time */
        command.CommandText = @"
SELECT
    bucket,
    incident_count
FROM (
    SELECT
        DATE_TRUNC('minute', event_time) AS bucket,
        COUNT(*) AS incident_count
    FROM blocked_process_reports
    WHERE server_id = $1
    AND   event_time >= $2
    AND   event_time <= $3
    GROUP BY DATE_TRUNC('minute', event_time)
) sub
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TrendPoint
            {
                Time = reader.GetDateTime(0),
                Count = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets deadlock trend (count of deadlocks per hour bucket).
    /// </summary>
    public async Task<List<TrendPoint>> GetDeadlockTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    bucket,
    deadlock_count
FROM (
    SELECT
        DATE_TRUNC('hour', deadlock_time) AS bucket,
        COUNT(*) AS deadlock_count
    FROM deadlocks
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY DATE_TRUNC('hour', deadlock_time)
) sub
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TrendPoint
            {
                Time = reader.GetDateTime(0),
                Count = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1))
            });
        }
        return items;
    }
}

public class TrendPoint
{
    public DateTime Time { get; set; }
    public int Count { get; set; }
}

public class DeadlockRow
{
    public DateTime CollectionTime { get; set; }
    public DateTime? DeadlockTime { get; set; }
    public string VictimProcessId { get; set; } = "";
    public string VictimSqlText { get; set; } = "";
    public string DeadlockGraphXml { get; set; } = "";
    public bool HasDeadlockXml => !string.IsNullOrEmpty(DeadlockGraphXml);

    /// <summary>
    /// Parses the deadlock graph XML and returns a summary of all processes involved.
    /// </summary>
    public string ProcessSummary
    {
        get
        {
            if (string.IsNullOrEmpty(DeadlockGraphXml))
            {
                return "";
            }

            try
            {
                var doc = System.Xml.Linq.XElement.Parse(DeadlockGraphXml);
                var processes = doc.Descendants("process");
                var summaries = new System.Collections.Generic.List<string>();

                foreach (var proc in processes)
                {
                    var id = proc.Attribute("id")?.Value ?? "?";
                    var spid = proc.Attribute("spid")?.Value ?? "?";
                    var db = proc.Attribute("currentdb")?.Value ?? "";
                    var isVictim = string.Equals(id, VictimProcessId, StringComparison.OrdinalIgnoreCase);
                    summaries.Add($"SPID {spid}{(isVictim ? " (victim)" : "")}");
                }

                return string.Join(" vs ", summaries);
            }
            catch
            {
                return "";
            }
        }
    }
}

public class DeadlockProcessDetail
{
    public DateTime? DeadlockTime { get; set; }
    public bool IsVictim { get; set; }
    public string ProcessId { get; set; } = "";
    public int Spid { get; set; }
    public string DatabaseName { get; set; } = "";
    public string SqlText { get; set; } = "";
    public string WaitResource { get; set; } = "";
    public long WaitTime { get; set; }
    public string LockMode { get; set; } = "";
    public string IsolationLevel { get; set; } = "";
    public long LogUsed { get; set; }
    public int TransactionCount { get; set; }
    public string ClientApp { get; set; } = "";
    public string HostName { get; set; } = "";
    public string LoginName { get; set; } = "";
    public string Status { get; set; } = "";
    public string DeadlockGraphXml { get; set; } = "";
    public bool HasDeadlockXml => !string.IsNullOrEmpty(DeadlockGraphXml);

    /* New fields from sp_BlitzLock analysis */
    public string DeadlockType { get; set; } = "";
    public string ObjectNames { get; set; } = "";
    public string ProcName { get; set; } = "";
    public string OwnerMode { get; set; } = "";
    public string WaiterMode { get; set; } = "";
    public string TransactionName { get; set; } = "";
    public int Priority { get; set; }
    public DateTime? LastTranStarted { get; set; }
    public DateTime? LastBatchStarted { get; set; }
    public DateTime? LastBatchCompleted { get; set; }

    public string DeadlockTimeLocal => ServerTimeHelper.FormatServerTime(DeadlockTime);
    public string VictimDisplay => IsVictim ? "Victim" : "";
    public string WaitTimeFormatted => WaitTime > 0 ? $"{WaitTime:N0} ms" : "";
    public string LastTranStartedLocal => ServerTimeHelper.FormatServerTime(LastTranStarted);

    /// <summary>
    /// Parses a list of DeadlockRow into per-process detail rows.
    /// </summary>
    public static List<DeadlockProcessDetail> ParseFromRows(List<DeadlockRow> rows)
    {
        var details = new List<DeadlockProcessDetail>();
        foreach (var row in rows)
        {
            if (string.IsNullOrEmpty(row.DeadlockGraphXml))
                continue;

            try
            {
                var doc = System.Xml.Linq.XElement.Parse(row.DeadlockGraphXml);

                /* Detect parallel deadlock */
                var resourceList = doc.Descendants("resource-list").FirstOrDefault();
                var isParallel = resourceList != null &&
                    (resourceList.Elements("exchangeEvent").Any() || resourceList.Elements("SyncPoint").Any());
                var deadlockType = isParallel ? "Parallel" : "Regular";

                /* Get victim IDs from victim-list */
                var victimIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var vp in doc.Descendants("victimProcess"))
                {
                    var id = vp.Attribute("id")?.Value;
                    if (id != null) victimIds.Add(id);
                }

                /* Parse lock resources to build per-process owner/waiter modes and object names */
                var processOwnerModes = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                var processWaiterModes = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                var processObjectNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

                if (resourceList != null)
                {
                    var lockTypes = new[] { "objectlock", "pagelock", "keylock", "ridlock", "rowgrouplock" };
                    foreach (var lockType in lockTypes)
                    {
                        foreach (var lockNode in resourceList.Elements(lockType))
                        {
                            var objectName = lockNode.Attribute("objectname")?.Value ?? "";

                            /* Parse owners */
                            foreach (var owner in lockNode.Descendants("owner"))
                            {
                                var ownerId = owner.Attribute("id")?.Value ?? "";
                                var ownerMode = owner.Attribute("mode")?.Value ?? "";
                                if (!string.IsNullOrEmpty(ownerId) && !string.IsNullOrEmpty(ownerMode))
                                {
                                    if (!processOwnerModes.ContainsKey(ownerId))
                                        processOwnerModes[ownerId] = new HashSet<string>();
                                    processOwnerModes[ownerId].Add(ownerMode);
                                }
                                if (!string.IsNullOrEmpty(ownerId) && !string.IsNullOrEmpty(objectName))
                                {
                                    if (!processObjectNames.ContainsKey(ownerId))
                                        processObjectNames[ownerId] = new HashSet<string>();
                                    processObjectNames[ownerId].Add(objectName);
                                }
                            }

                            /* Parse waiters */
                            foreach (var waiter in lockNode.Descendants("waiter"))
                            {
                                var waiterId = waiter.Attribute("id")?.Value ?? "";
                                var waiterMode = waiter.Attribute("mode")?.Value ?? "";
                                if (!string.IsNullOrEmpty(waiterId) && !string.IsNullOrEmpty(waiterMode))
                                {
                                    if (!processWaiterModes.ContainsKey(waiterId))
                                        processWaiterModes[waiterId] = new HashSet<string>();
                                    processWaiterModes[waiterId].Add(waiterMode);
                                }
                                if (!string.IsNullOrEmpty(waiterId) && !string.IsNullOrEmpty(objectName))
                                {
                                    if (!processObjectNames.ContainsKey(waiterId))
                                        processObjectNames[waiterId] = new HashSet<string>();
                                    processObjectNames[waiterId].Add(objectName);
                                }
                            }
                        }
                    }
                }

                /* Parse each process */
                foreach (var proc in doc.Descendants("process"))
                {
                    var id = proc.Attribute("id")?.Value ?? "";

                    /* Get proc name from execution stack */
                    var procName = "";
                    foreach (var frame in proc.Descendants("frame"))
                    {
                        var frameProcName = frame.Attribute("procname")?.Value ?? "";
                        if (!string.IsNullOrEmpty(frameProcName) && frameProcName != "adhoc" && frameProcName != "unknown")
                        {
                            procName = frameProcName;
                            break;
                        }
                    }

                    details.Add(new DeadlockProcessDetail
                    {
                        DeadlockTime = row.DeadlockTime,
                        ProcessId = id,
                        IsVictim = victimIds.Contains(id),
                        Spid = int.TryParse(proc.Attribute("spid")?.Value, out var spid) ? spid : 0,
                        DatabaseName = proc.Attribute("currentdbname")?.Value ?? "",
                        SqlText = proc.Element("inputbuf")?.Value?.Trim() ?? "",
                        WaitResource = proc.Attribute("waitresource")?.Value ?? "",
                        WaitTime = long.TryParse(proc.Attribute("waittime")?.Value, out var wt) ? wt : 0,
                        LockMode = proc.Attribute("lockMode")?.Value ?? "",
                        IsolationLevel = proc.Attribute("isolationlevel")?.Value ?? "",
                        LogUsed = long.TryParse(proc.Attribute("logused")?.Value, out var lu) ? lu : 0,
                        TransactionCount = int.TryParse(proc.Attribute("trancount")?.Value, out var tc) ? tc : 0,
                        ClientApp = proc.Attribute("clientapp")?.Value ?? "",
                        HostName = proc.Attribute("hostname")?.Value ?? "",
                        LoginName = proc.Attribute("loginname")?.Value ?? "",
                        Status = proc.Attribute("status")?.Value ?? "",
                        DeadlockGraphXml = row.DeadlockGraphXml,
                        DeadlockType = deadlockType,
                        ProcName = procName,
                        TransactionName = proc.Attribute("transactionname")?.Value ?? "",
                        Priority = int.TryParse(proc.Attribute("priority")?.Value, out var pri) ? pri : 0,
                        LastTranStarted = DateTime.TryParse(proc.Attribute("lasttranstarted")?.Value, out var lts) ? lts : null,
                        LastBatchStarted = DateTime.TryParse(proc.Attribute("lastbatchstarted")?.Value, out var lbs) ? lbs : null,
                        LastBatchCompleted = DateTime.TryParse(proc.Attribute("lastbatchcompleted")?.Value, out var lbc) ? lbc : null,
                        OwnerMode = processOwnerModes.TryGetValue(id, out var om) ? string.Join(", ", om) : "",
                        WaiterMode = processWaiterModes.TryGetValue(id, out var wm) ? string.Join(", ", wm) : "",
                        ObjectNames = processObjectNames.TryGetValue(id, out var on) ? string.Join(", ", on) : ""
                    });
                }
            }
            catch
            {
                /* If XML parsing fails, add a single fallback row */
                details.Add(new DeadlockProcessDetail
                {
                    DeadlockTime = row.DeadlockTime,
                    SqlText = row.VictimSqlText,
                    IsVictim = true,
                    DeadlockGraphXml = row.DeadlockGraphXml
                });
            }
        }
        return details;
    }
}

public class BlockedProcessReportRow
{
    public DateTime CollectionTime { get; set; }
    public DateTime? EventTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public int BlockedSpid { get; set; }
    public int BlockedEcid { get; set; }
    public int BlockingSpid { get; set; }
    public int BlockingEcid { get; set; }
    public long WaitTimeMs { get; set; }
    public string WaitResource { get; set; } = "";
    public string LockMode { get; set; } = "";
    public string BlockedStatus { get; set; } = "";
    public string BlockedIsolationLevel { get; set; } = "";
    public long BlockedLogUsed { get; set; }
    public int BlockedTransactionCount { get; set; }
    public string BlockedClientApp { get; set; } = "";
    public string BlockedHostName { get; set; } = "";
    public string BlockedLoginName { get; set; } = "";
    public string BlockedSqlText { get; set; } = "";
    public string BlockingStatus { get; set; } = "";
    public string BlockingIsolationLevel { get; set; } = "";
    public string BlockingClientApp { get; set; } = "";
    public string BlockingHostName { get; set; } = "";
    public string BlockingLoginName { get; set; } = "";
    public string BlockingSqlText { get; set; } = "";
    public string BlockedProcessReportXml { get; set; } = "";
    public string BlockedTransactionName { get; set; } = "";
    public string BlockingTransactionName { get; set; } = "";
    public DateTime? BlockedLastTranStarted { get; set; }
    public DateTime? BlockingLastTranStarted { get; set; }
    public DateTime? BlockedLastBatchStarted { get; set; }
    public DateTime? BlockingLastBatchStarted { get; set; }
    public DateTime? BlockedLastBatchCompleted { get; set; }
    public DateTime? BlockingLastBatchCompleted { get; set; }
    public int BlockedPriority { get; set; }
    public int BlockingPriority { get; set; }

    public string EventTimeLocal => ServerTimeHelper.FormatServerTime(EventTime);
    public string WaitTimeFormatted => WaitTimeMs < 1000 ? $"{WaitTimeMs} ms" : $"{WaitTimeMs / 1000.0:F1} sec";
    public bool HasReportXml => !string.IsNullOrEmpty(BlockedProcessReportXml);
    public bool IsLongBlock => WaitTimeMs > 30000;
    public string BlockedLastTranStartedLocal => ServerTimeHelper.FormatServerTime(BlockedLastTranStarted);
    public string BlockedLastBatchStartedLocal => ServerTimeHelper.FormatServerTime(BlockedLastBatchStarted);
    public string BlockedLastBatchCompletedLocal => ServerTimeHelper.FormatServerTime(BlockedLastBatchCompleted);
}

public class QuerySnapshotRow
{
    public int SessionId { get; set; }
    public string DatabaseName { get; set; } = "";
    public string ElapsedTimeFormatted { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string Status { get; set; } = "";
    public int BlockingSessionId { get; set; }
    public string WaitType { get; set; } = "";
    public long WaitTimeMs { get; set; }
    public long CpuTimeMs { get; set; }
    public long TotalElapsedTimeMs { get; set; }
    public long Reads { get; set; }
    public long Writes { get; set; }
    public long LogicalReads { get; set; }
    public double GrantedQueryMemoryGb { get; set; }
    public string TransactionIsolationLevel { get; set; } = "";
    public int Dop { get; set; }
    public int ParallelWorkerCount { get; set; }
    public string WaitResource { get; set; } = "";
    public DateTime CollectionTime { get; set; }
    public string? QueryPlan { get; set; }
    public string? LiveQueryPlan { get; set; }
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);
    public bool HasLiveQueryPlan => !string.IsNullOrEmpty(LiveQueryPlan);
    public string CollectionTimeLocal => CollectionTime == DateTime.MinValue ? "" : ServerTimeHelper.FormatServerTime(CollectionTime);
}
