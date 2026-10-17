using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The deadlock, blocking and regression evidence families (#6002, #5632 Phase C C6). Each read takes the store's collected
/// rows only and COPIES what it keeps into the frozen section: the section never points at a collected row, so it outlives the
/// row's retention (a deadlock on a temporary table keeps the temporary object's name after the graph row ages out).
/// Collected text sits under the keys <c>mcp.dataDetail</c> masks when the evidence is served: statements under
/// <c>sql_text</c>, <c>statement_text</c> and <c>input_buffer</c>, logins and hosts under their own keys, report XML by shape,
/// and compiled values under <c>compiled_value</c>.
/// </summary>
public sealed partial class InvestigationEvidenceCapture
{
    /// <summary>A report's XML is kept on its <c>blocking_chains</c> row up to this many characters; a longer one is left out
    /// (<c>report_xml_omitted</c>), since a cut XML document no longer parses and one report must not crowd out the others.</summary>
    public const int ReportXmlMaxChars = 8000;

    /// <summary>A deadlock party's statement is kept up to this many characters.</summary>
    public const int DeadlockStatementMaxChars = 1000;

    /// <summary>How far before the window end <c>regression_targets</c> looks for a target's plans: one day, so one capture
    /// reads one day of the server's Query Store rows and not the detector's fourteen.</summary>
    public static readonly TimeSpan RegressionLookback = TimeSpan.FromDays(1);

    /// <summary>A target's newest plan costs at least this many times its cheapest plan's CPU per call: the PLAN_REGRESSION
    /// detector's own 2x (aliased).</summary>
    public const int RegressionFactor = PlanInputComparison.PerRowFactor;

    /// <summary>How far before the window start the head blocker's statement is looked for in <c>query_stats</c>: a cached
    /// statement is re-collected every query_stats run, so a day holds any statement still in the plan cache.</summary>
    public static readonly TimeSpan StatementLookback = TimeSpan.FromDays(1);

    /// <summary>The deadlocks in the window, newest first. $1 server, $2 and $3 the window on <c>deadlock_time</c>, $4 the
    /// partition floor (<see cref="EventWindowFloor"/>).</summary>
    internal const string DeadlocksSql = $@"
SELECT deadlock_time, database_name, victim_process_id, deadlock_graph_xml
FROM deadlocks
WHERE server_id = $1
  AND deadlock_time >= $2
  AND deadlock_time <= $3
  AND collection_time >= $4
  AND deadlock_graph_xml IS NOT NULL
  AND deadlock_graph_xml <> ''
ORDER BY deadlock_time DESC
LIMIT 10";

    /// <summary>The blocked-process reports in the window, newest first. Same parameters as <see cref="DeadlocksSql"/>.</summary>
    internal const string BlockedReportsSql = @"
SELECT event_time, database_name, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, wait_resource, lock_mode,
       contentious_object, blocked_sql_text, blocking_sql_text, blocked_login_name, blocking_login_name, blocked_host_name,
       blocking_host_name, blocking_client_app, blocking_status, blocked_process_report_xml
FROM blocked_process_reports
WHERE server_id = $1
  AND event_time >= $2
  AND event_time <= $3
  AND collection_time >= $4
ORDER BY event_time DESC, wait_time_ms DESC
LIMIT 500";

    /// <summary>The statement a (sql handle, statement offsets) triple names, from the store's <c>query_stats</c>, which keeps
    /// each statement's own text with its handle and offsets: $1 server, $2 the handle as the collector writes it
    /// (<c>CONVERT(varchar, sql_handle, 1)</c>), $3 and $4 the offsets, $5 the floor on <c>collection_time</c>.</summary>
    internal const string StatementByOffsetSql = @"
SELECT query_text
FROM v_query_stats
WHERE server_id = $1
  AND sql_handle = $2
  AND statement_start_offset = $3
  AND statement_end_offset = $4
  AND collection_time >= $5
  AND query_text IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// The regression targets (#6002): per query, its newest plan against its cheapest plan over <see cref="RegressionLookback"/>,
    /// kept when the newest costs at least <see cref="RegressionFactor"/> times the cheapest per call, as the PLAN_REGRESSION
    /// detector compares them. The interval dedup is the detector's (latest collection per interval). A target that has not
    /// run since the window start is KEPT, with <c>window_executions</c> 0, and the capture marks it <c>stopped_running</c>.
    /// $1 server, $2 the lookback start, $3 the window start, $4 the window end, $5 the factor, $6 the row cap.
    /// </summary>
    internal const string RegressionTargetsSql = @"
WITH deduped AS (
    SELECT database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
           (array_agg(query_plan_hash ORDER BY collection_time DESC, execution_count DESC))[1] AS query_plan_hash,
           (array_agg(execution_count ORDER BY collection_time DESC, execution_count DESC))[1] AS execution_count,
           (array_agg(avg_cpu_time_us ORDER BY collection_time DESC, execution_count DESC))[1] AS avg_cpu_time_us,
           (array_agg(last_execution_time ORDER BY collection_time DESC, execution_count DESC))[1] AS last_execution_time
    FROM v_query_store_stats
    WHERE server_id = $1
      AND execution_type_desc = 'Regular'
      AND last_execution_time >= $2
      AND collection_time >= $2
      AND first_execution_time <= $4
    GROUP BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
), plans AS (
    SELECT database_name, query_id, plan_id, any_value(query_plan_hash)::text AS query_plan_hash,
           SUM(execution_count) AS executions,
           SUM(avg_cpu_time_us * execution_count)::double precision / NULLIF(SUM(execution_count), 0) AS cpu_per_exec_us,
           MAX(last_execution_time) AS last_execution_time,
           COALESCE(SUM(execution_count) FILTER (WHERE last_execution_time >= $3), 0) AS window_executions
    FROM deduped
    GROUP BY database_name, query_id, plan_id
), ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY last_execution_time DESC, plan_id DESC) AS newest,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY cpu_per_exec_us ASC NULLS LAST, plan_id) AS cheapest,
           MAX(last_execution_time) OVER (PARTITION BY database_name, query_id) AS query_last_execution,
           SUM(window_executions) OVER (PARTITION BY database_name, query_id) AS query_window_executions
    FROM plans AS p
    WHERE p.executions > 0
)
SELECT b.database_name, b.query_id,
       b.cpu_per_exec_us / g.cpu_per_exec_us AS regression_factor,
       b.query_last_execution AS last_execution_time,
       CAST(b.query_window_executions AS bigint) AS window_executions,
       g.plan_id AS good_plan_id, g.query_plan_hash AS good_plan_hash, g.cpu_per_exec_us AS good_cpu_per_exec_us,
       CAST(g.executions AS bigint) AS good_executions,
       b.plan_id AS bad_plan_id, b.query_plan_hash AS bad_plan_hash, b.cpu_per_exec_us AS bad_cpu_per_exec_us,
       CAST(b.executions AS bigint) AS bad_executions
FROM ranked AS b
JOIN ranked AS g
  ON g.database_name = b.database_name
 AND g.query_id = b.query_id
 AND g.cheapest = 1
WHERE b.newest = 1
  AND g.plan_id <> b.plan_id
  AND g.cpu_per_exec_us > 0
  AND b.cpu_per_exec_us >= $5 * g.cpu_per_exec_us
ORDER BY b.cpu_per_exec_us / g.cpu_per_exec_us DESC, b.database_name, b.query_id
LIMIT $6";

    /// <summary>
    /// <c>deadlocks</c>: each deadlock in the window with its parties (from the shared graph parser) and the objects and
    /// indexes its resources name, read from the graph at capture and COPIED into the row. A temporary object's generated name
    /// (<c>#orders_____…</c>) is kept as the graph spells it.
    /// </summary>
    private async Task<List<JsonObject>> ReadDeadlocksAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var rows = new List<JsonObject>();
        await using var command = _store.CreateCommand(DeadlocksSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
        DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(from));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var graph = reader.GetString(3);
            var model = DeadlockGraphParser.Parse(graph);
            var parties = new JsonArray();
            foreach (var p in model.Processes)
            {
                parties.Add(new JsonObject
                {
                    ["process_id"] = p.Id,
                    ["spid"] = p.Spid,
                    ["is_victim"] = p.IsVictim,
                    ["database_name"] = p.DatabaseName,
                    ["proc_name"] = p.ProcName,
                    ["lock_mode"] = p.LockMode,
                    ["wait_resource"] = p.WaitResource,
                    ["contentious_object"] = p.ContentiousObject,
                    ["isolation_level"] = p.IsolationLevel,
                    ["login_name"] = p.LoginName,
                    ["host_name"] = p.HostName,
                    ["client_app"] = p.ClientApp,
                    ["sql_text"] = Cut(p.SqlText, DeadlockStatementMaxChars),
                });
            }

            rows.Add(new JsonObject
            {
                ["deadlock_time"] = Value(reader.GetDateTime(0)),
                ["database_name"] = reader.IsDBNull(1) ? null : reader.GetString(1),
                ["victim_process_id"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                ["is_parallel"] = model.IsParallel,
                ["objects"] = DeadlockObjects(graph),
                ["parties"] = parties,
            });
        }

        return rows;
    }

    /// <summary>The objects and indexes a deadlock graph's resources name, once each: (resource kind, object name, index
    /// name, lock mode), in graph order. A graph that does not parse names none.</summary>
    internal static JsonArray DeadlockObjects(string? graphXml)
    {
        var objects = new JsonArray();
        var root = ParseXml(graphXml);
        if (root is null)
            return objects;

        var seen = new HashSet<(string, string?, string?)>();
        foreach (var list in root.DescendantsAndSelf().Where(e => e.Name.LocalName == "resource-list"))
        {
            foreach (var resource in list.Elements())
            {
                var objectName = (string?)resource.Attribute("objectname");
                var indexName = (string?)resource.Attribute("indexname");
                if (string.IsNullOrWhiteSpace(objectName) && string.IsNullOrWhiteSpace(indexName))
                    continue;
                var kind = resource.Name.LocalName;
                if (!seen.Add((kind, objectName, indexName)))
                    continue;
                objects.Add(new JsonObject
                {
                    ["kind"] = kind,
                    ["object_name"] = objectName,
                    ["index_name"] = indexName,
                    ["mode"] = (string?)resource.Attribute("mode"),
                });
            }
        }

        return objects;
    }

    /// <summary>One blocked-process report as the two blocking sections read it.</summary>
    private sealed record BlockedReport(
        DateTime EventTime, string? Database, int BlockedSpid, int BlockedEcid, int BlockingSpid, int BlockingEcid, long WaitMs,
        string? WaitResource, string? LockMode, string? ContentiousObject, string? BlockedSql, string? BlockingSql,
        string? BlockedLogin, string? BlockingLogin, string? BlockedHost, string? BlockingHost, string? BlockingApp,
        string? BlockingStatus, string? ReportXml);

    private async Task<List<BlockedReport>> ReadBlockedReportsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var reports = new List<BlockedReport>();
        await using var command = _store.CreateCommand(BlockedReportsSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
        DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(from));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            string? S(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
            int I(int i) => reader.IsDBNull(i) ? 0 : Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture);
            reports.Add(new BlockedReport(
                reader.GetDateTime(0), S(1), I(2), I(3), I(4), I(5),
                reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                S(7), S(8), S(9), S(10), S(11), S(12), S(13), S(14), S(15), S(16), S(17), S(18)));
        }

        return reports;
    }

    /// <summary>The session at the head of <paramref name="spid"/>'s chain: follow blocked to blocking through the window's
    /// reports (the newest report per blocked session) until a session that is not itself blocked; a cycle stops where it
    /// closes.</summary>
    private static int HeadOf(int spid, IReadOnlyDictionary<int, int> blockedBy)
    {
        var seen = new HashSet<int> { spid };
        var head = spid;
        while (blockedBy.TryGetValue(head, out var next) && next != 0 && seen.Add(next))
            head = next;
        return head;
    }

    /// <summary>
    /// <c>blocking_chains</c>: one row per blocked and blocking pair in the window (its longest wait), longest wait first, each
    /// with the head of its chain and the report XML (up to <see cref="ReportXmlMaxChars"/>).
    /// </summary>
    private async Task<List<JsonObject>> ReadBlockingChainsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var reports = await ReadBlockedReportsAsync(serverId, from, to, cancellationToken).ConfigureAwait(false);
        var blockedBy = BlockedBy(reports);
        return reports
            .GroupBy(r => (r.BlockedSpid, r.BlockingSpid))
            .Select(g => g.OrderByDescending(r => r.WaitMs).ThenByDescending(r => r.EventTime).First())
            .OrderByDescending(r => r.WaitMs).ThenBy(r => r.BlockedSpid)
            .Take(SectionRows)
            .Select(r =>
            {
                var keep = r.ReportXml is { Length: > 0 and <= ReportXmlMaxChars };
                return new JsonObject
                {
                    ["event_time"] = Value(r.EventTime),
                    ["database_name"] = r.Database,
                    ["blocked_spid"] = r.BlockedSpid,
                    ["blocked_ecid"] = r.BlockedEcid,
                    ["blocking_spid"] = r.BlockingSpid,
                    ["blocking_ecid"] = r.BlockingEcid,
                    ["head_blocker_spid"] = HeadOf(r.BlockedSpid, blockedBy),
                    ["wait_time_ms"] = r.WaitMs,
                    ["wait_resource"] = r.WaitResource,
                    ["lock_mode"] = r.LockMode,
                    ["contentious_object"] = r.ContentiousObject,
                    ["blocked_sql_text"] = r.BlockedSql,
                    ["blocking_sql_text"] = r.BlockingSql,
                    ["blocked_login_name"] = r.BlockedLogin,
                    ["blocking_login_name"] = r.BlockingLogin,
                    ["blocked_host_name"] = r.BlockedHost,
                    ["blocking_host_name"] = r.BlockingHost,
                    ["report_xml"] = keep ? r.ReportXml : null,
                    ["report_xml_omitted"] = !keep && !string.IsNullOrEmpty(r.ReportXml),
                };
            })
            .ToList();
    }

    /// <summary>The newest report's blocking session per blocked session.</summary>
    private static Dictionary<int, int> BlockedBy(IEnumerable<BlockedReport> reports)
    {
        var blockedBy = new Dictionary<int, int>();
        foreach (var r in reports.OrderByDescending(r => r.EventTime))
            blockedBy.TryAdd(r.BlockedSpid, r.BlockingSpid);
        return blockedBy;
    }

    /// <summary>
    /// <c>head_blocker</c>: each session at the head of a chain in the window (a blocking session no report shows blocked),
    /// most sessions blocked first, from its newest report. Its statement comes from the blocking process's top execution
    /// frame: the frame's sql handle and statement offsets are looked up in the store's <c>query_stats</c>, and the
    /// statement found there is <c>statement_source</c> <c>exact</c>. A report has no statement TEXT in its frames (the
    /// blocked-process report carries only the handle and offsets, checked on a SQL Server 2019 capture for #6002), and a
    /// sleeping blocker has no frame at all, so when there is no frame or no stored statement matches it the input buffer is
    /// kept and the source is <c>inferred</c>: never a procedure header passed off as the statement.
    /// </summary>
    private async Task<List<JsonObject>> ReadHeadBlockersAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var reports = await ReadBlockedReportsAsync(serverId, from, to, cancellationToken).ConfigureAwait(false);
        var blockedBy = BlockedBy(reports);
        var heads = reports
            .Where(r => r.BlockingSpid != 0 && !blockedBy.ContainsKey(r.BlockingSpid))
            .GroupBy(r => r.BlockingSpid)
            .Select(g => (Newest: g.OrderByDescending(r => r.EventTime).First(),
                          Blocked: reports.Count(r => HeadOf(r.BlockedSpid, blockedBy) == g.Key),
                          MaxWait: reports.Where(r => HeadOf(r.BlockedSpid, blockedBy) == g.Key).Max(r => r.WaitMs)))
            .OrderByDescending(h => h.Blocked).ThenByDescending(h => h.MaxWait).ThenBy(h => h.Newest.BlockingSpid)
            .Take(SectionRows)
            .ToList();

        var rows = new List<JsonObject>();
        foreach (var (newest, blocked, maxWait) in heads)
        {
            var process = BlockingProcess(newest.ReportXml);
            string? statement = null;
            if (process.SqlHandle is not null)
            {
                statement = await ReadStatementAsync(
                    serverId, process.SqlHandle, process.StmtStart, process.StmtEnd, from - StatementLookback, cancellationToken).ConfigureAwait(false);
            }

            var exact = statement is not null;
            rows.Add(new JsonObject
            {
                ["spid"] = newest.BlockingSpid,
                ["ecid"] = newest.BlockingEcid,
                ["database_name"] = newest.Database,
                ["reports_in_chain"] = blocked,
                ["max_wait_time_ms"] = maxWait,
                ["status"] = newest.BlockingStatus ?? process.Status,
                ["login_name"] = newest.BlockingLogin,
                ["host_name"] = newest.BlockingHost,
                ["client_app"] = newest.BlockingApp,
                ["sql_handle"] = process.SqlHandle,
                ["stmtstart"] = process.SqlHandle is null ? null : process.StmtStart,
                ["stmtend"] = process.SqlHandle is null ? null : process.StmtEnd,
                ["statement_source"] = exact ? "exact" : "inferred",
                ["statement_text"] = exact ? statement : process.InputBuffer ?? newest.BlockingSql,
                ["input_buffer"] = process.InputBuffer,
                ["report_time"] = Value(newest.EventTime),
            });
        }

        return rows;
    }

    /// <summary>What the head blocker's statement is resolved from: the blocking process's top execution frame (its sql
    /// handle in the collector's spelling, and the offsets, 0 and -1 when the frame leaves them out) and its input buffer.</summary>
    internal sealed record BlockingProcessFrame(string? SqlHandle, int StmtStart, int StmtEnd, string? InputBuffer, string? Status);

    /// <summary>Reads <see cref="BlockingProcessFrame"/> from a blocked-process report. A frame without a usable handle (absent,
    /// or all zeros as SQL Server writes for a frame it cannot name) counts as no frame.</summary>
    internal static BlockingProcessFrame BlockingProcess(string? reportXml)
    {
        var root = ParseXml(reportXml);
        var process = root?.DescendantsAndSelf().FirstOrDefault(e => e.Name.LocalName == "blocking-process")?
            .Elements().FirstOrDefault(e => e.Name.LocalName == "process");
        if (process is null)
            return new BlockingProcessFrame(null, 0, -1, null, null);

        var inputBuffer = process.Elements().FirstOrDefault(e => e.Name.LocalName == "inputbuf")?.Value.Trim();
        var status = (string?)process.Attribute("status");
        var frame = process.Elements().FirstOrDefault(e => e.Name.LocalName == "executionStack")?
            .Elements().FirstOrDefault(e => e.Name.LocalName == "frame");
        var handle = NormalizeHandle((string?)frame?.Attribute("sqlhandle"));
        var start = int.TryParse((string?)frame?.Attribute("stmtstart"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) ? s : 0;
        var end = int.TryParse((string?)frame?.Attribute("stmtend"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var e2) ? e2 : -1;
        return new BlockingProcessFrame(handle, start, end, string.IsNullOrEmpty(inputBuffer) ? null : inputBuffer, status);
    }

    /// <summary>A report's sql handle (<c>0x0200…</c>, lower-case hex) in the spelling <c>query_stats</c> stores
    /// (<c>CONVERT(varchar, sql_handle, 1)</c>: <c>0x</c> then upper-case hex); null for no handle or an all-zero one.</summary>
    internal static string? NormalizeHandle(string? handle)
    {
        if (string.IsNullOrWhiteSpace(handle) || !handle.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || handle.Length <= 2)
            return null;
        var hex = handle[2..];
        if (hex.All(c => c == '0'))
            return null;
        return "0x" + hex.ToUpperInvariant();
    }

    private async Task<string?> ReadStatementAsync(
        int serverId, string handle, int start, int end, DateTime floor, CancellationToken cancellationToken)
    {
        await using var command = _store.CreateCommand(StatementByOffsetSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, handle);
        DarlingMcpReadParameters.AddInt(command, start);
        DarlingMcpReadParameters.AddInt(command, end);
        DarlingMcpReadParameters.AddTimestamp(command, floor);
        var text = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    /// <summary>
    /// <c>regression_targets</c>: the targets of <see cref="RegressionTargetsSql"/>, each with its good (cheapest) and bad
    /// (newest) plan by id and hash, never XML, and both plans' parameter compiled values (<see cref="PlanInputComparison.CompiledValues"/>,
    /// the one stored copy) under <c>compiled_value</c>. A target that has not run since the window start is kept and marked
    /// <c>stopped_running</c>. A plan the store no longer holds, or cannot be read, has <c>compiled</c> null.
    /// </summary>
    private async Task<List<JsonObject>> ReadRegressionTargetsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        List<JsonObject> targets;
        await using (var command = _store.CreateCommand(RegressionTargetsSql))
        {
            command.CommandTimeout = CommandTimeoutSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddTimestamp(command, to - RegressionLookback);
            DarlingMcpReadParameters.AddTimestamp(command, from);
            DarlingMcpReadParameters.AddTimestamp(command, to);
            command.Parameters.Add(new NpgsqlParameter<double> { TypedValue = RegressionFactor });
            DarlingMcpReadParameters.AddInt(command, SectionRows);
            targets = await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
        }

        foreach (var target in targets)
        {
            var database = target["database_name"]?.GetValue<string>() ?? "";
            var good = target["good_plan_id"]!.GetValue<long>();
            var bad = target["bad_plan_id"]!.GetValue<long>();
            var plans = await ReadPlansAsync(serverId, database, good, bad, cancellationToken).ConfigureAwait(false);
            target["good_compiled"] = Compiled(plans.GetValueOrDefault(good));
            target["bad_compiled"] = Compiled(plans.GetValueOrDefault(bad));
            target["stopped_running"] = target["window_executions"]?.GetValue<long>() == 0;
        }

        return targets;
    }

    /// <summary>The stored plan XML of two Query Store plans, through the detector's own plan read (aliased).</summary>
    private async Task<Dictionary<long, string?>> ReadPlansAsync(int serverId, string database, long good, long bad, CancellationToken cancellationToken)
    {
        var plans = new Dictionary<long, string?>();
        await using var command = _store.CreateCommand(PgFactCollector.PlanRegressionPlansSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, database);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = new[] { good, bad } });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            plans[Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture)] = PayloadDimensions.ResolveContent(
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : (byte[])reader.GetValue(2));
        }

        return plans;
    }

    /// <summary>A plan's compiled values as <c>[{parameter, compiled_value}]</c>, or null when the plan is missing or unreadable.</summary>
    private static JsonArray? Compiled(string? planXml)
    {
        var values = PlanInputComparison.CompiledValues(planXml);
        if (values is null)
            return null;
        var array = new JsonArray();
        foreach (var (parameter, value) in values)
            array.Add(new JsonObject { ["parameter"] = parameter, ["compiled_value"] = value });
        return array;
    }

    private static string? Cut(string? text, int max) =>
        string.IsNullOrEmpty(text) ? null : text.Length <= max ? text : text[..max];

    /// <summary>Parses collected XML with DTDs refused; null for none or XML that does not parse.</summary>
    private static XElement? ParseXml(string? xml)
    {
        if (string.IsNullOrWhiteSpace(xml))
            return null;
        try
        {
            using var text = new StringReader(xml);
            using var reader = XmlReader.Create(text, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null });
            return XElement.Load(reader);
        }
        catch (XmlException)
        {
            return null;
        }
    }
}
