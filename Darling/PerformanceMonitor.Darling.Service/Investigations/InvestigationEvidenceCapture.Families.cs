using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.Json;
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
    /// <summary>A deadlock party's statement is kept up to this many characters.</summary>
    public const int DeadlockStatementMaxChars = 1000;

    /// <summary>A deadlock keeps this many parties, the victims first, and says how many more it had (<c>parties_dropped</c>), so
    /// one deadlock of many parties cannot outgrow the section cap and take every row with it (#6002 review).</summary>
    public const int DeadlockPartiesMax = 8;

    /// <summary>A deadlock keeps this many of the objects its resources name, in graph order, and says how many more it had
    /// (<c>objects_dropped</c>).</summary>
    public const int DeadlockObjectsMax = 20;

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

    /// <summary>The blocked-process reports in the window, newest first, at most 500 of them. Same parameters as
    /// <see cref="DeadlocksSql"/>. In a window with more, the oldest are not read: a chain link only an older report shows is
    /// then missing, and the head is resolved over the links that were read (a blocked session's head can be named one link
    /// short of the true head, never an error).</summary>
    internal const string BlockedReportsSql = @"
SELECT event_time, database_name, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, wait_resource, lock_mode,
       contentious_object, blocked_sql_text, blocking_sql_text, blocked_login_name, blocking_login_name, blocked_host_name,
       blocking_host_name, blocking_client_app, blocking_status, blocked_process_report_xml
FROM blocked_process_reports
WHERE server_id = $1
  AND event_time >= $2
  AND event_time <= $3
  AND collection_time >= $4
  AND " + PerformanceMonitor.Common.BlockingWaits.NotSelfWaitSql + @"
ORDER BY event_time DESC, wait_time_ms DESC
LIMIT 500";

    /// <summary>The statements a set of (sql handle, statement offsets) triples name, newest per triple, from the store's
    /// <c>query_stats</c>, which keeps each statement's own text with its handle and offsets. One read for every head blocker
    /// of a capture (#6002 review: one lookup per head was up to ten reads on an unindexed handle). $1 server, $2 the handles as
    /// the collector writes them (<c>CONVERT(varchar, sql_handle, 1)</c>), $3 and $4 the offsets, aligned with $2, $5 the floor
    /// on <c>collection_time</c>.</summary>
    internal const string StatementsByOffsetSql = @"
SELECT DISTINCT ON (q.sql_handle, q.statement_start_offset, q.statement_end_offset)
       q.sql_handle, q.statement_start_offset, q.statement_end_offset, q.query_text
FROM v_query_stats AS q
JOIN unnest($2::text[], $3::integer[], $4::integer[]) AS k (sql_handle, stmt_start, stmt_end)
  ON q.sql_handle = k.sql_handle
 AND q.statement_start_offset = k.stmt_start
 AND q.statement_end_offset = k.stmt_end
WHERE q.server_id = $1
  AND q.collection_time >= $5
  AND q.query_text IS NOT NULL
  AND btrim(q.query_text) <> ''
ORDER BY q.sql_handle, q.statement_start_offset, q.statement_end_offset, q.collection_time DESC";

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
  -- #6548: SQL Server own housekeeping statements (change tracking cleanup and the like) are not application code. Left
  -- out BEFORE the row cap, so they cannot use up its rows.
  AND NOT EXISTS (SELECT 1 FROM query_store_text AS x
                  WHERE x.server_id = $1
                    AND x.database_name = b.database_name
                    AND x.query_id = b.query_id
                    AND (lower(x.query_sql_text) ~ '" + InternalStatements.LowerTextPattern + @"'
                          -- #6665: or the monitor's own statement (its collector-side drop never fires on Darling)
                          OR " + PerformanceMonitor.Darling.Storage.QueryStoreSelfStatement.SelfOfStoredText + @"))
ORDER BY b.cpu_per_exec_us / g.cpu_per_exec_us DESC, b.database_name, b.query_id
LIMIT $6";

    /// <summary>
    /// <c>deadlocks</c>: each deadlock in the window with its parties (from the shared graph parser) and the objects and
    /// indexes its resources name, read from the graph at capture and COPIED into the row. A temporary object's generated name
    /// (<c>#orders_____…</c>) is kept as the graph spells it. The whole graph goes through the statement filter (#4348) before
    /// it is parsed or any statement is cut, so a cut can never hide a statement the filter would name. A deadlock keeps
    /// <see cref="DeadlockPartiesMax"/> parties and <see cref="DeadlockObjectsMax"/> objects, with counts of the rest.
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
            var graph = SensitiveStatements.Xml(reader.GetString(3)) ?? "";
            var model = DeadlockGraphParser.Parse(graph);
            var parties = new JsonArray();
            /* The victims first, then graph order, so the cap never drops the victim. */
            var kept = model.Processes.OrderByDescending(p => p.IsVictim).Take(DeadlockPartiesMax).ToList();
            foreach (var p in kept)
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

            var objects = DeadlockObjects(graph);
            var objectsDropped = Math.Max(0, objects.Count - DeadlockObjectsMax);
            for (var i = 0; i < objectsDropped; i++)
                objects.RemoveAt(objects.Count - 1);
            var row = new JsonObject
            {
                ["deadlock_time"] = Value(reader.GetDateTime(0)),
                ["database_name"] = reader.IsDBNull(1) ? null : reader.GetString(1),
                ["victim_process_id"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                ["is_parallel"] = model.IsParallel,
                ["objects"] = objects,
                ["parties"] = parties,
            };
            if (objectsDropped > 0)
                row["objects_dropped"] = objectsDropped;
            if (model.Processes.Count > kept.Count)
                row["parties_dropped"] = model.Processes.Count - kept.Count;
            rows.Add(row);
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
    private static int HeadOf(int spid, Dictionary<int, int> blockedBy)
    {
        var seen = new HashSet<int> { spid };
        var head = spid;
        while (blockedBy.TryGetValue(head, out var next) && next != 0 && seen.Add(next))
            head = next;
        return head;
    }

    /// <summary>
    /// <c>blocking_chains</c>: one row per blocked and blocking pair in the window (its longest wait), longest wait first, each
    /// with the head of its chain. Each row's report XML is returned beside the rows, aligned with them, for
    /// <see cref="FitReportXml"/> to fill in once the rows have their place under the cap.
    /// </summary>
    private async Task<SectionRead> ReadBlockingChainsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var reports = await ReadBlockedReportsAsync(serverId, from, to, cancellationToken).ConfigureAwait(false);
        var blockedBy = BlockedBy(reports);
        var chains = reports
            .GroupBy(r => (r.BlockedSpid, r.BlockingSpid))
            .Select(g => g.OrderByDescending(r => r.WaitMs).ThenByDescending(r => r.EventTime).First())
            .OrderByDescending(r => r.WaitMs).ThenBy(r => r.BlockedSpid).ThenBy(r => r.BlockingSpid)
            .Take(SectionRows)
            .ToList();
        var rows = chains.ConvertAll(r => new JsonObject
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
                    ["report_xml"] = null,
                });
        /* #4348: each report goes through the statement filter whole, before FitReportXml cuts it. */
        return new SectionRead(rows, ReportXml: chains.ConvertAll(r => SensitiveStatements.Xml(r.ReportXml)));
    }

    /// <summary>
    /// The <c>blocking_chains</c> content (#6002): the rows are placed first, by <see cref="Fit"/> with no report XML, and each kept
    /// row's report XML then takes what is left of <see cref="MaxSectionBytes"/>, in row order (longest wait first). A report that
    /// does not fit whole is cut at the cap and its row says <c>report_xml_truncated: true</c>; once the cap is spent, every later
    /// report is cut to nothing and says so. A report is never left out silently and never cut at a fixed length. A cut report no
    /// longer parses as XML, so under <c>maskLiterals</c> or <c>metadataOnly</c> the masker withholds it whole.
    /// <para>#6002 review: every row with a report first takes its floor, <c>report_xml: ""</c> and <c>report_xml_truncated:
    /// true</c>, so a report placed earlier can never leave a later row's flag over the cap (the store refuses the whole write
    /// then). When the floors themselves do not fit, rows come off the tail, counted in <c>rows_dropped</c>.</para>
    /// </summary>
    internal static string FitReportXml(DateTime from, DateTime to, List<JsonObject> rows, IReadOnlyList<string?> reportXml)
    {
        var content = JsonNode.Parse(Fit(from, to, rows))!.AsObject();
        var kept = content["rows"]!.AsArray();
        for (var i = 0; i < kept.Count && i < reportXml.Count; i++)
        {
            if (string.IsNullOrEmpty(reportXml[i]))
                continue;
            kept[i]!.AsObject()["report_xml"] = "";
            kept[i]!.AsObject()["report_xml_truncated"] = true;
        }

        while (kept.Count > 0 && FittedBytes(content) > MaxSectionBytes)
        {
            kept.RemoveAt(kept.Count - 1);
            content["rows_dropped"] = (content["rows_dropped"]?.GetValue<int>() ?? 0) + 1;
        }

        for (var i = 0; i < kept.Count && i < reportXml.Count; i++)
        {
            var xml = reportXml[i];
            if (string.IsNullOrEmpty(xml))
                continue;
            var row = kept[i]!.AsObject();
            row.Remove("report_xml_truncated");
            row["report_xml"] = xml;
            if (FittedBytes(content) <= MaxSectionBytes)
                continue;

            /* The longest prefix that fits, by halving: the bytes a prefix takes grow with its length. The floor ("") is known
               to fit, so the search always ends inside the cap. */
            row["report_xml_truncated"] = true;
            int low = 0, high = xml.Length - 1;
            while (low < high)
            {
                var mid = (low + high + 1) / 2;
                row["report_xml"] = Prefix(xml, mid);
                if (FittedBytes(content) <= MaxSectionBytes)
                    low = mid;
                else
                    high = mid - 1;
            }

            row["report_xml"] = Prefix(xml, low);
        }

        return content.ToJsonString(Relaxed);
    }

    /// <summary>XML kept as written (<c>&lt;</c>, not its <c><</c> escape), as the jsonb column prints it, so the cap counts
    /// what is stored.</summary>
    private static readonly JsonSerializerOptions Relaxed = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private static int FittedBytes(JsonObject content) => StoredBytes(content.ToJsonString(Relaxed));

    /// <summary>The first <paramref name="length"/> characters, one fewer when that would split a surrogate pair.</summary>
    private static string Prefix(string text, int length) =>
        length > 0 && char.IsHighSurrogate(text[length - 1]) ? text[..(length - 1)] : text[..length];

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
        /* #6002 review: the heads are the chains' own heads, grouped from every report, so each head has at least one report
           (a session blocked by 55 and later by 61 leaves 55 a blocker no chain resolves to, and that is no head). The head's
           newest report is the newest one that shows it blocking; a head no report shows blocking (a blocked session whose
           blocker is 0) has none to read a statement from and is left out. */
        var heads = reports
            .GroupBy(r => HeadOf(r.BlockedSpid, blockedBy))
            .Select(g => (Newest: reports.Where(r => r.BlockingSpid == g.Key).OrderByDescending(r => r.EventTime).FirstOrDefault(),
                          Blocked: g.Count(),
                          MaxWait: g.Max(r => r.WaitMs)))
            .Where(h => h.Newest is not null && h.Newest.BlockingSpid != 0)
            .OrderByDescending(h => h.Blocked).ThenByDescending(h => h.MaxWait).ThenBy(h => h.Newest!.BlockingSpid)
            .Take(SectionRows)
            .Select(h => (Newest: h.Newest!, h.Blocked, h.MaxWait,
                          /* #4348: the report goes through the statement filter whole before it is parsed. */
                          Process: BlockingProcess(SensitiveStatements.Xml(h.Newest!.ReportXml))))
            .ToList();

        var statements = await ReadStatementsAsync(
            serverId, heads.Where(h => h.Process.SqlHandle is not null).Select(h => h.Process).ToList(), from - StatementLookback,
            cancellationToken).ConfigureAwait(false);

        var rows = new List<JsonObject>();
        foreach (var (newest, blocked, maxWait, process) in heads)
        {
            string? statement = null;
            if (process.SqlHandle is not null)
                statements.TryGetValue((process.SqlHandle, process.StmtStart, process.StmtEnd), out statement);

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

    /// <summary>The stored statement of each frame, by (handle, start, end), in one read; a frame no stored statement matches is
    /// absent.</summary>
    private async Task<Dictionary<(string Handle, int Start, int End), string>> ReadStatementsAsync(
        int serverId, List<BlockingProcessFrame> frames, DateTime floor, CancellationToken cancellationToken)
    {
        var statements = new Dictionary<(string, int, int), string>();
        if (frames.Count == 0)
            return statements;

        await using var command = _store.CreateCommand(StatementsByOffsetSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = frames.ConvertAll(f => f.SqlHandle!).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = frames.ConvertAll(f => f.StmtStart).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = frames.ConvertAll(f => f.StmtEnd).ToArray() });
        DarlingMcpReadParameters.AddTimestamp(command, floor);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            statements[(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2))] = reader.GetString(3);
        return statements;
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
            /* #6002 review round 2: the plan filter below is synchronous (up to 10 s a call, two calls a target), so the
               section's deadline is checked between targets. */
            cancellationToken.ThrowIfCancellationRequested();
            var database = target["database_name"]?.GetValue<string>() ?? "";
            var good = target["good_plan_id"]!.GetValue<long>();
            var bad = target["bad_plan_id"]!.GetValue<long>();
            var plans = await ReadPlansAsync(serverId, database, good, bad, cancellationToken).ConfigureAwait(false);
            /* #4348 review: each plan goes through the statement filter, as every MCP plan read does (DarlingStoredPlanReader),
               before a compiled value is read from it: a value inside a statement the filter names comes back withheld, and
               CompiledValues returns null for a plan with a withheld value or the withheld placeholder. */
            target["good_compiled"] = Compiled(SensitiveStatements.Xml(plans.GetValueOrDefault(good)));
            cancellationToken.ThrowIfCancellationRequested();
            target["bad_compiled"] = Compiled(SensitiveStatements.Xml(plans.GetValueOrDefault(bad)));
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

    /// <summary>Characters of a top statement's sample text kept in <c>client_timeouts</c>.</summary>
    internal const int ClientTimeoutStatementMaxChars = 1000;

    /// <summary>
    /// <c>client_timeouts</c> (#6553): the blocks <c>get_client_timeouts</c> answers for the window, from the same shared statements
    /// (<see cref="AttentionWindowSql"/>, bounded: <c>collection_time &gt; from</c> and <c>&lt;= to</c>): the exact <c>count</c> of the
    /// window's event_counter rows, the <c>sampled_events</c> behind the lists, and the <c>top_client_apps</c>, <c>top_hosts</c>,
    /// <c>top_databases</c> and <c>top_statements</c> (a sample text each, cut to <see cref="ClientTimeoutStatementMaxChars"/>).
    /// One row. A window with no count and no sampled event is a true negative: an empty list. The lists are sampled events, a
    /// subset, so they say who was timing out and never how many; <c>count</c> is the exact figure.
    /// </summary>
    private async Task<List<JsonObject>> ReadClientTimeoutsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        async Task<List<T>> Query<T>(string sql, Func<NpgsqlDataReader, T> map)
        {
            await using var command = _store.CreateCommand(sql);
            command.CommandTimeout = CommandTimeoutSeconds;
            DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
            var result = new List<T>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                result.Add(map(reader));
            return result;
        }

        var count = (await Query(AttentionWindowSql.Counts("attention_events", bounded: true), r => r.GetInt64(1)).ConfigureAwait(false)).Sum();
        var apps = await Query(AttentionWindowSql.TopBy("attention_events", "client_app_name", bounded: true), r => (Name: r.GetString(0), Events: r.GetInt64(1))).ConfigureAwait(false);
        var hosts = await Query(AttentionWindowSql.TopBy("attention_events", "client_hostname", bounded: true), r => (Name: r.GetString(0), Events: r.GetInt64(1))).ConfigureAwait(false);
        var databases = await Query(AttentionWindowSql.TopBy("attention_events", "database_name", bounded: true), r => (Name: r.GetString(0), Events: r.GetInt64(1))).ConfigureAwait(false);
        var statements = await Query(AttentionWindowSql.TopStatements("attention_events", bounded: true),
            r => (Fingerprint: r.GetString(0), Events: r.GetInt64(1), Text: r.IsDBNull(2) ? null : r.GetString(2))).ConfigureAwait(false);
        if (count == 0 && apps.Count == 0 && hosts.Count == 0 && databases.Count == 0 && statements.Count == 0)
            return new List<JsonObject>();

        static JsonArray Named(IEnumerable<(string Name, long Events)> items, string key) =>
            new(items.Select(i => (JsonNode?)new JsonObject { [key] = i.Name, ["sampled_events"] = i.Events }).ToArray());

        return new List<JsonObject>
        {
            new()
            {
                ["count"] = count,
                ["top_client_apps"] = Named(apps, "client_app_name"),
                ["top_hosts"] = Named(hosts, "client_hostname"),
                ["top_databases"] = Named(databases, "database_name"),
                ["top_statements"] = new JsonArray(statements.Select(s => (JsonNode?)new JsonObject
                {
                    ["statement_fingerprint"] = s.Fingerprint,
                    ["sampled_events"] = s.Events,
                    ["sql_text"] = Cut(s.Text, ClientTimeoutStatementMaxChars),
                }).ToArray()),
                ["note"] = "count is exact (the session's event_counter). The lists come from sampled ring-buffer events, a subset: they show who and what was timing out, never how many.",
            },
        };
    }

    /* #6579: cut at a text-element boundary, like ClientTimeoutsRead, so a surrogate pair or combining sequence is never split. */
    private static string? Cut(string? text, int max) =>
        string.IsNullOrEmpty(text) ? null : text.Length <= max ? text : text[..McpHelpers.TextElementCutLength(text, max)];

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
