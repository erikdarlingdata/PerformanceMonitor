using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The evidence capture of Level 1 investigations (#6001, #5632 Phase C, D9 and D10): right after a High CPU request opens, in
/// the worker, it freezes the sections an agent would read first from the monitoring STORE's already collected rows.
/// <b>It opens no connection to a monitored server</b>: the only connection it holds is the store's data source, so it runs
/// the same against a store that has no monitored-server connection string at all. Sections are rows in
/// <c>config.investigation_evidence</c>: <c>top_statements</c> (the heaviest statements by CPU in the breach window, plans by
/// hash and never XML), <c>active_requests</c> (requests by host, program and login, each request counted once), <c>query_store</c>
/// (the latest Query Store health), <c>jobs</c> and <c>waits</c>.
///
/// <para><b>States.</b> A section is <c>ready</c> when the collector that feeds it (<see cref="SectionCollectors"/>) has COMPLETED
/// a run on the server at or after the window end, read from the run record: a run that found nothing is still a run, so a ready
/// section with no rows carries an empty list. The section is then FROZEN: the store never rewrites it. Until that run it is
/// <c>pending</c> and looked at again every pass, whatever rows the window holds so far (rows landing inside the window after the
/// first pass are in the frozen section); a collector with no run since the window end leaves it <c>pending</c> until
/// <see cref="PendingGrace"/> after the request opened (the collectors run every one to fifteen minutes) and only then
/// <c>not_collected</c>. A PostgreSQL target has none of these SQL Server collectors, so its sections are
/// <c>not_collected</c> at once. The store journals <c>evidence_ready</c> once per request when no section is pending
/// (<see cref="InvestigationRequestStore.RecordEvidenceAsync"/>); #6003 delivers it.</para>
///
/// <para><b>Slow collectors (#6001).</b> A section's grace is <see cref="GraceFor"/>: the larger of <see cref="PendingGrace"/> and
/// <see cref="SlowCollectorGraceMultiplier"/> of its collector's configured interval, read from the schedule the worker runs the
/// collector by. A section whose grace is above <see cref="PendingGrace"/> (<c>query_store</c> on the hourly default) does not hold
/// <c>evidence_ready</c> back: the event is journaled once the sections on the base grace are final and the base grace has passed
/// (or every section is final sooner), and a slow section still <c>pending</c> then is named under <c>pending</c> in its detail.
/// It stays <c>pending</c>, looked at every pass: its collector's run flips it to <c>ready</c> in place with its rows, or its own
/// grace flips it to <c>not_collected</c>. The request leaves the candidate set only when every section is final.</para>
///
/// <para><b>Size.</b> Each section's content is held to <see cref="MaxSectionBytes"/>, the report evidence's own cap (aliased,
/// not copied): rows are dropped from the tail until it fits, and the content says how many.</para>
/// </summary>
public sealed class InvestigationEvidenceCapture
{
    /// <summary>The cap on one section's content, in bytes: the report evidence's cap, aliased (#3684).</summary>
    public const int MaxSectionBytes = InvestigationToolCore.MaxEvidenceBytes;

    /// <summary>The metric whose requests this capture serves.</summary>
    public const string HighCpuMetric = AlertEngine.CpuPersistenceMetric;

    /// <summary>How far before the first alert the breach window reaches. The alert fires after
    /// <see cref="AlertEngine.CpuBreachSamples"/> breaching one-minute samples, so this holds the whole breach and its lead-in.</summary>
    public static readonly TimeSpan BreachLeadIn = TimeSpan.FromMinutes(15);

    /// <summary>How far after the first alert the window reaches, for the sample that was in flight when it fired.</summary>
    public static readonly TimeSpan BreachTail = TimeSpan.FromMinutes(1);

    /// <summary>How long after a request opened an empty section stays <c>pending</c> before it is <c>not_collected</c>.</summary>
    public static readonly TimeSpan PendingGrace = TimeSpan.FromMinutes(20);

    /// <summary>A collector gets two of its own intervals to land a run after the window end before it is called
    /// <c>not_collected</c>: the section's grace is the larger of <see cref="PendingGrace"/> and this many intervals.</summary>
    public const int SlowCollectorGraceMultiplier = 2;

    /// <summary>A request older than this is left as it is: its window is long past.</summary>
    public static readonly TimeSpan CaptureHorizon = TimeSpan.FromHours(6);

    /// <summary>Statements kept in <c>top_statements</c>.</summary>
    public const int TopStatements = 10;

    /// <summary>Rows kept in every other section.</summary>
    public const int SectionRows = 10;

    /// <summary>Databases kept in <c>query_store</c>: the ones that burned the most CPU in the breach window. The content says
    /// how many more there were (<c>databases_dropped</c>).</summary>
    public const int QueryStoreDatabasesPerCapture = 10;

    /// <summary>
    /// The most requests one pass looks at, oldest first, in pages of <see cref="BatchRequests"/>. Each request costs one
    /// evidence read, one collection-log read and up to five short section reads on the store (each held to a
    /// <see cref="CommandTimeoutSeconds"/> deadline), so 100 requests keep a pass to a few hundred short reads even in an alert
    /// storm across the fleet. A request left over waits for the next pass, and the leftovers are the NEWEST (farthest from
    /// <see cref="PendingGrace"/>), so the cap never costs a request its grace.
    /// A request waiting only on a slow collector stays a candidate for that collector's grace (two hours on the hourly default,
    /// and never past <see cref="CaptureHorizon"/>), so the cost of a pass is still this cap, and the requests that can hold a
    /// place in it are bounded by the route open limits over those two hours.
    /// </summary>
    public const int CandidatesPerTick = 100;

    private const int CommandTimeoutSeconds = 30;
    private const int BatchRequests = 25;

    /// <summary>The section names, in the order they are written.</summary>
    public static readonly IReadOnlyList<string> Sections = new[] { "top_statements", "active_requests", "query_store", "jobs", "waits" };

    /// <summary>The collector (<c>collect.collection_log.collector_name</c>) that feeds each section. A section is ready when its
    /// collector has completed a run at or after the window end, whether or not that run found anything.</summary>
    public static readonly IReadOnlyDictionary<string, string> SectionCollectors = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["top_statements"] = "query_stats",
        ["active_requests"] = "query_snapshots",
        ["query_store"] = "query_store_health",
        ["jobs"] = "running_jobs",
        ["waits"] = "wait_stats",
    };

    /// <summary>The collectors that have a successful run at or after <c>$3</c> for server <c>$1</c> ($2 the collector names).
    /// A run that found nothing is still a run, so this reads the run record, never the fact rows.</summary>
    internal const string CollectorRunsSql = @"
SELECT DISTINCT collector_name
FROM v_collection_log
WHERE server_id = $1
  AND collector_name = ANY($2)
  AND status = 'SUCCESS'
  AND collection_time >= $3";

    internal static readonly string TopStatementsSql = $@"
SELECT database_name, query_hash::text AS query_hash, MAX(query_plan_hash)::text AS query_plan_hash,
       CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
       CAST(SUM(delta_execution_count) AS bigint) AS executions,
       CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
       CAST(SUM(delta_logical_reads) AS bigint) AS total_reads
FROM query_stats
WHERE server_id = $1
  AND collection_time >= $2
  AND collection_time <= $3
  AND {TimescaleSupport.IntervalHonestSourceFilter}
GROUP BY database_name, query_hash
HAVING SUM(delta_worker_time) > 0
ORDER BY SUM(delta_worker_time) DESC NULLS LAST, database_name, query_hash
LIMIT {TopStatements}";

    /// <summary>How far two snapshots' derived request start times (snapshot time minus the request's elapsed time) may differ
    /// before they are two requests. The two readings are taken a moment apart on the monitored server, so the same
    /// request's derived start wobbles by well under a second; a new batch on the same session starts after the previous
    /// snapshot, so its start moves by at least the snapshot interval (a minute or more).</summary>
    private const int RequestStartToleranceSeconds = 5;

    /* #6001 review: <cpu_time_ms> on an active-request row is that request's RUNNING total, so a request seen in ten snapshots
       would count ten times under a SUM. The snapshots of one request (same session and request id, one derived start, no CPU
       total that goes backwards) collapse to their MAX first; only then are the requests summed and ranked. */
    internal static readonly string ActiveRequestsSql = $@"
WITH snaps AS (
    SELECT host_name, program_name, login_name, session_id, COALESCE(request_id, 0) AS request_id, collection_time, cpu_time_ms,
           collection_time - total_elapsed_time_ms * interval '1 millisecond' AS started_at
    FROM query_snapshots
    WHERE server_id = $1
      AND collection_time >= $2
      AND collection_time <= $3
), marked AS (
    SELECT s.*,
           CASE WHEN LAG(s.collection_time) OVER w IS NULL THEN 1
                WHEN s.cpu_time_ms < LAG(s.cpu_time_ms) OVER w THEN 1
                WHEN s.started_at > LAG(s.started_at) OVER w + interval '{RequestStartToleranceSeconds} seconds' THEN 1
                ELSE 0 END AS is_new
    FROM snaps AS s
    WINDOW w AS (PARTITION BY s.session_id, s.request_id ORDER BY s.collection_time)
), numbered AS (
    SELECT m.*, SUM(m.is_new) OVER (PARTITION BY m.session_id, m.request_id ORDER BY m.collection_time) AS run_no
    FROM marked AS m
), per_request AS (
    SELECT host_name, program_name, login_name, session_id, request_id, run_no, count(*) AS samples, MAX(cpu_time_ms) AS cpu_ms
    FROM numbered
    GROUP BY host_name, program_name, login_name, session_id, request_id, run_no
)
SELECT host_name, program_name, login_name, CAST(SUM(samples) AS bigint) AS samples, count(DISTINCT session_id) AS sessions,
       count(*) AS requests, CAST(SUM(cpu_ms) AS bigint) AS total_cpu_ms, CAST(MAX(cpu_ms) AS bigint) AS max_cpu_ms
FROM per_request
GROUP BY host_name, program_name, login_name
ORDER BY SUM(cpu_ms) DESC NULLS LAST, host_name, program_name, login_name
LIMIT {SectionRows}";

    internal const string JobsSql = @"
SELECT job_name, start_time, current_duration_seconds, avg_duration_seconds, is_running_long
FROM (
    SELECT DISTINCT ON (job_name) job_name, start_time, current_duration_seconds, avg_duration_seconds, is_running_long, collection_time
    FROM v_running_jobs
    WHERE server_id = $1
      AND collection_time >= $2
      AND collection_time <= $3
    ORDER BY job_name, collection_time DESC
) AS j
ORDER BY current_duration_seconds DESC NULLS LAST, job_name
LIMIT 10";

    private readonly NpgsqlDataSource _store;
    private readonly InvestigationRequestStore _requests;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly Func<string, int, int> _collectorIntervalMinutes;

    /// <param name="store">The monitoring store's data source: the only connection the capture holds.</param>
    /// <param name="requests">The request store, over the same store (it should share <paramref name="utcNow"/>).</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="utcNow">The one clock the grace is judged by (a test supplies its own).</param>
    /// <param name="collectorIntervalMinutes">A collector's configured interval in minutes on one server, as (collector name, server id):
    /// the worker passes the schedule it runs the collector by, overrides included. Without it the code defaults are used.</param>
    public InvestigationEvidenceCapture(
        NpgsqlDataSource store, InvestigationRequestStore requests, ILogger logger, Func<DateTime>? utcNow = null,
        Func<string, int, int>? collectorIntervalMinutes = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _requests = requests ?? throw new ArgumentNullException(nameof(requests));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _collectorIntervalMinutes = collectorIntervalMinutes ?? DefaultIntervalMinutes;
    }

    /// <summary>The interval the code defaults give a collector (no override): the same resolution the worker schedules it by.</summary>
    private static int DefaultIntervalMinutes(string collectorName, int serverId) =>
        CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(
            StoreConfigProvider.ResolveSchedule(collectorName, serverId, []).FrequencyMinutes);

    /// <summary>How long after the request opened a section may wait for its collector's run before it is <c>not_collected</c>:
    /// <see cref="PendingGrace"/>, or <see cref="SlowCollectorGraceMultiplier"/> of the collector's own interval when that is longer.</summary>
    public TimeSpan GraceFor(string section, int serverId)
    {
        var interval = TimeSpan.FromMinutes(_collectorIntervalMinutes(SectionCollectors[section], serverId));
        var slow = TimeSpan.FromTicks(interval.Ticks * SlowCollectorGraceMultiplier);
        return slow > PendingGrace ? slow : PendingGrace;
    }

    /// <summary>One pass: every High CPU request still needing evidence gets its sections looked at. A failed request is
    /// logged and the next is tried. Returns how many requests it recorded.</summary>
    public async Task<int> CaptureOnceAsync(CancellationToken cancellationToken)
    {
        var now = _utcNow();
        var recorded = 0;
        var looked = 0;
        InvestigationEvidenceCandidate? after = null;
        /* Oldest first, page after page, until a page comes back short (none left pending) or the per-pass cap is reached. */
        while (looked < CandidatesPerTick)
        {
            var page = await _requests.ListEvidenceCandidatesAsync(
                now - CaptureHorizon, Math.Min(BatchRequests, CandidatesPerTick - looked), HighCpuMetric, after, cancellationToken)
                .ConfigureAwait(false);
            foreach (var candidate in page)
            {
                try
                {
                    await CaptureAsync(candidate, now, cancellationToken).ConfigureAwait(false);
                    recorded++;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Evidence capture failed for investigation {RequestId}; it is tried again next pass", candidate.RequestId);
                }
            }

            if (page.Count == 0)
                break;
            looked += page.Count;
            after = page[^1];
        }

        return recorded;
    }

    private async Task CaptureAsync(InvestigationEvidenceCandidate request, DateTime now, CancellationToken cancellationToken)
    {
        var existing = await _requests.ReadEvidenceAsync(request.RequestId, cancellationToken).ConfigureAwait(false);
        var settled = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in existing)
        {
            if (row.State != "pending")
                settled.Add(row.Section);
        }

        var age = now - request.OpenedAtUtc;
        var from = request.FirstAlertAtUtc - BreachLeadIn;
        var to = request.FirstAlertAtUtc + BreachTail;
        var writes = new List<InvestigationEvidenceWrite>();
        var slow = new HashSet<string>(StringComparer.Ordinal);
        HashSet<string>? ran = null;
        foreach (var section in Sections)
        {
            if (settled.Contains(section))
                continue;
            if (request.IsPostgres)
            {
                writes.Add(new InvestigationEvidenceWrite(section, "not_collected", null));
                continue;
            }

            /* #6001 review: ready means the section's collector COMPLETED a run at or after the window end, whether or not the run
               found anything (an empty window is a true "nothing found", not a missing collection), so rows in the window
               never freeze a section early. A collector with no run since the window end keeps the section pending until the
               grace is over, and only then is it not_collected. */
            ran ??= await ReadCollectorRunsAsync(request.ServerId, to, cancellationToken).ConfigureAwait(false);
            if (ran.Contains(SectionCollectors[section]))
            {
                var rows = await ReadSectionAsync(section, request, from, to, cancellationToken).ConfigureAwait(false);
                writes.Add(new InvestigationEvidenceWrite(section, "ready", Fit(from, to, rows.Rows, rows.DatabasesDropped)));
            }
            else
            {
                /* A slow collector (its grace is above the base) does not hold evidence_ready back, and keeps its own, longer
                   grace before it is called not_collected. */
                var grace = GraceFor(section, request.ServerId);
                if (grace > PendingGrace)
                    slow.Add(section);
                writes.Add(new InvestigationEvidenceWrite(section, age >= grace ? "not_collected" : "pending", null));
            }
        }

        if (writes.Count > 0)
        {
            await _requests.RecordEvidenceAsync(
                request.RequestId, request.ServerId, writes, slow, age >= PendingGrace, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>What one section read found: its ranked rows and, for <c>query_store</c> only, how many databases the cap left out.</summary>
    private sealed record SectionRead(List<JsonObject> Rows, int? DatabasesDropped = null);

    /// <summary>The CPU each database burned in the window, the <c>query_store</c> section's own cost figure to rank by.</summary>
    internal static readonly string WindowCpuByDatabaseSql = $@"
SELECT database_name, CAST(SUM(delta_worker_time) AS bigint) AS window_cpu_us
FROM query_stats
WHERE server_id = $1
  AND collection_time >= $2
  AND collection_time <= $3
  AND {TimescaleSupport.IntervalHonestSourceFilter}
GROUP BY database_name";

    /// <summary>The collectors (of <see cref="SectionCollectors"/>) with a completed run at or after <paramref name="since"/>.</summary>
    private async Task<HashSet<string>> ReadCollectorRunsAsync(int serverId, DateTime since, CancellationToken cancellationToken)
    {
        var ran = new HashSet<string>(StringComparer.Ordinal);
        await using var command = _store.CreateCommand(CollectorRunsSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = SectionCollectors.Values.Distinct().ToArray() });
        DarlingMcpReadParameters.AddTimestamp(command, since);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            ran.Add(reader.GetString(0));
        return ran;
    }

    private async Task<SectionRead> ReadSectionAsync(
        string section, InvestigationEvidenceCandidate request, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        section switch
        {
            "top_statements" => new SectionRead(await ReadAsync(TopStatementsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "active_requests" => new SectionRead(await ReadAsync(ActiveRequestsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "query_store" => await ReadQueryStoreAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false),
            "jobs" => new SectionRead(await ReadAsync(JobsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "waits" => new SectionRead(await ReadWaitsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            _ => new SectionRead(new List<JsonObject>()),
        };

    /// <summary>The heaviest waits in the window, through the wait tools' own statement (aliased, not copied).</summary>
    private async Task<List<JsonObject>> ReadWaitsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var command = _store.CreateCommand(DarlingDataReader.WaitStatsSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
        DarlingMcpReadParameters.AddInt(command, SectionRows);
        return await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The latest Query Store health per database, through the Query Store tools' own statement (aliased), the
    /// <see cref="QueryStoreDatabasesPerCapture"/> databases that burned the most CPU in the window first (name breaks a tie).
    /// <c>databases_dropped</c> is the count of the databases read minus the ones kept, never inferred from a full page. The
    /// health collector is hourly, so a snapshot older than the window's start by more than two hours is not this breach's
    /// state and the section is empty.
    /// </summary>
    private async Task<SectionRead> ReadQueryStoreAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var windowCpu = new Dictionary<string, long>(StringComparer.Ordinal);
        await using (var cpu = _store.CreateCommand(WindowCpuByDatabaseSql))
        {
            cpu.CommandTimeout = CommandTimeoutSeconds;
            DarlingMcpReadParameters.AddWindow(cpu, serverId, from, to);
            await using var cpuReader = await cpu.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await cpuReader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!cpuReader.IsDBNull(0) && !cpuReader.IsDBNull(1))
                    windowCpu[cpuReader.GetString(0)] = cpuReader.GetInt64(1);
            }
        }

        var all = new List<(string Name, long Cpu, JsonObject Row)>();
        await using var command = _store.CreateCommand(DarlingConfigHistoryReader.QueryStoreHealthSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var capturedAt = reader.GetDateTime(12);
            if (capturedAt < from - TimeSpan.FromHours(2))
                return new SectionRead(new List<JsonObject>(), 0);
            var name = reader.IsDBNull(0) ? "" : reader.GetString(0);
            var cpu = windowCpu.TryGetValue(name, out var micros) ? micros : 0L;
            all.Add((name, cpu, new JsonObject
            {
                ["database_name"] = reader.IsDBNull(0) ? null : name,
                ["actual_state"] = reader.IsDBNull(1) ? null : reader.GetString(1),
                ["desired_state"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                ["readonly_reason"] = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                ["window_cpu_us"] = cpu,
                ["capture_time"] = capturedAt.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
            }));
        }

        var kept = all.OrderByDescending(d => d.Cpu).ThenBy(d => d.Name, StringComparer.Ordinal)
            .Take(QueryStoreDatabasesPerCapture).Select(d => d.Row).ToList();
        return new SectionRead(kept, all.Count - kept.Count);
    }

    private async Task<List<JsonObject>> ReadAsync(
        string sql, int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var command = _store.CreateCommand(sql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
        return await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<List<JsonObject>> ReadRowsAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<JsonObject>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new JsonObject();
            for (var i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = Value(reader.IsDBNull(i) ? null : reader.GetValue(i));
            rows.Add(row);
        }

        return rows;
    }

    private static JsonValue? Value(object? value) => value switch
    {
        null => null,
        string text => JsonValue.Create(text),
        bool flag => JsonValue.Create(flag),
        byte or sbyte or short or ushort or int or uint or long or ulong => JsonValue.Create(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        float or double or decimal => JsonValue.Create(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        DateTime time => JsonValue.Create(time.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture)),
        byte[] bytes => JsonValue.Create(Convert.ToHexString(bytes)),
        _ => JsonValue.Create(Convert.ToString(value, CultureInfo.InvariantCulture)),
    };

    /// <summary>The bytes <paramref name="json"/> takes as PostgreSQL prints a jsonb (<c>octet_length(content::text)</c>, which is
    /// what the table's check holds to the cap): PostgreSQL puts a space after every structural colon and comma, so each
    /// colon and comma is counted twice, strings included, which only over-counts.</summary>
    internal static int StoredBytes(string json)
    {
        var extra = 0;
        foreach (var c in json)
        {
            if (c == ':' || c == ',')
                extra++;
        }

        return Encoding.UTF8.GetByteCount(json) + extra;
    }

    /// <summary>
    /// The section's content as JSON held to <see cref="MaxSectionBytes"/>: <c>window_start</c>, <c>window_end</c>, the rows and,
    /// when rows had to go, <c>rows_dropped</c>. Rows come off the tail (the lowest-ranked) until it fits. A section with no rows
    /// is a true "nothing found": an empty list. <paramref name="databasesDropped"/> (the <c>query_store</c> section's count of
    /// databases the cap left out) is written as <c>databases_dropped</c>, 0 included, when the section has one.
    /// </summary>
    internal static string Fit(DateTime from, DateTime to, List<JsonObject> rows, int? databasesDropped = null)
    {
        var dropped = 0;
        while (true)
        {
            var content = new JsonObject
            {
                ["window_start"] = from.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                ["window_end"] = to.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                ["rows"] = new JsonArray(rows.GetRange(0, rows.Count - dropped).ConvertAll(r => (JsonNode?)r.DeepClone()).ToArray()),
            };
            if (dropped > 0)
                content["rows_dropped"] = dropped;
            if (databasesDropped is not null)
                content["databases_dropped"] = databasesDropped;
            var json = content.ToJsonString();
            if (StoredBytes(json) <= MaxSectionBytes || dropped >= rows.Count)
                return json;
            dropped++;
        }
    }
}
