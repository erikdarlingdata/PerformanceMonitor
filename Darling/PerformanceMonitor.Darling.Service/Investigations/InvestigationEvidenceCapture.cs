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
using PerformanceMonitor.Notifications;

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
///
/// <para><b>Families (#6002).</b> The sections a request gets follow EVERY metric linked to it, not only the one it opened on
/// (<see cref="SectionsByKey"/>): High CPU brings the five above, <c>Deadlocks Detected</c> brings <c>deadlocks</c>,
/// <c>Blocking Detected</c> and <c>Blocking Wait Time</c> bring <c>blocking_chains</c> and <c>head_blocker</c>, and the
/// <c>query_performance</c> family brings <c>regression_targets</c>. A metric's sections read the window around that metric's
/// first linked alert, and their grace counts from that alert when it came after the open, so a metric that joins after the
/// other sections froze gets its sections on the next pass under the same readiness rules. The family reads live in
/// <c>InvestigationEvidenceCapture.Families.cs</c>.</para>
/// </summary>
public sealed partial class InvestigationEvidenceCapture
{
    /// <summary>The cap on one section's content, in bytes: the report evidence's cap, aliased (#3684).</summary>
    public const int MaxSectionBytes = InvestigationToolCore.MaxEvidenceBytes;

    /// <summary>The metric whose requests this capture serves.</summary>
    public const string HighCpuMetric = AlertEngine.CpuPersistenceMetric;

    /// <summary>How far before the first alert the breach window reaches. The alert fires after
    /// <see cref="AlertEngine.SqlCpuBreachSamples"/> breaching one-minute samples, so this holds the whole breach and its lead-in.</summary>
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

    /// <summary>A section's grace ends at the latest this long before <see cref="CaptureHorizon"/> (#6002 review), so a pass (every
    /// 20 seconds) still sees the request after the grace is over and settles the section: a section of a metric that joined
    /// late would otherwise keep its grace past the horizon and stay <c>pending</c> for good.</summary>
    public static readonly TimeSpan HorizonMargin = TimeSpan.FromMinutes(5);

    /// <summary>All of ONE SECTION's reads share this deadline (#6002 review, per section since round 2), on top of each command's
    /// own <see cref="CommandTimeoutSeconds"/>. A section that runs past it is settled like any failed read (pending while its
    /// grace lasts, then frozen unavailable) and the next section is read: one slow section never starves the ones after it.</summary>
    public static readonly TimeSpan RequestDeadline = TimeSpan.FromMinutes(2);

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

    /// <summary>The High CPU section names, in the order they are written.</summary>
    public static readonly IReadOnlyList<string> Sections = new[] { "top_statements", "active_requests", "query_store", "jobs", "waits" };

    /// <summary>The deadlock alert's metric (#6002).</summary>
    public const string DeadlockMetric = AlertEngine.DeadlockWatermarkMetric;

    /// <summary>The Client Timeouts alert's metric (#6553): a request opened on it gets the <c>client_timeouts</c> section.</summary>
    public const string ClientTimeoutsMetric = AttentionEvaluator.MetricName;

    /// <summary>The two blocking alerts' metrics (#6002): the blocked-process report alert and the blocking wait alert.</summary>
    public static readonly IReadOnlyList<string> BlockingMetrics = new[] { AlertEngine.BlockingWatermarkMetric, AlertEngine.BlockingWaitPersistenceMetric };

    /// <summary>The sections each scope key brings (#6002), in the order they are written: a linked metric, or a family as
    /// <see cref="InvestigationEvidenceScope.FamilyKey"/>.</summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyList<string>> SectionsByKey = BuildSectionsByKey();

    /// <summary>The scope the request store lists candidates by: every key of <see cref="SectionsByKey"/> with each of its sections.</summary>
    public static readonly InvestigationEvidenceScope Scope = new(
        SectionsByKey.SelectMany(k => k.Value.Select(_ => k.Key)).ToArray(),
        SectionsByKey.SelectMany(k => k.Value).ToArray());

    private static Dictionary<string, IReadOnlyList<string>> BuildSectionsByKey()
    {
        var blocking = new[] { "blocking_chains", "head_blocker" };
        var map = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            [HighCpuMetric] = Sections,
            [DeadlockMetric] = new[] { "deadlocks" },
            [ClientTimeoutsMetric] = new[] { "client_timeouts" },
            [InvestigationEvidenceScope.FamilyKey(InvestigationFamily.QueryPerformance)] = new[] { "regression_targets" },
        };
        foreach (var metric in BlockingMetrics)
            map[metric] = blocking;
        return map;
    }

    /// <summary>The collector (<c>collect.collection_log.collector_name</c>) that feeds each section. A section is ready when its
    /// collector has completed a run at or after the window end, whether or not that run found anything.</summary>
    public static readonly IReadOnlyDictionary<string, string> SectionCollectors = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["top_statements"] = "query_stats",
        ["active_requests"] = "query_snapshots",
        ["query_store"] = "query_store_health",
        ["jobs"] = "running_jobs",
        ["waits"] = "wait_stats",
        ["deadlocks"] = "deadlocks",
        ["blocking_chains"] = "blocked_process_report",
        ["head_blocker"] = "blocked_process_report",
        ["regression_targets"] = "query_store",
        ["client_timeouts"] = AttentionWindowSql.CollectorName,
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

    /// <summary>#6007: how many plans one top statement keeps in its <c>plans</c> list: the heaviest by CPU, plus the cheapest per
    /// execution when that is not among them, so a plan change that made a query costlier shows both sides.</summary>
    public const int PlansPerStatement = 3;

    /* #6007: the same rows as before, grouped by plan first and by query second, in the one read. Each row keeps every field it had
       (query_plan_hash is still the query's MAX hash) and gains `plans`: per plan its hash, executions, CPU, CPU per execution and the
       first collection_time seen in the window. */
    internal static readonly string TopStatementsSql = $@"
WITH per_plan AS (
    SELECT database_name, query_hash, query_plan_hash,
           CAST(SUM(delta_worker_time) AS bigint) AS cpu_us,
           CAST(SUM(delta_execution_count) AS bigint) AS executions,
           CAST(SUM(delta_elapsed_time) AS bigint) AS elapsed_us,
           CAST(SUM(delta_logical_reads) AS bigint) AS reads,
           MIN(collection_time) AS first_seen
    FROM query_stats
    WHERE server_id = $1
      AND collection_time >= $2
      AND collection_time <= $3
      AND {TimescaleSupport.IntervalHonestSourceFilter}
    GROUP BY database_name, query_hash, query_plan_hash
), top_queries AS (
    SELECT database_name, query_hash, MAX(query_plan_hash)::text AS query_plan_hash,
           CAST(SUM(cpu_us) AS bigint) AS total_cpu_us,
           CAST(SUM(executions) AS bigint) AS executions,
           CAST(SUM(elapsed_us) AS bigint) AS total_elapsed_us,
           CAST(SUM(reads) AS bigint) AS total_reads
    FROM per_plan
    GROUP BY database_name, query_hash
    HAVING SUM(cpu_us) > 0
    ORDER BY SUM(cpu_us) DESC NULLS LAST, database_name, query_hash
    LIMIT {TopStatements}
), ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY p.database_name, p.query_hash ORDER BY p.cpu_us DESC NULLS LAST, p.query_plan_hash) AS heaviest,
           ROW_NUMBER() OVER (PARTITION BY p.database_name, p.query_hash
                              ORDER BY p.cpu_us::numeric / NULLIF(p.executions, 0) ASC NULLS LAST, p.query_plan_hash) AS cheapest
    FROM per_plan AS p
    JOIN top_queries AS t ON t.database_name IS NOT DISTINCT FROM p.database_name AND t.query_hash IS NOT DISTINCT FROM p.query_hash
)
SELECT t.database_name, t.query_hash::text AS query_hash, t.query_plan_hash, t.total_cpu_us, t.executions, t.total_elapsed_us, t.total_reads,
       (SELECT jsonb_agg(jsonb_build_object(
                   'query_plan_hash', r.query_plan_hash::text,
                   'executions', r.executions,
                   'cpu_us', r.cpu_us,
                   'cpu_per_exec_us', round(r.cpu_us::numeric / NULLIF(r.executions, 0), 1),
                   'first_seen', to_char(r.first_seen, 'YYYY-MM-DD""T""HH24:MI:SS""Z""')) ORDER BY r.first_seen, r.query_plan_hash)
        FROM ranked AS r
        WHERE r.database_name IS NOT DISTINCT FROM t.database_name AND r.query_hash IS NOT DISTINCT FROM t.query_hash
          AND (r.heaviest <= {PlansPerStatement} OR r.cheapest = 1)) AS plans
FROM top_queries AS t
ORDER BY t.total_cpu_us DESC NULLS LAST, t.database_name, t.query_hash";

    /// <summary>How far two snapshots' derived request start times (snapshot time minus the request's elapsed time) may differ
    /// before they are two requests. <c>collect.query_snapshots</c> does not carry the request's own start time (only the
    /// transaction start, which is not the request's), so the start is derived. A snapshot's <c>collection_time</c> is stamped
    /// from the service clock BEFORE the store read, the connection open and the DMV query, so the derived start of one request
    /// is off by that collector latency, and it differs from run to run. 30 seconds covers a slow monitored server and stays
    /// under the one-minute snapshot interval, so a new batch on the same session still reads as a new request. The exact
    /// identity needs the request start time collected.</summary>
    public static readonly TimeSpan RequestStartTolerance = TimeSpan.FromSeconds(30);

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
                WHEN s.started_at > LAG(s.started_at) OVER w + interval '{(int)RequestStartTolerance.TotalSeconds} seconds' THEN 1
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
    private readonly TimeSpan _sectionDeadline;

    /// <param name="store">The monitoring store's data source: the only connection the capture holds.</param>
    /// <param name="requests">The request store, over the same store (it should share <paramref name="utcNow"/>).</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="utcNow">The one clock the grace is judged by (a test supplies its own).</param>
    /// <param name="collectorIntervalMinutes">A collector's configured interval in minutes on one server, as (collector name, server id):
    /// the worker passes the schedule it runs the collector by, overrides included. Without it the code defaults are used.</param>
    /// <param name="sectionDeadline">How long one section's reads may take (default <see cref="RequestDeadline"/>; a test supplies a short one).</param>
    public InvestigationEvidenceCapture(
        NpgsqlDataSource store, InvestigationRequestStore requests, ILogger logger, Func<DateTime>? utcNow = null,
        Func<string, int, int>? collectorIntervalMinutes = null, TimeSpan? sectionDeadline = null)
    {
        _sectionDeadline = sectionDeadline ?? RequestDeadline;
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
    /// <see cref="PendingGrace"/>, or <see cref="SlowCollectorGraceMultiplier"/> of the collector's own interval when that is longer,
    /// and never more than <see cref="CaptureHorizon"/>: a request older than that is no longer read, so a longer grace would leave
    /// the section <c>pending</c> for good.</summary>
    public TimeSpan GraceFor(string section, int serverId)
    {
        var interval = TimeSpan.FromMinutes(_collectorIntervalMinutes(SectionCollectors[section], serverId));
        var slow = TimeSpan.FromTicks(interval.Ticks * SlowCollectorGraceMultiplier);
        var grace = slow > PendingGrace ? slow : PendingGrace;
        return grace > CaptureHorizon ? CaptureHorizon : grace;
    }

    /// <summary>One pass: every request in <see cref="Scope"/> still needing evidence gets its sections looked at. A failed
    /// request is logged and the next is tried. Returns how many requests it recorded.</summary>
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
                now - CaptureHorizon, Math.Min(BatchRequests, CandidatesPerTick - looked), Scope, after, cancellationToken)
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

        var writes = new List<InvestigationEvidenceWrite>();
        var slow = new HashSet<string>(StringComparer.Ordinal);
        var runsByWindowEnd = new Dictionary<DateTime, HashSet<string>>();
        /* #6002 review round 2: once the request is inside the last HorizonMargin of its horizon this is the final pass that can see
           it, so every section still unsettled is frozen on this pass. A section whose grace starts late (a late or skewed link)
           can otherwise still be pending, with a negative age, when the request ages out of the candidate query for good. */
        var finalPass = now >= request.OpenedAtUtc + CaptureHorizon - HorizonMargin;
        foreach (var (section, anchor) in SectionsOf(request))
        {
            if (settled.Contains(section))
                continue;

            /* #6002: each section reads the window around its own key's first alert, and its grace counts from that alert when
               it came after the open (a metric that joined later), so a joined family is judged on its own window. */
            var from = anchor - BreachLeadIn;
            var to = anchor + BreachTail;
            var graceStart = anchor > request.OpenedAtUtc ? anchor : request.OpenedAtUtc;
            var age = now - graceStart;
            if (request.IsPostgres)
            {
                writes.Add(new InvestigationEvidenceWrite(section, "not_collected", null));
                continue;
            }

            var grace = CappedGrace(GraceFor(section, request.ServerId), graceStart, request.OpenedAtUtc);
            var over = finalPass || age >= grace;

            /* #6002 review round 2: each section has its own deadline, so one that runs past it is settled below like any other
               failed read and the loop goes on to the next section. */
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(_sectionDeadline);
            var readToken = deadline.Token;
            try
            {

            /* #6001 review: ready means the section's collector COMPLETED a run at or after the window end, whether or not the run
               found anything (an empty window is a true "nothing found", not a missing collection), so rows in the window
               never freeze a section early. A collector with no run since the window end keeps the section pending until the
               grace is over, and only then is it not_collected. */
                if (!runsByWindowEnd.TryGetValue(to, out var ran))
                {
                    ran = await ReadCollectorRunsAsync(request.ServerId, to, readToken).ConfigureAwait(false);
                    runsByWindowEnd[to] = ran;
                }

                if (ran.Contains(SectionCollectors[section]))
                {
                    var rows = await ReadSectionAsync(section, request, from, to, readToken).ConfigureAwait(false);
                    var scrubbed = ScrubRows(rows.Rows, readToken);
                    writes.Add(new InvestigationEvidenceWrite(section, "ready", rows.ReportXml is null
                        ? Fit(from, to, scrubbed, rows.DatabasesDropped)
                        : FitReportXml(from, to, scrubbed, rows.ReportXml)));
                }
                else
                {
                    /* A slow collector (its grace is above the base) does not hold evidence_ready back, and keeps its own, longer
                       grace before it is called not_collected. */
                    if (grace > PendingGrace)
                        slow.Add(section);
                    writes.Add(new InvestigationEvidenceWrite(section, over ? "not_collected" : "pending", null));
                }
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                /* #6002 review: one section that fails does not cost the others their write. It is read again on the next pass
                   while its grace lasts, and is then frozen as unavailable with the error's class (never its message, which can
                   carry collected text). A section that ran past its own deadline is the same, whatever the read threw: a
                   cancelled command can surface as a PostgresException (57014) instead of an OperationCanceledException, so the
                   deadline token decides, and it is logged and recorded as a timeout, never as a fault of the read. */
                var timedOut = deadline.IsCancellationRequested;
                if (timedOut)
                {
                    _logger.LogWarning(
                        "Evidence section {Section} for investigation {RequestId} ran past its {Deadline} deadline; the next section is read",
                        section, request.RequestId, _sectionDeadline);
                }
                else
                {
                    _logger.LogWarning(ex, "Evidence section {Section} failed for investigation {RequestId}", section, request.RequestId);
                }

                if (grace > PendingGrace)
                    slow.Add(section);
                writes.Add(over
                    ? new InvestigationEvidenceWrite(section, "ready", Unavailable(from, to, timedOut ? new TimeoutException() : ex))
                    : new InvestigationEvidenceWrite(section, "pending", null));
            }
        }

        if (writes.Count > 0)
        {
            await _requests.RecordEvidenceAsync(
                request.RequestId, request.ServerId, writes, slow, now - request.OpenedAtUtc >= PendingGrace, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>A section's grace, ended no later than <see cref="HorizonMargin"/> before the request's
    /// <see cref="CaptureHorizon"/>: the grace counts from <paramref name="graceStart"/>, which is after the open for a metric
    /// that joined late (#6002 review).</summary>
    internal static TimeSpan CappedGrace(TimeSpan grace, DateTime graceStart, DateTime openedAt)
    {
        var latest = openedAt + CaptureHorizon - HorizonMargin - graceStart;
        if (latest < TimeSpan.Zero)
            latest = TimeSpan.Zero;
        return grace > latest ? latest : grace;
    }

    /// <summary>The content of a section whose read failed past its grace: no rows, <c>status: unavailable</c>, and the error's
    /// class name only.</summary>
    internal static string Unavailable(DateTime from, DateTime to, Exception error) => new JsonObject
    {
        ["window_start"] = from.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
        ["window_end"] = to.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
        ["rows"] = new JsonArray(),
        ["status"] = "unavailable",
        ["error"] = error.GetType().Name,
    }.ToJsonString();

    /// <summary>
    /// #4348 (#6002 review): each row through <see cref="DerivedStoreScrub.Json"/> BEFORE it is fitted, so the bytes the cap
    /// measures are the bytes stored (the store runs the same scrub at the write, and finds these rows clean). A row the filter
    /// cannot read is kept as <c>{"withheld": true}</c>, so the rows stay aligned with any report XML beside them.
    /// </summary>
    internal static List<JsonObject> ScrubRows(List<JsonObject> rows, CancellationToken cancellationToken = default) =>
        rows.ConvertAll(row =>
        {
            /* #6002 review round 2: the statement filter is synchronous work (up to its own 10 s per call), so a section past its
               deadline stops between rows instead of finishing every row first. */
            cancellationToken.ThrowIfCancellationRequested();
            var json = row.ToJsonString();
            var scrubbed = DerivedStoreScrub.Json(json);
            if (ReferenceEquals(scrubbed, json))
                return row;
            return scrubbed is null ? new JsonObject { ["withheld"] = true } : JsonNode.Parse(scrubbed)!.AsObject();
        });

    /// <summary>
    /// The sections <paramref name="request"/> gets (#6002), each with the alert time its window is built around: the sections of
    /// every linked metric in <see cref="SectionsByKey"/> (at that metric's first linked alert; the opening metric at the
    /// request's first alert) and of its family (at the first alert). A section two keys bring (the two blocking metrics) is
    /// listed once, at the earlier alert. High CPU's sections come first, in their written order.
    /// </summary>
    internal static IReadOnlyList<(string Section, DateTime Anchor)> SectionsOf(InvestigationEvidenceCandidate request)
    {
        var anchors = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        var order = new List<string>();
        void Add(string key, DateTime anchor)
        {
            if (!SectionsByKey.TryGetValue(key, out var sections))
                return;
            foreach (var section in sections)
            {
                if (!anchors.TryGetValue(section, out var seen))
                {
                    order.Add(section);
                    anchors[section] = anchor;
                }
                else if (anchor < seen)
                {
                    anchors[section] = anchor;
                }
            }
        }

        Add(request.FirstMetric, request.FirstAlertAtUtc);
        foreach (var key in SectionsByKey.Keys)
        {
            if (request.Metrics is not null && request.Metrics.TryGetValue(key, out var first))
                Add(key, string.Equals(key, request.FirstMetric, StringComparison.Ordinal) ? request.FirstAlertAtUtc : first);
        }

        Add(InvestigationEvidenceScope.FamilyKey(request.Family), request.FirstAlertAtUtc);
        return order.ConvertAll(s => (s, anchors[s]));
    }

    /// <summary>What one section read found: its ranked rows and, for <c>query_store</c> only, how many databases the cap left out.</summary>
    /// <remarks><c>blocking_chains</c> (#6002) also returns each row's report XML, aligned with the rows, for <see cref="FitReportXml"/>.</remarks>
    private sealed record SectionRead(List<JsonObject> Rows, int? DatabasesDropped = null, IReadOnlyList<string?>? ReportXml = null);

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
            "top_statements" => new SectionRead(ParsePlans(await ReadAsync(TopStatementsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false))),
            "active_requests" => new SectionRead(await ReadAsync(ActiveRequestsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "query_store" => await ReadQueryStoreAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false),
            "jobs" => new SectionRead(await ReadAsync(JobsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "waits" => new SectionRead(await ReadWaitsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "deadlocks" => new SectionRead(await ReadDeadlocksAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "blocking_chains" => await ReadBlockingChainsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false),
            "head_blocker" => new SectionRead(await ReadHeadBlockersAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "client_timeouts" => new SectionRead(await ReadClientTimeoutsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "regression_targets" => new SectionRead(await ReadRegressionTargetsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
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

    /// <summary>#6007: the <c>plans</c> column arrives as jsonb text; it becomes a JSON array on the row (null when the query has none).</summary>
    internal static List<JsonObject> ParsePlans(List<JsonObject> rows)
    {
        foreach (var row in rows)
        {
            if (row["plans"] is JsonValue value && value.TryGetValue<string>(out var text))
                row["plans"] = JsonNode.Parse(text);
        }

        return rows;
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
            /* #6002: a ready section that found nothing says so: a frozen true negative, never a dropped section. */
            if (rows.Count == 0)
                content["status"] = "empty";
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
