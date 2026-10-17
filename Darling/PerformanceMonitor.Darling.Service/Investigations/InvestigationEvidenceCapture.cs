using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;
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
/// hash and never XML), <c>active_requests</c> (sessions by host and program), <c>query_store</c> (the latest Query Store
/// health), <c>jobs</c> and <c>waits</c>.
///
/// <para><b>States.</b> A section with no rows yet is <c>pending</c> and is looked at again every pass until
/// <see cref="PendingGrace"/> after the request opened (the collectors run every one to fifteen minutes); one still empty then
/// is <c>not_collected</c>. A section with rows is <c>ready</c> and FROZEN: the store never rewrites it (a later collection does
/// not change the frozen statements). A PostgreSQL target has none of these SQL Server collectors, so its sections are
/// <c>not_collected</c> at once. The store journals <c>evidence_ready</c> once per request when no section is pending
/// (<see cref="InvestigationRequestStore.RecordEvidenceAsync"/>); #6003 delivers it.</para>
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

    /// <summary>A request older than this is left as it is: its window is long past.</summary>
    public static readonly TimeSpan CaptureHorizon = TimeSpan.FromHours(6);

    /// <summary>Statements kept in <c>top_statements</c>.</summary>
    public const int TopStatements = 10;

    /// <summary>Rows kept in every other section.</summary>
    public const int SectionRows = 10;

    private const int CommandTimeoutSeconds = 30;
    private const int BatchRequests = 25;

    /// <summary>The section names, in the order they are written.</summary>
    public static readonly IReadOnlyList<string> Sections = new[] { "top_statements", "active_requests", "query_store", "jobs", "waits" };

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

    internal const string ActiveRequestsSql = @"
SELECT host_name, program_name, count(*) AS samples, count(DISTINCT session_id) AS sessions,
       CAST(SUM(cpu_time_ms) AS bigint) AS total_cpu_ms, CAST(MAX(cpu_time_ms) AS bigint) AS max_cpu_ms
FROM query_snapshots
WHERE server_id = $1
  AND collection_time >= $2
  AND collection_time <= $3
GROUP BY host_name, program_name
ORDER BY SUM(cpu_time_ms) DESC NULLS LAST, host_name, program_name
LIMIT 10";

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

    /// <param name="store">The monitoring store's data source: the only connection the capture holds.</param>
    /// <param name="requests">The request store, over the same store (it should share <paramref name="utcNow"/>).</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="utcNow">The one clock the grace is judged by (a test supplies its own).</param>
    public InvestigationEvidenceCapture(
        NpgsqlDataSource store, InvestigationRequestStore requests, ILogger logger, Func<DateTime>? utcNow = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _requests = requests ?? throw new ArgumentNullException(nameof(requests));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>One pass: every High CPU request still needing evidence gets its sections looked at. A failed request is
    /// logged and the next is tried. Returns how many requests it recorded.</summary>
    public async Task<int> CaptureOnceAsync(CancellationToken cancellationToken)
    {
        var now = _utcNow();
        var candidates = await _requests.ListEvidenceCandidatesAsync(now - CaptureHorizon, BatchRequests, HighCpuMetric, cancellationToken)
            .ConfigureAwait(false);
        var recorded = 0;
        foreach (var candidate in candidates)
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

        var expired = now - request.OpenedAtUtc >= PendingGrace;
        var from = request.FirstAlertAtUtc - BreachLeadIn;
        var to = request.FirstAlertAtUtc + BreachTail;
        var writes = new List<InvestigationEvidenceWrite>();
        foreach (var section in Sections)
        {
            if (settled.Contains(section))
                continue;
            if (request.IsPostgres)
            {
                writes.Add(new InvestigationEvidenceWrite(section, "not_collected", null));
                continue;
            }

            var rows = await ReadSectionAsync(section, request, from, to, cancellationToken).ConfigureAwait(false);
            if (rows is { Count: > 0 })
                writes.Add(new InvestigationEvidenceWrite(section, "ready", Fit(from, to, rows)));
            else
                writes.Add(new InvestigationEvidenceWrite(section, expired ? "not_collected" : "pending", null));
        }

        if (writes.Count > 0)
            await _requests.RecordEvidenceAsync(request.RequestId, request.ServerId, writes, cancellationToken).ConfigureAwait(false);
    }

    private Task<List<JsonObject>?> ReadSectionAsync(
        string section, InvestigationEvidenceCandidate request, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        section switch
        {
            "top_statements" => ReadAsync(TopStatementsSql, request.ServerId, from, to, cancellationToken),
            "active_requests" => ReadAsync(ActiveRequestsSql, request.ServerId, from, to, cancellationToken),
            "query_store" => ReadQueryStoreAsync(request.ServerId, from, cancellationToken),
            "jobs" => ReadAsync(JobsSql, request.ServerId, from, to, cancellationToken),
            "waits" => ReadWaitsAsync(request.ServerId, from, to, cancellationToken),
            _ => Task.FromResult<List<JsonObject>?>(null),
        };

    /// <summary>The heaviest waits in the window, through the wait tools' own statement (aliased, not copied).</summary>
    private async Task<List<JsonObject>?> ReadWaitsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var command = _store.CreateCommand(DarlingDataReader.WaitStatsSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
        DarlingMcpReadParameters.AddInt(command, SectionRows);
        return await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The latest Query Store health per database, through the Query Store tools' own statement (aliased). The health
    /// collector is hourly, so a snapshot older than the window's start by more than two hours is not this breach's state.</summary>
    private async Task<List<JsonObject>?> ReadQueryStoreAsync(int serverId, DateTime from, CancellationToken cancellationToken)
    {
        await using var command = _store.CreateCommand(DarlingConfigHistoryReader.QueryStoreHealthSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        var rows = new List<JsonObject>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && rows.Count < SectionRows)
        {
            var capturedAt = reader.GetDateTime(12);
            if (capturedAt < from - TimeSpan.FromHours(2))
                return null;
            rows.Add(new JsonObject
            {
                ["database_name"] = reader.IsDBNull(0) ? null : reader.GetString(0),
                ["actual_state"] = reader.IsDBNull(1) ? null : reader.GetString(1),
                ["desired_state"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                ["readonly_reason"] = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                ["capture_time"] = capturedAt.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
            });
        }

        return rows;
    }

    private async Task<List<JsonObject>?> ReadAsync(
        string sql, int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var command = _store.CreateCommand(sql);
        command.CommandTimeout = CommandTimeoutSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, from, to);
        return await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<List<JsonObject>?> ReadRowsAsync(NpgsqlCommand command, CancellationToken cancellationToken)
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
    /// when rows had to go, <c>rows_dropped</c>. Rows come off the tail (the lowest-ranked) until it fits.
    /// </summary>
    internal static string Fit(DateTime from, DateTime to, List<JsonObject> rows)
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
            var json = content.ToJsonString();
            if (StoredBytes(json) <= MaxSectionBytes || dropped >= rows.Count)
                return json;
            dropped++;
        }
    }
}
