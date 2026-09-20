/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Provides time-bucketed baselines (hour-of-day x day-of-week) computed from
/// 30-day rolling history in Darling's Postgres store — Lite's BaselineProvider
/// (Lite/Analysis/BaselineProvider.cs) ported for the analysis slice AN2b, reading
/// the V4 passthrough views so the metric SQL stays Lite-verbatim wherever the
/// dialects agree.
///
/// <para>
/// Each baseline bucket contains mean, stddev, and sample count for a metric
/// at a specific (hour, day-of-week) combination. When a bucket has insufficient
/// samples, the provider collapses to less-specific tiers:
///   Full (hour+dow) -> Hour-only -> Flat (global mean/stddev)
/// Baselines are cached in memory with a 1-hour TTL to avoid redundant
/// recomputation during rapid re-analysis. Collapse math, cache keys, and the
/// public surface are Lite's, line-for-line.
/// </para>
///
/// <para>
/// Postgres discipline (see PgFindingStore): the window bounds are bound
/// naive-UTC Kind-Unspecified parameters ($1 server_id, $2 window start,
/// $3 analysis time) — never a bare <c>now()</c>/<c>CURRENT_TIMESTAMP</c>, which
/// would be timestamptz and compare in the PG server's time zone. Lite's SQL
/// already parameterized every bound, so no "now" replacement was needed here.
/// </para>
///
/// <para>
/// <b>The bucket key is the TARGET's local hour-of-week, not UTC (#3653 item 12, Q6).</b> <c>collection_time</c>
/// is the service host's <c>DateTime.UtcNow</c>, and keying <c>EXTRACT(HOUR/DOW FROM collection_time)</c> on it
/// pooled two local hours into one bucket across a DST change — the "Tue 22:00" a finding named was 17:00 on the
/// server in winter and 18:00 in summer. Three more parameters follow the window bounds: <c>$4</c> the offset
/// transition inside the window (naive UTC; the window end when there is none), <c>$5</c>/<c>$6</c> the offset
/// minutes before/after it, resolved ONCE per compute by <see cref="BaselineLocalClock"/> from the newest
/// <c>server_properties</c> row's <c>time_zone_id</c> (preferred — it knows WHEN the offset changed) or
/// <c>utc_offset_minutes</c> (a fixed shift), and 0/0 — today's UTC keying — when the server has no row. A
/// PostgreSQL target has no <c>server_properties</c> row at all, so <see cref="PgTargetBaselineProvider"/> overrides
/// the clock READ (<see cref="ReadServerClockAsync"/>, #3691) to hand the same resolver the target's own
/// <c>TimeZone</c> setting. <see cref="RobustTierScaffold"/> and the two event-family arms extract from
/// <see cref="BaselineLocalClock.LocalCollectionTimeSql"/>, and
/// <see cref="GetBaselineAsync(int, string, DateTime, CancellationToken)"/> looks the analysis
/// time up through the SAME three numbers (<see cref="LocalClockWindow.LocalKey"/>), cached beside the buckets.
/// Nothing keyed is stored, so the re-bucketing the ruling asks for is the next compute after the cache
/// expires. Neither PostgreSQL nor Npgsql rejects a statement that ignores <c>$4..$6</c> (measured), so an arm
/// that bypassed the scaffold would key on UTC silently — <c>LocalClockBucketKeyTests</c>' local-clock census
/// is what forbids it.
/// </para>
///
/// <para>
/// <b>A series may be scoped to one MEMBER of a population (#3691 lane 33).</b> The five-argument
/// <see cref="GetBaselineAsync(int, string, string?, DateTime, CancellationToken)"/> keys a bucket map on (server,
/// metric, key) — the key a <c>queryid</c> as text for the first consumer, a statement's own hour-of-week share of
/// the server's execution time — resolved through the THIRD seam, <see cref="ResolveKeyedBaselineQuery"/>, and bound
/// as <c>$7</c> after the clock parameters. The four-argument overload is the unkeyed series, byte-identical to what
/// it was, and the base declares no keyed metric: the SQL Server store's arms are all population-wide.
/// </para>
///
/// <para>
/// The arc's only genuine dialect work lives in <see cref="GetBaselineQuery"/>:
/// DuckDB's QUALIFY clause (used at four sites for restart-poisoning / rate
/// exclusion when this port was made; three since #3653 retired Lite's
/// BatchRequests heuristic — the rewrite numbering below is historical) does
/// not exist in Postgres. Each site is rewritten as
/// window-function-in-a-CTE + the identical predicate in an OUTER where — the
/// same idiom the Dashboard twin (SqlServerBaselineProvider) uses for T-SQL —
/// with the original DuckDB form preserved in a comment block and the
/// row-selection equivalence argued site-by-site.
/// </para>
///
/// <para>
/// <b>Two supplies for the perfmon and wait-stats families (#3653, A6/A10).</b> Their baseline
/// aggregates were replaced by interval-honest successors (<c>perfmon_interval_baseline</c>,
/// <c>wait_stats_interval_baseline</c>: <c>sample_interval_seconds IS DISTINCT FROM 0</c> baked in and
/// the measured interval carried), but a successor created <c>WITH NO DATA</c> and backfilled from raw
/// starts with less history than the legacy aggregate holds. So <see cref="GetBaselineQuery"/> is
/// written against the SUCCESSOR, <see cref="GetLegacyBaselineQuery"/> keeps the pre-#3653 text against
/// the legacy relation, and <see cref="ChooseSupplyAsync"/> picks per compute: the successor whenever it
/// reaches as far back into this server's window as the legacy does (or the legacy is gone), the legacy
/// otherwise. The choice is a catalog read plus two indexed <c>min(bucket)</c> probes, bounded by the
/// 1-hour bucket cache above it; the retirement that ends the choice is
/// <c>TimescaleSupport.SupersededBaselineRelations</c>.
/// </para>
///
/// <para>
/// <b>The three-state rule the successor arms apply</b> (Lite's since #3540): a stored interval of 0 is
/// the collector saying "no delta knowable" (first sighting, counter reset — a restart — or a gap past
/// the policy) and the aggregate's WHERE has already dropped it; NULL is a pre-column collection whose
/// interval was never recorded, kept, and for it the <c>LAG &gt; N</c> magnitude heuristic remains the
/// only restart guard there is; n is a measured interval and is AUTHORITATIVE — the row stays whatever
/// its magnitude, because a 0 over a measured interval is a real idle sample, not a restart. The
/// heuristic is therefore gated on <c>sample_interval_seconds IS NULL</c> at every successor site rather
/// than deleted: the fleet's raw tables carried NULL-interval rows until the V127/V128 rows aged out, and
/// a bring-your-own store may still. <c>PlanCacheAnomalyDetector.IsRealDeltaRow</c> is the same predicate
/// stated over in-memory rows (a prior row for the same key after this row's <c>server_start_time</c>);
/// here the collector's stored verdict is the stronger witness, because it saw the counter go backwards
/// and the detector can only infer it, so the SQL tier reads the column and the in-memory tier keeps the
/// predicate.
/// </para>
/// </summary>
public class PgBaselineProvider
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;
    private readonly BaselineLocalClock _localClock;

    /// <summary>Cache TTL — baselines are recomputed after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private readonly ConcurrentDictionary<string, CachedBaseline> _cache = new();

    /* #3691 lane 33: the keyed-cardinality note's once-per-cache-period gate (NoteKeyedCardinality). */
    private readonly object _keyedWarnGate = new();
    private DateTime? _keyedCardinalityWarnedAt;

    public PgBaselineProvider(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
        /* Information, not Warning: "this host cannot resolve that zone id" is a statement about the host's
           configuration, made once per zone per process by the resolver itself — see BaselineLocalClock. */
        _localClock = new BaselineLocalClock(message => _logger?.LogInformation("{Message}", message));
    }

    /// <summary>
    /// Gets the baseline for a specific metric, server, and time bucket.
    /// Returns the most specific bucket available, collapsing as needed.
    /// <para>The UNKEYED series — one per (server, metric), every caller and pin that existed before #3691 lane 33
    /// byte-identical: this overload delegates to the keyed one with <c>key: null</c>, which resolves through
    /// <see cref="ResolveBaselineQuery"/> exactly as before and binds exactly the six parameters it always bound.</para>
    /// </summary>
    public Task<BaselineBucket> GetBaselineAsync(
        int serverId, string metricName, DateTime analysisTime, CancellationToken cancellationToken = default)
        => GetBaselineAsync(serverId, metricName, key: null, analysisTime, cancellationToken);

    /// <summary>
    /// The KEYED series (#3691 lane 33): one hour×day-of-week bucket map per (server, metric, <paramref name="key"/>),
    /// for a baseline that is scoped to ONE member of a population — the first consumer is a statement's own
    /// hour-of-week share of the server's execution time, keyed by its <c>queryid</c> as text (lane 34), by Erik's
    /// 2026-09-20 ruling that the bad-actor share is graded as deviation from the statement's OWN baseline, which is
    /// impossible with one series per (server, metric).
    ///
    /// <para><b>Mechanism.</b> The key is a third segment of the cache key (<see cref="CacheKeyFor"/>), so two keys
    /// are two independent computes and two cache entries with their own TTL; the SQL is resolved through
    /// <see cref="ResolveKeyedBaselineQuery"/> — a SEPARATE seam from <see cref="ResolveBaselineQuery"/>, so a
    /// provider declares which of its metrics have a keyed shape and which do not (the base declares none: the SQL
    /// Server store has no keyed arm, and a keyed call for any metric there is "no baseline", never a server-wide
    /// bucket mistaken for a member's) — and the key is bound as <c>$7</c>, AFTER the six the unkeyed compute binds
    /// (<c>$1</c> server, <c>$2</c>/<c>$3</c> window, <c>$4..$6</c> the Q6 clock), so a keyed arm is an ordinary arm
    /// ending in <c>clean(collection_time, v)</c> + <see cref="RobustTierScaffold"/> whose own text adds
    /// <c>queryid = $7::BIGINT</c> (or whatever its dimension is) and inherits the local-clock key, the eight-column
    /// reader, the timeout classification and the degrade-to-empty posture unchanged. Neither PostgreSQL nor Npgsql
    /// complains about a bound parameter a statement does not reference (measured in #3749), which is why the
    /// census in <c>LocalClockBucketKeyTests</c> is TWO-ARMED: an unkeyed statement references exactly <c>$1..$6</c>,
    /// a keyed one exactly <c>$1..$7</c> — a keyed arm that forgot the clock would otherwise key on UTC silently, and
    /// an unkeyed arm that named <c>$7</c> would fail at execution on every pass.</para>
    ///
    /// <para><b>Cardinality, stated honestly.</b> A keyed series is one cache entry per (server, metric, key) and one
    /// 30-day scan per entry per <see cref="CacheTtl"/>. The provider refuses nothing — it cannot know which keys
    /// matter — so the CONSUMER bounds the population: a detector or scorer calling this overload asks only for the
    /// TOP-N members of its window (lane 34 asks for the top-share candidates the bad-actor read already returns,
    /// never every statement the server ran), and says so in its doc. What the provider does is make a runaway
    /// consumer visible: when the keyed entries in the cache exceed <see cref="KeyedBaselineCacheWarnCount"/> it
    /// logs the count once per cache period (<see cref="ShouldWarnKeyedCardinality"/>), which is the closest thing
    /// this class has to "once per pass" — within one TTL a pass's repeat lookups are cache hits that compute
    /// nothing.</para>
    ///
    /// <para><b>What it does not do.</b> No key travels into <c>BaselineBucket</c> or <c>BaselineMath</c> — the
    /// bucket a keyed call returns is the same shape an unkeyed one returns, and the caller that passed the key is the
    /// one that knows what it belongs to. The key is a string so the seam is engine- and dimension-neutral (a
    /// <c>queryid</c> today, a database name or a wait type tomorrow); an arm casts it to its own column's type.</para>
    /// </summary>
    public async Task<BaselineBucket> GetBaselineAsync(
        int serverId, string metricName, string? key, DateTime analysisTime, CancellationToken cancellationToken = default)
    {
        var cached = await GetOrComputeBaselinesAsync(serverId, metricName, key, analysisTime, cancellationToken);
        var baselines = cached.Buckets;
        if (baselines == null || baselines.Count == 0)
            return BaselineBucket.Empty;

        /* #3653 Q6: the lookup key is the analysis time on the TARGET's clock, through the same three numbers
           the SQL keyed the buckets with (cached beside them, so a cache hit and its lookup agree even if the
           server's stored clock changed since) — Sunday=0 matches EXTRACT(DOW) in both engines. */
        var (hourOfDay, dayOfWeek) = cached.Clock.LocalKey(analysisTime);

        return BaselineMath.SelectBucket(baselines, hourOfDay, dayOfWeek);
    }

    /// <summary>Forces cache eviction for a server — used during testing.</summary>
    public void InvalidateCache(int serverId)
    {
        var keysToRemove = _cache.Keys.Where(k => k.StartsWith($"{serverId}:", StringComparison.Ordinal)).ToList();
        foreach (var key in keysToRemove)
            _cache.TryRemove(key, out _);
    }

    /// <summary>Forces full cache clear — used during testing.</summary>
    public void ClearCache() => _cache.Clear();

    /// <summary>
    /// The cache identity of a series (#3691 lane 33): <c>{server}:{metric}</c> for the unkeyed series — the exact
    /// string it has always been, so <see cref="InvalidateCache"/>'s <c>{server}:</c> prefix sweep keeps finding it —
    /// and <c>{server}:{metric}:{key}</c> for a keyed one, which the same prefix sweep also finds. Two keys are two
    /// entries; a keyed and an unkeyed series of the same metric are two entries. A key is never empty here: the
    /// overload treats <c>""</c> as a key like any other, because "no key" is spelled <c>null</c> and nothing else.
    /// </summary>
    internal static string CacheKeyFor(int serverId, string metricName, string? key)
        => key is null ? $"{serverId}:{metricName}" : $"{serverId}:{metricName}:{key}";

    /// <summary>
    /// The keyed-entry count above which the provider logs the cache's size (#3691 lane 33) — a runaway consumer
    /// asking for every statement's series instead of its top-N is visible in the log before it is visible as a
    /// store that scans 30 days per statement per hour. Chosen, not measured: 500 is ten servers × a fifty-statement
    /// top-N at the widest consumer the campaign has ruled on (lane 34 asks for the top-1 share candidates), so a
    /// count above it means a consumer is NOT bounding — calibrate against the keyed entry counts a fleet pass
    /// actually reaches before the next release. A ceiling, not a cap: the provider refuses nothing (the lookup's
    /// doc says why).
    /// </summary>
    internal const int KeyedBaselineCacheWarnCount = 500;

    /// <summary>How many keyed series the cache holds right now — the number the cardinality note reports.</summary>
    internal int KeyedEntryCount => _cache.Count(pair => pair.Value.Key is not null);

    /// <summary>
    /// Pure: is it time to say the keyed cache is over <see cref="KeyedBaselineCacheWarnCount"/>? Yes when the count
    /// is over the bar AND the note was never logged, or was logged a full <see cref="CacheTtl"/> ago — once per cache
    /// period, the provider's grain for "once per pass" (a pass inside the TTL computes nothing new). At or under the
    /// bar, never — a consumer that stays bounded costs the log nothing.
    /// </summary>
    internal static bool ShouldWarnKeyedCardinality(int keyedEntries, DateTime? lastWarnedAt, DateTime nowUtc)
        => keyedEntries > KeyedBaselineCacheWarnCount
           && (lastWarnedAt is null || nowUtc - lastWarnedAt.Value >= CacheTtl);

    private async Task<CachedBaseline> GetOrComputeBaselinesAsync(
        int serverId, string metricName, string? key, DateTime analysisTime, CancellationToken cancellationToken)
    {
        var cacheKey = CacheKeyFor(serverId, metricName, key);
        var roundedHour = new DateTime(analysisTime.Year, analysisTime.Month, analysisTime.Day, analysisTime.Hour, 0, 0);

        if (_cache.TryGetValue(cacheKey, out var cached) &&
            cached.ComputedAt == roundedHour &&
            (DateTime.UtcNow - cached.RealTime) < CacheTtl)
        {
            return cached;
        }

        var (buckets, clock) = await ComputeBaselinesAsync(serverId, metricName, key, analysisTime, cancellationToken);

        var entry = new CachedBaseline
        {
            ComputedAt = roundedHour,
            RealTime = DateTime.UtcNow,
            Buckets = buckets,
            Clock = clock,
            Key = key
        };
        _cache[cacheKey] = entry;

        if (key is not null)
        {
            NoteKeyedCardinality(serverId, metricName);
        }

        return entry;
    }

    /// <summary>The cardinality note itself (#3691 lane 33): Warning, because a consumer over the bar is a defect in
    /// the consumer, not a statement about the host — the opposite of the clock resolver's Information note.</summary>
    private void NoteKeyedCardinality(int serverId, string metricName)
    {
        var keyedEntries = KeyedEntryCount;
        var now = DateTime.UtcNow;
        lock (_keyedWarnGate)
        {
            if (!ShouldWarnKeyedCardinality(keyedEntries, _keyedCardinalityWarnedAt, now))
            {
                return;
            }

            _keyedCardinalityWarnedAt = now;
        }

        _logger?.LogWarning(
            "[PgBaselineProvider] {KeyedEntries} keyed baseline series are cached (bar {WarnCount}; latest server {ServerId}, metric {MetricName}) — each is a 30-day scan per cache period, so a consumer of the keyed overload is not bounding itself to its window's top-N (#3691 lane 33). Noted once per cache period.",
            keyedEntries, KeyedBaselineCacheWarnCount, serverId, metricName);
    }

    private async Task<(Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>? Buckets, LocalClockWindow Clock)> ComputeBaselinesAsync(
        int serverId, string metricName, string? key, DateTime analysisTime, CancellationToken cancellationToken)
    {
        /* Two seams, never one: a keyed lookup resolves ONLY through the keyed seam, so a metric with an unkeyed arm
           and no keyed one answers "no baseline" to a keyed call rather than the population's buckets under a member's
           name — and the reverse, so lane 27's server-wide arm keeps answering the unkeyed call it always answered. */
        var query = key is null ? ResolveBaselineQuery(metricName) : ResolveKeyedBaselineQuery(metricName);
        if (query == null) return (null, LocalClockWindow.Utc(analysisTime));

        return await ComputeBucketsAsync(serverId, metricName, key, analysisTime, query, cancellationToken);
    }

    /// <summary>
    /// The newest <c>server_properties</c> row that carries an offset, for the clock the buckets key on (#3653 Q6).
    /// The same read <c>PgFindingStore.GetPriorOccurrencesSql</c> and Lite's <c>LocalDataService.ServerInfo</c> make,
    /// with <c>time_zone_id</c> (V134) riding along: skipping NULL offsets rather than taking the newest row blindly,
    /// because the column is nullable and a store migrated from before it holds snapshots that predate it. One row,
    /// on the compute's own connection, once per metric per cache period — an indexed <c>LIMIT 1</c> beside a
    /// 30-day aggregate scan. A PostgreSQL target has no <c>server_properties</c> row and would read (NULL, NULL) here,
    /// which <see cref="BaselineLocalClock.Resolve"/> turns into UTC keying — so <see cref="PgTargetBaselineProvider"/>
    /// overrides <see cref="ReadServerClockAsync"/> to read the target's own <c>TimeZone</c> setting instead (#3691).
    /// </summary>
    internal const string ServerClockSql = @"
SELECT utc_offset_minutes, time_zone_id
FROM server_properties
WHERE server_id = $1
AND   utc_offset_minutes IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// The second seam a derived provider overrides (#3691, after <see cref="ResolveBaselineQuery"/>): WHERE the
    /// target's clock comes from. The base reads <see cref="ServerClockSql"/> — the SQL Server collector's
    /// <c>server_properties</c> row — and its body is unchanged from #3749; <see cref="PgTargetBaselineProvider"/>
    /// answers from the PostgreSQL collector's <c>pg_server_config</c> <c>TimeZone</c> row instead, and everything
    /// downstream (<see cref="BaselineLocalClock.Resolve"/>, the <c>$4..$6</c> bind, the cached
    /// <see cref="LocalClockWindow"/> the lookup keys through) is inherited, so the two engines cannot disagree on
    /// how a clock becomes a bucket key — only on where the clock is read. The contract is the tuple
    /// <see cref="BaselineLocalClock.Resolve"/> takes: a zone id (preferred — it knows WHEN the offset changes), a
    /// fixed offset in minutes, or (null, null) for UTC keying. <paramref name="windowEndUtc"/> is the analysis
    /// time, naive UTC, so an override can anchor its read at or before the window an anchored pass (#2506) asked
    /// for; the base's newest-row read does not need it and ignores it — the SQL Server clock is a property of
    /// the host, not of the window.
    /// </summary>
    protected virtual async Task<(int? UtcOffsetMinutes, string? TimeZoneId)> ReadServerClockAsync(
        NpgsqlConnection connection, int serverId, DateTime windowEndUtc, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(ServerClockSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(serverId);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, null);
        }

        return (reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetValue(0)),
                reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    /// <summary>
    /// The one seam a derived provider overrides (#3542): which SQL computes <paramref name="metricName"/>'s
    /// buckets. The base answers from <see cref="GetBaselineQuery"/> — the SQL Server store tables and CAGGs.
    /// <see cref="PgTargetBaselineProvider"/> answers from its own <c>clean</c> CTEs over the PostgreSQL raw
    /// hypertables and inherits everything else here unchanged: the hour×dow cache, the parameter binding,
    /// the eight-column robust reader, the timeout classification (<see cref="IsCommandTimeout"/> — ONE
    /// definition, <c>BaselineTimeoutIsNamedTests</c>) and the degrade-to-empty posture. A second provider
    /// that copied that machinery to swap one <c>switch</c> would be the drift this seam exists to prevent.
    /// Null means "no baseline for this metric", exactly as it always has.
    /// </summary>
    protected virtual string? ResolveBaselineQuery(string metricName) => GetBaselineQuery(metricName);

    /// <summary>
    /// The third seam (#3691 lane 33, after <see cref="ResolveBaselineQuery"/> and <see cref="ReadServerClockAsync"/>):
    /// which SQL computes <paramref name="metricName"/>'s buckets for ONE member of a population, when the caller
    /// passed a key to <see cref="GetBaselineAsync(int, string, string?, DateTime, CancellationToken)"/>. The base
    /// declares no keyed metric — the SQL Server store's arms are all population-wide, and a keyed call against this
    /// class is "no baseline for this metric" exactly as an unknown name is; <c>PgBaselineProviderKeyedTests</c> pins
    /// that for every declared metric name. <see cref="PgTargetBaselineProvider"/> overrides it for the statement
    /// family. The text an override returns is an ordinary arm plus a <c>$7</c> predicate on its member column
    /// (<c>queryid = $7::BIGINT</c> — the key is bound as text, and an arm casts it to its own column's type), and the
    /// two-armed census in <c>LocalClockBucketKeyTests</c> holds it to exactly <c>$1..$7</c>.
    /// </summary>
    protected virtual string? ResolveKeyedBaselineQuery(string metricName) => null;

    /// <summary>
    /// Did this failure mean "the statement ran out of time" rather than "the connection broke"?
    ///
    /// <para>Worth a named predicate because the two are indistinguishable in the message Npgsql produces.
    /// Npgsql enforces its command timeout by CANCELLING the statement, so the server logs
    /// <c>canceling statement due to user request</c> and the client is left holding a torn stream, which it
    /// reports as "Exception while reading from stream". Read literally that says the network failed; what
    /// actually happened is a query outgrowing its deadline on a store that grew. On the dogfood box the two
    /// log lines sat 267 ms apart in different files, and correlating them by hand is not a diagnosis the
    /// next person should have to repeat.</para>
    ///
    /// <para>Structural, not message matching: <c>57014</c> is <c>query_canceled</c>, the server saying it
    /// cancelled; a <see cref="TimeoutException"/> anywhere in the chain is Npgsql's own deadline. Everything
    /// else stays "failed", because labelling a genuine connection fault a timeout is the same defect aimed
    /// the other way.</para>
    ///
    /// <para><b>Three call sites cite that as "the house discipline", so its SCOPE belongs here: it is
    /// about what a failure MEANS, not about every question one can be asked.</b> This predicate's question
    /// is whether the statement ran out of time, and structure answers it — every producer of <c>57014</c>
    /// ran out of time, so the code is sufficient and the transport's prose is worse than useless. WHOSE
    /// clock ran out is a different question and structure cannot answer it at all: PostgreSQL raises
    /// <c>57014</c> for the target's <c>statement_timeout</c>, for <c>pg_cancel_backend()</c> and for the
    /// CancelRequest Npgsql sends on its own deadline, so the code is shared and the message is the only
    /// field that differs. <c>CollectorFaultCancelOrigin</c> in the service reads it for exactly that, and
    /// treats every wording it does not recognise as unproven. Reading text where structure suffices is the
    /// defect this discipline names; refusing to read it where structure is provably silent is the same
    /// defect wearing the rule as a costume (#3118).</para>
    /// </summary>
    internal static bool IsCommandTimeout(Exception ex) =>
        ex is PostgresException { SqlState: "57014" }
        || ex is TimeoutException
        || ex.InnerException is TimeoutException;

    /// <summary>
    /// The (legacy, successor) supply pair a metric reads, or <c>null</c> for the metrics whose supply was
    /// never superseded. Keyed on the METRIC, not the relation: three arms read the two superseded relations
    /// (BatchRequests → perfmon; WaitStats and WaitMsPerSec → wait_stats), and the choice has to be made
    /// per arm because each arm's legacy text differs from its successor text.
    /// </summary>
    internal static (string Legacy, string Successor)? SupersededSupplyFor(string metricName) => metricName switch
    {
        MetricNames.BatchRequests => (TimescaleSupport.LegacyPerfmonBaselineView, TimescaleSupport.PerfmonIntervalBaselineView),
        MetricNames.WaitStats or MetricNames.WaitMsPerSec => (TimescaleSupport.LegacyWaitStatsBaselineView, TimescaleSupport.WaitStatsIntervalBaselineView),
        _ => null,
    };

    /// <summary>
    /// THE SUPPLY RULE (#3653) as this provider applies it — per metric, against THIS server's reach into the
    /// two baseline supplies: read the successor when the legacy relation is absent, or when the successor
    /// reaches at least as far back into the server's window as the legacy does. The rule itself lives ONCE, on
    /// <see cref="TimescaleSupport.PrefersSuccessor"/>, since Q12 gave it a second caller (the hourly rollup
    /// readers, through <c>RollupCoverage.HourlyRelationFor</c>, over a store's materialized floors rather than
    /// one server's); the argument for the comparison — against the legacy's OWN reach, never the window alone,
    /// so a server registered three days ago is not sent to the contaminated supply for twenty-seven days for
    /// no gain — is stated there. This alias keeps the provider's tests reading the rule under the name they
    /// pinned it by, and keeps the per-server inputs (<see cref="SupplyOldestBucketSql"/>) where the rollup
    /// readers have none.
    /// </summary>
    internal static bool PrefersSuccessor(bool legacyExists, DateTime? legacyOldest, DateTime? successorOldest, DateTime windowStart)
        => TimescaleSupport.PrefersSuccessor(legacyExists, legacyOldest, successorOldest, windowStart);

    /// <summary>
    /// One server's oldest bucket in a baseline relation. <c>WHERE server_id = $1</c> is what keeps this cheap on a
    /// real-time continuous aggregate: the materialized half answers off its <c>(server_id, bucket)</c> group
    /// index, and the un-materialized tail (raw past the watermark) is one server's last hour or two of rows
    /// rather than the whole fleet's.
    /// </summary>
    internal static string SupplyOldestBucketSql(string relation)
        => $"SELECT min(bucket) FROM {relation} WHERE server_id = $1";

    /// <summary>
    /// Picks the SQL to run for this compute: <paramref name="query"/> unchanged for every metric without a
    /// superseded supply, and for the three that have one, either the successor text it was handed or the
    /// legacy text, by <see cref="PrefersSuccessor"/>. Only swaps when <paramref name="query"/> IS this class's
    /// own successor text for the metric — a derived provider that resolved its own SQL for a metric of the same
    /// name (the <see cref="ResolveBaselineQuery"/> seam) keeps what it resolved.
    ///
    /// <para>Two probes, sequential on purpose: the legacy relation is absent on every store first installed at
    /// or after this build and on every store that has retired it, and a statement naming a relation that does
    /// not exist fails at parse time — so its existence is asked first and its <c>min(bucket)</c> only when it
    /// answered yes. The successor always exists once the ensure sweep has run (as a continuous aggregate or
    /// the plain fallback view), so a failure there is a real failure and is left to the caller's catch, which
    /// already degrades this metric to no baseline for the pass.</para>
    /// </summary>
    private async Task<string> ChooseSupplyAsync(
        NpgsqlConnection connection, int serverId, string metricName, string query, DateTime windowStart, CancellationToken cancellationToken)
    {
        var pair = SupersededSupplyFor(metricName);
        if (pair is null || !string.Equals(query, GetBaselineQuery(metricName), StringComparison.Ordinal))
        {
            return query;
        }

        var legacyQuery = GetLegacyBaselineQuery(metricName);
        if (legacyQuery is null)
        {
            return query;
        }

        var (legacy, successor) = pair.Value;

        bool legacyExists;
        using (var probe = new NpgsqlCommand(TimescaleSupport.BaselineRelationExistsSql(legacy), connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
        {
            legacyExists = await probe.ExecuteScalarAsync(cancellationToken) is true;
        }

        if (!legacyExists)
        {
            return query;
        }

        var legacyOldest = await OldestBucketAsync(connection, legacy, serverId, cancellationToken);
        var successorOldest = await OldestBucketAsync(connection, successor, serverId, cancellationToken);

        var prefersSuccessor = PrefersSuccessor(legacyExists, legacyOldest, successorOldest, windowStart);
        if (!prefersSuccessor)
        {
            _logger?.LogDebug(
                "[PgBaselineProvider] {MetricName} for server {ServerId} reads the superseded {Legacy} supply this pass: it reaches back to {LegacyOldest} where {Successor} reaches only {SuccessorOldest} against a window from {WindowStart} (#3653; the successor takes over once its backfill and live refresh reach as far).",
                metricName, serverId, legacy, legacyOldest, successor, successorOldest, windowStart);
        }

        return prefersSuccessor ? query : legacyQuery;
    }

    private static async Task<DateTime?> OldestBucketAsync(NpgsqlConnection connection, string relation, int serverId, CancellationToken cancellationToken)
    {
        using var probe = new NpgsqlCommand(SupplyOldestBucketSql(relation), connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        probe.Parameters.AddWithValue(serverId);
        return await probe.ExecuteScalarAsync(cancellationToken) is DateTime oldest ? oldest : null;
    }

    private async Task<(Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>? Buckets, LocalClockWindow Clock)> ComputeBucketsAsync(
        int serverId, string metricName, string? key, DateTime analysisTime, string query, CancellationToken cancellationToken)
    {
        var absStdDevFloor = BaselineMath.AbsStdDevFloorFor(metricName);
        var windowStart = analysisTime.AddDays(-BaselineMath.BaselineWindowDays);
        var clock = LocalClockWindow.Utc(analysisTime);

        /* Timed so the failure path can say how long it got, not just that it failed — see the catch. */
        var elapsed = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            /* The successor/legacy supply swap is the SQL Server pair's (#3653) and compares the text against this
               class's own unkeyed successor arm; a keyed arm's text never matches it, so the swap is inert for a keyed
               compute by construction — skipped explicitly so the two probes are not paid for nothing. */
            if (key is null)
            {
                query = await ChooseSupplyAsync(connection, serverId, metricName, query, windowStart, cancellationToken);
            }

            /* #3653 Q6: the target's clock over this window, read on the same connection and INSIDE this try on
               purpose — a store that cannot answer a one-row indexed read of server_properties cannot answer the
               aggregate scan either, and one classified catch (AnalysisShutdownResidueTests pins exactly one) is
               the right number of places for "this metric has no baseline this pass" to be said. */
            var (utcOffsetMinutes, timeZoneId) = await ReadServerClockAsync(connection, serverId, AsNaive(analysisTime), cancellationToken);
            clock = _localClock.Resolve(timeZoneId, utcOffsetMinutes, AsNaive(windowStart), AsNaive(analysisTime));

            using var cmd = new NpgsqlCommand(query, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            /* Window bounds arrive as bound naive-UTC parameters (Kind-Unspecified so Npgsql
               maps them to `timestamp`, matching the naive-UTC columns) — never bare now(). */
            cmd.Parameters.AddWithValue(AsNaive(windowStart));
            cmd.Parameters.AddWithValue(AsNaive(analysisTime));
            /* $4..$6: the clock BaselineLocalClock.LocalCollectionTimeSql keys on — the transition instant (naive
               UTC, same `timestamp` mapping as the bounds) and the offset minutes before/after it. Every statement
               this method runs must reference all three; the engine will not say so if one does not. */
            cmd.Parameters.AddWithValue(AsNaive(clock.TransitionAtUtc));
            cmd.Parameters.AddWithValue(clock.OffsetBeforeMinutes);
            cmd.Parameters.AddWithValue(clock.OffsetAfterMinutes);
            /* $7 (#3691 lane 33): the member key, bound as text and ONLY on a keyed compute — an unkeyed statement
               never sees a seventh parameter, so the SQL Server pass binds exactly what it bound before this seam
               existed. A keyed arm casts it to its column's type (queryid = $7::BIGINT); the two-armed census in
               LocalClockBucketKeyTests holds keyed text to $1..$7 and unkeyed text to $1..$6, because the engine
               accepts a surplus bind and would run a keyed arm that forgot the clock parameters on UTC without a word. */
            if (key is not null)
            {
                cmd.Parameters.AddWithValue(key);
            }

            var buckets = new Dictionary<(int, int), BaselineBucket>();

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            /* #1743: the robust-scaffold metrics return eight columns (…, median_val, mad_val)
               and carry sentinel tier rows; the two event-family metrics (blocking, deadlock)
               keep the six-column classical shape — detected by column count, so their buckets
               read Median=0/Mad=0 and the robust path degrades for them. */
            var hasRobustColumns = reader.FieldCount >= 8;
            while (await reader.ReadAsync(cancellationToken))
            {
                var hour = Convert.ToInt32(reader.GetValue(0));
                var dow = Convert.ToInt32(reader.GetValue(1));
                var mean = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
                var stddev = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                var count = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
                var distinctDays = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));
                var median = hasRobustColumns && !reader.IsDBNull(6) ? Convert.ToDouble(reader.GetValue(6)) : 0.0;
                var mad = hasRobustColumns && !reader.IsDBNull(7) ? Convert.ToDouble(reader.GetValue(7)) : 0.0;

                buckets[(hour, dow)] = new BaselineBucket
                {
                    HourOfDay = hour,
                    DayOfWeek = dow,
                    Mean = mean,
                    StdDev = stddev,
                    SampleCount = count,
                    DistinctDays = distinctDays,
                    AbsStdDevFloor = absStdDevFloor,
                    Median = median,
                    Mad = mad,
                    /* Sentinel rows from the GROUPING SETS scaffold carry their tier in their key:
                       (-1,-1) = the exact flat tier, (hour,-1) = the exact hour-only tier. A real
                       (hour, dow) bucket is Full even when sparse — SelectBucket's thresholds
                       decide whether it is USED, not what it IS. */
                    Tier = hour < 0 ? BaselineTier.Flat
                         : dow < 0 ? BaselineTier.HourOnly
                         : BaselineTier.Full
                };
            }

            return (buckets, clock);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            /* A command TIMEOUT and a genuine connection fault are the same message here, and that cost real
               diagnosis time on the dogfood box: Npgsql surfaces its own client-side timeout as
               "Exception while reading from stream" — it cancels the statement, the server logs
               `canceling statement due to user request`, and the client only sees the torn stream. Read
               literally, that says "the network broke"; what actually happened is that this query outgrew its
               timeout on a store that had grown. Correlating the two logs by timestamp (267 ms apart) is not
               something the next person should have to redo, so the distinction is reported here.

               Detected structurally rather than by message text: 57014 is the server telling us it cancelled,
               and a TimeoutException anywhere in the chain is Npgsql's own deadline. Anything else keeps the
               old wording, because calling a real connection fault a timeout would be the same defect
               pointing the other way. */
            if (IsCommandTimeout(ex))
            {
                _logger?.LogError(
                    "[PgBaselineProvider] Baseline query for {MetricName} did not finish within its command timeout — gave up after {Seconds:F1}s, so this metric has NO baseline this pass and its anomaly detection is silent (the collected data is unaffected). The store side logs this as 'canceling statement due to user request'. If it repeats, the window this query scans has outgrown the timeout: {Message}",
                    metricName, elapsed.Elapsed.TotalSeconds, ex.Message);
            }
            else
            {
                _logger?.LogError(
                    "[PgBaselineProvider] Failed to compute baselines for {MetricName} after {Seconds:F1}s: {Message}",
                    metricName, elapsed.Elapsed.TotalSeconds, ex.Message);
            }

            return (null, clock);
        }
    }

    /// <summary>
    /// #1743: the shared robust-tier scaffold appended to every raw-grain metric's cleaned rowset.
    /// The arm contributes a CTE chain ending in <c>clean(collection_time, v)</c> — its existing
    /// window-bound/restart-exclusion semantics untouched — and this scaffold computes mean, stddev,
    /// median, and MAD EXACTLY at all three tiers via GROUPING SETS: (hh,dw) = Full, (hh) = the
    /// hour-only sentinel row (day_of_week -1), () = the flat sentinel row (-1,-1). Medians cannot
    /// be pooled from per-bucket medians, which is why the tiers are computed in SQL rather than
    /// synthesized in BaselineMath — see SelectBucket's sentinel-key contract. MAD is a second
    /// percentile pass: every row joins each tier it belongs to (its full bucket, its hour, and the
    /// flat tier), then median of |v − tier median| per tier. Sentinel tiers also fix the flat
    /// tier's DistinctDays, which the pooled synthesis could only approximate with a MAX proxy.
    /// </summary>
    /*
       #2820: the tier expansion is written as an explicit UNION ALL feeding an EQUI-join, not as the
       obvious `ON (t.hour_of_day = -1 OR t.hour_of_day = k.hh) AND (t.day_of_week = -1 OR ...)`.
       Both express "every row joins each tier it belongs to" and both return byte-identical rows —
       the OR form was the original, and it is the reason io_latency spent a week timing out on the
       dogfood box. Postgres cannot hash an OR'd non-equi predicate, so it degrades to a join whose
       planner estimate reached cost 596,208,924 for a 193-row result and which spilled to temp;
       measured on use2, one server, 476,431 rows in the 30-day window: 23.7s OR-join vs 4.2s
       expanded, same 193 rows and the same checksum. DuckDB never needed this because it has a
       native mad() aggregate (Lite computes the same answer single-pass, no join at all); this
       scaffold is the Postgres emulation of that, so it has to earn its join shape explicitly.

       Expanding rows before an equi-join is deliberately the cheaper side of the trade: the fanout
       is identical either way (each row belongs to exactly three tiers), so this buys the hash join
       without adding a single row to the percentile sorts.

       #3653 Q6: hh, dw and d are extracted from LocalCollectionTime — collection_time shifted onto the
       target's clock by the $4..$6 step function — not from bare collection_time. That ONE substitution is
       what re-keys every arm ending in clean(collection_time, v): the SQL Server arms here, the legacy arms,
       and every PgTargetBaselineProvider arm, the quarter-hour I/O grain included (every real offset is a
       multiple of 15 minutes, so a date_bin'd sample and its rows shift into the same local hour). d is the
       LOCAL date, so distinct_days counts the server's days, and a Wednesday 03:00Z row at UTC−5 is a
       Tuesday-22h sample with a Tuesday date.
    */
    internal const string RobustTierScaffold = @"
keyed AS (
    SELECT v,
           EXTRACT(HOUR FROM " + LocalCollectionTime + @")::INT AS hh,
           EXTRACT(DOW FROM " + LocalCollectionTime + @")::INT AS dw,
           " + LocalCollectionTime + @"::DATE AS d
    FROM clean
),
tier_stats AS (
    SELECT COALESCE(hh, -1) AS hour_of_day,
           COALESCE(dw, -1) AS day_of_week,
           AVG(v) AS mean_val,
           STDDEV_SAMP(v) AS stddev_val,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS median_val,
           COUNT(*) AS sample_count,
           COUNT(DISTINCT d) AS distinct_days
    FROM keyed
    GROUP BY GROUPING SETS ((hh, dw), (hh), ())
),
keyed_tiers AS (
    SELECT v, hh AS hour_of_day, dw AS day_of_week FROM keyed
    UNION ALL SELECT v, hh, -1 FROM keyed
    UNION ALL SELECT v, -1, -1 FROM keyed
),
tier_mads AS (
    SELECT t.hour_of_day, t.day_of_week,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(k.v - t.median_val)) AS mad_val
    FROM keyed_tiers AS k
    JOIN tier_stats AS t
      ON t.hour_of_day = k.hour_of_day
     AND t.day_of_week = k.day_of_week
    GROUP BY t.hour_of_day, t.day_of_week
)
SELECT t.hour_of_day, t.day_of_week, t.mean_val, t.stddev_val, t.sample_count, t.distinct_days,
       t.median_val, m.mad_val
FROM tier_stats AS t
JOIN tier_mads AS m
  ON m.hour_of_day = t.hour_of_day
 AND m.day_of_week = t.day_of_week";

    /// <summary>
    /// The eleven per-metric baseline queries — Lite's, verbatim, except the QUALIFY
    /// sites rewritten for Postgres (no QUALIFY support). Internal (not private like Lite's)
    /// so Darling.Tests can pin every query's dialect and the rewrites' structure ungated.
    /// <para>#1743: the nine non-event metrics route their cleaned rowsets through
    /// <see cref="RobustTierScaffold"/> and return EIGHT columns (…, median_val, mad_val) — CPU
    /// and I/O latency included, reading their RAW hypertables at Lite's grain (their retired
    /// sum/sumsq rollups could not produce a median; both tables carry their own 30-day
    /// service-side retention, so this does not reopen #1757 — see the arms' notes).
    /// Blocking/deadlock are event-family (events/day, stddev 0) evaluated on the event-ratio
    /// path, deliberately untouched; the reader detects their six-column shape by count.</para>
    /// </summary>
    internal static string? GetBaselineQuery(string metricName)
    {
        // All queries return: hour_of_day, day_of_week, mean_val, stddev_val, sample_count
        // Cumulative metrics (batch requests, wait stats, query duration) use CTEs for
        // restart poisoning exclusion — exclude samples where value drops to near-zero
        // when the prior sample was significantly higher.
        // Multi-row-per-collection metrics (waits, sessions, queries) aggregate per
        // collection_time first, then bucket by hour+dow.
        return metricName switch
        {
            /* #1743 follow-up: CPU reads the RAW hypertable, at Lite's exact per-sample grain, so
               the robust scaffold applies — the old sum/sumsq rollup could reconstruct mean/stddev
               but structurally cannot produce a median. Reading raw here does NOT reopen #1757:
               that finding was 4 days of supply under a 30-day window, and cpu_utilization carries
               its own 30-DAY service-side retention (CollectorScheduleDefaults: 1-minute cadence,
               30-day retention; verified on a production store — no TimescaleDB retention policy
               on the table, service-side purge at 30d, compressed after 1 day). The mean/stddev
               this computes are the SAME per-sample statistics the rollup reconstruction produced.
               The now-unused cpu_utilization_baseline aggregate remains registered for upgrade
               compatibility; retiring it is separate cleanup. */
            MetricNames.Cpu => @"
WITH clean AS (
    SELECT collection_time, sqlserver_cpu_utilization::DOUBLE PRECISION AS v
    FROM cpu_utilization_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            /* QUALIFY rewrite 1 of 4 — cumulative counter, restart exclusion.
               Excludes samples where the delta drops to 0 when the prior sample was > 1000
               (restart signature for cumulative counters). Lite's DuckDB original (#3527 unit:
               v is delta / the row's measured sample_interval_seconds — a per-second rate):

                   WITH clean AS (
                       SELECT collection_time, delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0) AS v
                       FROM v_perfmon_stats
                       WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                       AND   counter_name = 'Batch Requests/sec'
                       AND   delta_cntr_value >= 0
                       AND   sample_interval_seconds > 0
                       QUALIFY NOT (delta_cntr_value = 0
                           AND COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) > 1000)
                   )

               QUALIFY evaluates AFTER window computation: LAG runs over every WHERE-surviving
               row (including rows QUALIFY itself is about to drop), THEN the predicate prunes.
               The rewrite computes the SAME LAG over the SAME WHERE-filtered rowset inside a
               CTE and applies the IDENTICAL predicate in the outer WHERE — window-before-filter
               is preserved, so only the FIRST zero after a >1000 sample is dropped, and a zero
               following another zero keeps LAG = 0 and SURVIVES (genuine idle, not a restart).
               Row selection is exactly the original's.

               #3527 divisor, re-taken by #3653: this arm reads the perfmon_interval_baseline supply
               (CreatePerfmonIntervalBaselineSql), which carries the collection's MEASURED
               sample_interval_seconds and has already dropped every interval-0 (unknowable) row. So v is
               delta / the stored interval — Lite's exact unit — and the LAG(collection_time) gap the
               legacy arm had to derive (GetLegacyBaselineQuery) is now only the fallback for a
               pre-column collection whose interval is NULL. Two row-selection consequences, both
               deliberate: (a) the window's FIRST collection is no longer skipped when it has a stored
               interval (only a NULL-interval first row still lacks a divisor); (b) the > 1000 heuristic is
               GATED on sample_interval_seconds IS NULL — a measured zero after a busy sample is a real
               idle sample (a restart writes interval 0 and never reaches this text), so the heuristic
               guards only the rows for which the collector left no verdict. The ::DOUBLE PRECISION cast
               is the io-arm rule: STDDEV_SAMP over numeric can overflow System.Decimal at
               materialization. */
            MetricNames.BatchRequests => @"
WITH windowed AS (
    SELECT collection_time, delta_cntr_value, sample_interval_seconds,
           COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) AS prior_delta,
           COALESCE(sample_interval_seconds::DOUBLE PRECISION,
                    extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))) AS interval_sec
    FROM perfmon_interval_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
clean AS (
    SELECT collection_time, delta_cntr_value::DOUBLE PRECISION / interval_sec AS v
    FROM windowed
    WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000 AND sample_interval_seconds IS NULL)
    AND   interval_sec > 0
)," + RobustTierScaffold,

            /* QUALIFY rewrite 2 of 4 — cumulative counter, multiple rows per collection (per
               wait type): aggregate to total wait ms per collection FIRST, then restart
               exclusion. Lite's DuckDB original applied QUALIFY inside the grouped CTE (since #3540
               Lite's WHERE also carries sample_interval_seconds IS DISTINCT FROM 0, dropping the
               calculator's unknowable rows before the sum; since #3653 this arm reads the
               wait_stats_interval_baseline aggregate, which bakes that same WHERE in and carries the
               interval — the legacy wait_stats_baseline could not, and GetLegacyBaselineQuery keeps the
               text that reads it for the window it still covers):

                   WITH per_collection AS (
                       SELECT collection_time,
                              SUM(delta_wait_time_ms) AS total_wait_ms
                       FROM v_wait_stats
                       WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                       AND   delta_wait_time_ms >= 0
                       GROUP BY collection_time
                       QUALIFY NOT (total_wait_ms = 0
                           AND COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) > 10000)
                   )
                   SELECT ... FROM per_collection GROUP BY hour_of_day, day_of_week

               In DuckDB that QUALIFY's LAG runs over the GROUPED rows (one per collection_time)
               before any exclusion. The rewrite splits grouping (per_collection) from windowing
               (with_lag) so LAG still sees EVERY grouped row — a row the filter drops still
               serves as its successor's LAG value — and the outer WHERE applies the identical
               predicate. Only the first 0-total immediately after a >10000ms collection (the
               restart signature) is excluded; consecutive zeros (genuine idle) survive because
               their LAG is 0, not >10000. Row selection is exactly the original's for every
               NULL-interval collection.

               #3653: the heuristic is GATED on sample_interval_seconds IS NULL. A collection with a
               measured interval that survived the supply's IS DISTINCT FROM 0 filter is, by the
               collector's own verdict, a real sample — its zero is idle, not a restart (a restart's rows
               carry interval 0 and never form a per-collection row here) — so the magnitude test is
               redundant for it and would wrongly drop a real quiet minute after a busy one. The gate
               keeps the heuristic exactly where it is still the only guard: pre-column history. */
            MetricNames.WaitStats => @"
WITH per_collection AS (
    SELECT collection_time, total_wait_ms, sample_interval_seconds
    FROM wait_stats_interval_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_lag AS (
    SELECT collection_time, total_wait_ms, sample_interval_seconds,
           COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) AS prior_total_wait_ms
    FROM per_collection
),
clean AS (
    SELECT collection_time, total_wait_ms AS v
    FROM with_lag
    WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000 AND sample_interval_seconds IS NULL)
)," + RobustTierScaffold,

            // Point-in-time, multiple rows per collection (per program_name) —
            // aggregate to total connections per collection first
            MetricNames.SessionCount => @"
WITH clean AS (
    SELECT collection_time, total_connections AS v
    FROM session_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            /* QUALIFY rewrite 3 of 4 — cumulative (plan cache), multiple rows per collection
               (per query): delta columns aggregated to total elapsed per collection, then
               restart exclusion. Lite's DuckDB original:

                   WITH per_collection AS (
                       SELECT collection_time,
                              SUM(delta_elapsed_time) AS total_elapsed
                       FROM v_query_stats
                       WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                       AND   delta_execution_count > 0
                       AND   delta_elapsed_time >= 0
                       GROUP BY collection_time
                       QUALIFY NOT (total_elapsed = 0
                           AND COALESCE(LAG(total_elapsed) OVER (ORDER BY collection_time), 0) > 100000)
                   )
                   SELECT ... FROM per_collection GROUP BY hour_of_day, day_of_week

               Same shape as rewrite 2: group first, window over ALL grouped rows in a separate
               CTE, apply the identical exclusion predicate in the outer WHERE — a 0-total right
               after a >100000us collection is the restart signature and is dropped; zeros after
               zeros survive. Row selection is exactly the original's. */
            MetricNames.QueryDuration => @"
WITH per_collection AS (
    SELECT collection_time, total_elapsed
    FROM query_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_lag AS (
    SELECT collection_time, total_elapsed,
           COALESCE(LAG(total_elapsed) OVER (ORDER BY collection_time), 0) AS prior_total_elapsed
    FROM per_collection
),
clean AS (
    SELECT collection_time, total_elapsed AS v
    FROM with_lag
    WHERE NOT (total_elapsed = 0 AND prior_total_elapsed > 100000)
)," + RobustTierScaffold,

            /* #1743 follow-up: same move as CPU — raw hypertable at Lite's per-file-row grain so
               the robust scaffold applies (file_io_stats also carries its own 30-day service-side
               retention; see the CPU arm's note). The stall/reads ratio keeps its DOUBLE PRECISION
               cast so a spurious large delta can't make STDDEV_SAMP produce a numeric that
               overflows System.Decimal when Npgsql materializes the aggregate. v stays NULLABLE
               (a write-only file row has no read latency): AVG/STDDEV/median/mad all ignore those
               rows while COUNT(*) keeps counting them — exactly the row_count-vs-ratio_count
               distinction the retired rollup documented, preserved at the raw grain. */
            MetricNames.IoLatency => @"
WITH clean AS (
    SELECT collection_time, delta_stall_read_ms::DOUBLE PRECISION / NULLIF(delta_reads, 0) AS v
    FROM file_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   (delta_reads > 0 OR delta_writes > 0)
)," + RobustTierScaffold,

            // Event-based — mean = events per day for this bucket, sample_count = distinct days observed.
            // No restart exclusion needed (event counts, not cumulative).
            /* #3653 Q6: the two event arms bypass the scaffold (six-column shape, no tiers), so they are the
               two places that must extract from LocalCollectionTime by hand — hour, dow AND the distinct
               DATE the per-day mean divides by. A bare collection_time here would run without complaint and
               key on UTC; the local-clock census in LocalClockBucketKeyTests is what forbids it. */
            MetricNames.Blocking => @"
SELECT EXTRACT(HOUR FROM " + LocalCollectionTime + @")::INT AS hour_of_day,
       EXTRACT(DOW FROM " + LocalCollectionTime + @")::INT AS day_of_week,
       SUM(event_count)::DOUBLE PRECISION / GREATEST(COUNT(DISTINCT " + LocalCollectionTime + @"::DATE), 1) AS mean_val,
       0::DOUBLE PRECISION AS stddev_val,
       COUNT(DISTINCT " + LocalCollectionTime + @"::DATE) AS sample_count,
       COUNT(DISTINCT " + LocalCollectionTime + @"::DATE) AS distinct_days
FROM blocked_process_baseline
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Event-based — same approach as blocking
            MetricNames.Deadlock => @"
SELECT EXTRACT(HOUR FROM " + LocalCollectionTime + @")::INT AS hour_of_day,
       EXTRACT(DOW FROM " + LocalCollectionTime + @")::INT AS day_of_week,
       SUM(event_count)::DOUBLE PRECISION / GREATEST(COUNT(DISTINCT " + LocalCollectionTime + @"::DATE), 1) AS mean_val,
       0::DOUBLE PRECISION AS stddev_val,
       COUNT(DISTINCT " + LocalCollectionTime + @"::DATE) AS sample_count,
       COUNT(DISTINCT " + LocalCollectionTime + @"::DATE) AS distinct_days
FROM deadlock_baseline
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Point-in-time metric (memory pressure %) — no restart exclusion needed
            MetricNames.Memory => @"
WITH clean AS (
    SELECT collection_time, memory_pressure_pct AS v
    FROM memory_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            // ── Chart-unit baselines (for UI bands — units match what the chart displays) ──

            /* QUALIFY rewrite 4 of 4 — wait ms per second (chart unit). Lite's DuckDB original (as it stood
               when this rewrite was made; since #3540 Lite's per_collection takes the collection's STORED
               sample_interval_seconds — MAX over its rows — and falls back to this LAG only for pre-v60
               rows, so a restart collection's 0 becomes NULL and is dropped by with_rate's WHERE. Since
               #3653 this arm follows: wait_stats_interval_baseline carries that same MAX and has already
               dropped the restart collection, so per_collection reads the stored interval and derives
               one from LAG(collection_time) only where it is NULL — the legacy text, which had no
               interval to read, is GetLegacyBaselineQuery's):

                   WITH per_collection AS (
                       SELECT collection_time,
                              SUM(delta_wait_time_ms)::DOUBLE PRECISION AS total_wait_ms,
                              extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
                       FROM v_wait_stats
                       WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                       AND   delta_wait_time_ms >= 0
                       GROUP BY collection_time
                   ),
                   with_rate AS (
                       SELECT collection_time,
                              CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec ELSE 0 END AS ms_per_sec
                       FROM per_collection
                       WHERE interval_sec IS NOT NULL
                       QUALIFY NOT (ms_per_sec = 0
                           AND COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) > 100)
                   )
                   SELECT ... FROM with_rate GROUP BY hour_of_day, day_of_week

               Two things to preserve exactly:
               (a) per_collection's LAG(collection_time) alongside GROUP BY collection_time is a
                   window over the GROUPED rows — standard SQL both engines share; it carries
                   over verbatim (no QUALIFY there).
               (b) In DuckDB, with_rate's WHERE runs BEFORE its QUALIFY window: the window's
                   first row (interval_sec NULL — no prior collection) is removed FIRST, so the
                   QUALIFY LAG is computed over only the rated rows. The rewrite keeps that
                   WHERE in with_rate, then windows in a LATER CTE (with_lag) over exactly the
                   post-WHERE rowset, then applies the identical predicate in the outer WHERE.
                   As in rewrites 1-3, LAG sees rows the filter drops, so only the first 0-rate
                   after a >100 ms/sec sample (restart signature) is excluded and idle zeros
                   after zeros survive. Row selection is exactly the original's for NULL-interval
                   collections; for measured ones the > 100 heuristic is gated off (the WaitStats
                   arm's reasoning) and the window's first collection is rated off its stored
                   interval rather than dropped for lacking a prior.

               #3653 (#3540 rule 1, readers NULL-not-0 on unknowable): the ms_per_sec arm ends at END,
               not ELSE 0, and with_rate's WHERE carries interval_sec > 0 beside IS NOT NULL — Lite's
               current text, verbatim (Lite/Analysis/BaselineProvider.cs, WaitMsPerSec). Both halves
               are needed together. The stored interval this arm reads is positive by the aggregate's
               own predicate, but the LAG fallback for a pre-column collection is NOT: two collections
               that date_trunc to the same second derive interval_sec = 0, and under IS NOT NULL alone
               that row reached with_rate, where ELSE 0 rated it 0 ms/sec — a fabricated idle sample in
               the baseline's mean and stddev, which the > 100 heuristic only caught after a busy one.
               With END alone the row would instead carry a NULL ms_per_sec into clean (NOT (NULL = 0
               AND …) is TRUE whenever either other conjunct is FALSE, which is the common case) and be
               COUNT(*)ed as a sample of nothing; interval_sec > 0 in with_rate drops it BEFORE the
               restart LAG, so the sample set holds only rated collections and the LAG window is exactly
               the rated rows, as (b) requires. Proven on a PG18 rig with three planted pre-column
               collections, one pair in the same second: old text count 3 / mean 66.7, END alone count 3 /
               mean 100, this text count 2 / mean 100. The same-second row is unknowable, not idle, and a
               baseline has no honest bucket for it. */
            MetricNames.WaitMsPerSec => @"
WITH per_collection AS (
    SELECT collection_time,
           total_wait_ms::DOUBLE PRECISION AS total_wait_ms,
           sample_interval_seconds,
           CASE WHEN sample_interval_seconds IS NULL
                THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                ELSE sample_interval_seconds
           END AS interval_sec
    FROM wait_stats_interval_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_rate AS (
    SELECT collection_time, sample_interval_seconds,
           CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END AS ms_per_sec
    FROM per_collection
    WHERE interval_sec IS NOT NULL AND interval_sec > 0
),
with_lag AS (
    SELECT collection_time, ms_per_sec, sample_interval_seconds,
           COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) AS prior_ms_per_sec
    FROM with_rate
),
clean AS (
    SELECT collection_time, ms_per_sec AS v
    FROM with_lag
    WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100 AND sample_interval_seconds IS NULL)
)," + RobustTierScaffold,

            // Blocking events per minute (chart shows event bars bucketed by minute)
            MetricNames.BlockingPerMinute => @"
WITH per_minute AS (
    SELECT DATE_TRUNC('minute', collection_time) AS minute_bucket,
           SUM(event_count)::DOUBLE PRECISION AS event_count
    FROM blocked_process_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY minute_bucket
),
clean AS (
    SELECT minute_bucket AS collection_time, event_count AS v
    FROM per_minute
)," + RobustTierScaffold,

            _ => null
        };
    }

    /// <summary>
    /// The pre-#3653 text of the three arms whose supply was superseded, VERBATIM but for one spelling, against
    /// the legacy relations (<c>perfmon_baseline</c>, <c>wait_stats_baseline</c>). Run by <see cref="ChooseSupplyAsync"/> only while a
    /// legacy relation still reaches further back into a server's window than its successor — on a store
    /// upgraded with the default 30-day raw horizon, roughly the first day after the upgrade — and never on a
    /// store that was first installed at or after this build. <c>null</c> for every other metric.
    ///
    /// <para>What this text does that the successor arms no longer do, which is why it is retained rather than
    /// generated: it has no interval to read, so it derives one from <c>LAG(collection_time)</c> (BatchRequests,
    /// WaitMsPerSec) and skips the window's first collection for lacking a prior; and it applies the
    /// <c>LAG &gt; N</c> heuristic to EVERY collection, because over a supply that summed the restart's zeros a
    /// magnitude test was the only restart guard there was. The row-selection arguments for the QUALIFY
    /// rewrites (window-before-filter, LAG over the unfiltered series) are the ones on <see cref="GetBaselineQuery"/>
    /// and hold here unchanged.</para>
    ///
    /// <para>The one spelling that is not the pre-#3653 text: the WaitMsPerSec arm's <c>ms_per_sec</c> CASE ends
    /// at <c>END</c> rather than <c>ELSE 0</c>, and its <c>with_rate</c> WHERE carries <c>interval_sec &gt; 0</c>
    /// beside <c>IS NOT NULL</c> — the same edit the successor arm took (#3540 rule 1, readers NULL-not-0 on
    /// unknowable; the reasoning is on that arm). Here the interval is ALWAYS the LAG derivation, so the
    /// same-second case the successor arm describes is the ordinary way this text meets an interval of 0, and
    /// under the old spelling it entered the legacy supply as a 0 ms/sec sample. Row selection is otherwise the
    /// legacy text's; on a store where this arm still runs (the first day after an upgrade) the change removes
    /// only rows the successor arm would also have refused to rate.</para>
    /// </summary>
    internal static string? GetLegacyBaselineQuery(string metricName)
    {
        return metricName switch
        {
            MetricNames.BatchRequests => @"
WITH windowed AS (
    SELECT collection_time, delta_cntr_value,
           COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) AS prior_delta,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM perfmon_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
clean AS (
    SELECT collection_time, delta_cntr_value::DOUBLE PRECISION / interval_sec AS v
    FROM windowed
    WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000)
    AND   interval_sec > 0
)," + RobustTierScaffold,

            MetricNames.WaitStats => @"
WITH per_collection AS (
    SELECT collection_time, total_wait_ms
    FROM wait_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_lag AS (
    SELECT collection_time, total_wait_ms,
           COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) AS prior_total_wait_ms
    FROM per_collection
),
clean AS (
    SELECT collection_time, total_wait_ms AS v
    FROM with_lag
    WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000)
)," + RobustTierScaffold,

            MetricNames.WaitMsPerSec => @"
WITH per_collection AS (
    SELECT collection_time,
           total_wait_ms::DOUBLE PRECISION AS total_wait_ms,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM wait_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_rate AS (
    SELECT collection_time,
           CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END AS ms_per_sec
    FROM per_collection
    WHERE interval_sec IS NOT NULL AND interval_sec > 0
),
with_lag AS (
    SELECT collection_time, ms_per_sec,
           COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) AS prior_ms_per_sec
    FROM with_rate
),
clean AS (
    SELECT collection_time, ms_per_sec AS v
    FROM with_lag
    WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100)
)," + RobustTierScaffold,

            _ => null
        };
    }

    /// <summary>
    /// The bucket key's time source (#3653 Q6): <see cref="BaselineLocalClock.LocalCollectionTimeSql"/>, the shared
    /// assembly's ONE spelling, aliased here so the arms read as SQL and so a derived provider's own-EXTRACT arm has
    /// a name to reach for. Never spell the shift by hand in an arm.
    /// </summary>
    internal const string LocalCollectionTime = BaselineLocalClock.LocalCollectionTimeSql;

    /// <summary>Kind-Unspecified for reads/writes — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private class CachedBaseline
    {
        public DateTime ComputedAt { get; init; }
        public DateTime RealTime { get; init; }
        public Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>? Buckets { get; init; }

        /// <summary>The clock the buckets were keyed with (#3653 Q6) — the lookup must use the SAME one.</summary>
        public LocalClockWindow Clock { get; init; } = LocalClockWindow.Utc(DateTime.MinValue);

        /// <summary>The member key this series is scoped to (#3691 lane 33); null for the population-wide series.
        /// Read only by <see cref="KeyedEntryCount"/> — the entry's identity is the cache key string.</summary>
        public string? Key { get; init; }
    }
}
