/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>How one Query Store force is doing (#5632).</summary>
public enum ForcedPlanState
{
    /// <summary>The forced plan ran after the force was first seen (or a variant of it did, #6160), and no other plan of the query did.</summary>
    Working,

    /// <summary>No plan of the query ran after the force was first seen.</summary>
    NotExercised,

    /// <summary>Another plan of the query ran after the force was first seen: the engine ran a different plan while the force was set.</summary>
    Ignored,

    /// <summary>The engine counts a force failure, or names a failure reason.</summary>
    Failing,

    /// <summary>The plan was forced in an earlier capture of its database inside the window and is absent from that database's newest capture.</summary>
    Removed,

    /// <summary>
    /// A later plan of the query ran after the force was first seen, and its XML is not in the store (the Query Store collector has not fetched it
    /// yet), or was not read because more later plans ran than one read checks (#6221). Whether it is a variant of the force, which would make the
    /// force WORKING, or another plan, which would make it IGNORED, is not known. A definite other plan beats it: IGNORED stays IGNORED.
    /// </summary>
    UnknownPlanNotCollected,
}

/// <summary>
/// A plan guide that covers a forced query (#6160): its name, whether it is enabled, and its scope (OBJECT, SQL or TEMPLATE). Where the
/// collector found it: an OBJECT-scoped guide on the query's object, or a guide whose name a plan of the query carries. A guide is a fact
/// beside the state, never a state.
/// </summary>
public sealed record ForcedPlanGuide(string Name, bool? Enabled, string? Scope);

/// <summary>One plan of the query other than the forced one, with what it did since the force was first seen.</summary>
public sealed record ForcedPlanOtherPlan(long PlanId, long Executions, DateTime? LastExecution, DarlingForcedPlanStateReader.Ran? Run = null);

/// <summary>
/// One force, its state and the evidence behind it. <see cref="Since"/> is the time "after the force" is measured from: the
/// caller's forced-at when one was given, else <see cref="FirstSeen"/>.
/// </summary>
public sealed record ForcedPlanFact(
    string DatabaseName,
    long QueryId,
    long PlanId,
    ForcedPlanState State,
    DateTime FirstSeen,
    DateTime LastSeen,
    DateTime? ForcedAt,
    DateTime Since,
    string? PlanForcingType,
    long FailureCount,
    string? FailureReason,
    long ForcedExecutions,
    DateTime? ForcedLastExecution,
    IReadOnlyList<ForcedPlanOtherPlan> OtherPlans,
    DarlingForcedPlanStateReader.Ran? ForcedRun = null,
    IReadOnlyList<ForcedPlanOtherPlan>? Variants = null,
    ForcedPlanGuide? PlanGuide = null,
    IReadOnlyList<ForcedPlanOtherPlan>? Unknowns = null,
    string? UnknownReason = null)
{
    /// <summary>
    /// The later plans that ran since <see cref="Since"/> whose stored XML could not be checked (#6221), so it is not known whether they are
    /// variants of the forced plan. They are in neither <see cref="OtherPlans"/> nor <see cref="VariantPlans"/>. Non-empty only with
    /// <see cref="UnknownReason"/>, and when <see cref="State"/> is not IGNORED, FAILING or REMOVED it is
    /// <see cref="ForcedPlanState.UnknownPlanNotCollected"/>.
    /// </summary>
    public IReadOnlyList<ForcedPlanOtherPlan> UnknownPlans => Unknowns ?? Array.Empty<ForcedPlanOtherPlan>();

    /// <summary>
    /// The variants of the forced plan that ran since <see cref="Since"/> (#6160): plans of the query, other than the forced one, whose
    /// stored XML carries <c>UsePlan="1"</c> and no <c>PlanGuideName</c>, which is how SQL Server stores a force it recompiled under a new
    /// plan id. Their runs count as the forced plan running, and they are not in <see cref="OtherPlans"/>.
    /// </summary>
    public IReadOnlyList<ForcedPlanOtherPlan> VariantPlans => Variants ?? Array.Empty<ForcedPlanOtherPlan>();
}

/// <summary>The newest forced-plan capture of one database inside the window.</summary>
/// <param name="ForcedPlanCount">Every force in that capture (counted in SQL over the whole capture, so a force the read cap cut still counts).</param>
public sealed record ForcedPlanCapture(string DatabaseName, DateTime NewestCapture, int ForcedPlanCount);

/// <summary>What <see cref="DarlingForcedPlanStateReader.ReadAsync"/> found.</summary>
/// <param name="Plans">One entry per force, a plan in its database's newest capture or a REMOVED one.</param>
/// <param name="Captures">Each database that has a marker row in the window, with its own newest capture.</param>
/// <param name="StatsFloor">The oldest Query Store stats row in the span the states were read over; null when there is none.</param>
/// <param name="StatsComplete">False when the stored stats start after the oldest time a state is measured from, or hold nothing in the span: a
/// NOT_EXERCISED state is then a statement about what the store holds, not about the server.</param>
/// <param name="ReadCapped">The window holds more forces than <see cref="DarlingForcedPlanStateReader.MaxForces"/> and the read left some out: failing forces
/// first, then the other forces of each database's newest capture, then the removed ones, each in database and plan order.</param>
public sealed record ForcedPlanRead(
    IReadOnlyList<ForcedPlanFact> Plans,
    IReadOnlyList<ForcedPlanCapture> Captures,
    DateTime? StatsFloor,
    bool StatsComplete,
    bool ReadCapped = false);

/// <summary>
/// #5632 step 1b: whether each Query Store force works, from <c>collect.query_store_forced_plans</c> (V195, the collector's
/// per-database marker row plus one row per forced plan, every pass) and the per-plan <c>query_store_stats</c> rows.
///
/// <para><b>The newest capture of a database</b> is the newest marker row (<c>plan_id</c> NULL) inside the window, per database, so a
/// database whose last capture is older than another's is judged against its own. A force is REMOVED when an earlier capture of
/// that database showed it and the newest one does not.</para>
///
/// <para><b>First seen.</b> The first capture of the CURRENT run of captures that show the force (a force removed and set again
/// starts a new run). Query Store keeps no force time, so the real force can be up to one collector pass earlier (five minutes by
/// default). A caller that knows the real time passes it as <c>forcedAt</c>, and it replaces first seen.</para>
///
/// <para><b>Access.</b> Both table reads are bounded by <c>server_id</c> and the window on <c>collection_time</c>
/// (<c>idx_query_store_forced_plans_time</c>). The stats read starts from the forced plans' own queries (an inner join on
/// database and query_id, so it takes <c>idx_query_store_stats_server_db_query_plan_time</c>) and is bounded below by the
/// plan's own since time, never the whole table.</para>
///
/// <para><b>Variants and plan guides come from the stored plans (#6221).</b> The collector reads no plan XML on the monitored server. When a
/// force's runs moved to a LATER plan id of the same query, this reads that plan's stored XML (the plan map, then <c>query_plan_dim</c>: two
/// index lookups a plan) and looks for <c>UsePlan="1"</c>: with no <c>PlanGuideName</c> it is a variant and counts as the force running, with
/// one it is another plan and the guide is named. Only the later plans that ran in the window are read, at most <see cref="MaxStoredPlanChecks"/>
/// plans a read. A later plan with no stored XML, and a plan past that bound, make the force
/// <see cref="ForcedPlanState.UnknownPlanNotCollected"/> with a reason, never IGNORED.</para>
/// </summary>
public static class DarlingForcedPlanStateReader
{
    /// <summary>The most forces one read returns. A database has at most 500 per pass (the collector's cap).</summary>
    public const int MaxForces = 2000;

    /// <summary>
    /// The most later plans one read looks up in the store (#6221), busiest first within each force. Past it the plans are not read and their
    /// force is UNKNOWN_PLAN_NOT_COLLECTED with a reason that says so. A force check reads one query, so it never gets near this; the fleet list
    /// of a database with hundreds of recompiling forces can.
    /// </summary>
    public const int MaxStoredPlanChecks = 200;

    /// <summary>
    /// The stored plan XML of the plans of one database that ran after a force, through the plan map. $1 server, $2 database array, $3 plan_id
    /// array (parallel). A plan with no map row is absent from the result; a row with NULL XML and NULL gzip is a map row whose content is not
    /// there (a NULL digest, or a digest the dimension no longer holds). The same two index lookups as the stored-plan read by plan_id.
    /// </summary>
    public const string StoredPlansSql = @"
WITH t AS (
    SELECT * FROM unnest($2::text[], $3::bigint[]) AS x(database_name, plan_id))
SELECT t.database_name, t.plan_id, d.query_plan_xml, d.query_plan_gz
FROM t
JOIN collect.query_store_plan_map AS m
  ON m.server_id = $1 AND m.database_name = t.database_name AND m.plan_id = t.plan_id
LEFT JOIN query_plan_dim AS d
  ON d.digest = m.digest";

    private enum PlanKind { Unknown, Variant, Guided, Plain }

    /// <summary>What a stored plan's XML says about a force (#6160's rule): <c>UsePlan="1"</c> and no plan guide name is a variant; a plan guide name makes it a guided plan.</summary>
    private static (PlanKind Kind, string? Guide) InspectPlanXml(string xml)
    {
        const string guideAttribute = "PlanGuideName=\"";
        var guideAt = xml.IndexOf(guideAttribute, StringComparison.Ordinal);
        if (guideAt >= 0)
        {
            var start = guideAt + guideAttribute.Length;
            var end = xml.IndexOf('"', start);
            return (PlanKind.Guided, end > start ? xml[start..end] : null);
        }

        return xml.Contains("UsePlan=\"1\"", StringComparison.Ordinal) ? (PlanKind.Variant, null) : (PlanKind.Plain, null);
    }

    /// <summary>
    /// The newest collector capture of each force, read from the window, with the row cap given. $1 server, $2 start, $3 end, $4 database filter,
    /// $5 query_id or NULL, $6 plan_id or NULL (#6560: both narrow the read itself, before the cap, so a force past the cap is still found). The cap cuts the LAST rows of this order: a force that fails (a failure count or
    /// a reason) in its database's newest capture first, then the other forces of the newest capture, then the removed ones, each by database and plan
    /// id, so a standing force that needs a person is never what a cap drops. It reads one row past the cap so the caller can tell the read was cut.
    /// First seen (#6833) comes from one grouped pass over the plan rows (<c>firsts</c>): the earliest row after the force's last absence. A correlated
    /// per-force minimum scanned every captured row once per force (500 forces over 24 hours of captures took 11 seconds).
    /// </summary>
    internal static string ForcesSql(DatabaseFilter databases, int maxForces) => $@"
WITH {BaseCtes(databases)},
absent AS (
    SELECT p.database_name, p.plan_id, MAX(m.collection_time) AS last_absent
    FROM (SELECT DISTINCT database_name, plan_id FROM plans) AS p
    JOIN win AS m ON m.database_name = p.database_name AND m.plan_id IS NULL
    WHERE NOT EXISTS (SELECT 1 FROM plans AS x WHERE x.database_name = p.database_name AND x.plan_id = p.plan_id AND x.collection_time = m.collection_time)
    GROUP BY p.database_name, p.plan_id),
firsts AS (
    SELECT x.database_name, x.plan_id, MIN(x.collection_time) AS first_seen
    FROM plans AS x
    LEFT JOIN absent AS a ON a.database_name = x.database_name AND a.plan_id = x.plan_id
    WHERE a.last_absent IS NULL OR x.collection_time > a.last_absent
    GROUP BY x.database_name, x.plan_id)
SELECT l.database_name, l.plan_id, l.query_id, l.plan_forcing_type, l.force_failure_count, l.last_force_failure_reason, l.collection_time,
       f.first_seen,
       n.newest_capture,
       l.variant_plan_ids, l.plan_guide_name, l.plan_guide_enabled, l.plan_guide_scope
FROM latest AS l
LEFT JOIN firsts AS f ON f.database_name = l.database_name AND f.plan_id = l.plan_id
LEFT JOIN newest AS n ON n.database_name = l.database_name
ORDER BY CASE WHEN l.collection_time >= n.newest_capture AND (l.force_failure_count > 0 OR btrim(COALESCE(l.last_force_failure_reason, '')) <> '') THEN 0
              WHEN l.collection_time >= n.newest_capture THEN 1
              ELSE 2 END,
         l.database_name, l.plan_id
LIMIT {maxForces + 1}";

    /// <summary>
    /// How many forces each database's newest capture holds, over the whole set (no cap), so <c>captures[].forced_plan_count</c> stays the
    /// database's real count when the read was cut. Same parameters as <see cref="ForcesSql(DatabaseFilter, int)"/>.
    /// </summary>
    public static string CountsSql(DatabaseFilter databases) => $@"
WITH {BaseCtes(databases)}
SELECT l.database_name, COUNT(*)
FROM latest AS l
JOIN newest AS n ON n.database_name = l.database_name AND l.collection_time >= n.newest_capture
GROUP BY l.database_name";

    private static string BaseCtes(DatabaseFilter databases) => $@"win AS (
    SELECT database_name, collection_time, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason,
           variant_plan_ids, plan_guide_name, plan_guide_enabled, plan_guide_scope
    FROM collect.query_store_forced_plans
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3{databases.Clause("database_name", 4)}),
plans AS (
    SELECT * FROM win WHERE plan_id IS NOT NULL AND ($5::bigint IS NULL OR query_id = $5) AND ($6::bigint IS NULL OR plan_id = $6)),
newest AS (
    SELECT database_name, MAX(collection_time) AS newest_capture FROM win WHERE plan_id IS NULL GROUP BY database_name),
latest AS (
    SELECT DISTINCT ON (database_name, plan_id) database_name, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason, collection_time,
           variant_plan_ids, plan_guide_name, plan_guide_enabled, plan_guide_scope
    FROM plans ORDER BY database_name, plan_id, collection_time DESC)";

    /// <summary>Each database's newest marker row in the window. $1 server, $2 start, $3 end, $4 database filter.</summary>
    public static string CapturesSql(DatabaseFilter databases) => $@"
SELECT database_name, MAX(collection_time)
FROM collect.query_store_forced_plans
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3 AND plan_id IS NULL{databases.Clause("database_name", 4)}
GROUP BY database_name
ORDER BY database_name";

    /// <summary>
    /// The executions of every plan of each forced plan's query after its since time. $1 server, $2 as_of, then five parallel arrays,
    /// one element per forced plan: database, query_id, forced plan_id, since (the instant a run counts as after the force) and
    /// read_from (since clamped to the window, the lower bound of the read). The interval dedupe is the history tool's (#1841): the
    /// collector stores an open interval again every cycle, so only its newest snapshot counts.
    /// </summary>
    public static readonly string StatsSql = @"
WITH t AS (
    SELECT * FROM unnest($3::text[], $4::bigint[], $5::bigint[], $6::timestamp[], $7::timestamp[])
        AS x(database_name, query_id, forced_plan_id, since, read_from)),
d AS (
    SELECT t.database_name AS target_database, t.forced_plan_id, t.since, s.plan_id, s.execution_count, s.last_execution_time,
           s.avg_cpu_time_us, s.avg_duration_us, s.max_duration_us, s.first_execution_time,
           ROW_NUMBER() OVER (PARTITION BY t.database_name, t.forced_plan_id, s.plan_id, " + IntervalIdentity + @"
                              ORDER BY s.collection_time DESC, s.execution_count DESC) AS rn
    FROM t
    JOIN query_store_stats AS s
        ON s.server_id = $1 AND s.database_name = t.database_name AND s.query_id = t.query_id
        AND s.collection_time >= t.read_from AND s.collection_time <= $2)
SELECT target_database, forced_plan_id, plan_id, SUM(execution_count), MAX(last_execution_time),
       " + WeightedAverage("avg_cpu_time_us") + @", " + WeightedAverage("avg_duration_us") + @", MAX(max_duration_us), MIN(first_execution_time)
FROM d
WHERE rn = 1 AND last_execution_time > since
GROUP BY target_database, forced_plan_id, plan_id";

    /// <summary>The columns that identify one Query Store interval of one plan: the dedupe key of every stats read here (#1841).</summary>
    private const string IntervalIdentity = "s.runtime_stats_interval_id, s.first_execution_time, s.execution_type_desc, s.replica_role";

    /// <summary>The execution-weighted mean of a per-execution average over the counted intervals (#5632, step 8): the sum of average times
    /// executions over the executions of the rows that carry an average. NULL when no counted row carries one.</summary>
    private static string WeightedAverage(string column) =>
        $"SUM({column}::numeric * execution_count) / NULLIF(SUM(execution_count) FILTER (WHERE {column} IS NOT NULL), 0)";

    /// <summary>
    /// What every plan of one query did in a window that ends at the force (#5632, step 8), with the same interval dedupe as
    /// <see cref="StatsSql"/>: only the newest stored snapshot of each interval counts, here the newest one collected by the window's end,
    /// so an interval that was open at the force is counted as it stood at the last collection before it. $1 server, $2 database, $3 query_id,
    /// $4 window start, $5 window end (both naive UTC, on <c>collection_time</c>). Per plan: executions, execution-weighted CPU and duration
    /// per execution (microseconds), the largest maximum duration, the start of the earliest interval and the last execution seen.
    /// </summary>
    public static readonly string WindowStatsSql = @"
WITH d AS (
    SELECT s.plan_id, s.execution_count, s.last_execution_time, s.avg_cpu_time_us, s.avg_duration_us, s.max_duration_us, s.first_execution_time,
           ROW_NUMBER() OVER (PARTITION BY s.plan_id, " + IntervalIdentity + @"
                              ORDER BY s.collection_time DESC, s.execution_count DESC) AS rn
    FROM query_store_stats AS s
    WHERE s.server_id = $1 AND s.database_name = $2 AND s.query_id = $3 AND s.collection_time >= $4 AND s.collection_time <= $5)
SELECT plan_id, SUM(execution_count), MAX(last_execution_time),
       " + WeightedAverage("avg_cpu_time_us") + @", " + WeightedAverage("avg_duration_us") + @", MAX(max_duration_us), MIN(first_execution_time)
FROM d
WHERE rn = 1
GROUP BY plan_id
ORDER BY SUM(execution_count) DESC, plan_id
LIMIT 50";

    private sealed record Inspected(PlanKind Kind, string? Guide);

    private sealed record Force(
        string Database, long PlanId, long QueryId, string? ForcingType, long FailureCount, string? FailureReason,
        DateTime LastSeen, DateTime FirstSeen, DateTime? Newest, string? VariantPlanIds = null, ForcedPlanGuide? Guide = null);

    /// <summary>The ids in a collector's <c>variant_plan_ids</c> text (<c>12,15</c>); unreadable parts are skipped.</summary>
    internal static IReadOnlySet<long> ParseVariantPlanIds(string? text)
    {
        var ids = new HashSet<long>();
        if (string.IsNullOrWhiteSpace(text)) return ids;
        foreach (var part in text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            if (long.TryParse(part, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var id))
                ids.Add(id);
        return ids;
    }

    /// <summary>What a plan did after a force: the executions of its intervals that ended after the force, and the newest of those ends.</summary>
    public readonly record struct Ran(long Executions, DateTime? LastExecution, double? AvgCpuUs = null, double? AvgDurationUs = null,
        long? MaxDurationUs = null, DateTime? FirstExecution = null);

    /// <summary>One plan of a query in <see cref="WindowStatsSql"/>'s window.</summary>
    public sealed record PlanWindow(long PlanId, Ran Run);

    private static Ran ReadRan(NpgsqlDataReader reader, int executions) => new(
        reader.IsDBNull(executions) ? 0 : (long)reader.GetDecimal(executions),
        reader.IsDBNull(executions + 1) ? null : reader.GetDateTime(executions + 1),
        reader.IsDBNull(executions + 2) ? null : (double)reader.GetDecimal(executions + 2),
        reader.IsDBNull(executions + 3) ? null : (double)reader.GetDecimal(executions + 3),
        reader.IsDBNull(executions + 4) ? null : reader.GetInt64(executions + 4),
        reader.IsDBNull(executions + 5) ? null : reader.GetDateTime(executions + 5));

    /// <summary>
    /// What every plan of one query did from <paramref name="start"/> to <paramref name="end"/> (naive UTC, on the stored rows'
    /// <c>collection_time</c>), most executions first, at most 50 plans (<see cref="WindowStatsSql"/>).
    /// </summary>
    public static async Task<IReadOnlyList<PlanWindow>> ReadWindowAsync(
        NpgsqlDataSource postgres, int serverId, string database, long queryId, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var plans = new List<PlanWindow>();
        await using var command = postgres.CreateCommand(WindowStatsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, database);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, queryId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* Columns: plan_id, executions, last execution, CPU, duration, max duration, first execution: ReadRan wants them in the stats
               read's order (executions, last, cpu, duration, max, first), which this select keeps. */
            plans.Add(new PlanWindow(reader.GetInt64(0), ReadRan(reader, 1)));
        }

        return plans;
    }

    /// <summary>
    /// The state, from the facts alone. REMOVED first (an absent force has no state to speak of), then FAILING (a failure count or a
    /// reason beats what ran), then IGNORED (another plan of the query ran after the force), WORKING (the forced plan ran, or a variant
    /// of it did: SQL Server recompiled the force under a new plan id, #6160) and NOT_EXERCISED (nothing ran). A variant is never
    /// <paramref name="otherPlanRan"/>. A later plan whose XML could not be checked (<paramref name="unknownPlanRan"/>, #6221) is UNKNOWN
    /// unless a definite other plan ran (IGNORED), whatever the forced plan or a variant did: the unknown plan could be either.
    /// </summary>
    public static ForcedPlanState Classify(bool inNewestCapture, long failureCount, string? failureReason, bool otherPlanRan, bool forcedPlanRan,
        bool variantPlanRan = false, bool unknownPlanRan = false)
    {
        forcedPlanRan |= variantPlanRan;
        if (!inNewestCapture) return ForcedPlanState.Removed;
        if (failureCount > 0 || !string.IsNullOrWhiteSpace(failureReason)) return ForcedPlanState.Failing;
        if (otherPlanRan) return ForcedPlanState.Ignored;
        if (unknownPlanRan) return ForcedPlanState.UnknownPlanNotCollected;
        return forcedPlanRan ? ForcedPlanState.Working : ForcedPlanState.NotExercised;
    }

    private static string UnknownReasonText(List<ForcedPlanOtherPlan> unknowns, int notChecked)
    {
        var ids = string.Join(", ", unknowns.Take(10).Select(u => u.PlanId.ToString(System.Globalization.CultureInfo.InvariantCulture)))
                  + (unknowns.Count > 10 ? ", ..." : "");
        var text = $"Later plan(s) {ids} of the query ran after the force, and whether they are variants of the forced plan (the force running) or other plans (the force ignored) is not known: ";
        var notCollected = unknowns.Count - notChecked;
        if (notCollected > 0)
            text += $"{notCollected} of them have no plan XML in the store yet (the Query Store collector has not fetched it)";
        if (notChecked > 0)
            text += (notCollected > 0 ? "; " : "") + $"{notChecked} were not looked up because more than {MaxStoredPlanChecks} later plans ran in this read";
        return text + ".";
    }

    /// <summary>
    /// Looks up the stored XML of the later plans that ran (#6221): plans of a forced query with a higher plan id than the forced plan, that ran
    /// after the force, and that no collector row already named a variant. At most <see cref="MaxStoredPlanChecks"/> plans, taken force by force,
    /// busiest first; a plan not in the result was not looked up. A plan whose XML is not in the store is <see cref="PlanKind.Unknown"/>.
    /// </summary>
    private static async Task<Dictionary<(string Database, long Plan), Inspected>> InspectLaterPlansAsync(
        NpgsqlDataSource postgres, int serverId, List<Force> live, Dictionary<(string Database, long ForcedPlan, long Plan), Ran> ran,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var result = new Dictionary<(string Database, long Plan), Inspected>();
        if (live.Count == 0 || ran.Count == 0) return result;

        var byForce = ran.GroupBy(r => (r.Key.Database, r.Key.ForcedPlan)).ToDictionary(g => g.Key, g => g.ToList());
        var wanted = new List<(string Database, long Plan)>();
        var seen = new HashSet<(string Database, long Plan)>();
        foreach (var f in live)
        {
            if (!byForce.TryGetValue((f.Database, f.PlanId), out var plans)) continue;
            var legacy = ParseVariantPlanIds(f.VariantPlanIds);
            foreach (var r in plans.Where(r => r.Key.Plan > f.PlanId && !legacy.Contains(r.Key.Plan))
                         .OrderByDescending(r => r.Value.Executions).ThenBy(r => r.Key.Plan))
            {
                var key = (f.Database, r.Key.Plan);
                if (seen.Contains(key)) continue;
                if (wanted.Count >= MaxStoredPlanChecks) break;
                seen.Add(key);
                wanted.Add(key);
            }
        }

        if (wanted.Count == 0) return result;

        var found = new Dictionary<(string Database, long Plan), string?>();
        await using (var command = postgres.CreateCommand(StoredPlansSql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, wanted.Select(w => w.Database).ToArray());
            command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, wanted.Select(w => w.Plan).ToArray());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var text = reader.IsDBNull(2) ? null : reader.GetString(2);
                var gzip = reader.IsDBNull(3) ? null : (byte[])reader.GetValue(3);
                found[(reader.GetString(0), reader.GetInt64(1))] = PayloadDimensions.ResolveContent(text, gzip);
            }
        }

        foreach (var w in wanted)
        {
            if (found.TryGetValue(w, out var xml) && !string.IsNullOrEmpty(xml))
            {
                var (kind, guide) = InspectPlanXml(xml);
                result[w] = new Inspected(kind, guide);
            }
            else
            {
                result[w] = new Inspected(PlanKind.Unknown, null);
            }
        }

        return result;
    }

    /// <summary>
    /// Reads the forces of one server in the window <paramref name="start"/> to <paramref name="end"/> (naive UTC), optionally
    /// narrowed to databases, to one query and to one plan id (<paramref name="planId"/>, a trailing parameter). <paramref name="forcedAt"/> is the seam for a caller that knows when a force was
    /// placed (key: database and plan_id): the time replaces first seen for that force's IGNORED and WORKING rules.
    /// </summary>
    public static Task<ForcedPlanRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, long? queryId, DateTime start, DateTime end,
        IReadOnlyDictionary<(string Database, long PlanId), DateTime>? forcedAt = null,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        long? planId = null,
        CancellationToken cancellationToken = default) =>
        ReadAsync(postgres, serverId, databases, queryId, start, end, MaxForces, forcedAt, commandTimeoutSeconds, planId, cancellationToken);

    /// <summary><see cref="ReadAsync(NpgsqlDataSource,int,DatabaseFilter,long?,DateTime,DateTime,IReadOnlyDictionary{ValueTuple{string,long},DateTime},int,long?,CancellationToken)"/> with the row cap given (a test lowers it).</summary>
    internal static async Task<ForcedPlanRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, long? queryId, DateTime start, DateTime end,
        int maxForces,
        IReadOnlyDictionary<(string Database, long PlanId), DateTime>? forcedAt = null,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        long? planId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var from = DateTime.SpecifyKind(start, DateTimeKind.Unspecified);
        var to = DateTime.SpecifyKind(end, DateTimeKind.Unspecified);

        var captures = new List<(string Database, DateTime Newest)>();
        await using (var command = postgres.CreateCommand(CapturesSql(databases)))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
            command.Parameters.Add(databases.Parameter());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                captures.Add((reader.GetString(0), reader.GetDateTime(1)));
        }

        var forces = new List<Force>();
        var readCapped = false;
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        if (captures.Count > 0)
        {
            await using (var countCommand = postgres.CreateCommand(CountsSql(databases)))
            {
                countCommand.CommandTimeout = commandTimeoutSeconds;
                countCommand.Parameters.AddWithValue(serverId);
                countCommand.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
                countCommand.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
                countCommand.Parameters.Add(databases.Parameter());
                countCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId.HasValue ? queryId.Value : DBNull.Value });
                countCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = planId.HasValue ? planId.Value : DBNull.Value });
                await using var countReader = await countCommand.ExecuteReaderAsync(cancellationToken);
                while (await countReader.ReadAsync(cancellationToken))
                    counts[countReader.GetString(0)] = checked((int)countReader.GetInt64(1));
            }

            await using var command = postgres.CreateCommand(ForcesSql(databases, maxForces));
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
            command.Parameters.Add(databases.Parameter());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId.HasValue ? queryId.Value : DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = planId.HasValue ? planId.Value : DBNull.Value });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (forces.Count == maxForces)
                {
                    /* The one row past the cap: the read was cut, and this row is not kept. */
                    readCapped = true;
                    break;
                }

                forces.Add(new Force(
                    reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetDateTime(6),
                    reader.IsDBNull(7) ? reader.GetDateTime(6) : reader.GetDateTime(7),
                    reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                    reader.IsDBNull(9) ? null : reader.GetString(9),
                    reader.IsDBNull(10) ? null : new ForcedPlanGuide(reader.GetString(10), reader.IsDBNull(11) ? null : reader.GetBoolean(11), reader.IsDBNull(12) ? null : reader.GetString(12))));
            }
        }

        var newestByDatabase = captures.ToDictionary(c => c.Database, c => c.Newest, StringComparer.Ordinal);
        bool InNewest(Force f) => newestByDatabase.TryGetValue(f.Database, out var newest) && f.LastSeen >= newest;

        DateTime SinceOf(Force f) =>
            forcedAt is not null && forcedAt.TryGetValue((f.Database, f.PlanId), out var at) ? at : f.FirstSeen;

        var live = forces.Where(InNewest).ToList();
        var ran = new Dictionary<(string Database, long ForcedPlan, long Plan), Ran>();
        DateTime? floor = null;
        var complete = true;
        if (live.Count > 0)
        {
            var since = live.Select(SinceOf).ToArray();
            var readFrom = since.Select(s => s < from ? from : s).ToArray();
            await using (var command = postgres.CreateCommand(StatsSql))
            {
                command.CommandTimeout = commandTimeoutSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
                command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, live.Select(f => f.Database).ToArray());
                command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, live.Select(f => f.QueryId).ToArray());
                command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, live.Select(f => f.PlanId).ToArray());
                command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, since.Select(s => DateTime.SpecifyKind(s, DateTimeKind.Unspecified)).ToArray());
                command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, readFrom.Select(s => DateTime.SpecifyKind(s, DateTimeKind.Unspecified)).ToArray());
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    ran[(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2))] = ReadRan(reader, 3);
                }
            }

            var oldestRead = readFrom.Min();
            floor = await RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.QueryStoreStats, serverId, oldestRead, to, commandTimeoutSeconds, cancellationToken);
            complete = floor is not null && !RawWindowFloor.IsTruncated(floor, oldestRead);
        }

        var inspected = await InspectLaterPlansAsync(postgres, serverId, live, ran, commandTimeoutSeconds, cancellationToken);

        var facts = new List<ForcedPlanFact>(forces.Count);
        foreach (var f in forces)
        {
            var since = SinceOf(f);
            DateTime? forcedAtValue = forcedAt is not null && forcedAt.TryGetValue((f.Database, f.PlanId), out var at) ? at : null;
            var inNewest = InNewest(f);
            var variantIds = ParseVariantPlanIds(f.VariantPlanIds);
            var ranOfQuery = ran
                .Where(r => r.Key.Database == f.Database && r.Key.ForcedPlan == f.PlanId && r.Key.Plan != f.PlanId && r.Value.Executions + 1 > 0)
                .Select(r => new ForcedPlanOtherPlan(r.Key.Plan, r.Value.Executions, r.Value.LastExecution, r.Value))
                .OrderByDescending(o => o.Executions).ThenBy(o => o.PlanId)
                .ToList();
            /* #6160: a variant (a plan the engine compiled to run the force) is the force running, not another plan. #6221: the collector reads no
               plan XML now, so a LATER plan is a variant, a guided plan or another plan by its stored XML; one with no stored XML is unknown. */
            var others = new List<ForcedPlanOtherPlan>();
            var variants = new List<ForcedPlanOtherPlan>();
            var unknowns = new List<ForcedPlanOtherPlan>();
            string? xmlGuide = null;
            var notChecked = 0;
            foreach (var o in ranOfQuery)
            {
                if (variantIds.Contains(o.PlanId))
                {
                    variants.Add(o);
                }
                else if (o.PlanId < f.PlanId)
                {
                    others.Add(o);
                }
                else if (!inspected.TryGetValue((f.Database, o.PlanId), out var what))
                {
                    unknowns.Add(o);
                    notChecked++;
                }
                else if (what.Kind == PlanKind.Unknown)
                {
                    unknowns.Add(o);
                }
                else if (what.Kind == PlanKind.Variant)
                {
                    variants.Add(o);
                }
                else
                {
                    others.Add(o);
                    if (what.Kind == PlanKind.Guided && what.Guide is not null && (xmlGuide is null || string.CompareOrdinal(what.Guide, xmlGuide) < 0))
                        xmlGuide = what.Guide;
                }
            }

            ran.TryGetValue((f.Database, f.PlanId, f.PlanId), out var own);
            var state = Classify(inNewest, f.FailureCount, f.FailureReason, others.Count > 0, own.LastExecution is not null, variants.Count > 0, unknowns.Count > 0);
            var guide = f.Guide ?? (xmlGuide is null ? null : new ForcedPlanGuide(xmlGuide, null, null));
            var unknownReason = unknowns.Count == 0 ? null : UnknownReasonText(unknowns, notChecked);
            facts.Add(new ForcedPlanFact(
                f.Database, f.QueryId, f.PlanId, state, f.FirstSeen, f.LastSeen, forcedAtValue, since, f.ForcingType,
                f.FailureCount, f.FailureReason, own.Executions, own.LastExecution, others, own.Executions > 0 || own.LastExecution is not null ? own : null,
                variants, guide, unknowns.Count == 0 ? null : unknowns, unknownReason));
        }

        var captureList = captures
            .Select(c => new ForcedPlanCapture(c.Database, c.Newest, counts.TryGetValue(c.Database, out var n) ? n : 0))
            .ToList();
        return new ForcedPlanRead(facts, captureList, floor, complete, readCapped);
    }
}
