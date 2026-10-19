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
    /// <summary>The forced plan ran after the force was first seen, and no other plan of the query did.</summary>
    Working,

    /// <summary>No plan of the query ran after the force was first seen.</summary>
    NotExercised,

    /// <summary>Another plan of the query ran after the force was first seen: the engine ran a different plan while the force was set.</summary>
    Ignored,

    /// <summary>The engine counts a force failure, or names a failure reason.</summary>
    Failing,

    /// <summary>The plan was forced in an earlier capture of its database inside the window and is absent from that database's newest capture.</summary>
    Removed,
}

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
    DarlingForcedPlanStateReader.Ran? ForcedRun = null);

/// <summary>The newest forced-plan capture of one database inside the window.</summary>
public sealed record ForcedPlanCapture(string DatabaseName, DateTime NewestCapture, int ForcedPlanCount);

/// <summary>What <see cref="DarlingForcedPlanStateReader.ReadAsync"/> found.</summary>
/// <param name="Plans">One entry per force, a plan in its database's newest capture or a REMOVED one.</param>
/// <param name="Captures">Each database that has a marker row in the window, with its own newest capture.</param>
/// <param name="StatsFloor">The oldest Query Store stats row in the span the states were read over; null when there is none.</param>
/// <param name="StatsComplete">False when the stored stats start after the oldest time a state is measured from, or hold nothing in the span: a
/// NOT_EXERCISED state is then a statement about what the store holds, not about the server.</param>
public sealed record ForcedPlanRead(
    IReadOnlyList<ForcedPlanFact> Plans,
    IReadOnlyList<ForcedPlanCapture> Captures,
    DateTime? StatsFloor,
    bool StatsComplete);

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
/// </summary>
public static class DarlingForcedPlanStateReader
{
    /// <summary>The most forces one read returns. A database has at most 500 per pass (the collector's cap).</summary>
    public const int MaxForces = 2000;

    /// <summary>The newest collector capture of each force, read from the window. $1 server, $2 start, $3 end, $4 database filter, $5 query_id or NULL.</summary>
    public static string ForcesSql(DatabaseFilter databases) => $@"
WITH win AS (
    SELECT database_name, collection_time, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason
    FROM collect.query_store_forced_plans
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3{databases.Clause("database_name", 4)}),
plans AS (
    SELECT * FROM win WHERE plan_id IS NOT NULL AND ($5::bigint IS NULL OR query_id = $5)),
absent AS (
    SELECT p.database_name, p.plan_id, MAX(m.collection_time) AS last_absent
    FROM (SELECT DISTINCT database_name, plan_id FROM plans) AS p
    JOIN win AS m ON m.database_name = p.database_name AND m.plan_id IS NULL
    WHERE NOT EXISTS (SELECT 1 FROM plans AS x WHERE x.database_name = p.database_name AND x.plan_id = p.plan_id AND x.collection_time = m.collection_time)
    GROUP BY p.database_name, p.plan_id),
newest AS (
    SELECT database_name, MAX(collection_time) AS newest_capture FROM win WHERE plan_id IS NULL GROUP BY database_name),
latest AS (
    SELECT DISTINCT ON (database_name, plan_id) database_name, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason, collection_time
    FROM plans ORDER BY database_name, plan_id, collection_time DESC)
SELECT l.database_name, l.plan_id, l.query_id, l.plan_forcing_type, l.force_failure_count, l.last_force_failure_reason, l.collection_time,
       (SELECT MIN(x.collection_time) FROM plans AS x WHERE x.database_name = l.database_name AND x.plan_id = l.plan_id AND (a.last_absent IS NULL OR x.collection_time > a.last_absent)),
       n.newest_capture
FROM latest AS l
LEFT JOIN absent AS a ON a.database_name = l.database_name AND a.plan_id = l.plan_id
LEFT JOIN newest AS n ON n.database_name = l.database_name
ORDER BY l.database_name, l.plan_id
LIMIT {MaxForces}";

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

    private sealed record Force(
        string Database, long PlanId, long QueryId, string? ForcingType, long FailureCount, string? FailureReason,
        DateTime LastSeen, DateTime FirstSeen, DateTime? Newest);

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
    /// reason beats what ran), then IGNORED (another plan of the query ran after the force), WORKING (the forced plan ran) and
    /// NOT_EXERCISED (nothing ran).
    /// </summary>
    public static ForcedPlanState Classify(bool inNewestCapture, long failureCount, string? failureReason, bool otherPlanRan, bool forcedPlanRan)
    {
        if (!inNewestCapture) return ForcedPlanState.Removed;
        if (failureCount > 0 || !string.IsNullOrWhiteSpace(failureReason)) return ForcedPlanState.Failing;
        if (otherPlanRan) return ForcedPlanState.Ignored;
        return forcedPlanRan ? ForcedPlanState.Working : ForcedPlanState.NotExercised;
    }

    /// <summary>
    /// Reads the forces of one server in the window <paramref name="start"/> to <paramref name="end"/> (naive UTC), optionally
    /// narrowed to databases and to one query. <paramref name="forcedAt"/> is the seam for a caller that knows when a force was
    /// placed (key: database and plan_id): the time replaces first seen for that force's IGNORED and WORKING rules.
    /// </summary>
    public static async Task<ForcedPlanRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, long? queryId, DateTime start, DateTime end,
        IReadOnlyDictionary<(string Database, long PlanId), DateTime>? forcedAt = null,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
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
        if (captures.Count > 0)
        {
            await using var command = postgres.CreateCommand(ForcesSql(databases));
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
            command.Parameters.Add(databases.Parameter());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId.HasValue ? queryId.Value : DBNull.Value });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                forces.Add(new Force(
                    reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetDateTime(6),
                    reader.IsDBNull(7) ? reader.GetDateTime(6) : reader.GetDateTime(7),
                    reader.IsDBNull(8) ? null : reader.GetDateTime(8)));
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

        var facts = new List<ForcedPlanFact>(forces.Count);
        foreach (var f in forces)
        {
            var since = SinceOf(f);
            DateTime? forcedAtValue = forcedAt is not null && forcedAt.TryGetValue((f.Database, f.PlanId), out var at) ? at : null;
            var inNewest = InNewest(f);
            var others = ran
                .Where(r => r.Key.Database == f.Database && r.Key.ForcedPlan == f.PlanId && r.Key.Plan != f.PlanId && r.Value.Executions + 1 > 0)
                .Select(r => new ForcedPlanOtherPlan(r.Key.Plan, r.Value.Executions, r.Value.LastExecution, r.Value))
                .OrderByDescending(o => o.Executions).ThenBy(o => o.PlanId)
                .ToList();
            ran.TryGetValue((f.Database, f.PlanId, f.PlanId), out var own);
            var state = Classify(inNewest, f.FailureCount, f.FailureReason, others.Count > 0, own.LastExecution is not null);
            facts.Add(new ForcedPlanFact(
                f.Database, f.QueryId, f.PlanId, state, f.FirstSeen, f.LastSeen, forcedAtValue, since, f.ForcingType,
                f.FailureCount, f.FailureReason, own.Executions, own.LastExecution, others, own.Executions > 0 || own.LastExecution is not null ? own : null));
        }

        var perDatabase = facts.Where(f => f.State != ForcedPlanState.Removed).GroupBy(f => f.DatabaseName, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var captureList = captures
            .Select(c => new ForcedPlanCapture(c.Database, c.Newest, perDatabase.TryGetValue(c.Database, out var n) ? n : 0))
            .ToList();
        return new ForcedPlanRead(facts, captureList, floor, complete);
    }
}
