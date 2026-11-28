using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The cost half of the PLAN_REGRESSION fact: the statement that finds the queries whose newest plan costs 2x or more of the best
/// plan, and the read that runs it. Split out of <c>CollectPlanRegressionFactsAsync</c> so a test can run the statement beside a
/// reference copy of it (#9195).
/// </summary>
public partial class DuckDbFactCollector
{
    internal static readonly string PlanRegressionCostSql = @"
-- #9681: a query yields a regression row only when its rows hold two distinct non-NULL plan hashes, because `compared` needs
-- l.query_plan_hash <> b.query_plan_hash. So the per-interval dedup below, the largest share of the statement's CPU, only needs those
-- queries' rows. A query that passes here keeps all its rows, NULL-hash rows included, so every later step sees what it saw before.
-- min and max skip NULL, so they differ exactly when there are two non-NULL hashes.
WITH multi_plan AS
(
    SELECT database_name, query_id, replica_role
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc = 'Regular'
    AND   last_execution_time >= $2
    AND   collection_time <= $3
    /*SEC*/
    /*DB*/
    GROUP BY database_name, query_id, replica_role
    HAVING min(query_plan_hash) <> max(query_plan_hash)
),
deduped AS
(
    -- Collapse incremental re-collections of the same open runtime-stats interval:
    -- keep only the latest collection_time row per logical interval.
    --
    -- #1850: replica_role is part of the interval's identity, not a passenger.
    -- sys.query_store_runtime_stats is keyed by (plan_id, interval, execution_type, replica_group), and
    -- on a SQL Server 2022+ AG with Query Store for secondary replicas enabled the primary holds ONE
    -- shared Query Store carrying every replica's rows. Two rows differing only in replica_role are
    -- distinct legitimate work, so a partition without it does not de-duplicate — it DISCARDS one
    -- replica's row at the rn = 1 filter. That is an under-count, which is strictly worse than the
    -- double-count this CTE exists to fix: a double-count is visible in the number, a dropped row is
    -- silent. Same reasoning, same key as the read side (#1845).
    -- execution_type_desc is correctly absent: the WHERE pins it to 'Regular', so it is constant here.
    -- #9195: no query_text here, or in the two grouping steps below: it is read once, for the candidates, in `texts`.
    -- #9195: one aggregate per interval, not ROW_NUMBER() OVER the same key. The window sorted every row of the 14-day window with
    -- its whole payload (most of the statement's time, whether or not any query regressed) to keep one row in a dozen. The newest
    -- row of an interval is the largest (collection_time, execution_count, collection_id), which is the old ORDER BY
    -- (collection_time DESC, execution_count DESC, collection_id DESC): a NULL execution_count sorted last there, so it is -1 here,
    -- below every count. The winner's fields ride in the struct, so they are the winner row's, as rn = 1 gave. collection_id is
    -- unique per row, so the key picks one row (a row held by both the hot table and the archive is the same row twice).
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        w.query_plan_hash AS query_plan_hash,
        w.execution_count AS execution_count,
        w.avg_cpu_time_us AS avg_cpu_time_us,
        w.avg_duration_us AS avg_duration_us,
        -- #5631: appended to the dedup's columns; feeds rows per call below.
        w.avg_rowcount AS avg_rowcount,
        w.last_execution_time AS last_execution_time,
        w.is_forced_plan AS is_forced_plan,
        w.force_failure_count AS force_failure_count,
        -- #9195: the row's key, carried so the candidates' text is read by it after the gates (`texts` below).
        w.collection_id AS collection_id
    FROM
    (
        SELECT
            database_name,
            query_id,
            plan_id,
            replica_role,
            max(
            {
                'collection_time': collection_time,
                'execution_rank': COALESCE(execution_count, -1),
                'collection_id': collection_id,
                'query_plan_hash': query_plan_hash,
                'execution_count': execution_count,
                'avg_cpu_time_us': avg_cpu_time_us,
                'avg_duration_us': avg_duration_us,
                'avg_rowcount': avg_rowcount,
                'last_execution_time': last_execution_time,
                'is_forced_plan': is_forced_plan,
                'force_failure_count': force_failure_count
            }) AS w
        FROM v_query_store_stats AS q
        SEMI JOIN multi_plan AS m
            ON  m.database_name = q.database_name
            AND m.query_id = q.query_id
            AND m.replica_role IS NOT DISTINCT FROM q.replica_role
        WHERE server_id = $1
        AND   execution_type_desc = 'Regular'
        AND   last_execution_time >= $2
        -- $3 is the window's end. A row collected after it was not in the store at that instant, so a pass as of a past
        -- instant ranks the plans that instant had; the dedup keeps each interval's newest row collected by then.
        AND   collection_time <= $3
        /*SEC*/
        -- #9353: one database at a time when the pass reads its databases in turn. The marker is gone from the statement otherwise.
        /*DB*/
        GROUP BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
    )
),
plan_agg AS
(
    -- Execution-weighted per-exec cost per plan_id, per replica. Keeping replica_role in the grain all
    -- the way down is what makes the wider dedup key an improvement rather than a blend: were it dropped
    -- here, both replicas' rows would survive the dedup and then be summed into one number, and primary
    -- and secondary workload would be indistinguishable in the output.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        MAX(last_execution_time) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count,
        MAX(collection_id) AS text_collection_id,
        -- #5631: rows returned per call, weighted by executions over the same deduplicated intervals as
        -- cpu_per_exec. NULL when any interval has no avg_rowcount: a partial average would understate the
        -- output, and the output rule must not fire on it.
        CASE WHEN COUNT(avg_rowcount) < COUNT(*) THEN NULL
             ELSE SUM(avg_rowcount * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0)
        END AS rows_per_exec
    FROM deduped
    GROUP BY database_name, query_id, plan_id, replica_role
),
plan_dedup AS
(
    -- Collapse plan_ids that share a query_plan_hash (a recompile can produce an
    -- identical plan under a new plan_id); keep only plans with enough executions.
    SELECT
        database_name,
        query_id,
        replica_role,
        query_plan_hash,
        SUM(execs) AS execs,
        SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0) AS dur_per_exec,
        MAX(last_exec) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count,
        -- #9195: the group's newest deduplicated row, whose text `texts` reads for a candidate.
        MAX(text_collection_id) AS text_collection_id,
        -- #5630: one representative plan_id per plan group, for the compiled-inputs check. The latest group's is
        -- the plan that ran last; the best group's is the one that ran most. Their XML is fetched live, for
        -- candidates only.
        -- Ties break on plan_id (the struct orders by its second field), as Darling's read does, so the pick is the
        -- same on every pass and in both products.
        arg_max(plan_id, (last_exec, plan_id)) AS newest_plan_id,
        arg_max(plan_id, (execs, plan_id)) AS busiest_plan_id,
        -- #5631: NULL when any plan_id of the group has none.
        CASE WHEN COUNT(rows_per_exec) < COUNT(*) THEN NULL
             ELSE SUM(rows_per_exec * execs) / NULLIF(SUM(execs), 0)
        END AS rows_per_exec
    FROM plan_agg
    GROUP BY database_name, query_id, replica_role, query_plan_hash
    HAVING SUM(execs) >= 25
),
ranked AS
(
    -- Rank WITHIN a replica: a regression means this replica's current plan is worse than the best plan
    -- this replica has run, never a cross-replica comparison of two different workloads.
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY last_exec DESC) AS recency,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY cpu_per_exec ASC) AS cheapness
    FROM plan_dedup
),
compared AS
(
    -- Latest active plan vs the best-performing plan for the same query on the same replica.
    -- IS NOT DISTINCT FROM, never =: replica_role is NULL on every standalone server, every non-AG
    -- server and everything below SQL Server 2022, and NULL = NULL is UNKNOWN — an equi-join here would
    -- match nothing and silently disable plan-regression detection for the overwhelming majority of
    -- installs. The NULL-safe operator groups those rows the same way the PARTITION BYs above do.
    SELECT
        l.query_id,
        -- #9195: the latest plan's group, so the statement text is read from that group's rows, as it was when it rode through the steps above.
        l.replica_role AS replica_role,
        l.query_plan_hash AS latest_plan_hash,
        l.cpu_per_exec AS latest_cpu,
        l.dur_per_exec AS latest_dur,
        l.is_forced_plan AS latest_is_forced,
        l.force_failure_count AS force_failure_count,
        b.cpu_per_exec AS best_cpu,
        b.dur_per_exec AS best_dur,
        -- #2138: CPU is the PRIMARY signal — duration alone is confounded by blocking, IO waits, and
        -- machine contention that no plan choice caused, so it must not fire a plan-regression verdict
        -- by itself. A CPU regression scores at its own ratio; a duration-dominant one fires only when
        -- EXTREME (>= 4x) AND corroborated by at least mild CPU worsening (>= 1.25x), scored at half
        -- the duration ratio so it competes honestly with CPU-detected rows. NULL when neither path
        -- fires — the >= 2 gate below drops it.
        CASE
            WHEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 2
                THEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0)
            WHEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) >= 4
             AND l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 1.25
                THEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) / 2
        END AS regression_factor,
        -- The resource-expenditure half of the importance gate (#2138): total CPU the LATEST plan burned
        -- over the window. The exec-count floor above only counts; this weighs.
        l.execs * l.cpu_per_exec AS latest_total_cpu_us,
        -- #7417: the CPU the regression lost in the window, the ranking key and a floor (PlanRegressionRanking): the
        -- per-execution difference times the latest plan's executions. A factor alone ranked a 455 us statement over a
        -- 1.2 s one. NULL cpu figures give NULL, which the floor drops.
        " + PerformanceMonitor.Analysis.PlanRegressionRanking.LostCpuExpressionSql + @" AS lost_cpu_us,
        l.database_name,
        -- #3953: when the best plan last ran, so the advice can state its age. The window reaches a full 14 days
        -- on both SKUs, so a best plan can be two weeks old.
        b.last_exec AS best_last_exec,
        -- #5630: the two groups' representative plans, for the OPTION (RECOMPILE) and compiled-inputs checks the C# side
        -- makes before it reports. The statement text comes after the ranking (#9195), see `texts` below.
        l.newest_plan_id AS latest_plan_id,
        b.busiest_plan_id AS best_plan_id,
        -- #5631: rows per call of the two plans, for the output rule (a newer plan that returns far more rows).
        l.rows_per_exec AS latest_rows,
        b.rows_per_exec AS best_rows,
        l.text_collection_id
    FROM ranked AS l
    JOIN ranked AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
      AND b.replica_role IS NOT DISTINCT FROM l.replica_role
      AND b.cheapness = 1
    WHERE l.recency = 1
    AND   l.query_plan_hash <> b.query_plan_hash
),
candidates AS
(
    -- The numeric gates, ahead of the text (#9195): the rows that can be offenders are a handful of the window's queries, so the
    -- statement text is read for those only.
    SELECT *
    FROM compared
    WHERE regression_factor >= 2
    -- 10 CPU-seconds across the window: a NOISE floor, not an importance ranking — it exists to exclude
    -- near-zero-cost queries whose ratios are all sampling jitter. Magnitude ranking is the ORDER BY below (#7417).
    AND   latest_total_cpu_us >= 10000000
    -- #7417: and a CPU-path regression must have LOST that much of it (PlanRegressionRanking). A duration-path row keeps the
    -- bar above: it got slower through waits more than CPU, so its CPU loss is small by construction.
    AND   " + PerformanceMonitor.Analysis.PlanRegressionRanking.LostCpuFloorSql + @"
),
texts AS
(
    /* #9195: the statement text of the candidates only, one row each: the newest deduplicated row of the plan group the
       candidate's latest plan belongs to (text_collection_id). The text rides through none of the steps above, and the read
       below fetches the candidates' rows by key, so it decodes the text of those rows and no others. The text of one query_id
       is the same on every row of it (Query Store keeps it once per query), so one row of the group gives the group's text.
       collection_id is unique per row (CollectionIdGenerator). The GROUP BY keeps one row per key if the hot table and the
       archive both hold a row, as they can between an archival's copy and its delete. That row passed the window, Regular and
       replica filters in `deduped`, so they are not repeated here. */
    SELECT s.collection_id, any_value(s.query_text) AS query_text
    FROM v_query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_id IN (SELECT text_collection_id FROM candidates)
    GROUP BY s.collection_id
)
SELECT
    c.query_id,
    c.latest_cpu,
    c.latest_dur,
    c.latest_is_forced,
    c.force_failure_count,
    c.best_cpu,
    c.best_dur,
    c.regression_factor,
    -- #3902: appended, so the ordinals above are untouched. With query_id it names each offender for the
    -- regressed-queries drill-down (AnalysisContext.PlanRegressionOffenders).
    c.database_name,
    -- #3953: appended for the same reason.
    c.best_last_exec,
    -- #5630: appended for the same reason.
    t.query_text,
    c.latest_plan_id,
    c.best_plan_id,
    -- #5631: appended for the same reason.
    c.latest_rows,
    c.best_rows,
    -- #9353: appended, the ranking key, so a read that took the databases one at a time can put their rows in this statement's order.
    c.lost_cpu_us,
    -- #9353: appended, the last part of that order's unique key (below).
    c.replica_role
FROM candidates AS c
LEFT JOIN texts AS t
  ON  t.collection_id = c.text_collection_id
-- #6548: SQL Server own housekeeping statements (change tracking cleanup and the like) are not application code. Left out
-- BEFORE the LIMIT below, so they cannot use up its rows or lead the fact.
WHERE NOT regexp_matches(lower(COALESCE(t.query_text, '')), '" + PerformanceMonitor.Common.InternalStatements.LowerTextPattern + @"')
-- #7417: by the CPU the regression lost, the factor second. The first row leads the card and its force statement.
-- #9353: then the row's own key, so two rows that tie on both come out in the same order every time, and a read that took the
-- databases one at a time (PlanRegressionRankKey) can put its rows in exactly this order. One row per (database, query, replica).
ORDER BY " + PerformanceMonitor.Analysis.PlanRegressionRanking.OrderBySql + @", c.database_name, c.query_id, c.replica_role
-- #5630: more rows than the fact keeps. The C# side drops the rows the shared input rule rules out and keeps
-- the first MaxPlanRegressionOffenders (20) of what is left, so a dropped row does not leave the fact one
-- offender short.
LIMIT 100";


    /// <summary>
    /// Runs <paramref name="sql"/> (the cost statement: $1 server, $2 the 14-day window start, $3 the window end, $4 and up the
    /// secondary-replica databases) on the caller's open connection, which holds the store's read lock.
    /// </summary>
    internal static async Task<List<PlanRegressionCandidate>> ReadPlanRegressionCandidatesAsync(
        DuckDBConnection connection, AnalysisContext context, ArchiveReadPlan plan, string sql)
        => (await ReadRankedPlanRegressionCandidatesAsync(connection, context, plan, sql, databases: null)).ConvertAll(r => r.Candidate);

    /// <summary>
    /// The cost statement's rows with the CPU each lost, the key the statement ranks by. <paramref name="databases"/> limits the read to
    /// those databases (#9353): every comparison in the statement stays inside one database, so its rows are those databases' rows of
    /// the whole read, and a pass that reads its databases in batches lets go of the store's lock between them.
    /// </summary>
    internal static async Task<List<RankedPlanRegression>> ReadRankedPlanRegressionCandidatesAsync(
        DuckDBConnection connection, AnalysisContext context, ArchiveReadPlan plan, string sql, IReadOnlyList<string>? databases,
        PlanRegressionCostWindow? window = null)
    {
        var scan = window ?? PlanRegressionCostWindow.Exact(context);
        var candidates = new List<RankedPlanRegression>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = scan.ScanStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = scan.ScanEnd });
        /* #5558: Query Store on a secondary copy is the primary's content, and rows from the days this node was
           primary stay in the window, so the read skips those databases. */
        cmd.CommandText = SecondaryReplicaScope.Apply(cmd.CommandText, cmd, context, "PLAN_REGRESSION", "database_name", 4);
        cmd.CommandText = plan.Apply(cmd.CommandText.Replace("/*DB*/", DatabaseFilter(cmd, databases)));
        using var reader = await cmd.ExecuteProfiledReaderAsync("plan-regression-cost", context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var candidate = new PlanRegressionCandidate(
                QueryId: reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0)),
                LatestCpu: reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                LatestIsForced: (!reader.IsDBNull(3) && Convert.ToBoolean(reader.GetValue(3))) ? 1 : 0,
                ForceFailureCount: reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4)),
                BestCpu: reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5)),
                RegressionFactor: reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7)),
                DatabaseName: reader.GetString(8),
                BestLastExec: reader.IsDBNull(9) ? null : Convert.ToDateTime(reader.GetValue(9)),
                QueryText: reader.IsDBNull(10) ? null : reader.GetString(10),
                LatestPlanId: reader.IsDBNull(11) ? null : ToInt64(reader.GetValue(11)),
                BestPlanId: reader.IsDBNull(12) ? null : ToInt64(reader.GetValue(12)),
                LatestRows: reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13)),
                BestRows: reader.IsDBNull(14) ? null : Convert.ToDouble(reader.GetValue(14)));
            /* NULL sorts last in the statement's ORDER BY, so a row with no lost CPU ranks below every other. */
            var lostCpu = reader.FieldCount > 15 && !reader.IsDBNull(15) ? Convert.ToDouble(reader.GetValue(15)) : double.NegativeInfinity;
            candidates.Add(new RankedPlanRegression(candidate, lostCpu, reader.FieldCount > 16 && !reader.IsDBNull(16) ? reader.GetString(16) : null));
        }
        return candidates;
    }

    /// <summary>A cost statement row with the two things its order needs besides the candidate's own fields.</summary>
    internal readonly record struct RankedPlanRegression(PlanRegressionCandidate Candidate, double LostCpu, string? ReplicaRole)
    {
        internal PlanRegressionRankKey Key => new(LostCpu, Candidate.RegressionFactor, Candidate.DatabaseName, Candidate.QueryId, ReplicaRole);
    }

    /// <summary>
    /// What the plan regression statements order by (#9353): the CPU the regression lost, the factor, then (database, query, replica),
    /// which is unique to a row. A pass that reads its databases in turn puts their rows through <see cref="InRankOrder"/>, and the
    /// result is the one statement's order, ties included. NULL sorts last in the statement, as -infinity does here.
    /// </summary>
    internal readonly record struct PlanRegressionRankKey(double LostCpu, double Factor, string Database, long QueryId, string? ReplicaRole);

    /// <summary>
    /// The statement's database predicate for its <c>/*DB*/</c> marker, adding the names to <paramref name="cmd"/> as parameters (#9353).
    /// No databases: no predicate, every database. A NULL database_name cannot reach the Query Store table (its column is NOT NULL), and a
    /// row with one would not reach a result anyway, because the statements pair a plan with the best plan of the same database
    /// (<c>b.database_name = l.database_name</c>), which a NULL never matches. So the rows never depend on how many databases the store has.
    /// </summary>
    internal static string DatabaseFilter(DuckDBCommand cmd, IReadOnlyList<string>? databases)
    {
        if (databases is null) return "";
        var markers = new List<string>();
        foreach (var database in databases)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = database });
            markers.Add("$" + cmd.Parameters.Count);
        }
        return $"AND database_name IN ({string.Join(", ", markers)})";
    }

    /// <summary>A database of the plan regression window and the rows it holds there, the measure of how long its read takes.</summary>
    internal readonly record struct PlanRegressionDatabase(string Name, long Rows);

    /// <summary>
    /// The longest hold of the store's read lock one batch of databases aims for (#9353): a fifth of the 5 s the housekeeping writers wait.
    /// </summary>
    internal static readonly TimeSpan PlanRegressionHoldTarget = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How many rows the next batch may read: the rows the last batch read, scaled by how far its hold was from <paramref name="target"/>.
    /// The rate is taken from the whole hold, so a batch whose hold was mostly the fixed cost of decoding the window (every read of
    /// the Parquet files decodes the same columns of every row group, whichever database it keeps) over-states the cost of a row, and
    /// the next batch is smaller than it could be, never larger.
    /// </summary>
    internal static long NextBatchRows(long rowsRead, TimeSpan held, TimeSpan target)
    {
        if (held <= TimeSpan.Zero) return long.MaxValue;
        var rows = rowsRead * (target.TotalSeconds / held.TotalSeconds);
        return rows >= long.MaxValue ? long.MaxValue : (long)rows;
    }

    internal static IOrderedEnumerable<T> InRankOrder<T>(IEnumerable<T> rows, Func<T, PlanRegressionRankKey> key) =>
        rows.OrderByDescending(r => key(r).LostCpu)
            .ThenByDescending(r => key(r).Factor)
            .ThenBy(r => key(r).Database, StringComparer.Ordinal)
            .ThenBy(r => key(r).QueryId)
            .ThenBy(r => key(r).ReplicaRole is null ? 1 : 0)
            .ThenBy(r => key(r).ReplicaRole, StringComparer.Ordinal);

    /// <summary>
    /// Runs a plan regression read (<paramref name="readOne"/>: its statement, on an open connection, for the listed databases, or for all
    /// of them when the list is null) over batches of databases, each batch under its own hold of the store's read lock, and returns every
    /// row. Every comparison in these statements stays inside one database, so a database's rows are its rows of the whole read, and a
    /// housekeeping writer waiting on its 5 s budget gets in between the reads (#9353). The databases are listed with their row counts
    /// first, under a hold of their own. One database, or none, is read as one statement. <paramref name="onlyDatabases"/> drops the
    /// databases a read cannot return a row for. The caller puts the rows in order and cuts them (<see cref="InRankOrder"/>).
    /// <para>
    /// Every read decodes the same columns of the window's Parquet row groups whichever databases it keeps, because rows are written in
    /// collection order and a row group holds several databases. A read per database multiplied the pass's time by the number of databases
    /// (20 databases took 4.7 times as long as one statement, 50 took 10.6 times), so the databases go in batches. The first batch is the
    /// largest database alone. Each next batch takes databases, largest first, while their rows fit what the last batch's hold says one
    /// <paramref name="holdTarget"/> can read (<see cref="NextBatchRows"/>), and always at least one. A database is never split, so one
    /// that holds longer than the target alone is a batch of its own.
    /// </para>
    /// <paramref name="betweenReads"/> is a test seam, run with the lock let go before each read after the first. The plan is taken once, before
    /// the first hold: a store reset between two holds moves the archive generation, and the plan then reads the views, which hold every row.
    /// </summary>
    internal static async Task<(List<T> Rows, PlanRegressionStatementTime Time)> ReadPlanRegressionByDatabaseAsync<T>(
        DuckDbInitializer duckDb, AnalysisContext context, ArchiveReadPlan plan,
        Func<DuckDBConnection, IReadOnlyList<string>?, Task<List<T>>> readOne, Action? betweenReads, ISet<string>? onlyDatabases = null,
        TimeSpan? holdTarget = null, PlanRegressionCostWindow? window = null)
    {
        var time = default(PlanRegressionStatementTime);
        List<PlanRegressionDatabase> databases;
        {
            var waitStarted = Stopwatch.GetTimestamp();
            using var readLock = duckDb.AcquireReadLock(context.CancellationToken);
            var heldFrom = Stopwatch.GetTimestamp();
            time.Wait += Stopwatch.GetElapsedTime(waitStarted, heldFrom);
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            var statementFrom = Stopwatch.GetTimestamp();
            databases = await ReadPlanRegressionDatabasesAsync(connection, context, plan, window);
            time.Statement += Stopwatch.GetElapsedTime(statementFrom);
            time.Held = Stopwatch.GetElapsedTime(heldFrom);
        }

        if (onlyDatabases is not null) databases = databases.Where(d => onlyDatabases.Contains(d.Name)).ToList();

        var rows = new List<T>();
        var target = holdTarget ?? PlanRegressionHoldTarget;
        var readBatches = databases.Count > 1;
        /* Largest first, name second, so the first batch is the one that sets the rate and the order is the same every time. */
        var pending = readBatches ? databases.OrderByDescending(d => d.Rows).ThenBy(d => d.Name, StringComparer.Ordinal).ToList() : new List<PlanRegressionDatabase>();
        var budget = 0L;
        var first = true;
        while (first || pending.Count > 0)
        {
            var batch = new List<string>();
            long batchRows = 0;
            if (readBatches)
            {
                /* The first batch is the largest database alone: nothing is known yet of how fast a row reads. */
                do
                {
                    batch.Add(pending[0].Name);
                    batchRows += pending[0].Rows;
                    pending.RemoveAt(0);
                }
                while (pending.Count > 0 && batchRows + pending[0].Rows <= budget);
            }

            if (!first) betweenReads?.Invoke();
            first = false;

            var waitStarted = Stopwatch.GetTimestamp();
            using var readLock = duckDb.AcquireReadLock(context.CancellationToken);
            var heldFrom = Stopwatch.GetTimestamp();
            time.Wait += Stopwatch.GetElapsedTime(waitStarted, heldFrom);
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            var statementFrom = Stopwatch.GetTimestamp();
            rows.AddRange(await readOne(connection, readBatches ? batch : null));
            time.Statement += Stopwatch.GetElapsedTime(statementFrom);
            var held = Stopwatch.GetElapsedTime(heldFrom);
            if (held > time.Held) time.Held = held;
            budget = NextBatchRows(batchRows, held, target);
        }

        return (rows, time);
    }

    /// <summary>
    /// The window the cost statement scans, and what keys the keep for it (#9957). <c>ScanStart</c> is the lower bound on
    /// <c>last_execution_time</c> (the window start less 14 days) and <c>ScanEnd</c> the upper bound on <c>collection_time</c>.
    /// </summary>
    internal readonly record struct PlanRegressionCostWindow(DateTime ScanStart, DateTime ScanEnd)
    {
        /// <summary>The context's own window: an anchored read, or a read whose store held no recent row to clamp to.</summary>
        internal static PlanRegressionCostWindow Exact(AnalysisContext context) =>
            new(context.TimeRangeStart.AddDays(-14), context.TimeRangeEnd);
    }

    /// <summary>How far before a clock window's end the newest row is looked for. A server with no row that recent keeps its exact window.</summary>
    internal static readonly TimeSpan PlanRegressionCostNewestRowLookback = TimeSpan.FromDays(1);

    /// <summary>
    /// The window the cost statement runs over for <paramref name="context"/> (#9957). An anchored read, or any read whose end is not the
    /// clock, keeps its exact window. A read whose end is the clock ends at the newest <c>collection_time</c> the store holds for the server
    /// at or before it: the statement cannot see a row after that, so the rows are the same, and the end no longer moves from one read to the
    /// next, so the reads between two collections share one scan. The start follows from that end and the window's length. The newest row is
    /// read through the archive view, looking back <see cref="PlanRegressionCostNewestRowLookback"/> so the read prunes the archive; a store
    /// with no row that recent for the server, or a read that fails, keeps the exact window. The key does not see rows stored below the newest
    /// <c>collection_time</c> (the backfill worker's, a repair's deletes), so those reach a read once the keep's lifetime ends.
    /// </summary>
    internal async Task<PlanRegressionCostWindow> ResolvePlanRegressionCostWindowAsync(AnalysisContext context)
    {
        var exact = PlanRegressionCostWindow.Exact(context);
        if (!context.WindowEndsAtClock) return exact;
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT max(collection_time) FROM v_query_store_stats WHERE server_id = $1 AND collection_time <= $2 AND collection_time >= $3";
            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd - PlanRegressionCostNewestRowLookback });
            var value = await cmd.ExecuteScalarAsync(context.CancellationToken);
            if (value is not DateTime at) return exact;
            var newest = DateTime.SpecifyKind(at, DateTimeKind.Utc);
            return new PlanRegressionCostWindow(newest - (context.TimeRangeEnd - context.TimeRangeStart) - TimeSpan.FromDays(14), newest);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Warn("DuckDbFactCollector", $"The plan regression cost window read failed for server {context.ServerId}; the read keeps its exact window: {ex.Message}");
            return exact;
        }
    }

    /// <summary>
    /// The cost statement's candidates in its own order and cut (#9353): the CPU lost, the factor second, the row's key last, the first
    /// <see cref="PlanRegressionCostCap"/> (#9888). Read in batches of databases (<see cref="ReadPlanRegressionByDatabaseAsync{T}"/>), and the
    /// same rows in the same order as one statement over every database gives, ties included. The rows the statement returned beyond the cap
    /// are counted in <see cref="PlanRegressionStatementTime.LeftOut"/>. A read for a server, scan window and secondary-database scope that
    /// was made under <see cref="PlanRegressionCostKeep.KeepFor"/> ago is answered from the rows it returned, without the statement
    /// (#9888): the periodic pass, a user's read and a repeated compare share one scan. The keep belongs to the store, so every collector
    /// on it shares it (#9957). The kept rows are the statement's whole result, so the cap is applied to them on every read.
    /// <paramref name="window"/> is the window <see cref="ResolvePlanRegressionCostWindowAsync"/> returned, with <paramref name="plan"/> planned
    /// for its start; null resolves it here.
    /// </summary>
    internal async Task<(List<PlanRegressionCandidate> Candidates, PlanRegressionStatementTime Time)> ReadPlanRegressionCostAsync(
        AnalysisContext context, ArchiveReadPlan plan, PlanRegressionCostWindow? window = null)
    {
        var cap = PlanRegressionCostCapForTests ?? PlanRegressionCostCap;
        var scan = window ?? await ResolvePlanRegressionCostWindowAsync(context);
        var secondaries = context.SecondaryReplicaDatabases is { Count: > 0 } set ? string.Join('\u001f', set.OrderBy(d => d, StringComparer.Ordinal)) : "";
        var key = new PlanRegressionCostKeep.Key(context.ServerId, scan.ScanStart, scan.ScanEnd, secondaries);
        var keep = CostKeep;
        var ranked = keep.TryGet(key);

        var time = default(PlanRegressionStatementTime);
        if (ranked is null)
        {
            Interlocked.Increment(ref _planRegressionCostStatementRuns);
            (ranked, time) = await ReadPlanRegressionByDatabaseAsync(
                _duckDb, context, plan,
                (connection, databases) => ReadRankedPlanRegressionCandidatesAsync(connection, context, plan, PlanRegressionCostSql, databases, scan),
                AfterPlanRegressionDatabaseReadForTests, holdTarget: PlanRegressionHoldTargetForTests, window: scan);
            time.KeepHitsSincePreviousStatement = keep.Store(key, ranked);
        }
        else
        {
            time.FromKeep = true;
        }

        var ordered = InRankOrder(ranked, r => r.Key).ToList();
        time.LeftOut = Math.Max(0, ordered.Count - cap);
        return (ordered.Take(cap).Select(r => r.Candidate).ToList(), time);
    }

    /// <summary>
    /// #9888: the most candidates one cost read prices. The fact keeps <see cref="MaxPlanRegressionOffenders"/> (20) and the plan check
    /// fetches at most <see cref="MaxPlanInputFetches"/> (20) plans, so a candidate past the first 50 by lost CPU is never read. The
    /// rows past it are counted in the reply and the log line, as the plan check names the candidates its fetch budget did not reach.
    /// </summary>
    internal const int PlanRegressionCostCap = 50;

    /// <summary>A test seam: the cap, so a test can set it below the candidates it seeds. Null in production.</summary>
    internal int? PlanRegressionCostCapForTests { get; set; }

    private PlanRegressionCostKeep? _costKeep;

    /// <summary>
    /// The keep this collector reads and fills. A collector built on its own holds a private keep; <see cref="AnalysisService"/> sets the
    /// store's keep (<see cref="PlanRegressionCostKeep.For"/>) on the collector it builds, so the pass and every MCP call share one (#9957).
    /// </summary>
    internal PlanRegressionCostKeep CostKeep
    {
        get => _costKeep ??= new PlanRegressionCostKeep();
        set => _costKeep = value;
    }

    private int _planRegressionCostStatementRuns;

    /// <summary>A test seam: how many times the cost statement ran for this collector, a read answered from the keep not counted (#9888).</summary>
    internal int PlanRegressionCostStatementRunsForTests => Volatile.Read(ref _planRegressionCostStatementRuns);

    /// <summary>The most rows the cost statement returns, and so the most a pass that reads its databases in turn keeps of all of them.</summary>
    internal const int PlanRegressionCandidateLimit = 100;

    /// <summary>
    /// The databases that hold Regular runs inside the cost statement's window, for the pass that reads them in turn (#9353). It is
    /// read under its own short hold of the store's read lock.
    /// </summary>
    internal static async Task<List<PlanRegressionDatabase>> ReadPlanRegressionDatabasesAsync(
        DuckDBConnection connection, AnalysisContext context, ArchiveReadPlan plan, PlanRegressionCostWindow? window = null)
    {
        var scan = window ?? PlanRegressionCostWindow.Exact(context);
        var databases = new List<PlanRegressionDatabase>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = plan.Apply(@"
SELECT database_name, count(*)
FROM v_query_store_stats
WHERE server_id = $1
AND   execution_type_desc = 'Regular'
AND   last_execution_time >= $2
AND   collection_time <= $3
AND   database_name IS NOT NULL
GROUP BY database_name
ORDER BY database_name");
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = scan.ScanStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = scan.ScanEnd });
        using var reader = await cmd.ExecuteProfiledReaderAsync("plan-regression-databases", context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            databases.Add(new PlanRegressionDatabase(reader.GetString(0), Convert.ToInt64(reader.GetValue(1))));
        }
        return databases;
    }
}
