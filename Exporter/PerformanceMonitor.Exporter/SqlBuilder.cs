namespace PerformanceMonitor.Exporter;

/// <summary>Builds the SQL text the exporter runs against a store for each table it exports.</summary>
public static class SqlBuilder
{
    /// <summary>
    /// Quotes a raw identifier the way PostgreSQL expects for a double-quoted identifier:
    /// wrap in double quotes and double up any embedded double quote.
    /// </summary>
    public static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>
    /// The `@from` / `@to` parameters for a window read over `collection_time`. Store timestamp columns are
    /// naive UTC `timestamp`, not `timestamptz`: a Kind=Utc DateTime makes Npgsql infer timestamptz and Postgres
    /// silently shifts the value by the session's time zone, so the Kind must be Unspecified here to bind it as
    /// a plain timestamp. The bounds are passed through exactly (minute precision for a sub-hour window).
    /// </summary>
    public static Npgsql.NpgsqlParameter[] WindowParameters(DateTime fromUtc, DateTime toUtc) =>
    [
        new Npgsql.NpgsqlParameter("from", NpgsqlTypes.NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified) },
        new Npgsql.NpgsqlParameter("to", NpgsqlTypes.NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(toUtc, DateTimeKind.Unspecified) },
    ];

    /// <summary>
    /// Builds the SELECT for a raw table: an explicit, ordinal-ordered column list (discovered
    /// at runtime, minus server_id) filtered on the export window. Every raw table's time
    /// column is collection_time.
    /// </summary>
    public static string BuildRawSelect(string table, IReadOnlyList<string> columns)
    {
        if (columns.Count == 0)
        {
            throw new ArgumentException($"Table '{table}' has no exportable columns (after dropping server_id).");
        }

        var columnList = string.Join(", ", columns.Select(QuoteIdentifier));
        return $"/* darling-export:raw.{table} */ SELECT {columnList} FROM collect.{QuoteIdentifier(table)} " +
               "WHERE collection_time >= @from AND collection_time < @to";
    }

    /// <summary>
    /// The information_schema query that discovers a raw table's exportable columns, in
    /// ordinal order, with server_id excluded (server_id is local to one store and must
    /// never leave it).
    /// </summary>
    public const string RawColumnDiscoverySql =
        "/* darling-export:raw.column_discovery */ SELECT column_name FROM information_schema.columns " +
        "WHERE table_schema = 'collect' AND table_name = @table AND column_name <> 'server_id' " +
        "ORDER BY ordinal_position";

    /// <summary>
    /// query_stats rolled up to the hour. Excludes rows where sample_interval_seconds = 0:
    /// the collector writes that when it could not compute a real delta (first sighting of
    /// the counter, a counter reset, or a collection gap), and the delta columns are 0 in
    /// that case, so summing them in would silently understate the hour.
    ///
    /// kept only as a test oracle. No code path passes this to an NpgsqlCommand any more --
    /// <see cref="QueryStatsHourlyDuckDbSql"/> computes the identical rollup in DuckDB, over
    /// the single query_stats read shared with plan_shape/query_text (<see
    /// cref="QueryStatsWindowCache"/>).
    /// </summary>
    public const string QueryStatsHourlySql = """
        /* darling-export:query_stats_hourly */
        SELECT server_name, database_name, query_hash, query_plan_hash, date_trunc('hour', collection_time) AS hour,
               sum(delta_execution_count)::bigint AS execution_count_sum,
               sum(delta_worker_time)::bigint AS worker_time_sum, min(delta_worker_time) AS worker_time_min, max(delta_worker_time) AS worker_time_max,
               sum(delta_elapsed_time)::bigint AS elapsed_time_sum, min(delta_elapsed_time) AS elapsed_time_min, max(delta_elapsed_time) AS elapsed_time_max,
               sum(delta_logical_reads)::bigint AS logical_reads_sum, sum(delta_logical_writes)::bigint AS logical_writes_sum,
               sum(delta_physical_reads)::bigint AS physical_reads_sum, sum(delta_rows)::bigint AS rows_sum, sum(delta_spills)::bigint AS spills_sum,
               sum(sample_interval_seconds)::bigint AS sample_interval_seconds_sum, count(*) AS sample_count
        FROM collect.query_stats
        WHERE collection_time >= @from AND collection_time < @to AND sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY server_name, database_name, query_hash, query_plan_hash, date_trunc('hour', collection_time)
        """;

    /// <summary>
    /// the single store read shared by `query_stats_hourly`, `plan_shape` (origin
    /// query_stats) and `query_text` (origin query_stats) -- a plain projection of
    /// `collect.query_stats` over the export window, with no GROUP BY, sort or key derivation
    /// on the store side. Every rollup and key below runs against the DuckDB-side
    /// `query_stats_raw` table this loads (see <see cref="QueryStatsWindow"/>,
    /// <see cref="QueryStatsWindowCache"/>), replacing three separate store reads
    /// (<see cref="QueryStatsHourlySql"/>, <see cref="PlanShapeKeySql"/>, <see
    /// cref="QueryTextKeySql"/>) with one.
    /// </summary>
    public const string QueryStatsWindowSql = """
        /* darling-export:query_stats.window */
        SELECT server_name, database_name, query_hash, query_plan_hash, collection_time,
               sample_interval_seconds,
               delta_execution_count, delta_worker_time, delta_elapsed_time,
               delta_logical_reads, delta_logical_writes, delta_physical_reads,
               delta_rows, delta_spills,
               query_plan_digest, query_text_digest
        FROM collect.query_stats
        WHERE collection_time >= @from AND collection_time < @to
        """;

    /// <summary>
    /// <see cref="QueryStatsHourlySql"/>'s rollup, run in DuckDB over the already-loaded `query_stats_raw` table
    /// instead of a second read of `collect.query_stats`. The window predicate is already applied by the raw read, so
    /// this only re-applies the hourly query's own `sample_interval_seconds IS DISTINCT FROM 0` filter.
    /// `sum(...)::bigint` in Postgres becomes `CAST(sum(...) AS BIGINT)` in DuckDB: both are exact integer sums (every
    /// delta column here is bigint -- verified against the Darling PgMigrations/GoldenCollectorSchema DDL), and both
    /// casts raise on overflow rather than silently wrapping.
    /// </summary>
    public const string QueryStatsHourlyDuckDbSql = """
        SELECT server_name, database_name, query_hash, query_plan_hash, date_trunc('hour', collection_time) AS hour,
               CAST(sum(delta_execution_count) AS BIGINT) AS execution_count_sum,
               CAST(sum(delta_worker_time) AS BIGINT) AS worker_time_sum, min(delta_worker_time) AS worker_time_min, max(delta_worker_time) AS worker_time_max,
               CAST(sum(delta_elapsed_time) AS BIGINT) AS elapsed_time_sum, min(delta_elapsed_time) AS elapsed_time_min, max(delta_elapsed_time) AS elapsed_time_max,
               CAST(sum(delta_logical_reads) AS BIGINT) AS logical_reads_sum, CAST(sum(delta_logical_writes) AS BIGINT) AS logical_writes_sum,
               CAST(sum(delta_physical_reads) AS BIGINT) AS physical_reads_sum, CAST(sum(delta_rows) AS BIGINT) AS rows_sum, CAST(sum(delta_spills) AS BIGINT) AS spills_sum,
               CAST(sum(sample_interval_seconds) AS BIGINT) AS sample_interval_seconds_sum, count(*) AS sample_count
        FROM query_stats_raw
        WHERE sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY server_name, database_name, query_hash, query_plan_hash, date_trunc('hour', collection_time)
        ORDER BY server_name, hour
        """;

    /// <summary>
    /// procedure_stats rolled up to the hour. procedure_stats has no rows column and no
    /// plan-hash column in the raw table, so this rollup does not have them either. Same
    /// sample_interval_seconds = 0 exclusion rule as query_stats, and for the same reason.
    /// </summary>
    public const string ProcedureStatsHourlySql = """
        /* darling-export:procedure_stats_hourly */
        SELECT server_name, database_name, schema_name, object_name, date_trunc('hour', collection_time) AS hour,
               sum(delta_execution_count)::bigint AS execution_count_sum,
               sum(delta_worker_time)::bigint AS worker_time_sum, min(delta_worker_time) AS worker_time_min, max(delta_worker_time) AS worker_time_max,
               sum(delta_elapsed_time)::bigint AS elapsed_time_sum, min(delta_elapsed_time) AS elapsed_time_min, max(delta_elapsed_time) AS elapsed_time_max,
               sum(delta_logical_reads)::bigint AS logical_reads_sum, sum(delta_logical_writes)::bigint AS logical_writes_sum,
               sum(delta_physical_reads)::bigint AS physical_reads_sum, sum(delta_spills)::bigint AS spills_sum,
               sum(sample_interval_seconds)::bigint AS sample_interval_seconds_sum, count(*) AS sample_count
        FROM collect.procedure_stats
        WHERE collection_time >= @from AND collection_time < @to AND sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY server_name, database_name, schema_name, object_name, date_trunc('hour', collection_time)
        """;

    /// <summary>
    /// the single store read shared by `procedure_stats_hourly`, the procedure plan keys and the oversized-plan
    /// count -- a plain projection of `collect.procedure_stats` over the export window, with no GROUP BY, sort or key
    /// derivation on the store side. `(query_plan_xml IS NULL) AS plan_xml_is_null` and `(query_plan_xml_bytes IS NOT
    /// NULL) AS has_plan_xml_bytes` never read the XML content itself -- `IS NULL` on a TOASTed column checks the null
    /// bitmap and does not detoast. Every rollup, key and count below runs against the DuckDB-side
    /// `procedure_stats_raw` table this loads (see <see cref="ProcedureStatsWindow"/>, <see
    /// cref="ProcedureStatsWindowCache"/>), replacing three separate store reads (<see
    /// cref="ProcedureStatsHourlySql"/>, <see cref="ProcedurePlanKeySql"/>, <see
    /// cref="ProcedurePlanOversizedCountSql"/>) with one.
    /// </summary>
    public const string ProcedureStatsWindowSql = """
        /* darling-export:procedure_stats.window */
        SELECT server_name, database_name, schema_name, object_name, object_type, collection_time,
               sample_interval_seconds,
               delta_execution_count, delta_worker_time, delta_elapsed_time,
               delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_spills,
               query_plan_digest, plan_handle, cached_time,
               (query_plan_xml IS NULL)            AS plan_xml_is_null,
               (query_plan_xml_bytes IS NOT NULL)  AS has_plan_xml_bytes
        FROM collect.procedure_stats
        WHERE collection_time >= @from AND collection_time < @to
        """;

    /// <summary>
    /// <see cref="ProcedureStatsHourlySql"/>'s rollup, run in DuckDB over the already-loaded `procedure_stats_raw`
    /// table instead of a second read of `collect.procedure_stats`. The window predicate is already applied by the raw
    /// read, so this only re-applies the hourly query's own `sample_interval_seconds IS DISTINCT FROM 0` filter.
    /// `sum(...)::bigint` in Postgres becomes `CAST(sum(...) AS BIGINT)` in DuckDB: every delta column here is bigint
    /// (verified against <c>ProcedureStatsCollector.cs</c>'s column definitions), and both casts raise on overflow
    /// rather than silently wrapping.
    /// </summary>
    public const string ProcedureStatsHourlyDuckDbSql = """
        SELECT server_name, database_name, schema_name, object_name, date_trunc('hour', collection_time) AS hour,
               CAST(sum(delta_execution_count) AS BIGINT) AS execution_count_sum,
               CAST(sum(delta_worker_time) AS BIGINT) AS worker_time_sum, min(delta_worker_time) AS worker_time_min, max(delta_worker_time) AS worker_time_max,
               CAST(sum(delta_elapsed_time) AS BIGINT) AS elapsed_time_sum, min(delta_elapsed_time) AS elapsed_time_min, max(delta_elapsed_time) AS elapsed_time_max,
               CAST(sum(delta_logical_reads) AS BIGINT) AS logical_reads_sum, CAST(sum(delta_logical_writes) AS BIGINT) AS logical_writes_sum,
               CAST(sum(delta_physical_reads) AS BIGINT) AS physical_reads_sum, CAST(sum(delta_spills) AS BIGINT) AS spills_sum,
               CAST(sum(sample_interval_seconds) AS BIGINT) AS sample_interval_seconds_sum, count(*) AS sample_count
        FROM procedure_stats_raw
        WHERE sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY server_name, database_name, schema_name, object_name, date_trunc('hour', collection_time)
        """;

    /// <summary>
    /// <see cref="ProcedurePlanKeySql"/>'s key, run in DuckDB over `procedure_stats_raw` instead of a second read
    /// of `collect.procedure_stats`. Same grouping as the store-side query -- one row per distinct (server, database,
    /// schema, object, type, digest) tuple, with first/last collection_time -- so <see cref="ProcedurePlanExporter"/>
    /// still groups these into (object, shape) keys downstream exactly as it does today. `query_plan_digest` is a
    /// plain GROUP BY column here (not aggregated), so no BLOB min()/collation concern applies to this query; ORDER BY
    /// the key columns makes the output order deterministic for the caller's first-in-order-wins reads of the object
    /// type, Q6). Procedure plan reuse: each row also carries the plan identity (`plan_handle`, `cached_time`) of one
    /// raw row of that (object, digest) group. When a digest was seen under several identities in the window, the row
    /// with the latest collection_time wins, then the smallest plan_handle (NULLs last), then the latest cached_time,
    /// so the pick is deterministic. `plan_handle` is the text form Darling stores (`0x...`), not a binary value.
    /// </summary>
    public const string ProcedurePlanKeyDuckDbSql = """
        SELECT server_name, database_name, schema_name, object_name, object_type, query_plan_digest,
               first_seen, last_seen, plan_handle, cached_time
        FROM (
            SELECT server_name, database_name, schema_name, object_name, object_type, query_plan_digest,
                   min(collection_time) OVER (PARTITION BY server_name, database_name, schema_name, object_name, object_type, query_plan_digest) AS first_seen,
                   max(collection_time) OVER (PARTITION BY server_name, database_name, schema_name, object_name, object_type, query_plan_digest) AS last_seen,
                   plan_handle, cached_time,
                   row_number() OVER (
                       PARTITION BY server_name, database_name, schema_name, object_name, object_type, query_plan_digest
                       ORDER BY collection_time DESC, plan_handle ASC NULLS LAST, cached_time DESC NULLS LAST) AS pick
            FROM procedure_stats_raw
            WHERE query_plan_digest IS NOT NULL
        )
        WHERE pick = 1
        ORDER BY server_name, database_name, schema_name, object_name, object_type, query_plan_digest
        """;

    /// <summary>
    /// <see cref="ProcedurePlanOversizedCountSql"/>'s count, run in DuckDB over
    /// `procedure_stats_raw` instead of a second read of `collect.procedure_stats`.
    /// `plan_xml_is_null` and `has_plan_xml_bytes` are the two booleans <see
    /// cref="ProcedureStatsWindowSql"/> projects in place of the XML content itself.
    /// </summary>
    public const string ProcedurePlanOversizedCountDuckDbSql = """
        SELECT count(*)
        FROM procedure_stats_raw
        WHERE query_plan_digest IS NULL AND plan_xml_is_null AND has_plan_xml_bytes
        """;

    /// <summary>
    /// The window's distinct (server_name, query_plan_hash) keys, with a deterministic
    /// example digest per key (min(), not sorted output) and the first/last collection_time
    /// seen for that key. Rows with a NULL plan hash or a NULL plan digest are excluded: they
    /// have no plan to attach. Keyed by plan hash rather than digest because showplan XML
    /// differs on every compile, so digests barely repeat within a plan hash (one day on one
    /// store had 302,106 distinct plan digests but only 4,247 distinct plan hashes).
    ///
    /// kept only as a test oracle. No code path passes this to an NpgsqlCommand any more --
    /// <see cref="PlanShapeKeyDuckDbSql"/> computes the identical key in DuckDB, over the
    /// single query_stats read shared with query_stats_hourly/query_text.
    /// </summary>
    public const string PlanShapeKeySql = """
        /* darling-export:plan_shape.key */
        SELECT server_name, query_plan_hash, min(collection_time) AS first_seen, max(collection_time) AS last_seen,
               min(query_plan_digest) AS example_digest
        FROM collect.query_stats
        WHERE collection_time >= @from AND collection_time < @to
              AND query_plan_digest IS NOT NULL AND query_plan_hash IS NOT NULL
        GROUP BY server_name, query_plan_hash
        """;

    /// <summary>
    /// <see cref="PlanShapeKeySql"/>'s key, run in DuckDB over `query_stats_raw` instead of a second read of
    /// `collect.query_stats`. `min(query_plan_digest)` over BLOB reproduces PostgreSQL's `min()` over bytea exactly:
    /// both compare as unsigned bytes. ORDER BY the key columns makes the output order deterministic for the caller's
    /// first-row-wins merge, same as the Query Store key queries. Plan-fetch gate: the last two columns are this
    /// window's totals for the key, `executions` = sum(delta_execution_count) and `cpu_us` = sum(delta_worker_time)
    /// (worker time is in microseconds). The delta columns are the exporter's own per-sample deltas, 0 when the
    /// collector could not compute a real one, so a plan's first-ever sample adds nothing (a conservative undercount:
    /// it can only delay a plan's fetch by one sample).
    /// </summary>
    public const string PlanShapeKeyDuckDbSql = """
        SELECT server_name, query_plan_hash, min(collection_time) AS first_seen, max(collection_time) AS last_seen,
               min(query_plan_digest) AS example_digest,
               CAST(sum(coalesce(delta_execution_count, 0)) AS BIGINT) AS executions,
               CAST(sum(coalesce(delta_worker_time, 0)) AS BIGINT) AS cpu_us
        FROM query_stats_raw
        WHERE query_plan_digest IS NOT NULL AND query_plan_hash IS NOT NULL
        GROUP BY server_name, query_plan_hash
        ORDER BY server_name, query_plan_hash
        """;

    /// <summary>
    /// The window's distinct (server_name, database_name, query_hash) keys, with a deterministic example text digest
    /// per key and the first/last collection_time seen. Rows with a NULL query_text_digest are excluded. kept only
    /// as a test oracle. No code path passes this to an NpgsqlCommand any more -- <see cref="QueryTextKeyDuckDbSql"/>
    /// computes the identical key in DuckDB, over the single query_stats read shared with
    /// query_stats_hourly/plan_shape. This is also the query that used to spill about 1.5 GB of temp per call on a
    /// large store.
    /// </summary>
    public const string QueryTextKeySql = """
        /* darling-export:query_text.key */
        SELECT server_name, database_name, query_hash, min(collection_time) AS first_seen, max(collection_time) AS last_seen,
               min(query_text_digest) AS example_digest
        FROM collect.query_stats
        WHERE collection_time >= @from AND collection_time < @to AND query_text_digest IS NOT NULL
        GROUP BY server_name, database_name, query_hash
        """;

    /// <summary>
    /// <see cref="QueryTextKeySql"/>'s key, run in DuckDB over `query_stats_raw` instead
    /// of a second read of `collect.query_stats`. `min(query_text_digest)` over BLOB
    /// reproduces PostgreSQL's `min()` over bytea exactly (see <see
    /// cref="PlanShapeKeyDuckDbSql"/>). ORDER BY the key columns makes the output order
    /// deterministic for the caller's first-row-wins merge.
    /// </summary>
    public const string QueryTextKeyDuckDbSql = """
        SELECT server_name, database_name, query_hash, min(collection_time) AS first_seen, max(collection_time) AS last_seen,
               min(query_text_digest) AS example_digest
        FROM query_stats_raw
        WHERE query_text_digest IS NOT NULL
        GROUP BY server_name, database_name, query_hash
        ORDER BY server_name, database_name, query_hash
        """;

    /// <summary>
    /// Query Store's distinct (server_id, server_name, database_name, query_plan_hash) plan
    /// keys for the window, widened by 2 hours past --to on collection_time the same way
    /// <see cref="QueryStoreStatsRawSql"/> is (a late snapshot of an interval that started
    /// just before --to can still carry a key the window needs), and filtered on
    /// interval_start_time_utc for the true window boundary. One key per (server_name,
    /// query_plan_hash): example_database_name/example_plan_id are the smallest
    /// (database_name, plan_id) pair for that key, used to resolve one example plan through
    /// query_store_plan_map. server_id is kept so the caller can look up plan_map by
    /// (server_id, database_name, plan_id); it never reaches the output file.
    /// </summary>
    public const string QueryStorePlanShapeKeySql = """
        /* darling-export:query_store_plan_shape.key */
        SELECT server_id, server_name, query_plan_hash, min(interval_start_time_utc) AS first_seen, max(interval_start_time_utc) AS last_seen,
               (array_agg(database_name ORDER BY database_name, plan_id))[1] AS example_database_name,
               (array_agg(plan_id ORDER BY database_name, plan_id))[1] AS example_plan_id
        FROM collect.query_store_stats
        WHERE collection_time >= @from AND collection_time < @to_plus_2h
              AND interval_start_time_utc >= @from AND interval_start_time_utc < @to
              AND query_plan_hash IS NOT NULL
        GROUP BY server_id, server_name, query_plan_hash
        """;

    /// <summary>
    /// the plan-key query, run in DuckDB over the already-loaded <c>query_store_raw</c> table (the rows of <see
    /// cref="QueryStoreStatsRawSql"/>, before dedup) instead of a second read of <c>collect.query_store_stats</c> on
    /// the store. The window predicate is already applied by the raw read, so this only re-applies the key queries'
    /// own NOT NULL filter. `first(... ORDER BY database_name ASC NULLS LAST, plan_id ASC NULLS LAST)` reproduces
    /// PostgreSQL's `(array_agg(x ORDER BY database_name, plan_id))[1]` exactly (both default to NULLS LAST; DuckDB's
    /// `first()` returns the first value including NULL). Because production's collation is libc 'C' (verified: both
    /// stores' `datcollate = 'C'`, no column collation on `database_name`), PostgreSQL text order equals DuckDB byte
    /// order here -- no collation-rank join is needed (see AssertCCollationAsync, which is a hard precondition of this
    /// query ever running). ORDER BY the key columns makes the output order deterministic for the caller's
    /// first-row-wins merge. Plan-fetch gate: the last two columns are this window's totals for the key. The collector
    /// re-fetches each interval 1-3 times as counters grow, so only the latest snapshot per interval is complete; `rn
    /// = 1` marks it with the same partition as <see cref="BuildQueryStoreDedupSql"/>, and the totals sum over those
    /// rows only: `executions` = sum(execution_count), `cpu_us` = sum(avg_cpu_time_us * execution_count). The product
    /// is summed as HUGEINT and clamped to the BIGINT range, so a pathological row cannot overflow. The other columns
    /// still aggregate over every snapshot, as before.
    /// </summary>
    public const string QueryStorePlanShapeKeyDuckDbSql = """
        SELECT server_id, server_name, query_plan_hash,
               min(interval_start_time_utc) AS first_seen, max(interval_start_time_utc) AS last_seen,
               first(database_name ORDER BY database_name ASC NULLS LAST, plan_id ASC NULLS LAST) AS example_database_name,
               first(plan_id ORDER BY database_name ASC NULLS LAST, plan_id ASC NULLS LAST) AS example_plan_id,
               CAST(coalesce(sum(coalesce(execution_count, 0)) FILTER (WHERE rn = 1), 0) AS BIGINT) AS executions,
               CAST(least(coalesce(sum(CAST(coalesce(avg_cpu_time_us, 0) AS HUGEINT) * CAST(coalesce(execution_count, 0) AS HUGEINT)) FILTER (WHERE rn = 1), 0),
                          CAST(9223372036854775807 AS HUGEINT)) AS BIGINT) AS cpu_us
        FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY server_name, database_name, query_id, plan_id,
                                    runtime_stats_interval_id, execution_type_desc, replica_role
                       ORDER BY collection_time DESC
                   ) AS rn
            FROM query_store_raw
            WHERE query_plan_hash IS NOT NULL
        )
        GROUP BY server_id, server_name, query_plan_hash
        ORDER BY server_id, server_name, query_plan_hash
        """;

    /// <summary>
    /// Query Store's distinct (server_name, database_name, query_hash) text keys for the
    /// window, same window rule as <see cref="QueryStorePlanShapeKeySql"/>. One key per
    /// (server_name, database_name, query_hash); example_query_id is the smallest query_id
    /// for that key, used to resolve one example text through query_store_text.
    /// </summary>
    public const string QueryStoreTextKeySql = """
        /* darling-export:query_store_text.key */
        SELECT server_id, server_name, database_name, query_hash, min(interval_start_time_utc) AS first_seen, max(interval_start_time_utc) AS last_seen,
               (array_agg(query_id ORDER BY query_id))[1] AS example_query_id
        FROM collect.query_store_stats
        WHERE collection_time >= @from AND collection_time < @to_plus_2h
              AND interval_start_time_utc >= @from AND interval_start_time_utc < @to
              AND query_hash IS NOT NULL
        GROUP BY server_id, server_name, database_name, query_hash
        """;

    /// <summary>
    /// the text-key query, run in DuckDB over <c>query_store_raw</c> (see <see
    /// cref="QueryStorePlanShapeKeyDuckDbSql"/> for why this reproduces
    /// <see cref="QueryStoreTextKeySql"/> exactly). `(array_agg(query_id ORDER BY
    /// query_id))[1]` equals `min(query_id)` under NULLS LAST: both pick the smallest
    /// non-NULL id, or NULL only when every id in the group is NULL.
    /// </summary>
    public const string QueryStoreTextKeyDuckDbSql = """
        SELECT server_id, server_name, database_name, query_hash,
               min(interval_start_time_utc) AS first_seen, max(interval_start_time_utc) AS last_seen,
               min(query_id) AS example_query_id
        FROM query_store_raw
        WHERE query_hash IS NOT NULL
        GROUP BY server_id, server_name, database_name, query_hash
        ORDER BY server_id, server_name, database_name, query_hash
        """;

    /// <summary>
    /// The window's distinct (server_name, database_name, schema_name, object_name, object_type, query_plan_digest)
    /// keys from procedure_stats, with first/last collection_time. Rows with a NULL digest are excluded: they
    /// have no plan to attach (a size-cap row, or a pre-digest row). Inline query_plan_xml rows are ignored here:
    /// on both live stores every current row resolves through query_plan_digest, none carry inline XML.
    /// </summary>
    public const string ProcedurePlanKeySql = """
        /* darling-export:procedure_plan.key */
        SELECT server_name, database_name, schema_name, object_name, object_type, query_plan_digest,
               min(collection_time) AS first_seen, max(collection_time) AS last_seen
        FROM collect.procedure_stats
        WHERE collection_time >= @from AND collection_time < @to AND query_plan_digest IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
        """;

    /// <summary>
    /// Counts procedure_stats rows in the window that carry a size but no plan content at all: over Darling's
    /// plan-capture size cap. Reported on the procedure_plan manifest entry as OversizedPlanRows.
    /// </summary>
    public const string ProcedurePlanOversizedCountSql = """
        /* darling-export:procedure_plan.oversized_count */
        SELECT count(*)
        FROM collect.procedure_stats
        WHERE collection_time >= @from AND collection_time < @to
              AND query_plan_digest IS NULL AND query_plan_xml IS NULL AND query_plan_xml_bytes IS NOT NULL
        """;

    /// <summary>
    /// The collation guard: before the Query Store keys are computed in DuckDB, assert that the store's default
    /// collation is C/POSIX and that `collect.query_store_stats.database_name` carries no column-level override. If
    /// either check fails, PostgreSQL text order may not equal DuckDB byte order, and the plan_shape/ query_text Query
    /// Store key derivation below would silently pick the wrong example row.
    /// </summary>
    public const string DatabaseCollationCheckSql = """
        /* darling-export:collation_check */
        SELECT d.datcollate,
               (SELECT c.collation_name FROM information_schema.columns c
                WHERE c.table_schema = 'collect' AND c.table_name = 'query_store_stats'
                      AND c.column_name = 'database_name')
        FROM pg_database d WHERE d.datname = current_database()
        """;

    /// <summary>Fetches plan content for a batch of digests. Content is query_plan_xml if not NULL, else the gzip bytes in query_plan_gz (decompressed in C#).</summary>
    public const string PlanDimBatchSql =
        "/* darling-export:plan_shape.content */ SELECT digest, query_plan_xml, query_plan_gz FROM collect.query_plan_dim WHERE digest = ANY(@digests)";

    /// <summary>Fetches query text content for a batch of digests.</summary>
    public const string QueryTextDimBatchSql =
        "/* darling-export:query_text.content */ SELECT digest, query_text FROM collect.query_text_dim WHERE digest = ANY(@digests)";

    /// <summary>
    /// The raw Query Store window select, minus server_id/collection_id/query_text/
    /// query_plan_text (excluded per the exporter's brief: query_text and query_plan_text are
    /// always NULL in this table, content lives elsewhere, and collection_id/server_id are
    /// local to one store). server_id IS kept, unlike the raw-table pattern, because the
    /// caller needs it in DuckDB to look up plan_map/text rows by (server_id, database_name,
    /// plan_id / query_id); it is dropped from the final Parquet output in the dedup step.
    /// Filtered on both collection_time (widened by 2 hours past --to to catch a late final
    /// snapshot) and interval_start_time_utc (the true window boundary: an interval belongs
    /// to the window it STARTED in, not the window it happened to be collected in).
    /// </summary>
    public const string QueryStoreStatsRawSql = """
        /* darling-export:query_store_stats.window */
        SELECT server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
               first_execution_time, last_execution_time, module_name, query_hash, execution_count,
               avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
               avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads,
               avg_logical_io_writes, min_logical_io_writes, max_logical_io_writes,
               avg_physical_io_reads, min_physical_io_reads, max_physical_io_reads,
               avg_clr_time_us, min_clr_time_us, max_clr_time_us, min_dop, max_dop,
               avg_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory,
               avg_rowcount, min_rowcount, max_rowcount,
               avg_num_physical_io_reads, min_num_physical_io_reads, max_num_physical_io_reads,
               avg_log_bytes_used, min_log_bytes_used, max_log_bytes_used,
               avg_tempdb_space_used, min_tempdb_space_used, max_tempdb_space_used,
               plan_type, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason,
               compatibility_level, query_plan_hash, replica_role, runtime_stats_interval_id,
               interval_start_time_utc, collection_time
        FROM collect.query_store_stats
        WHERE collection_time >= @from AND collection_time < @to_plus_2h
              AND interval_start_time_utc >= @from AND interval_start_time_utc < @to
        """;

    /// <summary>
    /// The Query Store window read from Darling's own deduplicated table (V145, `collect.query_store_interval_wide`:
    /// one row per interval identity, the latest snapshot). Same columns in the same order and types as <see
    /// cref="QueryStoreStatsRawSql"/> (`QueryStoreWindow.RawColumns`), except `server_name`, which the table does not
    /// carry: it comes back NULL here and DuckDB fills it from <see cref="QueryStoreWideNamesSql"/>. Selected by
    /// `first_execution_time`, the partition key, which lies in `[interval_start, interval_start + 1 h)` for every
    /// interval; for whole-hour `@from`/`@to` that selects exactly the window's rows. No `interval_start_time_utc`
    /// filter here: the loader validates it in DuckDB and falls back to the raw read when a row is outside the window.
    /// Run by <see cref="QueryStoreWideReadSetupSql"/>'s transaction so the planner uses a bitmap heap scan. the
    /// The planner then uses a bitmap heap scan.    /// </summary>
    public const string QueryStoreStatsWideSql = """
        /* darling-export:query_store_stats.wide */
        SELECT server_id, CAST(NULL AS text) AS server_name, database_name, query_id, plan_id, execution_type_desc,
               first_execution_time, last_execution_time, module_name, query_hash, execution_count,
               avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
               avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads,
               avg_logical_io_writes, min_logical_io_writes, max_logical_io_writes,
               avg_physical_io_reads, min_physical_io_reads, max_physical_io_reads,
               avg_clr_time_us, min_clr_time_us, max_clr_time_us, min_dop, max_dop,
               avg_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory,
               avg_rowcount, min_rowcount, max_rowcount,
               avg_num_physical_io_reads, min_num_physical_io_reads, max_num_physical_io_reads,
               avg_log_bytes_used, min_log_bytes_used, max_log_bytes_used,
               avg_tempdb_space_used, min_tempdb_space_used, max_tempdb_space_used,
               plan_type, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason,
               compatibility_level, query_plan_hash, replica_role, runtime_stats_interval_id,
               interval_start_time_utc, collection_time
        FROM collect.query_store_interval_wide
        WHERE first_execution_time >= @from AND first_execution_time < @to
        """;

    /// <summary>
    /// First statement of the wide read's transaction (<see cref="QueryStoreStatsWideSql"/>): read-only, and no plain
    /// index scan. Measured on a large store: the planner estimates 10-38k rows for an hour that holds
    /// 200-380k and picks an index scan (one random page per row, 31 s); a bitmap heap scan reads the same rows in 1.7 s.
    /// `SET LOCAL` ends with the transaction, so the session's other statements are unaffected.
    /// </summary>
    public const string QueryStoreWideReadSetupSql = """
        /* darling-export:query_store_stats.wide.setup */
        SET TRANSACTION READ ONLY;
        SET LOCAL enable_indexscan = off
        """;

    /// <summary>Wide-read gate, step 1: do the three tables exist. One row, one boolean.</summary>
    public const string QueryStoreWideTablesExistSql = """
        /* darling-export:query_store_stats.wide.gate_tables */
        SELECT to_regclass('collect.query_store_interval_wide') IS NOT NULL
               AND to_regclass('collect.query_store_interval_wide_coverage') IS NOT NULL
               AND to_regclass('collect.query_store_interval_wide_pending') IS NOT NULL
        """;

    /// <summary>Wide-read gate: one row per server Darling's table claims to cover.</summary>
    public const string QueryStoreWideCoverageSql = """
        /* darling-export:query_store_stats.wide.gate_coverage */
        SELECT server_id, filled_since, applied_through
        FROM collect.query_store_interval_wide_coverage
        """;

    /// <summary>
    /// Wide-read gate: servers with a failed batch. Empty in steady state; capped, and a result as long as the cap is
    /// treated as "pending" whoever the rows belong to.
    /// </summary>
    public const string QueryStoreWidePendingSql = """
        /* darling-export:query_store_stats.wide.gate_pending */
        SELECT server_id
        FROM collect.query_store_interval_wide_pending
        LIMIT 1000
        """;

    /// <summary>
    /// `server_name` for the wide rows' servers. The wide table does not carry it, so it comes from the raw table, which the
    /// export login can read: one index probe per server on `idx_query_store_stats_time (server_id, collection_time)`, within
    /// the same collection_time bounds the raw read uses. This is the value the raw read returns. The monitored-server registry
    /// is not read. A NULL name (no raw row in the bounds) makes the loader fall back to raw as `server_unmapped`.
    /// </summary>
    public const string QueryStoreWideNamesSql = """
        /* darling-export:query_store_stats.wide.names */
        SELECT s.server_id,
               (SELECT q.server_name FROM collect.query_store_stats q
                WHERE q.server_id = s.server_id AND q.collection_time >= @from AND q.collection_time < @to_plus_2h
                LIMIT 1) AS server_name
        FROM unnest(@ids) AS s(server_id)
        """;

    /// <summary>First statement of the names transaction: read-only. Unlike the wide read, the index scan stays on.</summary>
    public const string QueryStoreWideNamesSetupSql = """
        /* darling-export:query_store_stats.wide.names.setup */
        SET TRANSACTION READ ONLY
        """;

    /// <summary>
    /// The DuckDB dedup query run against the raw window loaded by <see
    /// cref="QueryStoreStatsRawSql"/>: the collector re-fetches each 60-minute Query Store
    /// interval 1-3 times as counters grow, so only the snapshot with the greatest
    /// collection_time per (server_name, database_name, query_id, plan_id,
    /// runtime_stats_interval_id, execution_type_desc, replica_role) is complete.
    /// snapshot_count records how many snapshots were collapsed into that row.
    /// server_id is kept (needed for the plan_map/text lookups) but dropped by the caller
    /// before writing the final Parquet output; collection_time itself is renamed to
    /// last_collection_time in the output.
    /// </summary>
    public static string BuildQueryStoreDedupSql(string rawTable) => $"""
        /* darling-export:query_store_stats.dedup */
        SELECT * EXCLUDE (rn, collection_time),
               collection_time AS last_collection_time,
               count(*) OVER (
                   PARTITION BY server_name, database_name, query_id, plan_id,
                                runtime_stats_interval_id, execution_type_desc, replica_role
               ) AS snapshot_count
        FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY server_name, database_name, query_id, plan_id,
                                    runtime_stats_interval_id, execution_type_desc, replica_role
                       ORDER BY collection_time DESC
                   ) AS rn
            FROM {QuoteDuckIdentifier(rawTable)}
        )
        QUALIFY rn = 1
        """;

    private static string QuoteDuckIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>
    /// Looks up plan_map rows for a batch of (server_id, database_name, plan_id) triples, run
    /// against the store in batches of 500 by binding three parallel arrays and joining them
    /// with unnest, the same batching size DimensionTableExporter uses for digest lookups.
    /// </summary>
    public const string QueryStorePlanMapBatchSql = """
        /* darling-export:query_store_plan_map.content */
        SELECT m.server_id, m.database_name, m.plan_id, m.digest, m.plan_hash
        FROM collect.query_store_plan_map m
        JOIN unnest(@sids::int4[], @dbs::text[], @pids::int8[]) AS k(sid, db, pid)
          ON m.server_id = k.sid AND m.database_name = k.db AND m.plan_id = k.pid
        """;

    /// <summary>
    /// Looks up query_store_text rows for a batch of (server_id, database_name, query_id)
    /// triples, same batching approach as the plan map lookup above.
    /// </summary>
    public const string QueryStoreTextBatchSql = """
        /* darling-export:query_store_text.content */
        SELECT t.server_id, t.database_name, t.query_id, t.query_sql_text, t.query_hash
        FROM collect.query_store_text t
        JOIN unnest(@sids::int4[], @dbs::text[], @qids::int8[]) AS k(sid, db, qid)
          ON t.server_id = k.sid AND t.database_name = k.db AND t.query_id = k.qid
        """;

    /// <summary>
    /// Batch primary-key fetch of the Postgres store's `pg_statement_text` dimension: probes `(server_id, queryid)` in
    /// a `unnest(...)` join, the same shape as <see cref="QueryStoreTextBatchSql"/> above, and carries the tag so
    /// pg_stat_statements deltas can name it. `server_id` is kept only so the caller can map back to `server_name` in
    /// DuckDB (it never lands in the Parquet output).
    /// </summary>
    public const string PgStatementTextBatchSql = """
        /* darling-export:opt.pg_statement_text */
        SELECT t.server_id, t.queryid, t.query_text, t.first_seen, t.last_seen
        FROM collect.pg_statement_text t
        JOIN unnest(@sids::int4[], @qids::int8[]) AS k(sid, qid)
          ON t.server_id = k.sid AND t.queryid = k.qid
        """;

    /// <summary>Returns the store-side SQL text for a table name from <see cref="ExportOptions.AllTables"/>. Dimension tables are built by <see cref="DimensionTableExporter"/>, not this path.</summary>
    public static string BuildSelect(string table, IReadOnlyList<string>? rawColumns) => table switch
    {
        HourlyTables.QueryStatsHourly => QueryStatsHourlySql,
        HourlyTables.ProcedureStatsHourly => ProcedureStatsHourlySql, // test oracle only; see ProcedureStatsHourlyDuckDbSql

        QueryStoreTables.QueryStoreStats => QueryStoreStatsRawSql,
        DimensionTables.PlanShape => PlanShapeKeySql,
        DimensionTables.QueryText => QueryTextKeySql,
        DimensionTables.ProcedurePlan => ProcedurePlanKeySql,
        _ => BuildRawSelect(table, rawColumns ?? throw new ArgumentNullException(nameof(rawColumns), $"Raw table '{table}' needs a discovered column list.")),
    };

    /// <summary>The time column a table's rows are ordered by in the Parquet output, after source_store, server_name.</summary>
    public static string TimeColumn(string table) => table switch
    {
        HourlyTables.QueryStatsHourly or HourlyTables.ProcedureStatsHourly => "hour",
        QueryStoreTables.QueryStoreStats => "interval_start_time_utc",
        DimensionTables.PlanShape or DimensionTables.QueryText or DimensionTables.ProcedurePlan => "last_seen",
        _ => "collection_time",
    };

    /// <summary>
    /// The information_schema query that discovers an optional table's exportable columns: schema-aware (bound by
    /// <c>@schema</c>/<c>@table</c>, unlike <see cref="RawColumnDiscoverySql"/>, which is hard-coded to collect), in
    /// ordinal order, excluding server_id and collection_id (store-local ids that never leave the store), any identity
    /// column, and any column whose default starts with <c>nextval(</c> (a store-local sequence). SQL
    /// Server/PostgreSQL target-side ids such as query_id, plan_id and queryid are NOT excluded — they belong to the
    /// monitored server, not the store, and stay in the projection.
    /// </summary>
    public const string OptionalColumnDiscoverySql =
        "/* darling-export:opt.column_discovery */ SELECT column_name, data_type, udt_name FROM information_schema.columns " +
        "WHERE table_schema = @schema AND table_name = @table " +
        "AND column_name NOT IN ('server_id', 'collection_id') " +
        "AND is_identity <> 'YES' " +
        "AND (column_default IS NULL OR column_default NOT LIKE 'nextval(%') " +
        "ORDER BY ordinal_position";

    /// <summary>
    /// Builds the SELECT for an optional table: an explicit, ordinal-ordered column list (as discovered by <see
    /// cref="OptionalColumnDiscoverySql"/>), tagged so pg_stat_statements deltas can name the table, against <c><see
    /// cref="OptionalTable.Schema"/>.&lt;table&gt;</c>. A window table is filtered on its time column for the export
    /// window; a snapshot table (kind <see cref="OptionalTableKind.Snapshot"/>, for example <c>servers</c>) carries no
    /// WHERE at all — it is read whole. Never emits ORDER BY, GROUP BY or DISTINCT.
    /// </summary>
    public static string BuildOptionalSelect(OptionalTable table, IReadOnlyList<string> columns)
    {
        return BuildOptionalSelect(table, columns.Select(c => new DiscoveredColumn(c, string.Empty, string.Empty)).ToList(), policy: null);
    }

    /// <summary>
    /// Builds the SELECT for an optional table, applying a column policy as a store-side projection. <paramref
    /// name="policy"/> null is byte-identical to the unchanged overload above. <c>Drop</c> leaves the column out of
    /// the projection. <c>KeepWhen</c> wraps it in <c>CASE WHEN &lt;predicate&gt; THEN left("c", N) END AS "c"</c> (or
    /// just the column, with no CASE, when there is no TruncateChars). <c>KeepOnly</c> restricts the projection to the
    /// discovered columns intersected with the list, in discovery order -- a discovered column not on the list is
    /// dropped, and a list column not discovered is ignored.
    /// </summary>
    public static string BuildOptionalSelect(OptionalTable table, IReadOnlyList<DiscoveredColumn> columns, TablePolicy? policy)
    {
        if (ColumnPolicies.IsDenied($"{table.Schema}.{table.Name}"))
        {
            throw new InvalidOperationException($"denied table: {table.Schema}.{table.Name}");
        }

        if (columns.Count == 0)
        {
            throw new ArgumentException($"Optional table '{table.Name}' has no exportable columns.");
        }

        var projected = columns;

        if (policy?.KeepOnly is { } keepOnly)
        {
            var keepSet = new HashSet<string>(keepOnly, StringComparer.Ordinal);
            projected = columns.Where(c => keepSet.Contains(c.Name)).ToList();
        }

        var rulesByColumn = policy?.Rules.ToDictionary(r => r.Column, StringComparer.Ordinal)
            ?? new Dictionary<string, ColumnRule>(StringComparer.Ordinal);

        var projections = new List<string>();
        foreach (var column in projected)
        {
            if (!rulesByColumn.TryGetValue(column.Name, out var rule))
            {
                projections.Add(QuoteIdentifier(column.Name));
                continue;
            }

            if (rule.Action == ColumnAction.Drop)
            {
                continue;
            }

            // ColumnAction.KeepWhen
            var quoted = QuoteIdentifier(column.Name);
            if (rule.TruncateChars is { } truncateChars)
            {
                projections.Add($"CASE WHEN {rule.PredicateSql} THEN left({quoted}, {truncateChars}) END AS {quoted}");
            }
            else
            {
                projections.Add(quoted);
            }
        }

        if (projections.Count == 0)
        {
            throw new ArgumentException($"Optional table '{table.Name}' has no exportable columns after policy.");
        }

        var columnList = string.Join(", ", projections);
        var sql = $"/* darling-export:opt.{table.Name} */ SELECT {columnList} FROM {table.Schema}.{QuoteIdentifier(table.Name)}";

        if (table.Kind == OptionalTableKind.Window)
        {
            if (string.IsNullOrEmpty(table.TimeColumn))
            {
                throw new ArgumentException($"Optional table '{table.Name}' is a window table but has no TimeColumn.");
            }

            sql += $" WHERE {table.TimeColumn} >= @from AND {table.TimeColumn} < @to";
        }

        return sql;
    }
}

/// <summary>One column discovered by <see cref="SqlBuilder.OptionalColumnDiscoverySql"/>: name, data_type and udt_name from information_schema.columns.</summary>
public sealed record DiscoveredColumn(string Name, string DataType, string UdtName);

