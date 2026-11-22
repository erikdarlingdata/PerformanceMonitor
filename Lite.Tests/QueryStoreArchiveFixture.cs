using System;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: a Query Store history shaped like a busy store's, for the tests and probes that read it back out of the Parquet archive.
/// Every (query, plan) pair of a server appears in a share of the hourly intervals, each interval stored twice (the open interval is
/// collected again an hour later, as the collector does), with its text on every row. Collection ids grow with collection time.
/// </summary>
internal static class QueryStoreArchiveFixture
{
    /// <summary>
    /// Inserts <paramref name="days"/> days of hourly intervals ending at <paramref name="nowUtc"/> for each server, server N holding
    /// <c>entitiesPerServer[N - 1]</c> (query, plan) pairs spread over <paramref name="databases"/> databases. <paramref name="keepPerMille"/>
    /// of the pair-and-interval combinations are stored. Returns the rows inserted.
    /// </summary>
    internal static async Task<long> SeedAsync(
        DuckDBConnection connection, DateTime nowUtc, int days, int[] entitiesPerServer, int databases, int keepPerMille, CancellationToken cancellationToken)
    {
        var baseHour = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Utc).AddDays(-days);
        var hours = days * 24 + 1;
        var maxEntities = 0;
        foreach (var n in entitiesPerServer)
        {
            maxEntities = Math.Max(maxEntities, n);
        }

        await ExecuteAsync(connection, $@"
CREATE OR REPLACE TEMP TABLE qs_ent AS
SELECT e,
       'Db' || lpad(((e % {databases}) + 1)::VARCHAR, 2, '0') AS db,
       (100000 + e)::BIGINT AS query_id,
       (500000 + e)::BIGINT AS plan_id,
       '/* q' || e || ' */ SELECT ' || (SELECT string_agg('t' || (e % 97) || '.c_' || md5(e::VARCHAR || '-' || k::VARCHAR), ', ') FROM range(3 + (e * 37) % 40) r(k)) || ' FROM dbo.tbl' || e || ' AS t' || (e % 97) AS txt
FROM range({maxEntities}) t(e)", cancellationToken);

        long total = 0;
        for (var server = 1; server <= entitiesPerServer.Length; server++)
        {
            var entities = entitiesPerServer[server - 1];
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count,
     avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
     avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     avg_rowcount, min_dop, max_dop, plan_type, is_forced_plan, force_failure_count, query_plan_hash,
     runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
SELECT
    ((i * 2 + s) * 10 + {server}) * 100000 + e,
    ct, {server}, 'SRV{server}', db, query_id, plan_id,
    CASE WHEN e % 97 = 0 THEN 'Aborted' ELSE 'Regular' END,
    ts + to_minutes(1 + e % 5), ct - to_minutes(1 + e % 7),
    CASE WHEN e % 5 = 0 THEN 'usp_proc' || e ELSE NULL END,
    txt, '0x' || upper(hex(e * 7919)),
    10 + (e % 90) + s * (20 + e % 50),
    1000 + (e * 7919) % 90000 + i % 13, 500 + e % 400, 200000 + (e * 31) % 700000,
    800 + (e * 104729) % 60000 + i % 7, 400 + e % 300, 150000 + (e * 17) % 500000,
    100 + (e * 31) % 5000, 10, 9000, 5 + e % 40, 3 + e % 30,
    10 + e % 400, 1, 1 + e % 4, 'Regular', FALSE, 0, '0x' || upper(hex(plan_id * 13)),
    1000 + i, ts, ts + to_hours(1)
FROM
(
    SELECT x.e, x.i, x.s, n.db, n.query_id, n.plan_id, n.txt,
           TIMESTAMP '{baseHour:yyyy-MM-dd HH:mm:ss}' + to_hours(x.i) AS ts,
           TIMESTAMP '{baseHour:yyyy-MM-dd HH:mm:ss}' + to_hours(x.i) + CASE WHEN x.s = 0 THEN to_minutes(20 + x.e % 30) ELSE to_minutes(75 + x.e % 40) END AS ct
    FROM (SELECT e, i, s FROM range({entities}) a(e), range({hours}) b(i), range(2) c(s) WHERE hash(e, i, {server}) % 1000 < {keepPerMille}) x
    JOIN qs_ent n ON n.e = x.e
) q
WHERE ct <= TIMESTAMP '{nowUtc:yyyy-MM-dd HH:mm:ss}'";
            total += await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return total;
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql, CancellationToken cancellationToken)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}
