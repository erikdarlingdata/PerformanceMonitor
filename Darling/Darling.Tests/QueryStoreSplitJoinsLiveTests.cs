/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: no PostgreSQL store here, only one scratch SQL Server database per class run (a GUID name). It shares "remediation-live-sql"
   with the other classes that use the one test SQL Server. */

/// <summary>
/// #6257 against a REAL SQL Server. The shared Query Store collector used to join several <c>sys.query_store_*</c> views in one statement (the
/// payload read) or two (the text fetch). It now stages each view into its own temp table and joins the temps. The row set must not move, so each
/// fact runs the collector's real statement and the statement it replaced (kept here, verbatim, as the reference) against one scratch database and
/// compares every column of every row, and the ship order. The database holds two runtime-stats intervals, a query with two plans, a forced plan,
/// a procedure (so a module name), flushed and in-memory slices of one interval, and Query Store capture is switched off before the reads so the
/// collector's own statements do not enter the store between the two runs. Needs <c>DARLING_TEST_SQL</c> with user and password; skips without.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class QueryStoreSplitJoinsLiveTests : IClassFixture<QueryStoreSplitJoinsLiveTests.ScratchDatabase>
{
    private const string OldPayloadWithText = @"IF NOT EXISTS
(
    SELECT
        1
    FROM sys.database_query_store_options
    WHERE actual_state IN (1, 2, 4)
    AND   readonly_reason & 8 = 0
)
BEGIN
    RETURN;
END;

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DROP TABLE IF EXISTS #pm_qs_slice;

SELECT /* PerformanceMonitorLite */
    qsrs.plan_id,
    qsrs.runtime_stats_interval_id,
    qsrs.execution_type_desc,
    qsrs.replica_group_id,
    first_execution_time = MIN(qsrs.first_execution_time),
    last_execution_time = MAX(qsrs.last_execution_time),
    count_executions = SUM(qsrs.count_executions),
    avg_duration = SUM(qsrs.avg_duration * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_duration = MIN(qsrs.min_duration),
    max_duration = MAX(qsrs.max_duration),
    avg_cpu_time = SUM(qsrs.avg_cpu_time * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_cpu_time = MIN(qsrs.min_cpu_time),
    max_cpu_time = MAX(qsrs.max_cpu_time),
    avg_logical_io_reads = SUM(qsrs.avg_logical_io_reads * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_logical_io_reads = MIN(qsrs.min_logical_io_reads),
    max_logical_io_reads = MAX(qsrs.max_logical_io_reads),
    avg_logical_io_writes = SUM(qsrs.avg_logical_io_writes * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_logical_io_writes = MIN(qsrs.min_logical_io_writes),
    max_logical_io_writes = MAX(qsrs.max_logical_io_writes),
    avg_physical_io_reads = SUM(qsrs.avg_physical_io_reads * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_physical_io_reads = MIN(qsrs.min_physical_io_reads),
    max_physical_io_reads = MAX(qsrs.max_physical_io_reads),
    avg_clr_time = SUM(qsrs.avg_clr_time * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_clr_time = MIN(qsrs.min_clr_time),
    max_clr_time = MAX(qsrs.max_clr_time),
    min_dop = MIN(qsrs.min_dop),
    max_dop = MAX(qsrs.max_dop),
    avg_query_max_used_memory = SUM(qsrs.avg_query_max_used_memory * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_query_max_used_memory = MIN(qsrs.min_query_max_used_memory),
    max_query_max_used_memory = MAX(qsrs.max_query_max_used_memory),
    avg_rowcount = SUM(qsrs.avg_rowcount * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_rowcount = MIN(qsrs.min_rowcount),
    max_rowcount = MAX(qsrs.max_rowcount),
    avg_num_physical_io_reads = SUM(qsrs.avg_num_physical_io_reads * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_num_physical_io_reads = MIN(qsrs.min_num_physical_io_reads),
    max_num_physical_io_reads = MAX(qsrs.max_num_physical_io_reads),
    avg_log_bytes_used = SUM(qsrs.avg_log_bytes_used * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_log_bytes_used = MIN(qsrs.min_log_bytes_used),
    max_log_bytes_used = MAX(qsrs.max_log_bytes_used),
    avg_tempdb_space_used = SUM(qsrs.avg_tempdb_space_used * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0),
    min_tempdb_space_used = MIN(qsrs.min_tempdb_space_used),
    max_tempdb_space_used = MAX(qsrs.max_tempdb_space_used)
INTO #pm_qs_slice
FROM sys.query_store_runtime_stats AS qsrs
WHERE qsrs.runtime_stats_interval_id IN
(
    SELECT
        i.runtime_stats_interval_id
    FROM sys.query_store_runtime_stats_interval AS i
    WHERE i.end_time > @cutoff_time
)
GROUP BY
    qsrs.plan_id,
    qsrs.runtime_stats_interval_id,
    qsrs.execution_type_desc,
    qsrs.replica_group_id
HAVING
    MAX(qsrs.last_execution_time) > @cutoff_time
OPTION(RECOMPILE);

SELECT /* PerformanceMonitorLite */ TOP (50000) WITH TIES
    query_id = qsq.query_id,
    plan_id = qsp.plan_id,
    execution_type_desc = qsrs.execution_type_desc,
    first_execution_time = qsrs.first_execution_time,
    last_execution_time = qsrs.last_execution_time,
    module_name =
        CASE
            WHEN qsq.object_id = 0
            THEN N'Adhoc'
            ELSE COALESCE(
                OBJECT_SCHEMA_NAME(qsq.object_id) + N'.' + OBJECT_NAME(qsq.object_id),
                N'Unknown')
        END,
    query_sql_text = qst.query_sql_text,
    query_hash = CONVERT(varchar(64), qsq.query_hash, 1),
    count_executions = qsrs.count_executions,
    avg_duration = qsrs.avg_duration,
    min_duration = qsrs.min_duration,
    max_duration = qsrs.max_duration,
    avg_cpu_time = qsrs.avg_cpu_time,
    min_cpu_time = qsrs.min_cpu_time,
    max_cpu_time = qsrs.max_cpu_time,
    avg_logical_io_reads = qsrs.avg_logical_io_reads,
    min_logical_io_reads = qsrs.min_logical_io_reads,
    max_logical_io_reads = qsrs.max_logical_io_reads,
    avg_logical_io_writes = qsrs.avg_logical_io_writes,
    min_logical_io_writes = qsrs.min_logical_io_writes,
    max_logical_io_writes = qsrs.max_logical_io_writes,
    avg_physical_io_reads = qsrs.avg_physical_io_reads,
    min_physical_io_reads = qsrs.min_physical_io_reads,
    max_physical_io_reads = qsrs.max_physical_io_reads,
    avg_clr_time = qsrs.avg_clr_time,
    min_clr_time = qsrs.min_clr_time,
    max_clr_time = qsrs.max_clr_time,
    min_dop = qsrs.min_dop,
    max_dop = qsrs.max_dop,
    avg_query_max_used_memory = qsrs.avg_query_max_used_memory,
    min_query_max_used_memory = qsrs.min_query_max_used_memory,
    max_query_max_used_memory = qsrs.max_query_max_used_memory,
    avg_rowcount = qsrs.avg_rowcount,
    min_rowcount = qsrs.min_rowcount,
    max_rowcount = qsrs.max_rowcount,
    qsrs.avg_num_physical_io_reads, qsrs.min_num_physical_io_reads, qsrs.max_num_physical_io_reads,
    avg_log_bytes_used = qsrs.avg_log_bytes_used, min_log_bytes_used = qsrs.min_log_bytes_used, max_log_bytes_used = qsrs.max_log_bytes_used,
    avg_tempdb_space_used = qsrs.avg_tempdb_space_used, min_tempdb_space_used = qsrs.min_tempdb_space_used, max_tempdb_space_used = qsrs.max_tempdb_space_used,
    plan_type = qsp.plan_type_desc,
    plan_forcing_type = qsp.plan_forcing_type_desc,
    is_forced_plan = qsp.is_forced_plan,
    force_failure_count = qsp.force_failure_count,
    last_force_failure_reason = qsp.last_force_failure_reason_desc,
    compatibility_level = qsp.compatibility_level,
    query_plan_text = CONVERT(nvarchar(1), NULL),
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
    replica_role = qsr.replica_name,
    runtime_stats_interval_id = qsrs.runtime_stats_interval_id,
    interval_start_time_utc = CONVERT(datetime2, qsrsi.start_time AT TIME ZONE 'UTC'),
    interval_end_time_utc = CONVERT(datetime2, qsrsi.end_time AT TIME ZONE 'UTC')
FROM #pm_qs_slice AS qsrs
JOIN sys.query_store_plan AS qsp
  ON qsp.plan_id = qsrs.plan_id
JOIN sys.query_store_query AS qsq
  ON qsq.query_id = qsp.query_id
JOIN sys.query_store_query_text AS qst
  ON qst.query_text_id = qsq.query_text_id
LEFT JOIN sys.query_store_runtime_stats_interval AS qsrsi
  ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
LEFT JOIN sys.query_store_replicas AS qsr
  ON qsr.replica_group_id = qsrs.replica_group_id
ORDER BY qsrs.last_execution_time ASC
OPTION(RECOMPILE);";

    /// <summary>The text fetch as it was before #6257: one statement joining the query view to the text view.</summary>
    private const string OldTextFetch = @"SET NOCOUNT ON;

SELECT /* PerformanceMonitorLite */
    query_id = qsq.query_id,
    query_hash = CONVERT(varchar(64), qsq.query_hash, 1),
    query_sql_text = qst.query_sql_text
INTO #text_fetch
FROM sys.query_store_query AS qsq
JOIN sys.query_store_query_text AS qst
  ON qst.query_text_id = qsq.query_text_id
WHERE qsq.query_id IN ({0})
OPTION(RECOMPILE, HASH JOIN);

SELECT /* PerformanceMonitorLite */
    query_id = b.query_id,
    query_hash = b.query_hash,
    query_sql_text = b.query_sql_text
FROM
(
    SELECT
        query_id = t.query_id,
        query_hash = t.query_hash,
        query_sql_text = t.query_sql_text,
        text_bytes = COALESCE(DATALENGTH(t.query_sql_text), 0),
        running_bytes = SUM(COALESCE(DATALENGTH(t.query_sql_text), 0)) OVER (ORDER BY t.query_id ROWS UNBOUNDED PRECEDING)
    FROM #text_fetch AS t
) AS b
WHERE b.running_bytes - b.text_bytes < {1}
ORDER BY b.query_id
OPTION(RECOMPILE);

DROP TABLE #text_fetch;";

    private const string OldNoTextColumn = "query_sql_text = qst.query_sql_text,";
    private const string NoTextColumn = "query_sql_text = CONVERT(nvarchar(1), NULL),";
    private const string OldTextJoin = "JOIN sys.query_store_query_text AS qst\n  ON qst.query_text_id = qsq.query_text_id\n";

    private readonly ScratchDatabase _scratch;

    public QueryStoreSplitJoinsLiveTests(ScratchDatabase scratch) => _scratch = scratch;

    /// <summary>The payload statement as it was before #6257, with or without the inline text, as the old code built it.</summary>
    private static string OldPayload(bool textSeparately)
    {
        var lf = OldPayloadWithText.Replace("\r\n", "\n", StringComparison.Ordinal);
        if (!textSeparately) return lf;
        Assert.Contains(OldNoTextColumn, lf, StringComparison.Ordinal);
        Assert.Contains(OldTextJoin, lf, StringComparison.Ordinal);
        return lf.Replace(OldNoTextColumn, NoTextColumn, StringComparison.Ordinal).Replace(OldTextJoin, "", StringComparison.Ordinal);
    }

    private static CollectorContext Context(bool azure, bool textSeparately) => new()
    {
        ServerId = 6257,
        ServerName = "qs-split-joins",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure, SqlMajorVersion = 17 },
        EnumerationProbeResult = 17,
        FetchQueryTextSeparately = textSeparately,
        CapturePlanXml = textSeparately,
    };

    private sealed record Result(string[] Columns, List<object?[]> Rows);

    private static async Task<Result> RunAsync(string database, CollectorQuery query, CancellationToken ct)
    {
        await using var connection = new SqlConnection(ScratchDatabase.Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 180);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var columns = new string[reader.FieldCount];
        for (var i = 0; i < columns.Length; i++) columns[i] = reader.GetName(i);
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(ct))
        {
            var values = new object[columns.Length];
            reader.GetValues(values);
            var row = new object?[values.Length];
            for (var i = 0; i < values.Length; i++) row[i] = values[i] is DBNull ? null : values[i];
            rows.Add(row);
        }

        return new Result(columns, rows);
    }

    private static string Cell(object? value) => value switch
    {
        null => "<null>",
        DateTime d => d.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset o => o.ToString("O", CultureInfo.InvariantCulture),
        double f => f.ToString("R", CultureInfo.InvariantCulture),
        float f => f.ToString("R", CultureInfo.InvariantCulture),
        IFormattable x => x.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "<null>",
    };

    private static string Line(object?[] row) => string.Join("|", row.Select(Cell));

    /// <summary>
    /// The same columns in the same order, the same rows, and the same ship order. Rows that tie on the sort key (<paramref name="orderColumn"/>) have
    /// no order the engine promises, so a tie group compares as a set, and the key sequence itself compares exactly.
    /// </summary>
    private static void AssertSame(Result expected, Result actual, string orderColumn)
    {
        Assert.Equal(expected.Columns, actual.Columns);
        Assert.Equal(expected.Rows.Count, actual.Rows.Count);
        var key = Array.IndexOf(expected.Columns, orderColumn);
        Assert.True(key >= 0, orderColumn);
        Assert.Equal(expected.Rows.Select(r => Cell(r[key])), actual.Rows.Select(r => Cell(r[key])));
        Assert.Equal(
            expected.Rows.Select(Line).OrderBy(s => s, StringComparer.Ordinal),
            actual.Rows.Select(Line).OrderBy(s => s, StringComparer.Ordinal));
    }

    private async Task<Result> AssertPayloadSameAsOldAsync(bool azure, bool textSeparately)
    {
        Assert.SkipWhen(_scratch.Database is null, "Set DARLING_TEST_SQL (with DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD) to run the Query Store split-join live tests.");
        var ct = TestContext.Current.CancellationToken;
        var database = _scratch.Database!;
        var context = Context(azure, textSeparately);
        var query = azure
            ? QueryStoreCollector.Instance.BuildQuery(context)
            : QueryStoreCollector.Instance.BuildPerItemQuery(database, context);

        var actual = await RunAsync(azure ? database : "master", query, ct);

        /* The reference runs the old statement on the database's own connection with the same bound cutoff. */
        var reference = new CollectorQuery(GatedOld(textSeparately), query.Parameters);
        var expected = await RunAsync(database, reference, ct);

        AssertFixtureIsRich(expected, _scratch.QueryTextIds);
        AssertSame(expected, actual, "last_execution_time");
        return expected;
    }

    private static string GatedOld(bool textSeparately) => OldPayload(textSeparately);

    /// <summary>The comparison proves nothing on an empty or thin result, so the fixture must hold what the join keys differ on.</summary>
    private static void AssertFixtureIsRich(Result expected, IReadOnlyDictionary<long, long> queryTextIds)
    {
        var intervals = Array.IndexOf(expected.Columns, "runtime_stats_interval_id");
        var plans = Array.IndexOf(expected.Columns, "plan_id");
        var queries = Array.IndexOf(expected.Columns, "query_id");
        var forced = Array.IndexOf(expected.Columns, "is_forced_plan");
        var module = Array.IndexOf(expected.Columns, "module_name");
        var replica = Array.IndexOf(expected.Columns, "replica_role");
        var start = Array.IndexOf(expected.Columns, "interval_start_time_utc");
        Assert.True(expected.Rows.Count >= 8, $"the reference read {expected.Rows.Count} rows");
        Assert.True(expected.Rows.Select(r => Cell(r[intervals])).Distinct().Count() >= 2, "two intervals");
        Assert.True(expected.Rows.Select(r => Cell(r[plans])).Distinct().Count() >= 6, "six plans");
        Assert.True(expected.Rows.GroupBy(r => Cell(r[queries])).Any(g => g.Select(r => Cell(r[plans])).Distinct().Count() >= 2), "a query with two plans");
        Assert.Contains(expected.Rows, r => Equals(r[forced], true));
        Assert.Contains(expected.Rows, r => Cell(r[module]) != "Adhoc" && Cell(r[module]) != "Unknown");
        Assert.All(expected.Rows, r => Assert.NotNull(r[replica]));
        Assert.All(expected.Rows, r => Assert.NotNull(r[start]));
        AssertTextIdsDifferFromQueryIds(expected.Rows.Select(r => Convert.ToInt64(r[queries], CultureInfo.InvariantCulture)), queryTextIds);
    }

    /// <summary>
    /// The text is keyed by <c>query_text_id</c>, not <c>query_id</c>. A fixture where every query has its own text makes the two ids match on every
    /// row, so a join on the wrong one would still pass. Here two shipped queries must share ONE text id (the same statement under two SET options
    /// is a second query with the same text), and at least one shipped query must have a text id that is not its own query id.
    /// </summary>
    private static void AssertTextIdsDifferFromQueryIds(IEnumerable<long> shippedQueryIds, IReadOnlyDictionary<long, long> queryTextIds)
    {
        var shipped = shippedQueryIds.Distinct().ToList();
        Assert.All(shipped, id => Assert.True(queryTextIds.ContainsKey(id), $"query {id} is not in the fixture map"));
        Assert.Contains(shipped.GroupBy(id => queryTextIds[id]), g => g.Count() >= 2);
        Assert.Contains(shipped, id => queryTextIds[id] != id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PayloadWithInlineText_ReturnsExactlyWhatTheJoinedStatementReturned(bool azure)
    {
        var expected = await AssertPayloadSameAsOldAsync(azure, textSeparately: false);
        var text = Array.IndexOf(expected.Columns, "query_sql_text");
        Assert.All(expected.Rows, r => Assert.False(string.IsNullOrEmpty(r[text] as string)));
        var queries = Array.IndexOf(expected.Columns, "query_id");
        var byTextId = expected.Rows.GroupBy(r => _scratch.QueryTextIds[Convert.ToInt64(r[queries], CultureInfo.InvariantCulture)]);
        Assert.All(byTextId, g => Assert.Single(g.Select(r => (string?)r[text]).Distinct(StringComparer.Ordinal)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PayloadWithoutText_ReturnsExactlyWhatTheJoinedStatementReturned(bool azure)
    {
        var expected = await AssertPayloadSameAsOldAsync(azure, textSeparately: true);
        var text = Array.IndexOf(expected.Columns, "query_sql_text");
        Assert.All(expected.Rows, r => Assert.Null(r[text]));
    }

    [Theory]
    [InlineData(long.MaxValue / 4)]
    [InlineData(300L)]
    public async Task TextFetch_ReturnsExactlyWhatTheJoinedStatementReturned(long budget)
    {
        Assert.SkipWhen(_scratch.Database is null, "Set DARLING_TEST_SQL (with DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD) to run the Query Store split-join live tests.");
        var ct = TestContext.Current.CancellationToken;
        var database = _scratch.Database!;
        var ids = _scratch.QueryIds;
        Assert.True(ids.Count >= 6, $"the fixture holds {ids.Count} queries");
        AssertTextIdsDifferFromQueryIds(ids, _scratch.QueryTextIds);
        var idList = string.Join(", ", ids.Select(i => i.ToString(CultureInfo.InvariantCulture)));

        var query = QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(database, Context(azure: false, textSeparately: true), ids, budget);
        var actual = await RunAsync("master", query, ct);
        var expected = await RunAsync(database, new CollectorQuery(string.Format(CultureInfo.InvariantCulture, OldTextFetch, idList, budget)), ct);

        Assert.NotEmpty(expected.Rows);
        if (budget < 1000)
        {
            /* A budget this small cuts the batch short: some ids ship and some do not, so the cut itself is compared. */
            Assert.InRange(expected.Rows.Count, 1, ids.Count - 1);
        }
        else
        {
            Assert.Equal(ids.Count, expected.Rows.Count);
        }

        AssertSame(expected, actual, "query_id");
        Assert.All(actual.Rows, r => Assert.False(string.IsNullOrEmpty(r[2] as string)));
    }

    public sealed class ScratchDatabase : IAsyncLifetime
    {
        public string? Database { get; private set; }
        public IReadOnlyList<long> QueryIds { get; private set; } = Array.Empty<long>();

        /// <summary>Every query's text id. Two queries share one when the same statement ran under different SET options.</summary>
        public IReadOnlyDictionary<long, long> QueryTextIds { get; private set; } = new Dictionary<long, long>();

        public static SqlConnectionStringBuilder Admin(string database) => new()
        {
            DataSource = Environment.GetEnvironmentVariable("DARLING_TEST_SQL"),
            InitialCatalog = database,
            UserID = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            Pooling = false,
        };

        private static async Task SqlAsync(string database, string sql, CancellationToken ct)
        {
            await using var connection = new SqlConnection(Admin(database).ConnectionString);
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(sql, connection) { CommandTimeout = 180 };
            await command.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask InitializeAsync()
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL"))) return;
            var ct = TestContext.Current.CancellationToken;
            var database = "pm_6257_" + Guid.NewGuid().ToString("N")[..12];
            Database = database;

            /* The table first and Query Store second, so the setup statements are not plans in the store. One minute intervals, so a wait makes a second one. */
            await SqlAsync("master", $"CREATE DATABASE [{database}];", ct);
            await SqlAsync(database, @"
CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, k int NOT NULL, pad char(200) NOT NULL DEFAULT 'x');
INSERT dbo.t (k) SELECT TOP (20000) 1 FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;
INSERT dbo.t (k) SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) + 1 FROM sys.all_columns;
CREATE INDEX ix_k ON dbo.t (k);", ct);
            await SqlAsync(database, "EXEC('CREATE PROCEDURE dbo.p1 @k int AS SELECT COUNT_BIG(*) AS p1c, MAX(pad) AS p1m FROM dbo.t WHERE k = @k;');", ct);
            await SqlAsync(database, "EXEC('CREATE PROCEDURE dbo.p2 @k int AS SELECT COUNT_BIG(*) AS p2c FROM dbo.t WHERE k = @k;');", ct);
            await SqlAsync("master", $"ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL, INTERVAL_LENGTH_MINUTES = 1, DATA_FLUSH_INTERVAL_SECONDS = 60);", ct);

            /* Interval one: four ad hoc queries, a procedure with two plans (a recompile under a different parameter), a second procedure. */
            await SqlAsync(database, @"
SELECT COUNT_BIG(*) AS qa FROM dbo.t WHERE k = 1;
SELECT COUNT_BIG(*) AS qa FROM dbo.t WHERE k = 1;
SELECT TOP (1) id AS qb FROM dbo.t;
SELECT MAX(pad) AS qc FROM dbo.t WHERE k = 7;
SELECT MIN(id) AS qd FROM dbo.t WHERE k BETWEEN 2 AND 9;
EXEC dbo.p1 @k = 5;
EXEC sp_recompile 'dbo.p1';
EXEC dbo.p1 @k = 1;
EXEC dbo.p2 @k = 3;", ct);

            /* One statement text, two queries: a second SET option set makes Query Store record a new query_id against the SAME query_text_id. Both settings
               are explicit, because the client's own default for ARITHABORT is not the server's. */
            await SqlAsync(database, "SET ARITHABORT ON; EXEC sys.sp_executesql N'SELECT MAX(id) AS qshared FROM dbo.t WHERE k = 3';", ct);
            await SqlAsync(database, "SET ARITHABORT OFF; EXEC sys.sp_executesql N'SELECT MAX(id) AS qshared FROM dbo.t WHERE k = 3';", ct);
            await SqlAsync(database, @"
DECLARE @q bigint, @p bigint;
SELECT TOP (1) @q = q.query_id FROM sys.query_store_query AS q WHERE q.object_id = OBJECT_ID('dbo.p1');
SELECT TOP (1) @p = p.plan_id FROM sys.query_store_plan AS p WHERE p.query_id = @q ORDER BY p.plan_id;
EXEC sys.sp_query_store_force_plan @q, @p;
EXEC sys.sp_query_store_flush_db;", ct);

            /* Wait out the one minute interval, then interval two: one old query again, one new, a procedure again. */
            await Task.Delay(TimeSpan.FromSeconds(66), ct);
            await SqlAsync(database, @"
SELECT COUNT_BIG(*) AS qa FROM dbo.t WHERE k = 1;
SELECT SUM(CONVERT(bigint, id)) AS qe FROM dbo.t WHERE k = 4;
EXEC dbo.p1 @k = 1;
EXEC dbo.p2 @k = 3;
EXEC sys.sp_query_store_flush_db;", ct);

            /* Capture off, so the collector's own statements never enter the store between the two reads. The queries it already holds keep their runtime
               stats, so one more run lands an in-memory slice beside the flushed one in the same interval. */
            await SqlAsync("master", $"ALTER DATABASE [{database}] SET QUERY_STORE (QUERY_CAPTURE_MODE = NONE);", ct);
            await SqlAsync(database, "SELECT COUNT_BIG(*) AS qa FROM dbo.t WHERE k = 1;", ct);

            var ids = new List<long>();
            var textIds = new Dictionary<long, long>();
            await using (var connection = new SqlConnection(Admin(database).ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var command = new SqlCommand("SELECT query_id, query_text_id FROM sys.query_store_query ORDER BY query_id;", connection) { CommandTimeout = 60 };
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    ids.Add(reader.GetInt64(0));
                    textIds[reader.GetInt64(0)] = reader.GetInt64(1);
                }
            }

            QueryIds = ids;
            QueryTextIds = textIds;
        }

        public async ValueTask DisposeAsync()
        {
            if (Database is null) return;
            try
            {
                await SqlAsync("master", $"IF DB_ID(N'{Database}') IS NOT NULL BEGIN ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{Database}]; END;", CancellationToken.None);
            }
            catch (SqlException)
            {
                /* The assertions report a real failure; a cleanup that cannot reach the server leaves a uniquely named scratch database. */
            }
        }
    }
}
