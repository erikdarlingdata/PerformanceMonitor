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
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3902: the parameter-sensitivity drill-down resolves statement text for the rows it PRINTS, not for
/// every plan-cache row in the analysis window.
///
/// <para>The read used to window over <c>v_query_stats</c>, whose <c>query_text</c> column is
/// <c>COALESCE(f.query_text, qtd.query_text)</c> over a LEFT JOIN to <c>query_text_dim</c> — the whole
/// fleet's text dimension. Selecting that column made the planner resolve text for every row in the
/// window and carry it through the <c>ROW_NUMBER</c> sort, to print five. On DARLING01 the plan was a
/// sequential scan of the dimension hashed against the window; on a fleet-sized dimension (seeded at
/// 400,000 texts) it was one index probe per window row.</para>
///
/// <para>Two halves, both live. The shipped read must return EXACTLY what the old one returned — every
/// column, every row, in order, including the three text cases the view's COALESCE decides (inline legacy
/// text wins over the digest; a digest with no dimension row yet reads as empty; long text truncates at
/// 500). The oracle is the old SQL verbatim. And it must touch the dimension for the output rows only,
/// measured from the plan's own row counts — with the oracle run through the same measurement as the
/// positive control, so a measurement that could not see a dimension read would fail there instead of
/// passing here.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ParameterSensitiveDrillDownTextLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -390201;
    private const string TestServerName = "PspTextSrv";

    /// <summary>The drill-down's own output cap (the SQL's <c>LIMIT 5</c>).</summary>
    private const int OutputRows = 5;

    /// <summary>Qualifying plans: more than the cap, so the LIMIT has something to cut.</summary>
    private const int QualifyingPlans = 12;

    /// <summary>Non-qualifying plans (worker spread under 10x) — window rows that never print.</summary>
    private const int QuietPlans = 40;

    /// <summary>Snapshots per plan inside the window: the latest must win, and its text with it.</summary>
    private const int SnapshotsPerPlan = 3;

    /// <summary>
    /// The pre-#3902 read, verbatim — the oracle the shipped read must match row for row. It is the slow
    /// shape by construction (text resolved for every window row), which is why it is the oracle and not
    /// the read.
    /// </summary>
    private const string OracleSql = @"
WITH svr AS
(
    SELECT COALESCE
    (
        (
            SELECT sp.utc_offset_minutes
            FROM server_properties AS sp
            WHERE sp.server_id = $1
            AND   sp.utc_offset_minutes IS NOT NULL
            ORDER BY sp.collection_time DESC
            LIMIT 1
        ),
        0
    ) AS offset_minutes
),
latest AS
(
    SELECT
        database_name,
        query_hash,
        query_plan_hash,
        execution_count,
        creation_time - make_interval(mins => svr.offset_minutes) AS creation_time_utc,
        min_worker_time,
        max_worker_time,
        min_grant_kb,
        max_grant_kb,
        min_spills,
        max_spills,
        query_text,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_hash, query_plan_hash
            ORDER BY collection_time DESC
        ) AS rn
    FROM v_query_stats, svr
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
)
SELECT
    database_name,
    query_hash,
    query_plan_hash,
    execution_count,
    min_worker_time,
    max_worker_time,
    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,
    max_grant_kb::DOUBLE PRECISION / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence,
    LEFT(query_text, 500) AS query_text
FROM latest
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time_utc <= $2
AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
LIMIT 5";

    /// <summary>Every command here is a sentinel-scoped seed, read or cleanup — seconds of work.</summary>
    private const int LiveTimeoutSeconds = 60;

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    [Fact]
    public async Task TheDrillDown_ReturnsTheOraclesRows_AndResolvesTextForThePrintedRowsOnly_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3902 PSP text test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var windowStart = windowEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
            };

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedAsync(connection, windowStart, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var shipped = await DrillDownRowsAsync(postgres, context);

            List<object?[]> oracle;
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                oracle = await ReadOracleAsync(connection, windowStart, windowEnd, ct);
            }

            /* The cap cut something, so the order and the cut are both under test. */
            Assert.Equal(OutputRows, oracle.Count);
            Assert.Equal(oracle.Count, shipped.Count);

            for (var i = 0; i < oracle.Count; i++)
            {
                var o = oracle[i];
                var s = shipped[i];
                Assert.Equal((string)o[0]!, s.GetProperty("database").GetString());
                Assert.Equal((string)o[1]!, s.GetProperty("query_hash").GetString());
                Assert.Equal((string)o[2]!, s.GetProperty("query_plan_hash").GetString());
                Assert.Equal(Convert.ToInt64(o[3], CultureInfo.InvariantCulture), s.GetProperty("execution_count").GetInt64());
                Assert.Equal(Convert.ToInt64(o[4], CultureInfo.InvariantCulture), s.GetProperty("min_worker_time_us").GetInt64());
                Assert.Equal(Convert.ToInt64(o[5], CultureInfo.InvariantCulture), s.GetProperty("max_worker_time_us").GetInt64());
                Assert.Equal(Convert.ToDouble(o[6], CultureInfo.InvariantCulture), s.GetProperty("worker_ratio").GetDouble());
                Assert.Equal(Convert.ToDouble(o[7], CultureInfo.InvariantCulture), s.GetProperty("grant_ratio").GetDouble());
                Assert.Equal(Convert.ToInt32(o[8], CultureInfo.InvariantCulture) == 1, s.GetProperty("spills_on_some_inputs").GetBoolean());
                Assert.Equal((string?)o[9] ?? "", s.GetProperty("query_text").GetString());
            }

            /* The three text cases the view's COALESCE decides, pinned by value as well as by agreement
               with the oracle, so an oracle that had drifted with the read could not carry them both. */
            Assert.Equal(InlineText(0), shipped[0].GetProperty("query_text").GetString());
            Assert.Equal("", shipped[1].GetProperty("query_text").GetString());
            Assert.Equal(DimText(2, latest: true)[..500], shipped[2].GetProperty("query_text").GetString());
            Assert.Equal(DimText(3, latest: true), shipped[3].GetProperty("query_text").GetString());

            /* The read shape: how many rows the plan resolves against the text dimension. The shipped read
               resolves the rows it prints; the old one resolved every window row. */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                var shippedResolved = await RowsResolvedAgainstTheDimensionAsync(
                    connection, PgDrillDownCollector.ParameterSensitiveSql, windowStart, windowEnd, ct);
                var oracleResolved = await RowsResolvedAgainstTheDimensionAsync(
                    connection, OracleSql, windowStart, windowEnd, ct);

                Assert.True(
                    shippedResolved <= OutputRows,
                    $"the drill-down resolved text for {shippedResolved} row(s) to print {OutputRows}: text is being "
                    + "resolved for window rows that never print (#3902).");
                Assert.True(
                    oracleResolved >= (QualifyingPlans + QuietPlans) * SnapshotsPerPlan,
                    $"positive control: the old read resolved text for only {oracleResolved} row(s), so this "
                    + "measurement cannot tell the two shapes apart.");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>
    /// The rows a plan resolves against <c>query_text_dim</c>: at every join with the dimension on one side,
    /// the actual rows (across loops) arriving from the OTHER side. Counted at the join rather than at the
    /// dimension's scan because the join METHOD is the planner's call — a hash join over a dimension this
    /// small reads all of it for five probe rows, and a nested loop over a large one reads one row per
    /// probe — while the probe side is the property the read shape decides.
    /// </summary>
    private static async Task<double> RowsResolvedAgainstTheDimensionAsync(
        NpgsqlConnection connection, string sql, DateTime windowStart, DateTime windowEnd, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        AddWindowParameters(cmd, windowStart, windowEnd);

        var json = (string)(await cmd.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);

        var resolved = 0.0;
        Walk(document.RootElement[0].GetProperty("Plan"));
        return resolved;

        void Walk(JsonElement node)
        {
            if (!node.TryGetProperty("Plans", out var children))
            {
                return;
            }

            /* Join inputs only: an InitPlan or SubPlan child is not a side of the join. */
            var inputs = children.EnumerateArray()
                .Where(c => c.GetProperty("Parent Relationship").GetString() is "Outer" or "Inner")
                .ToList();

            if (inputs.Count == 2 && inputs.Count(ReadsTheDimension) == 1)
            {
                var probe = inputs.Single(c => !ReadsTheDimension(c));
                resolved += probe.GetProperty("Actual Rows").GetDouble() * probe.GetProperty("Actual Loops").GetDouble();
            }

            foreach (var child in children.EnumerateArray())
            {
                Walk(child);
            }
        }

        static bool ReadsTheDimension(JsonElement node) =>
            (node.TryGetProperty("Relation Name", out var relation)
             && relation.GetString() == PayloadDimensions.QueryTextDimTable)
            || (node.TryGetProperty("Plans", out var children) && children.EnumerateArray().Any(ReadsTheDimension));
    }

    private static async Task<List<JsonElement>> DrillDownRowsAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PARAMETER_SENSITIVITY",
            StoryPath = "PARAMETER_SENSITIVITY",
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = ["PARAMETER_SENSITIVITY"],
            /* Past the display gate — below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("parameter_sensitive_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    private static async Task<List<object?[]>> ReadOracleAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(OracleSql, connection) { CommandTimeout = LiveTimeoutSeconds };
        AddWindowParameters(cmd, windowStart, windowEnd);

        var rows = new List<object?[]>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    private static void AddWindowParameters(NpgsqlCommand cmd, DateTime windowStart, DateTime windowEnd)
    {
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
    }

    /// <summary>
    /// The window: <see cref="QualifyingPlans"/> plans past every detector floor with strictly
    /// descending worker spreads (so the top five and their order are unambiguous), and
    /// <see cref="QuietPlans"/> that clear nothing. Each plan is collected <see cref="SnapshotsPerPlan"/>
    /// times, and only the LATEST snapshot carries the plan's printed text — older snapshots point at a
    /// different dimension row, so resolving the wrong row of the partition shows up as the wrong text.
    ///
    /// <para>Plan 0's latest row carries inline legacy text beside a digest (the COALESCE must prefer the
    /// inline text, as the view does); plan 1's latest digest has no dimension row yet (reads as empty);
    /// plan 2's text is longer than the 500-character cut.</para>
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime windowStart, CancellationToken ct)
    {
        var compiledBeforeWindow = windowStart.AddDays(-2);
        var id = -39_020_100L;

        for (var plan = 0; plan < QualifyingPlans + QuietPlans; plan++)
        {
            var qualifies = plan < QualifyingPlans;

            /* Qualifying: min 20 ms, max 20 ms x (60 - plan) — spreads of 60x down to 49x, all distinct.
               Quiet: a 2x spread, far under the 10x ratio floor. */
            var minWorker = 20_000L;
            var maxWorker = qualifies ? 20_000L * (60 - plan) : 40_000L;

            for (var snapshot = 0; snapshot < SnapshotsPerPlan; snapshot++)
            {
                var latest = snapshot == SnapshotsPerPlan - 1;
                var digest = Digest(plan, latest);

                if (!(plan == 1 && latest))
                {
                    await InsertDimRowAsync(connection, digest, DimText(plan, latest), ct);
                }

                await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, query_text_digest, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1024, $12, 0, $13, $14, $15, 25)", connection)
                {
                    CommandTimeout = LiveTimeoutSeconds
                };
                cmd.Parameters.AddWithValue(id--);
                cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart.AddMinutes(30 + (snapshot * 60)), DateTimeKind.Unspecified));
                cmd.Parameters.AddWithValue(TestServerId);
                cmd.Parameters.AddWithValue(TestServerName);
                cmd.Parameters.AddWithValue("PspTextDb" + (plan % 3).ToString(CultureInfo.InvariantCulture));
                cmd.Parameters.AddWithValue("0xQH_PSPTEXT_" + plan.ToString(CultureInfo.InvariantCulture));
                cmd.Parameters.AddWithValue("0xPH_PSPTEXT_" + plan.ToString(CultureInfo.InvariantCulture));
                cmd.Parameters.AddWithValue(DateTime.SpecifyKind(compiledBeforeWindow, DateTimeKind.Unspecified));
                cmd.Parameters.AddWithValue(500L + snapshot);
                cmd.Parameters.AddWithValue(minWorker);
                cmd.Parameters.AddWithValue(maxWorker);
                cmd.Parameters.AddWithValue(2048L * (plan + 1));
                cmd.Parameters.AddWithValue(plan % 2 == 0 ? 3L : 0L);
                cmd.Parameters.AddWithValue(plan == 0 && latest ? InlineText(plan) : DBNull.Value);
                cmd.Parameters.AddWithValue(digest);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task InsertDimRowAsync(NpgsqlConnection connection, byte[] digest, string text, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO query_text_dim (digest, query_text, last_seen) VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING",
            connection) { CommandTimeout = LiveTimeoutSeconds };
        cmd.Parameters.AddWithValue(digest);
        cmd.Parameters.AddWithValue(text);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A digest unique to this class, so its dimension rows can be cleaned up by value.</summary>
    private static byte[] Digest(int plan, bool latest) =>
        SHA256.HashData(Encoding.UTF8.GetBytes(
            string.Create(CultureInfo.InvariantCulture, $"#3902 psp text {TestServerId} {plan} {latest}")));

    private static string DimText(int plan, bool latest)
    {
        var head = string.Create(CultureInfo.InvariantCulture,
            $"SELECT /* {(latest ? "latest" : "older")} snapshot of plan {plan} */ col FROM dbo.PspText{plan}");

        /* Plan 2 carries text past the drill-down's 500-character cut. */
        return plan == 2 ? head + " WHERE " + string.Concat(Enumerable.Repeat("col = @p AND ", 60)) + "1 = 1" : head;
    }

    private static string InlineText(int plan) =>
        string.Create(CultureInfo.InvariantCulture, $"SELECT /* inline legacy text of plan {plan} */ col FROM dbo.PspText{plan}");

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var cmd = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        })
        {
            cmd.Parameters.AddWithValue(TestServerId);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        var digests = new List<byte[]>();
        for (var plan = 0; plan < QualifyingPlans + QuietPlans; plan++)
        {
            digests.Add(Digest(plan, latest: true));
            digests.Add(Digest(plan, latest: false));
        }

        await using (var cmd = new NpgsqlCommand("DELETE FROM query_text_dim WHERE digest = ANY($1)", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        })
        {
            cmd.Parameters.AddWithValue(digests.ToArray());
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
