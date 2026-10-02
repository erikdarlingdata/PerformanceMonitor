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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3959: the top-CPU drill-down resolves statement text for the five groups it PRINTS, not for every
/// plan-cache row in the analysis window.
///
/// <para>The read projected <c>query_text</c> from <c>v_query_stats</c> inside its window, and that column is
/// <c>COALESCE(f.query_text, qtd.query_text)</c> over a LEFT JOIN to <c>query_text_dim</c>, the whole fleet's
/// text dimension. The planner resolved text for every row the window read and carried it through the
/// <c>ROW_NUMBER</c> sort, to print <c>LEFT(MAX(query_text), 500)</c> for five groups.</para>
///
/// <para>Two halves, both live. The shipped read must return exactly what the old one returned, every column
/// of every row in order. The oracle is the old SQL verbatim, and the seed is built to break a restriction that
/// is almost right:
/// <list type="bullet">
/// <item>Three of the five printed groups have a NULL key: NULL database, NULL hash, both NULL. GROUP BY
/// groups them, and equality matches none of them.</item>
/// <item>One printed group's winning text is the inline legacy text beside a digest, which the view's COALESCE
/// prefers.</item>
/// <item>One printed group has a digest with no dimension row yet.</item>
/// <item>One printed group has a text past the 500-character cut.</item>
/// <item>Trap rows carry a text that would win MAX but lies outside the read's own filter: zero CPU, before the
/// window, another server.</item>
/// </list>
/// The second half is that the plan resolves text only for the printed groups' rows. The oracle runs through
/// the same measurement as the positive control, so a measurement that could not see a dimension read would
/// fail there instead of passing here.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TopCpuQueriesTextLiveTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int TestServerId = -395901;
    private const int OtherServerId = -395902;
    private const string TestServerName = "TopCpuTextSrv";

    /// <summary>Rows per printed group inside the window, all of them spending CPU.</summary>
    private const int RowsPerPrintedGroup = 4;

    /// <summary>Groups ranked below the five that print.</summary>
    private const int CrowdGroups = 40;

    /// <summary>Rows per crowd group inside the window.</summary>
    private const int RowsPerCrowdGroup = 3;

    /// <summary>
    /// The pre-#3959 read, verbatim — the oracle the shipped read must match row for row. It is the slow shape by
    /// construction (text resolved and sorted for every window row), which is why it is the oracle and not the
    /// read.
    /// </summary>
    private const string OracleSql = @"
WITH windowed AS
(
    SELECT database_name, query_hash, query_plan_hash, collection_time, max_dop,
           delta_worker_time, delta_execution_count, delta_spills, query_text,
           ROW_NUMBER() OVER
           (
               PARTITION BY database_name, query_hash
               ORDER BY collection_time DESC, creation_time DESC NULLS LAST, delta_worker_time DESC NULLS LAST
           ) AS newest_rn,
           MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan
    FROM v_query_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   delta_worker_time > 0
)
SELECT database_name, query_hash,
       SUM(delta_worker_time)::BIGINT AS total_cpu_us,
       SUM(delta_execution_count)::BIGINT AS exec_count,
       MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop,
       SUM(delta_spills)::BIGINT AS spills,
       LEFT(MAX(query_text), 500) AS query_text,
       COUNT(DISTINCT query_plan_hash) AS plan_count,
       MAX(max_dop_any_plan) AS max_dop_any_plan,
       MAX(CASE WHEN max_dop = max_dop_any_plan THEN collection_time END) AS max_dop_any_plan_last_seen
FROM windowed
GROUP BY database_name, query_hash
ORDER BY total_cpu_us DESC
LIMIT 5";

    /// <summary>Every command here is a sentinel-scoped seed, read or cleanup — seconds of work.</summary>
    private const int LiveTimeoutSeconds = 60;

    /// <summary>The five printed groups, in rank order: (database, hash, per-row CPU).</summary>
    private static readonly (string? Database, string? Hash, long CpuUs)[] Printed =
    [
        (null, "0xQH_TC_NULLDB", 2_500_000),
        ("TcTextDb1", null, 2_400_000),
        (null, null, 2_300_000),
        ("TcTextDb2", "0xQH_TC_LONG", 2_200_000),
        ("TcTextDb3", "0xQH_TC_PLAIN", 2_100_000),
    ];

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    [Fact]
    public async Task TheDrillDown_ReturnsTheOraclesRows_AndResolvesTextForThePrintedGroupsOnly_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3959 top-CPU text test.");

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
                ServerUtcOffset = TimeSpan.Zero,
            };

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedAsync(connection, windowStart, windowEnd, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var shipped = await DrillDownRowsAsync(postgres, context);

            List<object?[]> oracle;
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                oracle = await ReadOracleAsync(connection, windowStart, windowEnd, ct);
            }

            Assert.Equal(Printed.Length, oracle.Count);
            Assert.Equal(oracle.Count, shipped.Count);

            for (var i = 0; i < oracle.Count; i++)
            {
                var o = oracle[i];
                var s = shipped[i];
                Assert.Equal((string?)o[0] ?? "", s.GetProperty("database").GetString());
                Assert.Equal((string?)o[1] ?? "", s.GetProperty("query_hash").GetString());
                Assert.Equal(Convert.ToDouble(o[2], CultureInfo.InvariantCulture) / 1000.0, s.GetProperty("total_cpu_ms").GetDouble());
                Assert.Equal(Convert.ToInt64(o[3], CultureInfo.InvariantCulture), s.GetProperty("execution_count").GetInt64());
                Assert.Equal(Convert.ToInt32(o[4], CultureInfo.InvariantCulture), s.GetProperty("max_dop").GetInt32());
                Assert.Equal(Convert.ToInt64(o[5], CultureInfo.InvariantCulture), s.GetProperty("spills").GetInt64());
                Assert.Equal((string?)o[6] ?? "", s.GetProperty("query_text").GetString());
                Assert.Equal(Convert.ToInt64(o[7], CultureInfo.InvariantCulture), s.GetProperty("plan_count").GetInt64());
                Assert.Equal(Convert.ToInt32(o[8], CultureInfo.InvariantCulture), s.GetProperty("max_dop_any_plan").GetInt32());
                Assert.Equal(((DateTime)o[9]!).ToString("o"), s.GetProperty("max_dop_any_plan_last_seen").GetString());
            }

            /* The text cases, pinned by value as well as by agreement with the oracle, so an oracle that had
               drifted with the read could not carry them both. */
            Assert.Equal(DimText(0, 3), shipped[0].GetProperty("query_text").GetString());
            Assert.Equal(InlineText(1), shipped[1].GetProperty("query_text").GetString());
            Assert.Equal(DimText(2, 2), shipped[2].GetProperty("query_text").GetString());
            Assert.Equal(DimText(3, 3)[..500], shipped[3].GetProperty("query_text").GetString());
            Assert.Equal(DimText(4, 3), shipped[4].GetProperty("query_text").GetString());

            /* The read shape: rows resolved against the text dimension. The shipped read resolves the printed
               groups' rows; the old one resolved every window row it read. */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                var shippedResolved = await RowsResolvedAgainstTheDimensionAsync(
                    connection, PgDrillDownCollector.TopCpuQueriesSql, windowStart, windowEnd, ct);
                var oracleResolved = await RowsResolvedAgainstTheDimensionAsync(connection, OracleSql, windowStart, windowEnd, ct);

                Assert.True(
                    shippedResolved <= Printed.Length * RowsPerPrintedGroup,
                    $"the drill-down resolved text for {shippedResolved} row(s) to print {Printed.Length} groups of "
                    + $"{RowsPerPrintedGroup}: text is being resolved for window rows that never print (#3959).");
                Assert.True(
                    oracleResolved >= (Printed.Length * RowsPerPrintedGroup) + (CrowdGroups * RowsPerCrowdGroup),
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

    private static async Task<double> RowsResolvedAgainstTheDimensionAsync(
        NpgsqlConnection connection, string sql, DateTime windowStart, DateTime windowEnd, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        AddWindowParameters(cmd, windowStart, windowEnd);

        return ExplainJoinProbe.RowsResolvedAgainst((string)(await cmd.ExecuteScalarAsync(ct))!, PayloadDimensions.QueryTextDimTable);
    }

    private static async Task<List<JsonElement>> DrillDownRowsAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "CPU_SQL_PERCENT",
            StoryPath = "CPU_SQL_PERCENT",
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = ["CPU_SQL_PERCENT"],
            /* Past the display gate — below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("top_cpu_queries", out var raw), "top_cpu_queries was not collected");
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

    /* ── The seed ──────────────────────────────────────────────────────────────────────────────────────
       Printed group g (0..4): four rows inside the window over two plans, row k pointing at dimension text
       DimText(g, k), which sorts by k, so MAX wins on the group's LAST row. Group 1's third row also carries
       inline legacy text, which the view's COALESCE prefers and which sorts above every dimension text ('W' >
       'S'). Group 2's last row points at a digest with no dimension row, so its winner is the row before. Group
       3's texts run past the 500-character cut. Group 4 has three traps whose text would win MAX: a zero-CPU row
       inside the window, a row before the window, and the same keys on another server. Crowd group c: three
       rows at a CPU below every printed group. */

    private static async Task SeedAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, CancellationToken ct)
    {
        for (var g = 0; g < Printed.Length; g++)
        {
            var (database, hash, cpu) = Printed[g];
            for (var k = 0; k < RowsPerPrintedGroup; k++)
            {
                var missingDimRow = g == 2 && k == RowsPerPrintedGroup - 1;
                if (!missingDimRow)
                {
                    await InsertDimRowAsync(connection, Digest(g, k), DimText(g, k), ct);
                }

                await InsertRowAsync(connection, TestServerId, windowEnd.AddMinutes(-10 - (30 * k)), database, hash,
                    planHash: "0xPH_TC_" + g.ToString(CultureInfo.InvariantCulture) + "_" + (k % 2).ToString(CultureInfo.InvariantCulture),
                    cpuUs: cpu + k, maxDop: 1 + k,
                    inlineText: g == 1 && k == 2 ? InlineText(g) : null,
                    digest: Digest(g, k), ct);
            }
        }

        var (trapDatabase, trapHash, _) = Printed[4];
        await InsertDimRowAsync(connection, TrapDigest, TrapText, ct);
        await InsertRowAsync(connection, TestServerId, windowEnd.AddMinutes(-5), trapDatabase, trapHash,
            "0xPH_TC_TRAP", cpuUs: 0, maxDop: 1, inlineText: null, TrapDigest, ct);
        await InsertRowAsync(connection, TestServerId, windowStart.AddMinutes(-1), trapDatabase, trapHash,
            "0xPH_TC_TRAP", cpuUs: 50_000_000, maxDop: 1, inlineText: null, TrapDigest, ct);
        await InsertRowAsync(connection, OtherServerId, windowEnd.AddMinutes(-5), trapDatabase, trapHash,
            "0xPH_TC_TRAP", cpuUs: 50_000_000, maxDop: 1, inlineText: null, TrapDigest, ct);

        for (var c = 0; c < CrowdGroups; c++)
        {
            for (var k = 0; k < RowsPerCrowdGroup; k++)
            {
                var digest = Digest(100 + c, k);
                await InsertDimRowAsync(connection, digest, DimText(100 + c, k), ct);
                await InsertRowAsync(connection, TestServerId, windowEnd.AddMinutes(-15 - (30 * k)),
                    "TcTextDb" + (c % 3).ToString(CultureInfo.InvariantCulture),
                    "0xQH_TC_CROWD_" + c.ToString(CultureInfo.InvariantCulture),
                    "0xPH_TC_CROWD_" + c.ToString(CultureInfo.InvariantCulture),
                    cpuUs: 1_000_000 + c, maxDop: 2, inlineText: null, digest, ct);
            }
        }
    }

    private static async Task InsertRowAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, string? database, string? hash,
        string planHash, long cpuUs, long maxDop, string? inlineText, byte[] digest, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, max_dop, delta_execution_count, delta_worker_time, delta_spills, query_text, query_text_digest)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 10, $10, $11, $12, $13)", connection) { CommandTimeout = LiveTimeoutSeconds };
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(TestServerName);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)hash ?? DBNull.Value });
        cmd.Parameters.AddWithValue(planHash);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime.AddDays(-1), DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(maxDop);
        cmd.Parameters.AddWithValue(cpuUs);
        cmd.Parameters.AddWithValue(cpuUs % 3);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)inlineText ?? DBNull.Value });
        cmd.Parameters.AddWithValue(digest);
        await cmd.ExecuteNonQueryAsync(ct);
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
    private static byte[] Digest(int group, int row) =>
        SHA256.HashData(Encoding.UTF8.GetBytes(
            string.Create(CultureInfo.InvariantCulture, $"#3959 top cpu text {TestServerId} {group} {row}")));

    private static byte[] TrapDigest => Digest(-1, -1);

    /// <summary>Sorts above every other text in the seed, so it wins MAX if any trap row is read.</summary>
    private const string TrapText = "zzz /* a row outside the read's own filter */";

    /// <summary>Row <paramref name="row"/>'s text sorts by the row number, so MAX is the group's last row.
    /// Group 3's run past the 500-character cut.</summary>
    private static string DimText(int group, int row)
    {
        var head = string.Create(CultureInfo.InvariantCulture, $"SELECT /* group {group:D3} row {row} */ col FROM dbo.TopCpu{group}");
        return group == 3 ? head + " WHERE " + string.Concat(Enumerable.Repeat("col = 1 AND ", 60)) + "1 = 1" : head;
    }

    private static string InlineText(int group) =>
        string.Create(CultureInfo.InvariantCulture, $"WITH /* inline legacy text of group {group:D3} */ x AS (SELECT 1) SELECT * FROM x");

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
        await using (var cmd = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id IN ($1, $2)", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        })
        {
            cmd.Parameters.AddWithValue(TestServerId);
            cmd.Parameters.AddWithValue(OtherServerId);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        var digests = new List<byte[]> { TrapDigest };
        for (var g = 0; g < Printed.Length; g++)
        {
            for (var k = 0; k < RowsPerPrintedGroup; k++)
                digests.Add(Digest(g, k));
        }

        for (var c = 0; c < CrowdGroups; c++)
        {
            for (var k = 0; k < RowsPerCrowdGroup; k++)
                digests.Add(Digest(100 + c, k));
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
