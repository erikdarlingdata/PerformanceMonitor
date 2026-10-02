/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3959, Lite's twin of Darling's <c>TopCpuQueriesTextLiveTests</c>: the top-CPU drill-down ranks and cuts to
/// five without the statement text, then reads the text for the five that print. The SQL is byte-identical to
/// Darling's (<c>DrillDownDopProvenanceParityTests</c>), so this proves the same text on DuckDB.
///
/// <para>The oracle is the old SQL verbatim, and every column of every row must match it. The seed breaks a
/// restriction that is almost right:
/// <list type="bullet">
/// <item>Three of the five printed groups have a NULL key, which GROUP BY groups and equality cannot match.</item>
/// <item>A group's texts differ by row, so MAX has a choice to make.</item>
/// <item>One group's newest row has no text.</item>
/// <item>One group's text runs past the 500-character cut.</item>
/// <item>Trap rows carry a text that would win MAX but lie outside the read's own filter.</item>
/// </list></para>
/// </summary>
public sealed class TopCpuQueriesTextTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 39590;
    private const int OtherServerId = 39591;
    private const string ServerName = "TopCpuTextSrv";
    private const string LiteFile = "Lite/Analysis/DrillDownCollector.Queries.cs";

    /// <summary>The pre-#3959 read, verbatim — the oracle the shipped read must match row for row.</summary>
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

    /// <summary>The five printed groups, in rank order: (database, hash, per-row CPU).</summary>
    private static readonly (string? Database, string? Hash, long CpuUs)[] Printed =
    [
        (null, "0xQH_TC_NULLDB", 2_500_000),
        ("TcTextDb1", null, 2_400_000),
        (null, null, 2_300_000),
        ("TcTextDb2", "0xQH_TC_LONG", 2_200_000),
        ("TcTextDb3", "0xQH_TC_PLAIN", 2_100_000),
    ];

    private const int RowsPerPrintedGroup = 4;
    private const int CrowdGroups = 40;

    /* One UtcNow read, truncated to the second, so the round-tripped last-seen timestamps compare equal. */
    private static readonly DateTime WindowEnd =
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);

    /// <summary>Sorts above every other text in the seed, so it wins MAX if any trap row is read.</summary>
    private const string TrapText = "zzz /* a row outside the read's own filter */";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public TopCpuQueriesTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task TheDrillDown_ReturnsTheOraclesRows_WithTheTextOfThePrintedGroups()
    {
        await SeedAsync();

        var shipped = await DrillDownRowsAsync();
        var oracle = await ReadOracleAsync();

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

        /* The text cases, pinned by value as well as by agreement with the oracle. */
        Assert.Equal(Text(0, 3), shipped[0].GetProperty("query_text").GetString());
        Assert.Equal(Text(1, 3), shipped[1].GetProperty("query_text").GetString());
        Assert.Equal(Text(2, 2), shipped[2].GetProperty("query_text").GetString());
        Assert.Equal(Text(3, 3)[..500], shipped[3].GetProperty("query_text").GetString());
        Assert.Equal(Text(4, 3), shipped[4].GetProperty("query_text").GetString());
    }

    [Fact]
    public void TheShippedSql_ReadsTheTextAfterTheCut_NotInsideTheWindow()
    {
        var sql = Regex.Replace(LiteInlineSql("CollectTopCpuQueries"), @"--[^\r\n]*", string.Empty);

        var cut = sql.IndexOf("LIMIT 5", StringComparison.Ordinal);
        Assert.True(cut >= 0, "the cut to five should still be there");

        /* Neither the window nor the ranking carries the text... */
        Assert.DoesNotContain("query_text", sql[..cut], StringComparison.Ordinal);

        /* ...it is read after the cut, over the same filter, with a NULL-safe path for a NULL key. */
        Assert.Contains("SELECT LEFT(MAX(v.query_text), 500)", sql[cut..], StringComparison.Ordinal);
        Assert.Contains("v.database_name IS NOT DISTINCT FROM t.database_name", sql[cut..], StringComparison.Ordinal);
        Assert.Contains("v.query_hash IS NOT DISTINCT FROM t.query_hash", sql[cut..], StringComparison.Ordinal);
    }

    private async Task<List<JsonElement>> DrillDownRowsAsync()
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "CPU_SQL_PERCENT",
            StoryPath = "CPU_SQL_PERCENT",
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = ["CPU_SQL_PERCENT"],
            /* Past the 0.5 display gate — below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("top_cpu_queries", out var raw), "top_cpu_queries was not collected");
        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    private async Task<List<object?[]>> ReadOracleAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = OracleSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd });

        var rows = new List<object?[]>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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

    /* Printed group g (0..4): four rows inside the window over two plans, row k's text Text(g, k), which sorts
       by k, so MAX wins on the group's LAST row. Group 2's last row has no text, so its winner is the row
       before. Group 3's texts run past the 500-character cut. Group 4 has three traps carrying a text that
       would win MAX: a zero-CPU row inside the window, a row before the window, and the same keys on another
       server. Crowd group c: three rows at a CPU below every printed group. */
    private async Task SeedAsync()
    {
        for (var g = 0; g < Printed.Length; g++)
        {
            var (database, hash, cpu) = Printed[g];
            for (var k = 0; k < RowsPerPrintedGroup; k++)
            {
                await InsertRowAsync(ServerId, WindowEnd.AddMinutes(-10 - (30 * k)), database, hash,
                    "0xPH_TC_" + g.ToString(CultureInfo.InvariantCulture) + "_" + (k % 2).ToString(CultureInfo.InvariantCulture),
                    cpu + k, maxDop: 1 + k, text: g == 2 && k == RowsPerPrintedGroup - 1 ? null : Text(g, k));
            }
        }

        var (trapDatabase, trapHash, _) = Printed[4];
        await InsertRowAsync(ServerId, WindowEnd.AddMinutes(-5), trapDatabase, trapHash, "0xPH_TC_TRAP", 0, 1, TrapText);
        await InsertRowAsync(ServerId, WindowStart.AddMinutes(-1), trapDatabase, trapHash, "0xPH_TC_TRAP", 50_000_000, 1, TrapText);
        await InsertRowAsync(OtherServerId, WindowEnd.AddMinutes(-5), trapDatabase, trapHash, "0xPH_TC_TRAP", 50_000_000, 1, TrapText);

        for (var c = 0; c < CrowdGroups; c++)
        {
            for (var k = 0; k < 3; k++)
            {
                await InsertRowAsync(ServerId, WindowEnd.AddMinutes(-15 - (30 * k)),
                    "TcTextDb" + (c % 3).ToString(CultureInfo.InvariantCulture),
                    "0xQH_TC_CROWD_" + c.ToString(CultureInfo.InvariantCulture),
                    "0xPH_TC_CROWD_" + c.ToString(CultureInfo.InvariantCulture),
                    1_000_000 + c, 2, Text(100 + c, k));
            }
        }
    }

    private async Task InsertRowAsync(
        int serverId, DateTime collectionTime, string? database, string? hash, string planHash, long cpuUs, long maxDop, string? text)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, max_dop, min_dop, delta_execution_count, delta_worker_time, delta_spills, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, 10, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)database ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)hash ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxDop });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs % 3 });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)text ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Row <paramref name="row"/>'s text sorts by the row number, so MAX is the group's last row. Group
    /// 3's run past the 500-character cut.</summary>
    private static string Text(int group, int row)
    {
        var head = string.Create(CultureInfo.InvariantCulture, $"SELECT /* group {group:D3} row {row} */ col FROM dbo.TopCpu{group}");
        return group == 3 ? head + " WHERE " + string.Concat(Enumerable.Repeat("col = 1 AND ", 60)) + "1 = 1" : head;
    }

    /// <summary>
    /// Lite's inline SQL: the first <c>cmd.CommandText = @"..."</c> verbatim literal after the named method's
    /// declaration (the read contains no doubled quotes, so the literal ends at the first <c>";</c>).
    /// </summary>
    private static string LiteInlineSql(string methodName)
    {
        var source = ParitySource.ReadFile(LiteFile);
        var start = source.IndexOf($"Task {methodName}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{methodName} not found in {LiteFile}");
        const string Marker = "cmd.CommandText = @\"";
        var literalStart = source.IndexOf(Marker, start, StringComparison.Ordinal) + Marker.Length;
        var literalEnd = source.IndexOf("\";", literalStart, StringComparison.Ordinal);
        return source[literalStart..literalEnd];
    }
}
