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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of #4476: <see cref="WaitStatisticsArtifact.ArtifactPredicateSql"/> evaluated by REAL
/// PostgreSQL against a <c>VALUES</c> table, one row per shape, asserted equal to
/// <see cref="WaitStatisticsArtifact.IsIsolatedSingleSampleArtifact"/> for the same inputs — the SQL twin and
/// the C# original cannot drift apart without one of the two sides in this pin going red.
///
/// <para>The fragment is wrapped in <c>COALESCE((...), false)</c>: PostgreSQL's <c>AND</c> chain over a NULL
/// neighbour column evaluates to NULL, not false, and a caller summing <c>FILTER (WHERE is_artifact)</c>
/// needs a boolean, never an unknown.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class WaitStatisticsArtifactSqlEquivalenceLiveTests
{
    private const int Gauge65792 = PerfmonCounterTypes.PerfCounterLargeRawCount;
    private const int Rate272696576 = 272696576;

    [Fact]
    public async Task ThePredicateSql_AgreesWithTheCSharpOriginal_OnEveryFieldEvidenceShape()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the artifact predicate SQL equivalence pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* (id, cntr_type, object_name, prev_value, cntr_value, next_value, expected) */
        var rows = new List<(int Id, int CntrType, string? ObjectName, long? Prev, long Value, long? Next, bool Expected)>
        {
            /* The four field-evidence shapes. */
            (1, Gauge65792, "SQLServer:Wait Statistics", 318L, 214_396_451L, 1_021L, true),
            (2, Gauge65792, "SQLServer:Wait Statistics", 612L, 90_893_408L, 1_021L, true),
            (3, Gauge65792, "SQLServer:Wait Statistics", 40L, 32_713_827L, 878L, true),
            (4, Gauge65792, "SQLServer:Wait Statistics", 816L, 127_703_309L, 267L, true),
            /* Large but under the neighbour-ratio or absolute floor. */
            (5, Gauge65792, "SQLServer:Wait Statistics", 1_000L, 5_000_000L, 5_000_100L, false),
            (6, Gauge65792, "SQLServer:Wait Statistics", 300L, 200_000L, 300L, false),
            (7, Gauge65792, "SQLServer:Wait Statistics", 5L, 999_999L, 5L, false),
            /* A NULL neighbour on either side. */
            (8, Gauge65792, "SQLServer:Wait Statistics", null, 214_396_451L, 1_021L, false),
            (9, Gauge65792, "SQLServer:Wait Statistics", 318L, 214_396_451L, null, false),
            /* The wrong object. */
            (10, Gauge65792, "SQLServer:Buffer Manager", 318L, 214_396_451L, 1_021L, false),
            /* The wrong type: a rate row's own large cumulative count is not evidence of this artifact. */
            (11, Rate272696576, "SQLServer:Wait Statistics", 318L, 214_396_451L, 1_021L, false),
            /* A named instance's object name still matches by suffix. */
            (12, Gauge65792, "MSSQL$X:Wait Statistics", 318L, 214_396_451L, 1_021L, true),
        };

        var fragment = WaitStatisticsArtifact.ArtifactPredicateSql("cntr_type", "object_name", "prev_value", "cntr_value", "next_value");
        /* prev_value/cntr_value/next_value are cast to bigint in the VALUES list: the predicate multiplies
           the larger neighbour by NeighborRatio (1,000), which overflows int4 (VALUES' inferred type when
           every literal happens to fit int4) on several of these rows before the comparison ever runs. */
        var valuesSql = string.Join(", ", rows.ConvertAll(r =>
            $"({r.Id}, {r.CntrType}, {Literal(r.ObjectName)}, {LiteralBigint(r.Prev)}, {r.Value}::bigint, {LiteralBigint(r.Next)})"));

        var sql = $"""
            SELECT id, COALESCE(({fragment}), false) AS hit
            FROM (VALUES {valuesSql}) AS t(id, cntr_type, object_name, prev_value, cntr_value, next_value)
            ORDER BY id
            """;

        var actual = new Dictionary<int, bool>();
        await using (var command = new NpgsqlCommand(sql, connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                actual[reader.GetInt32(0)] = reader.GetBoolean(1);
            }
        }

        Assert.Equal(rows.Count, actual.Count);
        foreach (var row in rows)
        {
            var kind = row.CntrType == Gauge65792 ? PerfmonCounterKind.Gauge : PerfmonCounterKind.Other;
            var csharp = WaitStatisticsArtifact.IsIsolatedSingleSampleArtifact(row.ObjectName, kind, row.Prev, row.Value, row.Next);
            Assert.Equal(row.Expected, csharp);
            Assert.Equal(row.Expected, actual[row.Id]);
            Assert.Equal(csharp, actual[row.Id]);
        }
    }

    private static string Literal(string? value) =>
        value is null ? "NULL" : "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static string LiteralBigint(long? value) =>
        value is null ? "NULL::bigint" : value.Value.ToString(CultureInfo.InvariantCulture) + "::bigint";
}
