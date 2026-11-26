/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #9802: <see cref="ByteaMaxSql"/> must give the value that ordering by the bytes puts last, on every PostgreSQL the store runs on.
/// PostgreSQL 16 and 17 have no <c>max(bytea)</c>, so the statements that pick a group's newest text key or a stored plan failed there.
/// The values are the edges of byte ordering: the empty value, a value that is a prefix of another, a leading NUL byte, 0xff bytes, and
/// NULLs, which the aggregate must skip as <c>MAX</c> does.
/// </summary>
/* Reads no table: every statement is over VALUES on the DARLING_TEST_PG server, so the class needs no store of its own and no collection. */
public sealed class ByteaMaxSqlLiveTests
{
    private const string Values = @"(VALUES
    (1, '\x'::bytea), (2, '\x00'), (3, '\x0000'), (4, '\x00ff'), (5, '\x01'), (6, '\x61'), (7, '\x6162'), (8, '\x616200'),
    (9, '\xff'), (10, '\xff00'), (11, '\xffff'), (12, NULL), (13, NULL)) AS v(i, x)";

    /// <summary>Every pair of the values, as one group of two rows.</summary>
    private static readonly string Pairs = "SELECT a.i AS ai, b.i AS bi, u.x FROM " + Values.Replace("AS v(i, x)", "AS a(i, x)")
        + " CROSS JOIN " + Values.Replace("AS v(i, x)", "AS b(i, x)") + " CROSS JOIN LATERAL (VALUES (a.x), (b.x)) AS u(x)";

    private static async Task<object?[]> RowAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var row = new object?[reader.FieldCount];
        for (var i = 0; i < row.Length; i++)
        {
            row[i] = await reader.IsDBNullAsync(i, ct) ? null : reader.GetValue(i);
        }

        return row;
    }

    [Fact]
    public async Task EachGroupsLargestValue_IsTheOneOrderingByTheBytesPutsLast_AndNullsAreSkipped()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(baseCs);
        await connection.OpenAsync(ct);

        var row = await RowAsync(connection, $@"
WITH pairs AS ({Pairs}),
got AS (SELECT ai, bi, {ByteaMaxSql.Of("x")} AS largest FROM pairs GROUP BY ai, bi),
want AS (SELECT DISTINCT ON (ai, bi) ai, bi, x AS largest FROM pairs ORDER BY ai, bi, x DESC NULLS LAST)
SELECT count(*), count(*) FILTER (WHERE g.largest IS DISTINCT FROM w.largest), count(*) FILTER (WHERE g.largest IS NULL)
FROM got AS g JOIN want AS w USING (ai, bi)", ct);
        Assert.Equal(169L, row[0]);
        Assert.Equal(0L, row[1]);
        /* Only the four groups of two NULLs have no value. */
        Assert.Equal(4L, row[2]);

        var whole = await RowAsync(connection, $@"
SELECT {ByteaMaxSql.Of("x")}, {ByteaMaxSql.Of("x", "i <= 4")}, {ByteaMaxSql.Of("x", "i = 1 OR i >= 12")}, {ByteaMaxSql.Of("x", "i >= 12")},
       {ByteaMaxSql.Of("x", "i > 13")}, {ByteaMaxSql.Before}x{ByteaMaxSql.After}
FROM {Values}", ct);
        Assert.Equal(new byte[] { 0xff, 0xff }, whole[0]);
        Assert.Equal(new byte[] { 0x00, 0xff }, whole[1]);
        /* The empty value is a value: it wins over the NULLs. */
        Assert.Equal(System.Array.Empty<byte>(), whole[2]);
        Assert.Null(whole[3]);
        Assert.Null(whole[4]);
        Assert.Equal(new byte[] { 0xff, 0xff }, whole[5]);
    }

    [Fact]
    public async Task OnAServerThatHasMaxOverBytea_EachGroupsAnswerIsTheOneThatAggregateGives()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(baseCs);
        await connection.OpenAsync(ct);
        var version = await RowAsync(connection, "SELECT pg_catalog.current_setting('server_version_num')::int", ct);
        Assert.SkipWhen((int)version[0]! < 180000, "PostgreSQL has max(bytea) from 18, and this server is older.");

        var row = await RowAsync(connection, $@"
SELECT count(*), count(*) FILTER (WHERE g.largest IS DISTINCT FROM g.native)
FROM (SELECT ai, bi, {ByteaMaxSql.Of("x")} AS largest, MAX(x) AS native FROM ({Pairs}) AS p GROUP BY ai, bi) AS g", ct);
        Assert.Equal(169L, row[0]);
        Assert.Equal(0L, row[1]);
    }
}
