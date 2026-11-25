/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9195: <see cref="LivePlanXmlRead.FirstValueAsync"/> returns, from a real SQL Server through Microsoft.Data.SqlClient,
/// the same string <c>ExecuteScalarAsync</c> returns for the same query. <see cref="LivePlanXmlReadTests"/> holds the
/// read to its shape against a fake reader, which cannot say what the provider hands back for an <c>xml</c> or an
/// <c>nvarchar(max)</c> value read in chunks under <see cref="CommandBehavior.SequentialAccess"/>. Here both reads run
/// against the server: an <c>xml</c> value and an <c>nvarchar(max)</c> value of several million characters, small
/// values, an empty string, a NULL and a query with no row.
///
/// <para>Each large value is one unit of text repeated, and spans more than ninety chunks. The <c>nvarchar(max)</c> unit
/// is 27 UTF-16 code units and a chunk is a power of two, so across the value a chunk boundary lands on every offset
/// of the unit, between the two halves of its surrogate pair among them.</para>
///
/// <para>One class covers both products. Lite's <c>SqlPlanFetcher</c> and Darling's <c>PgPlanFetcher</c> hold no read
/// of their own: each hands its <see cref="SqlCommand"/> to this one helper.</para>
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It only runs
/// SELECT statements over literals and parameters: it reads no table and creates nothing.</para>
/// </summary>
public sealed class LivePlanXmlReadLiveSqlTests
{
    private const string SkipReason =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live plan read test.";

    private const int SeveralMillionCharacters = 3_000_000;

    private const string RepeatedXmlSql =
        "SELECT CONVERT(xml, @head + REPLICATE(CONVERT(nvarchar(max), @unit), @copies) + @tail);";

    private const string RepeatedTextSql =
        "SELECT REPLICATE(CONVERT(nvarchar(max), @unit), @copies);";

    private const string XmlHead =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\"><BatchSequence><Batch><Statements>";

    /// <summary>One statement of a batch plan: escaped characters and a non-ASCII letter in an attribute, an empty element and a text node.</summary>
    private const string XmlUnit =
        "<StmtSimple StatementText=\"SELECT c = N'a &amp; b \u00E9' WHERE 1 &lt; 2 AND 3 &gt; 2;\" StatementId=\"1\">" +
        "<QueryPlan><RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" /><Note>text</Note></QueryPlan></StmtSimple>";

    private const string XmlTail = "</Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>27 UTF-16 code units, a surrogate pair among them.</summary>
    private const string TextUnit = "plan text 0123456789 \u00E9 \uD83D\uDE00;.";

    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-9195-live-plan-read-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    [Fact]
    public async Task AnXmlValueOfSeveralMillionCharacters_ReadsTheSame_AsExecuteScalar()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);

        var (chunked, scalar) = await ReadBothWaysAsync(connectionString!, RepeatedXmlSql, command =>
        {
            command.Parameters.Add(new SqlParameter("@head", SqlDbType.NVarChar, -1) { Value = XmlHead });
            command.Parameters.Add(new SqlParameter("@unit", SqlDbType.NVarChar, -1) { Value = XmlUnit });
            command.Parameters.Add(new SqlParameter("@tail", SqlDbType.NVarChar, -1) { Value = XmlTail });
            command.Parameters.Add(new SqlParameter("@copies", SqlDbType.Int) { Value = 25_000 });
        });

        var expected = Assert.IsType<string>(scalar);
        Assert.True(expected.Length > SeveralMillionCharacters, $"The server returned {expected.Length} characters.");
        Assert.StartsWith("<ShowPlanXML", expected, StringComparison.Ordinal);
        AssertSameText(expected, chunked);
    }

    [Fact]
    public async Task AnNvarcharMaxValueOfSeveralMillionCharacters_ReadsTheSame_AsExecuteScalar()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        Assert.Equal(27, TextUnit.Length);

        const int Copies = 150_000;
        var (chunked, scalar) = await ReadBothWaysAsync(connectionString!, RepeatedTextSql, command =>
        {
            command.Parameters.Add(new SqlParameter("@unit", SqlDbType.NVarChar, -1) { Value = TextUnit });
            command.Parameters.Add(new SqlParameter("@copies", SqlDbType.Int) { Value = Copies });
        });

        var expected = Assert.IsType<string>(scalar);
        Assert.Equal(TextUnit.Length * Copies, expected.Length);
        Assert.True(expected.Length > SeveralMillionCharacters, $"The server returned {expected.Length} characters.");
        Assert.EndsWith(TextUnit, expected, StringComparison.Ordinal);
        AssertSameText(expected, chunked);
    }

    [Theory]
    [InlineData("SELECT CONVERT(xml, N'<a b=\"1 &amp; 2\"><c /><d>text</d></a>');")]
    [InlineData("SELECT CONVERT(nvarchar(max), N'a short value');")]
    [InlineData("SELECT CONVERT(nvarchar(max), N'');")]
    public async Task ASmallValue_ReadsTheSame_AsExecuteScalar(string sql)
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);

        var (chunked, scalar) = await ReadBothWaysAsync(connectionString!, sql, _ => { });

        AssertSameText(Assert.IsType<string>(scalar), chunked);
    }

    [Theory]
    [InlineData("SELECT CONVERT(xml, NULL);", true)]
    [InlineData("SELECT CONVERT(nvarchar(max), NULL);", true)]
    [InlineData("SELECT CONVERT(xml, N'<a />') WHERE 1 = 0;", false)]
    public async Task ANullValue_AndAQueryWithNoRow_AreNull(string sql, bool hasRow)
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);

        var (chunked, scalar) = await ReadBothWaysAsync(connectionString!, sql, _ => { });

        Assert.Null(chunked);
        if (hasRow)
        {
            Assert.Same(DBNull.Value, scalar);
        }
        else
        {
            Assert.Null(scalar);
        }
    }

    /// <summary>Runs the query twice on one connection: through the helper, then through <c>ExecuteScalarAsync</c>.</summary>
    private static async Task<(string? Chunked, object? Scalar)> ReadBothWaysAsync(
        string connectionString, string sql, Action<SqlCommand> addParameters)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        string? chunked;
        await using (var command = new SqlCommand(sql, connection) { CommandTimeout = 15 })
        {
            addParameters(command);
            chunked = await LivePlanXmlRead.FirstValueAsync(command, ct);
        }

        object? scalar;
        await using (var command = new SqlCommand(sql, connection) { CommandTimeout = 120 })
        {
            addParameters(command);
            scalar = await command.ExecuteScalarAsync(ct);
        }

        return (chunked, scalar);
    }

    /// <summary>Compares two long strings and names where they part, where an equality assert would print both.</summary>
    private static void AssertSameText(string expected, string? actual)
    {
        Assert.NotNull(actual);
        var shared = Math.Min(expected.Length, actual.Length);
        var at = 0;
        while (at < shared && expected[at] == actual[at])
        {
            at++;
        }

        Assert.True(
            at == expected.Length && at == actual.Length,
            $"ExecuteScalarAsync returned {expected.Length} characters and the chunked read {actual.Length}. They part at character {at}.");
    }
}
