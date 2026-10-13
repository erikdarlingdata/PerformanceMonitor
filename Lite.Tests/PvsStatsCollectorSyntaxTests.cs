/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5644 W5: the on-prem Version Store query shipped with <c>...df.total_db_size_kb / 1024.0),,</c> and every
/// on-prem run failed with "Incorrect syntax near ','". The definition tests only used <c>Contains</c>, so
/// nothing parsed the text. Two guards: a lexical check that always runs (empty select-list elements in any
/// variant the collector builds), and a live <c>SET PARSEONLY ON</c> parse against a real server when
/// <c>DARLING_TEST_SQL</c> is set (the same variable the Darling live E2E uses).
/// </summary>
public sealed class PvsStatsCollectorSyntaxTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    public static IEnumerable<object[]> Variants()
    {
        yield return new object[] { "onprem-2019", 15, false, false };
        yield return new object[] { "onprem-2022", 16, false, false };
        yield return new object[] { "onprem-2025", 17, false, false };
        yield return new object[] { "managed-instance", 16, false, true };
        yield return new object[] { "azure-sql-db", 16, true, false };
    }

    private static CollectorQuery BuildQuery(int major, bool azureSqlDb, bool managedInstance)
    {
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                SqlMajorVersion = major,
                IsAzureSqlDb = azureSqlDb,
                IsAzureManagedInstance = managedInstance,
            },
            /* A non-empty exclusion list so the on-prem exclusion clause is part of the text too. */
            ExcludedDatabases = new[] { "skipme" },
        };
        return PvsStatsCollector.Instance.BuildQuery(context);
    }

    /// <summary>Drops comments and string literals so the lexical checks see only code.</summary>
    private static string StripCommentsAndStrings(string sql)
    {
        string noBlock = Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        string noLine = Regex.Replace(noBlock, @"--[^\r\n]*", " ");
        return Regex.Replace(noLine, @"'(?:[^']|'')*'", "''");
    }

    [Theory]
    [MemberData(nameof(Variants))]
    public void Query_HasNoEmptyListElement(string variant, int major, bool azureSqlDb, bool managedInstance)
    {
        string code = StripCommentsAndStrings(BuildQuery(major, azureSqlDb, managedInstance).Text);

        Assert.False(Regex.IsMatch(code, @",\s*,"), $"{variant}: doubled comma in the query text");
        Assert.False(Regex.IsMatch(code, @"\(\s*,"), $"{variant}: comma straight after an opening parenthesis");
        Assert.False(Regex.IsMatch(code, @",\s*\)"), $"{variant}: comma straight before a closing parenthesis");
        Assert.False(
            Regex.IsMatch(code, @",\s*(FROM|WHERE|GROUP\s+BY|ORDER\s+BY|OPTION|UNION)\b", RegexOptions.IgnoreCase),
            $"{variant}: trailing comma before a clause keyword");
        Assert.False(
            Regex.IsMatch(code, @"\bSELECT\s*,", RegexOptions.IgnoreCase),
            $"{variant}: comma straight after SELECT");
    }

    [Theory]
    [MemberData(nameof(Variants))]
    public async Task Query_ParsesOnALiveServer(string variant, int major, bool azureSqlDb, bool managedInstance)
    {
        string? host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL (and _USER/_PASSWORD for sql auth) to parse the query on a live server.");

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host!,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
        };
        string? user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        if (string.IsNullOrEmpty(user))
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? "";
        }

        var query = BuildQuery(major, azureSqlDb, managedInstance);
        /* The collector binds its exclusion parameters at run time; declare them so the parse resolves the names. */
        string declares = string.Concat(query.Parameters.Select(p => $"DECLARE {p.Name} nvarchar(256); "));
        string text = declares + query.Text;
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);

        /* PARSEONLY checks syntax only and runs nothing, so it is safe on a server that holds real data and it
           works on versions that lack the DMVs (the parse of 2022-only columns does not need them to exist). */
        await using (var on = new SqlCommand("SET PARSEONLY ON;", connection))
        {
            await on.ExecuteNonQueryAsync(ct);
        }

        try
        {
            await using var parse = new SqlCommand(text, connection);
            await parse.ExecuteNonQueryAsync(ct);
        }
        catch (SqlException ex)
        {
            Assert.Fail($"{variant}: the query does not parse: {ex.Message}");
        }
        finally
        {
            await using var off = new SqlCommand("SET PARSEONLY OFF;", connection);
            await off.ExecuteNonQueryAsync(ct);
        }
    }
}
