/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store's own statement statistics (#3899), everything that can be proved without a server: the shape of the
/// SECURITY DEFINER readers and their grants, the scrub, and the MCP tool's refusals and text handling. The live
/// halves are <see cref="StoreStatementStatsLiveTests"/>.
/// </summary>
public class StoreStatementStatsTests
{
    [Fact]
    public void TheReaderFunctions_AreDefinersWithAPinnedPath_RevokedFromPublic_AndRefuseRoleDdl()
    {
        var sql = StoreStatementStats.BuildFunctionSql("config", "public");

        Assert.Equal(2, Regex.Matches(sql, "SECURITY DEFINER").Count);
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape("SET search_path = pg_catalog, pg_temp")).Count);
        Assert.Contains($"REVOKE ALL ON FUNCTION \"config\".{StoreStatementStats.FunctionName}() FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains($"REVOKE ALL ON FUNCTION \"config\".{StoreStatementStats.InfoFunctionName}() FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains("FROM \"public\".pg_stat_statements AS s", sql, StringComparison.Ordinal);
        Assert.Contains("FROM \"public\".pg_stat_statements_info AS i", sql, StringComparison.Ordinal);
        Assert.Contains($"s.query !~* '{StoreStatementStats.RoleDdlPattern}'", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.current_database()", sql, StringComparison.Ordinal);

        /* No dynamic SQL inside a definer: the bodies are fixed text. */
        Assert.DoesNotContain("EXECUTE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheExtensionSchema_IsQuotedAsAnIdentifier()
    {
        var sql = StoreStatementStats.BuildFunctionSql("config", "ext\"odd");

        Assert.Contains("FROM \"ext\"\"odd\".pg_stat_statements AS s", sql, StringComparison.Ordinal);
        Assert.Contains("FROM \"ext\"\"odd\".pg_stat_statements_info AS i", sql, StringComparison.Ordinal);
        Assert.Contains("\"ext\"\"odd\".pg_stat_statements_reset(", StoreStatementStats.BuildScrubRoleDdlSql("ext\"odd"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheGrant_CoversBothFunctions_OnlyForRolesThatExist()
    {
        var sql = StoreStatementStats.BuildGrantSql("config", ["admin", "view'er", "mcp"]);

        Assert.Contains("ARRAY['admin', 'view''er', 'mcp']::text[]", sql, StringComparison.Ordinal);
        Assert.Contains("IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee) THEN", sql, StringComparison.Ordinal);
        Assert.Contains($"GRANT EXECUTE ON FUNCTION %I.{StoreStatementStats.FunctionName}() TO %I", sql, StringComparison.Ordinal);
        Assert.Contains($"GRANT EXECUTE ON FUNCTION %I.{StoreStatementStats.InfoFunctionName}() TO %I", sql, StringComparison.Ordinal);
        Assert.Throws<ArgumentNullException>(() => StoreStatementStats.BuildGrantSql("config", null!));
    }

    [Fact]
    public void TheScrub_ResetsOnlyRoleDdl()
    {
        var sql = StoreStatementStats.ScrubRoleDdlSql;

        Assert.Contains("\"public\".pg_stat_statements_reset(s.userid, s.dbid, s.queryid)", sql, StringComparison.Ordinal);
        Assert.Contains($"WHERE s.query ~* '{StoreStatementStats.RoleDdlPattern}'", sql, StringComparison.Ordinal);
    }

    /// <summary>The tool orders by the reader function's columns and nothing else: every <c>order_by</c> value
    /// maps to a column the function returns, so a renamed column fails here rather than as a runtime 42703.</summary>
    [Fact]
    public void EveryOrderBy_NamesAColumnTheReaderReturns()
    {
        var sql = StoreStatementStats.BuildFunctionSql("config", "public");
        var returns = sql[sql.IndexOf("RETURNS TABLE", StringComparison.Ordinal)..sql.IndexOf("LANGUAGE sql", StringComparison.Ordinal)];
        var columns = Regex.Matches(returns, @"^\s+([a-z_]+)\s+(text|bigint|double precision)", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Contains("query", columns);
        foreach (var column in DarlingMcpStoreQueryStatsTools.OrderColumns.Values)
        {
            Assert.Contains(column, columns);
        }
    }

    [Fact]
    public void TheRoleVocabulary_IsTheProductsThreeRolesAndTheOwner()
    {
        Assert.Equal(
            new[] { "admin", "mcp", "owner", "viewer" },
            DarlingMcpStoreQueryStatsTools.RoleIdentities.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal("admin", DarlingManagedPostgres.AdminRoleName);
        Assert.Equal("viewer", DarlingManagedPostgres.ViewerRoleName);
        Assert.Equal("mcp", DarlingManagedPostgres.McpRoleName);
    }

    [Theory]
    [InlineData(0, "{\"status\":\"invalid\"", "\"parameter\":\"top\"")]
    [InlineData(1001, "{\"status\":\"invalid\"", "\"parameter\":\"top\"")]
    public async Task AnOutOfRangeTop_IsRefusedBeforeTheStoreIsTouched(int top, string status, string parameter)
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Password=nothing;Timeout=1");

        var result = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(unreachable, top: top);

        Assert.StartsWith(status, result, StringComparison.Ordinal);
        Assert.Contains(parameter, result, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUnknownOrderByOrRole_IsRefusedByName()
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Password=nothing;Timeout=1");

        var badOrder = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(unreachable, order_by: "duration");
        Assert.Contains("\"parameter\":\"order_by\"", badOrder, StringComparison.Ordinal);
        Assert.Contains("total_time", badOrder, StringComparison.Ordinal);

        var badRole = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(unreachable, role: "darling");
        Assert.Contains("\"parameter\":\"role\"", badRole, StringComparison.Ordinal);
        Assert.Contains("owner", badRole, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\n    -- the product's comment\n    SELECT   a,\n\t b  -- trailing\n    FROM t\n", 240, "SELECT a, b FROM t")]
    [InlineData("SELECT /* block\n comment */ 1", 240, "SELECT 1")]
    [InlineData("SELECT '--not a comment' AS x", 240, "SELECT '--not a comment' AS x")]
    [InlineData("SELECT abcdefghij FROM t", 10, "SELECT abc...")]
    [InlineData("", 240, "")]
    [InlineData(null, 240, "")]
    public void CompactSql_StripsCommentsAndWhitespace_AndCutsWithAnEllipsis(string? text, int max, string expected)
        => Assert.Equal(expected, DarlingMcpStoreQueryStatsTools.CompactSql(text, max));

    [Theory]
    [InlineData("42883", "not set up")]
    [InlineData("55000", "installed but not loaded")]
    [InlineData("42501", "may not read")]
    public void EachWayAStoreLacksStatistics_HasItsOwnRemedy(string sqlState, string phrase)
        => Assert.Contains(phrase, DarlingMcpStoreQueryStatsTools.UnavailableReason(sqlState), StringComparison.Ordinal);

    [Theory]
    [InlineData("57014")]
    [InlineData("08006")]
    [InlineData(null)]
    public void AnyOtherFailure_IsAnErrorNotUnavailable(string? sqlState)
        => Assert.Null(DarlingMcpStoreQueryStatsTools.UnavailableReason(sqlState));

    [Fact]
    public void TheReaders_ReadOnlyTheDefinerFunctions()
    {
        foreach (var sql in new[] { DarlingMcpStoreQueryStatsTools.InfoSql, DarlingMcpStoreQueryStatsTools.ByRoleSql, DarlingMcpStoreQueryStatsTools.BuildStatementsSql("total_exec_ms") })
        {
            Assert.DoesNotContain("pg_stat_statements AS", sql, StringComparison.Ordinal);
            Assert.Contains("config.store_statement_stats", sql, StringComparison.Ordinal);
        }

        Assert.Equal("total_exec_ms", DarlingMcpStoreQueryStatsTools.OrderColumns["total_time"]);
    }
}
