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
/// SECURITY DEFINER readers and their grants, the scrub, the sensitive-statement pattern, and the MCP tool's
/// refusals, preconditions, note and text handling. The live halves are <see cref="StoreStatementStatsLiveTests"/>.
/// </summary>
public class StoreStatementStatsTests
{
    private static string Literal(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    [Fact]
    public void TheReaderFunctions_AreDefinersWithAPinnedPathAndStringMode_RevokedFromPublic_AndShowOnlyNormalizedDmlText()
    {
        var sql = StoreStatementStats.BuildFunctionSql("config", "public");

        Assert.Equal(2, Regex.Matches(sql, "SECURITY DEFINER").Count);
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape("SET search_path = pg_catalog, pg_temp")).Count);

        /* #3904's review: a string-body SQL function is parsed at CALL time under the caller's settings, so the
           body's string mode is pinned on the function itself, not trusted from the session. */
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape("SET standard_conforming_strings = on")).Count);
        Assert.Contains($"REVOKE ALL ON FUNCTION \"config\".{StoreStatementStats.FunctionName}() FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains($"REVOKE ALL ON FUNCTION \"config\".{StoreStatementStats.InfoFunctionName}() FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains("FROM \"public\".pg_stat_statements AS s", sql, StringComparison.Ordinal);
        Assert.Contains("FROM \"public\".pg_stat_statements_info AS i", sql, StringComparison.Ordinal);
        /* #3915: an allowlist, and every row kept. Text is shown only for normalized DML the sensitive pattern
           does not name; everything else keeps its timings and reads as withheld, and no row is filtered out on
           its text any more. */
        Assert.Contains($"WHEN s.query ~* {Literal(StoreStatementStats.ReadableStatementPattern)}", sql, StringComparison.Ordinal);
        Assert.Contains($"AND s.query !~* {Literal(StoreStatementStats.SensitiveStatementPattern)}", sql, StringComparison.Ordinal);
        Assert.Contains($"ELSE {Literal(StoreStatementStats.WithheldText)}", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN s.query IS NULL THEN NULL", sql, StringComparison.Ordinal);
        Assert.Contains($"WHEN s.query = {Literal(StoreStatementStats.InsufficientPrivilegeText)} THEN s.query", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("OR s.query !~*", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_catalog.pg_roles AS r", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.current_database()", sql, StringComparison.Ordinal);

        /* No dynamic SQL inside a definer: the bodies are fixed text. */
        Assert.DoesNotContain("EXECUTE", sql, StringComparison.Ordinal);
    }

    /// <summary>On extension 1.8, which has no <c>pg_stat_statements_info</c>, the info reader answers nulls
    /// instead of failing the batch (and taking the main reader down with it) on every pass.</summary>
    [Fact]
    public void OnExtension18_TheInfoReaderAnswersNulls_AndTheMainReaderIsUnchanged()
    {
        var full = StoreStatementStats.BuildFunctionSql("config", "public");
        var withoutInfo = StoreStatementStats.BuildFunctionSql("config", "public", withInfoView: false);

        Assert.DoesNotContain("pg_stat_statements_info", withoutInfo, StringComparison.Ordinal);
        Assert.Contains("NULL::timestamp with time zone", withoutInfo, StringComparison.Ordinal);
        Assert.Contains("NULL::bigint", withoutInfo, StringComparison.Ordinal);

        /* The main reader is byte-identical in both, so only the info body depends on the version. */
        var main = new Func<string, string>(s => s[..s.IndexOf("CREATE OR REPLACE FUNCTION \"config\".store_statement_stats_info", StringComparison.Ordinal)]);
        Assert.Equal(main(full), main(withoutInfo));
    }

    /// <summary>
    /// The pattern means the same thing under either string mode because it has NO backslash in it (#3904's
    /// review: the first version's <c>\m ... \s ... \M</c> degraded to letters under
    /// <c>standard_conforming_strings = off</c>), and it names the credential-bearing statement families.
    /// </summary>
    [Fact]
    public void TheSensitivePattern_IsBackslashFree_AndNamesEveryCredentialBearingFamily()
    {
        var pattern = StoreStatementStats.SensitiveStatementPattern;

        Assert.DoesNotContain("\\", pattern, StringComparison.Ordinal);
        Assert.DoesNotContain("\\", StoreStatementStats.ReadableStatementPattern, StringComparison.Ordinal);
        foreach (var keyword in new[] { "select", "insert", "update", "delete", "merge", "with", "values", "table" })
        {
            Assert.Contains(keyword, StoreStatementStats.ReadableStatementPattern, StringComparison.Ordinal);
        }

        foreach (var family in new[] { "role", "user", "group", "subscription", "server", "password" })
        {
            Assert.Contains(family, pattern, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A schema or role name can never close a body early (#3915's review): quoting protects an identifier's
    /// double quotes, not a dollar sequence, so the dollar tags are chosen to occur in no body.
    /// </summary>
    [Fact]
    public void TheDollarTags_NeverOccurInTheBodyTheyClose()
    {
        var plain = StoreStatementStats.BuildFunctionSql("config", "public");
        Assert.Contains("AS $fn$", plain, StringComparison.Ordinal);

        var hostile = StoreStatementStats.BuildFunctionSql("config", "ext$fn$odd");
        Assert.Contains("AS $fn1$", hostile, StringComparison.Ordinal);
        Assert.DoesNotContain("AS $fn$", hostile, StringComparison.Ordinal);

        Assert.Contains("DO $do$", StoreStatementStats.BuildGrantSql("config", ["admin"]), StringComparison.Ordinal);
        var grant = StoreStatementStats.BuildGrantSql("con$do$fig", ["admin", "x$do1$y"]);
        Assert.Contains("DO $do2$", grant, StringComparison.Ordinal);
    }

    [Fact]
    public void TheExtensionSchema_IsQuotedAsAnIdentifier()
    {
        var sql = StoreStatementStats.BuildFunctionSql("config", "ext\"odd");

        Assert.Contains("FROM \"ext\"\"odd\".pg_stat_statements AS s", sql, StringComparison.Ordinal);
        Assert.Contains("FROM \"ext\"\"odd\".pg_stat_statements_info AS i", sql, StringComparison.Ordinal);
        Assert.Contains("\"ext\"\"odd\".pg_stat_statements_reset(", StoreStatementStats.BuildScrubSql("ext\"odd"), StringComparison.Ordinal);
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

    /// <summary>The scrub resets only the sensitive statements of THIS database (#3904's review: without the dbid
    /// predicate a superuser service login on a shared cluster reset other databases' entries every pass), and
    /// never through query id 0, which <c>pg_stat_statements_reset</c> reads as "every statement of the role".</summary>
    [Fact]
    public void TheScrub_ResetsOnlyThisDatabasesSensitiveStatements_NeverThroughQueryIdZero()
    {
        var sql = StoreStatementStats.ScrubSql;

        Assert.Contains("\"public\".pg_stat_statements_reset(s.userid, s.dbid, s.queryid)", sql, StringComparison.Ordinal);
        Assert.Contains($"AND   s.query ~* {Literal(StoreStatementStats.SensitiveStatementPattern)}", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE s.dbid =", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.current_database()", sql, StringComparison.Ordinal);
        Assert.Contains("AND   s.queryid <> 0", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The probe never reads <c>shared_preload_libraries</c>: that needs <c>pg_read_all_settings</c>, and the
    /// planner evaluated the read while estimating <c>unnest</c>'s rows even inside a <c>CASE</c>, so a
    /// non-superuser owner failed the whole probe on every pass (#3904's review). "Loaded" is read from
    /// <c>pg_settings</c> instead, and the privilege check uses USAGE, which is what the server itself checks.
    /// </summary>
    [Fact]
    public void TheProbe_ReadsLoadedFromPgSettings_NeverTheSuperuserOnlyPreloadList()
    {
        var probe = StoreStatementStats.ProbeSql;

        Assert.DoesNotContain("shared_preload_libraries", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("current_setting", probe, StringComparison.Ordinal);
        Assert.Contains("FROM pg_catalog.pg_settings", probe, StringComparison.Ordinal);
        Assert.Contains("'pg_stat_statements.max'", probe, StringComparison.Ordinal);
        Assert.Contains("'pg_stat_statements.track_utility'", probe, StringComparison.Ordinal);
        Assert.Contains("pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("'MEMBER'", probe, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("1.12", true, true)]
    [InlineData("1.10", true, true)]
    [InlineData("1.9", true, true)]
    [InlineData("1.8", true, false)]
    [InlineData("1.7", false, false)]
    [InlineData("1.4", false, false)]
    [InlineData("1.13dev", true, true)]
    [InlineData(null, true, true)]
    public void ExtensionVersions_GateTheReaderAt18_AndTheInfoViewAt19(string? version, bool reader, bool info)
    {
        Assert.Equal(reader, StoreStatementStats.ExtensionVersionAtLeast(version, StoreStatementStats.MinimumReaderVersion));
        Assert.Equal(info, StoreStatementStats.ExtensionVersionAtLeast(version, StoreStatementStats.InfoViewVersion));
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

    /// <summary>The four role keys, and identities that say who REALLY connects as each (#3904's review): the viewer
    /// login also carries remote read-only seats and the custom-alert evaluator, and a compose or bring-your-own
    /// store runs its web and MCP hosts as the owner.</summary>
    [Fact]
    public void TheRoleVocabulary_IsTheProductsThreeRolesAndTheOwner_WithHonestIdentities()
    {
        Assert.Equal(
            new[] { "admin", "mcp", "owner", "viewer" },
            DarlingMcpStoreQueryStatsTools.RoleIdentities.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal("admin", DarlingManagedPostgres.AdminRoleName);
        Assert.Equal("viewer", DarlingManagedPostgres.ViewerRoleName);
        Assert.Equal("mcp", DarlingManagedPostgres.McpRoleName);

        Assert.Contains("custom-alert", DarlingMcpStoreQueryStatsTools.RoleIdentities["viewer"], StringComparison.Ordinal);
        Assert.Contains("remote read-only", DarlingMcpStoreQueryStatsTools.RoleIdentities["viewer"], StringComparison.Ordinal);
        Assert.Contains("compose or bring-your-own", DarlingMcpStoreQueryStatsTools.RoleIdentities["owner"], StringComparison.Ordinal);
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

    /// <summary>A store that cannot be reached is an ERROR (the one helper's envelope), never a precondition: the
    /// precondition answers come from a catalog read that succeeded.</summary>
    [Fact]
    public async Task AnUnreachableStore_IsAnErrorEnvelope()
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Password=nothing;Timeout=1");

        var result = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(unreachable);

        Assert.StartsWith("{\"status\":\"error\"", result, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\n    -- the product's comment\n    SELECT   a,\n\t b  -- trailing\n    FROM t\n", 240, "SELECT a, b FROM t")]
    [InlineData("SELECT /* block\n comment */ 1", 240, "SELECT 1")]
    [InlineData("SELECT '--not a comment' AS x", 240, "SELECT '--not a comment' AS x")]
    [InlineData("SELECT abcdefghij FROM t", 10, "SELECT abc...")]
    [InlineData("SELECT count(*) FROM pg_class WHERE oid IN ($1 /*, ... */) /* probe */", 240, "SELECT count(*) FROM pg_class WHERE oid IN ($1 /*, ... */)")]
    [InlineData("SELECT 1 WHERE a IN ($1 /*, ... */) AND relname IN ($2 /*, ... */)", 240, "SELECT 1 WHERE a IN ($1 /*, ... */) AND relname IN ($2 /*, ... */)")]
    [InlineData("", 240, "")]
    [InlineData(null, 240, "")]
    public void CompactStatementText_StripsCommentsAndWhitespace_KeepsTheInListMarker_AndCutsWithAnEllipsis(string? text, int max, string expected)
        => Assert.Equal(expected, StoreStatementStats.CompactStatementText(text, max));

    private static DarlingMcpStoreQueryStatsTools.StatsState State(
        bool readerExists = true, bool mayRead = true, string? version = "1.12", bool loaded = true,
        string? trackUtility = "off", bool connectedAsOwner = false)
        => new(readerExists, mayRead, version, loaded, trackUtility, connectedAsOwner);

    /// <summary>Each way a store can lack statistics has its own remedy, decided from the catalog before the read,
    /// in the order the fixes have to happen in; a store with everything in place has none.</summary>
    [Fact]
    public void EachWayAStoreLacksStatistics_HasItsOwnRemedy()
    {
        Assert.Null(DarlingMcpStoreQueryStatsTools.PreconditionReason(State()));

        var notInstalled = DarlingMcpStoreQueryStatsTools.PreconditionReason(State(readerExists: false, mayRead: false, version: null, loaded: false));
        Assert.Contains("not set up", notInstalled, StringComparison.Ordinal);
        Assert.Contains("track_utility = off", notInstalled, StringComparison.Ordinal);
        Assert.Contains("CREATE EXTENSION pg_stat_statements", notInstalled, StringComparison.Ordinal);

        var dropped = DarlingMcpStoreQueryStatsTools.PreconditionReason(State(version: null));
        Assert.Contains("no longer installed", dropped, StringComparison.Ordinal);

        var old = DarlingMcpStoreQueryStatsTools.PreconditionReason(State(version: "1.7"));
        Assert.Contains("ALTER EXTENSION pg_stat_statements UPDATE", old, StringComparison.Ordinal);
        Assert.Contains("older than 1.8", old, StringComparison.Ordinal);

        var notBuilt = DarlingMcpStoreQueryStatsTools.PreconditionReason(State(readerExists: false, mayRead: false));
        Assert.Contains("has not built its reader", notBuilt, StringComparison.Ordinal);

        var notGranted = DarlingMcpStoreQueryStatsTools.PreconditionReason(State(mayRead: false));
        Assert.Contains("may not read", notGranted, StringComparison.Ordinal);

        var notLoaded = DarlingMcpStoreQueryStatsTools.PreconditionReason(State(loaded: false));
        Assert.Contains("installed but not loaded", notLoaded, StringComparison.Ordinal);

        /* The cadence the remedies cite is the real one: hourly only on a TimescaleDB store. */
        foreach (var reason in new[] { notInstalled, dropped, old, notBuilt, notGranted })
        {
            Assert.Contains("hourly on a TimescaleDB store", reason, StringComparison.Ordinal);
        }
    }

    /// <summary>The note says what the figures leave out, from the state that was read: the utility exclusion (or
    /// that utility statements are in), the owner-login deployment, text the definer may not read, the 1.8 gap,
    /// and what an eviction pass means.</summary>
    [Fact]
    public void TheNote_DisclosesWhatTheFiguresLeaveOut()
    {
        var plain = DarlingMcpStoreQueryStatsTools.BuildNote(State(), utilityTracked: false, hiddenText: 0, evictionPasses: 0);
        Assert.Contains("COPY", plain, StringComparison.Ordinal);
        Assert.Contains("get_collector_cost", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("store owner", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("insufficient privilege", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("eviction_passes counts", plain, StringComparison.Ordinal);

        var tracked = DarlingMcpStoreQueryStatsTools.BuildNote(State(trackUtility: "on"), utilityTracked: true, hiddenText: 0, evictionPasses: 0);
        Assert.Contains("Utility statements are tracked", tracked, StringComparison.Ordinal);

        var owner = DarlingMcpStoreQueryStatsTools.BuildNote(State(connectedAsOwner: true), utilityTracked: false, hiddenText: 0, evictionPasses: 0);
        Assert.Contains("compose or bring-your-own", owner, StringComparison.Ordinal);

        var hidden = DarlingMcpStoreQueryStatsTools.BuildNote(State(), utilityTracked: false, hiddenText: 7, evictionPasses: 0);
        Assert.Contains("7 statement(s) read as <insufficient privilege>", hidden, StringComparison.Ordinal);
        Assert.Contains("pg_read_all_stats", hidden, StringComparison.Ordinal);

        var old = DarlingMcpStoreQueryStatsTools.BuildNote(State(version: "1.8"), utilityTracked: false, hiddenText: 0, evictionPasses: null);
        Assert.Contains("predates pg_stat_statements_info", old, StringComparison.Ordinal);

        var evicted = DarlingMcpStoreQueryStatsTools.BuildNote(State(), utilityTracked: false, hiddenText: 0, evictionPasses: 3);
        Assert.Contains("about 5% of pg_stat_statements.max", evicted, StringComparison.Ordinal);
    }

    [Fact]
    public void TheReaders_ReadOnlyTheDefinerFunctions_AndTheStateReadOnlyTheCatalog()
    {
        foreach (var sql in new[] { DarlingMcpStoreQueryStatsTools.InfoSql, DarlingMcpStoreQueryStatsTools.ByRoleSql, DarlingMcpStoreQueryStatsTools.BuildStatementsSql("total_exec_ms") })
        {
            Assert.DoesNotContain("pg_stat_statements AS", sql, StringComparison.Ordinal);
            Assert.Contains("config.store_statement_stats", sql, StringComparison.Ordinal);
        }

        /* The precondition read touches no reader function (it is what decides whether calling one would fail)
           and no superuser-only setting. */
        var state = DarlingMcpStoreQueryStatsTools.StateSql;
        Assert.DoesNotContain("FROM config.store_statement_stats", state, StringComparison.Ordinal);
        Assert.DoesNotContain("shared_preload_libraries", state, StringComparison.Ordinal);
        Assert.Contains("to_regprocedure('config.store_statement_stats()')", state, StringComparison.Ordinal);

        Assert.Equal("total_exec_ms", DarlingMcpStoreQueryStatsTools.OrderColumns["total_time"]);
    }
}
