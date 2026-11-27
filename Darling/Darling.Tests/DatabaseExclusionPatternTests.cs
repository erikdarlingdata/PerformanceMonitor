/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9050: <c>*</c> in an excluded-databases entry matches any run of characters. The C# match, the SQL builder and the
/// validation, with the SQL proved on a real engine in <see cref="DatabaseExclusionPatternLiveTests"/>.
/// </summary>
public sealed class DatabaseExclusionPatternTests
{
    [Theory]
    [InlineData("archive_*", "archive_2024_01", true)]
    [InlineData("archive_*", "archive_", true)]
    [InlineData("archive_*", "archived", false)]
    [InlineData("archive_*", "xarchive_1", false)]
    [InlineData("a_b", "a_b", true)]
    [InlineData("a_b", "axb", false)]
    [InlineData("100%*", "100%x", true)]
    [InlineData("100%*", "100x", false)]
    [InlineData("[x]*", "[x]y", true)]
    [InlineData("[x]*", "xy", false)]
    [InlineData("a?c*", "a?cd", true)]
    [InlineData("a?c*", "abcd", false)]
    [InlineData("*_old", "sales_old", true)]
    [InlineData("*_old", "sales_older", false)]
    [InlineData("x*y*z", "xaybz", true)]
    [InlineData("x*y*z", "xyz", true)]
    [InlineData("x*y*z", "xzy", false)]
    [InlineData("ab*ba", "aba", false)]
    [InlineData("ab*ba", "abba", true)]
    [InlineData("ARCHIVE_*", "archive_1", true)]
    [InlineData("Exact", "exact", true)]
    [InlineData("*", "x", false)]
    [InlineData("**", "anything", false)]
    public void TheMatchFollowsTheOneWildcardRule(string entry, string name, bool matches) =>
        Assert.Equal(matches, DatabaseNamePattern.Matches(entry, name));

    [Fact]
    public void AnEntryWithoutAStarKeepsItsExactMeaning()
    {
        Assert.False(DatabaseNamePattern.IsPattern("a_b"));
        Assert.True(DatabaseNamePattern.MatchesAny(["x", "Sales"], "sales"));
        Assert.False(DatabaseNamePattern.MatchesAny(["x", "Sales"], "sales2"));
        Assert.True(DatabaseNamePattern.MatchesAnyTrimmed([" archive_* "], "archive_9"));
    }

    [Fact]
    public void AMixedListBindsNamesAsNamesAndPatternsAsEscapedOperands()
    {
        var (_, parameters) = DatabaseExclusionFilter.Build(["keep", "100%*", "[x]_*", "a\\b*", "tail"], "d.name");

        var names = parameters.Where(p => p.Name.StartsWith("@excl_db_", StringComparison.Ordinal)).ToList();
        var likes = parameters.Where(p => p.Name.StartsWith("@excl_like_", StringComparison.Ordinal)).ToList();
        Assert.Equal(["keep", "tail"], names.Select(p => (string?)p.Value).ToArray());
        Assert.Equal(["@excl_db_0", "@excl_db_1"], names.Select(p => p.Name).ToArray());
        Assert.Equal(["@excl_like_0", "@excl_like_1", "@excl_like_2"], likes.Select(p => p.Name).ToArray());
        Assert.Equal(["100\\%%", "\\[x]\\_%", "a\\\\b%"], likes.Select(p => (string?)p.Value).ToArray());
        Assert.All(likes, p => Assert.Equal(CollectorParameterType.NVarChar260, p.Type));
    }

    [Fact]
    public void ALongestNameWithEveryCharacterEscapedStillFitsItsParameter()
    {
        var (_, parameters) = DatabaseExclusionFilter.Build([new string('_', 127) + "*"], "d.name");
        var like = Assert.Single(parameters);
        Assert.Equal(255, ((string)like.Value!).Length);
    }

    [Fact]
    public void AnEntryMadeOnlyOfStarsIsIgnoredByTheBuilders()
    {
        /* A list that held a bare * before wildcards existed did nothing; it must not start dropping every database. */
        Assert.Equal(string.Empty, DatabaseExclusionFilter.Build(["*"], "d.name").Clause);
        Assert.Empty(DatabaseExclusionFilter.Build(["*", "**"], "d.name").Parameters);
        Assert.Equal(string.Empty, DatabaseExclusionFilter.BuildLiteralClause(["*"], "d.name"));
        Assert.Equal(string.Empty, DatabaseExclusionFilter.BuildLiteralClause(["*"], "d.name", forNestedDynamicSql: true));

        var (clause, parameters) = DatabaseExclusionFilter.Build(["*", "a"], "d.name");
        var only = Assert.Single(parameters);
        Assert.Equal("a", only.Value);
        Assert.NotEqual(string.Empty, clause);
        Assert.Equal(DatabaseExclusionFilter.BuildLiteralClause(["a"], "d.name"), DatabaseExclusionFilter.BuildLiteralClause(["*", "a"], "d.name"));
        Assert.Equal(DatabaseExclusionFilter.Build(["a"], "d.name").Clause, clause);
    }

    [Fact]
    public void AnEntryMadeOnlyOfStarsCoversNoNameInTheAlertFilters()
    {
        Assert.False(DatabaseNamePattern.MatchesAny(["*"], "Sales"));
        Assert.False(DatabaseNamePattern.MatchesAnyTrimmed([" * "], "Sales"));
        Assert.True(DatabaseNamePattern.MatchesAny(["*", "sales"], "Sales"));
        Assert.False(DarlingMcpPlanForceTools.IsExcludedDatabase(["*"], "Sales"));

        /* The Long-Running Query read's database arm builds no term for it, so no row is muted by LIKE '%'. */
        var predicates = LongRunningQueryExclusions.From([], []).BuildSqlPredicates("NULL", "NULL", "t.n", ["*"], 1);
        Assert.Equal("FALSE", predicates.DatabasePredicate);
        Assert.Empty(predicates.Operands);
    }

    [Fact]
    public void TheSharedMatchSitesSkipAPatternedNameAndKeepTheOthers()
    {
        string[] star = ["archive_*"];

        Assert.False(ColumnSelectivity.Allowed([], star, "archive_9"));
        Assert.True(ColumnSelectivity.Allowed([], star, "sales"));
        Assert.True(ColumnSelectivity.Allowed([], ["*"], "sales"));

        SchemaDatabaseProbe Probe(string name, int id) =>
            new(name, id, "ONLINE", false, false, null, true, null, null, null, null);
        var probes = new[] { Probe("archive_1", 6), Probe("sales", 7) };
        Assert.Equal(["sales"], DarlingSchemaCapture.InScope(probes, [], star));
        Assert.Equal(["archive_1", "sales"], DarlingSchemaCapture.InScope(probes, [], ["*"]));

        /* The long-query trace: another registration of the server keeps the session in every database it does not exclude. */
        var owner = new LongQueryTraceRegistration("other", "host", null, true, true, star, null);
        Assert.Equal(["sales"], LongQueryTraceDatabases.KeptElsewhere("self", "host", ["archive_1", "sales"], [owner], []));
        var matchAll = new LongQueryTraceRegistration("other", "host", null, true, true, ["*"], null);
        Assert.Equal(["archive_1", "sales"], LongQueryTraceDatabases.KeptElsewhere("self", "host", ["archive_1", "sales"], [matchAll], []));
    }

    [Theory]
    [InlineData("*")]
    [InlineData("**")]
    public void AnEntryOfOnlyStarsIsRefused(string entry)
    {
        Assert.NotNull(ExcludedDatabaseNames.Refusal([entry], "excluded_databases"));
        Assert.NotNull(MonitoredServerExclusions.Refusal([entry]));
        Assert.Equal(["keep"], ExcludedDatabaseNames.Normalize([entry, "keep"]));
    }

    [Theory]
    [InlineData("a*")]
    [InlineData("*_old")]
    [InlineData("a*b*c")]
    public void AnEntryWithOtherCharactersAndAStarIsAccepted(string entry)
    {
        Assert.Null(ExcludedDatabaseNames.Refusal([entry], "excluded_databases"));
        Assert.Null(MonitoredServerExclusions.Refusal([entry]));
        Assert.Equal([entry], ExcludedDatabaseNames.Normalize([entry]));
    }

    [Fact]
    public void TheExtendedEventsDatabaseListKeepsAStarAsACharacter() =>
        Assert.Null(MonitoredServerXeSessionDatabases.Refusal(["*"]));

    [Fact]
    public void TheCapOfEntriesStays() =>
        Assert.NotNull(ExcludedDatabaseNames.Refusal(Enumerable.Range(0, ExcludedDatabaseNames.MaxEntries + 1).Select(i => "d" + i + "*"), "excluded_databases"));

    private const string Graph = "<deadlock><process-list><process id=\"p1\" currentdbname=\"archive_2024\"/><process id=\"p2\" currentdbname=\"archive_2025\"/></process-list></deadlock>";

    [Fact]
    public void TheAlertFiltersHonourAPatternTheWayTheCollectorsDo()
    {
        Assert.True(AlertContextBuilders.IsDeadlockExcluded(new DeadlockAlertRow { DeadlockGraphXml = Graph }, ["archive_*"]));
        Assert.False(AlertContextBuilders.IsDeadlockExcluded(new DeadlockAlertRow { DeadlockGraphXml = Graph }, ["archive_2024"]));
        Assert.True(AlertContextBuilders.IsDeadlockExcluded(new DeadlockAlertRow { DeadlockGraphXml = Graph }, ["archive_2024", "*2025"]));

        var rows = new[]
        {
            new BlockedProcessAlertRow { DatabaseName = "archive_1", WaitTimeMs = 100000, BlockedSpid = 5, BlockingSpid = 6, EventTime = DateTime.UtcNow },
            new BlockedProcessAlertRow { DatabaseName = "Sales", WaitTimeMs = 100000, BlockedSpid = 7, BlockingSpid = 8, EventTime = DateTime.UtcNow },
        };
        var kept = AlertContextBuilders.BlockingWaitRows(rows, ["archive_*"]);
        Assert.DoesNotContain(kept, r => r.DatabaseName == "archive_1");
        Assert.Contains(kept, r => r.DatabaseName == "Sales");

        Assert.True(DarlingMcpPlanForceTools.IsExcludedDatabase(["archive_*"], "Archive_77"));
        Assert.False(DarlingMcpPlanForceTools.IsExcludedDatabase(["archive_*"], "Sales"));
    }

    [Fact]
    public void TheLongRunningQueryOperandKeepsExactEntriesExactAndTurnsStarsIntoPercent()
    {
        Assert.Equal("a\\_b", LongRunningQueryExclusions.ToDatabaseLikeOperand("a_b"));
        Assert.Equal("archive\\_%", LongRunningQueryExclusions.ToDatabaseLikeOperand("archive_*"));
        Assert.Equal("[x]%", LongRunningQueryExclusions.ToDatabaseLikeOperand("[x]*"));
    }
}

/// <summary>
/// #9050 on real engines. The shipped predicate runs over a VALUES list of awkward names and the survivors must be
/// exactly the names the C# match leaves alone. PostgreSQL runs with <c>DARLING_TEST_PG</c>, SQL Server with
/// <c>DARLING_TEST_SQL</c> (and optional <c>DARLING_TEST_SQL_USER</c> / <c>DARLING_TEST_SQL_PASSWORD</c>). All names and entries
/// are lower case, so the engine's collation and the C# case folding agree.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseExclusionPatternLiveTests
{
    private static readonly string[] Names =
    {
        "archive_2024_01", "archived", "xarchive_1", "archive_", "a_b", "axb", "100%x", "100x", "100%", "[x]y", "xy", "x",
        "a\\b", "a\\x", "it's", "it_s", "sales", "sales_old", "sales_older", "xaybz", "xzy", "a?c1", "abc1",
    };

    private static readonly string[][] Lists =
    {
        new[] { "archive_*" },
        new[] { "a_b", "sales" },
        new[] { "100%*", "[x]*" },
        new[] { "a\\*", "it's*" },
        new[] { "*_old", "x*y*z", "a?c*" },
        new[] { "sales", "archive_*", "100%*", "a_b" },
        new[] { "*" },
        new[] { "*", "a_b" },
        new[] { "a_b", "100%", "[x]y", "it's", "a\b" },
    };

    private static string[] Expected(string[] list) =>
        Names.Where(n => !DatabaseNamePattern.MatchesAny(list, n)).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task PostgresKeepsExactlyTheNamesTheMatchLeavesAlone()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live pattern test.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var values = string.Join(", ", Names.Select(n => "('" + n.Replace("'", "''") + "')"));
        foreach (var list in Lists)
        {
            var (clause, parameters) = DatabaseExclusionFilter.Build(list, "t.n");
            await using var command = new NpgsqlCommand($"SELECT t.n FROM (VALUES {values}) AS t(n) WHERE true {clause} ORDER BY t.n COLLATE \"C\"", connection);
            foreach (var p in parameters)
            {
                command.Parameters.Add(new NpgsqlParameter(p.Name, NpgsqlDbType.Text) { Value = p.Value });
            }

            Assert.Equal(Expected(list), await ReadAsync(command, ct));

            /* The Long-Running Query read's database arm: ILIKE with positional operands. */
            var predicates = LongRunningQueryExclusions.From([], []).BuildSqlPredicates("NULL", "NULL", "t.n", list, 1);
            await using var lrq = new NpgsqlCommand($"SELECT t.n FROM (VALUES {values}) AS t(n) WHERE NOT {predicates.DatabasePredicate} ORDER BY t.n COLLATE \"C\"", connection);
            foreach (var operand in predicates.Operands)
            {
                lrq.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = operand });
            }

            Assert.Equal(Expected(list), await ReadAsync(lrq, ct));
        }
    }

    [Fact]
    public async Task SqlServerKeepsExactlyTheNamesTheMatchLeavesAlone()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL to run the live pattern test.");
        var ct = TestContext.Current.CancellationToken;
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };
        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);

        var values = string.Join(", ", Names.Select(n => "(N'" + n.Replace("'", "''") + "')"));
        string Base(string clause) => $"SELECT t.n FROM (VALUES {values}) AS t(n) WHERE 1 = 1 {clause}";
        foreach (var list in Lists)
        {
            var (clause, parameters) = DatabaseExclusionFilter.Build(list, "t.n");
            await using var command = new SqlCommand(Base(clause), connection);
            foreach (var p in parameters)
            {
                command.Parameters.Add(new SqlParameter(p.Name, System.Data.SqlDbType.NVarChar, p.Type == CollectorParameterType.NVarChar260 ? 260 : 128) { Value = p.Value });
            }

            Assert.Equal(Expected(list), await ReadSqlAsync(command, ct));

            var literal = DatabaseExclusionFilter.BuildLiteralClause(list, "t.n");
            await using var literalCommand = new SqlCommand(Base(literal), connection);
            Assert.Equal(Expected(list), await ReadSqlAsync(literalCommand, ct));

            /* The nested form is a string inside the string handed to sp_executesql. */
            var nested = DatabaseExclusionFilter.BuildLiteralClause(list, "t.n", forNestedDynamicSql: true);
            var inner = Base("").Replace("'", "''", StringComparison.Ordinal);
            await using var nestedCommand = new SqlCommand($"DECLARE @sql nvarchar(max) = N'{inner} {nested}'; EXEC sys.sp_executesql @sql;", connection);
            Assert.Equal(Expected(list), await ReadSqlAsync(nestedCommand, ct));
        }
    }

    /* #9050: the default-trace read's NULL-safe arm. Server-level events carry a NULL database name and must survive any
       list, and the clause must stay one unit beside the query's other conditions (the 'hidden' row is cut by one of them). */
    private static readonly string?[] TraceNames = { null, "archive_1", "archive_2", "sales", "keep", "sales_old", "x_old", "hidden" };

    private static readonly string[][] TraceLists =
    {
        new[] { "sales" },
        new[] { "sales", "archive_*" },
        new[] { "archive_*", "*_old" },
        new[] { "sales", "archive_*", "*_old" },
        new[] { "*", "sales" },
    };

    private static string[] TraceExpected(string[] list) =>
        TraceNames.Where(n => n is null || (n != "hidden" && !DatabaseNamePattern.MatchesAny(list, n))).Select(n => n ?? "<null>").OrderBy(n => n, StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task PostgresDefaultTraceArmKeepsNullDatabaseRowsAndDropsTheExcludedOnes()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live pattern test.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var values = string.Join(", ", TraceNames.Select(n => n is null ? "(NULL::text)" : "('" + n + "')"));
        foreach (var list in TraceLists)
        {
            var (clause, parameters) = DefaultTraceEventsCollector.BuildNullSafeDatabaseExclusion(list);
            await using var command = new NpgsqlCommand($"SELECT coalesce(ft.DatabaseName, '<null>') FROM (VALUES {values}) AS ft(DatabaseName) WHERE coalesce(ft.DatabaseName, '') <> 'hidden' {clause}", connection);
            foreach (var p in parameters)
            {
                command.Parameters.Add(new NpgsqlParameter(p.Name, NpgsqlDbType.Text) { Value = p.Value });
            }

            Assert.Equal(TraceExpected(list), await ReadAsync(command, ct));
        }
    }

    [Fact]
    public async Task SqlServerDefaultTraceArmKeepsNullDatabaseRowsAndDropsTheExcludedOnes()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL to run the live pattern test.");
        var ct = TestContext.Current.CancellationToken;
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };
        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);

        var values = string.Join(", ", TraceNames.Select(n => n is null ? "(CAST(NULL AS nvarchar(128)))" : "(N'" + n + "')"));
        foreach (var list in TraceLists)
        {
            var (clause, parameters) = DefaultTraceEventsCollector.BuildNullSafeDatabaseExclusion(list);
            await using var command = new SqlCommand($"SELECT ISNULL(ft.DatabaseName, N'<null>') FROM (VALUES {values}) AS ft(DatabaseName) WHERE ISNULL(ft.DatabaseName, N'') <> N'hidden' {clause}", connection);
            foreach (var p in parameters)
            {
                command.Parameters.Add(new SqlParameter(p.Name, System.Data.SqlDbType.NVarChar, p.Type == CollectorParameterType.NVarChar260 ? 260 : 128) { Value = p.Value });
            }

            Assert.Equal(TraceExpected(list), await ReadSqlAsync(command, ct));
        }
    }

    private static async Task<string[]> ReadAsync(NpgsqlCommand command, System.Threading.CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows.OrderBy(n => n, StringComparer.Ordinal).ToArray();
    }

    private static async Task<string[]> ReadSqlAsync(SqlCommand command, System.Threading.CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows.OrderBy(n => n, StringComparer.Ordinal).ToArray();
    }
}
