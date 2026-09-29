/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The log tail's pick of the current file when two files carry the same modification instant (#4719).
///
/// <para><c>pg_ls_logdir()</c>'s <c>modification</c> is a stat mtime in whole seconds. A rotation by size or by
/// age puts the outgoing file's last write and the incoming file's first write in the same second, so the two
/// files tie, and <c>ORDER BY modification DESC LIMIT 1</c> alone may return the OUTGOING one. The tail then
/// treats the old file as current, reads nothing new, and stays there until the new file's mtime moves to a
/// later second. Measured in CI: the resume marker named the older file
/// (<c>postgresql-..._060231.log</c>) while the awaited line was in the newer one, and 54 attempts over 30 s
/// found 0 rows. All six <c>newest</c> CTEs — the stderr, csvlog and jsonlog routes, each with a text-read
/// and a binary-read constant — now order by <c>modification DESC, name DESC</c>.</para>
///
/// <para>This class is the text pin: it reads each constant's <c>newest</c> CTE and asserts its order. The
/// behaviour pin is <see cref="PgServerLogTailNewestTiebreakLivePostgresTests"/>.</para>
/// </summary>
public sealed class PgServerLogTailNewestTiebreakTests
{
    /// <summary>
    /// The <c>newest</c> CTE as every variant spells it, with the order captured. Whitespace-tolerant so a
    /// re-indented CTE still matches; the order clause itself must match exactly.
    /// </summary>
    internal static readonly Regex NewestCte = new(
        @"newest AS \(\s*SELECT name, size, modification\s+FROM listing\s+ORDER BY (?<order>[^\n]+?)\s+LIMIT 1\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    internal static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The six variants by constant name, with the suffix of the files each one lists.</summary>
    internal static (string Sql, string Suffix) Variant(string constantName) => constantName switch
    {
        nameof(PgServerLogTail.TailCteSql) => (PgServerLogTail.TailCteSql, "log"),
        nameof(PgServerLogTail.TailCteBinarySql) => (PgServerLogTail.TailCteBinarySql, "log"),
        nameof(PgServerLogTail.TailCsvCteSql) => (PgServerLogTail.TailCsvCteSql, "csv"),
        nameof(PgServerLogTail.TailCsvCteBinarySql) => (PgServerLogTail.TailCsvCteBinarySql, "csv"),
        nameof(PgServerLogTail.TailJsonCteSql) => (PgServerLogTail.TailJsonCteSql, "json"),
        nameof(PgServerLogTail.TailJsonCteBinarySql) => (PgServerLogTail.TailJsonCteBinarySql, "json"),
        _ => throw new ArgumentOutOfRangeException(nameof(constantName)),
    };

    [Theory]
    [InlineData(nameof(PgServerLogTail.TailCteSql))]
    [InlineData(nameof(PgServerLogTail.TailCteBinarySql))]
    [InlineData(nameof(PgServerLogTail.TailCsvCteSql))]
    [InlineData(nameof(PgServerLogTail.TailCsvCteBinarySql))]
    [InlineData(nameof(PgServerLogTail.TailJsonCteSql))]
    [InlineData(nameof(PgServerLogTail.TailJsonCteBinarySql))]
    public void EveryNewestCte_OrdersByModification_ThenByNameDescending(string constantName)
    {
        var sql = Lf(Variant(constantName).Sql);
        var matches = NewestCte.Matches(sql);

        var newest = Assert.Single(matches);
        Assert.Equal("modification DESC, name DESC", newest.Groups["order"].Value);
    }
}

/// <summary>
/// The behaviour pin for #4719: each variant's own <c>listing</c> and <c>newest</c> CTEs run over a two-row
/// listing standing in for <c>pg_ls_logdir()</c>, in both row orders, so a sort that keeps either tied row
/// fails one of the two orders. A real rotation cannot be made to tie on demand (it depends on the second
/// boundary), so the listing is the deterministic form of the same tie.
///
/// <para><b>The pin needs no special target.</b> The two <c>current_setting</c> gates in <c>listing</c>
/// (<c>logging_collector</c> and <c>log_destination</c>) are server-start settings the test cannot change, so
/// the swapped SQL is given the values of a target that passes them. It reads no file and no relation: the
/// shared <c>DARLING_TEST_PG</c> server is enough. It joins the shared collection only because every class
/// that connects to that server must.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgServerLogTailNewestTiebreakLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData(nameof(PgServerLogTail.TailCteSql))]
    [InlineData(nameof(PgServerLogTail.TailCteBinarySql))]
    [InlineData(nameof(PgServerLogTail.TailCsvCteSql))]
    [InlineData(nameof(PgServerLogTail.TailCsvCteBinarySql))]
    [InlineData(nameof(PgServerLogTail.TailJsonCteSql))]
    [InlineData(nameof(PgServerLogTail.TailJsonCteBinarySql))]
    public async Task Newest_TakesTheLaterName_WhenTwoFilesTieOnModification_InEitherRowOrder(string constantName)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the log-tail newest-file tie pin.");

        var ct = TestContext.Current.CancellationToken;
        var (tailSql, suffix) = PgServerLogTailNewestTiebreakTests.Variant(constantName);
        var older = $"postgresql-2026-09-29_060231.{suffix}";
        var later = $"postgresql-2026-09-29_060232.{suffix}";
        const string sameSecond = "2026-09-29 06:02:32+00";
        const string nextSecond = "2026-09-29 06:02:33+00";

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        /* The tie, in both row orders: the outgoing file's last write and the incoming file's first write in
           the same second. The later name is the newer file by the default log_filename pattern. */
        Assert.Equal(later, await NewestOfAsync(connection, tailSql, (older, sameSecond), (later, sameSecond), ct));
        Assert.Equal(later, await NewestOfAsync(connection, tailSql, (later, sameSecond), (older, sameSecond), ct));

        /* The name is only the tiebreak: a strictly newer modification wins over a later name, in both row
           orders. This is the half that fails if the name ever becomes the primary order. */
        Assert.Equal(older, await NewestOfAsync(connection, tailSql, (older, nextSecond), (later, sameSecond), ct));
        Assert.Equal(older, await NewestOfAsync(connection, tailSql, (later, sameSecond), (older, nextSecond), ct));
    }

    /// <summary>
    /// Runs the variant's own <c>listing</c> and <c>newest</c> CTEs over a two-row listing in place of
    /// <c>pg_ls_logdir()</c> and returns the name <c>newest</c> picked.
    /// </summary>
    private static async Task<string?> NewestOfAsync(
        NpgsqlConnection connection,
        string tailSql,
        (string Name, string Modification) first,
        (string Name, string Modification) second,
        CancellationToken ct)
    {
        var lf = PgServerLogTailNewestTiebreakTests.Lf(tailSql);
        var newest = PgServerLogTailNewestTiebreakTests.NewestCte.Match(lf);
        Assert.True(newest.Success, "The variant has no recognisable newest CTE to run.");

        var listingStart = lf.IndexOf("listing AS MATERIALIZED (", StringComparison.Ordinal);
        Assert.True(listingStart >= 0 && listingStart < newest.Index, "The variant's listing CTE should come before newest.");
        var slice = lf.Substring(listingStart, newest.Index + newest.Length - listingStart);

        static string Row((string Name, string Modification) file) =>
            $"('{file.Name}'::text, 1024::bigint, '{file.Modification}'::timestamptz)";

        slice = Swap(slice, "FROM pg_catalog.pg_ls_logdir()",
            $"FROM (VALUES {Row(first)}, {Row(second)}) AS logdir(name, size, modification)");
        slice = Swap(slice, "pg_catalog.current_setting('logging_collector')", "'on'::text");
        slice = Swap(slice, "pg_catalog.current_setting('log_destination')", "'stderr,csvlog,jsonlog'::text");
        Assert.DoesNotContain("pg_ls_logdir", slice, StringComparison.Ordinal);
        Assert.DoesNotContain("current_setting", slice, StringComparison.Ordinal);

        await using var command = new NpgsqlCommand("WITH " + slice + "\nSELECT name FROM newest", connection);
        return (string?)await command.ExecuteScalarAsync(ct);
    }

    /// <summary>Replaces exactly one occurrence, so a reworded CTE fails here instead of testing nothing.</summary>
    private static string Swap(string text, string from, string to)
    {
        Assert.Equal(1, Regex.Count(text, Regex.Escape(from)));
        return text.Replace(from, to, StringComparison.Ordinal);
    }
}
