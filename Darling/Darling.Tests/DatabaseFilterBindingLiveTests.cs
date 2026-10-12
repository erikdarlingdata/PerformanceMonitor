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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class reads no relation of the shared database. It only connects to run a SELECT over
   its own unnest() rows, so it cannot race the live classes that seed and clean the shared store. */

/// <summary>
/// #5245: the six awkward database names the web filter has to carry bind through <see cref="DatabaseFilter.Parameter"/>
/// as ONE <c>text[]</c>, and <c>= ANY</c> matches each of them exactly, and nothing else. The names are the ones a
/// naive splice, a comma split or a trim would get wrong: a comma, a closing bracket, a leading space, a quote, a
/// percent sign with a plus, and a markup tag. Each is surrounded by near-miss decoys (the comma's halves, the
/// trimmed form, a different case) that a lossy binding would also match. Skipped without <c>DARLING_TEST_PG</c>.
/// </summary>
public sealed class DatabaseFilterBindingLiveTests
{
    /// <summary>The six awkward names, kept exactly as a user could type them.</summary>
    private static readonly string[] AwkwardNames =
    [
        "A,B",
        "x]",
        " SalesDb",
        "O'Brien",
        "50%+off",
        "<img src=x onerror=alert(1)>",
    ];

    /// <summary>Names that are close to an awkward one and must never match it.</summary>
    private static readonly string[] Decoys =
    [
        "A", "B", "a,b", "A, B", "x", "x[", "SalesDb", "salesdb", "  SalesDb", "OBrien", "O''Brien", "o'brien",
        "50", "50%", "50 off", "<img src=x>", "Other",
    ];

    /// <summary>One row per name, awkward and decoy alike, read through the filter's own clause.</summary>
    private const string Sql = "SELECT t.v FROM (SELECT unnest($2::text[]) AS v) AS t WHERE TRUE";

    [Fact]
    public async Task EachAwkwardName_BindsInOneTextArray_AndMatchesItselfExactly_AndNothingElse()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5245 binding test.");

        var ct = TestContext.Current.CancellationToken;
        await using var source = NpgsqlDataSource.Create(connectionString!);
        var universe = AwkwardNames.Concat(Decoys).ToArray();

        /* All six together: ONE text[] parameter, the six names back, no decoy. */
        var all = DatabaseFilter.Of(AwkwardNames);
        Assert.Equal(AwkwardNames, all.Names);
        var parameter = all.Parameter();
        Assert.Equal(NpgsqlDbType.Array | NpgsqlDbType.Text, parameter.NpgsqlDbType);
        Assert.Equal(AwkwardNames, Assert.IsType<string[]>(parameter.Value));
        Assert.Equal(AwkwardNames.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
            (await MatchAsync(source, universe, all, ct)).OrderBy(n => n, StringComparer.Ordinal).ToArray());

        /* Each name alone matches itself and only itself. */
        foreach (var name in AwkwardNames)
        {
            var one = DatabaseFilter.One(name);
            Assert.Equal(new[] { name }, one.Names);
            Assert.Equal(new[] { name }, await MatchAsync(source, universe, one, ct));
        }

        /* A selection of only decoys never matches an awkward name, and the empty selection (SQL NULL) is every row. */
        Assert.Equal(new[] { "A" }, await MatchAsync(source, universe, DatabaseFilter.One("A"), ct));
        Assert.Equal(universe.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
            (await MatchAsync(source, universe, DatabaseFilter.All, ct)).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// Runs <c>t.v = ANY(filter)</c> over <paramref name="universe"/> with the filter's real clause and parameter. $1 is
    /// the filter; $2 carries the rows to test, bound as its own text[] so the only thing under test is the filter.
    /// </summary>
    private static async Task<string[]> MatchAsync(NpgsqlDataSource source, string[] universe, DatabaseFilter filter, System.Threading.CancellationToken ct)
    {
        await using var command = source.CreateCommand(Sql + filter.Clause("t.v", 1));
        command.Parameters.Add(filter.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = universe });
        Assert.Equal(2, command.Parameters.Count);

        var found = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found.Add(reader.GetString(0));
        }

        return found.ToArray();
    }
}
