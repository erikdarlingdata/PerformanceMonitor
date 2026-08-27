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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every shipped PostgreSQL read must survive PARSE ANALYSIS against a real PostgreSQL (#2554).
///
/// <para><b>The category, not the instance.</b> <c>get_pg_top_queries</c> shipped a query that could not
/// parse: #2219's <c>LEFT JOIN collect.pg_statement_text AS t</c> put <c>t.queryid</c> in scope beside
/// <c>differenced.queryid</c> and made the unqualified references ambiguous (42702). It threw on every call,
/// on every engine, for months. Roughly a dozen tests assert things about that query's TEXT and every one of
/// them passed throughout, because a substring assertion cannot resolve a name — only a server can. Fixing
/// the qualifier without adding this would leave the next <c>LEFT JOIN</c> free to do it again.</para>
///
/// <para><b>PREPARE is the right instrument, and it is cheap.</b> It runs the full front end — name
/// resolution, ambiguity detection, type inference — and stops before execution, so it needs no fixture, no
/// seeded rows and no engine-specific data. The defect it catches fires identically against an empty store
/// and a full one, which is exactly why zero rows was never a defence.</para>
///
/// <para><b>Derived, never enumerated.</b> The reads are discovered by reflection over the
/// <c>DarlingPg*Reader</c> types rather than listed here, so a new read is covered the day it lands instead
/// of the day someone remembers to add it. That makes the discovery itself load-bearing, so the count is
/// asserted too: a filter that quietly matched nothing would otherwise turn this into a test that passes by
/// finding no work to do — the precise failure mode of the text pins it replaces.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgReadSqlParsesLiveTests
{
    /// <summary>
    /// A floor, not a pin. It exists to catch a broken reflection filter, so it must not need editing every
    /// time a read is added — twelve constants exist today across nine readers.
    /// </summary>
    private const int MinimumExpectedReads = 10;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Every <c>const string</c> on a <c>DarlingPg*Reader</c> that is actually a query. Constants are used
    /// rather than the reader methods because a method would need a connection, a server and a window; the
    /// constant IS the shipped text, which is the thing under test.
    /// </summary>
    /// <para>The namespace comes from a reader TYPE rather than a string literal (#2530), for the reason
    /// the discovery is reflective in the first place: the readers moved to
    /// <c>PerformanceMonitor.Darling.Storage</c> so the WPF viewer could run the same query text, and a
    /// hardcoded namespace matched nothing afterwards. The anti-vacuity floor below caught it — a guard that
    /// stopped guarding, reported as one — but a filter anchored to a type it must find anyway cannot break
    /// that way twice.</para>
    private static IReadOnlyList<(string Name, string Sql)> ShippedReadSql() =>
        typeof(DarlingPgStatementReader).Assembly.GetTypes()
            .Where(t => t.Namespace == typeof(DarlingPgStatementReader).Namespace
                        && t.Name.StartsWith("DarlingPg", StringComparison.Ordinal)
                        && t.Name.EndsWith("Reader", StringComparison.Ordinal))
            .SelectMany(t => t
                .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
                .Where(f => f.IsLiteral && f.FieldType == typeof(string))
                .Select(f => (Name: t.Name + "." + f.Name, Sql: (string?)f.GetRawConstantValue() ?? string.Empty)))
            .Where(p => p.Sql.Contains("SELECT", StringComparison.OrdinalIgnoreCase))
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToList();

    [Fact]
    public async Task EveryShippedPostgreSqlReadPassesParseAnalysis()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to parse-check the shipped PostgreSQL reads.");

        var reads = ShippedReadSql();

        /* The anti-vacuity assertion. Without it, a reflection filter that stopped matching would report
           success over an empty list — a guard that has silently stopped guarding. */
        Assert.True(
            reads.Count >= MinimumExpectedReads,
            $"only {reads.Count} shipped PostgreSQL reads were discovered; the reflection filter has stopped "
            + "matching, so this test is no longer checking anything");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var failures = new List<string>();
        var index = 0;

        foreach (var (name, sql) in reads)
        {
            /* A distinct name per read: PREPARE is session-scoped, and reusing one name would make the
               second read fail as "already exists" rather than on its own merits. */
            var statement = "pg_parse_probe_" + index++;
            try
            {
                await using (var prepare = new NpgsqlCommand($"PREPARE {statement} AS {sql}", connection))
                {
                    await prepare.ExecuteNonQueryAsync(ct);
                }

                await using var deallocate = new NpgsqlCommand($"DEALLOCATE {statement}", connection);
                await deallocate.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex)
            {
                failures.Add($"{name}: {ex.SqlState} {ex.MessageText}");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} of {reads.Count} shipped PostgreSQL reads do not parse:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }
}
