/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
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
    /// A ratchet, not a slack floor, and it sits AT the live population rather than below it. Growth never
    /// trips it — a read added only ever raises the count — so it still does not need editing when one
    /// lands; a read REMOVED reds, which is the whole point, because that is the direction a coverage loss
    /// travels in.
    ///
    /// <para><b>What the number replaced.</b> This was 10, against a doc claim of "twelve constants across
    /// nine readers" that had gone badly stale: measured, 28 reader types ship 49 reads. A floor of 10
    /// against 49 tolerated a 39-read drop — 79% of the population could leave silently while the guard
    /// reported a clean pass. That is not a floor catching a broken reflection filter, which is what it was
    /// written to be; it is a floor that could only catch total filter collapse, and #2530's namespace break
    /// is the only shape it ever would have seen.</para>
    /// </summary>
    private const int MinimumExpectedReads = 49;

    /// <summary>
    /// The reader TYPE count, on the same ratchet and for the same reason. Separate from the read count
    /// because the two fail differently: a namespace or name-pattern break takes every type at once, while
    /// reads leaving one at a time keep the type count intact.
    /// </summary>
    private const int MinimumReaderTypes = 28;

    /// <summary>
    /// Static string fields on a reader that are NOT queries, so the source census below can tell a field
    /// that legitimately holds no SQL from a read that has dropped out of reflection's reach. Named
    /// explicitly rather than inferred, because every rule for inferring it reads the field's VALUE — which
    /// is reflection, the very thing the census exists to check independently.
    ///
    /// <para>Two of the three are SQL FRAGMENTS interpolated into a query rather than queries themselves,
    /// and the third is the trap that makes a name-shaped rule useless here:
    /// <c>StaleStatisticsChurnRatioSql</c> is spelled like a query and holds <c>"0.2"</c>. Keyed per SITE
    /// rather than per name, so the same name on a different reader is not excused by inheritance from
    /// this one — and the unused-entry clause below makes a move show up as an edit here rather than as
    /// a silently widened exemption.</para>
    /// </summary>
    private static readonly HashSet<string> NotQueryFields = new(StringComparer.Ordinal)
    {
        "DarlingPgColumnStatsReader.EvidenceStatusList",
        "DarlingPgServerConfigReader.SessionScopedSources",
        "DarlingPgTableBloatReader.StaleStatisticsChurnRatioSql",
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Every reader type the reads are discovered from. Anchored to a reader TYPE rather than a namespace
    /// string (#2530) for the reason the discovery is reflective at all: the readers moved to
    /// <c>PerformanceMonitor.Darling.Storage</c> so the WPF viewer could run the same query text, and a
    /// hardcoded namespace matched nothing afterwards.
    /// </summary>
    private static IReadOnlyList<Type> ReaderTypes() =>
        typeof(DarlingPgStatementReader).Assembly.GetTypes()
            .Where(t => t.Namespace == typeof(DarlingPgStatementReader).Namespace
                        && t.Name.StartsWith("DarlingPg", StringComparison.Ordinal)
                        && t.Name.EndsWith("Reader", StringComparison.Ordinal))
            .OrderBy(t => t.Name, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Every static string field on a <c>DarlingPg*Reader</c> that is actually a query. Fields rather than
    /// the reader methods because a method would need a connection, a server and a window; the field IS the
    /// shipped text, which is the thing under test.
    ///
    /// <para><b>Both field kinds, not just <c>const</c>.</b> This read <c>f.IsLiteral</c> only, and a
    /// <c>static readonly string</c> composed by concatenation is not a literal — so
    /// <c>DarlingPgColumnStatsReader.CoverageEvidenceSql</c>, a shipped read executed at every column-stats
    /// coverage call, had never been parse-checked at all. Composing SQL that way is ordinary here (a status
    /// list and two collector thresholds are interpolated into that one), and it is not a shape a guard can
    /// ask people to avoid to stay covered.</para>
    /// </summary>
    private static IReadOnlyList<(string Name, string Sql)> ShippedReadSql() =>
        ReaderTypes()
            .SelectMany(t => QueryFields(t).Select(f => (Name: t.Name + "." + f.Name, Sql: FieldText(f))))
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToList();

    private static IEnumerable<FieldInfo> QueryFields(Type reader) =>
        reader.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(string) && (f.IsLiteral || f.IsInitOnly))
            .Where(f => FieldText(f).Contains("SELECT", StringComparison.OrdinalIgnoreCase));

    /// <summary><c>GetRawConstantValue</c> for a <c>const</c>, <c>GetValue</c> for a <c>static readonly</c>
    /// — the latter runs the type's initializer, which is exactly what makes the composed text readable.
    /// </summary>
    private static string FieldText(FieldInfo field) =>
        (field.IsLiteral ? (string?)field.GetRawConstantValue() : (string?)field.GetValue(null))
        ?? string.Empty;

    /// <summary>
    /// <para>The population itself, asserted without a server so it runs everywhere the suite runs — the
    /// Windows <c>build</c> job included, where <c>DARLING_TEST_PG</c> is unset and the parse check below
    /// skips. The anti-vacuity assertion used to live inside that skipped test, so on half the CI matrix
    /// nothing checked the discovery at all.</para>
    ///
    /// <para><b>Three clauses, because they fail differently.</b> A total ratchet catches reads deleted. A
    /// per-type clause catches a type whose reads all left while the total still cleared — <c>DarlingPgTrendReader</c>
    /// alone ships 9, so a total floor with any slack cannot see one type emptying. And the SOURCE census is
    /// the only clause with a denominator from outside reflection: it counts what the reader FILES declare,
    /// so a read that leaves reflection's reach while its declaration sits in the file — a type renamed off
    /// the <c>DarlingPg*Reader</c> pattern, a field moved to another namespace, a field kind the filter does
    /// not read — reds instead of quietly shrinking the population.</para>
    /// </summary>
    [Fact]
    public void TheReadPopulationIsWhatTheReaderFilesDeclare()
    {
        var types = ReaderTypes();

        Assert.True(
            types.Count >= MinimumReaderTypes,
            $"only {types.Count} DarlingPg*Reader types were found (ratchet {MinimumReaderTypes}); the "
            + "namespace anchor or the name pattern has stopped matching, so every clause below is vacuous");

        var reads = ShippedReadSql();

        Assert.True(
            reads.Count >= MinimumExpectedReads,
            $"only {reads.Count} shipped PostgreSQL reads were discovered (ratchet {MinimumExpectedReads}); "
            + "reads have left the population, so the parse check below no longer covers what it did");

        var empty = types.Where(t => !QueryFields(t).Any()).Select(t => t.Name).ToList();

        Assert.True(
            empty.Count == 0,
            "these reader types contribute no parse-checked read at all, so nothing verifies their SQL "
            + "resolves: " + string.Join(", ", empty));

        /* The census. Keyed file-stem-to-type-name, which holds because each reader file declares exactly
           the one reader type it is named for — asserted by the type clause above finding the same 28. */
        var discovered = new HashSet<string>(reads.Select(r => r.Name), StringComparer.Ordinal);
        var declared = DeclaredStringFields();
        var missing = declared.Where(d => !discovered.Contains(d) && !NotQueryFields.Contains(d)).ToList();

        Assert.True(
            declared.Count >= MinimumExpectedReads,
            $"only {declared.Count} static string fields were read out of the reader SOURCE (expected at "
            + $"least the {MinimumExpectedReads} reads reflection finds); the file glob or the declaration "
            + "pattern has broken, and a census with no denominator cannot fail");

        Assert.True(
            missing.Count == 0,
            "these string fields are declared in a DarlingPg*Reader source file but are not in the "
            + "parse-checked population, so their SQL is shipped unverified — add them to the population, or "
            + $"to {nameof(NotQueryFields)} if they hold no query: " + string.Join(", ", missing));

        /* A stale allowlist is the same defect one level down: an entry for a field nobody declares any more
           silently excuses whatever later takes that name. */
        var unused = NotQueryFields.Where(n => !declared.Contains(n)).ToList();

        Assert.True(
            unused.Count == 0,
            $"{nameof(NotQueryFields)} excuses fields no reader declares any more: " + string.Join(", ", unused));
    }

    /// <summary>
    /// <c>Type.Field</c> for every <c>const string</c> and <c>static readonly string</c> declared in the
    /// Storage project's <c>DarlingPg*Reader*.cs</c> files, read from the SOURCE. Read through
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> so a declaration spelled in a doc comment or
    /// inside a string cannot enter the census — the same instrument the store-SQL pins scan with.
    /// </summary>
    private static IReadOnlyList<string> DeclaredStringFields([CallerFilePath] string thisFile = "")
    {
        var storage = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Storage"));

        var declaration = new Regex(
            @"(?:const|static\s+readonly)\s+string\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*=",
            RegexOptions.Compiled);

        var fields = new List<string>();

        foreach (var path in Directory.EnumerateFiles(storage, "DarlingPg*Reader*.cs", SearchOption.TopDirectoryOnly))
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

            foreach (Match match in declaration.Matches(code))
            {
                fields.Add(Path.GetFileNameWithoutExtension(path) + "." + match.Groups["name"].Value);
            }
        }

        return fields;
    }

    [Fact]
    public async Task EveryShippedPostgreSqlReadPassesParseAnalysis()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to parse-check the shipped PostgreSQL reads.");

        var reads = ShippedReadSql();

        /* The anti-vacuity assertion. Without it, a reflection filter that stopped matching would report
           success over an empty list — a guard that has silently stopped guarding. Kept here as well as
           in TheReadPopulationIsWhatTheReaderFilesDeclare because that pin is what makes this number
           mean anything, and this test is the one that skips: if it ever runs against a shrunken
           population it should say so itself rather than parse-check a handful of reads and pass. */
        Assert.True(
            reads.Count >= MinimumExpectedReads,
            $"only {reads.Count} shipped PostgreSQL reads were discovered (ratchet {MinimumExpectedReads}); "
            + "the reflection filter has stopped matching, so this test is no longer checking anything");

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
