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
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3540, the measurement contract's rule 4: <b>every delta family persists the interval its deltas accrued
/// over.</b> The shared calculator reports (delta 0, interval 0) when no delta is knowable — first sighting,
/// counter reset, a gap past the measured 3600 s policy — and (0, n) when an interval was genuinely idle, and
/// the interval is the ONLY thing that tells those apart. Four of the six SQL Server delta families
/// (<c>wait_stats</c>, <c>file_io_stats</c>, <c>latch_stats</c>, <c>spinlock_stats</c>) discarded it at the
/// write until Darling V127 / Lite v60, so a restart's fabricated zero survived as a measured one and every
/// per-second reader LAG-divided it into a confident 0.00; the remaining four (<c>procedure_stats</c>,
/// <c>memory_grant_stats</c>, <c>pg_wait_stats</c>, <c>pg_statement_stats</c>) followed at Darling V128 /
/// Lite v61. This is the census that keeps an eleventh family from shipping naked. Rule 4 is COMPLETE: the
/// still-naked list below is empty, and asserted empty, so the claim "every delta family stores its
/// interval" is a test rather than a sentence.
/// </summary>
public sealed class DeltaFamilyIntervalColumnTests
{
    private const string IntervalColumn = "sample_interval_seconds";

    /// <summary>
    /// The delta families that persist NO interval, named so the list can only SHRINK deliberately. EMPTY
    /// since Darling V128 / Lite v61 (#3540). From V127 / v60 until then it named four: <c>procedure_stats</c>
    /// and <c>memory_grant_stats</c> took the calculator's bare long; <c>pg_wait_stats</c> and
    /// <c>pg_statement_stats</c> asked for the interval only to skip idle rows at the write and stored
    /// nothing. Each was a follow-up in the #3540 campaign, not a permanent exemption, and the rung that
    /// dressed them removed them from here — the reverse-direction assertion below is what made forgetting
    /// to loud. Kept declared, and asserted empty, so a twelfth family that ships naked has to name itself
    /// here to pass and the diff says so.
    /// </summary>
    private static readonly HashSet<string> StillNaked = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Every delta family: the two that were never naked, the four V127 / v60 dressed, and the four
    /// V128 / v61 dressed. Stated as a literal so the PR that adds a family has to say so here too.</summary>
    private static readonly string[] Dressed =
    {
        "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats", "perfmon_stats", "query_stats",
        "procedure_stats", "memory_grant_stats", "pg_wait_stats", "pg_statement_stats",
    };

    /// <summary>
    /// Every member of <see cref="CollectorDeltaCalculator.DeltaFamilyCollectors"/> either carries the
    /// interval column or is on the named still-naked list — and nothing on that list carries it. Both
    /// directions, so a new delta family cannot ship without the column by omission, and a family that gains
    /// the column cannot keep claiming it has not. Since V128 / v61 the list is empty, so the "named as still
    /// naked" branch is the historical record of how the four got here and the census reduces to: every
    /// family carries the column.
    /// </summary>
    [Fact]
    public void EveryDeltaFamily_PersistsItsInterval_OrIsNamedAsStillNaked()
    {
        var families = CollectorDeltaCalculator.DeltaFamilyCollectors.OrderBy(f => f, StringComparer.Ordinal).ToList();
        Assert.NotEmpty(families);
        Assert.Equal(10, families.Count); /* the floor that makes "every family" mean something */

        /* Rule 4 is complete: nothing is exempt. A family added to the list has to be a deliberate diff. */
        Assert.Empty(StillNaked);

        foreach (var family in families)
        {
            var definition = CollectorCatalog.Find(family);
            Assert.True(definition is not null, $"delta family '{family}' has no catalog definition — the census cannot inspect its columns");

            var carriesInterval = definition!.PayloadColumns.Any(c => c.Name == IntervalColumn);

            if (StillNaked.Contains(family))
            {
                Assert.False(carriesInterval,
                    $"'{family}' now persists {IntervalColumn} — remove it from the still-naked list so the census describes the product");
            }
            else
            {
                Assert.True(carriesInterval,
                    $"delta family '{family}' persists deltas with no {IntervalColumn} column, so its (0, 0) unknowable marker cannot survive the write and readers will fabricate 0.00 at restarts (#3540). Add the column or name it in the still-naked list with the follow-up.");
            }
        }

        /* The still-naked list names only real delta families — a renamed or retired collector would
           otherwise sit on it forever, exempting nothing. */
        Assert.All(StillNaked, naked => Assert.Contains(naked, families, StringComparer.OrdinalIgnoreCase));

        /* And the dressed set is exactly the family minus the naked list — stated as a literal so the
           PR that dresses the next family has to say so here too. */
        Assert.Equal(
            families.Where(f => !StillNaked.Contains(f)).OrderBy(f => f, StringComparer.Ordinal),
            Dressed.OrderBy(f => f, StringComparer.Ordinal));
    }

    /// <summary>
    /// The column's shape is identical across every family that carries it: <see cref="CollectorColumnType.Integer"/>
    /// (perfmon_stats' and query_stats' type from the start — DuckDB INTEGER, PostgreSQL integer), so the
    /// <c>NULLIF(sample_interval_seconds, 0)</c> idiom and the <c>IS DISTINCT FROM 0</c> aggregate filter mean
    /// the same thing on every table they touch.
    /// </summary>
    [Fact]
    public void TheIntervalColumn_IsTheSameIntegerOnEveryFamilyThatCarriesIt()
    {
        foreach (var family in Dressed)
        {
            var column = CollectorCatalog.Find(family)!.PayloadColumns.Single(c => c.Name == IntervalColumn);
            Assert.Equal(CollectorColumnType.Integer, column.Type);
        }
    }

    /// <summary>The families the two rungs dressed: V127 / v60's four, then V128 / v61's four. On every one
    /// the column was ADDED by ALTER TABLE, so it must be the tail — see the test below.</summary>
    private static readonly string[] DressedByRung =
    {
        "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats",
        "procedure_stats", "memory_grant_stats", "pg_wait_stats", "pg_statement_stats",
    };

    /// <summary>
    /// On the eight families the two rungs dressed, the column is the LAST payload column. Both stores'
    /// writers are positional — the DuckDB appender writes one value per declared column in order, the
    /// PostgreSQL COPY writer likewise — and an existing database receives the column by
    /// <c>ALTER TABLE ADD COLUMN</c>, which can only ever land at the end. A column declared anywhere else
    /// would shift every later ordinal on an upgraded store and write deltas into the wrong columns.
    /// (perfmon_stats and query_stats are not held to the tail: they were extracted with the column already
    /// in place and query_stats has since appended others behind it — the V128 offsets among them.)
    /// </summary>
    [Fact]
    public void OnTheEightRungDressedFamilies_TheIntervalIsTheTrailingColumn()
    {
        foreach (var family in DressedByRung)
        {
            var columns = CollectorCatalog.Find(family)!.PayloadColumns;
            Assert.Equal(IntervalColumn, columns[^1].Name);
        }
    }

    /// <summary>
    /// The DuckDB generator carries the column into a fresh store's DDL for each family Lite stores, as the
    /// trailing column and nullable — the same shape the v60 / v61 <c>ALTER TABLE ... ADD COLUMN IF NOT
    /// EXISTS sample_interval_seconds INTEGER</c> gives an upgraded store, so fresh and upgraded databases
    /// agree. The PostgreSQL pair is not a DuckDB table (Lite monitors no PostgreSQL), so it is not here;
    /// <c>DeltaFamilyIntervalCompletionRungTests</c> pins its generated PostgreSQL DDL.
    /// </summary>
    [Fact]
    public void TheDuckDbGenerator_EmitsTheIntervalAsTheTrailingNullableColumn_OnTheSixLiteStores()
    {
        var liteTables = DuckDbSchemaGenerator.CollectorTableNames().ToHashSet(StringComparer.Ordinal);
        var lite = DressedByRung.Where(liteTables.Contains).ToList();
        Assert.Equal(6, lite.Count);
        Assert.DoesNotContain("pg_wait_stats", lite);
        Assert.DoesNotContain("pg_statement_stats", lite);

        foreach (var family in lite)
        {
            var ddl = DuckDbSchemaGenerator.CreateTable(CollectorCatalog.Find(family)!);
            var lines = ddl.Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();

            /* The last column line before the closing paren. */
            var closing = lines.FindLastIndex(l => l.StartsWith(")", StringComparison.Ordinal));
            Assert.True(closing > 0, $"{family}: generated DDL has no closing paren:\n{ddl}");
            Assert.Equal($"{IntervalColumn} INTEGER", lines[closing - 1]);
        }
    }

    /// <summary>
    /// The Lite migration that adds the column to an existing database exists, names all four tables, and the
    /// schema version was bumped for it — the twin of Darling's V127, held by the parity rule that state added
    /// to one store must be added to the other or the shared read layer asks Lite a question its store cannot
    /// answer (here: the appender would fail EndRow() on the first batch).
    /// </summary>
    [Fact]
    public void TheLiteMigration_AddsTheColumnToAllFour_AtSchemaVersion60()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 60);

        var source = RepoSource.Read("Lite", "Database", "DuckDbInitializer.cs");
        Assert.Contains("if (fromVersion < 60)", source, StringComparison.Ordinal);
        Assert.Contains(
            "foreach (var table in new[] { \"wait_stats\", \"file_io_stats\", \"latch_stats\", \"spinlock_stats\" })",
            source, StringComparison.Ordinal);
        Assert.Contains(
            "$\"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sample_interval_seconds INTEGER\"",
            source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v61 twin of Darling's V128: the interval on the two remaining Lite-stored families, the two
    /// statement offsets on query_stats, and the version bump. Four (table, column) pairs, every one an
    /// idempotent INTEGER ADD COLUMN, held by the same parity rule as v60 — and the offsets' semantics
    /// (BYTE offsets into the batch's nvarchar text; -1 = end of batch; stored raw) stated in the block so the
    /// next reader finds them where they will look.
    /// </summary>
    [Fact]
    public void TheLiteMigration_CompletesTheIntervalAndStoresTheOffsets_AtSchemaVersion61()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 61);

        var source = RepoSource.Read("Lite", "Database", "DuckDbInitializer.cs");
        var block = source[source.IndexOf("if (fromVersion < 61)", StringComparison.Ordinal)..];

        foreach (var pair in new[]
        {
            "(\"procedure_stats\", \"sample_interval_seconds\")",
            "(\"memory_grant_stats\", \"sample_interval_seconds\")",
            "(\"query_stats\", \"statement_start_offset\")",
            "(\"query_stats\", \"statement_end_offset\")",
        })
        {
            Assert.Contains(pair, block, StringComparison.Ordinal);
        }

        Assert.Contains("$\"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} INTEGER\"", block, StringComparison.Ordinal);

        /* The offsets' semantics, in the block, in these words: the pair a future reader second-guesses. */
        Assert.Contains("BYTES", block, StringComparison.Ordinal);
        Assert.Contains("statement_end_offset = -1 means \"to the end of the batch\"", block, StringComparison.Ordinal);
        Assert.Contains("VERBATIM", block, StringComparison.Ordinal);
    }

    private static class RepoSource
    {
        public static string Read(params string[] parts)
        {
            var dir = AppContext.BaseDirectory;
            for (int i = 0; i < 8 && dir is not null; i++)
            {
                var candidate = System.IO.Path.Combine(new[] { dir }.Concat(parts).ToArray());
                if (System.IO.File.Exists(candidate))
                {
                    return System.IO.File.ReadAllText(candidate);
                }

                dir = System.IO.Path.GetDirectoryName(dir);
            }

            throw new System.IO.FileNotFoundException($"Could not locate {string.Join('/', parts)} walking up from {AppContext.BaseDirectory}");
        }
    }
}
