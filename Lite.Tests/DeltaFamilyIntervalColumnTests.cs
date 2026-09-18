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
/// per-second reader LAG-divided it into a confident 0.00. This is the census that keeps a seventh family
/// from shipping naked, and keeps the four that still are named rather than assumed.
/// </summary>
public sealed class DeltaFamilyIntervalColumnTests
{
    private const string IntervalColumn = "sample_interval_seconds";

    /// <summary>
    /// The delta families that persist NO interval today, named so the list can only SHRINK deliberately.
    /// <c>procedure_stats</c> and <c>memory_grant_stats</c> take the calculator's bare long;
    /// <c>pg_wait_stats</c> and <c>pg_statement_stats</c> ask for the interval only to skip idle rows at the
    /// write and store nothing. Each is a follow-up in the #3540 campaign, not a permanent exemption — a
    /// rung that gives one of them the column must remove it from here, and the reverse-direction assertion
    /// below is what makes forgetting to loud.
    /// </summary>
    private static readonly HashSet<string> StillNaked = new(StringComparer.OrdinalIgnoreCase)
    {
        "procedure_stats",
        "memory_grant_stats",
        "pg_wait_stats",
        "pg_statement_stats",
    };

    /// <summary>The families this PR dressed, plus the two that were never naked.</summary>
    private static readonly string[] Dressed =
    {
        "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats", "perfmon_stats", "query_stats",
    };

    /// <summary>
    /// Every member of <see cref="CollectorDeltaCalculator.DeltaFamilyCollectors"/> either carries the
    /// interval column or is on the named still-naked list — and nothing on that list carries it. Both
    /// directions, so a new delta family cannot ship without the column by omission, and a family that gains
    /// the column cannot keep claiming it has not.
    /// </summary>
    [Fact]
    public void EveryDeltaFamily_PersistsItsInterval_OrIsNamedAsStillNaked()
    {
        var families = CollectorDeltaCalculator.DeltaFamilyCollectors.OrderBy(f => f, StringComparer.Ordinal).ToList();
        Assert.NotEmpty(families);

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

    /// <summary>
    /// On the four families this PR dressed, the column is the LAST payload column. Both stores' writers are
    /// positional — the DuckDB appender writes one value per declared column in order, the PostgreSQL COPY
    /// writer likewise — and an existing database receives the column by <c>ALTER TABLE ADD COLUMN</c>, which
    /// can only ever land at the end. A column declared anywhere else would shift every later ordinal on an
    /// upgraded store and write deltas into the wrong columns. (perfmon_stats and query_stats are not held to
    /// the tail: they were extracted with the column already in place and query_stats has since appended
    /// others behind it.)
    /// </summary>
    [Fact]
    public void OnTheFourNewlyDressedFamilies_TheIntervalIsTheTrailingColumn()
    {
        foreach (var family in new[] { "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats" })
        {
            var columns = CollectorCatalog.Find(family)!.PayloadColumns;
            Assert.Equal(IntervalColumn, columns[^1].Name);
        }
    }

    /// <summary>
    /// The DuckDB generator carries the column into a fresh store's DDL for each of the four, as the trailing
    /// column and nullable — the same shape the v60 <c>ALTER TABLE ... ADD COLUMN IF NOT EXISTS
    /// sample_interval_seconds INTEGER</c> gives an upgraded store, so fresh and upgraded databases agree.
    /// </summary>
    [Fact]
    public void TheDuckDbGenerator_EmitsTheIntervalAsTheTrailingNullableColumn_OnTheFour()
    {
        foreach (var family in new[] { "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats" })
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
