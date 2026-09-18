/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V127 / #3540: <c>sample_interval_seconds</c> on the four delta families that persisted their deltas NAKED —
/// <c>wait_stats</c>, <c>file_io_stats</c>, <c>latch_stats</c>, <c>spinlock_stats</c>. The measurement-layer
/// keystone.
///
/// <para>The shared delta calculator reports (delta 0, interval 0) when no delta is knowable and (0, n) when an
/// interval was genuinely idle; the interval is the ONLY thing that tells those apart, and these four
/// collectors discarded it at the write. So a restart's fabricated zero survived as a measured one, and every
/// per-second reader LAG-divided it into a confident 0.00 ms/sec at exactly the moments it was unknowable.
/// perfmon_stats and query_stats carried the column from the start; this rung gives the other four the same
/// column in the same type.</para>
///
/// <para>This file carries the "I am the top rung" claims that moved off the previous top rung's test when this
/// rung landed — a fully-migrated store must map to EXACTLY this version, or the viewer's connect-time gate
/// refuses a store that is actually current.</para>
/// </summary>
public sealed class DeltaFamilyIntervalColumnsRungTests
{
    private const int RungVersion = 127;

    private const int PreviousVersion = 126;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 102;

    private const string IntervalColumn = "sample_interval_seconds";

    private static readonly string[] Tables = { "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats" };

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "delta-family-interval-columns",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds ONE nullable, default-less <c>integer</c> column to each of the four tables, schema-qualified,
    /// idempotent, and refreshes each table's <c>SELECT *</c> passthrough view — and does nothing else.
    ///
    /// <para><c>integer</c> is pinned against the type perfmon_stats and query_stats already use through the
    /// generator, not as a literal alone, so the six interval columns cannot drift apart and
    /// <c>NULLIF(sample_interval_seconds, 0)</c> means the same thing on every one. No DEFAULT and no backfill:
    /// a historical row never recorded its interval, so NULL is the honest value and a backfilled 0 would
    /// stamp all of history as "unknowable" and blank every rate chart for 30 days.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsTheNullableIntegerToAllFour_SchemaQualified_AndRefreshesTheirViews()
    {
        /* LF-normalised: the repo checks out CRLF on Windows (.gitattributes eol=crlf) and a verbatim string
           carries the file's line endings, so the multi-line anchors below are written against LF. */
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var perfmonType = PgSchemaGenerator.TypeFor(
            PerfmonStatsCollector.Instance.PayloadColumns.Single(c => c.Name == IntervalColumn));
        Assert.Equal("integer", perfmonType);

        foreach (var table in Tables)
        {
            /* Schema-qualified: the migrate session's search_path puts collect first, so a bare name would
               work today and stop working the day that changes — and CONTRIBUTING makes it the rule. */
            Assert.Equal(1, CountOf(rung, $"ALTER TABLE collect.{table}\n"));
            Assert.DoesNotContain($"ALTER TABLE {table}\n", rung, StringComparison.Ordinal);

            Assert.Contains(
                $"ALTER TABLE collect.{table}\n    ADD COLUMN IF NOT EXISTS {IntervalColumn} {perfmonType};",
                rung, StringComparison.Ordinal);

            /* Postgres freezes a view's SELECT * at CREATE (V14, V80, V81): without this line the passthrough
               every reader uses would never show the column. */
            Assert.Equal(1, CountOf(rung, $"CREATE OR REPLACE VIEW collect.v_{table} AS SELECT * FROM collect.{table};"));
        }

        Assert.Equal(4, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));
        /* Counted on the statement prefix, not the bare phrase — the rung's own SQL comment names
           CREATE OR REPLACE VIEW in prose. */
        Assert.Equal(4, CountOf(rung, "CREATE OR REPLACE VIEW collect.v_"));

        /* Nullable, no default, no backfill, no CHECK, no GRANT, and no touch of the wait_stats_baseline
           continuous aggregate — that change is the documented follow-up (a new aggregate under a new name,
           the #2007 retirement shape), because a CAGG cannot change its query in place and a rebuild forfeits
           baseline history raw can no longer refill. */
        Assert.DoesNotContain("DEFAULT", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("CHECK", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("MATERIALIZED", rung, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_stats_baseline", rung, StringComparison.Ordinal);
    }

    /// <summary>
    /// A FRESH store gets the column from the generated CREATE TABLE (the collector definitions carry it now),
    /// as the trailing column in the same type the rung's ALTER adds — so fresh-through-V127 and
    /// upgraded-to-V127 stores are shaped identically and the rung's ALTERs no-op on the former.
    /// </summary>
    [Fact]
    public void TheGeneratedCreateTable_CarriesTheColumnLast_OnAllFour()
    {
        foreach (var table in Tables)
        {
            var definition = CollectorCatalog.Find(table);
            Assert.NotNull(definition);

            Assert.Equal(IntervalColumn, definition!.PayloadColumns[^1].Name);
            Assert.Equal(CollectorColumnType.Integer, definition.PayloadColumns[^1].Type);

            var ddl = PgSchemaGenerator.CreateTable(definition);
            Assert.EndsWith($"    {IntervalColumn} integer\n);", ddl, StringComparison.Ordinal);
        }
    }

    /* ---- the probe (three sites, top arm) ------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm.
    ///
    /// <para>The probe asks the question, the caller reads the answer, the map has the parameter — three
    /// sites, and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column.
    /// Miss all three and a fully-migrated store probes one rung short, so the connect-time gate refuses a
    /// store that is in fact current — permanently, because no later upgrade changes the answer.</para>
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            $"table_name = 'wait_stats'\n                                                     AND   column_name = '{IntervalColumn}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasDeltaFamilyIntervalColumns", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The top rung's sentinel IS the last argument. */
        Assert.Equal(ProbeOrdinal, arity - 1);

        /* Every sentinel true = a fully-migrated store, which must map to exactly this version. Built by
           reflection so the arity tracks the signature. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* This rung's own arm answers for a store that stopped here. Expressed as "false above" rather than
           as one named ordinal, so a rung landing on top of this one does not quietly turn this case into a
           test of that rung. */
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        /* One rung behind: the same store WITHOUT this rung's sentinel reports the previous rung. */
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* And in the source, the arm sits ABOVE the previous rung's — newest-first is the whole contract of
           that method — and returns this build's version rather than a literal that could drift from it. */
        var thisArm = viewer.IndexOf("if (hasDeltaFamilyIntervalColumns)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasSelfDiskWarnGbFloor)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V127 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V127 arm sits below the previous rung's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..], StringComparison.Ordinal);
    }

    /* ---- the writers ---------------------------------------------------------------------------------- */

    /// <summary>
    /// The four collectors now WRITE the interval — through <c>CalculateDeltaWithInterval</c>, never the bare
    /// <c>CalculateDelta</c> — as the minimum over each row's delta groups. The column without the writer would
    /// be a NULL forever; the writer taking one headline group's interval would let an independently reset
    /// sibling counter's 0 read as idle over a real interval.
    /// </summary>
    [Fact]
    public void TheFourCollectors_WriteTheIntervalAsTheMinimumOverTheirDeltaGroups()
    {
        foreach (var (file, groups) in new[]
        {
            ("WaitStatsCollector.cs", 3),
            ("FileIoStatsCollector.cs", 8),
            ("LatchStatsCollector.cs", 3),
            ("SpinlockStatsCollector.cs", 4),
        })
        {
            var source = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", file);

            Assert.Equal(groups, CountOf(source, "context.Deltas.CalculateDeltaWithInterval("));
            Assert.DoesNotContain("context.Deltas.CalculateDelta(", source, StringComparison.Ordinal);
            Assert.Contains("var sampleIntervalSeconds = Math.Min(", source, StringComparison.Ordinal);
            Assert.Contains(".Value(sampleIntervalSeconds);", source, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The calculator's own doc claim is TRUE again. It said "every consumer already maps 0 to NULL via
    /// NULLIF(sample_interval_seconds, 0)" while four of six families discarded the interval at the write;
    /// the sentence now names which families the claim holds for and which still do not persist one, so it
    /// cannot silently become false again by a seventh family shipping naked.
    /// </summary>
    [Fact]
    public void TheCalculatorDoc_NamesTheFamiliesTheNullifClaimHoldsFor()
    {
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "CollectorDeltaCalculator.cs");

        Assert.DoesNotContain("every consumer already maps 0 to NULL", source, StringComparison.Ordinal);
        Assert.Contains("wait_stats, file_io_stats,", source, StringComparison.Ordinal);
        Assert.Contains("latch_stats and spinlock_stats since Darling V127 / Lite v60, #3540", source, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", source, StringComparison.Ordinal);
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal);
             at >= 0;
             at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
