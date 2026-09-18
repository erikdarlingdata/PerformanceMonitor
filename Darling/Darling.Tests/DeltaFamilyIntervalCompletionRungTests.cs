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
/// V128 / #3540: the completion of V127. <c>sample_interval_seconds</c> on the four delta families V127 left
/// naked — <c>procedure_stats</c>, <c>memory_grant_stats</c>, <c>pg_wait_stats</c>, <c>pg_statement_stats</c>
/// — and the two statement offsets on <c>query_stats</c> that its delta key is made of. After this rung every
/// member of <c>CollectorDeltaCalculator.DeltaFamilyCollectors</c> stores the interval its deltas accrued
/// over, so the calculator's (delta 0, interval 0) "no delta knowable" marker reaches the store from every
/// family, and the restart seed can rebuild the one key the store could not reproduce.
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="DeltaFamilyIntervalColumnsRungTests"/> (V127) when this rung landed, the same handoff that file
/// received from <see cref="SelfDiskWarnGbFloorRungTests"/> (V126) — a fully-migrated store must map to
/// EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually current.</para>
/// </summary>
public sealed class DeltaFamilyIntervalCompletionRungTests
{
    private const int RungVersion = 128;

    private const int PreviousVersion = 127;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 103;

    private const string IntervalColumn = "sample_interval_seconds";

    private static readonly string[] IntervalTables = { "procedure_stats", "memory_grant_stats", "pg_wait_stats", "pg_statement_stats" };

    private static readonly string[] OffsetColumns = { "statement_start_offset", "statement_end_offset" };

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "delta-family-interval-completion",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The hand-written head of the rung — everything before the V54 pre-add the ladder entry concatenates
    /// — adds ONE nullable, default-less <c>integer</c> interval column to each of the four tables and the two
    /// offset columns to <c>query_stats</c>, all schema-qualified and idempotent; refreshes the ONE
    /// <c>SELECT *</c> passthrough among them; drops the payload-resolving <c>v_query_stats</c> for the
    /// regenerated definition to follow; and does nothing else.
    ///
    /// <para><c>integer</c> is pinned against the type perfmon_stats already uses through the generator, not
    /// as a literal alone, so the ten interval columns cannot drift apart and
    /// <c>NULLIF(sample_interval_seconds, 0)</c> means the same thing on every one. No DEFAULT and no backfill
    /// on any of the six: a historical row never recorded its interval or its offsets, a backfilled 0
    /// interval would stamp all of history "unknowable", and a backfilled 0/-1 offset pair would seed
    /// baselines under a key nothing will ever present.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsSixNullableIntegers_SchemaQualified_RefreshesTheGrantView_AndDropsTheResolvingView()
    {
        var full = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The V121 idiom: the hand-written head, then V54's gz pre-add, then every payload column's pre-add,
           then the regenerated resolving view. The head is what this test inspects statement by statement;
           MigrationLadderPins holds the ordering of the rest. */
        var v54 = full.IndexOf("ALTER TABLE query_plan_dim", StringComparison.Ordinal);
        Assert.True(v54 > 0, "the rung does not carry V54's gz pre-add ahead of the regenerated resolving view");
        var head = full[..v54];

        var perfmonType = PgSchemaGenerator.TypeFor(
            PerfmonStatsCollector.Instance.PayloadColumns.Single(c => c.Name == IntervalColumn));
        Assert.Equal("integer", perfmonType);

        foreach (var table in IntervalTables)
        {
            Assert.Equal(1, CountOf(head, $"ALTER TABLE collect.{table}\n"));
            Assert.DoesNotContain($"ALTER TABLE {table}\n", head, StringComparison.Ordinal);
            Assert.Contains(
                $"ALTER TABLE collect.{table}\n    ADD COLUMN IF NOT EXISTS {IntervalColumn} {perfmonType};",
                head, StringComparison.Ordinal);
        }

        foreach (var column in OffsetColumns)
        {
            Assert.Contains(
                $"ALTER TABLE collect.query_stats\n    ADD COLUMN IF NOT EXISTS {column} integer;",
                head, StringComparison.Ordinal);
        }

        Assert.Equal(6, CountOf(head, "ADD COLUMN IF NOT EXISTS"));

        /* The one SELECT * passthrough among the five tables (PgSchemaGenerator.AllPassthroughViews pins the
           set): Postgres freezes a view's column list at CREATE (V14, V80, V81, V127). */
        Assert.Equal(1, CountOf(head, "CREATE OR REPLACE VIEW collect.v_memory_grant_stats AS SELECT * FROM collect.memory_grant_stats;"));
        Assert.Equal(1, CountOf(head, "CREATE OR REPLACE VIEW collect.v_"));
        foreach (var table in new[] { "procedure_stats", "pg_wait_stats", "pg_statement_stats" })
        {
            Assert.DoesNotContain($"v_{table}", head, StringComparison.Ordinal);
            Assert.DoesNotContain("v_" + table, PgSchemaGenerator.AllPassthroughViews);
        }

        /* v_query_stats is the payload-RESOLVING view (#1767) and the offsets land ahead of its digest columns,
           an alteration CREATE OR REPLACE VIEW refuses: DROP here, regenerate in the tail (V51 / V121). */
        Assert.Equal(1, CountOf(head, "DROP VIEW IF EXISTS collect.v_query_stats;"));
        Assert.DoesNotContain("CASCADE", head, StringComparison.Ordinal);
        var tail = full[v54..];
        Assert.Equal(1, CountOf(tail, "CREATE OR REPLACE VIEW v_query_stats AS"));
        Assert.True(
            tail.LastIndexOf("CREATE OR REPLACE VIEW v_query_stats AS", StringComparison.Ordinal)
            > tail.LastIndexOf("ADD COLUMN IF NOT EXISTS", StringComparison.Ordinal),
            "the regenerated resolving view must be the LAST statement, after every pre-add");
        foreach (var column in OffsetColumns)
        {
            Assert.Contains($"f.{column}", tail, StringComparison.Ordinal);
        }

        /* Nullable, no default, no backfill, no CHECK, no GRANT, and no touch of any continuous aggregate —
           the wait_stats_baseline follow-up V127 documented is an aggregate-plus-retirement operation, not a
           column, and not this rung. */
        Assert.DoesNotContain("DEFAULT", head, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", head, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", head, StringComparison.Ordinal);
        Assert.DoesNotContain("CHECK", head, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", head, StringComparison.Ordinal);
        Assert.DoesNotContain("MATERIALIZED", head, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_stats_baseline", head, StringComparison.Ordinal);
        Assert.DoesNotContain("_hourly", head, StringComparison.Ordinal);
    }

    /// <summary>
    /// The offsets' semantics are stated in the rung's own doc, in the words a reader will search for: they
    /// are BYTE offsets into the batch's nvarchar text (so a character slice divides by two), <c>-1</c> as the
    /// end offset means "to the end of the batch", and they are stored VERBATIM — never normalized — because
    /// the delta key is built over the raw values. A future reader will second-guess exactly that pair, and
    /// the rung doc is where they will look.
    /// </summary>
    [Fact]
    public void TheRungDoc_StatesTheOffsetsSemantics_BytesEndOfBatchAndVerbatim()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V128 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V128Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the V128 rung has no XML doc block ahead of V128Sql");
        var doc = source[start..end];

        Assert.Contains("<b>BYTES</b>", doc, StringComparison.Ordinal);
        Assert.Contains("divides by two", doc, StringComparison.Ordinal);
        Assert.Contains("<c>statement_end_offset = -1</c>", doc, StringComparison.Ordinal);
        Assert.Contains("means \"to the end of the batch\"", doc, StringComparison.Ordinal);
        Assert.Contains("<b>verbatim as the DMV reports them</b>", doc, StringComparison.Ordinal);
        Assert.Contains("never normalized", doc, StringComparison.Ordinal);
        /* And the SQL itself repeats the two facts beside the ALTERs. */
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;
        Assert.Contains("BYTE offsets", sql, StringComparison.Ordinal);
        Assert.Contains("statement_end_offset = -1 means", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A FRESH store gets the columns from the generated CREATE TABLE (the collector definitions carry them
    /// now): the interval as the trailing column on all four, in the same type the rung's ALTER adds, and the
    /// two offsets as the trailing pair on query_stats — so fresh-through-V128 and upgraded-to-V128 stores are
    /// shaped identically and the rung's ALTERs no-op on the former.
    /// </summary>
    [Fact]
    public void TheGeneratedCreateTable_CarriesTheColumnsLast_OnAllFive()
    {
        foreach (var table in IntervalTables)
        {
            var definition = CollectorCatalog.Find(table);
            Assert.NotNull(definition);

            Assert.Equal(IntervalColumn, definition!.PayloadColumns[^1].Name);
            Assert.Equal(CollectorColumnType.Integer, definition.PayloadColumns[^1].Type);

            var ddl = PgSchemaGenerator.CreateTable(definition);
            Assert.EndsWith($"    {IntervalColumn} integer\n);", ddl, StringComparison.Ordinal);
        }

        var queryStats = CollectorCatalog.Find("query_stats")!;
        Assert.Equal(OffsetColumns[0], queryStats.PayloadColumns[^2].Name);
        Assert.Equal(OffsetColumns[1], queryStats.PayloadColumns[^1].Name);
        Assert.All(OffsetColumns, c => Assert.Equal(CollectorColumnType.Integer, queryStats.PayloadColumns.Single(p => p.Name == c).Type));
        Assert.EndsWith("    statement_start_offset integer,\n    statement_end_offset integer\n);", PgSchemaGenerator.CreateTable(queryStats), StringComparison.Ordinal);

        /* The COPY column list is the PayloadColumns order, so the new columns ride LAST there too. */
        Assert.EndsWith(", query_plan_xml_bytes, statement_start_offset, statement_end_offset) FROM STDIN (FORMAT BINARY)",
            PgCollectorRowWriter.CopyCommandFor(QueryStatsCollector.Instance), StringComparison.Ordinal);
        foreach (var table in IntervalTables)
        {
            Assert.EndsWith($", {IntervalColumn}) FROM STDIN (FORMAT BINARY)",
                PgCollectorRowWriter.CopyCommandFor(CollectorCatalog.Find(table)!), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The regenerated resolving view names every query_stats payload column in order — the two offsets ahead
    /// of the digests, which is WHY the rung drops and recreates rather than replacing — and the pre-add guard
    /// covers them, so a store replaying V51, V54 or V121 on this build (all of which re-emit the view) has
    /// the columns before the view names them.
    /// </summary>
    [Fact]
    public void TheResolvingView_NamesTheOffsets_AndEveryRungReEmittingItPreAddsThem()
    {
        var view = PgSchemaGenerator.GenerateQueryStatsResolvingView();
        var startAt = view.IndexOf("f.statement_start_offset", StringComparison.Ordinal);
        var endAt = view.IndexOf("f.statement_end_offset", StringComparison.Ordinal);
        var digestAt = view.IndexOf("f.query_text_digest", StringComparison.Ordinal);
        Assert.True(startAt > 0 && endAt > startAt && digestAt > endAt,
            "the offsets must be projected in order and ahead of the digest columns");

        var preAdds = PgSchemaGenerator.GenerateQueryStatsPayloadColumnPreAdds();
        foreach (var column in OffsetColumns)
        {
            Assert.Contains($"ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS {column} integer;", preAdds, StringComparison.Ordinal);
        }

        /* Every rung that emits the RESOLVING view (V38, V51, V54, V121, V128) names f.<offset>; V4 and V14
           define the passthrough and name neither, so they are skipped on `use < 0` the way
           MigrationLadderPins skips them. The resolving definers must pre-add first: presence, then order. */
        var resolvingDefiners = 0;
        foreach (var rung in PgMigrations.Scripts.Where(m => m.Sql.Contains("VIEW v_query_stats AS", StringComparison.Ordinal)))
        {
            foreach (var column in OffsetColumns)
            {
                var use = rung.Sql.IndexOf($"f.{column}", StringComparison.Ordinal);
                if (use < 0)
                {
                    continue;
                }

                resolvingDefiners++;
                var guard = rung.Sql.IndexOf($"ADD COLUMN IF NOT EXISTS {column} ", StringComparison.Ordinal);
                Assert.True(guard >= 0, $"V{rung.Version} names f.{column} and never adds it");
                Assert.True(guard < use, $"V{rung.Version} adds {column} AFTER the view uses it");
            }
        }

        /* Two offsets × the five resolving definers: a scan that skipped everything would pass while asserting
           nothing. */
        Assert.Equal(10, resolvingDefiners);
    }

    /* ---- the probe (three sites, top arm) ------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm.
    ///
    /// <para>The probe asks the question, the caller reads the answer, the map has the parameter — three
    /// sites, and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column.
    /// Miss all three and a fully-migrated store probes one rung short, so the connect-time gate refuses a
    /// store that is in fact current — permanently, because no later upgrade changes the answer. The
    /// sentinel shares V127's column NAME on a different TABLE, which is exactly why the table is in the
    /// predicate.</para>
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            $"table_name = 'procedure_stats'\n                                                     AND   column_name = '{IntervalColumn}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasDeltaFamilyIntervalCompletion", viewer, StringComparison.Ordinal);

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
        var thisArm = viewer.IndexOf("if (hasDeltaFamilyIntervalCompletion)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasDeltaFamilyIntervalColumns)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V128 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V128 arm sits below the previous rung's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..], StringComparison.Ordinal);
    }

    /* ---- the writers ---------------------------------------------------------------------------------- */

    /// <summary>
    /// The four collectors now WRITE the interval — through <c>CalculateDeltaWithInterval</c> for every group,
    /// never the bare <c>CalculateDelta</c> — as the minimum over each row's delta groups (the V127 rule). The
    /// two SQL Server collectors take the minimum in <c>WritePayload</c>; the two PostgreSQL collectors compute
    /// their deltas in <c>ReadAsync</c> (the idle-row skip needs them before the row exists), so the minimum
    /// rides the Row and <c>WritePayload</c> writes it from there. The column without the writer would be a
    /// NULL forever; the writer taking one headline group's interval would let an independently reset sibling
    /// counter's 0 read as idle over a real interval.
    /// </summary>
    [Fact]
    public void TheFourCollectors_WriteTheIntervalAsTheMinimumOverTheirDeltaGroups()
    {
        foreach (var (file, groups, written) in new[]
        {
            ("ProcedureStatsCollector.cs", 7, ".Value(sampleIntervalSeconds);"),
            ("MemoryGrantsCollector.cs", 2, ".Value(sampleIntervalSeconds);"),
            ("PgWaitStatsCollector.cs", 2, ".Value(row.SampleIntervalSeconds);"),
            ("PgStatementStatsCollector.cs", 3, ".Value(row.SampleIntervalSeconds);"),
        })
        {
            var source = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", file);

            Assert.Equal(groups, CountOf(source, "context.Deltas.CalculateDeltaWithInterval("));
            Assert.DoesNotContain("context.Deltas.CalculateDelta(", source, StringComparison.Ordinal);
            Assert.Contains("var sampleIntervalSeconds = Math.Min(", source, StringComparison.Ordinal);
            Assert.Contains(written, source, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// query_stats WRITES the two offsets it keys on, raw, from the same Row fields the key is built from — so
    /// the stored pair and the key's pair are one value, not two that agree today.
    /// </summary>
    [Fact]
    public void TheQueryStatsCollector_WritesTheOffsetsItKeysOn_Raw()
    {
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "QueryStatsCollector.cs");

        Assert.Contains("$\"{row.SqlHandle}:{row.StatementStartOffset}:{row.StatementEndOffset}:{row.PlanHandle}\"", source, StringComparison.Ordinal);
        Assert.Contains(".Value(row.StatementStartOffset)", source, StringComparison.Ordinal);
        Assert.Contains(".Value(row.StatementEndOffset);", source, StringComparison.Ordinal);
        /* No arithmetic on the way to the store: the collector's SUBSTRING divides by two to SLICE the text,
           and that is the only place the byte offsets are ever transformed. */
        Assert.DoesNotContain(".Value(row.StatementStartOffset / ", source, StringComparison.Ordinal);
        Assert.DoesNotContain(".Value(row.StatementEndOffset / ", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The calculator's doc claim is now true for ALL families and says so: every member of
    /// <c>DeltaFamilyCollectors</c> persists the interval, and the sentence names the census that pins it
    /// (Lite.Tests' <c>DeltaFamilyIntervalColumnTests</c>, whose still-naked list is empty), so it cannot
    /// silently go false again by an eleventh family shipping naked.
    /// </summary>
    [Fact]
    public void TheCalculatorDoc_SaysEveryFamilyStoresTheInterval_AndNamesTheCensus()
    {
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "CollectorDeltaCalculator.cs");

        Assert.Contains("for EVERY delta family: all ten members of DeltaFamilyCollectors persist a", source, StringComparison.Ordinal);
        Assert.Contains("procedure_stats, memory_grant_stats, pg_wait_stats and pg_statement_stats since", source, StringComparison.Ordinal);
        Assert.Contains("Darling V128 / Lite v61, #3540)", source, StringComparison.Ordinal);
        Assert.Contains("DeltaFamilyIntervalColumnTests is the census", source, StringComparison.Ordinal);
        /* The "still persist no interval" sentence V127 wrote is gone with the list it described. */
        Assert.DoesNotContain("persist no interval", source, StringComparison.Ordinal);
        Assert.DoesNotContain("naked list shrinks", source, StringComparison.Ordinal);
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
