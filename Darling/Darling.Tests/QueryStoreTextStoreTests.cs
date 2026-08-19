/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2150 V74: where the query-text fetch lands statement text, and the ladder invariants around it.
///
/// <para>The payload used to select <c>query_sql_text</c> (<c>nvarchar(max)</c>) inside a
/// <c>TOP ... WITH TIES ... ORDER BY last_execution_time</c>, and a Top-N Sort carries every output column
/// through the sort while reading ALL of its input before emitting row one — so choosing the rows to ship
/// materialized text for the entire qualifying set. With #2210's plan XML already gone and that column as
/// the only difference: time-to-first-row 4.67s against 0.45s at 1,505 rows, 5.02s against 0.57s at 4,037.
/// Neither the row cap nor the client byte budget bounds it (TOP (500) measured the same as TOP (50000);
/// wall time flat from a 4 MB to a 256 MB budget).</para>
///
/// <para><b>The fetch is live as of the reader conversion.</b> <c>FetchQueryTextSeparately</c> is on for the
/// sweep, so the payload's inline <c>query_sql_text</c> is null for newly collected rows and all six reader
/// surfaces resolve text from this table, falling back to the inline column for history collected before the
/// cutover. The flip and that conversion had to land together: an unconverted reader would have shown blank
/// text for new rows while looking perfectly healthy.</para>
/// </summary>
public sealed class QueryStoreTextStoreTests
{
    /// <summary>
    /// The rung and the helper's own DDL must agree, or a fresh store and an upgraded one get different
    /// tables — the same discipline the ladder diff enforces for collector tables, applied by hand because a
    /// non-collector table is outside that generator.
    /// </summary>
    [Fact]
    public void TheRungMatchesTheHelpersCreateTableSql()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == 74);

        Assert.Equal("query-store-text", rung.Name);
        Assert.Equal(Normalize(QueryStoreTextStore.CreateTableSql), Normalize(rung.Sql));
    }

    /// <summary>
    /// The ladder's invariants: this rung is the top, the ladder is ordered and dense above the one
    /// sanctioned historical hole, and the build's schema version tracks it. A gap is skipped SILENTLY on
    /// every upgraded store, so the objects would never exist and no later upgrade would repair it.
    /// </summary>
    [Fact]
    public void TheRungIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* #2316 added V75, so this rung is no longer the top — the "I am the top" claim moves to the
           newest rung's own test (PlanContentRetentionTests) and this one keeps the invariants that stay
           true forever: the rung is PRESENT, the ladder is ordered and dense, and the build's schema
           version tracks the maximum. */
        Assert.Contains(74, versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The Viewer's connect-time gate has to be able to SEE a fully-migrated store, or every store at the
    /// newest version reads as below the required one and the Viewer refuses to open. Three things move in
    /// lockstep for that — a probe column, a reader argument and a map parameter — which is why this asserts
    /// the mapping rather than trusting the count.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreTo74()
    {
        /* #2316: no longer the top (that claim lives in PlanContentRetentionTests) — this fact keeps
           pinning that a store at exactly 74 maps to 74 and one at 73 maps to 73, forever. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        /* 49 positional sentinels then the V74 one by name — the map takes 50 parameters. Present => 74,
           newest-first. */
        var all = Enumerable.Repeat(true, 49).Cast<object>().ToArray();

        Assert.Equal(74, InvokeMap(all, hasQueryStoreText: true));

        /* And absent => the previous arm still answers 73 rather than falling through to the floor. */
        Assert.Equal(73, InvokeMap(all, hasQueryStoreText: false));
    }

    /// <summary>
    /// The probe SQL must ASK for the table, and the three places that move in lockstep must agree: a probe
    /// column, a reader argument, and a map parameter. A probe that cannot SEE the newest object maps every
    /// fully-migrated store below the required version and the Viewer refuses to open.
    ///
    /// <para>Asserted on the reader's highest ordinal rather than by counting <c>EXISTS (</c>, because one
    /// probe column is a COMPOUND <c>EXISTS(...) OR EXISTS(...)</c> — so the occurrence count is one higher
    /// than the column count and a naive comparison fails for a reason that has nothing to do with this
    /// rung.</para>
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheTable_AndTheThreePlacesAgree()
    {
        Assert.Contains("table_name = 'query_store_text'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var mapParameters = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetParameters().Length;

        var viewerSource = ReadViewerSource();

        /* The reader must hand over exactly one argument per map parameter: ordinals are 0-based, so the
           highest is Count - 1, and the next one up must NOT appear. */
        Assert.Contains($"reader.GetBoolean({mapParameters - 1})", viewerSource, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({mapParameters})", viewerSource, StringComparison.Ordinal);
    }

    /// <summary>
    /// The table's shape: keyed on <c>query_id</c> — which is already a stored fact column, so this rung
    /// adds a table and touches nothing existing — and indexed on the column it is pruned by.
    /// </summary>
    [Fact]
    public void TheTableIsKeyedOnQueryIdAndIndexedForThePrune()
    {
        Assert.Contains("PRIMARY KEY (server_id, database_name, query_id)", QueryStoreTextStore.CreateTableSql, StringComparison.Ordinal);
        Assert.Contains("idx_query_store_text_last_seen", QueryStoreTextStore.CreateTableSql, StringComparison.Ordinal);
        Assert.Contains(QueryStoreTextStore.LastSeenColumn, QueryStoreTextStore.CreateTableSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The upsert overwrites the TEXT, not just the stamp, and that is load-bearing rather than defensive:
    /// <c>query_id</c> is unique within a database only until Query Store is RESET, which renumbers from the
    /// start — so id 5 afterwards is a different statement than id 5 before. The refresh horizon brings us
    /// back to re-read it, and this is where the corrected text has to land. Touching only <c>last_seen</c>
    /// would leave the old statement's text attached to the new id forever, which reads as a plausible wrong
    /// answer rather than as missing data.
    /// </summary>
    [Fact]
    public void TheUpsertOverwritesTheTextAndOrdersByTheConflictKey()
    {
        Assert.Contains("query_sql_text = EXCLUDED.query_sql_text", QueryStoreTextStore.UpsertSql, StringComparison.Ordinal);
        Assert.Contains("last_seen = EXCLUDED.last_seen", QueryStoreTextStore.UpsertSql, StringComparison.Ordinal);

        /* #1801: concurrent batches touching overlapping keys in different orders deadlock. */
        Assert.Contains("ORDER BY server_id, database_name, query_id", QueryStoreTextStore.UpsertSql, StringComparison.Ordinal);

        /* Monotonic stamp, so an out-of-order write cannot age a row backwards into the prune's reach. */
        Assert.Contains("WHERE EXCLUDED.last_seen >= query_store_text.last_seen", QueryStoreTextStore.UpsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prune is bounded to roughly one chunk-width of the OLDEST rows per call, so a single sweep cannot
    /// take an unbounded row lock — the same shape as the plan map's.
    /// </summary>
    [Fact]
    public void ThePruneIsBoundedToOneChunkWidth()
    {
        var sql = QueryStoreTextStore.PruneSql(7);

        Assert.StartsWith("DELETE FROM collect.query_store_text WHERE last_seen < $1", sql, StringComparison.Ordinal);
        Assert.Contains("INTERVAL '7 days'", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT min(last_seen)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The retention margin is ADDED to the fact horizon, never subtracted. Text has to OUTLIVE the rows
    /// that reference it: retired early, a fact's statement reads as absent, which nothing distinguishes
    /// from a statement that never had text. Being late costs a few unread rows.
    /// </summary>
    [Fact]
    public void TheRetentionMarginKeepsTextAliveLongerThanTheFacts()
        => Assert.True(QueryStoreTextStore.PruneMarginDays > 0,
            "a zero or negative margin would retire text at or before the facts that reference it");

    /// <summary>
    /// The flip is ON for the scheduled sweep and OFF for the on-demand read, and that difference is not
    /// cosmetic: the sweep STORES the text it stops shipping inline, while <c>FetchRowsAsync</c> hands rows
    /// straight back to its caller and writes nothing — no store insert, no text fetch, no watermark — so
    /// nulling the inline column there would lose the statement outright rather than relocate it.
    /// Asserted by splitting the source on the method rather than by counting occurrences, because the
    /// failure mode worth catching is a flag flipped in the WRONG context, which any count would pass.
    /// </summary>
    [Fact]
    public void TheFetchIsOnForTheSweepAndOffForTheOnDemandRead()
    {
        var source = ReadRunnerSource();

        var split = source.IndexOf("FetchRowsAsync<TRow>", StringComparison.Ordinal);
        Assert.True(split > 0,
            "FetchRowsAsync was renamed or moved — this guard can no longer tell the sweep context from the on-demand one");

        var sweep = source[..split];
        var onDemand = source[split..];

        Assert.Contains("FetchQueryTextSeparately = true,", sweep, StringComparison.Ordinal);
        Assert.DoesNotContain("FetchQueryTextSeparately = false,", sweep, StringComparison.Ordinal);
        Assert.Contains("FetchQueryTextSeparately = false,", onDemand, StringComparison.Ordinal);
        Assert.DoesNotContain("FetchQueryTextSeparately = true,", onDemand, StringComparison.Ordinal);

        /* The pass the flip depends on: with the flag on and this call gone, the payload would ship no text
           and nothing would land any, which is the one combination that loses data silently. */
        Assert.Contains("FetchAndStoreQueryTextAsync(textFetchConnection", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2312: the text watermark retired with the plan one — the fetch is activity-driven against this
    /// store's own rows now, so there is no textwm: family to save, and the shared prune set must not
    /// claim it (a prefix listed there without a writer is a standing invitation to delete nothing and
    /// call it hygiene). The V77 migration deleted the orphaned rows wholesale.
    /// </summary>
    [Fact]
    public void TheTextWatermarkFamilyIsRetired()
    {
        var source = ReadRunnerSource();

        Assert.DoesNotContain("textwm:", source, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStorePerDatabaseState.PrunableKeys,
            k => k.Prefix == "textwm:");
    }

    /// <summary>
    /// Calls the map with 49 positional sentinels plus the V74 one, by reflection — the signature has 50
    /// parameters and hand-writing that many literals is how a miscount turns into a confusing compile
    /// error instead of a clear failure.
    /// </summary>
    private static int InvokeMap(object[] leading, bool hasQueryStoreText)
    {
        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        /* #2316 and #2319 appended parameters after this rung's — pass them FALSE so these facts keep
           exercising the V74/V73 arms rather than the newer ones. */
        /* Parameters appended by LATER rungs are padded FALSE, so this fact keeps exercising its own
           arm rather than a newer one. Derived from the method's arity rather than listed by hand, so
           a future rung does not have to edit this file -- #2357 (V78) was the fourth that would have. */
        var args = leading.Concat(new object[] { hasQueryStoreText }).ToArray();
        args = args
            .Concat(Enumerable.Repeat((object)false, method.GetParameters().Length - args.Length))
            .ToArray();
        Assert.Equal(method.GetParameters().Length, args.Length);

        return (int)method.Invoke(null, args)!;
    }

    private static string Normalize(string sql) =>
        string.Join(" ", sql.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    private static string ReadViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }

    private static string ReadRunnerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
