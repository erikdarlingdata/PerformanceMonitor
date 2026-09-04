/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V108 rung - the server-scoped phase split persisted on <c>collection_log</c>.
///
/// <para><b>Why it exists.</b> #2851 decomposes a server-scoped collector's <c>sql_duration_ms</c> into
/// <c>open:</c> and <c>drain:</c> and reports the watermark read beside them, and every bit of it lived only
/// in an app-log line. So the question the split was built to answer - which phase owns a collector's cost,
/// across servers and over time - could not be aggregated, trended, or reached through the MCP at all. On
/// 2026-09-03 an investigation into <c>procedure_stats</c>' 4,724 ms drain stalled outright when AWS SSO
/// started refusing <c>GetRoleCredentials</c>: the store was answering fine, but the numbers were only on the
/// box. Three columns turn that into a store query.</para>
/// </summary>
public class CollectionLogPhaseSplitStoreTests
{
    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collection-log-phase-split", PgMigrations.Scripts.Single(s => s.Version == 108).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// Three nullable integers and a view refresh. Nullable with no DEFAULT is what makes this safe to run
    /// against the busiest table in the store: a catalog-only change stays instant on a compressed
    /// hypertable, where adding a column WITH a default is the shape TimescaleDB has historically refused.
    /// Verified against a real store before merge - 37 compressed chunks, applied in ~100 ms.
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumns_Idempotently_AndWithoutADefaultOrBackfill()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 108).Sql;

        Assert.Contains("ALTER TABLE collect.collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS sql_open_ms integer", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS sql_drain_ms integer", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS watermark_ms integer", sql, StringComparison.Ordinal);

        /* No DEFAULT and no backfill: a row written before this rung does not know its phases, and NULL says
           so where 0 would claim a measured instant open - the distinction the whole design turns on. */
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The half of this rung that is easy to forget and impossible to notice: Postgres FREEZES a view's
    /// <c>SELECT *</c> column list at CREATE, so without the refresh an UPGRADED store keeps serving the
    /// pre-V108 list forever while a FRESH one works perfectly - the worst possible split. V14 exists
    /// because it already happened once, and V80 re-learned it on this very table.
    /// </summary>
    [Fact]
    public void TheRungRefreshesThePassthroughView_AndTheReadsGoThroughIt()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 108).Sql;

        Assert.Contains(
            "CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;",
            sql, StringComparison.Ordinal);

        Assert.Contains("FROM v_collection_log", DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The residual is NOT a column, and this is the pin that keeps it that way. <c>other:</c> is defined
    /// against <c>sql_duration_ms</c>, so a stored copy could drift from the parent it completes - and the
    /// sum-to-parent property is the entire reason a large residual is a finding rather than noise.
    /// </summary>
    [Fact]
    public void TheRungStoresNoResidualColumn_BecauseAStoredResidualCanGoStale()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 108).Sql;

        Assert.DoesNotContain("other_ms", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sql_other_ms", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// All three or none. A row carrying an open with no drain cannot be read as a split, so the writer
    /// gates on #2851's MEASURED flag rather than on any figure being non-zero - which is precisely the
    /// distinction that flag was added for. A genuinely instant open must store 0, not vanish into NULL.
    /// </summary>
    [Fact]
    public void ThePhaseTriple_IsAllOrNothing_AndAMeasuredZeroSurvives()
    {
        var measured = new CollectorRunResult(
            Rows: 150, SqlMs: 4902, StorageMs: 300,
            ServerPhasesMeasured: true, ServerOpenMs: 149, ServerDrainMs: 4724, ServerWatermarkMs: 51);

        var phases = measured.ServerPhases;
        Assert.NotNull(phases);
        Assert.Equal(149, phases!.Value.OpenMs);
        Assert.Equal(4724, phases.Value.DrainMs);
        Assert.Equal(51, phases.Value.WatermarkMs);

        /* Measured, and every phase genuinely zero. This MUST still produce a triple: NULL here would say
           "this path emits no split", which is a different fact and the one #2851 added the flag to tell
           apart. */
        var instant = new CollectorRunResult(
            Rows: 5, SqlMs: 100, StorageMs: 10,
            ServerPhasesMeasured: true, ServerOpenMs: 0, ServerDrainMs: 0, ServerWatermarkMs: 0);

        Assert.NotNull(instant.ServerPhases);
        Assert.Equal(0, instant.ServerPhases!.Value.OpenMs);

        /* Not measured - the enumerated and per-database paths - stores nothing rather than three zeros. */
        var unmeasured = new CollectorRunResult(Rows: 10, SqlMs: 500, StorageMs: 20);
        Assert.Null(unmeasured.ServerPhases);
    }

    /// <summary>
    /// The write really does carry the three columns, and the MCP read really does select them back. Pinned
    /// on the SQL rather than a round-trip so it runs everywhere, including where no Postgres is available.
    /// </summary>
    [Fact]
    public void TheWriterPersistsTheTriple_AndTheMcpReadSelectsItBack()
    {
        var insert = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs");
        Assert.Contains("sql_open_ms, sql_drain_ms, watermark_ms", insert, StringComparison.Ordinal);
        Assert.Contains("$15, $16, $17", insert, StringComparison.Ordinal);

        Assert.Contains("sql_open_ms", DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);
        Assert.Contains("sql_drain_ms", DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);
        Assert.Contains("watermark_ms", DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The residual the MCP surfaces is derived, sums to its parent by construction, and stays NULL when the
    /// run recorded no split - so "not measured" never reads as "measured zero" downstream either.
    /// </summary>
    [Fact]
    public void TheMcpResidual_IsDerived_SumsToParent_AndIsNullWhenUnmeasured()
    {
        var measured = new DarlingDataReader.CollectionLogEntry(
            "procedure_stats", DateTime.UtcNow, 5202, 4902, 300, 150, "SUCCESS", null,
            SqlOpenMs: 149, SqlDrainMs: 4724, WatermarkMs: 51);

        Assert.Equal(29, measured.SqlOtherMs);
        Assert.Equal(measured.SqlDurationMs, measured.SqlOpenMs + measured.SqlDrainMs + measured.SqlOtherMs);

        /* A pre-V108 row, or any run whose path emits no split. */
        var legacy = new DarlingDataReader.CollectionLogEntry(
            "wait_stats", DateTime.UtcNow, 120, 100, 20, 42, "SUCCESS", null);
        Assert.Null(legacy.SqlOtherMs);

        /* Separate stopwatches, so tiny skew must clamp rather than surface as a negative duration. */
        var skewed = new DarlingDataReader.CollectionLogEntry(
            "skewed", DateTime.UtcNow, 100, 100, 10, 1, "SUCCESS", null,
            SqlOpenMs: 60, SqlDrainMs: 50, WatermarkMs: 0);
        Assert.Equal(0, skewed.SqlOtherMs);
    }

    /// <summary>
    /// EVERY writer of the shared collection_log INSERT binds exactly as many parameters as the statement
    /// declares. Added because this rung broke the OTHER one: <c>InsertCollectionLogSql</c> is deliberately
    /// shared between the per-collector writer and the fleet-wide retention run-record, so widening the
    /// column list without widening both binding blocks produced
    /// <c>08P01: bind message supplies 14 parameters, but prepared statement "" requires 17</c>.
    ///
    /// <para>It failed SILENTLY, which is the reason this is a pin rather than a fixed typo: both writers
    /// are failure-isolated by design - an observability write must never break the collection loop - so the
    /// exception went to a Debug log and the run-record simply never appeared. It surfaced only because a
    /// live-Postgres test happened to assert on that record's existence. A third writer added later would
    /// fail exactly as quietly, so the invariant is asserted over the whole file rather than the two sites
    /// that exist today (the [[documented-is-not-enforced]] shape: the comment beside the fanout block
    /// already warned that the statement is shared, and a comment cannot enforce anything).</para>
    /// </summary>
    [Fact]
    public void EveryWriterOfTheSharedInsert_BindsExactlyAsManyParametersAsTheStatementDeclares()
    {
        var source = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs");

        var placeholders = System.Text.RegularExpressions.Regex
            .Matches(source[source.IndexOf("INSERT INTO collection_log", StringComparison.Ordinal)..].Split(';')[0], @"\$\d+")
            .Select(m => m.Value).Distinct().Count();

        /* Deliberately a literal, and deliberately maintained by hand: it is the tripwire that makes
           widening this statement a conscious act. 17 at V108, 22 at V109 (#2864's five drain-forensics
           columns). Bumping it is the moment you are forced to ask whether EVERY writer was widened
           too - which is the failure it was written for. */
        Assert.Equal(22, placeholders);

        var writerStarts = System.Text.RegularExpressions.Regex
            .Matches(source, @"new NpgsqlCommand\(InsertCollectionLogSql")
            .Select(m => m.Index)
            .ToList();

        /* Two today. If this count changes, the new writer needs the same audit - which is the point. */
        Assert.Equal(2, writerStarts.Count);

        foreach (var start in writerStarts)
        {
            var end = source.IndexOf("ExecuteNonQueryAsync", start, StringComparison.Ordinal);
            Assert.True(end > start, "Could not find the execute call closing a collection_log writer.");

            var bindings = System.Text.RegularExpressions.Regex
                .Matches(source[start..end], @"command\.Parameters\.Add")
                .Count;

            Assert.True(bindings == placeholders,
                $"A writer of the shared collection_log INSERT binds {bindings} parameters but the statement "
                + $"declares {placeholders}. Npgsql raises 08P01 at RUNTIME for this, and both writers swallow "
                + "their exceptions by design, so the row silently never lands. Widen every binding block when "
                + "you widen the column list.");
        }
    }

    /// <summary>
    /// The connect-time gate. A COLUMN sentinel rather than a table one, because collection_log has existed
    /// since V1 and only its columns are new. Being the TOP rung, a fully-migrated store must map to exactly
    /// this version or the viewer refuses a store that is perfectly current.
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheColumn_AndMapsAFullyMigratedStoreToThisRung()
    {
        Assert.Contains(
            "table_name = 'collection_log' AND column_name = 'sql_drain_ms'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        Assert.Contains("reader.GetBoolean(83)", ReadViewerSource(), StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* Every sentinel true = a fully-migrated store, which must map to THIS rung. As the top rung this
           is also the "and no more than that" guard: a later rung appending a sentinel without its own arm
           would leave this returning 108 for a store that is actually further along. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
    }

    private static string ReadViewerSource() =>
        ReadSource("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");

    /// <summary>Reads a repo source file by walking up from the test binary to the repo root.</summary>
    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 12 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }

            dir = Directory.GetParent(dir)?.FullName;
        }

        throw new FileNotFoundException($"Could not locate {relativePath} from {AppContext.BaseDirectory}");
    }
}
