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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V144 (#4211): two new tables, <c>collect.raw_chunk_interval_rung_history</c> and
/// <c>collect.raw_chunk_interval_reconcile_runs</c> — the store <see cref="RawChunkIntervalReconciler"/> reads
/// and writes.
///
/// <para>This file took over the "I am the top rung" claim that moved off
/// <see cref="QueryStoreIntervalLatestRungTests"/> (V143) when this rung landed, and hands it on to
/// the #3953 B4 wide-interval-table rung (V145) when that one did.</para>
/// </summary>
public sealed class RawChunkIntervalRungHistoryRungTests
{
    private const int RungVersion = 144;

    private const int ProbeOrdinal = 119;

    private const string HistoryTable = "raw_chunk_interval_rung_history";
    private const string RunsTable = "raw_chunk_interval_reconcile_runs";

    private static PgMigrations.Migration V144 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("raw-chunk-interval-rung-history", V144.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V145 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>Two additive, idempotent tables plus their time indexes — no GRANT, no view, no ALTER of an
    /// existing table.</summary>
    [Fact]
    public void TheRungCreatesTwoTablesAndTheirTimeIndexes_AndNothingElse()
    {
        var sql = V144.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"CREATE TABLE IF NOT EXISTS collect.{HistoryTable}", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE TABLE IF NOT EXISTS collect.{RunsTable}", sql, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(sql, "CREATE TABLE").Count);
        Assert.Equal(2, Regex.Matches(sql, "CREATE INDEX").Count);

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "GRANT", "VIEW", "DROP ", "ALTER TABLE", "CONCURRENTLY" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        /* Naive-UTC timestamp columns per the store's cross-engine contract (StoreSelfMetrics.SweepAsync's own
           comment) — never timestamptz. */
        Assert.DoesNotContain("timestamptz", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("changed_at timestamp NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("run_at timestamp NOT NULL", sql, StringComparison.Ordinal);

        /* No serial id — the V111 store_log_events/store_log_captures shape this rung mirrors. */
        Assert.DoesNotContain("serial", sql, StringComparison.OrdinalIgnoreCase);
    }

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */

    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        Assert.Contains(
            $"table_name = '{HistoryTable}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasRawChunkIntervalRungHistory", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V145 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasRawChunkIntervalRungHistory", method.GetParameters()[ProbeOrdinal].Name);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(143, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasRawChunkIntervalRungHistory)", StringComparison.Ordinal);
        var nextArm = viewer.IndexOf("if (hasQueryStoreIntervalLatest)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V144 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(nextArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < nextArm, "the V144 arm sits above V143's, so a current store maps to the newest rung");

        /* V145 landed above this rung (#3953 B4): its arm must sit ABOVE this one (newest-first), not the
           other way around, or a current V145 store would map one rung low. */
        var aboveArm = viewer.IndexOf("if (hasQueryStoreIntervalWide)", StringComparison.Ordinal);
        Assert.True(aboveArm >= 0, "the V145 arm is gone, so the handoff this file claims never happened");
        Assert.True(aboveArm < thisArm, "the V145 arm sits below the V144 arm, so a current V145 store maps one rung low");

        var armProseStart = viewer.LastIndexOf("/* V144 (#4211)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V144 arm has no comment block saying why it exists");
        var prose = viewer[armProseStart..thisArm];
        Assert.DoesNotContain(HistoryTable, prose, StringComparison.Ordinal);
        Assert.DoesNotContain(RunsTable, prose, StringComparison.Ordinal);
    }
}
