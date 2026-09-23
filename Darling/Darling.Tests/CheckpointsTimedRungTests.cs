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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V140 (#4037): ONE nullable <c>bigint</c> column, <c>checkpoints_timed</c>, on the
/// <c>checkpointer</c> row of <c>collect.store_metrics</c> — <c>pg_stat_checkpointer.num_timed</c> /
/// <c>pg_stat_bgwriter.checkpoints_timed</c>, beside the write-ms/sync-ms/requested counters V137 already carries
/// and the postmaster start time V139 added. Store Checkpointer Pressure (#3783) and <c>get_store_metrics</c>
/// judged the interval's SUMMED sync-phase milliseconds against a PER-CHECKPOINT bar, so a healthy store running
/// twelve five-to-eight-second checkpoints an hour breached the bar every interval even though no single
/// checkpoint came near it. Judging the AVERAGE (<c>SyncMs / (timed + requested)</c>) needs the timed count on
/// the row the sync-ms delta already comes from. No new table, no new hypertable
/// (<see cref="TimescaleSupport.HypertableCount"/> stays 72), no DEFAULT, no backfill, no passthrough
/// (<c>store_metrics</c> has no <c>v_</c> view), no Lite twin (Lite stores no <c>store_metrics</c>).
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="PostmasterStartTimeRungTests"/> (V139) when this rung landed: a fully-migrated store must map to
/// EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually current. When the
/// next rung lands they move on again, and what stays is everything true of this rung wherever it sits.</para>
///
/// <para>The rule the column exists for — the per-checkpoint average and the pressure judgement — is pinned
/// where the reader lives, in <c>StoreToastAndCheckpointerTests</c>. This file is the RUNG: the ladder, the
/// DDL, and the probe.</para>
/// </summary>
public sealed class CheckpointsTimedRungTests
{
    private const int RungVersion = 140;
    private const int PreviousVersion = 139;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 115;

    private const string Column = "checkpoints_timed";

    private static PgMigrations.Migration V140 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("checkpointer-timed-count", V140.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// ONE ALTER adding exactly one column — nullable, no DEFAULT, no backfill, no table, no index, no view,
    /// nothing else — on the store's own table only (<c>pg_write_stats.num_timed</c>, V88, is the
    /// MONITORED-target series, a different row for a different server, untouched by this rung).
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableBigintToStoreMetrics_AndNothingElse()
    {
        var sql = V140.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"ALTER TABLE collect.store_metrics\n    ADD COLUMN IF NOT EXISTS {Column} bigint;", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "ALTER TABLE"));
        Assert.Single(Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS"));

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "DEFAULT", "NOT NULL", "UPDATE ", "INSERT ", "DELETE ", "CREATE ", "DROP ", "VIEW" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        /* store_metrics has no passthrough, so there is no view to refresh — a CREATE OR REPLACE VIEW here
           would CREATE one the generator does not know about. */
        Assert.DoesNotContain("v_store_metrics", PgSchemaGenerator.AllPassthroughViews);

        Assert.DoesNotContain(CollectorCatalog.All, c => c.TargetTable == "store_metrics");
    }

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm. The
    /// probe asks the question, the caller reads the answer, the map has the parameter — a sentinel present at
    /// only some of them shifts every LATER ordinal onto the wrong column, and a missing top arm maps a
    /// fully-migrated store one rung short, permanently: <c>RequiredStoreSchemaVersion</c> is
    /// <c>StorageVersion.SchemaVersion</c>, so the connect-time gate would refuse a store that is current.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            $"table_name = 'store_metrics'\n                                                     AND   column_name = '{Column}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasCheckpointsTimed", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The top rung's sentinel IS the last argument. Derived from the signature rather than named by hand,
           so the arity tracks the method — a literal here goes stale the next rung. */
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasCheckpointsTimed", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this build's version. */
        var thisArm = viewer.IndexOf("if (hasCheckpointsTimed)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasPostmasterStartTime)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V140 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V140 arm sits below the previous rung's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table and the column are named in the probe line and nowhere in the arm's prose —
           the coverage ratchet strips information_schema lines but cannot strip a comment. The arm's comment block
           sits ABOVE the `if`, so the prose searched is the span from the previous arm's end to this one's. */
        var armProseStart = viewer.LastIndexOf("/* V140 (#4037)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V140 arm has no comment block saying why it exists");
        var prose = viewer[armProseStart..previousArm];
        foreach (var name in new[] { "store_metrics", Column })
        {
            Assert.DoesNotContain(name, prose, StringComparison.Ordinal);
        }
    }
}
