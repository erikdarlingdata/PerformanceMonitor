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
/// <para>This file carried the "I am the top rung" claims that moved off
/// <see cref="PostmasterStartTimeRungTests"/> (V139) when this rung landed, and handed them on to
/// <c>CollectionCaveatsRungTests</c> (V141, #3691) when that one did. What stays is everything true of this
/// rung wherever it sits.</para>
///
/// <para>The rule the column exists for — the per-checkpoint average and the pressure judgement — is pinned
/// where the reader lives, in <c>StoreToastAndCheckpointerTests</c>. This file is the RUNG: the ladder, the
/// DDL, and the probe.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CheckpointsTimedRungTests
{
    private const int RungVersion = 140;
    private const int PreviousVersion = 139;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V141 (#3691,
    /// the collection-caveats table) appended its own — so this is a position within the signature rather
    /// than its end, the handoff <see cref="PostmasterStartTimeRungTests"/> made to this file one rung ago.</summary>
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
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V141 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
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
    /// The viewer's schema map, given every sentinel up to this rung's ordinal, returns this rung's version, and with
    /// this rung's sentinel clear, the previous rung's: a sentinel at the wrong ordinal maps a store one rung off.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }
}
