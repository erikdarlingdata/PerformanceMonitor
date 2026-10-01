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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that stores the hour's longest single checkpoint sync (#4834): two nullable columns,
/// <c>checkpoint_longest_sync_ms</c> (<c>bigint</c>) and <c>checkpoint_longest_sync_at</c> (<c>timestamp</c>, naive UTC),
/// on the <c>checkpointer</c> row of <c>collect.store_metrics</c>. The once-a-minute sampler (#4823) finds the longest
/// single sync in the hour, and until this rung only the Store Checkpointer Pressure alert saw it, in memory; the
/// hourly row is what <c>get_store_metrics</c> reads, so in the hour one long sync fired the alert the tool said there
/// was no pressure. No new table, no new hypertable (<see cref="TimescaleSupport.HypertableCount"/> stays 72), no
/// DEFAULT, no backfill, no passthrough (<c>store_metrics</c> has no <c>v_</c> view), no Lite twin (Lite stores no
/// <c>store_metrics</c>).
///
/// <para><b>The rung's number lives in one place.</b> Every fact below finds the rung by NAME and reads its version
/// off the migration, so renumbering it (another rung landing first) is one edit to the registration, the
/// <c>StorageVersion</c> constant and the viewer's <c>return</c>, and no test text changes.</para>
///
/// <para>The rule the columns exist for - the reading and the pressure judgement - is pinned where the reader lives,
/// in <c>StoreToastAndCheckpointerTests</c>. This file is the RUNG: the ladder, the DDL, and the probe.</para>
/// </summary>
public sealed class CheckpointLongestSyncRungTests
{
    /// <summary>The rung's registered name - how every test here (and the live schema-drop helper) finds it.</summary>
    public const string RungName = "checkpoint-longest-sync";

    /// <summary>This rung's sentinel ordinal in the viewer probe - the newest, so the last argument. The ordinal is a
    /// fact of the probe's shape, not of the rung's number, so a renumber leaves it alone.</summary>
    private const int ProbeOrdinal = 131;

    private const string MillisecondsColumn = "checkpoint_longest_sync_ms";
    private const string InstantColumn = "checkpoint_longest_sync_at";

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    /// <summary>The rung's version, read off the migration registered under <see cref="RungName"/>.</summary>
    public static int RungVersion => Rung.Version;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* No longer the top rung: V157 (mute-rule server_id) landed above it. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(RungVersion - 1, versions);
    }

    /// <summary>
    /// ONE ALTER adding exactly two columns - nullable, no DEFAULT, no backfill, no table, no index, no view, nothing
    /// else - on the store's own plain table only.
    /// </summary>
    [Fact]
    public void TheRungAddsTwoNullableColumnsToStoreMetrics_AndNothingElse()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains(
            $"ALTER TABLE collect.store_metrics\n    ADD COLUMN IF NOT EXISTS {MillisecondsColumn} bigint,\n    ADD COLUMN IF NOT EXISTS {InstantColumn} timestamp;",
            sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "ALTER TABLE"));
        Assert.Equal(2, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "DEFAULT", "NOT NULL", "UPDATE ", "INSERT ", "DELETE ", "CREATE ", "DROP ", "VIEW" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        /* store_metrics has no passthrough, so there is no view to refresh, and it is not a collector table, so it is
           no hypertable: it must stay the plain table the sweep's own retention DELETE assumes. */
        Assert.DoesNotContain("v_store_metrics", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain(CollectorCatalog.All, c => c.TargetTable == "store_metrics");
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung and the map treats it as the TOP arm: a missing top arm maps a
    /// fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains($"table_name = 'store_metrics' AND column_name = '{MillisecondsColumn}'", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasCheckpointLongestSync", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(RungVersion - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasCheckpointLongestSync)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0);
        var previousArm = viewer.IndexOf("if (hasQueryStoreIntervalEnd)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no sentinel arm for this rung - a fully-migrated store would map one rung short");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "this rung's arm sits below the previous rung's, so a current store maps one rung short");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table and the columns are named in the probe line and nowhere in the arm's prose - the
           coverage ratchet strips information_schema lines but cannot strip a comment. The arm's comment block sits
           ABOVE the `if`, so the prose searched is the span from the comment's start to the arm. */
        var armProseStart = viewer.LastIndexOf("/* V", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        var prose = viewer[armProseStart..thisArm];
        foreach (var name in new[] { "store_metrics", MillisecondsColumn, InstantColumn })
        {
            Assert.DoesNotContain(name, prose, StringComparison.Ordinal);
        }
    }
}
