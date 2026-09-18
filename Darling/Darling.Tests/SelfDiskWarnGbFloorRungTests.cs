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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V126 / #3528: the Store Disk Pressure warning's GB floor moves onto <c>config.config_alert_settings</c>.
///
/// <para>The self-store warn condition was percent-only, its own comment calling a GB floor "a trivial
/// follow-up if an operator ever wants one" — and #3528's example is the want: 400 GB free on a 4 TB store
/// volume fired a CRITICAL "act now". The floor is an AND qualifier (the <c>pvs_floor_gb</c> composition,
/// deliberately not the target-volume pair's OR, whose GB dimension ADDS fires), so a large volume at a
/// low percent stays quiet until absolute free space is genuinely short; 0 removes the floor.</para>
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="CollectorDatabaseScopeRungTests"/> (V125) when this rung landed, the same handoff that file
/// received from <see cref="FleetSweepCadenceKnobRungTests"/> (V124) — a fully-migrated store must map to
/// EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually current.</para>
/// </summary>
public sealed class SelfDiskWarnGbFloorRungTests
{
    private const int RungVersion = 126;
    private const int PreviousVersion = 125;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 101;

    private const string FloorColumn = "self_disk_free_warn_gb";

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "self-disk-warn-gb-floor",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds ONE column to the singleton settings row, schema-qualified, with the shipped constant
    /// as its default.
    ///
    /// <para>The DEFAULT is compared against <c>DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb</c> rather
    /// than a literal (restated in the rung only because a rung is a SQL string), so a moved shipped
    /// default cannot leave upgraded stores qualifying at a floor no surface reports. The default is
    /// NON-ZERO on upgrade deliberately, unlike V122's knobs: their acceptance was "an untouched store
    /// fires exactly where it did", while #3528's is that the untouched firing IS the defect — the issue's
    /// own example is a default-configured store paging with 400 GB of runway. Any store volume at or
    /// under 500 GB (floor ÷ warn percent) keeps the exact pre-#3528 percent behaviour.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumn_SchemaQualified_WithTheShippedConstantAsDefault()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Equal(1, CountOf(rung, "ALTER TABLE config.config_alert_settings"));
        Assert.DoesNotContain("ALTER TABLE config_alert_settings", rung, StringComparison.Ordinal);

        /* IF NOT EXISTS so re-running the ladder over a store that already has it is a no-op rather than
           a 42701 that aborts the whole migration. integer, matching self_disk_free_warn_percent and the
           low-disk GB columns on this same table — a whole-GB knob has no meaningful fractional part. */
        Assert.Equal(1, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));
        Assert.DoesNotContain("double precision", rung, StringComparison.Ordinal);
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {FloorColumn} integer NOT NULL DEFAULT "
            + ((int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb).ToString(CultureInfo.InvariantCulture) + ";",
            rung, StringComparison.Ordinal);

        /* And the C# seed names the same figure, so a fresh file-plane config and an upgraded store row
           agree without either citing the other. The constant is whole-valued by construction — the cast
           in the assertion above must not be hiding a fractional shipped default. */
        Assert.Equal(DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb, (int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb);
        Assert.Equal((int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb, new AlertsConfig().SelfDiskFreeWarnGb);

        /* No reload beacon of its own: config_alert_settings already carries V17's statement-level
           trg_bump_alert_settings, so a second trigger here would be a duplicate bump per write. */
        Assert.DoesNotContain("config_bump_version", rung, StringComparison.Ordinal);

        /* And no per-table GRANT: this table carries table-level grants with no column carve, which is
           what every earlier knob rung on it says. */
        Assert.DoesNotContain("GRANT", rung, StringComparison.Ordinal);
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
        Assert.Contains($"column_name = '{FloorColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasSelfDiskWarnGbFloor", viewer, StringComparison.Ordinal);

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

        /* And in the source, the arm sits ABOVE V125's — newest-first is the whole contract of that method —
           and returns this build's version rather than a literal that could drift from it. This is the
           textual half of the top-arm claim, inherited from CollectorDatabaseScopeRungTests the way that
           file inherited it from FleetSweepCadenceKnobRungTests. */
        var v126 = viewer.IndexOf("if (hasSelfDiskWarnGbFloor)", StringComparison.Ordinal);
        var v125 = viewer.IndexOf("if (hasCollectorScheduleDatabases)", StringComparison.Ordinal);
        Assert.True(v126 >= 0, "the viewer has no V126 sentinel arm — a fully-migrated store would map to 125");
        Assert.True(v125 >= 0, "the V125 arm is gone, so this pin is comparing against nothing");
        Assert.True(v126 < v125, "the V126 arm sits below V125's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v126..], StringComparison.Ordinal);
    }

    /* ---- every settings-row surface handles the column ------------------------------------------------ */

    /// <summary>
    /// EVERY wired surface that reads or writes the settings row names the column — and the ONE surface
    /// deliberately not wired yet is pinned to its abstinence. The wired lists drive ordinals or parameter
    /// positions, so a column added to one and not the others re-maps reads and writes at once.
    ///
    /// <para><b>The viewer's select/upsert is the pinned abstinence</b>, unlike every earlier knob rung:
    /// this knob lands backend-first (store plane + the two MCP tools), and the Settings window's box
    /// follows in the viewer pass. The viewer's explicit column lists mean its select and save are
    /// untouched by the new column — nothing throws, and a viewer Save cannot null the floor out. When the
    /// viewer pass wires the box, this assertion is where that decision flips.</para>
    /// </summary>
    [Fact]
    public void EveryWiredSettingsRowSurfaceNamesTheColumn_AndTheViewerAbstains()
    {
        var service = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");

        /* The service must READ it, not merely select it — ApplyToConfig replaces config.Alerts wholesale,
           so a selected-but-unread column resets the floor to the shipped default on every worker start.
           Both halves, because one of the two being present is what an off-by-one produces. */
        Assert.Contains(FloorColumn, service, StringComparison.Ordinal);
        Assert.Contains("SelfDiskFreeWarnGb = reader.GetInt32(", service, StringComparison.Ordinal);

        /* The MCP read names the column and the report/accept pair carries the wire key — readable AND
           writable, because a read-only knob leaves the UPDATE in someone's runbook. */
        Assert.Contains(FloorColumn, DarlingAlertReader.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("disk_free_warn_gb = s.SelfDiskFreeWarnGb", tools, StringComparison.Ordinal);
        Assert.Contains(
            "case \"disk_free_warn_gb\": AddInt(\"self_disk_free_warn_gb\", n, \"self_alerts.disk_free_warn_gb\", 0, int.MaxValue); break;",
            tools, StringComparison.Ordinal);

        /* The deliberate abstinence: the viewer's settings surface does not name the column yet. */
        var viewerSettings = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs");
        Assert.DoesNotContain(FloorColumn, viewerSettings, StringComparison.Ordinal);
    }

    /* ---- the seam reaches the gate -------------------------------------------------------------------- */

    /// <summary>
    /// The settings adapter defaults to the shipped constant and clamps a hand-edited store value at the
    /// 0 floor — the raw-in/clamped-out split every knob on this table uses, with 0 IN range because it
    /// removes the floor (the <c>pvs_floor_gb</c> reading) rather than being nonsense. The write bound in
    /// <see cref="EveryWiredSettingsRowSurfaceNamesTheColumn_AndTheViewerAbstains"/> is the same
    /// <c>[0, int.MaxValue]</c>, so no accepted value is one this clamp rewrites.
    /// </summary>
    [Fact]
    public void TheSettingsSeamDefaultsToTheConstant_AndClampsAtZero()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        Assert.Equal((int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb, settings.SelfDiskFreeWarnGb);

        config.Alerts.SelfDiskFreeWarnGb = -5;
        Assert.Equal(0, settings.SelfDiskFreeWarnGb);

        config.Alerts.SelfDiskFreeWarnGb = 0;
        Assert.Equal(0, settings.SelfDiskFreeWarnGb);

        config.Alerts.SelfDiskFreeWarnGb = 400;
        Assert.Equal(400, settings.SelfDiskFreeWarnGb);
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
