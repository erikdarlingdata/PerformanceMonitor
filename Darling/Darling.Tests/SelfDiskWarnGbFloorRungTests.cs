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
using Npgsql;
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
/// <para>The "I am the top rung" claims have moved ON to <see cref="DeltaFamilyIntervalColumnsRungTests"/>
/// (V127), the same handoff this file received from <see cref="CollectorDatabaseScopeRungTests"/> (V125)
/// and that file received from <see cref="FleetSweepCadenceKnobRungTests"/> (V124). What stays here is
/// everything true of this rung wherever it sits in the ladder; what left is every claim that was really
/// about being NEWEST — keeping a copy of those would assert this rung is still the top, which is how the
/// NEXT rung's build goes red.</para>
/// </summary>
public sealed class SelfDiskWarnGbFloorRungTests
{
    private const int RungVersion = 126;
    private const int PreviousVersion = 125;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V127
    /// appended its own — so this is a position within the signature rather than its end.</summary>
    private const int ProbeOrdinal = 101;

    private const string FloorColumn = "self_disk_free_warn_gb";

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "self-disk-warn-gb-floor",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which
           stopped being true when V127 landed. The invariant that outlives the handoff is that the
           LADDER's top and the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);

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
    /// The viewer probe's three sites carry this rung's sentinel, and a store that stopped here maps to it.
    ///
    /// <para>The probe asks the question, the caller reads the answer, the map has the parameter — three
    /// sites, and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column.
    /// The top-arm claims (last argument, textual newest-first ordering) moved to
    /// <see cref="DeltaFamilyIntervalColumnsRungTests"/> with V127.</para>
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains($"column_name = '{FloorColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasSelfDiskWarnGbFloor", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is
           the NEWEST sentinel, which stopped being true the moment V127 appended its own. Strictly-less is
           the form every other non-top rung's test here uses. */
        Assert.True(ProbeOrdinal < arity - 1);

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

        /* And in the source, the arm sits ABOVE V125's — newest-first is the whole contract of that method.
           It returns this rung's own literal now, not the build's version: the "returns
           StorageVersion.SchemaVersion" half of the top-arm claim moved to V127's test with the top. */
        var v126 = viewer.IndexOf("if (hasSelfDiskWarnGbFloor)", StringComparison.Ordinal);
        var v125 = viewer.IndexOf("if (hasCollectorScheduleDatabases)", StringComparison.Ordinal);
        Assert.True(v126 >= 0, "the viewer has no V126 sentinel arm — a store that stopped here would map to 125");
        Assert.True(v125 >= 0, "the V125 arm is gone, so this pin is comparing against nothing");
        Assert.True(v126 < v125, "the V126 arm sits below V125's, so a V126 store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v126..], StringComparison.Ordinal);
    }

    /* ---- every settings-row surface handles the column ------------------------------------------------ */

    /// <summary>
    /// EVERY surface that reads or writes the settings row names the column — the viewer INCLUDED. The
    /// wired lists drive ordinals or parameter positions, so a column added to one and not the others
    /// re-maps reads and writes at once.
    ///
    /// <para><b>The viewer's select/upsert was this rung's pinned abstinence</b>, unlike every earlier knob
    /// rung: the knob landed backend-first (store plane + the two MCP tools), and this assertion pinned
    /// <c>DoesNotContain</c> until the viewer pass (#3563) wired the Settings window's box — the flip that
    /// paragraph promised. What the abstinence protected still holds now that it is wired: the viewer's
    /// explicit column lists mean a Save writes the floor rather than nulling it out.</para>
    /// </summary>
    [Fact]
    public void EverySettingsRowSurfaceNamesTheColumn_TheViewerIncluded()
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

        /* The viewer pass (#3563): the viewer's select reads the column, its upsert WRITES it (or Save
           silently drops whatever the box held), and its reader maps the appended ordinal. */
        var viewerSettings = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs");
        Assert.Contains(FloorColumn, ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains(
            $"{FloorColumn} = EXCLUDED.{FloorColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains("SelfDiskFreeWarnGb = reader.GetInt32(66)", viewerSettings, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Settings window's box (#3563): prefilled from the row, saved through the same bound the MCP
    /// writer accepts and the read-side clamp keeps (<c>&gt;= 0</c> — 0 removes the floor), following the
    /// alerts master switch like every #2107 sibling, and Restore Defaults writes the shipped constant.
    ///
    /// <para><b>The viewer restates the shipped default as a literal</b> — in the row initializer and the
    /// Restore Defaults button — because <c>DarlingSelfAlertEvaluator</c> lives on the Service assembly the
    /// viewer does not reference (its percent sibling's "10" has the same shape). Both literals are pinned
    /// equal to the constant here, the same equality the rung SQL's own default carries, so a moved shipped
    /// default cannot leave the window handing out a floor no other surface reports.</para>
    /// </summary>
    [Fact]
    public void TheSettingsWindowBox_PrefillsSavesGatesAndRestores_AtTheSharedBoundAndDefault()
    {
        var window = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");
        var xaml = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml");

        /* The box exists, one knob over from its percent sibling. */
        Assert.Contains("x:Name=\"AlertSelfDiskWarnGbBox\"", xaml, StringComparison.Ordinal);

        /* Prefill, save gate (the [0, int.MaxValue) bound's floor), and the master-switch follow. */
        Assert.Contains("AlertSelfDiskWarnGbBox.Text = r.SelfDiskFreeWarnGb.ToString(", window, StringComparison.Ordinal);
        Assert.Contains(
            "if (int.TryParse(AlertSelfDiskWarnGbBox.Text, out var selfDiskGb) && selfDiskGb >= 0)",
            window, StringComparison.Ordinal);
        Assert.Contains("row.SelfDiskFreeWarnGb = selfDiskGb;", window, StringComparison.Ordinal);
        Assert.Contains("AlertSelfDiskWarnGbBox.IsEnabled = enabled;", window, StringComparison.Ordinal);

        /* Restore Defaults writes the shipped figure, and the viewer row seeds it — both as literals
           pinned equal to the constant. */
        var shipped = ((int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb).ToString(CultureInfo.InvariantCulture);
        Assert.Contains($"AlertSelfDiskWarnGbBox.Text = \"{shipped}\";", window, StringComparison.Ordinal);
        Assert.Equal((int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb, AlertSettingsRow.Defaults().SelfDiskFreeWarnGb);
        Assert.Equal((int)DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb, new AlertsConfig().SelfDiskFreeWarnGb);
    }

    /// <summary>
    /// The viewer row round-trips through the bind at the APPENDED ordinal — the four-parallel-sequences
    /// trap (the column list, the upsert's $N placeholders, the bind order, and the reader ordinals),
    /// held for the one sequence no SQL text pin can see: the bind. The V124 rung's shape, one knob on.
    /// </summary>
    [Fact]
    public void TheViewerRow_RoundTripsThroughTheBind_AtTheAppendedOrdinal()
    {
        var bind = typeof(ViewerDataService)
            .GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;

        var row = AlertSettingsRow.Defaults();
        /* Deliberately NOT the shipped 50 — a bind that dropped the column and fell back to the default
           would otherwise still present the right value at the position. 75 is the MCP round-trip test's
           sample, for the same reason. */
        row.SelfDiskFreeWarnGb = 75;

        using var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });

        /* The bind supplies exactly as many parameters as the upsert's highest placeholder. */
        var highestPlaceholder = System.Text.RegularExpressions.Regex
            .Matches(ViewerDataService.AlertSettingsUpsertSql, @"\$(\d+)")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .Max();
        Assert.Equal(command.Parameters.Count, highestPlaceholder);

        /* The new column rides at the END — appended, the rule every knob rung on this table follows,
           so every earlier ordinal keeps its column. */
        Assert.Equal(75, Assert.IsType<NpgsqlParameter<int>>(command.Parameters[^1]).TypedValue);
    }

    /* ---- the seam reaches the gate -------------------------------------------------------------------- */

    /// <summary>
    /// The settings adapter defaults to the shipped constant and clamps a hand-edited store value at the
    /// 0 floor — the raw-in/clamped-out split every knob on this table uses, with 0 IN range because it
    /// removes the floor (the <c>pvs_floor_gb</c> reading) rather than being nonsense. The write bound in
    /// <see cref="EverySettingsRowSurfaceNamesTheColumn_TheViewerIncluded"/> is the same
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
