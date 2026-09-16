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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V124 / #3466 (lane 2): the fleet sweep's cadence knobs move onto the singleton
/// <c>config_alert_settings</c> row — <c>fleet_sweep_enabled</c> and
/// <c>fleet_sweep_interval_minutes</c>, the spec's "user-configured cadence (hourly by default)" as an
/// operator knob from birth rather than a constant that graduates later the way #3297 and #3444's had
/// to. The chosen home is the analysis cadence's exact path (control-plane Stage 1): the settings row,
/// the wholesale config swap, <c>get_alert_settings</c>/<c>update_alert_settings</c>, and the Viewer's
/// Settings window — with the one deliberate difference that the sweep's switch is NOT the alert
/// master switch's business, because sweeps under master-off are the muted-mode contract's whole
/// point.
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="FleetSweepStateRungTests"/> (V123) when this rung landed, the same handoff that file
/// received from <see cref="PgAlertCountKnobRungTests"/> (V122) — a fully-migrated store must map to
/// EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually
/// current.</para>
/// </summary>
public sealed class FleetSweepCadenceKnobRungTests
{
    private const int RungVersion = 124;
    private const int PreviousVersion = 123;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 99;

    private const string EnabledColumn = "fleet_sweep_enabled";
    private const string IntervalColumn = "fleet_sweep_interval_minutes";

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "fleet-sweep-cadence-knobs",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds both columns to the singleton settings row, schema-qualified, with the shared
    /// constants as their defaults — restated as literals in the rung only because a rung is a SQL
    /// string, and derived from the constants here so a moved default reds this pin. Enabled defaults
    /// TRUE (the V124 rung doc carries the shipping-on reasoning), and the interval default is the
    /// spec's own hourly.
    /// </summary>
    [Fact]
    public void TheRungAddsBothColumns_SchemaQualified_WithTheDefaultsTheConstantsShipAt()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Equal(2, CountOf(rung, "ALTER TABLE config.config_alert_settings"));
        Assert.DoesNotContain("ALTER TABLE config_alert_settings", rung, StringComparison.Ordinal);

        /* IF NOT EXISTS on both, so re-running the ladder over a store that already has them is a no-op
           rather than a 42701 that aborts the whole migration. */
        Assert.Equal(2, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));

        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {EnabledColumn} boolean NOT NULL DEFAULT TRUE;",
            rung, StringComparison.Ordinal);
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {IntervalColumn} integer NOT NULL DEFAULT "
            + FleetSweepCadence.DefaultIntervalMinutes.ToString(CultureInfo.InvariantCulture) + ";",
            rung, StringComparison.Ordinal);

        /* The default IS the spec's hourly, and it sits inside the write bounds — so the shipped value
           can never be one the write path refuses on its way back in. */
        Assert.Equal(60, FleetSweepCadence.DefaultIntervalMinutes);
        Assert.InRange(
            FleetSweepCadence.DefaultIntervalMinutes,
            FleetSweepCadence.IntervalMinutesFloor,
            FleetSweepCadence.IntervalMinutesCeiling);

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
        Assert.Contains($"column_name = '{EnabledColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasFleetSweepCadenceKnobs", viewer, StringComparison.Ordinal);

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

        /* And in the source, the arm sits ABOVE V123's — newest-first is the whole contract of that method —
           and returns this build's version rather than a literal that could drift from it. This is the
           textual half of the top-arm claim, inherited from FleetSweepStateRungTests the way that file
           inherited it from PgAlertCountKnobRungTests. */
        var v124 = viewer.IndexOf("if (hasFleetSweepCadenceKnobs)", StringComparison.Ordinal);
        var v123 = viewer.IndexOf("if (hasFleetSweepState)", StringComparison.Ordinal);
        Assert.True(v124 >= 0, "the viewer has no V124 sentinel arm — a fully-migrated store would map to 123");
        Assert.True(v123 >= 0, "the V123 arm is gone, so this pin is comparing against nothing");
        Assert.True(v124 < v123, "the V124 arm sits below V123's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v124..], StringComparison.Ordinal);
    }

    /* ---- every settings-row surface names both columns ------------------------------------------------ */

    /// <summary>
    /// EVERY surface that reads or writes the settings row names BOTH new columns — the defect class a
    /// knob rung is most exposed to, not the DDL: every one of these lists drives ordinals or parameter
    /// positions, so a column added to one and not the others re-maps reads and writes at once.
    /// </summary>
    [Fact]
    public void EverySettingsRowSurfaceNamesBothColumns()
    {
        var surfaces = new (string What, string Text)[]
        {
            ("service seed + read", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs")),
            ("viewer select + upsert", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs")),
            ("mcp read", PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertSettingsSelectSql),
            ("mcp report + accept", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains(EnabledColumn, text, StringComparison.Ordinal);
            Assert.Contains(IntervalColumn, text, StringComparison.Ordinal);
        }

        /* The service must READ them, not merely select them — ApplyToConfig replaces config.Alerts
           wholesale, so a selected-but-unread column resets the knob to the shipped default on every
           worker start. Both halves, because one of the two being read is what an off-by-one produces. */
        var service = surfaces.Single(s => s.What == "service seed + read").Text;
        Assert.Contains("FleetSweepEnabled = reader.GetBoolean(", service, StringComparison.Ordinal);
        Assert.Contains("FleetSweepIntervalMinutes = reader.GetInt32(", service, StringComparison.Ordinal);

        /* And the viewer's upsert must WRITE them, or Save silently drops whatever the boxes held. */
        Assert.Contains($"{EnabledColumn} = EXCLUDED.{EnabledColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains($"{IntervalColumn} = EXCLUDED.{IntervalColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MCP's ACCEPTED bound, the worker's read-side clamp and the Viewer's save gate are the SAME
    /// named constants, held STRUCTURALLY — all three sites name <see cref="FleetSweepCadence"/>'s floor
    /// and ceiling rather than carrying matching literals. The parity is the "setting did not stick"
    /// guard every knob on this row holds: a wider bound in any writer lets a value in that another
    /// surface then silently rewrites.
    /// </summary>
    [Fact]
    public void TheMcpWriteBound_TheWorkerClamp_AndTheViewerGate_AreTheSameConstants_NotMatchingLiterals()
    {
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        var engine = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "FleetSweepEngine.cs");
        var window = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");

        foreach (var (what, text) in new[]
                 {
                     ("engine clamp", engine), ("mcp write bound", tools), ("settings window gate", window),
                 })
        {
            Assert.True(
                CountOf(text, "FleetSweepCadence.IntervalMinutesFloor") >= 1,
                $"the {what} must reach the shared floor constant, not a literal");
            Assert.True(
                CountOf(text, "FleetSweepCadence.IntervalMinutesCeiling") >= 1,
                $"the {what} must reach the shared ceiling constant, not a literal");
        }

        /* The figures themselves, pinned once: the floor is the fastest cadence at which "two
           consecutive sweeps" still confirms a sustained condition (the constants' own doc), the
           ceiling is one sweep a day — the lane-4 rollup's grain. And the clamp is the constants
           applied, both directions. */
        Assert.Equal(15, FleetSweepCadence.IntervalMinutesFloor);
        Assert.Equal(1440, FleetSweepCadence.IntervalMinutesCeiling);
        Assert.Equal(FleetSweepCadence.IntervalMinutesFloor, FleetSweepEngine.ClampIntervalMinutes(1));
        Assert.Equal(FleetSweepCadence.IntervalMinutesFloor, FleetSweepEngine.ClampIntervalMinutes(FleetSweepCadence.IntervalMinutesFloor));
        Assert.Equal(90, FleetSweepEngine.ClampIntervalMinutes(90));
        Assert.Equal(FleetSweepCadence.IntervalMinutesCeiling, FleetSweepEngine.ClampIntervalMinutes(FleetSweepCadence.IntervalMinutesCeiling));
        Assert.Equal(FleetSweepCadence.IntervalMinutesCeiling, FleetSweepEngine.ClampIntervalMinutes(int.MaxValue));
    }

    /// <summary>
    /// Every surface needing the shipped default NAMES the constant rather than restating the number —
    /// the #3060 finding that a number copied into five places is four places that can be left behind.
    /// </summary>
    [Fact]
    public void EverySurfaceNeedingTheShippedDefault_NamesTheConstant()
    {
        var surfaces = new (string What, string Text)[]
        {
            ("AlertsConfig seed", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "DarlingConfig.cs")),
            ("viewer row initializer", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs")),
            ("Restore Defaults button", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs")),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains("FleetSweepCadence.DefaultIntervalMinutes", text, StringComparison.Ordinal);
        }

        /* And the values the surfaces resolve to. Asserted through the objects rather than through the
           source, so this half is a behaviour check rather than a second text scan. The enabled default
           is TRUE on both — see the V124 rung doc for why the feature does not ship dark. */
        Assert.Equal(FleetSweepCadence.DefaultIntervalMinutes, new AlertsConfig().FleetSweepIntervalMinutes);
        Assert.True(new AlertsConfig().FleetSweepEnabled);
        Assert.Equal(FleetSweepCadence.DefaultIntervalMinutes, AlertSettingsRow.Defaults().FleetSweepIntervalMinutes);
        Assert.True(AlertSettingsRow.Defaults().FleetSweepEnabled);
    }

    /* ─────────────────────── the seam actually reaches the schedule ─────────────────────── */

    /// <summary>
    /// The worker's launch gate reads the sweep's OWN switch and the CLAMPED cadence — and deliberately
    /// not the alert master switch, which is passed to the engine as a FACT for the document's mute
    /// header and the would-have-paged derivation, never consulted as a gate. That asymmetry is the
    /// muted-mode contract in one line: master-off silences delivery, it does not blind the report
    /// surface.
    /// </summary>
    [Fact]
    public void TheWorkerGatesOnTheSweepsOwnSwitch_AndClampsTheCadence_AndTheMasterSwitchIsDataNotAGate()
    {
        var worker = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        /* The gate is the sweep's own switch... */
        Assert.Contains("if (config.Alerts.FleetSweepEnabled", worker, StringComparison.Ordinal);

        /* ...the cadence is read ONCE through the shared clamp into the local both the stamp and the
           engine use... */
        Assert.Equal(1, CountOf(worker, "FleetSweepEngine.ClampIntervalMinutes(config.Alerts.FleetSweepIntervalMinutes)"));

        /* ...and the master switch rides into the engine as data (the run row's alerts_enabled header),
           on the launch call itself. */
        Assert.Contains("_fleetSweep = FleetSweepEngine.RunAsync(", worker, StringComparison.Ordinal);
        var launch = worker.IndexOf("_fleetSweep = FleetSweepEngine.RunAsync(", StringComparison.Ordinal);
        Assert.Contains("config.Alerts.Enabled", worker[launch..worker.IndexOf(';', launch)], StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer row round-trips through the bind at the APPENDED ordinals — the four-parallel-sequences
    /// trap the V119/V120/V122 files name, held for the one sequence no SQL text pin can see: the bind.
    /// </summary>
    [Fact]
    public void TheViewerRow_RoundTripsThroughTheBind_AtTheAppendedOrdinals()
    {
        var bind = typeof(ViewerDataService)
            .GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;

        var row = AlertSettingsRow.Defaults();
        /* Deliberately NOT the shipped values — a bind that dropped one column and shifted the rest
           would otherwise still present the right value at one of the two positions. */
        row.FleetSweepEnabled = false;
        row.FleetSweepIntervalMinutes = 240;

        using var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });

        /* The bind supplies exactly as many parameters as the upsert's highest placeholder. */
        var highestPlaceholder = System.Text.RegularExpressions.Regex
            .Matches(ViewerDataService.AlertSettingsUpsertSql, @"\$(\d+)")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .Max();
        Assert.Equal(command.Parameters.Count, highestPlaceholder);

        /* The two new columns ride at the END — appended, the rule every knob rung on this table follows,
           so every earlier ordinal keeps its column. */
        Assert.False(Assert.IsType<NpgsqlParameter<bool>>(command.Parameters[^2]).TypedValue);
        Assert.Equal(240, Assert.IsType<NpgsqlParameter<int>>(command.Parameters[^1]).TypedValue);
    }

    /// <summary>Non-overlapping occurrences of <paramref name="needle"/>.</summary>
    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var at = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (at >= 0)
        {
            count++;
            at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }
}
