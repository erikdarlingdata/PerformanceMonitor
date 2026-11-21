/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9270: how <see cref="ConfigChangeDiff.DiffDatabaseConfigChanges"/> treats a run of transitional captures (RESTORING, RECOVERING,
/// RECOVERY_PENDING) of a database, and the stateless recapture rule <see cref="TransitionalDatabaseRuns.RecaptureIsDue"/> both products
/// share. Pure: no store.
/// </summary>
public sealed class TransitionalDatabaseRunsTests
{
    private static readonly DateTime T0 = new(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc);
    private static readonly int Snapshot = ConfigChangeDiff.DatabaseConfigChangeSettingNames.ToList().IndexOf("snapshot_isolation_state");

    private static ConfigChangeDiff.DatabaseConfigSnapshot Config(int minutes, string database, string state, string snapshotIsolation)
    {
        var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
        values[0] = state;
        values[Snapshot] = snapshotIsolation;
        return new ConfigChangeDiff.DatabaseConfigSnapshot(T0.AddMinutes(minutes), database, values);
    }

    private static List<ConfigChangeDiff.DatabaseConfigChange> Diff(params ConfigChangeDiff.DatabaseConfigSnapshot[] snapshots) =>
        ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0.AddDays(-1), T0.AddDays(1));

    [Fact]
    public void ARunHeldFiftyMinutesWithNoLaterCapture_GivesOneStateRow_AndNoOtherSetting()
    {
        var changes = Diff(
            Config(0, "Sales", "ONLINE", "ON"),
            Config(10, "Sales", "RECOVERY_PENDING", "OFF"),
            Config(60, "Sales", "RECOVERY_PENDING", "OFF"));

        var change = Assert.Single(changes);
        Assert.Equal("state_desc", change.SettingName);
        Assert.Equal("ONLINE", change.OldValue);
        Assert.Equal("RECOVERY_PENDING", change.NewValue);
        Assert.Equal(T0.AddMinutes(60), change.ChangeTime);
        Assert.Equal(T0, change.PreviousCaptureTime);
    }

    [Fact]
    public void ARunHeldTwentyMinutes_GivesNoRow()
    {
        Assert.Empty(Diff(
            Config(0, "Sales", "ONLINE", "ON"),
            Config(10, "Sales", "RECOVERY_PENDING", "OFF"),
            Config(30, "Sales", "RECOVERY_PENDING", "OFF")));
    }

    [Fact]
    public void ARunThatEndsBeforeItIsStuck_GivesNoRow_AndTheOtherSettingsCompareSettledToSettled()
    {
        var changes = Diff(
            Config(0, "Sales", "ONLINE", "OFF"),
            Config(10, "Sales", "RESTORING", "OFF"),
            Config(20, "Sales", "ONLINE", "ON"));

        var change = Assert.Single(changes);
        Assert.Equal("snapshot_isolation_state", change.SettingName);
        Assert.Equal("OFF", change.OldValue);
        Assert.Equal("ON", change.NewValue);
    }

    [Fact]
    public void AStuckRunThatEnds_GivesTwoStateRows_AndTheOtherSettingsCompareSettledToSettled()
    {
        var changes = Diff(
            Config(0, "Sales", "ONLINE", "ON"),
            Config(10, "Sales", "RESTORING", "OFF"),
            Config(60, "Sales", "RESTORING", "OFF"),
            Config(120, "Sales", "ONLINE", "ON"));

        var states = changes.Where(c => c.SettingName == "state_desc").OrderBy(c => c.ChangeTime).ToList();
        Assert.Equal(2, states.Count);
        Assert.Equal(("ONLINE", "RESTORING", T0.AddMinutes(60)), (states[0].OldValue, states[0].NewValue, states[0].ChangeTime));
        Assert.Equal(("RESTORING", "ONLINE", T0.AddMinutes(120)), (states[1].OldValue, states[1].NewValue, states[1].ChangeTime));
        Assert.Equal(2, changes.Count);
    }

    [Fact]
    public void AStuckRunThatEnds_StillReportsARealChangeAcrossIt()
    {
        var changes = Diff(
            Config(0, "Sales", "ONLINE", "OFF"),
            Config(10, "Sales", "RESTORING", "OFF"),
            Config(60, "Sales", "RESTORING", "OFF"),
            Config(120, "Sales", "ONLINE", "ON"));

        Assert.Contains(changes, c => c.SettingName == "snapshot_isolation_state" && c.OldValue == "OFF" && c.NewValue == "ON");
        Assert.Equal(3, changes.Count);
    }

    [Fact]
    public void ADatabaseRestoringInEveryCapture_GivesNoRows()
    {
        Assert.Empty(Diff(
            Config(0, "Standby", "RESTORING", "OFF"),
            Config(60, "Standby", "RESTORING", "OFF"),
            Config(120, "Standby", "RESTORING", "OFF")));
    }

    [Fact]
    public void ADatabaseTransitionalSinceFirstSeen_ThenSettled_GivesNoRowForTheRun()
    {
        Assert.Empty(Diff(
            Config(0, "Standby", "RESTORING", "OFF"),
            Config(60, "Standby", "RESTORING", "OFF"),
            Config(120, "Standby", "ONLINE", "ON")));
    }

    [Fact]
    public void AStuckRow_IsStampedAtTheCaptureThatMadeItStuck_AndTheWindowRuleApplies()
    {
        var snapshots = new[]
        {
            Config(0, "Sales", "ONLINE", "ON"),
            Config(10, "Sales", "RECOVERY_PENDING", "OFF"),
            Config(60, "Sales", "RECOVERY_PENDING", "OFF"),
        };

        Assert.Empty(ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0.AddMinutes(60), T0.AddDays(1)));
        Assert.Empty(ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0.AddDays(-1), T0.AddMinutes(59)));
        Assert.Single(ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0.AddMinutes(59), T0.AddMinutes(60)));
    }

    [Fact]
    public void TheDatabasesAreJudgedOneByOne()
    {
        var changes = Diff(
            Config(0, "Sales", "ONLINE", "ON"),
            Config(0, "Other", "ONLINE", "ON"),
            Config(10, "Sales", "RESTORING", "OFF"),
            Config(10, "Other", "ONLINE", "ON"),
            Config(60, "Sales", "RESTORING", "OFF"),
            Config(60, "Other", "ONLINE", "ON"));

        Assert.Equal("Sales", Assert.Single(changes).DatabaseName);
    }

    private static TransitionalDatabaseRuns.Capture Seen(int minutes, string database, string state) => new(T0.AddMinutes(minutes), database, state);

    [Fact]
    public void ANewlyTransitionalDatabase_IsDueEarly()
    {
        Assert.True(TransitionalDatabaseRuns.RecaptureIsDue([Seen(0, "Sales", "ONLINE"), Seen(10, "Sales", "RESTORING")]));
    }

    [Fact]
    public void TheSeventhCaptureOfARun_IsNotDueEarly_AndTheSixthIs()
    {
        var captures = new List<TransitionalDatabaseRuns.Capture> { Seen(0, "Sales", "ONLINE") };
        for (var i = 1; i <= TransitionalDatabaseRuns.MaxRecaptures; i++)
        {
            captures.Add(Seen(i * 10, "Sales", "RESTORING"));
            Assert.True(TransitionalDatabaseRuns.RecaptureIsDue(captures), "capture " + i + " of the run");
        }

        captures.Add(Seen((TransitionalDatabaseRuns.MaxRecaptures + 1) * 10, "Sales", "RESTORING"));
        Assert.False(TransitionalDatabaseRuns.RecaptureIsDue(captures));
    }

    [Fact]
    public void ADatabaseTransitionalSinceFirstSeen_IsNotDueEarly()
    {
        Assert.False(TransitionalDatabaseRuns.RecaptureIsDue([Seen(0, "Standby", "RESTORING")]));
    }

    [Fact]
    public void ASettledNewestCapture_IsNotDueEarly_AndNoCapturesIsNot()
    {
        Assert.False(TransitionalDatabaseRuns.RecaptureIsDue([Seen(0, "Sales", "RESTORING"), Seen(10, "Sales", "ONLINE")]));
        Assert.False(TransitionalDatabaseRuns.RecaptureIsDue([]));
    }

    [Fact]
    public void ARestartWithAStuckDatabasePresent_IsStillDueEarly_ForAnotherDatabaseNewlyRecovering()
    {
        var captures = new List<TransitionalDatabaseRuns.Capture>
        {
            Seen(-1000, "Stuck", "ONLINE"), Seen(-900, "Stuck", "RECOVERY_PENDING"),
            Seen(0, "Stuck", "RECOVERY_PENDING"), Seen(0, "Other", "ONLINE"),
            Seen(10, "Stuck", "RECOVERY_PENDING"), Seen(10, "Other", "RECOVERING"),
        };
        for (var i = 0; i < TransitionalDatabaseRuns.MaxRecaptures + 2; i++)
        {
            captures.Add(Seen(-800 + i, "Stuck", "RECOVERY_PENDING"));
        }

        Assert.True(TransitionalDatabaseRuns.RecaptureIsDue(captures));
    }

    [Fact]
    public void ADatabaseMissingFromTheNewestCapture_DoesNotHoldTheCollectorEarly()
    {
        Assert.False(TransitionalDatabaseRuns.RecaptureIsDue(
            [Seen(0, "Gone", "ONLINE"), Seen(10, "Gone", "RESTORING"), Seen(20, "Sales", "ONLINE")]));
    }
}
