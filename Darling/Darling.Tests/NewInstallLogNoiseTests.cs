/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9695: the decisions that keep a new install's log from reading like a store in trouble. Each is driven by its inputs.
/// </summary>
[Trait("Stage", "Guard")]
public class NewInstallLogNoiseTests
{
    private static readonly DateTime Now = new(2026, 11, 26, 12, 0, 0, DateTimeKind.Unspecified);

    private static PostgresException Pg(string sqlState)
        => new("boom", "ERROR", "ERROR", sqlState);

    [Theory]
    [InlineData(1, "4 days", true)]
    [InlineData(95, "4 days", true)]
    [InlineData(24 * 3, "4 days", true)]
    [InlineData(24 * 4, "4 days", true)]
    [InlineData(24 * 4 + 1, "4 days", false)]
    [InlineData(24 * 30, "4 days", false)]
    [InlineData(24 * 30, "90 days", true)]
    [InlineData(24 * 91, "90 days", false)]
    public void ARelationYoungerThanItsHorizonHasNothingToDrop(int oldestRowAgeHours, string dropAfter, bool nothingToDrop)
    {
        Assert.Equal(nothingToDrop, TimescaleSupport.NothingToDropYet(Now.AddHours(-oldestRowAgeHours), dropAfter, Now));
    }

    [Theory]
    [InlineData("1 mon")]
    [InlineData("4")]
    [InlineData("four days")]
    [InlineData("-4 days")]
    [InlineData("")]
    public void AHorizonThatDoesNotReadAsWholeDaysKeepsTheHoldAtItsWarning(string dropAfter)
    {
        Assert.False(TimescaleSupport.NothingToDropYet(Now.AddHours(-1), dropAfter, Now));
    }

    [Fact]
    public void AnUnknownOldestRowKeepsTheHoldAtItsWarning()
    {
        Assert.False(TimescaleSupport.NothingToDropYet(null, "4 days", Now));
    }

    [Theory]
    [InlineData(24 * 4 * 60 + 1, true)]
    [InlineData(24 * 4 * 60 + 30, true)]
    [InlineData(24 * 4 * 60 + 120, true)]
    [InlineData(24 * 4 * 60 + 121, false)]
    [InlineData(24 * 4 * 60, false)]
    [InlineData(24 * 3 * 60, false)]
    [InlineData(24 * 30 * 60, false)]
    public void AHeldRelationWarnsOnThePassesRightAfterItsOldestRowAgesPastTheHorizon(int oldestRowAgeMinutes, bool crossed)
    {
        Assert.Equal(crossed, TimescaleSupport.CrossedHorizonRecently(Now.AddMinutes(-oldestRowAgeMinutes), "4 days", Now));
    }

    [Fact]
    public void AnUnknownOldestRowOrAnUnreadableHorizonIsNeverACrossing()
    {
        Assert.False(TimescaleSupport.CrossedHorizonRecently(null, "4 days", Now));
        Assert.False(TimescaleSupport.CrossedHorizonRecently(Now.AddDays(-4).AddMinutes(-30), "1 mon", Now));
    }

    [Fact]
    public void TheCrossingWindowSpansTwoHourlyPasses()
    {
        Assert.Equal(TimeSpan.FromHours(2), TimescaleSupport.HorizonCrossingWindow);
    }

    [Fact]
    public void EveryRetentionPolicyHorizonReadsAsWholeDays()
    {
        /* A policy whose horizon the check cannot read would keep the Warning on a new store without anyone noticing. */
        foreach (var (relation, dropAfter, _, _) in TimescaleSupport.RetentionPolicies)
        {
            Assert.True(
                TimescaleSupport.NothingToDropYet(Now, dropAfter, Now),
                $"{relation}: a row written now is within its '{dropAfter}' horizon, and the check does not read that horizon");
        }
    }

    [Fact]
    public void AStoreSideCaptureFailureWarnsOncePerCause()
    {
        var notice = new StoreLogCaptureNotice();

        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
        Assert.False(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
        Assert.False(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
    }

    [Fact]
    public void ADifferentCaptureCauseWarnsAgain()
    {
        var notice = new StoreLogCaptureNotice();

        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.InsufficientPrivilege)));
        Assert.False(notice.ShouldWarn(Pg(PostgresErrorCodes.InsufficientPrivilege)));

        /* The operator granted the read and the store then reported it keeps no log folder: that is a new fact. */
        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
        Assert.False(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
    }

    [Fact]
    public void ACaptureThatWorkedMakesTheNextFailureNewsAgain()
    {
        var notice = new StoreLogCaptureNotice();

        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
        notice.Succeeded();
        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
        Assert.False(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
    }

    [Fact]
    public void AFaultTheStoreDoesNotCauseWarnsEveryTimeAndDoesNotResetTheCauseAlreadyReported()
    {
        var notice = new StoreLogCaptureNotice();

        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.QueryCanceled)));
        Assert.True(notice.ShouldWarn(new InvalidOperationException("x")));
        Assert.True(notice.ShouldWarn(Pg(PostgresErrorCodes.QueryCanceled)));
        Assert.False(notice.ShouldWarn(Pg(PostgresErrorCodes.UndefinedFile)));
    }

    [Fact]
    public void AStoreThatHasNeverStartedGetsOnePlainSentenceForItsMissingRegistry()
    {
        var note = DarlingCliCommands.RegistryReadFailureNote(
            new PostgresException("relation \"config_monitored_servers\" does not exist", "ERROR", "ERROR", PostgresErrorCodes.UndefinedTable),
            "validating darling.json's own server list instead.");

        Assert.Equal("NOTE: the store has no server registry yet, so validating darling.json's own server list instead.", note);
        Assert.DoesNotContain("42P01", note, StringComparison.Ordinal);
        Assert.DoesNotContain("POSITION", note, StringComparison.Ordinal);
    }

    [Fact]
    public void AnyOtherRegistryReadFailureKeepsItsOwnText()
    {
        var deniedException = Pg(PostgresErrorCodes.InsufficientPrivilege);
        var denied = DarlingCliCommands.RegistryReadFailureNote(deniedException, "validating darling.json's own server list instead.");
        var broken = DarlingCliCommands.RegistryReadFailureNote(
            new InvalidOperationException("the connection is closed"), "validating darling.json's own server list instead.");

        Assert.Equal($"NOTE: could not read the store's registry ({deniedException.Message}), so validating darling.json's own server list instead.", denied);
        Assert.Contains("42501", denied, StringComparison.Ordinal);
        Assert.Equal("NOTE: could not read the store's registry (the connection is closed), so validating darling.json's own server list instead.", broken);
    }
}
