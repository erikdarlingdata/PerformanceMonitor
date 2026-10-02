/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4961: the on-premises counterpart of the Azure co-owner rule. Where the session is the server's own, two registrations
/// of one install can reach the same instance under different host names, so the instance's last-known
/// <c>@@SERVERNAME</c> decides whether one registration's drop would stop the trace another one keeps. These pin the pure
/// decision both apps share, and the read of the name from a carrier's persisted identity row.
/// </summary>
public sealed class LongQueryTraceInstanceGuardTests
{
    private static LongQueryTraceInstance Other(string? name, bool enabled = true, bool traceOn = true) => new(enabled, traceOn, name);

    [Fact]
    public void AnotherRegistrationOfTheSameInstance_WithItsTraceOn_KeepsTheSession()
    {
        Assert.True(LongQueryTraceDatabases.KeptOnInstance("SQL01", new[] { Other("SQL01") }));
    }

    [Fact]
    public void TheNamesCompareIgnoringCaseAndPadding()
    {
        Assert.True(LongQueryTraceDatabases.KeptOnInstance(" sql01 ", new[] { Other("SQL01 ") }));
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public void ARegistrationThatIsNotMonitoredOrHasItsTraceOff_KeepsNothing(bool enabled, bool traceOn)
    {
        Assert.False(LongQueryTraceDatabases.KeptOnInstance("SQL01", new[] { Other("SQL01", enabled, traceOn) }));
    }

    [Fact]
    public void ARegistrationOfAnotherInstance_KeepsNothing()
    {
        Assert.False(LongQueryTraceDatabases.KeptOnInstance("SQL01", new[] { Other("SQL02") }));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void AnUnknownName_MatchesNothing_OnEitherSide(string? unknown)
    {
        Assert.False(LongQueryTraceDatabases.KeptOnInstance(unknown, new[] { Other("SQL01") }));
        Assert.False(LongQueryTraceDatabases.KeptOnInstance("SQL01", new[] { Other(unknown) }));
        Assert.False(LongQueryTraceDatabases.KeptOnInstance(unknown, new[] { Other(unknown) }));
    }

    [Fact]
    public void NoOtherRegistration_KeepsNothing()
    {
        Assert.False(LongQueryTraceDatabases.KeptOnInstance("SQL01", new List<LongQueryTraceInstance>()));
    }

    [Fact]
    public void ThePersistedIdentityRow_GivesItsName()
    {
        var stamp = new ServerEpoch.Stamp(new System.DateTime(2026, 10, 2, 8, 0, 0, System.DateTimeKind.Utc), "SQL01");
        var state = new Dictionary<string, string> { [ServerEpoch.IdentityStateKey] = ServerEpoch.Serialize(stamp) };

        Assert.Equal("SQL01", ServerEpoch.LastKnownName(state));
    }

    [Fact]
    public void AnIdentityRowWithNoName_AndNoRow_GiveNull()
    {
        var noName = new Dictionary<string, string>
        {
            [ServerEpoch.IdentityStateKey] = ServerEpoch.Serialize(new ServerEpoch.Stamp(new System.DateTime(2026, 10, 2, 8, 0, 0, System.DateTimeKind.Utc), null)),
        };

        Assert.Null(ServerEpoch.LastKnownName(noName));
        Assert.Null(ServerEpoch.LastKnownName(new Dictionary<string, string>()));
        Assert.Null(ServerEpoch.LastKnownName(new Dictionary<string, string> { [ServerEpoch.IdentityStateKey] = "not an identity" }));
    }

    [Fact]
    public void TheCarriers_AreTheCollectorsThatPersistTheIdentity()
    {
        Assert.Equal(new[] { WaitStatsCollector.Instance.Name, CpuUtilizationCollector.Instance.Name }, ServerEpoch.IdentityCarrierCollectors);
    }
}
