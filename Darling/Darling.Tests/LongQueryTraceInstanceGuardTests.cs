/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
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

    /* A removed server is checked once and never again, so its session stays whenever the match cannot be ruled out: its own
       name or the name of a registration that keeps the trace on is not known. The trace-off drop asks Kept instead, and an
       unknown name there matches nothing. */

    [Fact]
    public void TheRemoval_DropsTheSession_WhenNoOtherRegistrationCouldKeepIt_WhateverTheNames()
    {
        Assert.Null(LongQueryTraceInstanceGuard.NoKeepers.RemovalSkipReason());
        Assert.Null(new LongQueryTraceInstanceGuard(null, new[] { Other(null, enabled: false), Other(null, traceOn: false) }).RemovalSkipReason());
        Assert.Null(new LongQueryTraceInstanceGuard("SQL01", new[] { Other(null, enabled: false), Other(null, traceOn: false) }).RemovalSkipReason());
    }

    [Fact]
    public void TheRemoval_LeavesTheSession_WhenItsOwnNameIsNotKnown_AndAnotherRegistrationKeepsTheTraceOn()
    {
        var guard = new LongQueryTraceInstanceGuard(null, new[] { Other("SQL01") });

        Assert.Contains("this server's instance name is not known", guard.RemovalSkipReason(), System.StringComparison.Ordinal);
        Assert.False(guard.Kept);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void TheRemoval_LeavesTheSession_WhenAKeepersNameIsNotKnown_BecauseItCannotBeRuledOut(string? unknown)
    {
        var guard = new LongQueryTraceInstanceGuard("SQL01", new[] { Other(unknown) });

        Assert.Contains("another registration", guard.RemovalSkipReason(), System.StringComparison.Ordinal);
        Assert.Contains("not known", guard.RemovalSkipReason(), System.StringComparison.Ordinal);

        /* The trace-off drop is not the removal: a name that is not known matches nothing there, and it drops as it always did. */
        Assert.False(guard.Kept);
    }

    [Fact]
    public void TheRemoval_LeavesTheSession_WhenOneKeeperIsAnotherInstance_AndAnotherKeepersNameIsNotKnown()
    {
        var guard = new LongQueryTraceInstanceGuard("SQL01", new[] { Other("SQL02"), Other(null) });

        Assert.NotNull(guard.RemovalSkipReason());
    }

    [Fact]
    public void TheRemoval_IgnoresTheUnknownNameOfARegistrationThatCouldNotKeepTheSession()
    {
        var guard = new LongQueryTraceInstanceGuard("SQL01", new[] { Other("SQL02"), Other(null, enabled: false), Other(null, traceOn: false) });

        Assert.Null(guard.RemovalSkipReason());
    }

    [Fact]
    public void TheRemoval_DropsTheSession_WhenEveryKeepersKnownNameIsAnotherInstance()
    {
        Assert.Null(new LongQueryTraceInstanceGuard("SQL01", new[] { Other("SQL02"), Other("SQL03") }).RemovalSkipReason());
    }

    [Fact]
    public void TheRemoval_SaysSo_WhenAnotherRegistrationKeepsTheSessionOnTheSameInstance()
    {
        var guard = new LongQueryTraceInstanceGuard("sql01", new[] { Other("SQL02"), Other(" SQL01 ") });

        Assert.Contains("keeps it on the same instance", guard.RemovalSkipReason(), System.StringComparison.Ordinal);
        Assert.True(guard.Kept);
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

    /* ── The guard the worker builds for one registration (#4961) ── */

    private const int OwnId = 4944;

    private static MonitoredServer Registration(int id, string engine = "sqlserver") =>
        new() { Name = "registration-" + id, Host = "host-" + id, Engine = engine, StoredServerId = id };

    /// <summary>
    /// The persisted identity rows of a few registrations, read the way the store's reader gives them: one carrier's state at
    /// a time. Every read is counted, with the registration it was for.
    /// </summary>
    private sealed class Names
    {
        private readonly Dictionary<(int Id, string Carrier), string> _held = new();

        public List<int> Reads { get; } = new();

        public Names Hold(int id, string name, string? carrier = null)
        {
            _held[(id, carrier ?? ServerEpoch.IdentityCarrierCollectors[0])] = name;
            return this;
        }

        public System.Threading.Tasks.Task<Dictionary<string, string>> ReadAsync(int id, string carrier)
        {
            Reads.Add(id);
            var state = new Dictionary<string, string>();
            if (_held.TryGetValue((id, carrier), out var name))
            {
                state[ServerEpoch.IdentityStateKey] = ServerEpoch.Serialize(
                    new ServerEpoch.Stamp(new System.DateTime(2026, 10, 2, 8, 0, 0, System.DateTimeKind.Utc), name));
            }

            return System.Threading.Tasks.Task.FromResult(state);
        }
    }

    private static System.Threading.Tasks.Task<LongQueryTraceInstanceGuard> GuardFor(IReadOnlyList<MonitoredServer>? live, Names names, int[]? traceOff = null) =>
        DarlingWorker.LongQueryTraceInstanceGuardFor(OwnId, live, id => traceOff is null || !traceOff.Contains(id), names.ReadAsync);

    /// <summary>A SQL Server registration whose trace is on, on the instance this registration's own name says, keeps the session, and both names were read.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_ASqlServerRegistrationWithItsTraceOn_IsAKeeper_WhenBothNamesAreKnown()
    {
        var names = new Names().Hold(OwnId, "SQL01").Hold(101, "sql01");

        var guard = await GuardFor(new[] { Registration(101) }, names);

        Assert.Equal("SQL01", guard.ServerName);
        var keeper = Assert.Single(guard.Keepers);
        Assert.Equal(new LongQueryTraceInstance(Enabled: true, TraceOn: true, "sql01"), keeper);
        Assert.True(guard.Kept);
        Assert.Contains(101, names.Reads);
    }

    /// <summary>A PostgreSQL registration holds no Extended Events session: it is not a keeper, even with its trace on, and its state is never read.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_APostgresRegistration_IsNotAKeeper_AndNothingIsRead()
    {
        var names = new Names().Hold(OwnId, "SQL01").Hold(101, "SQL01");

        var guard = await GuardFor(new[] { Registration(101, engine: "postgres") }, names);

        Assert.Empty(guard.Keepers);
        Assert.False(guard.Kept);
        Assert.Empty(names.Reads);
    }

    /// <summary>A registration whose effective trace setting is off is not a keeper, whatever name it last reported.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_ARegistrationWithItsTraceOff_IsNotAKeeper_AndNothingIsRead()
    {
        var names = new Names().Hold(OwnId, "SQL01").Hold(101, "SQL01");

        var guard = await GuardFor(new[] { Registration(101) }, names, traceOff: new[] { 101 });

        Assert.Empty(guard.Keepers);
        Assert.False(guard.Kept);
        Assert.Empty(names.Reads);
    }

    /// <summary>
    /// The live registry holds only the monitored servers, so a registration that is disabled is not in it and keeps nothing.
    /// With no registry published yet, or only this registration in it, there is no other registration to keep the session.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_WithNoOtherRegistrationInTheRegistry_HasNoKeepers_AndReadsNothing()
    {
        var names = new Names().Hold(OwnId, "SQL01");

        Assert.Same(LongQueryTraceInstanceGuard.NoKeepers, await GuardFor(null, names));
        Assert.Same(LongQueryTraceInstanceGuard.NoKeepers, await GuardFor(System.Array.Empty<MonitoredServer>(), names));
        Assert.Same(LongQueryTraceInstanceGuard.NoKeepers, await GuardFor(new[] { Registration(OwnId) }, names));
        Assert.Empty(names.Reads);
    }

    /// <summary>With no name of its own, this registration matches nothing, so the names of the others are not read.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_WithNoNameOfItsOwn_ReadsNoKeepersName_AndMatchesNothing()
    {
        var names = new Names().Hold(101, "SQL01");

        var guard = await GuardFor(new[] { Registration(101) }, names);

        Assert.Null(guard.ServerName);
        Assert.False(guard.Kept);
        Assert.All(names.Reads, id => Assert.Equal(OwnId, id));
        Assert.NotEmpty(names.Reads);

        /* The keeper is still listed, with no name, for a caller that must rule it out. */
        Assert.Equal(new LongQueryTraceInstance(Enabled: true, TraceOn: true, ServerName: null), Assert.Single(guard.Keepers));
    }

    /// <summary>A keeper that has reported no name yet is not a match, and a keeper of another instance is not one either.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_AKeeperWithNoNameYet_OrAnotherInstance_IsNotAMatch()
    {
        var names = new Names().Hold(OwnId, "SQL01").Hold(102, "SQL02");

        var guard = await GuardFor(new[] { Registration(101), Registration(102) }, names);

        Assert.Equal(2, guard.Keepers.Count);
        Assert.False(guard.Kept);
        Assert.Null(guard.Keepers[0].ServerName);
        Assert.Equal("SQL02", guard.Keepers[1].ServerName);
    }

    /// <summary>The name comes from the first carrier that holds one, so a name only the second carrier holds still matches.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_ANameOnlyTheSecondCarrierHolds_StillMatches()
    {
        var second = ServerEpoch.IdentityCarrierCollectors[1];
        var names = new Names().Hold(OwnId, "SQL01", second).Hold(101, "SQL01", second);

        var guard = await GuardFor(new[] { Registration(101) }, names);

        Assert.True(guard.Kept);
    }

    /// <summary>Only the registrations whose trace is on are candidates: the others' names are never read.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheGuard_ReadsOnlyTheNamesOfTheRegistrationsThatCouldKeepTheSession()
    {
        var names = new Names().Hold(OwnId, "SQL01").Hold(101, "SQL01").Hold(102, "SQL01").Hold(103, "SQL01");

        var guard = await GuardFor(
            new[] { Registration(101), Registration(102, engine: "postgres"), Registration(103) }, names, traceOff: new[] { 103 });

        Assert.Single(guard.Keepers);
        Assert.DoesNotContain(102, names.Reads);
        Assert.DoesNotContain(103, names.Reads);
    }
}
