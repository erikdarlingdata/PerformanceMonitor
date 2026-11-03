/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7213: the time columns of the People window, the Agent changes tab, the Server actions tab, the Investigations window and the Proposals grid belong to no one server,
/// so they follow the display mode like the other fleet-wide grids: UTC (labelled) in Server and UTC modes, this machine's
/// zone (labelled) in Local mode. They used to print the machine's local time with no label in every mode.
/// </summary>
/* Flips the process-wide display mode and active server clock (each restored in Dispose). */
[Collection("viewer-time-statics")]
public sealed class ViewerFleetWideRowTimesTests : IDisposable
{
    private static readonly DateTime s_at = new(2026, 7, 1, 14, 30, 0, DateTimeKind.Utc);

    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
    }

    private static PersonViewRow PersonRowWith()
    {
        var identity = new PersonIdentityRow(1, "slack", "T01:U02", s_at, "person:erik");
        var grant = new PersonGrantRow(1, null, null, null, null, "ask", [], s_at, "person:erik");
        return new PersonViewRow(new PersonRow(1, "Ann", true, s_at, "person:erik", s_at, "person:erik", [identity], [grant]));
    }

    private static ServerActionViewRow ActionRow() => ServerActionViewRow.From(
    [
        new ServerActionSwitchRow(3, "alpha-sql-01", "sqlserver",
        [
            new ServerActionSwitchState(ServerActionNames.KillSession, true, true, "person:erik", s_at),
        ]),
    ])[0];

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void TheTimes_InServerAndUtcMode_ReadUtcWithItsLabel_NotTheActiveServersClock(TimeDisplayMode mode)
    {
        ViewerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-420);
        ViewerTimeHelper.CurrentDisplayMode = mode;

        var person = PersonRowWith();
        Assert.Equal("2026-07-01 14:30 UTC", person.UpdatedDisplay);
        Assert.Equal("2026-07-01 14:30 UTC by erik", person.Identities[0].AddedDisplay);
        Assert.Equal("2026-07-01 14:30 UTC by erik", person.Grants[0].AddedDisplay);
        Assert.Equal("2026-07-01 14:30 UTC", ActionRow().ChangedDisplay);
    }

    [Fact]
    public void TheTimes_InLocalMode_ReadThisMachinesZone()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        var expected = TimeZoneInfo.ConvertTimeFromUtc(s_at, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " ";

        Assert.StartsWith(expected, PersonRowWith().UpdatedDisplay, StringComparison.Ordinal);
        Assert.StartsWith(expected, PersonRowWith().Identities[0].AddedDisplay, StringComparison.Ordinal);
        Assert.StartsWith(expected, PersonRowWith().Grants[0].AddedDisplay, StringComparison.Ordinal);
        Assert.StartsWith(expected, ActionRow().ChangedDisplay, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAgentChangeAndRefusalTimes_FollowTheDisplayMode()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;

        var change = new AgentChangeRow(1, s_at, Guid.NewGuid(), 7, "agent:a", "why", "{\"kind\":\"mute\",\"tool\":\"t\"}",
            AgentChangeResult.Done, null, null, null, null);
        Assert.Equal("2026-07-01 14:30 UTC", new AgentChangeViewRow(change).WhenDisplay);
        Assert.Equal("2026-07-01 14:30 UTC", new AgentRefusalViewRow(new AgentRefusalRow(1, s_at, null, 7, "agent:a", "code", "tool", "SQL2019")).WhenDisplay);
    }

    private static InvestigationRequestViewRow RequestRow() => new()
    {
        OpenedAtUtc = s_at,
        ExpiresAtUtc = s_at.AddDays(1),
        LeaseUntilUtc = s_at.AddMinutes(5),
    };

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void TheInvestigationAndProposalTimes_InServerAndUtcMode_ReadUtcWithItsLabel(TimeDisplayMode mode)
    {
        ViewerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-420);
        ViewerTimeHelper.CurrentDisplayMode = mode;

        var request = RequestRow();
        Assert.Equal("2026-07-01 14:30 UTC", request.OpenedDisplay);
        Assert.Equal("2026-07-02 14:30 UTC", request.ExpiresDisplay);
        Assert.Equal("14:35 UTC", request.LeaseDisplay);
        Assert.Equal("2026-07-01 14:30:00 UTC", new InvestigationAlertViewRow(s_at, "m", null).TimeDisplay);
        Assert.Equal("2026-07-01 14:30:00 UTC", new InvestigationJournalViewRow(s_at, "e", "a", null).TimeDisplay);

        var note = new InvestigationNoteViewRow { CreatedAtUtc = s_at, ExpiresAtUtc = s_at.AddDays(30) };
        Assert.Equal("2026-07-01 14:30 UTC", note.CreatedDisplay);
        Assert.Equal("2026-07-31 UTC", note.ExpiresDisplay);

        var proposal = new ProposalViewRow { ProposalId = Guid.NewGuid(), Kind = "k", Server = "s", Target = "t", State = "pending", ExpiresAtUtc = s_at };
        Assert.Equal("2026-07-01 14:30 UTC", proposal.ExpiresDisplay);
    }

    [Fact]
    public void TheInvestigationAndProposalTimes_InLocalMode_ReadThisMachinesZone()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        string Local(DateTime utc, string format) => TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.Local).ToString(format, CultureInfo.InvariantCulture) + " ";

        var request = RequestRow();
        Assert.StartsWith(Local(s_at, "yyyy-MM-dd HH:mm"), request.OpenedDisplay, StringComparison.Ordinal);
        Assert.StartsWith(Local(s_at.AddMinutes(5), "HH:mm"), request.LeaseDisplay, StringComparison.Ordinal);
        Assert.StartsWith(Local(s_at, "yyyy-MM-dd HH:mm:ss"), new InvestigationAlertViewRow(s_at, "m", null).TimeDisplay, StringComparison.Ordinal);
        Assert.StartsWith(Local(s_at.AddDays(30), "yyyy-MM-dd"), new InvestigationNoteViewRow { ExpiresAtUtc = s_at.AddDays(30) }.ExpiresDisplay, StringComparison.Ordinal);
    }
}
