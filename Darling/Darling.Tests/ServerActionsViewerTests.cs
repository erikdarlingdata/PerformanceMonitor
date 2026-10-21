/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>#6269: what the Viewer's Server actions grid lists, what it says before a switch is turned on, and how it reads the service's answer.</summary>
public sealed class ServerActionsViewerTests
{
    private static ServerActionSwitchRow Server(string engine) => new(3, "alpha-sql-01", engine, new[]
    {
        new ServerActionSwitchState(ServerActionNames.ForcePlan, engine == "sqlserver", true, "person:erik", new DateTime(2026, 10, 20, 12, 0, 0, DateTimeKind.Utc)),
        new ServerActionSwitchState(ServerActionNames.EvictPlan, engine == "sqlserver", false, null, null),
        new ServerActionSwitchState(ServerActionNames.KillSession, true, false, null, null),
    });

    [Fact]
    public void OnlyTheActionsTheEngineCanRun_AreListed()
    {
        Assert.Equal(3, ServerActionViewRow.From(new[] { Server("sqlserver") }).Count);
        var pg = ServerActionViewRow.From(new[] { Server("postgres") });
        Assert.Equal(new[] { ServerActionNames.KillSession }, pg.Select(r => r.Action).ToArray());
        Assert.Equal("PostgreSQL", pg[0].EngineDisplay);
    }

    [Fact]
    public void ARow_ShowsItsState_WhoChangedIt_AndWhen()
    {
        var rows = ServerActionViewRow.From(new[] { Server("sqlserver") });
        Assert.Equal("ON", rows[0].StateDisplay);
        Assert.Equal("person:erik", rows[0].ChangedBy);
        Assert.NotEqual("", rows[0].ChangedDisplay);
        Assert.Equal("off", rows[1].StateDisplay);
        Assert.Equal("", rows[1].ChangedBy);
        Assert.Equal("", rows[1].ChangedDisplay);
    }

    [Fact]
    public void TheWarning_SaysWhatTheSwitchLetsAnAgentDo_WhatItNeeds_AndThatItIsJournaled()
    {
        var text = ServerActionViewRow.WarningText("alpha-sql-01", ServerActionNames.KillSession);
        Assert.Contains("alpha-sql-01", text, StringComparison.Ordinal);
        Assert.Contains("kill a session", text, StringComparison.Ordinal);
        Assert.Contains("remediation login", text, StringComparison.Ordinal);
        Assert.Contains("journaled", text, StringComparison.Ordinal);
        Assert.Contains("undone", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ChangeError_RefusesNoSelection_AndAChangeToTheStateItAlreadyHolds()
    {
        var rows = ServerActionViewRow.From(new[] { Server("sqlserver") });
        Assert.NotNull(ServerActionViewRow.ChangeError(null, true));
        Assert.NotNull(ServerActionViewRow.ChangeError(rows[0], true));
        Assert.Null(ServerActionViewRow.ChangeError(rows[0], false));
        Assert.NotNull(ServerActionViewRow.ChangeError(rows[1], false));
        Assert.Null(ServerActionViewRow.ChangeError(rows[1], true));
    }

    [Theory]
    [InlineData("{\"status\":\"set\",\"message\":\"force_plan on alpha-sql-01 is now on (it was off).\"}", true, "set")]
    [InlineData("{\"status\":\"unchanged\",\"message\":\"already on.\"}", true, "unchanged")]
    [InlineData("{\"status\":\"refused\",\"code\":\"not_for_engine\",\"message\":\"force_plan cannot run on pg-01.\"}", false, "refused")]
    [InlineData("{\"error\":\"set_server_action needs target_server_id\"}", false, "failed")]
    [InlineData("not json", false, "failed")]
    [InlineData(null, false, "failed")]
    public void TheServicesAnswer_IsReadAsItCame_AndAnythingElseIsAFailure(string? json, bool succeeded, string status)
    {
        var answer = ServerActionAnswer.Read(json, "failed");
        Assert.Equal(succeeded, answer.Succeeded);
        Assert.Equal(status, answer.Status);
        Assert.False(string.IsNullOrWhiteSpace(answer.Message));
    }
}
