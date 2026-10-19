/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>#5917: what the Viewer's Agent changes grid says for each ending of a call, and when Undo is offered.</summary>
public sealed class AgentChangesViewerTests
{
    private static AgentChangeRow Row(AgentChangeResult result, DateTime? undoneAt = null, string kind = "mute", int ageDays = 1) =>
        new(1, DateTime.UtcNow.AddDays(-ageDays), Guid.NewGuid(), 7, "agent:a", "why", "{\"kind\":\"" + kind + "\",\"tool\":\"t\"}",
            result, null, undoneAt, undoneAt is null ? null : "person:erik", null);

    [Theory]
    [InlineData(AgentChangeResult.Done, "done")]
    [InlineData(AgentChangeResult.Failed, "failed")]
    [InlineData(AgentChangeResult.Unknown, "result unknown")]
    public void EachEnding_HasItsWords_AndAnUnknownOneIsNeverOfferedUndo(AgentChangeResult result, string words)
    {
        var view = new AgentChangeViewRow(Row(result));
        Assert.Equal(words, view.ResultDisplay);
        Assert.Equal(result == AgentChangeResult.Done, view.CanUndo);
        Assert.Equal(result == AgentChangeResult.Unknown, AgentChangesTab.UndoError(view) is not null);
    }

    [Theory]
    [InlineData(2, 5, true, "checks answered 2 of 5")]
    [InlineData(0, 3, true, "not reviewed yet")]
    [InlineData(0, 3, false, "not reviewed")]
    [InlineData(1, 1, false, "checks answered 1 of 1")]
    public void APlanForce_ShowsItsReviewMark_AndAnyOtherChangeShowsNone(int answered, int opened, bool running, string words)
    {
        var force = Row(AgentChangeResult.Done, kind: "plan_force") with { ForceReview = new ForceReviewMark(opened, answered, running) };
        Assert.Equal(words, new AgentChangeViewRow(force).ReviewDisplay);
        Assert.Equal("", new AgentChangeViewRow(Row(AgentChangeResult.Done)).ReviewDisplay);
    }

    /// <summary>#5632 step 9: a plan force or unforce is offered Undo whether it is done, failed or unknown (the server's state decides), and the store kinds keep their rule.</summary>
    [Theory]
    [InlineData("plan_force", AgentChangeResult.Done, true)]
    [InlineData("plan_force", AgentChangeResult.Failed, true)]
    [InlineData("plan_force", AgentChangeResult.Unknown, true)]
    [InlineData("plan_unforce", AgentChangeResult.Unknown, true)]
    [InlineData("mute", AgentChangeResult.Failed, false)]
    [InlineData("mute", AgentChangeResult.Unknown, false)]
    public void APlanRow_IsOfferedUndoWhateverItsResult_AndAStoreKindOnlyWhenDone(string kind, AgentChangeResult result, bool offered)
    {
        var view = new AgentChangeViewRow(Row(result, kind: kind));
        Assert.Equal(offered, view.CanUndo);
        Assert.Equal(!offered, AgentChangesTab.UndoError(view) is not null);
        Assert.False(new AgentChangeViewRow(Row(result, DateTime.UtcNow, kind)).CanUndo);
        Assert.False(new AgentChangeViewRow(Row(result, kind: kind, ageDays: 366)).CanUndo);
    }

    [Fact]
    public void TheConfirmStep_ShowsTheMessage_TheStateRead_AndTheExactStatement()
    {
        var state = System.Text.Json.Nodes.JsonNode.Parse("{\"forced_plan\":{\"plan_id\":42,\"forcing_type\":\"MANUAL\"}}");
        var text = AgentChangesTab.ConfirmText(new PlanUndoAnswer(PlanUndoAnswer.Confirm, null, "Undo would unforce plan 42.", "EXEC [d].sys.sp_query_store_unforce_plan @query_id = 1, @plan_id = 42;", state));
        Assert.Contains("Undo would unforce plan 42.", text, StringComparison.Ordinal);
        Assert.Contains("plan 42 is forced (MANUAL)", text, StringComparison.Ordinal);
        Assert.Contains("EXEC [d].sys.sp_query_store_unforce_plan @query_id = 1, @plan_id = 42;", text, StringComparison.Ordinal);
        Assert.Contains("no plan of the query is forced", AgentChangesTab.StateText(System.Text.Json.Nodes.JsonNode.Parse("{\"forced_plan\":null}")), StringComparison.Ordinal);
    }

    [Fact]
    public void AnUndoneChange_SaysWhoUndidIt_AndIsNotOfferedAgain_AndAnOldOrUnknownKindIsNot()
    {
        Assert.Equal("undone by person:erik", new AgentChangeViewRow(Row(AgentChangeResult.Done, DateTime.UtcNow)).ResultDisplay);
        Assert.False(new AgentChangeViewRow(Row(AgentChangeResult.Done, DateTime.UtcNow)).CanUndo);
        Assert.False(new AgentChangeViewRow(Row(AgentChangeResult.Done, ageDays: 366)).CanUndo);
        Assert.False(new AgentChangeViewRow(Row(AgentChangeResult.Done, kind: "edit_server")).CanUndo);
        Assert.True(new AgentChangeViewRow(Row(AgentChangeResult.Done, ageDays: 364)).CanUndo);
    }
}
