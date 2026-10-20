/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>The <c>undo</c> a written force or unforce answers (#6207, part of #5632): every case, without a server.</summary>
public sealed class AgentPlanUndoTests
{
    private const string Server = "sql-a";
    private const string Database = "Sales";

    private static AgentPlanUndo? Undo(bool force, long planId, long? forcedPlanId = null, string? forcingType = null) =>
        AgentPlanUndoBuilder.For(force, Server, Database, 11, planId, forcedPlanId, forcingType);

    [Fact]
    public void AForce_WithNothingForcedBefore_IsUndoneByAnUnforceOfTheSamePlan()
    {
        var undo = Undo(force: true, 5, null)!;
        Assert.Equal("unforce_query_store_plan", undo.Tool);
        Assert.Equal(new AgentPlanUndoArguments(Server, Database, 11, 5), undo.Arguments);
        Assert.Null(undo.Note);
    }

    [Fact]
    public void AForce_ThatReplacedAManualForce_IsUndoneByForcingTheOldPlanAgain_WithoutANote()
    {
        var undo = Undo(force: true, 5, 3, "MANUAL")!;
        Assert.Equal("force_query_store_plan", undo.Tool);
        Assert.Equal(new AgentPlanUndoArguments(Server, Database, 11, 3), undo.Arguments);
        Assert.Null(undo.Note);
    }

    [Theory]
    [InlineData("AUTO")]
    [InlineData("auto")]
    public void AForce_ThatReplacedAnAutoForce_SaysForcingItAgainMakesItManual(string forcingType)
    {
        var undo = Undo(force: true, 5, 3, forcingType)!;
        Assert.Equal("force_query_store_plan", undo.Tool);
        Assert.Equal(3, undo.Arguments.PlanId);
        Assert.Contains("manual force", undo.Note, StringComparison.Ordinal);
        Assert.Contains("not an automatic one", undo.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void AForce_ThatReplacedAForceOfUnknownKind_HasNoNote()
    {
        Assert.Null(Undo(force: true, 5, 3, "unknown")!.Note);
    }

    [Fact]
    public void AForce_OfThePlanThatIsAlreadyForced_HasNoUndo_BecauseTheToolBlocksIt()
    {
        Assert.Null(Undo(force: true, 5, 5, "MANUAL"));
    }

    [Fact]
    public void AnUnforce_IsUndoneByForcingTheSamePlan()
    {
        var undo = Undo(force: false, 5, 5, "AUTO")!;
        Assert.Equal("force_query_store_plan", undo.Tool);
        Assert.Equal(new AgentPlanUndoArguments(Server, Database, 11, 5), undo.Arguments);
        Assert.Null(undo.Note);
    }

    [Fact]
    public void TheAnswerShape_IsSnakeCase_AndOmitsAMissingNote()
    {
        var json = JsonSerializer.SerializeToNode(Undo(force: true, 5, null))!.AsObject();
        Assert.Equal(new[] { "tool", "arguments" }, json.Select(p => p.Key).ToArray());
        var arguments = json["arguments"]!.AsObject();
        Assert.Equal(new[] { "server_name", "database_name", "query_id", "plan_id" }, arguments.Select(p => p.Key).ToArray());
        Assert.Equal(Server, (string?)arguments["server_name"]);
        Assert.Equal(5, (long)arguments["plan_id"]!);

        var withNote = JsonSerializer.SerializeToNode(Undo(force: true, 5, 3, "AUTO"))!.AsObject();
        Assert.NotNull(withNote["note"]);
    }
}
