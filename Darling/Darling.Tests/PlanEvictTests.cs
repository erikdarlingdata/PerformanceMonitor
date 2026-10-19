/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#5632 step 10: the parts of the eviction that need no server and no store. Behaviour only.</summary>
public sealed class PlanEvictTests
{
    [Theory]
    [InlineData("0x06000700aabbccdd", "0x06000700AABBCCDD")]
    [InlineData("0X06000700AABBCCDD", "0x06000700AABBCCDD")]
    public void AHandle_HasOneNormalForm_AndTheStatementNamesIt(string given, string normal)
    {
        Assert.True(PlanEvictStatement.TryNormalize(given, out var got));
        Assert.Equal(normal, got);
        Assert.Equal($"DBCC FREEPROCCACHE ({normal});", PlanEvictStatement.For(got));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0x")]
    [InlineData("0x0")]
    [InlineData("06000700")]
    [InlineData("0x0600; SELECT 1")]
    public void ANonHandle_DoesNotNormalize_AndTheStatementRefusesToBuild(string? given)
    {
        Assert.False(PlanEvictStatement.TryNormalize(given, out _));
        Assert.Throws<ArgumentException>(() => PlanEvictStatement.For(given ?? ""));
    }

    private const string Statement = "DBCC FREEPROCCACHE (0x06);";

    [Fact]
    public void TheOnlyThreeRefusals_AreNamedWithTheSharedReasonStrings_InTheOrderTheFactsAreRead()
    {
        const string name = "evict_plan";
        Assert.Equal(EvictDegradeReasons.UnsupportedOnPlatform, SqlServerRemediationExecutor.EvictRefusal(name, true, 5, true, 1, Statement)!.Reason);
        /* Azure SQL Database wins over a missing grant: no grant fixes it. */
        Assert.Equal(EvictDegradeReasons.UnsupportedOnPlatform, SqlServerRemediationExecutor.EvictRefusal(name, true, 5, false, null, Statement)!.Reason);
        Assert.Equal(EvictDegradeReasons.PermissionDenied, SqlServerRemediationExecutor.EvictRefusal(name, false, 3, false, null, Statement)!.Reason);
        Assert.Equal(RemediationResult.Reasons.NotCached, SqlServerRemediationExecutor.EvictRefusal(name, false, 3, true, 0, Statement)!.Reason);
        Assert.Equal(RemediationResult.Reasons.NotCached, SqlServerRemediationExecutor.EvictRefusal(name, false, 3, true, null, Statement)!.Reason);
        Assert.Null(SqlServerRemediationExecutor.EvictRefusal(name, false, 3, true, 1, Statement));
        /* A handle covering several plans (a count above one) is not a refusal. */
        Assert.Null(SqlServerRemediationExecutor.EvictRefusal(name, false, 4, true, 7, Statement));
        Assert.Equal("evict_unsupported_on_platform", EvictDegradeReasons.UnsupportedOnPlatform);
        Assert.Equal("evict_permission_denied", EvictDegradeReasons.PermissionDenied);
    }

    [Fact]
    public void ARefusalByTheExecutor_StillCarriesTheStatement_SoTheAnswerShowsWhatWouldHaveRun()
    {
        var refusal = SqlServerRemediationExecutor.EvictRefusal("evict_plan", true, 3, false, null, Statement)!;
        Assert.Equal(Statement, refusal.Statement);
        Assert.False(refusal.Wrote);
    }

    [Fact]
    public void TheEvictReadBatch_ReadsEditionThenGrantThenCount_BeforeAnyWrite()
    {
        var sql = SqlServerRemediationExecutor.EvictFactsSql;
        var edition = sql.IndexOf("EngineEdition", StringComparison.Ordinal);
        var grant = sql.IndexOf("ALTER SERVER STATE", StringComparison.Ordinal);
        var count = sql.IndexOf("dm_exec_cached_plans", StringComparison.Ordinal);
        Assert.True(edition >= 0 && edition < grant && grant < count);
        Assert.DoesNotContain("FREEPROC", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheEvictKind_HasNoUndo_EvenWhenDone_AndInsideTheWindow()
    {
        Assert.DoesNotContain(AgentChangeKinds.PlanEvict, AgentChangeUndo.UndoableKinds);
        var detail = "{\"kind\":\"plan_evict\",\"tool\":\"evict_cached_plan\"}";
        var row = new AgentChangeRow(1, DateTime.UtcNow.AddMinutes(-5), Guid.NewGuid(), 7, "agent:a", "why", detail, AgentChangeResult.Done, null, null, null, null);
        Assert.Equal("plan_evict", row.Kind);
        Assert.False(row.IsPlanChange);
        Assert.False(row.CanTryUndo(DateTime.UtcNow));
        var failed = row with { Result = AgentChangeResult.Failed };
        Assert.False(failed.CanTryUndo(DateTime.UtcNow));
        var unknown = row with { Result = AgentChangeResult.Unknown };
        Assert.False(unknown.CanTryUndo(DateTime.UtcNow));
    }

    [Fact]
    public void TheWarnings_SayHowManyStatementsTheHandleCovers_OrThatItWasNeverCollected()
    {
        Assert.Contains("has not collected this handle", Assert.Single(DarlingMcpPlanForceTools.EvictWarnings(0)), StringComparison.Ordinal);
        Assert.Contains("get_compiles_after_evict cannot follow it", DarlingMcpPlanForceTools.EvictWarnings(0)[0], StringComparison.Ordinal);
        Assert.Contains("1 statement Darling collected. Evicting it evicts every statement in that batch.", DarlingMcpPlanForceTools.EvictWarnings(1)[0], StringComparison.Ordinal);
        Assert.Contains("3 statements Darling collected", DarlingMcpPlanForceTools.EvictWarnings(3)[0], StringComparison.Ordinal);
        Assert.Contains("at least 50 statements", DarlingMcpPlanForceTools.EvictWarnings(DarlingPlanEvictStore.MaxStatements)[0], StringComparison.Ordinal);
    }

    private static readonly DateTime Created = new(2026, 10, 19, 8, 0, 0, DateTimeKind.Unspecified);

    private static PlanEvictStatementImage Image(string plan = "0xP1", DateTime? created = null, string handle = "0xH1", long executions = 10) =>
        new("0xQH", plan, "0xSQL", 0, -1, handle, DarlingPlanEvictStore.Time(created ?? Created), 1, executions, DarlingPlanEvictStore.Time(Created.AddHours(1)),
            DarlingPlanEvictStore.Time(Created.AddHours(2)), 12.5, 20.0, 300.0);

    private static DarlingPlanEvictStore.Row Row(PlanEvictStatementImage image, DateTime? collected = null) =>
        new(image, DateTime.Parse(image.CreationTime!, System.Globalization.CultureInfo.InvariantCulture), collected ?? Created.AddDays(1));

    private static JsonNode Judge(PlanEvictStatementImage before, params DarlingPlanEvictStore.Row[] rows) =>
        JsonSerializer.SerializeToNode(DarlingMcpCompilesAfterEvictTools.Judge(before, rows))!;

    [Fact]
    public void NoRowsAfterTheEviction_IsNotCollectedYet_AndTheWordsSayTheCollectorTakesOnlyTheCostliest()
    {
        var state = Judge(Image());
        Assert.Equal("not_collected_yet", (string?)state["state"]);
        Assert.Contains("costliest statements", (string?)state["message"], StringComparison.Ordinal);
    }

    [Fact]
    public void RowsWithTheOldCreationTime_AreOldPlanStillSeen_EvenUnderANewHandle_AndOtherStatementsRowsDoNotCount()
    {
        var other = Image() with { QueryHash = "0xOTHER", CreationTime = DarlingPlanEvictStore.Time(Created.AddHours(3)) };
        Assert.Equal("old_plan_still_seen", (string?)Judge(Image(), Row(Image())) ["state"]);
        Assert.Equal("old_plan_still_seen", (string?)Judge(Image(), Row(Image(handle: "0xH2")))["state"]);
        Assert.Equal("not_collected_yet", (string?)Judge(Image(), Row(other))["state"]);
        var offsets = Image() with { StatementStartOffset = 40, CreationTime = DarlingPlanEvictStore.Time(Created.AddHours(3)) };
        Assert.Equal("not_collected_yet", (string?)Judge(Image(), Row(offsets))["state"]);
    }

    [Fact]
    public void ALaterCreationTime_IsARecompile_WithTheSamePlanShapeWhenTheHashesAreEqual_AndTheSameHandleStillCounts()
    {
        var after = Image(created: Created.AddHours(3), executions: 4);
        var same = Judge(Image(), Row(after));
        Assert.Equal("recompiled", (string?)same["state"]);
        Assert.True((bool)same["same_plan_shape"]!);
        Assert.False((bool)same["plan_handle_changed"]!, "the same batch can compile again under the same handle");
        Assert.Equal(4, (long)same["after"]!["executions_since"]!);
        Assert.Equal(10, (long)same["before"]!["execution_count"]!);
        Assert.Equal(12.5, (double)same["before"]!["avg_cpu_ms"]!);

        var differentShape = Judge(Image(), Row(after with { QueryPlanHash = "0xP2", PlanHandle = "0xH2" }));
        Assert.False((bool)differentShape["same_plan_shape"]!);
        Assert.True((bool)differentShape["plan_handle_changed"]!);
    }

    [Fact]
    public void TheCompileIsDecidedByTheCreationTimes_NotByTheStoresCollectionTime()
    {
        /* A row collected long after the eviction but with the OLD creation_time is no compile; a row collected a second after, with a later one, is. */
        var oldPlanLate = Row(Image(), Created.AddDays(6));
        Assert.Equal("old_plan_still_seen", (string?)Judge(Image(), oldPlanLate)["state"]);
        var newPlanSoon = Row(Image(created: Created.AddSeconds(1)), Created.AddSeconds(2));
        Assert.Equal("recompiled", (string?)Judge(Image(), newPlanSoon)["state"]);
    }

    [Fact]
    public void TheEvictTool_IsAnAgentActionTool_AndIsNotServedWithTheWrites()
    {
        Assert.Contains("evict_cached_plan", InvestigationRules.AgentActionToolNames);
        Assert.Contains("evict_cached_plan", DarlingMcpWritePolicy.AgentActionTools);
        Assert.True(DarlingMcpWritePolicy.IsWrite("evict_cached_plan"));
        Assert.False(DarlingMcpWritePolicy.IsWrite("get_compiles_after_evict"));
    }
}
