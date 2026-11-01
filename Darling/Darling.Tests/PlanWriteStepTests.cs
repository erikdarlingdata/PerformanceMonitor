/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7108: the write step of the plan tools checks the statement it is about to run against the prepared one, before the executor is touched.
/// The executor here has no remediation login, so a write that passes the check is refused for the missing login and never connects.
/// </summary>
public sealed class PlanWriteStepTests
{
    private static SqlServerRemediationExecutor Executor() => new(7, new RemediationServer("127.0.0.1", Port: 1), null, null);

    private static ServerActionPermit Permit(string action, string id, string? database) =>
        new(action, 7, "srv-7", id, database, Guid.NewGuid(), "person:test");

    private const string Handle = "0x06000700AABBCCDD";

    [Fact]
    public async Task AForceWrite_WhoseStatementIsNotThePreparedOne_IsRefusedBeforeTheExecutorIsUsed()
    {
        var state = new PlanForceState("db1", 5, 9, new RequestedPlanState(9, false, null, null), null);
        var facts = new DarlingMcpPlanForceTools.ForcePlanFacts(true, "db1", 5, 9, PlanForceStatement.For("db1", 5, 9, true), null, false, state);

        var changed = await DarlingMcpPlanForceTools.WriteForcePlanAsync(Executor(), Permit(ServerActionNames.ForcePlan, "9", "db2"), facts, TestContext.Current.CancellationToken);
        Assert.Equal(DarlingMcpPlanForceTools.StatementChanged, changed.Reason);
        Assert.False(changed.Wrote);

        var same = await DarlingMcpPlanForceTools.WriteForcePlanAsync(Executor(), Permit(ServerActionNames.ForcePlan, "9", "db1"), facts, TestContext.Current.CancellationToken);
        Assert.NotEqual(DarlingMcpPlanForceTools.StatementChanged, same.Reason);
        Assert.False(same.Wrote);
    }

    [Fact]
    public async Task AnEvictWrite_WhoseStatementIsNotThePreparedOne_IsRefusedBeforeTheExecutorIsUsed()
    {
        var facts = new DarlingMcpPlanForceTools.EvictPlanFacts(Handle, PlanEvictStatement.For("0x06000700AABBCCEE"), new { }, []);

        var changed = await DarlingMcpPlanForceTools.WriteEvictPlanAsync(Executor(), Permit(ServerActionNames.EvictPlan, Handle, null), facts, TestContext.Current.CancellationToken);
        Assert.Equal(DarlingMcpPlanForceTools.StatementChanged, changed.Reason);

        var same = new DarlingMcpPlanForceTools.EvictPlanFacts(Handle, PlanEvictStatement.For(Handle), new { }, []);
        var passed = await DarlingMcpPlanForceTools.WriteEvictPlanAsync(Executor(), Permit(ServerActionNames.EvictPlan, Handle, null), same, TestContext.Current.CancellationToken);
        Assert.NotEqual(DarlingMcpPlanForceTools.StatementChanged, passed.Reason);
    }

    [Fact]
    public async Task ARecompileWrite_WhoseStatementIsNotThePreparedOne_IsRefusedBeforeTheExecutorIsUsed()
    {
        const string module = "[dbo].[p1]";
        var stale = new DarlingMcpPlanForceTools.RecompileModuleFacts("db1", module, ModuleRecompileStatement.For("db1", "[dbo].[p2]"), "SQL_STORED_PROCEDURE", new { });
        var changed = await DarlingMcpPlanForceTools.WriteRecompileModuleAsync(Executor(), Permit(ServerActionNames.RecompileModule, module, "db1"), stale, TestContext.Current.CancellationToken);
        Assert.Equal(DarlingMcpPlanForceTools.StatementChanged, changed.Reason);

        var facts = stale with { Statement = ModuleRecompileStatement.For("db1", module) };
        var passed = await DarlingMcpPlanForceTools.WriteRecompileModuleAsync(Executor(), Permit(ServerActionNames.RecompileModule, module, "db1"), facts, TestContext.Current.CancellationToken);
        Assert.NotEqual(DarlingMcpPlanForceTools.StatementChanged, passed.Reason);
    }

    [Fact]
    public async Task APlanGuideCreateWrite_WhoseStatementIsNotThePreparedOne_IsRefusedBeforeTheExecutorIsUsed()
    {
        var (definition, _, _) = PlanGuideDefinition.TryCreate("g1", "SELECT 1", "SQL", "SELECT 1", null, "OPTION (MAXDOP 1)");
        Assert.NotNull(definition);
        var stale = new DarlingMcpPlanForceTools.CreatePlanGuideFacts("db1", definition, PlanGuideStatement.Create("db1", definition), new { });
        var changed = await DarlingMcpPlanForceTools.WriteCreatePlanGuideAsync(Executor(), Permit(ServerActionNames.CreatePlanGuide, "g1", "db2"), stale, TestContext.Current.CancellationToken);
        Assert.Equal(DarlingMcpPlanForceTools.StatementChanged, changed.Reason);

        var passed = await DarlingMcpPlanForceTools.WriteCreatePlanGuideAsync(Executor(), Permit(ServerActionNames.CreatePlanGuide, "g1", "db1"), stale, TestContext.Current.CancellationToken);
        Assert.NotEqual(DarlingMcpPlanForceTools.StatementChanged, passed.Reason);
    }
}
