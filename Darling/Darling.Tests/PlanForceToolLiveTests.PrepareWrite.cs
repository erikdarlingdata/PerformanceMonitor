/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The prepare step and the write step of the plan force tool (#7108): the same store and SQL Server setup (a partial class, so the private setup is shared).
   The prepare step takes the permit and the parsed target, no agent_action; the write step takes the facts the prepare step returned. */
public sealed partial class PlanForceToolLiveTests
{
    private static async Task<ServerActionPermit> PermitForAsync(Ctx c, string tool, string serverAction, string id, string? database)
    {
        var store = new InvestigationRequestStore(c.Source, 30);
        var decision = await ServerActionGate.AuthorizeAsync(store, c.Source, c.Log, tool, serverAction, c.Arg, new AgentActionTarget(7, "srv-7", id, database), c.Ct);
        Assert.Null(decision.Refusal);
        return decision.Permit!;
    }

    [Fact]
    public async Task APrepareOfAForce_ReturnsTheBindingFacts_AndTheWriteWithThoseFactsForcesThePlan()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf7108a", c.Ct);
            var permit = await PermitForAsync(c, DarlingMcpPlanForceTools.ForceToolName, ServerActionNames.ForcePlan, planA.ToString(CultureInfo.InvariantCulture), c.Sql.Database);

            var prepared = await DarlingMcpPlanForceTools.PrepareForcePlanAsync(c.Source, c.Log, permit, 7, "srv-7", force: true, c.Sql.Database, query, planA, dryRun: true,
                static (_, _) => Task.CompletedTask, c.Ct);
            var facts = prepared.Facts!;
            Assert.Contains("\"dry_run\"", prepared.Answer, StringComparison.Ordinal);
            Assert.Equal(query, facts.QueryId);
            Assert.Equal(planA, facts.PlanId);
            Assert.False(facts.IsForced);
            Assert.Null(facts.ForcedPlanId);
            Assert.Equal(PlanForceStatement.For(c.Sql.Database, query, planA, force: true), facts.Statement);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));

            var written = await DarlingMcpPlanForceTools.WriteForcePlanAsync(prepared.Executor!, permit, facts, c.Ct);
            Assert.True(written.Wrote);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
        });
    }

    [Fact]
    public async Task APrepareOfAnEvict_ReturnsTheStatement_AndTheWriteWithThoseFactsEvictsTheHandle()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var marker = "ev7" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 3, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);
            await SeedStatsAsync(c, cached, DateTime.UtcNow.AddMinutes(-5), cached.Executions);
            var handle = cached.PlanHandle.ToUpperInvariant().Replace("0X", "0x", StringComparison.Ordinal);
            var permit = await PermitForAsync(c, DarlingMcpPlanForceTools.EvictToolName, ServerActionNames.EvictPlan, handle, null);

            var prepared = await DarlingMcpPlanForceTools.PrepareEvictPlanAsync(c.Source, c.Source, c.Log, permit, 7, "srv-7", handle, dryRun: false,
                static (_, _) => Task.CompletedTask, c.Ct);
            Assert.Null(prepared.Answer);
            Assert.Equal("DBCC FREEPROCCACHE (" + handle + ");", prepared.Facts!.Statement);

            var written = await DarlingMcpPlanForceTools.WriteEvictPlanAsync(prepared.Executor!, permit, prepared.Facts, c.Ct);
            Assert.True(written.Wrote);
            Assert.False(await IsCachedAsync(c.Sql, handle, c.Ct));
        });
    }

    [Fact]
    public async Task APrepareOfARecompile_ReturnsTheStatementAndTheObjectType_AndTheWriteMarksTheModule()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            var module = $"[dbo].[{name}]";
            var permit = await PermitForAsync(c, DarlingMcpPlanForceTools.RecompileToolName, ServerActionNames.RecompileModule, module, c.Sql.Database);

            var prepared = await DarlingMcpPlanForceTools.PrepareRecompileModuleAsync(c.Source, c.Log, permit, 7, "srv-7", c.Sql.Database, module, dryRun: false,
                static (_, _) => Task.CompletedTask, c.Ct);
            Assert.Null(prepared.Answer);
            Assert.Equal("SQL_STORED_PROCEDURE", prepared.Facts!.ObjectType);
            Assert.Contains("sp_recompile", prepared.Facts.Statement, StringComparison.OrdinalIgnoreCase);

            var written = await DarlingMcpPlanForceTools.WriteRecompileModuleAsync(prepared.Executor!, permit, prepared.Facts, c.Ct);
            Assert.True(written.Wrote);
            Assert.Equal(0, await ProcedureExecutionsAsync(c, name));
        });
    }
}
