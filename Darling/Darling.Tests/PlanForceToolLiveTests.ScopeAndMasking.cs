/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* Two scope rules of the plan tools, both settled before the tool reaches the monitored server, so these tests make only the store (storeOnly: no SQL Server database):
   force and unforce (and the force undo) refuse a database the server's entry excludes, as recompile_module and the plan guide tools do; and a plan guide's batch text
   and hint string follow mcp.dataDetail in the drop answer and in the journal row get_agent_changes reads back. */
public sealed partial class PlanForceToolLiveTests
{
    private const string GuideBatch = "SELECT a FROM dbo.t WHERE b = 424242 AND a = 'secret-batch-literal'";
    private const string GuideHints = "OPTION (OPTIMIZE FOR (@p = 987654))";

    private static async Task ExcludeDatabaseAsync(Ctx c) =>
        await PgExecAsync(c.Connection, $"UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['{c.Sql.Database.ToUpperInvariant()}'] WHERE server_id = 7", c.Ct);

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task ForceAndUnforce_OnADatabaseTheServersEntryExcludes_AreRefused_ForADryRunAndAWrite_AndNothingIsJournaledAsACall(bool force, bool dryRun)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await ExcludeDatabaseAsync(c);

            var json = Parse(await c.Tool(force, queryId: 11, planId: 22, dryRun));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("database_excluded", (string?)json["reason"]);
            Assert.Contains("is excluded in this server's entry, so Darling does not act in it", (string?)json["message"], StringComparison.Ordinal);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'database_excluded' AND actor = 'agent:agent-a' "
                + "AND request_id = (SELECT request_id FROM config.investigation_requests LIMIT 1)", c.Ct));
        }, storeOnly: true);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ForceAndUnforce_OnADatabaseThatIsNotExcluded_GoPastTheExclusionCheck(bool force)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await PgExecAsync(c.Connection, "UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['SomeOtherDatabase'] WHERE server_id = 7", c.Ct);

            /* The server row's host is a closed port here, so the tool stops at the before-state read; it was not refused as excluded. */
            var json = Parse(await c.Tool(force, queryId: 11, planId: 22, dryRun: true));
            Assert.Equal("failed", (string?)json["status"]);
            Assert.Null((string?)json["reason"]);
            Assert.DoesNotContain("is excluded in this server's entry", (string?)json["message"], StringComparison.Ordinal);
            Assert.Equal("0", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE reason = 'database_excluded'", c.Ct));
        }, storeOnly: true);
    }

    [Theory]
    [InlineData(ForceKind)]
    [InlineData(UnforceKind)]
    public async Task TheUndoOfAForceOrUnforce_OnADatabaseTheServersEntryExcludes_IsRefused_AndRecorded(string kind)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var called = await SeedCallAsync(c, kind, queryId: 11, planId: 22, beforePlan: kind == ForceKind ? null : 22, beforeType: "MANUAL", result: "done");
            await ExcludeDatabaseAsync(c);

            var answer = await UndoAsync(c, called, confirm: true);
            Assert.Equal(PlanUndoAnswer.RefusedStatus, answer.Status);
            Assert.Equal("database_excluded", answer.Code);
            Assert.Contains("by hand", answer.Message, StringComparison.Ordinal);
            Assert.Contains("exclusion list", answer.Message, StringComparison.Ordinal);
            Assert.Equal("1", await PgScalarAsync(c.Connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'database_excluded' AND actor = 'person:alice' AND (detail->>'called_id')::bigint = {called.ToString(CultureInfo.InvariantCulture)}", c.Ct));
            Assert.Equal(0, await UndoneRowsAsync(c));
        }, storeOnly: true);
    }

    /// <summary>The before image a plan guide drop journals and answers with: the tool's own <c>GuideImage</c>, serialized the way the tool serializes it. The guide's batch text and hint string hold literals.</summary>
    private static JsonObject DroppedGuideBefore(Ctx c, PlanGuideDefinition? guide = null) =>
        JsonNode.Parse(JsonSerializer.Serialize(new
        {
            database = c.Sql.Database,
            guide = DarlingMcpPlanForceTools.GuideImage(guide ?? new PlanGuideDefinition("guide_one", "SQL", GuideBatch, GuideBatch, "@p int", GuideHints), disabled: false),
        }, McpHelpers.JsonOptions))!.AsObject();

    [Theory]
    [InlineData(McpDataDetail.Full)]
    [InlineData(McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.MetadataOnly)]
    public async Task AnObjectGuidesModuleName_IsKeptAtEveryLevel_WhileItsBatchKeyIsMasked(McpDataDetail mode)
    {
        await RunAsync(Without.Nothing, c =>
        {
            var guide = new PlanGuideDefinition("guide_obj", "OBJECT", GuideBatch, "[dbo].[proc_one]", null, GuideHints);
            var answer = new JsonObject { ["status"] = "dry_run", ["before"] = DroppedGuideBefore(c, guide) }.ToJsonString();

            var masked = JsonNode.Parse(McpDataDetailMasker.ForAnswer(mode, PseudonymTestKey.Key).Answer(answer))!["before"]!["guide"]!;
            Assert.Equal("[dbo].[proc_one]", (string?)masked["module"]);
            if (mode == McpDataDetail.MetadataOnly)
                Assert.NotEqual("[dbo].[proc_one]", (string?)masked["module_or_batch"]);
            return Task.CompletedTask;
        }, storeOnly: true);
    }

    [Theory]
    [InlineData(McpDataDetail.Full, true)]
    [InlineData(McpDataDetail.MaskLiterals, false)]
    [InlineData(McpDataDetail.MetadataOnly, false)]
    public async Task TheGuideBatchAndHints_OfADropAnswer_FollowDataDetail(McpDataDetail mode, bool literalsShown)
    {
        await RunAsync(Without.Nothing, c =>
        {
            /* The answer shape RunDropPlanGuideAsync builds: before.guide is the GuideImage the Undo re-creates from. */
            var answer = new JsonObject
            {
                ["status"] = "dry_run", ["dry_run"] = true,
                ["statement"] = "EXEC sp_control_plan_guide N'DROP', N'guide_one';",
                ["before"] = DroppedGuideBefore(c),
            }.ToJsonString();

            AssertLiterals(McpDataDetailMasker.ForAnswer(mode, PseudonymTestKey.Key).Answer(answer), literalsShown);
            return Task.CompletedTask;
        }, storeOnly: true);
    }

    [Theory]
    [InlineData(McpDataDetail.Full, true)]
    [InlineData(McpDataDetail.MaskLiterals, false)]
    [InlineData(McpDataDetail.MetadataOnly, false)]
    public async Task TheGuideBatchAndHints_OfADropsJournalRow_FollowDataDetail_InGetAgentChanges(McpDataDetail mode, bool literalsShown)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var request = await PgScalarAsync(c.Connection, "SELECT request_id FROM config.investigation_requests LIMIT 1", c.Ct);
            var detail = new JsonObject { ["tool"] = "drop_plan_guide", ["kind"] = DropKind, ["before"] = DroppedGuideBefore(c) };
            var called = await InsertJournalAsync(c, request!, "action_called", "agent:agent-a", detail.ToJsonString());
            await InsertJournalAsync(c, request!, "action_done", "agent:agent-a", new JsonObject { ["called_id"] = called }.ToJsonString());

            var read = await DarlingMcpIncidentActionTools.GetAgentChanges(c.Source, cancellationToken: c.Ct);
            Assert.Contains("guide_one", read, StringComparison.Ordinal);
            AssertLiterals(McpDataDetailMasker.ForAnswer(mode, PseudonymTestKey.Key).Answer(read), literalsShown);
        }, storeOnly: true);
    }

    private static void AssertLiterals(string answer, bool shown)
    {
        foreach (var literal in new[] { "424242", "secret-batch-literal", "987654" })
        {
            if (shown)
                Assert.Contains(literal, answer, StringComparison.Ordinal);
            else
                Assert.DoesNotContain(literal, answer, StringComparison.Ordinal);
        }

        /* The guide's name and the database are not statement text and stay. */
        Assert.Contains("guide_one", answer, StringComparison.Ordinal);
    }
}
