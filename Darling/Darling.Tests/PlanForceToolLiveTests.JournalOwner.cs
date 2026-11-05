/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #6569: the rows of an agent action (action_called, action_done, action_failed) are the owner's alone. These run the plan tools the way the
   MCP host does: the tool's own data source is a login that holds the mcp role's journal grants and meets the journal's trigger, and the
   store owner's data source is handed in beside it. The same partial class, so the store and SQL Server setup is the force tests'. */
public sealed partial class PlanForceToolLiveTests
{
    /// <summary>Runs <paramref name="body"/> with a login that holds the mcp role's reads and journal INSERT (the columns of
    /// <see cref="DarlingManagedRoles.JournalWriterColumns"/>) and the request row lock, on a store that carries the journal's trigger.</summary>
    private static Task RunAsMcpLoginAsync(Func<Ctx, NpgsqlDataSource, Task> body) =>
        RunAsync(Without.Nothing, async c =>
        {
            await PgExecAsync(c.Connection, DarlingManagedRoles.BuildJournalEventRulesSql("config"), c.Ct);
            var role = "pf_mcp_" + Guid.NewGuid().ToString("N")[..8];
            var password = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
            await PgExecAsync(c.Connection,
                $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{password}'; GRANT USAGE ON SCHEMA config, collect TO {role}; "
                + $"GRANT SELECT ON ALL TABLES IN SCHEMA config, collect TO {role}; "
                + $"GRANT INSERT ({DarlingManagedRoles.JournalWriterColumns}) ON config.investigation_journal TO {role}; "
                + $"GRANT UPDATE (state) ON config.investigation_requests TO {role};", c.Ct);
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(c.Connection.ConnectionString)
                {
                    Username = role,
                    Password = password,
                    SearchPath = PgSchemaGenerator.SearchPath,
                };
                await using var mcp = NpgsqlDataSource.Create(builder.ConnectionString);

                /* The login itself is held to the trigger: an insert of any of the three rows is refused. */
                await using (var asMcp = await mcp.OpenConnectionAsync(c.Ct))
                {
                    var request = await PgScalarAsync(c.Connection, "SELECT request_id FROM config.investigation_requests LIMIT 1", c.Ct);
                    foreach (var journalEvent in AgentActionEvents.OwnerOnly)
                    {
                        var refused = await Assert.ThrowsAsync<PostgresException>(() => PgExecAsync(asMcp,
                            "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor) "
                            + $"VALUES (now(), '{request}', 7, '{journalEvent}', 'agent:other')", c.Ct));
                        Assert.Equal("42501", refused.SqlState);
                    }
                }

                await body(c, mcp);
            }
            finally
            {
                await PgExecAsync(c.Connection, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", CancellationToken.None);
            }
        });

    private static async Task<string> McpUndoAsync(Ctx c, NpgsqlDataSource mcp, long calledId)
    {
        var shown = Parse(await DarlingMcpPlanUndoTools.RunAsync(mcp, c.Source, c.Log, calledId, confirm: false, shown: null, c.Ct));
        Assert.Equal("confirm", (string?)shown["status"]);
        return await DarlingMcpPlanUndoTools.RunAsync(mcp, c.Source, c.Log, calledId, confirm: true, (string?)shown["statement"], c.Ct);
    }

    /// <summary>
    /// #6569 final review: the web's plan Undo builds its journal writer on the store owner's source, as the MCP undo does, and keeps only the sign-in gate on the web login.
    /// Here the web login is a login the journal's trigger holds (an admin login is held the same way): an Undo whose reply is lost still journals its result-unknown
    /// row (kind plan_undo, result unknown, the person's name), which that login could not write itself.
    /// </summary>
    [Fact]
    public async Task AWebPlanUndo_OnAStoreWhoseWebLoginIsHeldByTheJournalTrigger_JournalsItsResultUnknownRowOnTheOwner()
    {
        await RunAsMcpLoginAsync(async (c, webLogin) =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pjo6", c.Ct);
            var forced = Parse(await DarlingMcpPlanForceTools.RunAsync(webLogin, c.Source, c.Log, force: true, "srv-7", c.Sql.Database, query, planA, c.Arg, dry_run: false, c.Ct));
            Assert.Equal("done", (string?)forced["status"]);
            var calledId = (long)forced["journal_id"]!;

            var ownerSeam = new StoreOwnerSource();
            ownerSeam.Publish(c.Source);
            async Task<(JsonObject Body, int Status)> WebUndoAsync(string payload)
            {
                var http = new Microsoft.AspNetCore.Http.DefaultHttpContext();
                http.Request.Body = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(payload));
                return await DarlingWebEndpoints.UndoPlanChangeAsync(http, new AgentChangeUndo(webLogin, 30), ownerSeam, c.Log, calledId, "alice");
            }

            var (shown, shownStatus) = await WebUndoAsync("{}");
            Assert.Equal(200, shownStatus);
            Assert.Equal(PlanUndoAnswer.Confirm, (string?)shown["status"]);
            var confirm = new JsonObject { ["confirm"] = true, ["statement"] = (string?)shown["statement"] }.ToJsonString();

            /* The real procedure runs and unforces the plan; then the reply is lost. */
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = async (command, token) =>
            {
                await command.ExecuteNonQueryAsync(token);
                throw new System.IO.IOException("The connection was reset.");
            };
            (JsonObject Body, int Status) lost;
            try
            {
                lost = await WebUndoAsync(confirm);
            }
            finally
            {
                PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = null;
            }

            Assert.Equal(PlanUndoAnswer.OutcomeUnknown, (string?)lost.Body["status"]);
            var attempt = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes, x => x.CalledId != calledId);
            Assert.Equal("person:alice", attempt.Actor);
            Assert.Equal(AgentChangeKinds.PlanUndo, attempt.Kind);
            Assert.Equal(AgentChangeResult.Unknown, attempt.Result);
        });
    }

    [Fact]
    public async Task AForceThroughTheMcpLogin_JournalsItsRowsOnTheOwner_AndTheUndoStillWorks()
    {
        await RunAsMcpLoginAsync(async (c, mcp) =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pjo1", c.Ct);
            var answer = await DarlingMcpPlanForceTools.RunAsync(mcp, c.Source, c.Log, force: true, "srv-7", c.Sql.Database, query, planA, c.Arg, dry_run: false, c.Ct);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Single(await RowsAsync(c, "action_done"));

            var calledId = (long)json["journal_id"]!;
            /* The undo journals the token that made it: its name here, never the token's fingerprint. */
            ReadCaller.SetAmbientMcpToken("named token ops");
            try
            {
                Assert.Equal("undone", (string?)Parse(await McpUndoAsync(c, mcp, calledId))["status"]);
            }
            finally
            {
                ReadCaller.SetAmbientMcpToken(null);
            }

            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            var undone = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.Equal(calledId, (long)undone["called_id"]!);
            Assert.Equal("person:mcp named token ops", await PgScalarAsync(c.Connection, "SELECT actor FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct));
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AGuideThroughTheMcpLogin_JournalsItsRowsOnTheOwner_AndTheUndoStillWorks()
    {
        await RunAsMcpLoginAsync(async (c, mcp) =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);

            var answer = await DarlingMcpPlanForceTools.RunCreatePlanGuideAsync(mcp, c.Source, c.Log, "srv-7", c.Sql.Database, guide, GuideStatement, "OBJECT",
                $"[dbo].[{name}]", null, "OPTION (RECOMPILE)", c.Arg, dry_run: false, c.Ct);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Equal(guide, await GuideColumnAsync(c, guide, "name"));
            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(GuideKind, (string?)called["kind"]);
            Assert.Single(await RowsAsync(c, "action_done"));

            var calledId = (long)json["journal_id"]!;
            Assert.Equal("undone", (string?)Parse(await McpUndoAsync(c, mcp, calledId))["status"]);
            Assert.Null(await GuideColumnAsync(c, guide, "name"));
            Assert.Equal(calledId, (long)Assert.Single(await RowsAsync(c, "action_undone"))["called_id"]!);
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }
}
