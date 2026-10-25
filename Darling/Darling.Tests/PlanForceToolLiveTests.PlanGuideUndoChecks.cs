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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* Review round 2 of #6569: the Undo of a plan guide create or drop applies the create's checks and acts only on a row an agent action wrote, a SQL guide's cached-batch count reads only
   its own database, and a refusal for a DML trigger names its table. The same store and SQL Server setup as the other plan guide live tests (a partial class). */
public sealed partial class PlanForceToolLiveTests
{
    /// <summary>
    /// Journals a copy of the <paramref name="calledId"/> call with <paramref name="edit"/> applied to its detail, as a hand-made row would be: the agent actor, the call's request and server
    /// unless the caller gives others. Returns the new row's id.
    /// </summary>
    private static async Task<long> CloneCalledAsync(Ctx c, long calledId, Action<JsonObject> edit, string actor = "agent:agent-a", int server = 7, Guid? request = null)
    {
        var detailText = await PgScalarAsync(c.Connection, "SELECT detail::text FROM config.investigation_journal WHERE journal_id = " + calledId.ToString(CultureInfo.InvariantCulture), c.Ct);
        var requestText = request?.ToString() ?? await PgScalarAsync(c.Connection, "SELECT request_id::text FROM config.investigation_journal WHERE journal_id = " + calledId.ToString(CultureInfo.InvariantCulture), c.Ct);
        var detail = JsonNode.Parse(detailText!)!.AsObject();
        edit(detail);
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) VALUES (now(), $1::uuid, $2, 'action_called', $3, $4::jsonb) RETURNING journal_id", c.Connection);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = requestText!, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = server, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = actor, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = detail.ToJsonString(), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        return (long)(await command.ExecuteScalarAsync(c.Ct))!;
    }

    private static async Task<long> RealDropAsync(Ctx c, string guide)
    {
        var json = Parse(await DropGuideAsync(c, guide, dryRun: false));
        Assert.Equal("done", (string?)json["status"]);
        return (long)json["journal_id"]!;
    }

    private static async Task<int> UndoneRowsAsync(Ctx c) =>
        int.Parse(await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct) ?? "0", CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheUndoOfADropWhoseJournaledDefinitionTheCreateRefuses_IsRefused_AndNothingRuns()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, guide, name);
            await TurnDropOnAsync(c);
            var dropped = await RealDropAsync(c, guide);

            var edits = new (string Why, Action<JsonObject> Edit)[]
            {
                ("hints that do not start with OPTION (", d => d["before"]!["guide"]!["hints"] = "RECOMPILE"),
                ("an OBJECT module that does not normalize", d => d["before"]!["guide"]!["module_or_batch"] = "a.b.c.d"),
                ("a TEMPLATE type on a guide that has a module", d => d["before"]!["guide"]!["type"] = "TEMPLATE"),
                ("a name over 128 characters", d => d["target"]!["id"] = new string('g', 129)),
            };
            foreach (var (why, edit) in edits)
            {
                var row = await CloneCalledAsync(c, dropped, edit);
                var answer = await UndoAsync(c, row, confirm: true);
                Assert.Equal(PlanUndoAnswer.RefusedStatus, answer.Status);
                Assert.True(answer.Code == "not_undoable", $"{why}: {answer.Code} {answer.Message}");
                Assert.Null(await GuideRowAsync(c, guide));
            }

            Assert.Equal(0, await UndoneRowsAsync(c));

            /* The real row still undoes. */
            var undone = await UndoAsync(c, dropped, confirm: true);
            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.NotNull(await GuideRowAsync(c, guide));
        });
    }

    [Fact]
    public async Task TheUndoOfACreateWhoseJournaledNameIsOver128Characters_IsRefused_AndNothingRuns()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            var created = (long)Parse(await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: "dbo." + name))["journal_id"]!;

            /* A name the server would cut to 128 characters must not reach it: the guide that exists under the first 128 characters is not dropped. */
            var row = await CloneCalledAsync(c, created, d => d["target"]!["id"] = guide + new string('x', 140));
            var answer = await UndoAsync(c, row, confirm: true);
            Assert.Equal("not_undoable", answer.Code);
            Assert.Equal("OBJECT", await GuideColumnAsync(c, guide, "scope_type_desc"));
            Assert.Equal(0, await UndoneRowsAsync(c));
        });
    }

    [Theory]
    [InlineData("excluded", 0)]
    [InlineData("azure", 5)]
    [InlineData("azure", 8)]
    [InlineData("postgres", 0)]
    public async Task TheUndoOfAGuideCreateOrDrop_OnAnExcludedDatabase_AnAzureTarget_OrAPostgreSqlTarget_IsRefused_AndRecorded(string kind, int engineEdition)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var made = "g_" + name;
            var dropMe = "d_" + name;
            await MakeProcedureAsync(c, name);
            await MakeProcedureAsync(c, name + "_2");
            await MakeObjectGuideAsync(c, dropMe, name + "_2");
            await TurnGuideOnAsync(c);
            await TurnDropOnAsync(c);
            var created = (long)Parse(await CreateGuideAsync(c, made, dryRun: false, moduleOrBatch: "dbo." + name))["journal_id"]!;
            var dropped = await RealDropAsync(c, dropMe);

            if (kind == "excluded")
                await PgExecAsync(c.Connection, $"UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['{c.Sql.Database.ToUpperInvariant()}'] WHERE server_id = 7", c.Ct);
            else if (kind == "azure")
                await PgExecAsync(c.Connection, $"UPDATE collect.servers SET sql_engine_edition = {engineEdition.ToString(CultureInfo.InvariantCulture)} WHERE server_id = 7", c.Ct);
            else
                await PgExecAsync(c.Connection, "UPDATE collect.servers SET engine_kind = 'postgres' WHERE server_id = 7", c.Ct);
            var code = kind switch { "excluded" => "database_excluded", "azure" => "engine_not_supported", _ => "postgres_target" };

            foreach (var row in new[] { created, dropped })
            {
                var answer = await UndoAsync(c, row, confirm: true);
                Assert.Equal(PlanUndoAnswer.RefusedStatus, answer.Status);
                Assert.Equal(code, answer.Code);
                Assert.Equal("1", await PgScalarAsync(c.Connection,
                    $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{code}' AND actor = 'person:alice' AND (detail->>'called_id')::bigint = {row.ToString(CultureInfo.InvariantCulture)}", c.Ct));
            }

            /* Nothing ran: the created guide is still there, the dropped one is still gone. */
            Assert.NotNull(await GuideColumnAsync(c, made, "name"));
            Assert.Null(await GuideColumnAsync(c, dropMe, "name"));
            Assert.Equal(0, await UndoneRowsAsync(c));
        });
    }

    [Fact]
    public async Task AGuideRowAnAgentActionDidNotWrite_IsNotUndone_ButARealCreateAndDropStillAre()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var made = "g_" + name;
            var dropMe = "d_" + name;
            await MakeProcedureAsync(c, name);
            await MakeProcedureAsync(c, name + "_2");
            await MakeObjectGuideAsync(c, dropMe, name + "_2");
            await TurnGuideOnAsync(c);
            await TurnDropOnAsync(c);
            var created = (long)Parse(await CreateGuideAsync(c, made, dryRun: false, moduleOrBatch: "dbo." + name))["journal_id"]!;
            var dropped = await RealDropAsync(c, dropMe);

            foreach (var real in new[] { created, dropped })
            {
                var other = Guid.NewGuid();
                var fakes = new (string Why, long Row)[]
                {
                    ("the actor is a person", await CloneCalledAsync(c, real, _ => { }, actor: "person:bob")),
                    ("the request is on another server", await CloneCalledAsync(c, real, _ => { }, server: 8)),
                    ("the request has no row", await CloneCalledAsync(c, real, _ => { }, request: other)),
                };
                foreach (var (why, row) in fakes)
                {
                    var answer = await UndoAsync(c, row, confirm: true);
                    Assert.True(answer.Status == PlanUndoAnswer.RefusedStatus && answer.Code == "not_undoable", $"{why}: {answer.Status} {answer.Code} {answer.Message}");
                }
            }

            Assert.NotNull(await GuideColumnAsync(c, made, "name"));
            Assert.Null(await GuideColumnAsync(c, dropMe, "name"));
            Assert.Equal(0, await UndoneRowsAsync(c));

            /* A request that was not Level 3 when the row was written is not one the tools wrote under. */
            var request = await PgScalarAsync(c.Connection, "SELECT request_id::text FROM config.investigation_journal WHERE journal_id = " + created.ToString(CultureInfo.InvariantCulture), c.Ct);
            await PgExecAsync(c.Connection, $"UPDATE config.investigation_requests SET autonomy_level = 2 WHERE request_id = '{request}'", c.Ct);
            foreach (var real in new[] { created, dropped })
            {
                var answer = await UndoAsync(c, real, confirm: true);
                Assert.Equal("not_undoable", answer.Code);
            }

            await PgExecAsync(c.Connection, $"UPDATE config.investigation_requests SET autonomy_level = 3 WHERE request_id = '{request}'", c.Ct);
            Assert.Equal(PlanUndoAnswer.Undone, (await UndoAsync(c, created, confirm: true)).Status);
            Assert.Equal(PlanUndoAnswer.Undone, (await UndoAsync(c, dropped, confirm: true)).Status);
            Assert.Null(await GuideColumnAsync(c, made, "name"));
            Assert.NotNull(await GuideColumnAsync(c, dropMe, "name"));
        });
    }

    [Fact]
    public async Task ASqlGuideCountsOnlyTheBatchesCachedInItsOwnDatabase()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await TurnGuideOnAsync(c);
            var statement = "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= " + Random.Shared.Next(1000, 99999);
            var other = new SqlScratch();
            await other.CreateAsync(c.Ct);
            try
            {
                await using (var otherAdmin = await OpenAsync(other.Admin(other.Database), c.Ct))
                {
                    await ExecAsync(otherAdmin, statement, c.Ct);
                    await ExecAsync(otherAdmin, statement, c.Ct);
                }

                /* The batch is cached, but in another database: this guide's database counts none. */
                var elsewhere = Parse(await CreateGuideAsync(c, "g_" + Guid.NewGuid().ToString("N")[..8], dryRun: true, type: "SQL", statement: statement));
                Assert.Equal("dry_run", (string?)elsewhere["status"]);
                Assert.Equal(0, (long)elsewhere["matches"]!);

                /* The same batch cached in the guide's own database is counted. */
                await AdminExecAsync(c, statement);
                var here = Parse(await CreateGuideAsync(c, "g_" + Guid.NewGuid().ToString("N")[..8], dryRun: true, type: "SQL", statement: statement));
                Assert.True((long)here["matches"]! >= 1, "the batch cached in the guide's own database is counted");
            }
            finally
            {
                await other.DropAsync();
            }
        });
    }

    [Fact]
    public async Task ALoginWithoutAlterOnTheTriggersTable_IsRefusedAnObjectGuideOnTheTrigger_WithTheTableNamed()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var trigger = "trg_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + trigger;
            await AdminExecAsync(c, $"CREATE TRIGGER dbo.[{trigger}] ON dbo.t AFTER INSERT AS SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;");
            await TurnGuideOnAsync(c);
            await TurnDropOnAsync(c);
            /* VIEW DEFINITION on the table lets the login see the trigger and read its text; it holds no ALTER on the table. */
            await AdminExecAsync(c, $"ALTER ROLE db_owner DROP MEMBER [{c.Sql.Login}]; GRANT VIEW DEFINITION ON OBJECT::dbo.t TO [{c.Sql.Login}];");

            foreach (var dryRun in new[] { true, false })
            {
                var refused = Parse(await CreateGuideAsync(c, guide, dryRun, moduleOrBatch: "dbo." + trigger));
                Assert.Equal("refused", (string?)refused["status"]);
                Assert.Equal("guide_permission_denied", (string?)refused["reason"]);
                var message = (string?)refused["message"] ?? "";
                Assert.Contains("the table [dbo].[t]", message, StringComparison.Ordinal);
                Assert.Contains($"[dbo].[{trigger}]", message, StringComparison.Ordinal);
            }

            Assert.Null(await GuideColumnAsync(c, guide, "name"));

            /* An OBJECT guide on the trigger already exists: the drop is refused the same way. */
            await AdminExecAsync(c, $"EXEC sys.sp_create_plan_guide @name = N'{guide}', @stmt = N'{GuideStatement}', @type = N'OBJECT', @module_or_batch = N'dbo.{trigger}', @params = NULL, @hints = N'OPTION (RECOMPILE)';");
            var drop = Parse(await DropGuideAsync(c, guide, dryRun: false));
            Assert.Equal("refused", (string?)drop["status"]);
            Assert.Equal("guide_permission_denied", (string?)drop["reason"]);
            Assert.Contains("the table [dbo].[t]", (string?)drop["message"] ?? "", StringComparison.Ordinal);
            Assert.NotNull(await GuideColumnAsync(c, guide, "name"));

            /* ALTER on the table is what a guide on its trigger needs: the drop then goes through. */
            await AdminExecAsync(c, $"GRANT ALTER ON OBJECT::dbo.t TO [{c.Sql.Login}];");
            var dropped = Parse(await DropGuideAsync(c, guide, dryRun: false));
            Assert.Equal("done", (string?)dropped["status"]);
            Assert.Null(await GuideColumnAsync(c, guide, "name"));
            var created = Parse(await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: "dbo." + trigger));
            Assert.Equal("done", (string?)created["status"]);
        });
    }
}
