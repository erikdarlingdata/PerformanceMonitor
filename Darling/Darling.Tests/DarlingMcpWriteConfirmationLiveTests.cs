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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The confirmation question (#9562) against a real store, through the real MCP host: what the store holds after each answer. Yes changes
/// it, no leaves it as it was, and a client that cannot be asked, or an agent's call, changes it with no question, as before.
/// </summary>
public sealed class DarlingMcpWriteConfirmationLiveTests
{
    private const string Token = DarlingMcpWritePolicyTests.Token;
    private const string Yes = "{\"action\":\"accept\",\"content\":{\"confirm\":true}}";

    private static readonly McpConfig Config = new() { AllowWrites = true, AllowServerAdmin = true };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<Microsoft.AspNetCore.TestHost.TestServer, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the confirmation live pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        /* The service provisions the queue function at start, not the migrations: purge_now queues through it. */
        await using (var queue = new NpgsqlCommand(DarlingManagedRoles.BuildRequestMcpCommandFunctionSql("config"), owner))
        {
            await queue.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            using var server = await DarlingMcpWritePolicyTests.BuildServer(Config, Token, connectionString);
            await body(server, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<int> AddServerTagAsync(NpgsqlConnection connection, string name, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("INSERT INTO config.server_tags (name) VALUES ($1) RETURNING id", connection);
        command.Parameters.AddWithValue(name);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static Task<long> TagCountAsync(NpgsqlConnection connection, int id, CancellationToken ct) =>
        ScalarAsync(connection, $"SELECT count(*) FROM config.server_tags WHERE id = {id}", ct);

    private static string AnswerOf(string body)
    {
        var dataLine = Array.Find(body.Split((char)10), l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        return document.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!;
    }

    private static string StatusOf(string body)
    {
        using var answer = JsonDocument.Parse(AnswerOf(body));
        return answer.RootElement.GetProperty("status").GetString()!;
    }

    [Fact]
    public async Task ADeleteWaitsForAnAnswer_YesDeletes_NoAndADismissedQuestionLeaveTheRow()
    {
        await WithStoreAsync(async (server, owner, ct) =>
        {
            var tag = await AddServerTagAsync(owner, "Prod", ct);
            var args = $"{{\"tag_id\":{tag}}}";

            var asked = await DarlingMcpWriteConfirmationTests.CallAsync(server, "delete_server_tag", args, negotiate: true);
            Assert.Contains($"server tag {tag}", DarlingMcpWriteConfirmationTests.QuestionOf(asked), StringComparison.Ordinal);
            Assert.Equal(1, await TagCountAsync(owner, tag, ct));

            foreach (var no in new[] { "{\"action\":\"decline\"}", "{\"action\":\"cancel\"}", "{\"action\":\"accept\",\"content\":{\"confirm\":false}}" })
            {
                var declined = await DarlingMcpWriteConfirmationTests.CallAsync(server, "delete_server_tag", args, negotiate: true, inputResponse: no);
                Assert.Equal("declined", StatusOf(declined));
                Assert.Equal(1, await TagCountAsync(owner, tag, ct));
            }

            var yes = await DarlingMcpWriteConfirmationTests.CallAsync(server, "delete_server_tag", args, negotiate: true, inputResponse: Yes);
            Assert.Equal("deleted", StatusOf(yes));
            Assert.Equal(0, await TagCountAsync(owner, tag, ct));
        });
    }

    [Fact]
    public async Task AClientThatDidNotNegotiateTheFlow_OrCannotElicit_DeletesWithNoQuestion_AsBefore()
    {
        await WithStoreAsync(async (server, owner, ct) =>
        {
            var older = await AddServerTagAsync(owner, "Older client", ct);
            var noElicitation = await AddServerTagAsync(owner, "No elicitation", ct);

            var first = await DarlingMcpWriteConfirmationTests.CallAsync(server, "delete_server_tag", $"{{\"tag_id\":{older}}}", negotiate: false);
            var second = await DarlingMcpWriteConfirmationTests.CallAsync(
                server, "delete_server_tag", $"{{\"tag_id\":{noElicitation}}}", negotiate: true, elicitation: false);

            Assert.Null(DarlingMcpWriteConfirmationTests.QuestionOf(first));
            Assert.Null(DarlingMcpWriteConfirmationTests.QuestionOf(second));
            Assert.Equal("deleted", StatusOf(first));
            Assert.Equal("deleted", StatusOf(second));
            Assert.Equal(0, await TagCountAsync(owner, older, ct) + await TagCountAsync(owner, noElicitation, ct));
        });
    }

    [Fact]
    public async Task ACallWithAnAgentAction_NeverAsks_WhereTheSameCallWithoutOneDoes()
    {
        await WithStoreAsync(async (server, _, _) =>
        {
            const string agentAction = "{\\\"request_id\\\":1}";

            var plain = await DarlingMcpWriteConfirmationTests.CallAsync(server, "delete_mute_rule", "{\"rule_id\":\"nope\"}", negotiate: true);
            var agent = await DarlingMcpWriteConfirmationTests.CallAsync(
                server, "delete_mute_rule", "{\"rule_id\":\"nope\",\"agent_action\":\"" + agentAction + "\"}", negotiate: true);

            Assert.NotNull(DarlingMcpWriteConfirmationTests.QuestionOf(plain));
            Assert.Null(DarlingMcpWriteConfirmationTests.QuestionOf(agent));
            Assert.Equal("not_found", StatusOf(agent));
        });
    }

    [Fact]
    public async Task ADropPlanGuideThatIsOnlyADryRun_ChangesNothingSoItAsksNothing()
    {
        await WithStoreAsync(async (server, _, _) =>
        {
            var dryRun = await DarlingMcpWriteConfirmationTests.CallAsync(
                server, "drop_plan_guide", "{\"database_name\":\"Sales\",\"name\":\"PG_orders\",\"server_name\":\"no-such-server\"}", negotiate: true);
            var real = await DarlingMcpWriteConfirmationTests.CallAsync(
                server, "drop_plan_guide", "{\"database_name\":\"Sales\",\"name\":\"PG_orders\",\"server_name\":\"no-such-server\",\"dry_run\":false}", negotiate: true);

            Assert.Null(DarlingMcpWriteConfirmationTests.QuestionOf(dryRun));
            Assert.NotNull(DarlingMcpWriteConfirmationTests.QuestionOf(real));
        });
    }

    [Fact]
    public async Task APurge_NamesTheRetentionItApplies_YesQueuesItOnce_NoQueuesNothing()
    {
        await WithStoreAsync(async (server, owner, ct) =>
        {
            const string args = "{\"retention_days\":30}";
            const string queued = "SELECT count(*) FROM config.config_command WHERE command_type = 'purge_now'";

            var asked = await DarlingMcpWriteConfirmationTests.CallAsync(server, "purge_now", args, negotiate: true);
            Assert.Contains("30 days for", DarlingMcpWriteConfirmationTests.QuestionOf(asked), StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync(owner, queued, ct));

            var declined = await DarlingMcpWriteConfirmationTests.CallAsync(
                server, "purge_now", args, negotiate: true, inputResponse: "{\"action\":\"decline\"}");
            Assert.Equal("declined", StatusOf(declined));
            Assert.Equal(0, await ScalarAsync(owner, queued, ct));

            var yes = await DarlingMcpWriteConfirmationTests.CallAsync(server, "purge_now", args, negotiate: true, inputResponse: Yes);
            Assert.True(StatusOf(yes) == "queued", AnswerOf(yes));
            Assert.Equal(1, await ScalarAsync(owner, queued, ct));
        });
    }
}
