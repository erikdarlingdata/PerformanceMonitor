/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
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
/// The query tag rule tools (#5982, wave 2 of #5971) through the tool methods against a real store: the write gate, each refusal,
/// the test tool over planted sightings (it runs the shared resolver, never a matcher of its own), and the rules version bump.
/// One test per behaviour.
/// </summary>
public sealed class DarlingMcpQueryTagRuleToolsLiveTests
{
    private const int ServerA = 7301;
    private const int ServerB = 7302;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the query tag rule tool pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var source = NpgsqlDataSource.Create(connectionString);
        foreach (var id in new[] { ServerA, ServerB })
        {
            await ExecAsync(source, $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({id}, 'srv{id}', 'srv{id}', TRUE, 16, now(), now())", ct);
        }

        var bodySucceeded = false;
        try
        {
            await body(source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static Task<long> RulesVersionAsync(NpgsqlDataSource source, CancellationToken ct) =>
        CountAsync(source, "SELECT rules_version FROM config.query_tag_state WHERE id = 1", ct);

    private static Task<long> RuleCountAsync(NpgsqlDataSource source, CancellationToken ct) =>
        CountAsync(source, "SELECT count(*) FROM config.query_tag_rules", ct);

    private static async Task<int> NewTagAsync(NpgsqlDataSource source, string name, int? parent, CancellationToken ct) =>
        (int)await CountAsync(source, $"INSERT INTO config.query_tags (name, parent_id) VALUES ('{name}', {(parent is null ? "NULL" : parent.Value.ToString(CultureInfo.InvariantCulture))}) RETURNING id", ct);

    private static Task SightAsync(NpgsqlDataSource source, int server, string key, string? application, string? text, CancellationToken ct, string lastSeen = "now()") =>
        ExecAsync(source,
            "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, application_name, text_sample, last_seen) VALUES " +
            $"({server}, 'hash', '{key}', {(application is null ? "NULL" : "'" + application + "'")}, {(text is null ? "NULL" : "'" + text + "'")}, {lastSeen})", ct);

    private static JsonNode Parse(string json) => JsonNode.Parse(json)!;

    private static string Status(string json) => (string)Parse(json)["status"]!;

    private static string Refusal(string json) => (string)Parse(json)["refusal"]!;

    [Fact]
    public void WithWritesOff_TheThreeRuleWritesAreNotServed_AndTheTwoReadsAre()
    {
        var off = DarlingMcpWritePolicy.Resolve(new McpConfig { AllowWrites = false }, tokenPresent: true);
        var noToken = DarlingMcpWritePolicy.Resolve(new McpConfig { AllowWrites = true }, tokenPresent: false);
        var on = DarlingMcpWritePolicy.Resolve(new McpConfig { AllowWrites = true }, tokenPresent: true);

        foreach (var write in new[] { "create_query_tag_rule", "update_query_tag_rule", "delete_query_tag_rule" })
        {
            Assert.False(off.Offers(write), write + " must not be served with writes off");
            Assert.False(noToken.Offers(write), write + " must not be served without the token");
            Assert.True(on.Offers(write), write + " must be served with writes on and the token");
        }

        foreach (var read in new[] { "list_query_tag_rules", "test_query_tag_rule" })
        {
            Assert.True(off.Offers(read), read + " is an ordinary read");
        }
    }

    [Fact]
    public async Task EachInvalidRule_IsRefusedWithItsCode_AndNothingIsWritten()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);

            Assert.Equal("empty_pattern", Refusal(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "   ", tag, cancellationToken: ct)));
            Assert.Equal("pattern_too_long", Refusal(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", new string('x', 401), tag, cancellationToken: ct)));
            Assert.Equal("text_tag_names_tag", Refusal(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "text_tag", null, tag, cancellationToken: ct)));
            Assert.Equal("tag_missing", Refusal(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "login", "sa", null, cancellationToken: ct)));
            Assert.Equal("invalid_field", Refusal(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "regex", "x", tag, cancellationToken: ct)));

            var saved = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "ok", tag, cancellationToken: ct));
            var id = (int)saved["rule"]!["id"]!;
            Assert.Equal("empty_pattern", Refusal(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"pattern\":\"\"}", ct)));
            Assert.Equal("tag_missing", Refusal(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"tag_id\":null}", ct)));

            Assert.Equal(1, await RuleCountAsync(source, ct));
        });
    }

    [Fact]
    public async Task ARuleForAnUnknownTagOrAnUnknownServer_IsRefused()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);

            var noTag = await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "host", "web*", 9999, cancellationToken: ct);
            Assert.Equal("unknown_tag", Refusal(noTag));

            var noServer = await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "host", "web*", tag, server_id: 9999, cancellationToken: ct);
            Assert.Equal("unknown_server", Refusal(noServer));

            var saved = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "host", "web*", tag, cancellationToken: ct));
            var id = (int)saved["rule"]!["id"]!;
            Assert.Equal("unknown_server", Refusal(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"server_id\":9999}", ct)));
            Assert.Equal("unknown_tag", Refusal(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"tag_id\":9999}", ct)));

            Assert.Equal(1, await RuleCountAsync(source, ct));
        });
    }

    [Fact]
    public async Task ARuleMatchingTwoOfThreeKeys_ReportsTwo_AndSamplesTheirMatchedFieldAndValue()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);
            await SightAsync(source, ServerA, "0xA1", "Billing", null, ct);
            await SightAsync(source, ServerA, "0xA2", "billing-api", null, ct);
            await SightAsync(source, ServerA, "0xA3", "Reports", null, ct);

            var result = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "BILL*", tag_id: tag, cancellationToken: ct));

            Assert.Equal("ok", (string)result["status"]!);
            Assert.Equal(3, (int)result["keys_examined"]!);
            Assert.Equal(2, (int)result["keys_matched"]!);
            Assert.Equal(2, (int)result["would_tag"]!);
            Assert.Equal(0, (int)result["held_by_hand_assignment"]!);
            var samples = result["samples"]!.AsArray();
            Assert.Equal(new[] { "0xA1", "0xA2" }, samples.Select(s => (string)s!["key"]!).OrderBy(k => k, StringComparer.Ordinal).ToArray());
            Assert.All(samples, s => Assert.Equal("application", (string)s!["matched_field"]!));
            Assert.Contains(samples, s => (string)s!["matched_value"]! == "billing-api");
        });
    }

    [Fact]
    public async Task AKeyHeldByAHandAssignment_IsReportedAsHeld_NotAsChanged()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var billing = await NewTagAsync(source, "Billing", null, ct);
            var other = await NewTagAsync(source, "Pinned", null, ct);
            await SightAsync(source, ServerA, "0xA1", "Billing", null, ct);
            await SightAsync(source, ServerA, "0xA2", "Billing", null, ct);
            await ExecAsync(source, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({other}, 'hash', '0xA1', 'test')", ct);

            var result = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "billing", tag_id: billing, cancellationToken: ct));

            Assert.Equal(2, (int)result["keys_matched"]!);
            Assert.Equal(1, (int)result["would_tag"]!);
            Assert.Equal(1, (int)result["held_by_hand_assignment"]!);
            var held = Assert.Single(result["samples"]!.AsArray(), s => (string)s!["key"]! == "0xA1");
            Assert.Equal("held_by_hand_hash", (string)held!["outcome"]!);
            Assert.Equal("Pinned", (string)held["held_tag"]!);
        });
    }

    [Fact]
    public async Task AKeyHeldByARuleThatRanksAhead_IsReportedAsHeldByThatRule_AndASavedRuleTestsByItsId()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var billing = await NewTagAsync(source, "Billing", null, ct);
            var vip = await NewTagAsync(source, "Vip", null, ct);
            await SightAsync(source, ServerA, "0xA1", "Billing-VIP", null, ct);
            await SightAsync(source, ServerA, "0xA2", "Billing-api", null, ct);
            var first = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "*vip", vip, priority: 10, cancellationToken: ct));
            var second = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "billing*", billing, priority: 100, cancellationToken: ct));

            var result = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, rule_id: (int)second["rule"]!["id"]!, cancellationToken: ct));

            Assert.False((bool)result["draft"]!);
            Assert.Equal(2, (int)result["keys_matched"]!);
            Assert.Equal(1, (int)result["would_tag"]!);
            Assert.Equal(1, (int)result["held_by_higher_rule"]!);
            var held = Assert.Single(result["samples"]!.AsArray(), s => (string)s!["outcome"]! == "held_by_rule");
            Assert.Equal((int)first["rule"]!["id"]!, (int)held!["held_by_rule_id"]!);
        });
    }

    [Fact]
    public async Task TheTestReturnsAtMost25Samples_AndStillCountsEveryMatch()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Bulk", null, ct);
            for (var i = 0; i < 30; i++)
            {
                await SightAsync(source, ServerA, $"0x{i:X2}", "bulk-loader", null, ct);
            }

            var result = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "bulk*", tag_id: tag, cancellationToken: ct));

            Assert.Equal(30, (int)result["keys_matched"]!);
            Assert.Equal(25, result["samples"]!.AsArray().Count);
            Assert.True((bool)result["samples_truncated"]!);
        });
    }

    [Fact]
    public async Task TheTestReadsOneServerOrEveryServer_AndOnlyTheWindow()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);
            await SightAsync(source, ServerA, "0xA1", "billing", null, ct);
            await SightAsync(source, ServerB, "0xB1", "billing", null, ct);
            await SightAsync(source, ServerB, "0xB2", "billing", null, ct, lastSeen: "now() - interval '30 days'");

            var all = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "billing", tag_id: tag, cancellationToken: ct));
            var one = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "billing", tag_id: tag, server_id: ServerB, cancellationToken: ct));
            var wide = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "billing", tag_id: tag, window_hours: 24 * 60, cancellationToken: ct));

            Assert.Equal(2, (int)all["keys_matched"]!);
            Assert.Equal(1, (int)one["keys_matched"]!);
            Assert.Equal(3, (int)wide["keys_matched"]!);
        });
    }

    [Fact]
    public async Task ATagCommentRule_TagsByName_AndAnAmbiguousNameMatchesNoTagAndDoesNotThrow()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var left = await NewTagAsync(source, "Left", null, ct);
            var right = await NewTagAsync(source, "Right", null, ct);
            await NewTagAsync(source, "Billing", left, ct);
            await NewTagAsync(source, "Billing", right, ct);
            await NewTagAsync(source, "Reports", null, ct);
            await SightAsync(source, ServerA, "0xC1", null, "/* tag:Billing */ SELECT 1", ct);
            await SightAsync(source, ServerA, "0xC2", null, "-- tag:reports\nSELECT 2", ct);

            var result = Parse(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "text_tag", cancellationToken: ct));

            Assert.Equal("ok", (string)result["status"]!);
            var sample = Assert.Single(result["samples"]!.AsArray());
            Assert.Equal("0xC2", (string)sample!["key"]!);
            Assert.Equal("Reports", (string)sample["tag"]!);
            Assert.Equal("text_tag", (string)sample["matched_field"]!);
            Assert.Equal(1, (int)result["keys_matched"]!);
        });
    }

    [Fact]
    public async Task TheTestWritesNothing_AndEachRuleWriteBumpsTheRulesVersion()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);
            await SightAsync(source, ServerA, "0xA1", "billing", null, ct);

            var start = await RulesVersionAsync(source, ct);
            await DarlingMcpQueryTagRuleTools.TestQueryTagRule(source, field: "application", pattern: "billing", tag_id: tag, cancellationToken: ct);
            Assert.Equal(start, await RulesVersionAsync(source, ct));
            Assert.Equal(0, await RuleCountAsync(source, ct));
            Assert.Equal(0, await CountAsync(source, "SELECT count(*) FROM config.query_tag_map", ct));

            var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "billing", tag, cancellationToken: ct));
            var id = (int)created["rule"]!["id"]!;
            var afterCreate = await RulesVersionAsync(source, ct);
            Assert.True(afterCreate > start);
            Assert.Equal(afterCreate, (long)created["rules_version"]!);
            Assert.Equal(0, await CountAsync(source, "SELECT count(*) FROM config.query_tag_map", ct));

            var updated = Parse(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"priority\":5}", ct));
            var afterUpdate = await RulesVersionAsync(source, ct);
            Assert.True(afterUpdate > afterCreate);
            Assert.Equal(afterUpdate, (long)updated["rules_version"]!);

            var deleted = Parse(await DarlingMcpQueryTagRuleTools.DeleteQueryTagRule(source, id, ct));
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.True(await RulesVersionAsync(source, ct) > afterUpdate);
            Assert.Equal(0, await RuleCountAsync(source, ct));
            Assert.Equal("rule_not_found", Refusal(await DarlingMcpQueryTagRuleTools.DeleteQueryTagRule(source, id, ct)));
        });
    }

    [Fact]
    public async Task DisablingARuleThroughUpdate_KeepsItInTheListWithEnabledFalse()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);
            var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "login", "etl_*", tag, cancellationToken: ct));
            var id = (int)created["rule"]!["id"]!;

            var updated = Parse(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"enabled\":false}", ct));
            Assert.Equal("updated", (string)updated["status"]!);

            var list = Parse(await DarlingMcpQueryTagRuleTools.ListQueryTagRules(source, cancellationToken: ct));
            var rule = Assert.Single(list["rules"]!.AsArray());
            Assert.Equal(id, (int)rule!["id"]!);
            Assert.False((bool)rule["enabled"]!);
            Assert.Equal("etl_*", (string)rule["pattern"]!);
            Assert.Equal("Billing", (string)rule["tag"]!);
        });
    }

    [Fact]
    public async Task AnUpdateChangesOnlyTheNamedFields_AnExplicitNullServerWidensTheRule_AndAnUnknownFieldIsRefused()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = await NewTagAsync(source, "Billing", null, ct);
            var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "host", "web*", tag, priority: 50, server_id: ServerA, cancellationToken: ct));
            var id = (int)created["rule"]!["id"]!;
            Assert.Equal(ServerA, (int)created["rule"]!["server_id"]!);

            var widened = Parse(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"server_id\":null}", ct));
            Assert.Null(widened["rule"]!["server_id"]);
            Assert.Equal(50, (int)widened["rule"]!["priority"]!);
            Assert.Equal("web*", (string)widened["rule"]!["pattern"]!);

            Assert.Equal("unknown_field", Refusal(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"field\":\"login\"}", ct)));
            Assert.Equal("unchanged", Status(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, id, "{\"priority\":50}", ct)));
            Assert.Equal("rule_not_found", Refusal(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, 9999, "{\"priority\":1}", ct)));

            var forServer = Parse(await DarlingMcpQueryTagRuleTools.ListQueryTagRules(source, ServerB, ct));
            Assert.Equal(1, (int)forServer["count"]!);
        });
    }
}
