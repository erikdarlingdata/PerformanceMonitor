/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's threshold-overrides logic (#5916 part 4), with a stand-in for the store: an entry the tool would refuse (a key
/// outside the registry, a self-monitoring or enable key, a value outside the key's bounds, a database on a server-only key, a
/// value that is not a whole number, no server) is refused with a sentence and the store is never called; a good entry is written
/// once with the login shaped to what the store accepts; a read-only seat's refusal is a sentence, not a crash.
/// </summary>
public sealed class ThresholdOverridesViewModelTests
{
    private sealed class FakeGateway : IThresholdOverrideGateway
    {
        public List<string> Calls { get; } = new();
        public bool ReadOnly { get; set; }

        public Task<IReadOnlyList<ThresholdOverrideRow>> ListThresholdOverridesAsync(CancellationToken cancellationToken = default)
        {
            Calls.Add("list");
            IReadOnlyList<ThresholdOverrideRow> rows = new[]
            {
                new ThresholdOverrideRow(7, "srv-7", null, "cpu.threshold_percent", 95, 80, "erik", null, new DateTime(2026, 10, 16, 0, 0, 0, DateTimeKind.Utc)),
                new ThresholdOverrideRow(7, "srv-7", "shop", "pvs.threshold_percent", 70, null, "agent:a", Guid.Parse("0f1e2d3c-0000-0000-0000-000000000000"), new DateTime(2026, 10, 16, 0, 0, 0, DateTimeKind.Utc)),
            };
            return Task.FromResult(rows);
        }

        public Task SaveThresholdOverrideAsync(int serverId, string? database, string key, int value, string setBy, CancellationToken cancellationToken = default)
        {
            if (ReadOnly)
                throw new ViewerReadOnlyException(new InvalidOperationException("42501"));
            Calls.Add($"save {serverId}|{database}|{key}|{value}|{setBy}");
            return Task.CompletedTask;
        }

        public Task RemoveThresholdOverrideAsync(int serverId, string? database, string key, CancellationToken cancellationToken = default)
        {
            if (ReadOnly)
                throw new ViewerReadOnlyException(new InvalidOperationException("42501"));
            Calls.Add($"remove {serverId}|{database}|{key}");
            return Task.CompletedTask;
        }
    }

    [Theory]
    [InlineData(null, "cpu.threshold_percent", "90", null, "Choose a server")]
    [InlineData(7, "cpu.nonsense", "90", null, "not a tunable threshold")]
    [InlineData(7, "", "90", null, "key is required")]
    [InlineData(7, "self_alerts.disk_free", "90", null, "self-monitoring")]
    [InlineData(7, "cpu.enabled", "1", null, "enable flag")]
    [InlineData(7, "cpu.cooldown_minutes", "5", null, "cooldown")]
    [InlineData(7, "cpu.threshold_percent", "101", null, "between 1 and 100")]
    [InlineData(7, "cpu.threshold_percent", "0", null, "between 1 and 100")]
    [InlineData(7, "cpu.threshold_percent", "-5", null, "between 1 and 100")]
    [InlineData(7, "cpu.threshold_percent", "ninety", null, "whole number")]
    [InlineData(7, "cpu.threshold_percent", "", null, "whole number")]
    [InlineData(7, "cpu.threshold_percent", "9.5", null, "whole number")]
    [InlineData(7, "cpu.threshold_percent", "90", "shop", "server override only")]
    [InlineData(7, "long_running_job.multiplier", "21", null, "between 2 and 20")]
    public async Task ARefusedEntry_SaysWhy_AndNeverReachesTheStore(int? serverId, string key, string value, string? database, string expected)
    {
        var gateway = new FakeGateway();
        var model = new ThresholdOverridesViewModel(gateway, "erik");

        var outcome = await model.SaveAsync(serverId, key, value, database, TestContext.Current.CancellationToken);

        Assert.False(outcome.Ok);
        Assert.Contains(expected, outcome.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(gateway.Calls);
    }

    [Fact]
    public async Task AGoodEntry_IsWrittenOnce_WithTheLoginShapedForTheStore_AndADatabaseOnlyWhereTheKeyTakesOne()
    {
        var gateway = new FakeGateway();
        var model = new ThresholdOverridesViewModel(gateway, "CORP\\erik darling");

        Assert.True((await model.SaveAsync(7, "cpu.threshold_percent", " 95 ", "  ", TestContext.Current.CancellationToken)).Ok);
        Assert.True((await model.SaveAsync(7, "pvs.threshold_percent", "70", " shop ", TestContext.Current.CancellationToken)).Ok);
        Assert.True((await model.SaveAsync(8, "blocking.pg_count_threshold", "4", null, TestContext.Current.CancellationToken)).Ok);

        Assert.Equal(
            new[]
            {
                "save 7||cpu.threshold_percent|95|CORP_erik_darling",
                "save 7|shop|pvs.threshold_percent|70|CORP_erik_darling",
                "save 8||blocking.pg_count_threshold|4|CORP_erik_darling",
            },
            gateway.Calls);
    }

    [Fact]
    public async Task EveryKeyTheFormOffers_IsOneTheToolWouldAccept_AtItsOwnBounds()
    {
        foreach (var key in ThresholdOverridesViewModel.Keys)
        {
            Assert.True(PerformanceMonitor.Alerting.DarlingThresholdKeys.TryGet(key, out var entry), key);
            Assert.Null(ThresholdOverridesViewModel.Validate(7, key, entry.Min.ToString(System.Globalization.CultureInfo.InvariantCulture), null, out _));
            Assert.Null(ThresholdOverridesViewModel.Validate(7, key, entry.Max.ToString(System.Globalization.CultureInfo.InvariantCulture), null, out _));
        }

        await Task.CompletedTask;
    }

    [Fact]
    public async Task ARemove_NeedsARow_AndNamesTheRowItRemoves()
    {
        var gateway = new FakeGateway();
        var model = new ThresholdOverridesViewModel(gateway, "erik");

        var none = await model.RemoveAsync(null, TestContext.Current.CancellationToken);
        Assert.False(none.Ok);
        Assert.Empty(gateway.Calls);

        var rows = await model.LoadAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);
        Assert.Equal("Whole server", rows[0].DatabaseText);
        Assert.Equal("95", rows[0].ValueText);
        Assert.Equal("80", rows[0].GlobalValueText);
        Assert.Equal("", rows[1].GlobalValueText);
        Assert.Equal("0f1e2d3c", rows[1].RequestText);

        Assert.True((await model.RemoveAsync(rows[1], TestContext.Current.CancellationToken)).Ok);
        Assert.Equal("remove 7|shop|pvs.threshold_percent", gateway.Calls.Last());
    }

    [Fact]
    public async Task AReadOnlySeat_GetsASentence_NotAnException()
    {
        var gateway = new FakeGateway { ReadOnly = true };
        var model = new ThresholdOverridesViewModel(gateway, "erik");

        var save = await model.SaveAsync(7, "cpu.threshold_percent", "90", null, TestContext.Current.CancellationToken);
        Assert.False(save.Ok);
        Assert.Contains("read-only", save.Message, StringComparison.OrdinalIgnoreCase);

        var rows = await model.LoadAsync(TestContext.Current.CancellationToken);
        var remove = await model.RemoveAsync(rows[0], TestContext.Current.CancellationToken);
        Assert.False(remove.Ok);
        Assert.Contains("read-only", remove.Message, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// The Admin page's Threshold Overrides tab (#5916 part 4), run under Node through the Admin page harness: the rows the list
/// read becomes, the entries the form refuses before sending, the words each answer gets (a 403 under a viewer login carries the
/// service's admin-login sentence through), and who gets the form and the Remove buttons (a login that can write).
/// </summary>
[Trait("Cost", "Slow")]
public sealed class ThresholdOverrideAdminTabTests
{
    private static JsonElement Pure(string fn, params object?[] args) =>
        AdminServerEditBehaviourTests.Run("pure", JsonSerializer.Serialize(new { fn, args })).GetProperty("result");

    private static readonly JsonNode Keys = JsonNode.Parse("""
        [{"key":"cpu.threshold_percent","min":1,"max":100,"database_scope":false},
         {"key":"pvs.threshold_percent","min":0,"max":100,"database_scope":true}]
        """)!;

    private static JsonElement Validate(string server, string key, string value, string database) =>
        Pure("validateOverride", JsonSerializer.Deserialize<JsonElement>(Keys.ToJsonString()), new { server, key, value, database });

    [Fact]
    public void TheRows_NameTheServerDatabaseAndGlobalValue_AndAWholeServerOverrideSaysSo_Pure()
    {
        var data = JsonNode.Parse("""
            {"overrides":[
              {"server_id":7,"server_name":"srv-7","database":null,"key":"cpu.threshold_percent","value":95,"global_value":80,"set_by":"erik","request_id":null,"updated_at":"2026-10-16T00:00:00.0000000Z"},
              {"server_id":7,"server_name":"srv-7","database":"shop","key":"pvs.threshold_percent","value":70,"global_value":null,"set_by":"agent:a","request_id":"0f1e2d3c-0000-0000-0000-000000000000","updated_at":"2026-10-16T00:00:00.0000000Z"}
            ]}
            """);
        var rows = Pure("overrideRows", JsonSerializer.Deserialize<JsonElement>(data!.ToJsonString())).EnumerateArray().ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal("Whole server", rows[0].GetProperty("database_text").GetString());
        Assert.Equal(80, rows[0].GetProperty("global_value").GetInt32());
        Assert.Equal("shop", rows[1].GetProperty("database_text").GetString());
        Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("global_value").ValueKind);
        Assert.Equal("0f1e2d3c", rows[1].GetProperty("request").GetString());
    }

    [Theory]
    [InlineData("", "cpu.threshold_percent", "90", "", "Choose a server")]
    [InlineData("srv-7", "cpu.nonsense", "90", "", "tunable threshold")]
    [InlineData("srv-7", "cpu.threshold_percent", "101", "", "between 1 and 100")]
    [InlineData("srv-7", "cpu.threshold_percent", "0", "", "between 1 and 100")]
    [InlineData("srv-7", "cpu.threshold_percent", "9.5", "", "whole number")]
    [InlineData("srv-7", "cpu.threshold_percent", "abc", "", "whole number")]
    [InlineData("srv-7", "cpu.threshold_percent", "90", "shop", "server override only")]
    public void AnEntryTheServiceWouldRefuse_IsRefusedBeforeAnythingIsSent_Pure(string server, string key, string value, string database, string expected)
    {
        Assert.Contains(expected, Validate(server, key, value, database).GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AGoodEntry_PassesTheFormCheck_Pure()
    {
        Assert.Equal(JsonValueKind.Null, Validate("srv-7", "cpu.threshold_percent", "90", "").ValueKind);
        Assert.Equal(JsonValueKind.Null, Validate("srv-7", "pvs.threshold_percent", "70", "shop").ValueKind);
    }

    [Fact]
    public void TheAnswers_GetPlainWords_AndAViewerLoginsRefusalCarriesTheAdminLoginSentence_Pure()
    {
        var refusal = "Changing a threshold override needs a store login with the admin role. Managed and compose stores connect the web dashboard as the viewer role.";
        var results = AdminServerEditBehaviourTests.Run("pure", JsonSerializer.Serialize(new
        {
            calls = new object[]
            {
                new { fn = "overrideOutcome", args = new object[] { "set", new { status = 200, body = new { status = "ok" } } } },
                new { fn = "overrideOutcome", args = new object[] { "clear", new { status = 200, body = new { status = "cleared" } } } },
                new { fn = "overrideOutcome", args = new object[] { "set", new { status = 403, body = new { error = refusal } } } },
                new { fn = "overrideOutcome", args = new object[] { "set", new { status = 400, body = new { error = "cpu.threshold_percent must be an integer between 1 and 100." } } } },
                new { fn = "overrideOutcome", args = new object[] { "set", new { status = 500, body = new { error = "The store did not answer." } } } },
            },
        })).GetProperty("results").EnumerateArray().ToList();

        Assert.False(results[0].GetProperty("isError").GetBoolean());
        Assert.True(results[0].GetProperty("reread").GetBoolean());
        Assert.Contains("removed", results[1].GetProperty("message").GetString());
        Assert.True(results[2].GetProperty("isError").GetBoolean());
        Assert.Equal(refusal, results[2].GetProperty("message").GetString());
        Assert.False(results[2].GetProperty("reread").GetBoolean());
        Assert.Contains("between 1 and 100", results[3].GetProperty("message").GetString());
        Assert.DoesNotContain("answered", results[3].GetProperty("message").GetString());
        Assert.Contains("answered 500", results[4].GetProperty("message").GetString());
    }

    [Fact]
    public void OnlyALoginThatCanWrite_GetsTheChangeColumn_Pure()
    {
        var readOnly = Pure("overrideColumns", false).EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToList();
        var admin = Pure("overrideColumns", true).EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToList();
        Assert.DoesNotContain("actions", readOnly);
        Assert.Equal("actions", admin.Last());
        Assert.Equal(readOnly.Count + 1, admin.Count);
    }
}
