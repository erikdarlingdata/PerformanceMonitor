using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9639: Lite's create_mute_rule, update_mute_rule, delete_mute_rule and set_mute_rule_enabled. Each call goes through the
/// <see cref="MuteRuleService"/> Manage Mute Rules saves through, over a real <see cref="DuckDbMuteRuleStore"/>, and the result is read
/// back through that store, the table the window reads.
/// </summary>
public sealed class McpMuteRuleWriteTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly DuckDbMuteRuleStore _store;
    private readonly MuteRuleService _service;

    public McpMuteRuleWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpMuteRuleWriteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbMuteRuleStore(_initializer);
        _service = new MuteRuleService(_store, new AppLoggerAdapter<MuteRuleService>());
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private string Archive => _initializer.ArchivePath;

    private static JsonDocument Parse(string json) => JsonDocument.Parse(json);

    private static string Status(string json)
    {
        using var doc = Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    private async Task<string> CreateAsync(string? server = null, string? metric = null, string? database = null, string? reason = null, string? expires = null,
        string? waitType = null) =>
        await McpAlertTools.CreateMuteRuleAsync(_service, Archive, server, metric, database, null, waitType, null, null, reason, expires);

    private async Task<MuteRule> OnlyStoredRuleAsync() => Assert.Single(await _store.LoadAllAsync());

    [Fact]
    public async Task Create_SavesTheRuleTheWindowReads_AndTheRunningServiceMutesWithIt()
    {
        var expires = DateTime.UtcNow.AddHours(2);
        var answer = await CreateAsync("SQLA", "Blocking Detected", "  Sales  ", " planned work ", expires.ToString("o"));

        using var doc = Parse(answer);
        Assert.Equal("created", doc.RootElement.GetProperty("status").GetString());
        var id = doc.RootElement.GetProperty("mute_rule").GetProperty("id").GetString();
        Assert.Equal("delete_mute_rule", doc.RootElement.GetProperty("undo").GetProperty("tool").GetString());
        Assert.Equal(id, doc.RootElement.GetProperty("undo").GetProperty("arguments").GetProperty("rule_id").GetString());

        var stored = await OnlyStoredRuleAsync();
        Assert.Equal(id, stored.Id);
        Assert.True(stored.Enabled);
        Assert.Equal("SQLA", stored.ServerName);
        Assert.Equal("Blocking Detected", stored.MetricName);
        Assert.Equal("Sales", stored.DatabasePattern);
        Assert.Equal("planned work", stored.Reason);
        Assert.Equal(expires, stored.ExpiresAtUtc!.Value, TimeSpan.FromMilliseconds(1));

        Assert.True(_service.IsAlertMuted(new AlertMuteContext { ServerName = "SQLA", MetricName = "Blocking Detected", DatabaseName = "Sales" }));
        Assert.False(_service.IsAlertMuted(new AlertMuteContext { ServerName = "SQLB", MetricName = "Blocking Detected", DatabaseName = "Sales" }));
    }

    [Fact]
    public async Task Create_WithNoFields_MutesEveryAlert_LikeTheWindowsConfirmedSave()
    {
        Assert.Equal("created", Status(await CreateAsync()));

        var stored = await OnlyStoredRuleAsync();
        Assert.True(stored.MatchesEveryAlert);
    }

    [Fact]
    public async Task Create_Twice_AnswersAlreadyExists_AndWritesNothingTheSecondTime()
    {
        var first = await CreateAsync("SQLA", "High CPU", reason: "first");
        var second = await CreateAsync("sqla", "High CPU", reason: "second");

        using var doc = Parse(second);
        Assert.Equal("already_exists", doc.RootElement.GetProperty("status").GetString());
        using var firstDoc = Parse(first);
        Assert.Equal(firstDoc.RootElement.GetProperty("mute_rule").GetProperty("id").GetString(), doc.RootElement.GetProperty("rule_id").GetString());
        Assert.Single(await _store.LoadAllAsync());
    }

    [Fact]
    public async Task Create_ADisabledRuleDoesNotCountAsADuplicate()
    {
        await CreateAsync("SQLA", "High CPU");
        var rule = await OnlyStoredRuleAsync();
        await McpAlertTools.SetMuteRuleEnabledAsync(_service, Archive, rule.Id, false);

        Assert.Equal("created", Status(await CreateAsync("SQLA", "High CPU")));
        Assert.Equal(2, (await _store.LoadAllAsync()).Count);
    }

    [Theory]
    [InlineData("not a date", "not a valid ISO-8601")]
    [InlineData("2001-01-01T00:00:00Z", "already in the past")]
    public async Task Create_RefusesABadExpiry_NamingTheArgument_AndWritesNothing(string expires, string expected)
    {
        var answer = await CreateAsync("SQLA", expires: expires);

        using var doc = Parse(answer);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString()!;
        Assert.Contains("expires_at", message);
        Assert.Contains(expected, message);
        Assert.Empty(await _store.LoadAllAsync());
    }

    [Fact]
    public async Task Create_RefusesAPatternTheMetricNeverCarries_AsTheWindowDropsIt()
    {
        var answer = await CreateAsync("SQLA", "High CPU", waitType: "PAGEIOLATCH");

        using var doc = Parse(answer);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("wait_type_pattern", doc.RootElement.GetProperty("message").GetString());
        Assert.Empty(await _store.LoadAllAsync());

        Assert.Equal("created", Status(await CreateAsync("SQLA", "Poison Wait", waitType: "RESOURCE_SEMAPHORE")));
    }

    [Fact]
    public async Task SetEnabled_FlipsTheFlagAlone_AndAnUnchangedCallWritesNothing()
    {
        await CreateAsync("SQLA", "High CPU");
        var before = await OnlyStoredRuleAsync();

        using (var off = Parse(await McpAlertTools.SetMuteRuleEnabledAsync(_service, Archive, before.Id, false)))
        {
            Assert.Equal("updated", off.RootElement.GetProperty("status").GetString());
            Assert.True(off.RootElement.GetProperty("previous").GetProperty("enabled").GetBoolean());
            Assert.Equal("set_mute_rule_enabled", off.RootElement.GetProperty("undo").GetProperty("tool").GetString());
        }

        var after = await OnlyStoredRuleAsync();
        Assert.False(after.Enabled);
        Assert.Equal(before.CreatedAtUtc, after.CreatedAtUtc);
        Assert.False(_service.IsAlertMuted(new AlertMuteContext { ServerName = "SQLA", MetricName = "High CPU" }));

        Assert.Equal("unchanged", Status(await McpAlertTools.SetMuteRuleEnabledAsync(_service, Archive, before.Id, false)));
        Assert.Equal("updated", Status(await McpAlertTools.SetMuteRuleEnabledAsync(_service, Archive, before.Id, true)));
        Assert.True((await OnlyStoredRuleAsync()).Enabled);
    }

    [Fact]
    public async Task AllFourWrites_AnswerUnavailable_WhileARestoreOfTheTableIsPending_AndChangeNothing()
    {
        await CreateAsync("SQLA", "High CPU");
        var before = await OnlyStoredRuleAsync();
        File.WriteAllText(
            Path.Combine(Archive, PreservedTableRestore.RestoreMarkerFileName),
            "pm_preserve_r6\nconfig_mute_rules\n");

        var answers = new[]
        {
            await CreateAsync("SQLB", "Blocking Detected"),
            await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, before.Id, "{\"reason\":\"changed\"}"),
            await McpAlertTools.SetMuteRuleEnabledAsync(_service, Archive, before.Id, false),
            await McpAlertTools.DeleteMuteRuleAsync(_service, Archive, before.Id),
        };

        foreach (var answer in answers)
        {
            using var doc = Parse(answer);
            Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            Assert.Contains("until Lite restarts", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        var after = await OnlyStoredRuleAsync();
        Assert.Equal(before.Id, after.Id);
        Assert.True(after.Enabled);
        Assert.Equal(before.Reason, after.Reason);
    }

    [Theory]
    [InlineData("missing-id", "not_found")]
    [InlineData("  ", "invalid")]
    public async Task SetEnabled_AndDelete_RefuseAnUnknownOrBlankId(string id, string expected)
    {
        Assert.Equal(expected, Status(await McpAlertTools.SetMuteRuleEnabledAsync(_service, Archive, id, true)));
        Assert.Equal(expected, Status(await McpAlertTools.DeleteMuteRuleAsync(_service, Archive, id)));
    }

    [Fact]
    public async Task Delete_RemovesTheRow_AndItsUndoCreatesTheSameRuleAgain()
    {
        var expires = DateTime.UtcNow.AddDays(1);
        await CreateAsync("SQLA", "Blocking Detected", "Sales", "keep me", expires.ToString("o"));
        var original = await OnlyStoredRuleAsync();

        var answer = await McpAlertTools.DeleteMuteRuleAsync(_service, Archive, original.Id);

        using var doc = Parse(answer);
        Assert.Equal("deleted", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(original.Id, doc.RootElement.GetProperty("previous").GetProperty("mute_rule").GetProperty("id").GetString());
        Assert.Empty(await _store.LoadAllAsync());
        Assert.False(_service.IsAlertMuted(new AlertMuteContext { ServerName = "SQLA", MetricName = "Blocking Detected", DatabaseName = "Sales" }));

        var undo = doc.RootElement.GetProperty("undo");
        Assert.Equal("create_mute_rule", undo.GetProperty("tool").GetString());
        var args = undo.GetProperty("arguments");
        await McpAlertTools.CreateMuteRuleAsync(_service, Archive,
            Arg(args, "server_name"), Arg(args, "metric_name"), Arg(args, "database_pattern"), Arg(args, "query_text_pattern"),
            Arg(args, "wait_type_pattern"), Arg(args, "job_name_pattern"), Arg(args, "client_app_pattern"), Arg(args, "reason"), Arg(args, "expires_at"));

        var recreated = await OnlyStoredRuleAsync();
        Assert.NotEqual(original.Id, recreated.Id);
        Assert.Equal(original.Summary, recreated.Summary);
        Assert.Equal("keep me", recreated.Reason);
        Assert.Equal(original.ExpiresAtUtc!.Value, recreated.ExpiresAtUtc!.Value, TimeSpan.FromMilliseconds(1));
    }

    private static string? Arg(JsonElement args, string name) =>
        args.TryGetProperty(name, out var value) ? value.GetString() : null;

    [Fact]
    public async Task Update_ChangesOnlyTheSentFields_NullClears_AndTheCreationDateNeverMoves()
    {
        await CreateAsync("SQLA", "Blocking Detected", "Sales", "old reason", DateTime.UtcNow.AddHours(3).ToString("o"));
        var before = await OnlyStoredRuleAsync();

        var answer = await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, before.Id,
            "{\"reason\":\"root cause found\",\"expires_at_utc\":null,\"database_pattern\":\"Orders\"}");

        using var doc = Parse(answer);
        Assert.Equal("updated", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("update_mute_rule", doc.RootElement.GetProperty("undo").GetProperty("tool").GetString());
        var after = await OnlyStoredRuleAsync();
        Assert.Equal("root cause found", after.Reason);
        Assert.Equal("Orders", after.DatabasePattern);
        Assert.Null(after.ExpiresAtUtc);
        Assert.Equal("SQLA", after.ServerName);
        Assert.Equal("Blocking Detected", after.MetricName);
        Assert.True(after.Enabled);
        Assert.Equal(before.CreatedAtUtc, after.CreatedAtUtc);

        /* The undo's own arguments put the previous values back. */
        var undoArgs = doc.RootElement.GetProperty("undo").GetProperty("arguments");
        Assert.Equal("updated", Status(await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, undoArgs.GetProperty("rule_id").GetString()!, undoArgs.GetProperty("changes_json").GetString()!)));
        var restored = await OnlyStoredRuleAsync();
        Assert.Equal("old reason", restored.Reason);
        Assert.Equal("Sales", restored.DatabasePattern);
        Assert.Equal(before.ExpiresAtUtc!.Value, restored.ExpiresAtUtc!.Value, TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public async Task Update_SendingTheStoredValues_IsUnchanged_AndWritesNothing()
    {
        await CreateAsync("SQLA", "High CPU", reason: "same");
        var rule = await OnlyStoredRuleAsync();

        Assert.Equal("unchanged", Status(await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, rule.Id, "{\"reason\":\"same\"}")));
    }

    [Fact]
    public async Task Update_TakesCreatesExpiresAtSpelling_AsAnAlias()
    {
        await CreateAsync("SQLA", "High CPU");
        var rule = await OnlyStoredRuleAsync();
        var when = DateTime.UtcNow.AddDays(2);

        Assert.Equal("updated", Status(await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, rule.Id, "{\"expires_at\":\"" + when.ToString("o") + "\"}")));

        Assert.Equal(when, (await OnlyStoredRuleAsync()).ExpiresAtUtc!.Value, TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public async Task Update_UnknownRule_IsNotFound()
    {
        Assert.Equal("not_found", Status(await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, "nope", "{\"reason\":\"x\"}")));
    }

    [Theory]
    [InlineData("{\"enabled\":false}", "enabled")]
    [InlineData("{\"id\":\"x\"}", "'id'")]
    [InlineData("{\"created_at_utc\":\"2020-01-01T00:00:00Z\"}", "created_at_utc")]
    [InlineData("{\"starts_at\":\"2030-01-01T00:00:00Z\"}", "starts_at")]
    [InlineData("{\"recur_start\":\"22:00\"}", "recur_start")]
    [InlineData("{\"action\":\"digest\"}", "action")]
    [InlineData("{\"reason\":\"  \"}", "'reason' is blank")]
    [InlineData("{\"reason\":5}", "'reason' must be a string")]
    [InlineData("{\"expires_at_utc\":\"soon\"}", "expires_at_utc")]
    [InlineData("{\"expires_at\":null,\"expires_at_utc\":null}", "two names for the same field")]
    [InlineData("{\"wait_type_pattern\":\"PAGEIOLATCH\"}", "wait_type_pattern")]
    [InlineData("{}", "No editable")]
    [InlineData("[]", "JSON object")]
    [InlineData("{not json", "changes_json")]
    public async Task Update_RefusesWithAPlainMessage_AndTheStoredRuleIsUntouched(string changes, string expected)
    {
        await CreateAsync("SQLA", "High CPU", reason: "keep");
        var before = await OnlyStoredRuleAsync();

        var answer = await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, before.Id, changes);

        using var doc = Parse(answer);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains(expected, doc.RootElement.GetProperty("message").GetString());
        var after = await OnlyStoredRuleAsync();
        Assert.Equal("keep", after.Reason);
        Assert.Equal(before.Summary, after.Summary);
    }

    [Fact]
    public async Task Update_MovingTheMetric_RefusesAPatternItWouldStrand()
    {
        await CreateAsync("SQLA", "Poison Wait", waitType: "RESOURCE_SEMAPHORE");
        var rule = await OnlyStoredRuleAsync();

        var refused = await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, rule.Id, "{\"metric_name\":\"High CPU\"}");
        Assert.Equal("invalid", Status(refused));
        Assert.Equal("Poison Wait", (await OnlyStoredRuleAsync()).MetricName);

        Assert.Equal("updated", Status(await McpAlertTools.UpdateMuteRuleAsync(_service, Archive, rule.Id, "{\"metric_name\":\"High CPU\",\"wait_type_pattern\":null}")));
        Assert.Equal("High CPU", (await OnlyStoredRuleAsync()).MetricName);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheFourTools_AreListedOnlyWhenTheWritesSwitchIsOn(bool allowWrites)
    {
        using var server = await McpWritePolicyTests.BuildServer(allowWrites);

        var body = await McpWritePolicyTests.PostAsync(server, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");

        foreach (var tool in new[] { "create_mute_rule", "update_mute_rule", "delete_mute_rule", "set_mute_rule_enabled" })
        {
            Assert.Equal(allowWrites, body.Contains("\"name\":\"" + tool + "\"", StringComparison.Ordinal));
        }

        Assert.Contains("\"name\":\"get_mute_rules\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFourTools_AreWrites_ServedOnlyWhenWritesAreOn_AndGetMuteRulesIsNot()
    {
        foreach (var tool in new[] { "create_mute_rule", "update_mute_rule", "delete_mute_rule", "set_mute_rule_enabled" })
        {
            Assert.True(McpWritePolicy.IsWrite(tool), tool);
        }

        Assert.False(McpWritePolicy.IsWrite("get_mute_rules"));
    }
}
