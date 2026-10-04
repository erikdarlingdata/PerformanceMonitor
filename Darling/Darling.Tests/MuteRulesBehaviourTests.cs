/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Mute Rules page's form logic, from the shipped <c>mute-rules.js</c> run under Node
/// (<c>mute-rules-harness.mjs</c>): which fields a PATCH carries, the explicit-null clear, the empty-create
/// warning and each server response the page handles. Skipped when Node is not installed.
/// </summary>
public sealed class MuteRulesBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "mute-rules-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "mute-rules.js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the mute rules harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the mute rules harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void APatchCarriesOnlyTheChangedFields_AndNeverEnabled()
    {
        if (!TryRun("patch", out var r)) return;

        Assert.Equal("{\"reason\":\"now quiet\"}", r.GetProperty("onlyChanged").GetRawText());
        Assert.Equal("{}", r.GetProperty("nothing").GetRawText());
        Assert.False(r.GetProperty("withEnabled").GetBoolean());
    }

    [Fact]
    public void ABlankedField_IsSentAsAnExplicitNull()
    {
        if (!TryRun("clear", out var r)) return;

        Assert.Equal("{\"server_name\":null}", r.GetProperty("cleared").GetRawText());
        Assert.Equal("{\"expires_at_utc\":null}", r.GetProperty("expiryCleared").GetRawText());
    }

    [Fact]
    public void AnEmptyCreate_IsRecognised_AndATrimmedBodyHoldsOnlyFilledFields()
    {
        if (!TryRun("empty", out var r)) return;

        Assert.True(r.GetProperty("blank").GetBoolean());
        Assert.True(r.GetProperty("reasonOnly").GetBoolean());
        Assert.False(r.GetProperty("scoped").GetBoolean());
        Assert.Equal("{\"server_name\":\"A\",\"metric_name\":\"M\"}", r.GetProperty("body").GetRawText());
        Assert.Contains("mute EVERY alert", r.GetProperty("warning").GetString());
    }

    [Fact]
    public void EachServerResponse_IsInterpretedForThePage()
    {
        if (!TryRun("writes", out var r)) return;

        var exists = r.GetProperty("exists");
        Assert.Equal("exists", exists.GetProperty("kind").GetString());
        Assert.Equal("abc", exists.GetProperty("existingId").GetString());
        var invalid = r.GetProperty("invalid");
        Assert.Equal("invalid", invalid.GetProperty("kind").GetString());
        Assert.Equal("expires_at_utc", invalid.GetProperty("field").GetString());
        Assert.Equal("notfound", r.GetProperty("notfound").GetProperty("kind").GetString());
        Assert.Equal("readonly", r.GetProperty("readonly").GetProperty("kind").GetString());
        Assert.Equal("n1", r.GetProperty("created").GetProperty("rule").GetProperty("id").GetString());
    }

    /* The pin through the product's own matching: the body the page's "Mute this alert" produces is created by
       the real create core and judged by MuteRule.MatchesAt against the context the producer builds for that
       alert (DarlingWorker passes the stored name and the store id; the self-alert family passes its stored label
       and no id). The display name deliberately differs from the stored one. */
    private static async Task<MuteRule> CreateFrom(JsonElement body, int registeredId)
    {
        var store = new FakeMuteRuleStore();
        var result = await DarlingMcpAlertTools.CreateMuteRuleCore(store, body.GetRawText(),
            id => Task.FromResult<string?>(id == registeredId ? "stored-host" : null));
        Assert.Equal("created", DarlingMcpTestData.StatusOf(result));
        var id = (string)JsonNode.Parse(result)!["mute_rule"]!["id"]!;
        return store.Row(id)!;
    }

    [Fact]
    public async Task MuteThisAlert_OnAnEngineRow_MatchesTheProducersContext_AndNotAnotherServer()
    {
        if (!TryRun("bodies", out var r)) return;
        var rule = await CreateFrom(r.GetProperty("engine"), 7);
        var now = DateTime.UtcNow;

        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = "stored-host", ServerId = 7, MetricName = "High CPU" }, now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "stored-host", ServerId = 8, MetricName = "High CPU" }, now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "stored-host", ServerId = 7, MetricName = "Blocking" }, now));

        var narrowed = await CreateFrom(r.GetProperty("engineDetail"), 7);
        Assert.True(narrowed.MatchesAt(new AlertMuteContext { ServerName = "stored-host", ServerId = 7, MetricName = "High CPU", DatabaseName = "Sales", WaitType = "LCK_M_X" }, now));
        Assert.False(narrowed.MatchesAt(new AlertMuteContext { ServerName = "stored-host", ServerId = 7, MetricName = "High CPU", DatabaseName = "Other", WaitType = "LCK_M_X" }, now));
    }

    [Fact]
    public async Task MuteThisAlert_OnASelfAlertRow_MatchesItsStoredLabel_AndNotAnotherServer()
    {
        if (!TryRun("bodies", out var r)) return;
        var rule = await CreateFrom(r.GetProperty("self"), 0);
        var now = DateTime.UtcNow;

        Assert.False(r.GetProperty("self").TryGetProperty("server_id", out _));
        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = "Monitor Store", MetricName = "Disk Pressure" }, now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "other-host", MetricName = "Disk Pressure" }, now));
    }

    [Fact]
    public void ThePrefill_ParsesDetailText_LikeTheDesktop_AndKeysOnTheIdOnlyWhenOneExists()
    {
        if (!TryRun("prefill", out var r)) return;

        var engine = r.GetProperty("engine");
        Assert.Equal(7, engine.GetProperty("server_id").GetInt32());
        Assert.Equal("stored-host", engine.GetProperty("server_name").GetString());
        var self = r.GetProperty("self");
        Assert.Equal(JsonValueKind.Null, self.GetProperty("server_id").ValueKind);
        Assert.Equal("Monitor Store", self.GetProperty("server_name").GetString());
        Assert.Equal("Display Name", r.GetProperty("oldPayload").GetProperty("server_name").GetString());

        var detail = r.GetProperty("detail");
        Assert.Equal("Sales", detail.GetProperty("database_pattern").GetString());
        Assert.Equal("LCK_M_X", detail.GetProperty("wait_type_pattern").GetString());
        Assert.Equal("Nightly", detail.GetProperty("job_name_pattern").GetString());
        Assert.Equal("SELECT 1 FROM t", detail.GetProperty("query_text_pattern").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("custom").GetProperty("database_pattern").ValueKind);
        Assert.Equal(200, r.GetProperty("longQuery").GetInt32());
        Assert.Equal(7, r.GetProperty("body").GetProperty("server_id").GetInt32());
    }

    [Fact]
    public void AnExpiredSession_IsSignInNotReadOnly_AndAnUnparseable2xxIsNotASave()
    {
        if (!TryRun("expiry", out var r)) return;

        Assert.Equal("expired", r.GetProperty("s401").GetProperty("kind").GetString());
        Assert.Equal("readonly", r.GetProperty("s403").GetProperty("kind").GetString());
        Assert.Equal("expired", r.GetProperty("html200").GetProperty("kind").GetString());
        Assert.Equal("expired", r.GetProperty("empty200").GetProperty("kind").GetString());
        Assert.Equal("ok", r.GetProperty("json201").GetProperty("kind").GetString());
        /* The 401 reached the shared sign-in takeover; the 403 did not. */
        Assert.Equal(1, r.GetProperty("after401").GetInt32());
        Assert.Equal(1, r.GetProperty("after403").GetInt32());
    }

    [Fact]
    public void CancellingTheForm_LetsTheSameMuteLinkReopenIt()
    {
        if (!TryRun("reopen", out var r)) return;

        Assert.Equal(JsonValueKind.Null, r.GetProperty("last").ValueKind);
    }
}
