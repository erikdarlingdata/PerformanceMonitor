/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7656: the web Admin page's Slack bot token form. The page's rules run under Node (<c>slack-bot-tokens-harness.mjs</c>): a typed token
/// posts under its field, Remove posts the clear marker, a blank box posts nothing, and the outcome says set or not set in the page's own
/// words, never a value the answer carried. The service side (<see cref="DarlingWebSlackBotTokens"/>) seals each token for its slot on the
/// settings row, refuses saved text and unknown fields by name, and answers presence only. Skipped when Node is not installed.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class SlackBotTokenWebBehaviourTests
{
    private const string TypedBot = "xoxb-9-harness-bot-not-real";
    private const string TypedApp = "xapp-1-harness-app-not-real";

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            UseShellExecute = false,
        };
        psi.Environment["TZ"] = "UTC";
        psi.ArgumentList.Add(Path.Combine(RepoRoot(), "Darling", "Darling.Tests", "slack-bot-tokens-harness.mjs"));
        psi.ArgumentList.Add(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var stdout = proc.StandardOutput.ReadToEnd();
            var stderr = proc.StandardError.ReadToEnd();
            proc.WaitForExit();
            Assert.True(proc.ExitCode == 0, "The harness failed: " + stderr);
            return JsonDocument.Parse(stdout).RootElement;
        }
    }

    [Fact]
    public void ThePayload_PostsATypedToken_TheClearMarkerForRemove_AndNothingForABlankBox()
    {
        var r = Run("payload");

        Assert.Equal(TypedBot, r.GetProperty("typed").GetProperty("slack_bot_token").GetString());
        Assert.Equal(TypedApp, r.GetProperty("typed").GetProperty("slack_app_token").GetString());
        Assert.Equal(TypedBot, r.GetProperty("botOnly").GetProperty("slack_bot_token").GetString());
        Assert.False(r.GetProperty("botOnly").TryGetProperty("slack_app_token", out _));
        Assert.Equal("-", r.GetProperty("cleared").GetProperty("slack_bot_token").GetString());
        Assert.Equal("-", r.GetProperty("cleared").GetProperty("slack_app_token").GetString());
        Assert.Equal(TypedBot, r.GetProperty("typedWins").GetProperty("slack_bot_token").GetString());
        Assert.Empty(r.GetProperty("blank").EnumerateObject());
    }

    [Fact]
    public void TheOutcome_SaysSetOrNotSet_InThePagesWords_AndNeverRepeatsWhatTheAnswerCarried()
    {
        var r = Run("outcome");

        Assert.False(r.GetProperty("ok").GetProperty("isError").GetBoolean());
        Assert.Equal("Slack bot token: set. Slack app-level token: not set.", r.GetProperty("ok").GetProperty("message").GetString());
        Assert.Equal("Nothing differs from the saved Slack bot tokens.", r.GetProperty("unchanged").GetProperty("message").GetString());
        Assert.True(r.GetProperty("refused").GetProperty("isError").GetBoolean());
        Assert.Equal("This sign-in cannot change the Slack bot tokens.", r.GetProperty("refused").GetProperty("message").GetString());
        Assert.Equal("The Slack bot tokens were not saved. Unknown field: x.", r.GetProperty("bad").GetProperty("message").GetString());
        Assert.Equal("Network error.", r.GetProperty("network").GetProperty("message").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("expired").GetProperty("message").ValueKind);
        var echo = r.GetProperty("echo").GetRawText();
        Assert.DoesNotContain(TypedBot, echo, StringComparison.Ordinal);
        Assert.DoesNotContain(TypedApp, echo, StringComparison.Ordinal);
        Assert.Equal("Slack bot token: set. Slack app-level token: set.", r.GetProperty("echo").GetProperty("message").GetString());
    }

    [Fact]
    public void TheWrite_SealsEachTypedToken_ForItsSlotOnTheSettingsRow_AndCarriesNoTokenText()
    {
        using var key = PasswordPrivateKey.Generate();
        var body = new JsonObject { ["slack_bot_token"] = " " + TypedBot + " ", ["slack_app_token"] = "-" };

        var (columns, values, error) = DarlingWebSlackBotTokens.BuildWrite(body, DarlingPasswordKey.FromPrivateKey(key));

        Assert.Null(error);
        Assert.Equal(new[] { "slack_bot_token", "slack_app_token" }, columns);
        Assert.True(PasswordSeal.IsSealed(values[0]));
        Assert.DoesNotContain(TypedBot, values[0], StringComparison.Ordinal);
        Assert.Equal(TypedBot, PasswordSeal.Open(values[0], key, PasswordBinding.ForWebhook("slack_bot", PasswordBinding.WebhookSettingsRow, null, null)));
        Assert.Equal("", values[1]);
    }

    [Fact]
    public void TheWrite_RefusesSavedText_AnUnknownField_ANonTextValue_AndNoKey_ByName()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);

        var saved = DarlingWebSlackBotTokens.BuildWrite(new JsonObject { ["slack_app_token"] = "sealed:v1:0123456789abcdef:AAAA" }, ring);
        Assert.Equal("The Slack app-level token must be the value itself, not saved text.", saved.Error);

        var unknown = DarlingWebSlackBotTokens.BuildWrite(new JsonObject { ["slack_bot_token"] = TypedBot, ["slack_url"] = "x" }, ring);
        Assert.Equal("Unknown field: slack_url.", unknown.Error);

        var number = DarlingWebSlackBotTokens.BuildWrite(new JsonObject { ["slack_bot_token"] = 5 }, ring);
        Assert.Equal("slack_bot_token must be text.", number.Error);

        var noKey = DarlingWebSlackBotTokens.BuildWrite(new JsonObject { ["slack_bot_token"] = TypedBot }, DarlingPasswordKey.Refusing("The service has published no key."));
        Assert.Equal("The service has published no key.", noKey.Error);
        Assert.DoesNotContain(TypedBot, noKey.Error, StringComparison.Ordinal);

        var blank = DarlingWebSlackBotTokens.BuildWrite(new JsonObject { ["slack_bot_token"] = "  ", ["slack_app_token"] = null }, ring);
        Assert.Null(blank.Error);
        Assert.Empty(blank.Columns);
    }

    [Fact]
    public void TheAnswer_CarriesPresenceOnly()
    {
        var answer = DarlingWebSlackBotTokens.Answer(new[] { "slack_bot_token", "slack_app_token" }, new bool?[] { false, true }, new[] { "sealed:v1:0123456789abcdef:AAAA", "" });

        Assert.Equal("updated", answer["status"]!.GetValue<string>());
        Assert.Equal("not set", answer["secrets"]!["slack_bot_token"]!["before"]!.GetValue<string>());
        Assert.Equal("set", answer["secrets"]!["slack_bot_token"]!["after"]!.GetValue<string>());
        Assert.Equal("set", answer["secrets"]!["slack_app_token"]!["before"]!.GetValue<string>());
        Assert.Equal("not set", answer["secrets"]!["slack_app_token"]!["after"]!.GetValue<string>());
        Assert.DoesNotContain("sealed:", answer.ToJsonString(), StringComparison.Ordinal);
    }
}
