/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2431. <c>McpSettings.Load</c> ended in a bare <c>catch</c> whose fallback is MCP OFF, so the same
/// trailing comma #2425 fixed elsewhere also revoked an endpoint the user deliberately configured — and
/// said nothing, anywhere, about that endpoint specifically.
///
/// <para><b>Off is pinned on purpose, and so is the silence being gone.</b> Two of these tests assert the
/// endpoint stays disabled on an unreadable file. That is not the defect; it is the decision. These two
/// keys are consent and address for a TCP listener, an unreadable file supplies neither, and starting on
/// guesses opens a port nobody asked for at an address no client is aimed at. The defect is that nothing
/// distinguished that from a user who never wanted MCP, which is why every other test here is about the
/// <c>Problem</c> string existing and naming what went wrong.</para>
///
/// <para><b>Absent stays silent.</b> A first run has no settings.json, off is correct, and there is no
/// endpoint to have lost. If that case ever starts producing a Problem, the reporting becomes noise on
/// every clean install and gets ignored exactly when it matters.</para>
/// </summary>
public sealed class McpSettingsGuardTests
{
    private static string NewTempDir(string tag)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_mcp_{tag}_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void WriteSettings(string dir, string content) =>
        File.WriteAllText(Path.Combine(dir, "settings.json"), content);

    private static void InTempConfig(string tag, string? content, Action<McpSettings> check)
    {
        var dir = NewTempDir(tag);
        try
        {
            if (content is not null)
            {
                WriteSettings(dir, content);
            }

            check(McpSettings.Load(dir));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The legitimate first run: no file, no endpoint, nothing to say. The control that stops every other
    /// test here from being satisfied by a Load that simply complains about everything.
    /// </summary>
    [Fact]
    public void Load_IsSilentlyOff_WhenThereIsNoFile()
    {
        InTempConfig("absent", null, settings =>
        {
            Assert.False(settings.Enabled);
            Assert.Equal(McpSettings.DefaultPort, settings.Port);
            Assert.Null(settings.Problem);
            Assert.False(settings.DisabledByUnreadableSettings);
        });
    }

    /// <summary>
    /// The other control: an ordinary file still configures the endpoint, and still says nothing. Without
    /// this one, a Load that returned a Problem unconditionally would pass all the reporting tests.
    /// </summary>
    [Fact]
    public void Load_ReadsBothKeys_AndStaysSilent_ForAnOrdinaryFile()
    {
        InTempConfig("ordinary", @"{
  ""alerts_enabled"": true,
  ""mcp_enabled"": true,
  ""mcp_port"": 5199
}", settings =>
        {
            Assert.True(settings.Enabled);
            Assert.Equal(5199, settings.Port);
            Assert.Null(settings.Problem);
        });
    }

    /// <summary>
    /// The reported case. A hand-edited file with one trailing comma — the shape #2418's sample makes
    /// likelier, not rarer — used to return defaults and no signal at all.
    /// </summary>
    [Fact]
    public void Load_SaysWhyTheEndpointIsOff_WhenTheFileIsUnparseable()
    {
        InTempConfig("comma", @"{
  ""mcp_enabled"": true,
  ""mcp_port"": 5199,
}", settings =>
        {
            Assert.True(settings.DisabledByUnreadableSettings);
            Assert.NotNull(settings.Problem);
        });
    }

    /// <summary>
    /// The Problem has to be worth reading, not just non-null. "settings.json is broken" sends someone
    /// through a file they believe is correct; a line and a position is a minute's work, and it is the
    /// reason this reuses #2425's guard rather than growing a second read path with its own message.
    /// </summary>
    [Fact]
    public void Load_NamesTheLineAndPosition_SoTheCommaCanBeFound()
    {
        InTempConfig("position", @"{
  ""mcp_enabled"": true,
  ""mcp_port"": 5199,
}", settings =>
        {
            Assert.NotNull(settings.Problem);
            Assert.Contains("line", settings.Problem, StringComparison.Ordinal);
            Assert.Contains("position", settings.Problem, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// The JSON literal <c>null</c> parses, so it never reached the old catch as a parse failure — the
    /// first TryGetProperty threw on it instead, and landed in the same silent fallback. #2425 routes it to
    /// Unreadable because it is the shape that used to make a save replace the whole document.
    /// </summary>
    [Fact]
    public void Load_SaysWhyTheEndpointIsOff_WhenTheRootIsTheJsonLiteralNull()
    {
        InTempConfig("nullroot", "null", settings =>
        {
            Assert.False(settings.Enabled);
            Assert.True(settings.DisabledByUnreadableSettings);
        });
    }

    /// <summary>
    /// A quoted boolean is the other hand-edit that turns the endpoint off in silence, and the document
    /// parses fine, so the file guard alone does not catch it. The old code reached GetBoolean, threw, and
    /// fell into the same bare catch. Naming the key is the whole value: nothing else in Lite can tell
    /// someone that <c>mcp_enabled</c> in particular is the reason a connection is being refused.
    /// </summary>
    [Fact]
    public void Load_NamesTheKey_WhenMcpEnabledHoldsTheWrongKindOfValue()
    {
        InTempConfig("quotedbool", @"{ ""mcp_enabled"": ""true"" }", settings =>
        {
            Assert.False(settings.Enabled);
            Assert.True(settings.DisabledByUnreadableSettings);
            Assert.Contains("mcp_enabled", settings.Problem!, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// Same for the port, and the case matters more than it looks: the endpoint was explicitly enabled
    /// here, and the only thing wrong is the address. Starting anyway on the 5151 fallback would bind a
    /// port the operator's firewall rule (#2414) does not follow and no client is aimed at, so this stays
    /// off — loudly.
    /// </summary>
    [Fact]
    public void Load_StaysOffAndNamesTheKey_WhenMcpPortHoldsTheWrongKindOfValue()
    {
        InTempConfig("quotedport", @"{ ""mcp_enabled"": true, ""mcp_port"": ""5199"" }", settings =>
        {
            Assert.False(settings.Enabled);
            Assert.True(settings.DisabledByUnreadableSettings);
            Assert.Contains("mcp_port", settings.Problem!, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// The decision, pinned so a later "be helpful and start it anyway" cannot land quietly: a file that
    /// says the endpoint is enabled but cannot be read leaves it OFF. Consent that cannot be parsed is not
    /// consent, and this is the assertion that would go red if someone read the enabled flag out of a
    /// half-parsed document.
    /// </summary>
    [Fact]
    public void Load_FailsClosed_WhenAFileThatEnablesTheEndpointCannotBeParsed()
    {
        InTempConfig("failclosed", @"{
  ""mcp_enabled"": true,
  ""mcp_port"": 5199
  ""alerts_enabled"": true
}", settings =>
        {
            Assert.False(settings.Enabled);
            Assert.True(settings.DisabledByUnreadableSettings);
        });
    }
}

/// <summary>
/// The wiring half of #2431. <c>McpSettings.Load</c> can now say the endpoint was lost, but a signal
/// nobody reads is the same silence with more code in it — and the three readers all live in WPF windows
/// that no test in this suite can instantiate, so the source is the only place the wiring is visible.
///
/// <para>These are shape pins, not text pins: they require that the branch exists and reports, not that
/// it reports any particular wording.</para>
/// </summary>
public sealed class McpEndpointLossReportingTests
{
    /// <summary>
    /// The load path must have no arm that answers a failure with defaults and nothing else. This is the
    /// literal shape that shipped — <c>catch { return new McpSettings(); }</c> — and banning it stops the
    /// fix being undone by someone tidying an unreachable-looking arm back into a bare catch.
    /// </summary>
    [Fact]
    public void McpSettingsLoad_HasNoCatchThatReturnsDefaults()
    {
        var source = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Mcp", "McpSettings.cs")));

        Assert.DoesNotMatch(
            new Regex(@"catch\s*(\([^)]*\))?\s*\{\s*return new McpSettings\(\);"),
            source);
    }

    /// <summary>
    /// The one moment Lite ever knows the endpoint is gone. Before this, the loss took the same
    /// <c>if (!mcpSettings.Enabled) return;</c> as a user who never wanted MCP, and the symptom surfaced
    /// as a refused connection on a different machine against an app reporting itself healthy.
    /// </summary>
    [Fact]
    public void StartMcpServer_ReportsTheLostEndpoint_BeforeItGivesUpQuietly()
    {
        var body = MethodBody(
            File.ReadAllText(FindRepoFile(Path.Combine("Lite", "MainWindow.xaml.cs"))),
            "private async Task StartMcpServerAsync()");

        var lossBranch = body.IndexOf("DisabledByUnreadableSettings", StringComparison.Ordinal);
        Assert.True(lossBranch >= 0,
            "StartMcpServerAsync never asks whether the endpoint is off because settings.json could not be "
                + "read, so an unreadable file is still indistinguishable from a user who left MCP off.");

        var quietReturn = body.IndexOf("if (!mcpSettings.Enabled) return;", StringComparison.Ordinal);
        Assert.True(quietReturn >= 0,
            "The quiet not-enabled return moved; this guard's anchor is stale and it is pinning nothing.");
        Assert.True(lossBranch < quietReturn,
            "The unreadable-settings branch has to come BEFORE the quiet return, or the loss is swallowed "
                + "by the branch that legitimately says nothing.");

        Assert.Contains("AppLogger.Error", body.Substring(lossBranch, quietReturn - lossBranch),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// #2425's startup report is what the person at the keyboard actually sees, and "every setting is at
    /// its default" does not tell them an endpoint on another machine just stopped answering. The report
    /// has to name the capability, not the settings.
    /// </summary>
    [Fact]
    public void TheUnreadableSettingsReport_NamesTheMcpEndpoint()
    {
        var source = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "App.xaml.cs")));

        Assert.Contains("MCP", MethodBody(source, "private static void ReportUnreadableSettings(string? problem)"),
            StringComparison.Ordinal);
        Assert.Contains("MCP", MethodBody(source, "private static void ReportUnreadableSettingsToUser()"),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The Settings window is the one place in the UI that claims to show the endpoint's configuration.
    /// On an unreadable file it shows an unticked box and port 5151, which is a fallback and not a reading
    /// of anything, so the status line beside them must not call that "Disabled".
    /// </summary>
    [Fact]
    public void TheSettingsWindow_DoesNotCallAnUnreadableEndpointDisabled()
    {
        var body = MethodBody(
            File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs"))),
            "private void UpdateMcpStatus()");

        Assert.Contains("_mcpSettingsProblem", body, StringComparison.Ordinal);

        var problemBranch = body.IndexOf("_mcpSettingsProblem != null", StringComparison.Ordinal);
        var disabled = body.IndexOf("\"Status: Disabled\"", StringComparison.Ordinal);
        Assert.True(problemBranch >= 0,
            "UpdateMcpStatus never checks whether settings.json could be read, so an endpoint lost to a "
                + "parse error is reported with the same words as one nobody ever turned on.");
        Assert.True(disabled < 0 || problemBranch < disabled,
            "The unreadable branch has to be reached before the \"Disabled\" wording, or the window agrees "
                + "with the file that the endpoint was never wanted.");
    }

    /// <summary>
    /// A warning that cannot go away is its own defect. Every Save in this window copies an unreadable
    /// settings.json aside and writes a fresh one, and the window stays open afterwards — so a status
    /// line set once at construction would keep telling the user the file cannot be read over a file
    /// they have just fixed, for the rest of the session. Found in review on the first round of #2431.
    /// </summary>
    [Fact]
    public void TheSettingsWindow_ReconsidersTheEndpointStateAfterASave()
    {
        var body = MethodBody(
            File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs"))),
            "private async void SaveButton_Click(");

        Assert.Contains("_mcpSettingsProblem", body, StringComparison.Ordinal);
        Assert.Contains("UpdateMcpStatus()", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Everything between a method's signature and the first line that closes it at method indentation.
    /// Crude on purpose: it is enough to keep an assertion from being satisfied by a match somewhere else
    /// in a two-thousand-line window class, which is the only thing that would make these pins vacuous.
    /// </summary>
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' was not found; this guard's anchor is stale.");

        var end = source.IndexOf("\n    }", start, StringComparison.Ordinal);
        Assert.True(end > start, $"No close found for '{signature}'.");

        return source.Substring(start, end - start);
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
