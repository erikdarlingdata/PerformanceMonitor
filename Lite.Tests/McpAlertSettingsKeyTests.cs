/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the key names Lite's <c>get_alert_settings</c> emits, because three of them were RENAMED to match
/// Darling (#1840 review) and a rename with nothing holding it is a rename that comes back. Darling's side
/// has had <c>DarlingMcpAlertToolsTests</c> asserting its shape all along; this is Lite's missing half.
///
/// <para>The old spellings are asserted ABSENT as well as the new ones present. Presence alone would pass
/// just as happily if someone re-added the old key beside the new one, which is the likelier accident than
/// deleting the new one — and for an MCP client, two keys meaning the same thing is its own bug.</para>
///
/// <para>Runtime rather than source-parsing: the payload is an anonymous type handed to
/// <c>JsonSerializer</c> with the SHARED <c>McpHelpers.JsonOptions</c>, and what that turns a C# identifier
/// into is that object's business, not this file's — it carries no naming policy today, and the day it
/// acquires one every key here changes without a line of this payload being touched. Only serializing it
/// actually proves what a client receives.</para>
///
/// <para>#1965: because it is runtime, <see cref="Settings"/> reads whatever the App.Alert* statics hold at
/// that instant, so this class shares the "app-alert-statics" collection with the classes that write them —
/// <c>LiteAlertForwardingTests</c> (direct writes) and <c>AlertSettingsCredentialLoadTests</c> (via
/// App.LoadAlertSettings). xUnit runs separate classes in parallel, and a foreign write of
/// <c>CpuAlertMode.Total</c> landing between this class's set and its assert failed the SqlOnly case. The
/// <c>finally</c> restore below could not help: the window is before the assert, not after it.</para>
///
/// <para>#2394 widened it from pinning three renamed keys to pinning the whole SHAPE. Lite reported four
/// groups where Darling reports nineteen, so the drift this class was written to catch had already happened
/// on a scale no per-key assertion would notice. The parity assertions below therefore DERIVE Darling's shape
/// from Darling's source rather than transcribing it — a hand-copied list of nineteen groups is precisely the
/// artifact that stays green on the day a twentieth arrives.</para>
/// </summary>
[Collection("app-alert-statics")]
public sealed class McpAlertSettingsKeyTests
{
    private static JsonElement Settings()
    {
        /* Static, parameterless, and reads App's static settings - no WPF Application instance needed, and
           the SMTP password probe is internally guarded, so an absent credential store returns null rather
           than faulting the call. */
        var json = McpAlertTools.GetAlertSettings().GetAwaiter().GetResult();
        return JsonDocument.Parse(json).RootElement;
    }

    private static IReadOnlyList<string> KeysOf(JsonElement element) =>
        element.EnumerateObject().Select(p => p.Name).ToList();

    /// <summary>
    /// #1911: the value half of the same alignment. Lite reports <c>cpu.mode</c> in Darling's vocabulary, so
    /// the enum name must never reach the wire — <c>App.AlertCpuMode.ToString()</c> would emit
    /// <c>"Total"</c>/<c>"SqlOnly"</c>, which Darling's <c>update_alert_settings</c> rejects outright.
    /// </summary>
    [Theory]
    [InlineData(CpuAlertMode.Total, "total")]
    [InlineData(CpuAlertMode.SqlOnly, "sql")]
    public void GetAlertSettings_CpuMode_UsesDarlingsLowercaseVocabulary(CpuAlertMode mode, string expected)
    {
        var original = App.AlertCpuMode;
        try
        {
            App.AlertCpuMode = mode;
            var cpu = Settings().GetProperty("cpu");

            Assert.Contains("mode", KeysOf(cpu));
            Assert.Equal(expected, cpu.GetProperty("mode").GetString());

            /* The enum names, asserted absent: emitting one is the specific regression this guards. */
            Assert.NotEqual("Total", cpu.GetProperty("mode").GetString());
            Assert.NotEqual("SqlOnly", cpu.GetProperty("mode").GetString());
        }
        finally
        {
            App.AlertCpuMode = original;
        }
    }

    /// <summary>
    /// The cross-app half, and the reason this is source-parsing: Lite.Tests cannot reference the Darling
    /// Viewer, so the only way to prove the two apps agree on the LITERALS is to read Darling's declaration.
    /// Asserting Lite's two constants alone would keep passing on the day Darling changes its vocabulary,
    /// which is the drift the whole issue was about.
    /// </summary>
    [Fact]
    public void CpuModeVocabulary_IsByteForByteDarlings()
    {
        var darling = File.ReadAllText(FindRepoFile(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs")));

        Assert.Contains($"CpuModeSql = \"{McpAlertTools.CpuModeSql}\"", darling, StringComparison.Ordinal);
        Assert.Contains($"CpuModeTotal = \"{McpAlertTools.CpuModeTotal}\"", darling, StringComparison.Ordinal);

        /* And that Darling's writable surface still accepts exactly these two, so a read from either app
           round-trips through the other's update_alert_settings. */
        var darlingTools = File.ReadAllText(FindRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")));
        Assert.Contains(
            $"AddEnum(\"cpu_mode\", n, \"cpu.mode\", \"{McpAlertTools.CpuModeSql}\", \"{McpAlertTools.CpuModeTotal}\")",
            darlingTools,
            StringComparison.Ordinal);
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

    /// <summary>Darling's one group Lite deliberately does not report. Named once so the omission reads as a
    /// decision in both places that reference it.</summary>
    private const string SelfAlertsGroup = "self_alerts";

    /// <summary>#2417: the MEMBER-level counterpart to <see cref="SelfAlertsGroup"/> — Darling keys Lite
    /// genuinely has no equivalent for, exempted BY NAME so the hole is a decision someone justified here
    /// rather than a loosened assertion. Each entry is paid for by a test asserting the omission is still
    /// real, so an exemption cannot outlive its reason.
    ///
    /// <para>EMPTY, and the emptiness is asserted in
    /// <see cref="GetAlertSettings_ReportsEveryGroupDarlingDoes_SpelledDarlingsWay"/> rather than merely
    /// being true today. Its one entry was <c>ag.disconnect_refire_minutes</c>, Darling's #1696 /
    /// store-V37 knob, which Lite had no equivalent for at all — no static, no settings.json key, and no
    /// edge state that could re-announce a still-disconnected replica. #2426 built the re-fire, so the
    /// exemption came out with it and all four AG members compare. The seam stays for the next such
    /// case: adding an entry means writing the test that pays for it.</para></summary>
    private static readonly string[] LiteOmittedMembers = Array.Empty<string>();

    /// <summary>
    /// Darling's <c>BuildAlertSettingsPayload</c> shape read out of Darling's SOURCE — each top-level key in
    /// document order with its nested keys (an empty list for a scalar like <c>cooldown_minutes</c>). Derived
    /// rather than transcribed for the reason in the class summary, and the same reasoning that makes
    /// <see cref="CpuModeVocabulary_IsByteForByteDarlings"/> read Darling's file instead of asserting Lite's
    /// own constants back at itself.
    /// </summary>
    private static IReadOnlyList<KeyValuePair<string, IReadOnlyList<string>>> DarlingPayloadShape()
    {
        var source = File.ReadAllText(FindRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")));

        /* Comments come out first: the payload carries several, and the "(0 = off)" inside one would
           otherwise scan as a key. Nothing between the anchor and the initializer's closing brace is a
           string literal, so comments are the only C# escape this has to understand. */
        source = Regex.Replace(source, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        source = Regex.Replace(source, "//[^\r\n]*", " ");

        /* Anchored on the DEFINITION rather than the name: get_alert_settings CALLS
           BuildAlertSettingsPayload earlier in the file, so a bare name search would brace-match that call
           site's catch block and silently return the wrong object. */
        var start = source.IndexOf("private static object BuildAlertSettingsPayload", StringComparison.Ordinal);
        Assert.True(start >= 0, "Darling's BuildAlertSettingsPayload definition could not be located.");

        var shape = new List<KeyValuePair<string, IReadOnlyList<string>>>();
        List<string>? nested = null;
        var depth = 0;

        for (var i = source.IndexOf('{', start); i >= 0 && i < source.Length; i++)
        {
            var c = source[i];
            if (c == '{')
            {
                depth++;
                continue;
            }

            if (c == '}')
            {
                depth--;
                if (depth == 0) break;
                continue;
            }

            if ((depth != 1 && depth != 2) || !(char.IsLetter(c) || c == '_')) continue;

            /* The tail of an identifier already consumed, or a member access (s.CpuEnabled) - not a key. */
            var previous = i > 0 ? source[i - 1] : ' ';
            if (char.IsLetterOrDigit(previous) || previous == '_' || previous == '.') continue;

            var end = i;
            while (end < source.Length && (char.IsLetterOrDigit(source[end]) || source[end] == '_')) end++;

            var after = end;
            while (after < source.Length && char.IsWhiteSpace(source[after])) after++;

            /* An identifier followed by a single '=' is an initializer key. Depth 1 opens a group, depth 2
               fills the one it opened; document order makes that association exact without a stack. */
            if (after < source.Length && source[after] == '=' &&
                (after + 1 >= source.Length || source[after + 1] != '='))
            {
                var name = source[i..end];
                if (depth == 1)
                {
                    nested = new List<string>();
                    shape.Add(new KeyValuePair<string, IReadOnlyList<string>>(name, nested));
                }
                else
                {
                    nested?.Add(name);
                }
            }

            i = end - 1;
        }

        /* The harness proves itself before anything is trusted to it. A parse that quietly returned nothing
           would make every assertion built on it vacuously true, which is worse than having no check at all. */
        var groups = shape.Select(g => g.Key).ToList();
        Assert.InRange(shape.Count, 15, 40);
        Assert.Contains("cpu", groups);
        Assert.Contains("analysis", groups);
        Assert.Contains(SelfAlertsGroup, groups);
        Assert.DoesNotContain("smtp", groups);
        Assert.Equal(new[] { "enabled", "threshold_percent", "mode" }, shape.Single(g => g.Key == "cpu").Value);

        return shape;
    }

    /// <summary>
    /// #2394: Lite reported four groups — cpu, blocking, deadlocks, smtp — where Darling reports nineteen,
    /// even though the SHARED alert engine was already evaluating every one of them here through
    /// <c>AppAlertEngineSettings</c>. Nothing about Lite's alerting was narrower; only the MCP surface was, so
    /// an agent triaging a Lite instance could not read whether tempdb-space, low-disk, PVS, file-growth,
    /// long-running-query/job, failed-job, database-state or analysis alerting was even switched on.
    /// <para>Both directions are asserted. A key Lite emits that Darling does not is as much a defect as a
    /// missing one: two spellings of the same setting across the two apps is exactly the #1839/#1911 class of
    /// bug this file exists to stop, and only smtp is a legitimate Lite addition.</para>
    /// </summary>
    [Fact]
    public void GetAlertSettings_ReportsEveryGroupDarlingDoes_SpelledDarlingsWay()
    {
        var darling = DarlingPayloadShape();
        var root = Settings();
        var problems = new List<string>();

        foreach (var (group, darlingKeys) in darling)
        {
            if (group == SelfAlertsGroup) continue;

            if (!root.TryGetProperty(group, out var element))
            {
                problems.Add($"missing group '{group}'");
                continue;
            }

            /* A scalar (cooldown_minutes, excluded_databases) has no nested keys to compare. */
            if (darlingKeys.Count == 0) continue;

            var liteKeys = KeysOf(element);
            problems.AddRange(darlingKeys
                .Where(k => !LiteOmittedMembers.Contains($"{group}.{k}", StringComparer.Ordinal))
                .Except(liteKeys)
                .Select(k => $"missing '{group}.{k}'"));
            problems.AddRange(liteKeys.Except(darlingKeys).Select(k => $"'{group}.{k}' is Lite-only"));
        }

        Assert.True(
            problems.Count == 0,
            "Lite's get_alert_settings has drifted from Darling's shape: " + string.Join("; ", problems));

        /* #2426: nothing is exempted today, and that is asserted rather than merely true — an entry added
           to LiteOmittedMembers without the test that justifies it would silently narrow this comparison,
           which is the drift this whole class exists to stop. */
        Assert.Empty(LiteOmittedMembers);

        /* smtp is Lite's ONE addition — Lite delivers its own email where Darling manages delivery
           credentials outside the settings row. Pinned as an exact set so a second Lite-only group cannot be
           added without this test being the place someone justifies it. */
        Assert.Equal(
            new[] { "smtp" },
            KeysOf(root).Except(darling.Select(g => g.Key)).ToArray());
    }

    /// <summary>
    /// #2426, and the inversion of the exemption that stood here: <c>ag.disconnect_refire_minutes</c> was
    /// the one MEMBER Lite could not report, because it had no AG disconnect re-fire at all — a replica
    /// disconnected for a week announced itself exactly once, where Darling re-announced it. Both halves
    /// of the old exemption are now asserted the other way round. Darling must still emit it (or the
    /// comparison is over a key nobody publishes), and Lite must too, carrying its own live value rather
    /// than the constant 0 that was rejected for telling an agent it can tune something the app cannot.
    ///
    /// <para>The value assertion is the half that matters most and the half a key-presence check would
    /// miss entirely: it is what distinguishes a real knob from the placeholder this PR exists to avoid
    /// shipping.</para>
    /// </summary>
    [Fact]
    public void GetAlertSettings_ReportsAgDisconnectRefire_WithLitesOwnValue()
    {
        var original = App.AgDisconnectRefireMinutes;
        try
        {
            App.AgDisconnectRefireMinutes = 17;

            Assert.Contains("disconnect_refire_minutes", DarlingPayloadShape().Single(g => g.Key == "ag").Value);

            var ag = Settings().GetProperty("ag");
            Assert.Contains("disconnect_refire_minutes", KeysOf(ag));
            Assert.Equal(17, ag.GetProperty("disconnect_refire_minutes").GetInt32());
        }
        finally
        {
            App.AgDisconnectRefireMinutes = original;
        }
    }

    /// <summary>
    /// The one group Lite deliberately does NOT report, asserted so the hole reads as a decision rather than
    /// the oversight it would otherwise look like. <c>AppAlertEngineSettings</c> returns shipped constants for
    /// three of self_alerts' four members precisely because a single-instance WPF app has no headless store
    /// volume and no fleet collection loop to self-monitor, and Lite has no concept whatsoever of the fourth,
    /// store_job_cadence_warn_percent. Reporting constants under names that read as knobs would tell an agent
    /// it can tune something Lite cannot.
    /// </summary>
    [Fact]
    public void GetAlertSettings_OmitsSelfAlerts_WhichLiteHasNoEquivalentFor()
    {
        Assert.Contains(SelfAlertsGroup, DarlingPayloadShape().Select(g => g.Key));
        Assert.DoesNotContain(SelfAlertsGroup, KeysOf(Settings()));
    }

    /// <summary>
    /// The value half for <c>delivery.mode</c> — the second value-level alignment after <c>cpu.mode</c>, and
    /// the one place <c>ToString()</c> is the right answer rather than a mapping: <see cref="AlertNotificationMode"/>
    /// is the SHARED enum both SKUs run on, and Darling's store holds literally its <c>ToString()</c>, so the
    /// two apps cannot drift the way Lite's app-local <c>CpuAlertMode</c> could.
    /// <para>Pinned against Darling's ACCEPTED vocabulary rather than against the enum, because a rename that
    /// moved the shared enum would move Lite's emitted value with it and an enum-derived assertion would
    /// happily follow — while Darling's validator, which holds the two names as literals, would not.</para>
    /// </summary>
    [Theory]
    [InlineData(AlertNotificationMode.Summary, "Summary")]
    [InlineData(AlertNotificationMode.PerEvent, "PerEvent")]
    public void GetAlertSettings_DeliveryMode_IsDarlingsAcceptedVocabulary(AlertNotificationMode mode, string expected)
    {
        var original = App.AlertDeliveryMode;
        try
        {
            App.AlertDeliveryMode = mode;

            Assert.Equal(expected, Settings().GetProperty("delivery").GetProperty("mode").GetString());

            var darlingTools = File.ReadAllText(FindRepoFile(Path.Combine(
                "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")));
            Assert.Contains(
                "AddEnum(\"delivery_mode\", n, \"delivery.mode\", \"Summary\", \"PerEvent\")",
                darlingTools,
                StringComparison.Ordinal);
        }
        finally
        {
            App.AlertDeliveryMode = original;
        }
    }

    [Fact]
    public void GetAlertSettings_TopLevelMasterSwitch_IsAlertsEnabled()
    {
        var keys = KeysOf(Settings());

        /* alerts_enabled is Darling's name for the master switch. The old notifications_enabled did not
           merely differ - Darling uses that name for the ANALYSIS section's own toggle, so the same key
           meant two things depending on which app answered. */
        Assert.Contains("alerts_enabled", keys);
        Assert.DoesNotContain("notifications_enabled", keys);
    }

    [Fact]
    public void GetAlertSettings_BlockingKeys_MatchDarlingsSpelling()
    {
        var keys = KeysOf(Settings().GetProperty("blocking"));

        Assert.Contains("count_threshold", keys);
        Assert.Contains("wait_threshold_seconds", keys);

        /* threshold_seconds was the #1839 bug (a COUNT under a seconds name); threshold_count was the
           short-lived fix that #1840's review replaced with Darling's existing spelling. */
        Assert.DoesNotContain("threshold_count", keys);
        Assert.DoesNotContain("threshold_seconds", keys);
    }

    [Fact]
    public void GetAlertSettings_DeadlockThreshold_IsCountThreshold()
    {
        var keys = KeysOf(Settings().GetProperty("deadlocks"));

        Assert.Contains("count_threshold", keys);
        Assert.DoesNotContain("threshold", keys);
    }

    [Fact]
    public void GetAlertSettings_ShapeSurvives_SoAClientCanStillFindTheRest()
    {
        var root = Settings();

        /* The keys NOT renamed, pinned so a future edit to the payload cannot quietly drop one while the
           three assertions above stay green. */
        Assert.Contains("notify_connection_changes", KeysOf(root));
        Assert.Contains("threshold_percent", KeysOf(root.GetProperty("cpu")));
        Assert.Contains("enabled", KeysOf(root.GetProperty("cpu")));
        Assert.Contains("password_configured", KeysOf(root.GetProperty("smtp")));
    }
}
