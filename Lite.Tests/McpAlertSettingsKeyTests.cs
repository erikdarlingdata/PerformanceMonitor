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

    /// <summary>
    /// #3653 (from #3541): <c>poison_wait.threshold_ms</c> is reported but consulted by nothing since #3593
    /// moved the shared engine to accumulated wait over a window. The key stays and a note sits beside it on
    /// the wire. Cross-app for the reason <see cref="CpuModeVocabulary_IsByteForByteDarlings"/> is: Lite
    /// cannot reference the service assembly, so Darling's declaration is READ, and the day either app
    /// rewords the note the other fails here rather than the two saying different things about one knob.
    /// The description carries the same fact for the agent who reads it before calling.
    /// </summary>
    [Fact]
    public void PoisonWaitThresholdMs_CarriesDarlingsRetirementNote_ByteForByte()
    {
        var poison = Settings().GetProperty("poison_wait");
        Assert.Contains("threshold_ms", KeysOf(poison));
        Assert.Equal(McpAlertTools.PoisonWaitThresholdMsNote, poison.GetProperty("threshold_ms_note").GetString());
        Assert.StartsWith("retired by #3593", McpAlertTools.PoisonWaitThresholdMsNote, StringComparison.Ordinal);

        var darlingTools = File.ReadAllText(FindRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")));
        var declaration = Regex.Match(darlingTools, @"PoisonWaitThresholdMsNote\s*=\s*""([^""]*)"";");
        Assert.True(declaration.Success, "Darling's PoisonWaitThresholdMsNote declaration could not be located.");
        Assert.Equal(McpAlertTools.PoisonWaitThresholdMsNote, declaration.Groups[1].Value);

        var description = typeof(McpAlertTools).GetMethod(nameof(McpAlertTools.GetAlertSettings))!
            .GetCustomAttributes(typeof(System.ComponentModel.DescriptionAttribute), false)
            .Cast<System.ComponentModel.DescriptionAttribute>().Single().Description;
        Assert.Contains("poison_wait.threshold_ms is RETIRED (#3593)", description, StringComparison.Ordinal);
        Assert.Contains("threshold_ms_note", description, StringComparison.Ordinal);
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

    /// <summary>A group Darling reports and Lite deliberately does not. Named once so the omission reads as a
    /// decision in both places that reference it. Stated without a count, for the reason
    /// <see cref="GetAlertSettings_OmitsSelfAlerts_WhichLiteHasNoEquivalentFor"/> gives about numerals in this
    /// file: there is now more than one, and a numeral here would go stale without going red.</summary>
    private const string SelfAlertsGroup = "self_alerts";

    /// <summary>#3466 (V124): the third group Darling reports and Lite deliberately does not. The fleet
    /// sweep is a scheduled report about a FLEET — what changed across the monitored population since the
    /// last sweep — and Lite monitors one server from one desktop with no fleet to summarize: nothing in
    /// this SKU references the sweep engine, the sweep store, or the cadence constants, which is checkable
    /// and checked (<see cref="GetAlertSettings_OmitsFleetSweep_BecauseLiteHasNoFleetToSweep"/>). Emitting
    /// <c>fleet_sweep.enabled</c>/<c>fleet_sweep.interval_minutes</c> here would advertise knobs this
    /// product cannot act on — the placeholder failure this file's history rejected for
    /// <c>ag.disconnect_refire_minutes</c>. Omission is the honest answer; if Lite ever grows a fleet,
    /// that test is where it shows up.</summary>
    private const string FleetSweepGroup = "fleet_sweep";

    /// <summary>#3368: the other group Darling reports and Lite does not. The health-band tiers decide what
    /// colour a server's card reads and how the worst-first ranking and <c>get_fleet_overview</c> count bands
    /// — and Lite has NO health band at all: it never constructs <c>ServerHealthMetrics</c>, never calls
    /// <c>ClassifyBand</c> or <c>OverallMetricSeverity</c>, and drives its card brushes from
    /// <c>ClassifyFreshness</c> plus the severity enum. So reporting these two would advertise a knob this
    /// product cannot act on, which is the placeholder failure
    /// <see cref="GetAlertSettings_OmitsSelfAlerts_WhichLiteHasNoEquivalentFor"/>'s own history rejected for
    /// <c>ag.disconnect_refire_minutes</c>. Omission is the honest answer until Lite grows a band.</summary>
    private const string HealthBandsGroup = "health_bands";

    /// <summary>#2417: the MEMBER-level counterpart to <see cref="SelfAlertsGroup"/> — Darling keys Lite
    /// genuinely has no equivalent for, exempted BY NAME so the hole is a decision someone justified here
    /// rather than a loosened assertion. Each entry is paid for by a test asserting the omission is still
    /// real, so an exemption cannot outlive its reason.
    ///
    /// <para>#3444 (V122) put the seam back to work after #2426 emptied it: Darling's <c>blocking</c> and
    /// <c>deadlocks</c> groups each gained a <c>pg_count_threshold</c> — the PostgreSQL version of that
    /// group's count gate, deliberately a separate figure from the SQL Server one beside it — and Lite has
    /// no PostgreSQL seam for either to govern. Nothing in this SKU ever sets
    /// <c>CollectorTargetEngine.PostgreSql</c>, its DuckDB schema generator emits SQL Server definitions
    /// only, and its servers table has no engine_kind column to read (<c>McpEngineCapability</c> states
    /// and relies on the same fact), so emitting the pair would advertise knobs this product cannot act
    /// on — the placeholder failure this array's own history rejected for
    /// <c>ag.disconnect_refire_minutes</c>, the #1696 / store-V37 entry that came out when #2426 built
    /// Lite's re-fire and the exemption lost its reason. Paid for by
    /// <see cref="GetAlertSettings_OmitsPgCountThresholds_BecauseLiteHasNoPostgresSeam"/>, which also
    /// holds the exact-set pin that stood in the parity test as an <c>Assert.Empty</c> while the array
    /// was empty.</para>
    ///
    /// <para>#3712 (V137) added the third entry: Darling's <c>analysis</c> group gained
    /// <c>uncorroborated_route_source</c>, the PROVENANCE of its <c>uncorroborated_route</c> — which of the
    /// knob's two Darling homes decided it, the settings row's <c>analysis_uncorroborated_route</c> column or
    /// darling.json's <c>analysis.uncorroboratedRoute</c> (<c>store</c> / <c>file</c> / <c>default</c>). Lite
    /// has ONE home for the knob, its settings file (<c>analysis_uncorroborated_route</c>, edited in Settings
    /// → Alerts), so a provenance key here could only ever read <c>file</c>: a constant dressed as a reading,
    /// which is exactly the placeholder this array's history rejected. Lite's <c>uncorroborated_route</c> and
    /// <c>uncorroborated_route_note</c> still compare — the route and the where-to-edit note are real on both
    /// SKUs. Paid for by <see cref="GetAlertSettings_OmitsUncorroboratedRouteSource_BecauseLiteHasOneHomeForTheKnob"/>.</para></summary>
    private static readonly string[] LiteOmittedMembers =
        { "blocking.pg_count_threshold", "deadlocks.pg_count_threshold", "analysis.uncorroborated_route_source" };

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
            if (group == SelfAlertsGroup || group == HealthBandsGroup || group == FleetSweepGroup) continue;

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
    /// the self_alerts members Lite has any analogue of, because a single-instance WPF app has no headless
    /// store volume and no fleet collection loop to self-monitor; the remainder name TimescaleDB machinery a
    /// DuckDB store does not contain at all (the background-job cadence knob, and #3297's Retention Held
    /// tiers). Reporting either kind under names that read as knobs would tell an agent it can tune something
    /// Lite cannot.
    ///
    /// <para>The split is stated without a COUNT on purpose: a numeral here would be a frozen enumeration
    /// that the next Darling self-alert knob leaves behind, and this assertion is over the group's presence
    /// rather than its size, so it would go stale without going red.</para>
    /// </summary>
    [Fact]
    public void GetAlertSettings_OmitsSelfAlerts_WhichLiteHasNoEquivalentFor()
    {
        Assert.Contains(SelfAlertsGroup, DarlingPayloadShape().Select(g => g.Key));
        Assert.DoesNotContain(SelfAlertsGroup, KeysOf(Settings()));
    }

    /// <summary>
    /// #3368's counterpart, and the same two halves: Darling must still publish the group, or the omission
    /// below is over a key nobody emits, and Lite must still not publish it.
    ///
    /// <para>The third assertion is the one that makes this an omission rather than a gap. Lite has no health
    /// band anywhere in its source, so the tiers would be a knob with nothing behind it — and that is
    /// checkable rather than assertable, which is why it is checked. If Lite ever grows a band, this test is
    /// where that shows up, and the group stops being exempt from the shape comparison at the same time.</para>
    /// </summary>
    [Fact]
    public void GetAlertSettings_OmitsHealthBands_BecauseLiteHasNoBandToTune()
    {
        Assert.Contains(HealthBandsGroup, DarlingPayloadShape().Select(g => g.Key));
        Assert.DoesNotContain(HealthBandsGroup, KeysOf(Settings()));

        var liteRoot = Path.GetDirectoryName(FindRepoFile(Path.Combine("Lite", "PerformanceMonitorLite.csproj")))!;
        var bandCallers = Directory
            .EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => File.ReadAllText(f) is var text
                     && (text.Contains("ClassifyBand", StringComparison.Ordinal)
                      || text.Contains("OverallMetricSeverity", StringComparison.Ordinal)
                      || text.Contains("ServerHealthMetrics", StringComparison.Ordinal)
                      /* The fourth term closes the one route the other three miss: a Lite surface could
                         call the per-metric band DIRECTLY without ever building the bundle or folding it,
                         and that is the route that would matter for THESE tiers, which feed
                         DeadlockSeverity and nothing else. QUALIFIED deliberately - a bare
                         "DeadlockSeverity" matches four Lite files today (GetDeadlockSeverityStatsAsync
                         and the charts it feeds, a different quantity with a colliding name), so the
                         unqualified term would make this test fail on code that has no band at all. */
                      || text.Contains("ServerHealthClassifier.DeadlockSeverity", StringComparison.Ordinal)))
            .Select(f => Path.GetFileName(f))
            .ToArray();

        Assert.Empty(bandCallers);
    }

    /// <summary>
    /// #3466 (V124): the group-level omission's paying test, in the health-bands shape's three halves.
    /// Darling must still emit the group (or the exemption above is dead weight hiding a Darling
    /// regression), Lite must still not emit it, and the absence of the machinery the knobs would tune is
    /// checkable rather than assertable — so it is checked: no Lite source references the sweep engine,
    /// the sweep state store, the cadence constants, or the store columns. If Lite ever grows a fleet to
    /// sweep, this scan is where that shows up, the group comes out of the parity skip, and the members
    /// compare — #2426's ag history, repeated at group grain.
    /// </summary>
    [Fact]
    public void GetAlertSettings_OmitsFleetSweep_BecauseLiteHasNoFleetToSweep()
    {
        Assert.Contains(FleetSweepGroup, DarlingPayloadShape().Select(g => g.Key));
        Assert.DoesNotContain(FleetSweepGroup, KeysOf(Settings()));

        var liteRoot = Path.GetDirectoryName(FindRepoFile(Path.Combine("Lite", "PerformanceMonitorLite.csproj")))!;
        var fleetSweepCallers = Directory
            .EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => File.ReadAllText(f) is var text
                     && (text.Contains("FleetSweepEngine", StringComparison.Ordinal)
                      || text.Contains("FleetSweepStore", StringComparison.Ordinal)
                      || text.Contains("FleetSweepCadence", StringComparison.Ordinal)
                      || text.Contains("fleet_sweep_interval_minutes", StringComparison.Ordinal)))
            .Select(f => Path.GetFileName(f))
            .ToArray();

        Assert.Empty(fleetSweepCallers);
    }

    /// <summary>
    /// #3444 (V122): the two PostgreSQL count gates, and the test that pays for both
    /// <see cref="LiteOmittedMembers"/> entries. Darling reports <c>pg_count_threshold</c> inside
    /// <c>blocking</c> and <c>deadlocks</c> — a separate figure from the SQL Server gate beside it, because
    /// the two engines' counts are calibrated against different evidence — and Lite must not, because Lite
    /// has no PostgreSQL seam for either figure to govern. The same three halves as the health-bands
    /// omission above: Darling must still emit both (or the exemption is dead weight hiding a Darling
    /// regression), Lite must still not (or the exemption is masking keys that have since arrived and
    /// could now be compared), and the seam's absence is checkable rather than assertable, which is why it
    /// is checked.
    ///
    /// <para>The scan looks for the machinery the keys would tune — the evaluator whose gates they are and
    /// the two settings names that carry them — NOT for <c>CollectorTargetEngine.PostgreSql</c>, the
    /// seam's own name: the one Lite file that mentions it, <c>McpEngineCapability</c>, does so in prose
    /// ARGUING the absence this test checks, so that term would fail on the sentence stating the fact. If
    /// Lite ever grows a PostgreSQL seam and these gates with it, this scan is where that shows up, the
    /// entries come out of <see cref="LiteOmittedMembers"/>, and the members compare — #2426's ag history,
    /// repeated.</para>
    /// </summary>
    [Fact]
    public void GetAlertSettings_OmitsPgCountThresholds_BecauseLiteHasNoPostgresSeam()
    {
        var darling = DarlingPayloadShape();
        Assert.Contains("pg_count_threshold", darling.Single(g => g.Key == "blocking").Value);
        Assert.Contains("pg_count_threshold", darling.Single(g => g.Key == "deadlocks").Value);

        var root = Settings();
        Assert.DoesNotContain("pg_count_threshold", KeysOf(root.GetProperty("blocking")));
        Assert.DoesNotContain("pg_count_threshold", KeysOf(root.GetProperty("deadlocks")));

        var liteRoot = Path.GetDirectoryName(FindRepoFile(Path.Combine("Lite", "PerformanceMonitorLite.csproj")))!;
        var pgGateCallers = Directory
            .EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => File.ReadAllText(f) is var text
                     && (text.Contains("PostgresAlertEvaluator", StringComparison.Ordinal)
                      || text.Contains("PgBlockingCountThreshold", StringComparison.Ordinal)
                      || text.Contains("PgDeadlockCountThreshold", StringComparison.Ordinal)
                      || text.Contains("pg_count_threshold", StringComparison.Ordinal)))
            .Select(f => Path.GetFileName(f))
            .ToArray();

        Assert.Empty(pgGateCallers);

        /* Pinned as an exact set, so a fourth member-level exemption cannot be added without this test
           being the place someone justifies it — the same guard the smtp assertion applies to groups, and
           the non-empty successor to the Assert.Empty that stood in the parity test while the array was
           empty. The third entry is #3712's; its paying test is the one below. */
        Assert.Equal(
            new[] { "blocking.pg_count_threshold", "deadlocks.pg_count_threshold", "analysis.uncorroborated_route_source" },
            LiteOmittedMembers);
    }

    /// <summary>
    /// #3712 (V137): the provenance key, and the test that pays for its <see cref="LiteOmittedMembers"/> entry
    /// in the same three halves as the PostgreSQL gates above. Darling must still emit
    /// <c>analysis.uncorroborated_route_source</c> (or the exemption is dead weight hiding a Darling
    /// regression); Lite must still not (or the exemption masks a key that has since arrived and could be
    /// compared); and the reason — Lite has ONE home for the knob — is checkable rather than assertable, so it
    /// is checked: no Lite source names Darling's store column or its settings table, and Lite's own
    /// <c>App.LoadAlertSettings</c> reads the route from the settings file under the key the file uses. The
    /// route itself and its note are NOT exempt and compare like every other member: Lite reports a real
    /// value there (<c>App.AnalysisUncorroboratedRoute</c>) and a real note (where it is edited). If Lite ever
    /// grows a second home for the knob, this scan is where that shows up, the entry comes out, and the
    /// member compares — #2426's ag history, repeated.
    /// </summary>
    [Fact]
    public void GetAlertSettings_OmitsUncorroboratedRouteSource_BecauseLiteHasOneHomeForTheKnob()
    {
        var darlingAnalysis = DarlingPayloadShape().Single(g => g.Key == "analysis").Value;
        Assert.Contains("uncorroborated_route", darlingAnalysis);
        Assert.Contains("uncorroborated_route_source", darlingAnalysis);
        Assert.Contains("uncorroborated_route_note", darlingAnalysis);

        var analysis = Settings().GetProperty("analysis");
        Assert.Contains("uncorroborated_route", KeysOf(analysis));
        Assert.Contains("uncorroborated_route_note", KeysOf(analysis));
        Assert.DoesNotContain("uncorroborated_route_source", KeysOf(analysis));
        /* A real value, not a placeholder: the live static, in its wire spelling. */
        Assert.Equal(FindingRouting.RouteText(App.AnalysisUncorroboratedRoute), analysis.GetProperty("uncorroborated_route").GetString());

        var liteRoot = Path.GetDirectoryName(FindRepoFile(Path.Combine("Lite", "PerformanceMonitorLite.csproj")))!;
        var storeHomeCallers = Directory
            .EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => File.ReadAllText(f) is var text
                     && (text.Contains("config_alert_settings", StringComparison.Ordinal)
                      || text.Contains("StoreUncorroboratedRoute", StringComparison.Ordinal)
                      || text.Contains("uncorroborated_route_source", StringComparison.Ordinal)))
            .Select(f => Path.GetFileName(f))
            .ToArray();

        Assert.Empty(storeHomeCallers);

        /* The one home, named: the settings-file key App.LoadAlertSettings parses the route from. */
        var app = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "App.xaml.cs")));
        Assert.Contains("read.TryGetProperty(\"analysis_uncorroborated_route\", out v)", app, StringComparison.Ordinal);
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
