// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9639: <c>update_alert_settings</c>, the twin of Darling's tool. The tool sets the App statics the Settings window's Save sets and
/// writes the same settings.json keys through the window's own <c>WriteAlertKeys</c>, so each test reads the result back through the
/// file loader (<c>App.LoadAlertSettings</c>) as well as the statics. The settings document is a temp file, never the real one, and
/// every static is put back after each test.
/// </summary>
[Collection("app-alert-statics")]
public sealed class McpUpdateAlertSettingsTests : IDisposable
{
    private readonly Dictionary<PropertyInfo, object?> _snapshot = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"lite-update-alerts-{Guid.NewGuid():N}");
    private readonly JsonObject _document = new();
    private int _writes;

    public McpUpdateAlertSettingsTests()
    {
        Directory.CreateDirectory(_dir);
        foreach (var property in AlertStatics())
        {
            var value = property.GetValue(null);
            _snapshot[property] = value is IList list ? list.Cast<object>().Select(o => o?.ToString()).ToList() : value;
        }
    }

    public void Dispose()
    {
        foreach (var (property, value) in _snapshot)
        {
            property.SetValue(null, value is List<string?> list ? list.Select(s => s!).ToList() : value);
        }

        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private static IEnumerable<PropertyInfo> AlertStatics() =>
        typeof(App).GetProperties(BindingFlags.Public | BindingFlags.Static)
            .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0
                && (p.Name.StartsWith("Alert", StringComparison.Ordinal) || p.Name.StartsWith("Analysis", StringComparison.Ordinal)
                    || p.Name.StartsWith("Ag", StringComparison.Ordinal) || p.Name.StartsWith("Notify", StringComparison.Ordinal)
                    || p.Name is "EmailCooldownMinutes" or "ConnectionRefireMinutes"));

    private string Call(string json, bool writeSucceeds = true) =>
        McpAlertSettingsWriteTools.UpdateAlertSettingsCore(
            json,
            readRoot: () => (JsonObject)_document.DeepClone(),
            write: node =>
            {
                _writes++;
                if (!writeSucceeds)
                {
                    return false;
                }

                _document.Clear();
                foreach (var (key, value) in (JsonObject)node)
                {
                    _document[key] = value?.DeepClone();
                }

                return true;
            });

    /// <summary>Loads what the call wrote back through the file loader, the read the next launch makes.</summary>
    private void ReloadFromDocument()
    {
        File.WriteAllText(Path.Combine(_dir, "settings.json"), _document.ToJsonString());
        App.LoadAlertSettings(_dir, _ => "", (_, _) => { });
    }

    [Fact]
    public void AValidUpdate_ChangesTheRunningSettings_AndTheFileLoadsBackTheSame()
    {
        var result = JsonNode.Parse(Call("""
            {"cpu":{"threshold_percent":91,"mode":"sql"},"cooldown_minutes":7,
             "delivery":{"mode":"PerEvent","cooldown_minutes":30,"per_event_max":9},
             "excluded_databases":["archive_*"," tmp "],"analysis":{"uncorroborated_route":"page","notify_severity":1.25},
             "runnable_queue":{"warning_per_scheduler":4,"critical_per_scheduler":12}}
            """))!;

        Assert.Equal("updated", result["status"]!.GetValue<string>());
        Assert.Equal(91, App.AlertCpuThreshold);
        Assert.Equal(CpuAlertMode.SqlOnly, App.AlertCpuMode);
        Assert.Equal(7, App.AlertCooldownMinutes);
        Assert.Equal(AlertNotificationMode.PerEvent, App.AlertDeliveryMode);
        Assert.Equal(30, App.EmailCooldownMinutes);
        Assert.Equal(new[] { "archive_*", "tmp" }, App.AlertExcludedDatabases);
        Assert.Equal(FindingRoute.Page, App.AnalysisUncorroboratedRoute);

        /* The document the Settings window's Save would have written: the keys App.LoadAlertSettings reads. */
        Assert.Equal(91, _document["alert_cpu_threshold"]!.GetValue<int>());
        Assert.Equal(7, _document["alert_cooldown_minutes"]!.GetValue<int>());
        Assert.Equal("page", _document["analysis_uncorroborated_route"]!.GetValue<string>());

        /* Nothing else moved: a field that was not sent keeps its value in the answer's settings. */
        Assert.Equal(91, result["settings"]!["cpu"]!["threshold_percent"]!.GetValue<int>());
        Assert.Equal("sql", result["settings"]!["cpu"]!["mode"]!.GetValue<string>());

        App.AlertCpuThreshold = 1;
        App.AlertCooldownMinutes = 1;
        App.AlertExcludedDatabases = new List<string>();
        ReloadFromDocument();
        Assert.Equal(91, App.AlertCpuThreshold);
        Assert.Equal(7, App.AlertCooldownMinutes);
        Assert.Equal(new[] { "archive_*", "tmp" }, App.AlertExcludedDatabases);
        Assert.Equal(FindingRoute.Page, App.AnalysisUncorroboratedRoute);
    }

    [Fact]
    public void ThePreviousValuesAndTheUndo_PutTheSentFieldsBack()
    {
        var before = App.AlertCpuThreshold;
        var beforeMode = App.AlertCpuMode;
        var result = JsonNode.Parse(Call("""{"cpu":{"threshold_percent":55,"mode":"sql"}}"""))!;

        Assert.Equal(before, result["previous"]!["cpu"]!["threshold_percent"]!.GetValue<int>());
        Assert.Equal("update_alert_settings", result["undo"]!["tool"]!.GetValue<string>());

        var undo = result["undo"]!["arguments"]!["settings_json"]!.GetValue<string>();
        var undone = JsonNode.Parse(Call(undo))!;

        Assert.Equal("updated", undone["status"]!.GetValue<string>());
        Assert.Equal(before, App.AlertCpuThreshold);
        Assert.Equal(beforeMode, App.AlertCpuMode);
    }

    [Fact]
    public async Task AWholeGetAlertSettingsPayload_CanBeHandedBack()
    {
        var read = await McpAlertTools.GetAlertSettings();
        var payload = JsonNode.Parse(read)!.AsObject();
        payload.Remove("smtp");

        var result = JsonNode.Parse(Call(payload.ToJsonString()))!;

        Assert.Equal("updated", result["status"]!.GetValue<string>());
        /* The retired threshold rides in the payload, so the one warning says so. */
        Assert.Single(result["warnings"]!.AsArray());
    }

    [Theory]
    [InlineData("""{"cpu":{"threshold_percent":101}}""", "cpu.threshold_percent")]
    [InlineData("""{"cpu":{"threshold_percent":0}}""", "cpu.threshold_percent")]
    [InlineData("""{"cpu":{"threshold_percent":"90"}}""", "cpu.threshold_percent")]
    [InlineData("""{"cpu":{"threshold_percent":90.5}}""", "cpu.threshold_percent")]
    [InlineData("""{"cpu":{"mode":"half"}}""", "cpu.mode")]
    [InlineData("""{"cpu":{"enabled":"yes"}}""", "cpu.enabled")]
    [InlineData("""{"cooldown_minutes":0}""", "cooldown_minutes")]
    [InlineData("""{"cooldown_minutes":121}""", "cooldown_minutes")]
    [InlineData("""{"delivery":{"cooldown_minutes":500}}""", "delivery.cooldown_minutes")]
    [InlineData("""{"delivery":{"mode":"Loud"}}""", "delivery.mode")]
    [InlineData("""{"delivery":{"per_event_max":101}}""", "delivery.per_event_max")]
    [InlineData("""{"analysis":{"interval_minutes":4}}""", "analysis.interval_minutes")]
    [InlineData("""{"analysis":{"notify_severity":2.5}}""", "analysis.notify_severity")]
    [InlineData("""{"analysis":{"notify_cooldown_minutes":29}}""", "analysis.notify_cooldown_minutes")]
    [InlineData("""{"analysis":{"uncorroborated_route":null}}""", "analysis.uncorroborated_route")]
    [InlineData("""{"analysis":{"uncorroborated_route":"sometimes"}}""", "analysis.uncorroborated_route")]
    [InlineData("""{"file_growth":{"lookback_minutes":4}}""", "file_growth.lookback_minutes")]
    [InlineData("""{"long_running_job":{"multiplier":1}}""", "long_running_job.multiplier")]
    [InlineData("""{"excluded_databases":["a",5]}""", "excluded_databases")]
    [InlineData("""{"excluded_volumes":"C:\\"}""", "excluded_volumes")]
    [InlineData("""{"cpu":{"nonsense":1}}""", "cpu.nonsense")]
    [InlineData("""{"nonsense":1}""", "nonsense")]
    /* A field sent nested AND as a dotted key was checked on its first value and written with its last: here 10 against 20 passes
       the Critical-at-or-above-Warning rule and 50 is stored. It is refused, naming the path. */
    [InlineData("""{"runnable_queue":{"warning_per_scheduler":10,"critical_per_scheduler":20},"runnable_queue.warning_per_scheduler":50}""", "runnable_queue.warning_per_scheduler")]
    [InlineData("""{"cpu.threshold_percent":10,"cpu":{"threshold_percent":20}}""", "cpu.threshold_percent")]
    [InlineData("""{"smtp":{"server":"mail.example.test"}}""", "smtp")]
    [InlineData("""{"runnable_queue":{"warning_per_scheduler":10,"critical_per_scheduler":5}}""", "Runnable queue critical")]
    [InlineData("""{"runnable_queue":{"critical_per_scheduler":1}}""", "Runnable queue critical")]
    [InlineData("""{"client_timeouts":{"warning_per_minute":100,"critical_per_minute":10}}""", "critical")]
    [InlineData("""{"client_timeouts":{"warning_multiple":20,"critical_multiple":2}}""", "critical")]
    public void ARefusedValue_NamesTheField_AndChangesAndWritesNothing(string json, string named)
    {
        /* A valid sibling rides along: one bad field must stop the whole call, not just itself. */
        var withSibling = JsonNode.Parse(json)!.AsObject();
        withSibling["alerts_enabled"] = !App.AlertsEnabled;
        var alertsEnabled = App.AlertsEnabled;
        var cpu = App.AlertCpuThreshold;

        var result = JsonNode.Parse(Call(withSibling.ToJsonString()))!;

        Assert.Equal("invalid", result["status"]!.GetValue<string>());
        Assert.Contains(named, result["message"]!.GetValue<string>(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(alertsEnabled, App.AlertsEnabled);
        Assert.Equal(cpu, App.AlertCpuThreshold);
        Assert.Equal(0, _writes);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("[1,2]")]
    [InlineData("{}")]
    public void ABodyThatIsNotAFieldObject_IsRefused_AndWritesNothing(string json)
    {
        var result = JsonNode.Parse(Call(json))!;

        Assert.Equal("invalid", result["status"]!.GetValue<string>());
        Assert.Equal(0, _writes);
    }

    [Fact]
    public void AFileThatCannotBeWritten_PutsTheRunningSettingsBack()
    {
        var cpu = App.AlertCpuThreshold;
        var excluded = App.AlertExcludedDatabases.ToList();

        var result = JsonNode.Parse(Call("""{"cpu":{"threshold_percent":42},"excluded_databases":["zzz"]}""", writeSucceeds: false))!;

        Assert.Equal("failed", result["status"]!.GetValue<string>());
        Assert.Equal(cpu, App.AlertCpuThreshold);
        Assert.Equal(excluded, App.AlertExcludedDatabases);
    }

    /// <summary>The Settings window's Save re-applies every alert control, so it would silently undo an update made while it is open.
    /// The call is refused, nothing is applied and nothing is written.</summary>
    [Fact]
    public void WhileTheSettingsWindowIsOpen_TheCallIsRefused_AndChangesAndWritesNothing()
    {
        var cpu = App.AlertCpuThreshold;
        var writes = 0;

        var result = JsonNode.Parse(McpAlertSettingsWriteTools.UpdateAlertSettingsCore(
            """{"cpu":{"threshold_percent":42}}""",
            readRoot: () => (JsonObject)_document.DeepClone(),
            write: _ => { writes++; return true; },
            settingsWindowOpen: () => true))!;

        Assert.Equal("invalid", result["status"]!.GetValue<string>());
        Assert.Contains("Settings window is open", result["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(cpu, App.AlertCpuThreshold);
        Assert.Equal(0, writes);

        var closed = JsonNode.Parse(Call("""{"cpu":{"threshold_percent":42}}"""))!;
        Assert.Equal("updated", closed["status"]!.GetValue<string>());
    }

    /// <summary>Every other settings.json writer reads the whole document, changes one key and writes it back on the UI thread. The
    /// tool is called off it, so its read-modify-write is handed to that thread: a writer of another key that arrives in the middle
    /// of it runs after it and reads its result, so both keys are in the file. Run on the calling thread, the other writer finished
    /// while the tool held its stale copy and the tool's write erased the other key.</summary>
    [Fact]
    public void AToolWrite_AndAnotherKeysWriteOnTheUiThread_BothSurvive()
    {
        /* The shared STA thread is the UI thread here: its body awaits the tool call, which runs on a pool thread as an MCP call does. */
        StaTestThread.Run(async () =>
        {
            var dispatcher = Dispatcher.CurrentDispatcher;
            using var otherWriterDone = new ManualResetEventSlim();
            var gate = new object();

            JsonObject Read()
            {
                lock (gate) { return (JsonObject)_document.DeepClone(); }
            }

            bool Write(JsonNode node)
            {
                lock (gate)
                {
                    _document.Clear();
                    foreach (var (key, value) in (JsonObject)node)
                    {
                        _document[key] = value?.DeepClone();
                    }

                    return true;
                }
            }

            /* App.WriteSetting for the sidebar: read the document, set one key, write it back. */
            void OtherWriter()
            {
                var root = Read();
                root["sidebar_collapse_state"] = "collapsed";
                Write(root);
                otherWriterDone.Set();
            }

            var json = await Task.Run(() => McpAlertSettingsWriteTools.UpdateAlertSettingsCore(
                """{"cpu":{"threshold_percent":42}}""",
                readRoot: () =>
                {
                    var stale = Read();
                    /* The other writer arrives while the tool holds its copy. On the UI thread it queues behind the tool and this wait
                       times out; anywhere else it runs now and finishes inside the wait. */
                    dispatcher.BeginInvoke(OtherWriter);
                    otherWriterDone.Wait(TimeSpan.FromMilliseconds(500));
                    return stale;
                },
                write: Write,
                dispatcher: dispatcher));

            Assert.Equal("updated", JsonNode.Parse(json)!["status"]!.GetValue<string>());
            await Task.Run(() => Assert.True(otherWriterDone.Wait(TimeSpan.FromSeconds(10)), "the other writer never ran"));

            var final = Read();
            Assert.Equal(42, final["alert_cpu_threshold"]!.GetValue<int>());
            Assert.Equal("collapsed", final["sidebar_collapse_state"]!.GetValue<string>());
        });
    }

    [Fact]
    public void ALoneBar_IsCheckedAgainstTheStoredOther()
    {
        App.AlertRunnableQueueWarningPerScheduler = 8;
        App.AlertRunnableQueueCriticalPerScheduler = 16;

        var refused = JsonNode.Parse(Call("""{"runnable_queue":{"warning_per_scheduler":20}}"""))!;
        Assert.Equal("invalid", refused["status"]!.GetValue<string>());
        Assert.Equal(8, App.AlertRunnableQueueWarningPerScheduler);

        var accepted = JsonNode.Parse(Call("""{"runnable_queue":{"warning_per_scheduler":10}}"""))!;
        Assert.Equal("updated", accepted["status"]!.GetValue<string>());
        Assert.Equal(10, App.AlertRunnableQueueWarningPerScheduler);
    }
}
