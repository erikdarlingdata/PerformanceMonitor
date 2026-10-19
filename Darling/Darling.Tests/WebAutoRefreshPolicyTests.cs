using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4666: the web dashboard's per-page auto-refresh. The back-off rule is a pure JS module, run here under
/// Node when Node is available and otherwise pinned by its source constants; the setting itself is a new
/// optional root key on both stored definition kinds, validated by the one server authority the web CRUD and
/// the MCP custom-view tools share. The remaining pins read the shipped modules (no JS runner in the repo).
/// </summary>
public sealed class WebAutoRefreshPolicyTests
{
    private static readonly string s_wwwroot = Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");

    private static string Js(params string[] parts) => RepoFile.ReadRepoFile(new[] { s_wwwroot }.Concat(parts).ToArray());

    private const string Panel =
        "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private static string Dashboard(string refresh) => "{\"panels\":[" + Panel + "]" + refresh + "}";

    private static string Notebook(string refresh) =>
        "{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\",\"text\":\"n\"}," + Panel.Replace("{", "{\"type\":\"panel\",") + "]" + refresh + "}";

    [Theory]
    [InlineData(@"^function route\(")]
    [InlineData(@"^function refresh\(")]
    [InlineData(@"^function isNoPollRoute\(")]
    [InlineData(@"^async function refreshSidebar\(")]
    [InlineData(@"^async function refreshAgNav\(")]
    [InlineData(@"^async function refreshViewList\(")]
    public void AppJs_DeclaresEachLoopFunctionOnce(string pattern)
    {
        // A duplicated paste of these declarations is a SyntaxError in an ES module and blanks the dashboard.
        var count = System.Text.RegularExpressions.Regex.Matches(
            Js("app.js"), pattern, System.Text.RegularExpressions.RegexOptions.Multiline).Count;
        Assert.Equal(1, count);
    }

    [Fact]
    public void ComposedPanelRead_IsCounted_ThroughApiSendRead()
    {
        // The composed-panel run is a read that travels as a POST. It must count in inFlightReads so the
        // overlap guard waits it out and the back-off measures the render that contains it.
        var compose = Js("compose.js");
        var run = compose.Substring(compose.IndexOf("export function runCompose(", StringComparison.Ordinal));
        run = run.Substring(0, run.IndexOf("\n}", StringComparison.Ordinal));
        Assert.Contains("apiSendRead(\"POST\", \"/api/compose/run\"", run);
        Assert.DoesNotContain("apiSend(", run);

        var util = Js("util.js");
        var fn = util.Substring(util.IndexOf("export async function apiSendRead(", StringComparison.Ordinal));
        fn = fn.Substring(0, fn.IndexOf("\n}", StringComparison.Ordinal));
        Assert.Contains("inFlightReads++", fn);
        var finallyAt = fn.IndexOf("finally", StringComparison.Ordinal);
        Assert.True(finallyAt > 0, "the decrement must sit in a finally");
        Assert.True(fn.IndexOf("inFlightReads--", StringComparison.Ordinal) > finallyAt);

        var callers = Directory.GetFiles(
                RepoFile.PathTo(s_wwwroot), "*.js", SearchOption.AllDirectories)
            .Where(f => File.ReadAllText(f).Contains("apiSendRead(", StringComparison.Ordinal))
            .Select(Path.GetFileName).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "compose.js", "util.js" }, callers);
    }

    [Theory]
    [InlineData("off")]
    [InlineData("1m")]
    [InlineData("5m")]
    [InlineData("15m")]
    public void Definitions_AcceptEachRefreshChoice(string choice)
    {
        var tail = ",\"refresh\":\"" + choice + "\"";
        Assert.True(DarlingWebEndpoints.ValidateDefinition(Dashboard(tail)).IsValid, "dashboard " + choice);
        Assert.True(DarlingWebEndpoints.ValidateDefinition(Notebook(tail)).IsValid, "notebook " + choice);
    }

    [Theory]
    [InlineData(",\"refresh\":\"2m\"")]
    [InlineData(",\"refresh\":60")]
    [InlineData(",\"refresh\":null")]
    [InlineData(",\"refresh\":true")]
    public void Definitions_RejectAnyOtherRefreshValue(string tail)
    {
        foreach (var json in new[] { Dashboard(tail), Notebook(tail) })
        {
            var result = DarlingWebEndpoints.ValidateDefinition(json);
            Assert.False(result.IsValid, json);
            Assert.Contains("refresh must be one of", result.Error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Definitions_WithoutRefresh_StayValid()
    {
        Assert.True(DarlingWebEndpoints.ValidateDefinition(Dashboard("")).IsValid);
        Assert.True(DarlingWebEndpoints.ValidateDefinition(Notebook("")).IsValid);
    }

    [Fact]
    public async Task McpValidateCustomView_UsesTheSameRefreshRule()
    {
        var ok = await DarlingMcpCustomViewTools.ValidateCustomView(Notebook(",\"refresh\":\"15m\""));
        using (var doc = JsonDocument.Parse(ok))
        {
            Assert.True(doc.RootElement.GetProperty("valid").GetBoolean(), ok);
        }

        var bad = await DarlingMcpCustomViewTools.ValidateCustomView(Dashboard(",\"refresh\":\"2m\""));
        using var badDoc = JsonDocument.Parse(bad);
        Assert.False(badDoc.RootElement.GetProperty("valid").GetBoolean(), bad);
    }

    [Fact]
    public void RefreshPolicyJs_CarriesTheBackOffConstants()
    {
        var js = Js("refresh-policy.js");
        Assert.Contains("export function nextRefreshDelayMs(intervalMs, lastRenderMs) {", js, StringComparison.Ordinal);
        Assert.Contains("lastRenderMs > intervalMs / 2", js, StringComparison.Ordinal);
        Assert.Contains("4 * lastRenderMs", js, StringComparison.Ordinal);
        Assert.Contains("export const MAX_BACKOFF_MS = 15 * 60 * 1000;", js, StringComparison.Ordinal);
        Assert.Contains("export const REFRESH_CHOICES = { off: 0, \"1m\": 60000, \"5m\": 300000, \"15m\": 900000 };", js, StringComparison.Ordinal);
    }

    /// <summary>The acceptance case as a real run: a 40 s render at a 60 s interval waits 160 s, a fast page keeps
    /// its interval, a very slow page is capped at 15 minutes, and Off yields no schedule.</summary>
    [Fact]
    public void RefreshPolicyJs_BackOffRunsUnderNode()
    {
        var script = RepoFile.PathTo(s_wwwroot, "refresh-policy.js");
        var url = new Uri(script).AbsoluteUri;
        ProcessStartInfo psi;
        try
        {
            psi = new ProcessStartInfo("node")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            psi.ArgumentList.Add("--input-type=module");
            psi.ArgumentList.Add("-e");
            psi.ArgumentList.Add(
                "import(\"" + url + "\").then(m => console.log([m.nextRefreshDelayMs(60000, 40000), m.nextRefreshDelayMs(60000, 10000), m.nextRefreshDelayMs(60000, 300000), m.nextRefreshDelayMs(0, 1)].map(String).join(' ')))");
            using var proc = Process.Start(psi)!;
            var output = proc.StandardOutput.ReadToEnd().Trim();
            proc.WaitForExit(20000);
            Assert.Equal("160000 60000 900000 null", output);
        }
        catch (System.ComponentModel.Win32Exception)
        {
            // RefreshPolicyJs_CarriesTheBackOffConstants still pins the rule.
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
        }
    }

    [Fact]
    public void AppJs_SchedulesThePageThroughThePolicy()
    {
        var app = Js("app.js");
        Assert.DoesNotContain("}, POLL_MS);", app, StringComparison.Ordinal);
        Assert.Contains("nextRefreshDelayMs(pageIntervalMs(), pageLastRenderMs)", app, StringComparison.Ordinal);
        Assert.Contains("setInterval(schedulerTick, SCHEDULER_TICK_MS);", app, StringComparison.Ordinal);
        Assert.Contains("currentViewRefresh()", app, StringComparison.Ordinal);
        Assert.Contains("Pause all auto-refresh", app, StringComparison.Ordinal);
        Assert.Contains("Resume auto-refresh", app, StringComparison.Ordinal);
        Assert.Contains("(slow page)", app, StringComparison.Ordinal);
    }

    [Fact]
    public void Editors_WriteRefreshBackIntoTheDefinition()
    {
        foreach (var file in new[] { "editor.js", "notebook.js" })
        {
            var js = Js(file);
            Assert.Contains("refresh: typeof def.refresh === \"string\" ? def.refresh : null,", js, StringComparison.Ordinal);
            Assert.Contains("if (model.refresh) def.refresh = model.refresh;", js, StringComparison.Ordinal);
            Assert.Contains("buildRefreshControl(", js, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ViewsPage_HasALabelledControlThatSavesOrSaysItDidNot()
    {
        var views = Js("pages", "views.js");
        Assert.Contains("export function currentViewRefresh() {", views, StringComparison.Ordinal);
        Assert.Contains("api.updateView(view.id", views, StringComparison.Ordinal);
        Assert.Contains("not saved: read-only", views, StringComparison.Ordinal);
        Assert.Contains("\"Auto-refresh:\"", Js("refresh-control.js"), StringComparison.Ordinal);
        Assert.Contains("REFRESH_CHOICES", Js("views-api.js"), StringComparison.Ordinal);
    }
}
