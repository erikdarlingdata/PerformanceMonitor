using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4774: a web page render ends when its reads are done, even if the tab is hidden or auto-refresh is paused at
/// that moment. The settle check used to sit below the hidden-or-paused return in the scheduler tick, so a render
/// whose reads finished during such a span stayed "rendering" until the first visible, unpaused tick. Its duration
/// then counted the whole span, the back-off pushed the next refresh out to 15 minutes, the hint read "(slow
/// page)", and the page was never due because a due page needs <c>!pageRendering</c>.
///
/// <para>The scenarios run the real <c>app.js</c> scheduler and the real <c>refresh-policy.js</c> under Node on a
/// virtual clock (<c>web-scheduler-harness.mjs</c>); Node is skipped, the way
/// <see cref="WebAutoRefreshPolicyTests"/> skips it, when it is not installed, and the source-order pin at the
/// bottom then still holds the fix in place.</para>
/// </summary>
public sealed class WebRenderSettleTests
{
    private static readonly string s_wwwroot = Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");

    /// <summary>Runs one harness scenario and returns its JSON, or false when Node is not installed.</summary>
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var harness = RepoFile.PathTo("Darling", "Darling.Tests", "web-scheduler-harness.mjs");
        Assert.True(File.Exists(harness), "the scheduler harness is missing: " + harness);

        var psi = new ProcessStartInfo("node")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add(harness);
        psi.ArgumentList.Add(RepoFile.PathTo(s_wwwroot, "app.js"));
        psi.ArgumentList.Add(RepoFile.PathTo(s_wwwroot, "refresh-policy.js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            // Node is not installed on this machine; the source-order pin below still holds the fix in place.
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the scheduler harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the scheduler harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static bool Rendering(JsonElement snap) => snap.GetProperty("rendering").GetBoolean();

    private static long LastRenderMs(JsonElement snap) => snap.GetProperty("lastRenderMs").GetInt64();

    private static int Renders(JsonElement snap) => snap.GetProperty("renders").GetInt32();

    private static string Hint(JsonElement snap) => snap.GetProperty("hint").GetString() ?? "";

    /// <summary>The render ended when its reads did: it is no longer rendering, its measured time is the 2 s the
    /// reads took (one 1-second tick of slack), and nothing re-rendered while the span lasted.</summary>
    private static void AssertSettledAtItsOwnEnd(JsonElement away)
    {
        Assert.False(Rendering(away));
        Assert.InRange(LastRenderMs(away), 1, 3000);
        Assert.Equal(1, Renders(away));
    }

    /// <summary>The page then refreshes on its normal 60 s interval: it rendered again, the new render was timed at
    /// about 2 s, the hint does not say "(slow page)", and the next refresh sits 60 s past the settle.</summary>
    private static void AssertRefreshesOnTheNormalInterval(JsonElement back)
    {
        Assert.Equal(2, Renders(back));
        Assert.False(Rendering(back));
        Assert.InRange(LastRenderMs(back), 1, 3000);
        Assert.DoesNotContain("(slow page)", Hint(back), StringComparison.Ordinal);
        Assert.Equal(60000, back.GetProperty("nextRefreshInMs").GetInt64());
    }

    /// <summary>The acceptance case: reads are out when the tab hides, finish 2 s later, and the tab stays hidden
    /// for 10 minutes. Before the fix the render read 601,000 ms, the next refresh was 15 minutes out and no new
    /// render ever started.</summary>
    [Fact]
    public void HiddenTab_RenderFinishedWhileHidden_SettlesAtItsOwnEndAndRefreshesOnReturn()
    {
        if (!TryRun("hiddenTab", out var run)) return;

        var away = run.GetProperty("away");
        var back = run.GetProperty("back");
        AssertSettledAtItsOwnEnd(away);
        AssertRefreshesOnTheNormalInterval(back);
        Assert.InRange(back.GetProperty("secondRenderAt").GetInt64() - away.GetProperty("at").GetInt64(), 0, 3000);
    }

    /// <summary>A tab opened in the background is hidden before it ever renders, so its first render is the one
    /// that used to be stuck.</summary>
    [Fact]
    public void TabOpenedInTheBackground_RenderFinishedWhileHidden_SettlesAtItsOwnEndAndRefreshesOnReturn()
    {
        if (!TryRun("backgroundTab", out var run)) return;

        var away = run.GetProperty("away");
        var back = run.GetProperty("back");
        AssertSettledAtItsOwnEnd(away);
        AssertRefreshesOnTheNormalInterval(back);
        Assert.InRange(back.GetProperty("secondRenderAt").GetInt64() - away.GetProperty("at").GetInt64(), 0, 3000);
    }

    /// <summary>Pause with this tab's button while reads are out, resume 10 minutes later. Resuming already forced
    /// one refresh before the fix, so the end state matched; what changed is that the render is now settled during
    /// the pause instead of sitting open until the resume.</summary>
    [Fact]
    public void PausedWithTheButton_RenderFinishedWhilePaused_SettlesAtItsOwnEndAndResumeRefreshesNormally()
    {
        if (!TryRun("pausedWithButton", out var run)) return;

        var away = run.GetProperty("away");
        var back = run.GetProperty("back");
        AssertSettledAtItsOwnEnd(away);
        Assert.Equal("Auto-refresh paused", Hint(away));
        AssertRefreshesOnTheNormalInterval(back);
        Assert.Equal(away.GetProperty("at").GetInt64(), back.GetProperty("secondRenderAt").GetInt64());
    }

    /// <summary>The pause is a localStorage flag, so another tab can set and clear it. This tab then gets no click
    /// on resume, only the next tick, and the fix is what lets that tick refresh the page instead of measuring the
    /// whole pause as a render (601,000 ms and a 15-minute back-off before the fix).</summary>
    [Fact]
    public void PauseClearedFromAnotherTab_RenderFinishedWhilePaused_SettlesAtItsOwnEndAndRefreshesNormally()
    {
        if (!TryRun("pausedByAnotherTab", out var run)) return;

        var away = run.GetProperty("away");
        var back = run.GetProperty("back");
        AssertSettledAtItsOwnEnd(away);
        AssertRefreshesOnTheNormalInterval(back);
        Assert.InRange(back.GetProperty("secondRenderAt").GetInt64() - away.GetProperty("at").GetInt64(), 0, 3000);
    }

    /// <summary>The next refresh is anchored to the moment the render settled (2 s), not to the return to the tab
    /// (20 s): a tab that comes back early does not refresh at once, and the page refreshes at 2 s + 60 s.</summary>
    [Fact]
    public void HiddenBriefly_NextRefreshIsMeasuredFromTheSettle_NotFromTheReturn()
    {
        if (!TryRun("hiddenBriefly", out var run)) return;

        var away = run.GetProperty("away");
        var early = run.GetProperty("early");
        var back = run.GetProperty("back");
        AssertSettledAtItsOwnEnd(away);
        Assert.Equal(1, Renders(early));
        Assert.Equal(2, Renders(back));
        Assert.Equal(62000, back.GetProperty("secondRenderAt").GetInt64());
    }

    /// <summary>No hide and no pause: a render that is still loading is left alone, is not started a second time,
    /// and when it finishes its real duration feeds the back-off exactly as it did before the fix.</summary>
    [Fact]
    public void RenderStillLoading_WithNoHideOrPause_BacksOffFromItsRealDuration()
    {
        if (!TryRun("stillLoading", out var run)) return;

        var loading = run.GetProperty("away");
        Assert.True(Rendering(loading));
        Assert.Equal(0, LastRenderMs(loading));
        Assert.Equal(1, Renders(loading));
        Assert.DoesNotContain("(slow page)", Hint(loading), StringComparison.Ordinal);

        var done = run.GetProperty("back");
        Assert.False(Rendering(done));
        Assert.Equal(45000, LastRenderMs(done));
        Assert.Equal(1, Renders(done));
        Assert.Contains("(slow page)", Hint(done), StringComparison.Ordinal);
        Assert.Equal(180000, done.GetProperty("nextRefreshInMs").GetInt64());
    }

    /// <summary>The source-order pin for machines without Node: inside <c>schedulerTick</c> the settle call comes
    /// before the hidden-or-paused return, whose own line stays as it is (WebFetchLayerTests pins its text).</summary>
    [Fact]
    public void SchedulerTick_SettlesTheRenderBeforeTheHiddenOrPausedReturn()
    {
        var path = RepoFile.PathTo(s_wwwroot, "app.js");
        Assert.True(File.Exists(path), "app.js is missing: " + path);
        var app = File.ReadAllText(path);

        var tick = app.IndexOf("function schedulerTick() {", StringComparison.Ordinal);
        Assert.True(tick >= 0, "schedulerTick was not found");
        var settle = app.IndexOf("if (pageRendering && !hasInFlightReads()) settlePageRender(now);", tick, StringComparison.Ordinal);
        var guard = app.IndexOf("if (document.hidden || isAutoRefreshPaused() || isSessionExpired()) {", tick, StringComparison.Ordinal);

        Assert.True(settle > tick, "schedulerTick does not settle a finished render");
        Assert.True(guard > tick, "schedulerTick lost its hidden-or-paused return");
        Assert.True(settle < guard, "the settle check must run before the hidden-or-paused return, or a render that finishes there never settles");
    }
}
