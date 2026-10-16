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
using System.Windows;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1891: the Add Server dialogs used to clamp against <c>SystemParameters.WorkArea</c>, which always reports
/// the PRIMARY monitor. On a secondary monitor shorter than the primary that reproduces the very
/// off-screen-footer bug #1828/#1829 fixed.
///
/// <para>Two halves are pinned here, because the fix has two failure modes. The arithmetic — where the top
/// edge belongs given a work area — is unit-tested directly. The wiring — that both dialogs actually ASK for
/// their own monitor rather than the primary — is source-parsed, since a WPF <c>Window</c> needs an STA thread
/// and an <c>Application</c> to instantiate, and the regression that matters is one app being fixed while its
/// twin is not.</para>
/// </summary>
public sealed class WindowWorkAreaTests
{
    /// <summary>A 1080p work area with a 40px taskbar, at the origin: the primary-monitor shape.</summary>
    private static readonly Rect Primary = new(0, 0, 1920, 1040);

    /// <summary>
    /// A SHORTER monitor to the right of the primary — the configuration that made this a bug. Its bottom is
    /// 808, well above the primary's 1040, so anything clamped against the primary overshoots it.
    /// </summary>
    private static readonly Rect ShorterSecondary = new(1920, 0, 1280, 768);

    [Fact]
    public void ClampTop_LeavesAWindowThatFits_ExactlyWhereItIs()
    {
        /* The common case, and the one a multi-monitor change is most likely to break: a dialog that fits
           must not be nudged by a single pixel. */
        Assert.Equal(100, WindowWorkArea.ClampTop(top: 100, actualHeight: 400, Primary));
        Assert.Equal(0, WindowWorkArea.ClampTop(top: 0, actualHeight: 1040, Primary));
    }

    [Fact]
    public void ClampTop_PullsUpAWindowWhoseBottomHasGrownPastTheWorkArea()
    {
        /* #1828's case: SizeToContent growth is top-anchored, so the footer walks off the bottom. */
        Assert.Equal(640, WindowWorkArea.ClampTop(top: 900, actualHeight: 400, Primary));
    }

    [Fact]
    public void ClampTop_OnAShorterSecondaryMonitor_UsesThatMonitorsBottom_NotThePrimarys()
    {
        /* The #1891 case stated as arithmetic. A dialog 400 tall at top 600 fits the primary (1000 <= 1040)
           and is left alone there - which is exactly why clamping the secondary against the primary's numbers
           did nothing while the footer sat 232px below the secondary's visible area. */
        Assert.Equal(600, WindowWorkArea.ClampTop(top: 600, actualHeight: 400, Primary));
        Assert.Equal(368, WindowWorkArea.ClampTop(top: 600, actualHeight: 400, ShorterSecondary));
    }

    [Fact]
    public void ClampTop_NeverPushesTheTitleBarAboveTheWorkArea()
    {
        /* A dialog taller than the screen has to lose its BOTTOM, not its title bar: dragging it back is the
           only recovery, and that needs the title bar reachable. */
        Assert.Equal(ShorterSecondary.Top, WindowWorkArea.ClampTop(top: 100, actualHeight: 2000, ShorterSecondary));
    }

    [Fact]
    public void ClampTop_RespectsAWorkAreaThatDoesNotStartAtTheOrigin()
    {
        /* A monitor above-left of the primary has NEGATIVE coordinates, and a taskbar on top pushes the work
           area's Top down. Both are ordinary and both break a clamp that assumes 0. */
        var aboveLeft = new Rect(-1600, -900, 1600, 860);
        Assert.Equal(-100, WindowWorkArea.ClampTop(top: 200, actualHeight: 60, aboveLeft));
        Assert.Equal(aboveLeft.Top, WindowWorkArea.ClampTop(top: 0, actualHeight: 5000, aboveLeft));
    }

    [Theory]
    [InlineData("Lite", @"Lite\Windows\AddServerDialog.xaml.cs", @"Lite\Windows\AddServerDialog.xaml")]
    [InlineData("Darling", @"Darling\PerformanceMonitor.Darling.Viewer\AddServerDialog.xaml.cs", @"Darling\PerformanceMonitor.Darling.Viewer\AddServerDialog.xaml")]
    public void BothAddServerDialogs_ClampAgainstTheirOwnMonitor(string app, string codeBehind, string xaml)
    {
        var code = File.ReadAllText(FindRepoFile(codeBehind));
        var markup = File.ReadAllText(FindRepoFile(xaml));

        /* The handler bodies must delegate, not re-implement: a private copy is how the two dialogs drifted
           into needing this fix in the first place. */
        Assert.Contains("WindowWorkArea.Clamp(this)", code, StringComparison.Ordinal);

        /* And must not consult the primary monitor behind the helper's back. The helper itself falls back to
           SystemParameters.WorkArea, which is correct THERE and wrong here. */
        Assert.DoesNotContain("SystemParameters.WorkArea", code, StringComparison.Ordinal);

        /* All three triggers, because each covers a different way the answer goes stale: initial show
           (SourceInitialized is the first moment an HWND exists to ask about), growth, and being dragged to
           another monitor. Miss LocationChanged and the dialog keeps the cap of the screen it opened on. */
        foreach (var handler in new[] { "SizeChanged=\"Dialog_SizeChanged\"", "SourceInitialized=\"Dialog_SourceInitialized\"", "LocationChanged=\"Dialog_LocationChanged\"" })
        {
            Assert.True(markup.Contains(handler, StringComparison.Ordinal), $"{app}: {xaml} is missing {handler}");
        }
    }

    [Fact]
    public void TheTwoDialogs_WireTheSameHandlerSet()
    {
        /* Cross-app equality rather than two independent checklists: Lite/Viewer window-behaviour drift is the
           recurring complaint, so what is pinned is that they AGREE, not merely that each is non-empty. */
        var lite = HandlerNames(File.ReadAllText(FindRepoFile(@"Lite\Windows\AddServerDialog.xaml")));
        var darling = HandlerNames(File.ReadAllText(FindRepoFile(@"Darling\PerformanceMonitor.Darling.Viewer\AddServerDialog.xaml")));

        Assert.NotEmpty(lite);
        Assert.Equal(lite, darling);
    }

    /// <summary>The window-level event handler names declared on the root Window element, sorted.</summary>
    private static string[] HandlerNames(string markup)
    {
        var window = markup[..markup.IndexOf('>')];
        var names = new System.Collections.Generic.SortedSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(window, @"\b(SizeChanged|SourceInitialized|LocationChanged|Loaded|Closing)\s*=\s*""([^""]+)"""))
        {
            names.Add(m.Groups[1].Value);
        }

        return [.. names];
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
