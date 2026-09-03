/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2802: a time-series chart must span the REQUESTED time window, not the data's own first/last-point extent.
///
/// <para><b>The bug.</b> <c>renderLineChart</c> set its x-domain from <c>rows[0].t</c>/<c>rows[last].t</c>. A
/// dense series (CPU, memory) spans the whole window, so it looked right; a sparse discrete-event series
/// (Blocking Events, Deadlocks) only has rows inside a short burst, so a chart titled "last 24 hours" zoomed its
/// axis to that ~30-minute burst AND dropped the calendar date from the tick labels. Because the axis re-scaled
/// to the burst, a burst that happened ~19h before the view time read as if it were happening now — an old
/// incident presented as current, which is the failure mode this fix exists to kill.</para>
///
/// <para><b>The fix.</b> <c>renderLineChart</c> takes an OPTIONAL <c>windowStart</c>/<c>windowEnd</c> (UTC-epoch
/// ms). When given, the axis spans that window and the points plot at their true position inside it; when absent,
/// the data-extent domain is byte-for-byte the original. The window's END is the query-time "now", NEVER the last
/// data point — anchoring to the last point would slide the burst back to the right edge and reintroduce the bug.
/// The callers thread the window they fetched over: the server tabs and the shared <c>vizLine</c> from the
/// range selector's hours ending now, and a composed panel from the same inputs <c>buildRunBody</c> uses
/// (zoom → the #2788 per-panel pin → the view scope's hours).</para>
///
/// <para>This repository carries no JavaScript test runner, so these are source pins over the shipped modules
/// (the <see cref="ServerPageTabsTests"/> chart-module pattern — a regression here is invisible until a sparse
/// series is on camera, which is exactly when it is seen). Behaviour was verified separately in the running
/// dashboard by the reviewing session.</para>
/// </summary>
public sealed class ChartWindowDomainTests
{
    private static string ChartsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "charts.js"));

    private static string UtilJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js"));

    private static string PanelsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"));

    private static string ServerTabsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

    private static string ComposeJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "compose.js"));

    private static string EndpointsCs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"));

    private static string ComposeSpecCs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "Compose", "ComposeSpec.cs"));

    /// <summary>
    /// The renderer honours an optional window as its x-domain, keeps the data extent SEPARATE (so a sparse
    /// series' burst can never re-scale the axis), and derives the date-in-labels decision from the DOMAIN bounds
    /// rather than the first/last sample. A degenerate window is ignored so a bad input can't collapse the axis.
    /// </summary>
    [Fact]
    public void RenderLineChart_UsesTheRequestedWindowAsXDomain_WhenGiven()
    {
        var charts = ChartsJs;

        /* The spec grew two optional fields, both defaulting to null (absent ⇒ unchanged). */
        Assert.Contains("windowStart = null, windowEnd = null } = spec;", charts, StringComparison.Ordinal);

        /* The data extent is computed but kept DISTINCT from the domain — the domain switches to the window when
           one is passed, so the burst of a sparse series never re-scales the axis to itself. */
        Assert.Contains("const dataTMin = rows[0].t.getTime();", charts, StringComparison.Ordinal);
        Assert.Contains("const dataTMax = rows[rows.length - 1].t.getTime();", charts, StringComparison.Ordinal);
        Assert.Contains("const tMin = hasWindow ? windowStart : dataTMin;", charts, StringComparison.Ordinal);
        Assert.Contains("const tMax = hasWindow ? windowEnd : dataTMax;", charts, StringComparison.Ordinal);

        /* A window is honoured only when it is two real numbers with end AFTER start — a degenerate window falls
           back to the data extent, never a collapsed (zero/negative-span) axis. */
        Assert.Contains("typeof windowStart === \"number\" && typeof windowEnd === \"number\"", charts, StringComparison.Ordinal);
        Assert.Contains("windowEnd > windowStart;", charts, StringComparison.Ordinal);

        /* The date-in-tick-labels decision is the DOMAIN's, not the data's: a same-day burst inside a 24h window
           that crosses midnight must still carry the calendar date. */
        Assert.Contains("const crossesDay = new Date(tMin).toDateString() !== new Date(tMax).toDateString();", charts, StringComparison.Ordinal);

        /* The single-bucket geometry (#2773) is not re-scaled away: the centered-x fallback still keys on
           spanMs === 0, which a valid window (span > 0) never triggers, so a lone point plots at its true x. */
        Assert.Contains("spanMs === 0 ? M.l + plotW / 2", charts, StringComparison.Ordinal);
    }

    /// <summary>
    /// The shared "last N hours" window helper anchors the window's END at NOW, with the width from hours — never
    /// at the last data point. This is the anti-regression for "an old burst reads as current": the width comes
    /// from the caller's requested hours, the end is the render-time now, and a missing/non-positive hours yields
    /// null so the chart keeps its data-extent domain unchanged.
    /// </summary>
    [Fact]
    public void WindowFromHours_AnchorsToNow_NotTheLastDataPoint()
    {
        var util = UtilJs;

        Assert.Contains("export function windowFromHours(hours) {", util, StringComparison.Ordinal);
        /* END = now (a UTC epoch, directly comparable to the parseUtc'd data times), START = now - hours. The
           last data point is NEVER read here — that is the whole point of the fix. */
        Assert.Contains("const windowEnd = Date.now();", util, StringComparison.Ordinal);
        Assert.Contains("return { windowStart: windowEnd - h * 3600000, windowEnd };", util, StringComparison.Ordinal);
        /* A missing/degenerate hours ⇒ null ⇒ the chart falls back to its data-extent domain (byte-for-byte). */
        Assert.Contains("if (!isFinite(h) || h < 1) return null;", util, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v1 line renderer and the hand-built server-tab trends both pass the requested window. <c>vizLine</c>
    /// derives it from the panel's own <c>hours</c> param (or the <c>windowHours</c> a fanout injects, since a
    /// fanout spec carries no params); the four hand-built composites spread the window from <c>ctx.hours</c>.
    /// </summary>
    [Fact]
    public void ServerTabTrends_PassTheRequestedWindow()
    {
        var panels = PanelsJs;
        /* vizLine: width from the descriptor's own hours (windowHours when a fanout injected it, else params.hours). */
        Assert.Contains("const win = windowFromHours(desc.windowHours != null ? desc.windowHours : desc.params && desc.params.hours);", panels, StringComparison.Ordinal);
        Assert.Contains("windowStart: win ? win.windowStart : null,", panels, StringComparison.Ordinal);
        Assert.Contains("windowEnd: win ? win.windowEnd : null,", panels, StringComparison.Ordinal);

        var serverTabs = ServerTabsJs;
        /* The hand-built trend composites (wait/perfmon/query/file-io) span ctx.hours ending now. */
        Assert.Contains("...windowFromHours(ctx.hours)", serverTabs, StringComparison.Ordinal);
        /* A fanout hands its shared fetch's hours to each spec as windowHours, so a fanout line panel (Current
           Waits, Blocking/Deadlock Severity, ...) is windowed too instead of falling back to its sparse extent. */
        Assert.Contains("windowHours: params && params.hours", serverTabs, StringComparison.Ordinal);
    }

    /// <summary>
    /// A composed time-series panel spans the run window, resolved from the SAME inputs <c>buildRunBody</c> uses
    /// (a brush-zoom, then the #2788 <c>effectivePin</c>, then the view scope), so the drawn axis can never
    /// disagree with the window the rows were fetched over. The relative-window default is kept in lockstep with
    /// the run endpoint's own default so a live panel's axis matches the server's window.
    /// </summary>
    [Fact]
    public void ComposedTimeSeries_SpanTheRunWindow_MirroringBuildRunBody()
    {
        var compose = ComposeJs;

        Assert.Contains("function resolveChartWindow(panelSpec, scope, zoom) {", compose, StringComparison.Ordinal);
        /* Same precedence as buildRunBody: zoom (absolute) wins, then the per-panel pin the "Pinned" badge reads. */
        Assert.Contains("if (zoom && zoom.startIso && zoom.endIso) {", compose, StringComparison.Ordinal);
        Assert.Contains("const pin = effectivePin(panelSpec);", compose, StringComparison.Ordinal);
        /* A relative window ends at the render-time now (the run response echoes no window/as-of, and the endpoint
           anchors a relative hours at its own now) — never at the last bucket. */
        Assert.Contains("const windowEnd = Date.now();", compose, StringComparison.Ordinal);
        Assert.Contains("windowStart: windowEnd - hours * 3600000, windowEnd", compose, StringComparison.Ordinal);
        /* Resolved where scope + zoom both live, then threaded into the chart. */
        Assert.Contains("const chartWindow = resolveChartWindow(panelSpec, scope, opts.zoom);", compose, StringComparison.Ordinal);
        Assert.Contains("windowStart: opts.chartWindow ? opts.chartWindow.windowStart : null,", compose, StringComparison.Ordinal);
        Assert.Contains("windowEnd: opts.chartWindow ? opts.chartWindow.windowEnd : null,", compose, StringComparison.Ordinal);

        /* The live-panel default window is kept in lockstep with the run endpoint's DefaultComposeHours, so a
           live composed panel's drawn axis matches the server's default window. */
        Assert.Contains("const DEFAULT_COMPOSE_HOURS = 24;", compose, StringComparison.Ordinal);
        Assert.Contains("private const int DefaultComposeHours = 24;", EndpointsCs, StringComparison.Ordinal);

        /* The relative-window ceiling is clamped to the SAME MaxWindowHours the run endpoint applies (Math.Clamp),
           so a stored/imported range.hours beyond it draws an axis matching the clamped window the server serves,
           not the raw value (#2802 review). The JS mirror constant is pinned in lockstep with ComposeLimits so it
           cannot drift, and the clamp is asserted actually applied to the resolved hours. */
        Assert.Contains("const MAX_COMPOSE_WINDOW_HOURS = 24 * 90;", compose, StringComparison.Ordinal);
        Assert.Contains("Math.min(rawHours, MAX_COMPOSE_WINDOW_HOURS)", compose, StringComparison.Ordinal);
        Assert.Contains("public const int MaxWindowHours = 24 * 90;", ComposeSpecCs, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
