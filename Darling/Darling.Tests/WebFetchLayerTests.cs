/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web viewer's fetch layer (#4187, #4191, #4190) — a vendor-style perf review's three findings against the
/// same client-side plumbing: <c>util.js</c>'s fetch helpers, <c>app.js</c>'s poll loop, and the server page's
/// panel reads.
///
/// <para><b>#4187.</b> An unauthenticated <c>/api/*</c> call now gets 401 + JSON from the server (pinned live,
/// over the real pipeline, in <see cref="DarlingWebHostGateLiveTests"/>) instead of the 200 HTML login form. The
/// pins here cover the CLIENT half: <c>classifyResponse</c> reads a 401 — and, belt-and-braces, a 200 whose body
/// is not JSON — as its own "auth" kind rather than folding it into "error" or "data", and the shell (app.js)
/// latches the FIRST one it sees into a one-time sign-in takeover that stops polling instead of leaving every
/// open panel to render its own "signed out" guess.</para>
///
/// <para><b>#4191.</b> A render's panel reads (server.js's <c>redrawPanels</c>, via panels.js's
/// <c>renderPanel</c>) now carry an AbortSignal that a NEWER render aborts before starting its own batch — but
/// never the shared <c>/api/fleet</c> read, which stays joined-not-owned exactly as server.js's own generation-
/// counter comment requires. Separately, app.js's poll tick skips re-rendering the route while the last render's
/// reads are still outstanding, tracked by a page-agnostic counter around <c>apiGet</c>/<c>readTool</c> (every
/// page already funnels through those, so no page's <c>build()</c> signature has to grow a signal parameter to
/// participate).</para>
///
/// <para><b>#4190.</b> A sub-tab click (a hashchange with the SAME server already on screen) no longer re-fetches
/// <c>/api/fleet</c> for the header card — it reuses the last one this page already has. Only a server this page
/// has never rendered, or the 60s poll's own tick, fetches again.</para>
///
/// <para>This repository carries no JavaScript test runner, so these are source pins over the shipped modules
/// (the <see cref="ServerPageTabsTests"/> / <see cref="ChartWindowDomainTests"/> pattern) — a text scan proves
/// the shipped code STILL HAS the mechanism, not that a browser executes it correctly; that was checked by
/// hand against the live pipeline (see the PR).</para>
/// </summary>
public sealed class WebFetchLayerTests
{
    private static string UtilJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js"));

    private static string PanelsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"));

    private static string AppJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js"));

    private static string ServerJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js"));

    /* ---------------------------------------------------------------------------------------------------
       #4187 — the client's own half of the 401/JSON split.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>A 401 is its OWN response kind, not folded into "error" — the shell branches on it to run the
    /// sign-in takeover rather than a per-panel red error strip, and the `login` field the server names rides
    /// along rather than a hardcoded client-side "/".</summary>
    [Fact]
    public void ClassifyResponse_A401_IsItsOwnAuthKind()
    {
        var util = UtilJs;

        Assert.Contains("if (resp.status === 401) {", util, StringComparison.Ordinal);
        Assert.Contains("return { kind: \"auth\", message, login };", util, StringComparison.Ordinal);
    }

    /// <summary>Belt-and-braces (the shape the bug actually shipped as): a 200 whose body fails JSON.parse is
    /// ALSO read as "auth", not as `{ kind: "data", data: null }` — the exact fold the issue reported.</summary>
    [Fact]
    public void ClassifyResponse_A200WithNonJsonBody_IsAlsoAuthNotData()
    {
        Assert.Contains("if (resp.ok && raw && body === null) {", UtilJs, StringComparison.Ordinal);
        Assert.Contains("return { kind: \"auth\", message, login: \"/\" };", UtilJs, StringComparison.Ordinal);
    }

    /// <summary>The one-shot latch + listener registry app.js's takeover is built on, and that every apiGet/
    /// apiGetFleet/apiSend caller reaches through the shared classifyResponse — so ANY read anywhere on the
    /// page can trip it, not just the sidebar's.</summary>
    [Fact]
    public void UtilJs_ExportsTheSessionExpiredLatch()
    {
        var util = UtilJs;

        Assert.Contains("export function isSessionExpired() {", util, StringComparison.Ordinal);
        Assert.Contains("export function onSessionExpired(fn) {", util, StringComparison.Ordinal);
    }

    /// <summary>app.js registers exactly one takeover handler, and both entry points a page render can come
    /// through — the hash router AND the poll loop — check the latch before doing any work, so a stray
    /// hashchange or a poll tick landing after the takeover fired cannot paint over the sign-in prompt.</summary>
    [Fact]
    public void AppJs_WiresTheSessionExpiredTakeover()
    {
        var app = AppJs;

        Assert.Contains("function showSignedOutState(message, _login) {", app, StringComparison.Ordinal);
        Assert.Contains("onSessionExpired(showSignedOutState);", app, StringComparison.Ordinal);

        // route() and refresh() each check the latch as their own first act.
        var routeGuard = app.IndexOf("function route(opts) {", StringComparison.Ordinal);
        var refreshGuard = app.IndexOf("function refresh() {", StringComparison.Ordinal);
        Assert.True(routeGuard >= 0, "route(opts) not found");
        Assert.True(refreshGuard >= 0, "refresh() not found");

        var routeCheck = app.IndexOf("if (isSessionExpired()) return;", routeGuard, StringComparison.Ordinal);
        var refreshCheck = app.IndexOf("if (isSessionExpired()) return;", refreshGuard, StringComparison.Ordinal);
        Assert.True(routeCheck >= 0 && routeCheck < routeGuard + 1000, "route(opts) does not check isSessionExpired() near its start");
        Assert.True(refreshCheck >= 0 && refreshCheck < refreshGuard + 1000, "refresh() does not check isSessionExpired() near its start");
    }

    /// <summary>
    /// #4221: an alert link is a hash route, and a stale session is the normal way to click one — the session
    /// dies while the tab is open, the takeover fires, and "Sign in again" has to land back on the same route.
    /// The link reloads THIS BROWSER's own current location (never the server-supplied <c>login</c> value —
    /// that stays untrusted per #4187), so pathname/search/hash all survive.
    /// </summary>
    [Fact]
    public void AppJs_SignInAgainLink_ReloadsCurrentLocationWithHash()
    {
        Assert.Contains(
            "el(\"a\", { href: location.pathname + location.search + location.hash, text: \"Sign in again\" }),",
            AppJs, StringComparison.Ordinal);
    }

    /* ---------------------------------------------------------------------------------------------------
       #4191 — cancel per render (never the shared /api/fleet read), and skip an overlapping poll tick.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>apiGet and readTool both take an optional AbortSignal, threaded straight to fetch — the seam
    /// panels.js's renderPanel (every page's panel reads) and server.js's redrawPanels (the controller that
    /// owns it) are built on.</summary>
    [Fact]
    public void ApiGetAndReadTool_AcceptAnAbortSignal()
    {
        var util = UtilJs;

        Assert.Contains("export async function apiGet(path, signal) {", util, StringComparison.Ordinal);
        Assert.Contains("resp = await fetch(path, { headers: { Accept: \"application/json\" }, signal });", util, StringComparison.Ordinal);
        Assert.Contains("export function readTool(tool, params, signal) {", util, StringComparison.Ordinal);
    }

    /// <summary>An aborted fetch is its own response kind too (never "error") — a cancelled panel read must not
    /// flash a red error strip into a slot the newer render is about to replace anyway.</summary>
    [Fact]
    public void ApiGet_ReadsAnAbortedFetchAsItsOwnKind()
    {
        Assert.Contains("if (e && e.name === \"AbortError\") return { kind: \"aborted\" };", UtilJs, StringComparison.Ordinal);
    }

    /// <summary>panels.js is the ONE seam every page's panel reads funnel through (table()/stat()/line() in
    /// server-tabs.js, and every other page's descriptor arrays), so a page participates in per-render abort by
    /// this module picking up the current signal — no build(server, ctx) signature has to grow a parameter.
    /// loadPanel drops an aborted OR auth-kind result rather than mounting into a slot it no longer owns.</summary>
    [Fact]
    public void PanelsJs_CapturesAndAppliesTheRenderSignal()
    {
        var panels = PanelsJs;

        Assert.Contains("export function setPanelSignal(signal) {", panels, StringComparison.Ordinal);
        Assert.Contains("async function loadPanel(desc, body, signal) {", panels, StringComparison.Ordinal);
        Assert.Contains("loadPanel(desc, body, signal);", panels, StringComparison.Ordinal);
        Assert.Contains("if (res.kind === \"aborted\" || res.kind === \"auth\") {", panels, StringComparison.Ordinal);
    }

    /// <summary>server.js's redrawPanels owns ONE AbortController per panel batch: every redraw (a fresh
    /// render, the loadServerCard callback, or the range picker) aborts the PREVIOUS batch before building the
    /// next, so a poll tick or a tab click landing mid-load stops the old batch's reads instead of leaving them
    /// to finish for a screen that no longer shows them — the doubled audit_config the issue measured.</summary>
    [Fact]
    public void ServerJs_RedrawPanelsOwnsAPerBatchAbortController()
    {
        var server = ServerJs;

        Assert.Contains("let panelAbort = null;", server, StringComparison.Ordinal);
        Assert.Contains("if (panelAbort) panelAbort.abort();", server, StringComparison.Ordinal);
        Assert.Contains("panelAbort = new AbortController();", server, StringComparison.Ordinal);
        Assert.Contains("setPanelSignal(panelAbort.signal);", server, StringComparison.Ordinal);

        /* The renderGeneration counter above panelAbort stays untouched by this change — it still protects the
           SHARED /api/fleet read, which panelAbort must never reach (a losing render cancelling a request the
           winning render also depends on would break the winning render too). */
        Assert.Contains("let renderGeneration = 0;", server, StringComparison.Ordinal);
    }

    /// <summary>The poll loop skips re-rendering the route while the LAST render's reads are still outstanding
    /// — evaluated from a page-agnostic in-flight counter, so it works for every page without each one exposing
    /// its own "am I still loading" flag. The sidebar/view-list/AG-nav probe still runs every tick regardless;
    /// only the heavier per-page render waits.</summary>
    [Fact]
    public void AppJs_RefreshSkipsARouteRenderWhileItsLastOneIsStillInFlight()
    {
        var app = AppJs;

        Assert.Contains("|| hasInFlightReads();", app, StringComparison.Ordinal);
        Assert.Contains("route({ poll: true });", app, StringComparison.Ordinal);

        // The guard is computed before refreshSidebar/refreshViewList/refreshAgNav start this tick's OWN reads.
        var refreshAt = app.IndexOf("function refresh() {", StringComparison.Ordinal);
        Assert.True(refreshAt >= 0, "refresh() not found");
        var skipRouteAt = app.IndexOf("hasInFlightReads();", refreshAt, StringComparison.Ordinal);
        var sidebarAt = app.IndexOf("refreshSidebar();", refreshAt, StringComparison.Ordinal);
        Assert.True(skipRouteAt >= 0 && sidebarAt >= 0, "expected both anchors inside refresh()");
        Assert.True(skipRouteAt < sidebarAt, "the in-flight check must run before this tick's own reads start");
    }

    /* ---------------------------------------------------------------------------------------------------
       #4222d — shell-level auto-refresh play/pause, plus the #/triage poll-cost guard.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>#/triage joins the composer routes in the poll skip: the alert-notebook deep link's landing
    /// page must not be re-rendered by the periodic tick, same cost concern as the editor routes, evaluated
    /// on the SAME line so a paused tab and an in-flight-read tab share the one guard.</summary>
    [Fact]
    public void AppJs_RefreshSkipsTriageRouteToo()
    {
        var app = AppJs;

        Assert.Contains("routeName === \"triage\"", app, StringComparison.Ordinal);

        var refreshAt = app.IndexOf("function refresh() {", StringComparison.Ordinal);
        Assert.True(refreshAt >= 0, "refresh() not found");
        var triageAt = app.IndexOf("routeName === \"triage\"", refreshAt, StringComparison.Ordinal);
        var sidebarAt = app.IndexOf("refreshSidebar();", refreshAt, StringComparison.Ordinal);
        Assert.True(triageAt >= 0 && triageAt < sidebarAt, "triage must join the skip guard, computed before this tick's own reads start");
    }

    /// <summary>The shell-level play/pause control (#4222d): a real, keyboard-reachable button whose state is
    /// persisted in localStorage, and which the 60s interval AND the visibility-change handler both consult —
    /// so pausing stops the tick outright, it does not just hide the already-automatic tab-hidden pause.
    /// Resuming runs one refresh immediately rather than waiting out the rest of the 60s window.</summary>
    [Fact]
    public void AppJs_HasAShellLevelAutoRefreshPauseThatGatesBothPollPaths()
    {
        var app = AppJs;

        Assert.Contains("const AUTO_REFRESH_PAUSED_KEY = \"darling.autoRefreshPaused\";", app, StringComparison.Ordinal);
        Assert.Contains("function isAutoRefreshPaused() {", app, StringComparison.Ordinal);
        Assert.Contains("localStorage.getItem(AUTO_REFRESH_PAUSED_KEY) === \"1\";", app, StringComparison.Ordinal);
        Assert.Contains("function setAutoRefreshPaused(paused) {", app, StringComparison.Ordinal);
        Assert.Contains("function initAutoRefreshToggle() {", app, StringComparison.Ordinal);
        Assert.Contains("document.getElementById(\"auto-refresh-toggle\")", app, StringComparison.Ordinal);
        Assert.Contains("if (!paused) refresh();", app, StringComparison.Ordinal);

        // Both poll paths — the interval tick and the visibility-change handler — check the paused flag,
        // not just document.hidden; pausing must stop the tick even on a visible tab.
        Assert.Contains("if (!document.hidden && !isAutoRefreshPaused()) refresh();", app, StringComparison.Ordinal);
        var occurrences = 0;
        var idx = 0;
        const string gate = "if (!document.hidden && !isAutoRefreshPaused()) refresh();";
        while ((idx = app.IndexOf(gate, idx, StringComparison.Ordinal)) >= 0)
        {
            occurrences++;
            idx += 1;
        }
        Assert.Equal(2, occurrences);
    }

    /// <summary>The button itself lives in the shell chrome (index.html's sidebar brand block), not inside any
    /// page render, so it survives every route change; the shell only sets its text/attrs, never its own
    /// markup wholesale (R4 — el()/textContent, no innerHTML for the label).</summary>
    [Fact]
    public void IndexHtml_HasTheAutoRefreshToggleButtonInTheShellChrome()
    {
        var index = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "index.html"));

        Assert.Contains("<button id=\"auto-refresh-toggle\" class=\"auto-refresh-toggle\" type=\"button\" aria-pressed=\"false\"></button>", index, StringComparison.Ordinal);
    }

    /// <summary>The counter itself: apiGetFleet and apiSend are deliberately NOT counted (the shared fleet read
    /// and a mutation are not "page reads" a poll tick should wait out) — only apiGet/readTool's own fetch
    /// counts, which is what makes the guard above a proxy for "the page's panel reads", not "any network
    /// activity at all".</summary>
    [Fact]
    public void UtilJs_InFlightCounterExcludesFleetAndMutations()
    {
        var util = UtilJs;

        Assert.Contains("export function hasInFlightReads() {", util, StringComparison.Ordinal);
        Assert.Contains("inFlightReads++;", util, StringComparison.Ordinal);
        Assert.Contains("inFlightReads--;", util, StringComparison.Ordinal);

        // apiGetFleet's own signature took no signal/counter change — still the #3895 shared-request shape.
        Assert.Contains("export async function apiGetFleet() {", util, StringComparison.Ordinal);
        Assert.Contains("export async function apiSend(method, path, body) {", util, StringComparison.Ordinal);
    }

    /* ---------------------------------------------------------------------------------------------------
       #4190 — a sub-tab click reuses the last /api/fleet read; only a new server or the poll re-fetches.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The exact gate: loadServerCard (the only /api/fleet read on this page) runs only when nothing
    /// is cached for this server yet, or this call is the poll's own refresh — never for a plain sub-tab
    /// click, which the synchronous `remembered` paint just above already served from the page's own cache.</summary>
    [Fact]
    public void ServerJs_OnlyRefetchesFleetForAnUnseenServerOrThePoll()
    {
        var server = ServerJs;

        Assert.Contains("export function renderServer(main, server, tabId, opts) {", server, StringComparison.Ordinal);
        Assert.Contains("const isPoll = !!(opts && opts.poll === true);", server, StringComparison.Ordinal);
        Assert.Contains("if (!remembered || isPoll) {", server, StringComparison.Ordinal);

        // loadServerCard's one CALL site (not its `function loadServerCard(server, onCard)` definition,
        // which also contains the literal "loadServerCard(server,") must be reached only through that gate.
        var occurrences = 0;
        var idx = 0;
        const string callSite = "loadServerCard(server, (card, reason) => {";
        while ((idx = server.IndexOf(callSite, idx, StringComparison.Ordinal)) >= 0)
        {
            occurrences++;
            idx += 1;
        }
        Assert.Equal(1, occurrences);
    }

    /// <summary>app.js threads the poll flag down to renderServer through route(), and ONLY there — the other
    /// page renderers (fleet/ag/sweeps/alerts/...) take no such flag, because #4190 is specific to the server
    /// page's own fleet-card cache.</summary>
    [Fact]
    public void AppJs_RouteThreadsThePollFlagToRenderServer()
    {
        Assert.Contains("if (r.name === \"server\") renderServer(main, r.param, r.tab, opts);", AppJs, StringComparison.Ordinal);
    }
}
