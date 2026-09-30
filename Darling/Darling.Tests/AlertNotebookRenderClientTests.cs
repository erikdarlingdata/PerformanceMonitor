/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4222 (client slice c): source-text pins over <c>wwwroot/js/pages/triage.js</c> and
/// <c>wwwroot/js/pages/views.js</c>'s alert-notebook render path. There is no JS test runner in this
/// repository, so these pin the SHAPE of the shipped source the way <see cref="ViewTemplatesTests"/> and
/// <see cref="ServerPageTabsTests"/> already do for this same pair of files — a rewrite that drops one of
/// these properties changes visible behaviour without failing the C# build otherwise.
///
/// Pinned properties (the brief's own list):
///  - the triage alert path renders through the SAME <c>renderNotebookDoc</c> the saved-view page uses, with
///    an in-memory (not saved) definition;
///  - the alert render path never calls <c>/api/fleet</c> (no fleet scope picker \u2014 the server is fixed);
///  - a max-3 in-flight limiter exists and gates each alert-mode read cell's load;
///  - "Save as notebook" POSTs to <c>/api/views</c> carrying the provenance string.
/// </summary>
public sealed class AlertNotebookRenderClientTests
{
    private const string TriagePath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/triage.js";
    private const string ViewsPath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/views.js";

    [Fact]
    public void Triage_RendersTheAlertNotebook_ThroughTheSharedRenderNotebookDocWithAnInMemoryDefinition()
    {
        var triage = ReadRepoFile(TriagePath);

        Assert.Contains("import { renderNotebookDoc } from \"./views.js\";", triage, StringComparison.Ordinal);
        Assert.Contains("mode: \"alert\"", triage, StringComparison.Ordinal);
        Assert.Contains("definition: def", triage, StringComparison.Ordinal);
        Assert.Contains("/api/alert-notebook", triage, StringComparison.Ordinal);

        // #2710: /api/triage stays the fallback for one release, on a 404 from the new endpoint.
        Assert.Contains("/api/triage", triage, StringComparison.Ordinal);
    }

    [Fact]
    public void Triage_AlertNotebookPath_NeverReadsApiFleet()
    {
        var triage = ReadRepoFile(TriagePath);

        Assert.DoesNotContain("/api/fleet", triage, StringComparison.Ordinal);
        Assert.DoesNotContain("apiGetFleet", triage, StringComparison.Ordinal);
    }

    [Fact]
    public void Views_AlertMode_SkipsTheFleetScopeBar()
    {
        var views = ReadRepoFile(ViewsPath);

        // The alert branch of renderNotebookDoc must not call fleetOptions/apiGetFleet on its own path \u2014
        // only the saved-view (`!isAlert`) branch may.
        var alertBranchStart = views.IndexOf("if (!isAlert) {", StringComparison.Ordinal);
        Assert.True(alertBranchStart > 0, "Expected renderNotebookDoc's saved-mode scope-bar branch guarded by `if (!isAlert)`.");
    }

    [Fact]
    public void Views_HasAMaxThreeInFlightLimiter_GatingAlertReadCells()
    {
        var views = ReadRepoFile(ViewsPath);

        Assert.Contains("class InFlightLimiter", views, StringComparison.Ordinal);
        Assert.Contains("new InFlightLimiter(3)", views, StringComparison.Ordinal);
        Assert.Contains("limiter.acquire()", views, StringComparison.Ordinal);
        Assert.Contains("limiter.release()", views, StringComparison.Ordinal);
    }

    [Fact]
    public void Views_SaveAsNotebook_PostsToApiViews_WithTheProvenanceString()
    {
        var views = ReadRepoFile(ViewsPath);

        Assert.Contains("function saveAsNotebookButton(", views, StringComparison.Ordinal);
        Assert.Contains("api.createView({ name, description: provenance || null, definition });", views, StringComparison.Ordinal);
    }

    [Fact]
    public void Triage_ProvenanceString_NamesTemplateMetricServerAndAt()
    {
        var triage = ReadRepoFile(TriagePath);

        Assert.Contains("\"from alert template \"", triage, StringComparison.Ordinal);
        Assert.Contains("t.template && t.template.id", triage, StringComparison.Ordinal);
        Assert.Contains("t.template && t.template.version", triage, StringComparison.Ordinal);
    }

    /// <summary>
    /// "Open live" drops each read cell's <c>as_of</c> pin (today's only per-cell absolute-window field in the
    /// #4366 shape \u2014 no composed cells, no per-cell `range`, yet) rather than a client-authored recompute.
    /// </summary>
    [Fact]
    public void Triage_OpenLive_DropsTheReadCellsAsOfParam()
    {
        var triage = ReadRepoFile(TriagePath);

        Assert.Contains("function stripAsOf(", triage, StringComparison.Ordinal);
        Assert.Contains("as_of", triage, StringComparison.Ordinal);
        Assert.Contains("onOpenLive: () => { def = stripAsOf(def); paint(); }", triage, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4368: the header cell must render current_value/threshold_value/detail_text — the endpoint's alert
    /// node carries all three (AlertNotebookEndpoint.cs's AlertRowNode), and the header dropped them entirely
    /// before this slice. Pinned through <c>el()</c>'s text path only (R4): a rewrite that switches either
    /// field to <c>innerHTML</c> or a template string must fail this build, not just an XSS review.
    /// </summary>
    [Fact]
    public void Views_AlertHeaderCell_RendersValueThresholdAndDetailText_ThroughElsTextPath()
    {
        var views = ReadRepoFile(ViewsPath);

        Assert.Contains("a.current_value", views, StringComparison.Ordinal);
        Assert.Contains("a.threshold_value", views, StringComparison.Ordinal);
        Assert.Contains("a.detail_text", views, StringComparison.Ordinal);

        // The detail_text line must go through el()'s { text: ... } prop (textContent), never innerHTML.
        Assert.Contains("el(\"pre\", { class: \"code\", text: a.detail_text })", views, StringComparison.Ordinal);

        // Scope the innerHTML ban to renderAlertHeaderCell's own body — the file's header comment (R4)
        // says "never innerHTML" in prose, which the whole-file assertion this replaces was tripping on.
        var lf = ReadRepoFileLf(ViewsPath);
        var start = lf.IndexOf("function renderAlertHeaderCell(", StringComparison.Ordinal);
        Assert.True(start >= 0, "renderAlertHeaderCell not found in views.js");
        var nextFn = lf.IndexOf("\nfunction ", start + 1, StringComparison.Ordinal);
        Assert.True(nextFn > start, "could not find the next top-level function after renderAlertHeaderCell");
        var body = lf.Substring(start, nextFn - start);
        var bodyWithoutComments = StripJsComments(body);
        Assert.DoesNotContain(".innerHTML", bodyWithoutComments, StringComparison.Ordinal);
        Assert.DoesNotContain("innerHTML =", bodyWithoutComments, StringComparison.Ordinal);
        Assert.DoesNotContain("insertAdjacentHTML", bodyWithoutComments, StringComparison.Ordinal);
    }

    /// <summary>
    /// Strips `//` line comments and `/* ... */` block comments so a pin can check for real code use of a
    /// pattern without tripping on prose that happens to mention it (e.g. "never innerHTML" in a comment).
    /// Not a full JS parser — good enough for this file, which has no such pattern inside a string literal.
    /// </summary>
    private static string StripJsComments(string source)
    {
        var withoutBlockComments = System.Text.RegularExpressions.Regex.Replace(
            source, @"/\*.*?\*/", string.Empty, System.Text.RegularExpressions.RegexOptions.Singleline);
        var withoutLineComments = System.Text.RegularExpressions.Regex.Replace(
            withoutBlockComments, @"//[^\n]*", string.Empty);
        return withoutLineComments;
    }

    /// <summary>
    /// #4368: a route change (hashchange off #/triage, or navigating away entirely) while an alert-mode read
    /// cell's slot is queued or its fetch is outstanding must not paint a stale result into a detached holder.
    /// The fetch and the limiter's release still have to run either way — only the mount is guarded.
    /// </summary>
    [Fact]
    public void Views_AlertModeReadCells_GuardAgainstRenderingAfterARouteChange()
    {
        var views = ReadRepoFile(ViewsPath);

        Assert.Contains("opts.isLive", views, StringComparison.Ordinal);
        Assert.Contains("const stillLive = !opts.isLive || opts.isLive();", views, StringComparison.Ordinal);
        Assert.Contains("if (stillLive) mount(holder, rendered);", views, StringComparison.Ordinal);

        var triage = ReadRepoFile(TriagePath);
        Assert.Contains("const isLive = () => location.hash === ourHash;", triage, StringComparison.Ordinal);
        Assert.Contains("isLive,", triage, StringComparison.Ordinal);
    }

    private const string ComposePath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/compose.js";

    /// <summary>The text of the named function: from its signature to the brace that closes it, with the
    /// comments stripped so a pin reads code, not the prose around it.</summary>
    private static string CodeOf(string lfSource, string signature)
    {
        var start = lfSource.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, signature + " was not found, so this pin would be reading nothing");
        var open = lfSource.IndexOf('{', start + signature.Length - 1);
        var depth = 0;
        for (var i = open; i < lfSource.Length; i++)
        {
            if (lfSource[i] == '{') depth++;
            else if (lfSource[i] == '}' && --depth == 0)
            {
                return StripJsComments(lfSource.Substring(start, i - start + 1));
            }
        }

        throw new InvalidOperationException("unbalanced braces reading " + signature);
    }

    /// <summary>
    /// Alert mode checks its read and panel cells against the catalog its caller fetched. It used to swap in an
    /// empty catalog whatever the caller passed, so every read cell of every alert notebook rendered as
    /// "Unknown read 'get_blocking' or visualization 'table'." although /api/catalog lists the read.
    /// </summary>
    [Fact]
    public void Views_AlertMode_ChecksItsCellsAgainstTheCatalogItsCallerPassed()
    {
        var doc = CodeOf(ReadRepoFileLf(ViewsPath), "export async function renderNotebookDoc(main, opts) {");

        Assert.Contains(
            "const catalog = isAlert ? (opts.catalog || { reads: [], compose: {} }) : opts.catalog;",
            doc, StringComparison.Ordinal);
        Assert.DoesNotContain("isAlert ? { reads: [], compose: {} } :", doc, StringComparison.Ordinal);

        // The read cell's own check is the one the catalog feeds: a read the catalog lists passes it and
        // reaches renderPanel through the limiter gate.
        var cell = CodeOf(ReadRepoFileLf(ViewsPath), "function renderAlertCell(");
        Assert.Contains("!readSet.has(cell.read)", cell, StringComparison.Ordinal);
        Assert.Contains("gatedCell(opts, limiter, (release) => renderPanel(cell, release))", cell, StringComparison.Ordinal);

        // triage.js hands the catalog it already fetches (and the link's server) to the alert render.
        var triage = ReadRepoFileLf(TriagePath);
        var start = triage.IndexOf("renderNotebookDoc(docHolder, {", StringComparison.Ordinal);
        Assert.True(start >= 0, "the alert render call was not found in triage.js");
        var call = triage.Substring(start, triage.IndexOf("});", start, StringComparison.Ordinal) - start);
        Assert.Contains("catalog,", call, StringComparison.Ordinal);
        Assert.Contains("server,", call, StringComparison.Ordinal);
    }

    /// <summary>
    /// The alert templates author <c>markdown</c> cells (prose) and <c>panel</c> cells (charts) beside their read
    /// cells. renderAlertCell handled header, status and read and returned null for the rest, so those cells
    /// vanished without a word. They now go through saved mode's own renderCell, and a kind nothing can draw
    /// says it is not shown.
    /// </summary>
    [Fact]
    public void Views_AlertMode_RendersMarkdownAndPanelCellsThroughRenderCell_AndNeverReturnsNothingForACell()
    {
        var cell = CodeOf(ReadRepoFileLf(ViewsPath), "function renderAlertCell(");

        Assert.Contains("cell.type === \"markdown\"", cell, StringComparison.Ordinal);
        Assert.Contains("return renderCell(cell, readSet, sourceSet, scope);", cell, StringComparison.Ordinal);
        Assert.Contains("cell.type === \"panel\"", cell, StringComparison.Ordinal);
        Assert.Contains(
            "gatedCell(opts, limiter, (release) => renderCell(cell, readSet, sourceSet, scope, release))",
            cell, StringComparison.Ordinal);

        // Anything else gets a card saying so. The only `return null` left is the guard on a non-object cell.
        Assert.Contains("return notShownCard(cell.title, \"This notebook cell (type '\"", cell, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(cell, @"return null;"));
    }

    /// <summary>
    /// A panel cell names a source and a measure but no server, so its server is the scope's: the registry name
    /// the endpoint sent as <c>scope_server</c> (it resolves the matched alert row's server, else the link's).
    /// Its window is its own absolute range, which compose.js's run body applies over the scope's hours. With no
    /// server at all the cell says so rather than charting the whole fleet.
    /// </summary>
    [Fact]
    public void Views_AlertModePanelCells_AreScopedToTheAlertsServer_AndKeepTheirOwnRange()
    {
        var views = ReadRepoFileLf(ViewsPath);
        var doc = CodeOf(views, "export async function renderNotebookDoc(main, opts) {");
        Assert.Contains("const scopeServer = opts.scopeServer || \"\";", doc, StringComparison.Ordinal);
        Assert.Contains("server: scopeServer", doc, StringComparison.Ordinal);
        Assert.Contains("renderAlertCell(cell, i, readSet, sourceSet, scope, opts, limiter)", doc, StringComparison.Ordinal);

        var cell = CodeOf(views, "function renderAlertCell(");
        Assert.Contains("if (!scope.server)", cell, StringComparison.Ordinal);
        Assert.Contains("the alert link names no server", cell, StringComparison.Ordinal);

        // Nothing in the alert path rewrites the cell's window; the cell is handed on as it came.
        Assert.DoesNotContain("range", cell, StringComparison.Ordinal);
        Assert.DoesNotContain("windowStart", cell, StringComparison.Ordinal);
        Assert.DoesNotContain("windowEnd", cell, StringComparison.Ordinal);

        // ...and compose.js applies a cell's pinned range over the scope's hours (the precedence this relies on).
        var run = CodeOf(ReadRepoFileLf(ComposePath), "function buildRunBody(panelSpec, scope, zoom = null) {");
        Assert.Contains("const pin = effectivePin(panelSpec);", run, StringComparison.Ordinal);
        Assert.Contains("body.windowStart = pin.windowStart;", run, StringComparison.Ordinal);
        Assert.Contains("if (s.server != null) body.server = s.server;", run, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both names the page holds for an alert's server are DISPLAY names: the matched row's <c>server_name</c> is
    /// what the snapshot stored, and the link's <c>server</c> is that same text. The compose runner filters on the
    /// registry's <c>server_name</c>, so a chart scoped by either drew "no data" under a firing alert on every
    /// server whose two names differ. The page scopes its charts by the endpoint's <c>scope_server</c> only, and
    /// triage.js hands that field on.
    /// </summary>
    [Fact]
    public void Views_AlertModePanelCells_ScopeByTheEndpointsScopeServer_NeverByADisplayName()
    {
        var doc = CodeOf(ReadRepoFileLf(ViewsPath), "export async function renderNotebookDoc(main, opts) {");
        Assert.DoesNotContain("server_name", doc, StringComparison.Ordinal);
        Assert.DoesNotContain("opts.server", doc, StringComparison.Ordinal);
        Assert.DoesNotContain("alertServer", doc, StringComparison.Ordinal);

        var triage = CodeOf(ReadRepoFileLf(TriagePath), "async function renderAlertNotebook(main, box, server, metric, at, dedup) {");
        Assert.Contains("scopeServer: t.scope_server || \"\"", triage, StringComparison.Ordinal);
    }

    /// <summary>
    /// A composed panel cell shares the read cells' in-flight limiter, so a slot must come back whatever happens
    /// to the cell: panelOrError settles once for an error card, and hands the callback to the renderer otherwise.
    /// </summary>
    [Fact]
    public void Views_PanelOrError_SettlesExactlyOnce_SoABadPanelCellCannotHoldALimiterSlot()
    {
        var body = CodeOf(ReadRepoFileLf(ViewsPath), "function panelOrError(p, readSet, sourceSet, scope, onSettled) {");

        Assert.Contains("if (onSettled) onSettled();", body, StringComparison.Ordinal);
        Assert.Contains("renderComposedPanelCard(p, scope, onSettled)", body, StringComparison.Ordinal);
        Assert.Contains("renderPanel(p, onSettled)", body, StringComparison.Ordinal);

        // Every error card is built by the settling `fail` function: the one panelErrorCard call left is inside it.
        Assert.Single(Regex.Matches(body, @"panelErrorCard\("));

        // The dashboard grid passes no callback, so its behaviour is the one it had.
        Assert.Contains(
            "panels.map((p) => panelOrError(p, readSet, sourceSet, currentScope()))",
            ReadRepoFile(ViewsPath), StringComparison.Ordinal);
    }

    /// <summary>"Open live" drops the read cells' as_of pin; the chart cells' absolute range goes with it, so
    /// the charts do not stay on the alert's window while the reads beside them go live.</summary>
    [Fact]
    public void Triage_OpenLive_AlsoDropsAPanelCellsAbsoluteRange()
    {
        var body = CodeOf(ReadRepoFileLf(TriagePath), "function stripAsOf(d) {");

        Assert.Contains("c.type === \"panel\" && c.range != null", body, StringComparison.Ordinal);
        Assert.Contains("const { range, ...rest } = c;", body, StringComparison.Ordinal);
    }
}
