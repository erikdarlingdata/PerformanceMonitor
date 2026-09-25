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

namespace Darling.Tests;

/// <summary>
/// #4194: the Alert History page (<c>wwwroot/js/pages/alerts.js</c>) used to build every row's full Detail
/// expansion up front — all 200 rows, even though each one starts collapsed behind a one-line disclosure — and
/// rebuilt the entire table from scratch on every 60s poll tick, including the header and filter box a person
/// might be mid-keystroke in. Both are pinned here from the JS SOURCE the way <see cref="AlertTrayStatusLoggedTests"/>
/// and <see cref="ServerPageTabsTests"/> already pin this page's other behaviour: there is no browser/DOM test
/// runner in this project, so a source-shape pin is what stands guard against either regressing.
/// </summary>
public sealed class AlertHistoryLazyRenderTests
{
    private static string AlertsJs => RepoFile.ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "alerts.js");

    /// <summary>Isolates one top-level <c>function name(...) { ... }</c> body by brace counting, so a pin reads
    /// exactly the function it names rather than "from here to the next blank line" (fragile against
    /// reformatting) or "to the next function" (wrong once a helper is inserted between them).</summary>
    private static string ExtractFunction(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"signature not found, this pin would read nothing: {signature}");
        var braceOpen = source.IndexOf('{', start);
        Assert.True(braceOpen >= 0, $"no open brace after signature: {signature}");
        var depth = 0;
        for (var i = braceOpen; i < source.Length; i++)
        {
            if (source[i] == '{') depth++;
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0) return source.Substring(start, i - start + 1);
            }
        }
        throw new InvalidOperationException($"unbalanced braces reading function body: {signature}");
    }

    [Fact]
    public void DetailCell_NeverParsesOrBuildsFieldRows_OutsideTheToggleHandler()
    {
        var js = AlertsJs;
        var detailCell = ExtractFunction(js, "function detailCell(a) {");

        /* The expensive part - parsing detail_text into fields and building one DOM row per field - must not
           run in detailCell's own synchronous body. If it did, it would run for all 200 rows on every render,
           which is the #4194 defect (~19,500 DOM nodes for a page that shows 200 collapsed one-liners) this
           pins against regressing. */
        Assert.DoesNotContain("parseDetailFields(", detailCell, StringComparison.Ordinal);
        Assert.DoesNotContain("fieldRow(", detailCell, StringComparison.Ordinal);

        /* It must instead defer to a lazy build, gated on the native <details> toggle firing AND actually being
           open (the event also fires on close), built at most once. */
        Assert.Contains("addEventListener(\"toggle\"", detailCell, StringComparison.Ordinal);
        Assert.Contains("if (built || !node.open) return;", detailCell, StringComparison.Ordinal);
        Assert.Contains("mount(placeholder, detailBody(a));", detailCell, StringComparison.Ordinal);

        /* The control: detailBody is where that work actually happens, so the absence above is real and not a
           matcher for text that was never going to appear anywhere in this file. Same field/error shape as the
           pre-#4194 eager version - only when it runs has changed. */
        var detailBody = ExtractFunction(js, "function detailBody(a) {");
        Assert.Contains("parseDetailFields(", detailBody, StringComparison.Ordinal);
        Assert.Contains("fieldRow", detailBody, StringComparison.Ordinal);
        Assert.Contains("detail-fields", detailBody, StringComparison.Ordinal);
        Assert.Contains("detail-error", detailBody, StringComparison.Ordinal);
    }

    [Fact]
    public void SixtySecondPoll_ReconcilesTheExistingTableByRowKey_InsteadOfRebuildingIt()
    {
        var js = AlertsJs;

        /* app.js's refresh loop calls renderAlerts(main) again on every tick; this page has to remember its own
           last mount (renderAlerts gets no help from app.js's route(), which just calls it again) so a tick
           landing on the still-open page reconciles instead of rebuilding. isConnected tells that apart from a
           fresh navigation, where mount() on some OTHER page already detached this page's old head element. */
        Assert.Contains("let live = null;", js, StringComparison.Ordinal);
        Assert.Contains("live && live.main === main && live.headEl.isConnected", js, StringComparison.Ordinal);
        Assert.Contains("await refreshAlerts(live);", js, StringComparison.Ordinal);

        /* The row identity: get_alert_history carries no surrogate id, so the key is the same
           (server, metric, fired instant) triple triageCell() already uses to deep-link one alert - not a
           second, independent notion of "which row is this". */
        var alertKey = ExtractFunction(js, "function alertKey(a) {");
        Assert.Contains("a.server_name", alertKey, StringComparison.Ordinal);
        Assert.Contains("a.metric_name", alertKey, StringComparison.Ordinal);
        Assert.Contains("a.alert_time", alertKey, StringComparison.Ordinal);

        /* Reconciliation removes rows no longer present, then walks the new row order re-using an existing <tr>
           by key and only building a fresh one for a key it has not seen before - the mechanism that keeps an
           unchanged row's DOM node stable across a poll instead of tearing the whole <tbody> down and rebuilding
           every row's cells, which is the other half of the #4194 defect. */
        var reconcile = ExtractFunction(js, "function reconcileRows(tbody, rows, rowMap) {");
        Assert.Contains("tr.remove();", reconcile, StringComparison.Ordinal);
        Assert.Contains("rowMap.delete(key);", reconcile, StringComparison.Ordinal);
        Assert.Contains("tr = alertRowNode(row);", reconcile, StringComparison.Ordinal);
        Assert.Contains("tbody.insertBefore(tr, anchor);", reconcile, StringComparison.Ordinal);
    }

    [Fact]
    public void SixtySecondPoll_NeverShowsTheLoadingStrip_OnlyTheFirstMountDoes()
    {
        /* A poll tick that re-showed "Loading alerts..." while re-fetching would itself BE a visible rebuild
           every 60s - the one thing #4194 asks this page to stop doing. loadingStrip( is used exactly once in
           the whole file: the first mount in renderAlerts, before `live` exists. The poll path (refreshAlerts)
           must not call it - the previous table stays exactly as it is until the new page is ready. */
        var js = AlertsJs;
        var loadingStripUses = Regex.Matches(js, Regex.Escape("loadingStrip(")).Count;
        Assert.Equal(1, loadingStripUses);

        var refreshAlerts = ExtractFunction(js, "async function refreshAlerts(state) {");
        Assert.DoesNotContain("loadingStrip", refreshAlerts, StringComparison.Ordinal);
    }

    [Fact]
    public void PollRefresh_NeverRebuildsTheHeaderAndFilterBox_TheGuardReturnsFirst()
    {
        /* mount(main, ...) clears main's children and rebuilds them, including the filter <input> - so a poll
           tick that reached it would recreate the filter box on every 60s tick and silently drop whatever an
           operator had typed into it. A source-text count of `mount(main,` alone cannot tell that apart from
           the pre-#4194 shape (also exactly one call site, just unconditional) - what has to be pinned is that
           the live-mount guard's `return;` sits BEFORE it, which is what makes the rebuild actually unreachable
           once `live` is set, not merely coincidentally unused on any one poll tick. */
        var js = AlertsJs;
        var renderAlerts = ExtractFunction(js, "export async function renderAlerts(main) {");

        var guardBlock = "if (live && live.main === main && live.headEl.isConnected) {\n" +
                          "    await refreshAlerts(live);\n" +
                          "    return;\n" +
                          "  }";
        Assert.Contains(guardBlock, renderAlerts, StringComparison.Ordinal);

        var guardIndex = renderAlerts.IndexOf(guardBlock, StringComparison.Ordinal);
        var mountMainIndex = renderAlerts.IndexOf("mount(main,", StringComparison.Ordinal);
        Assert.True(mountMainIndex > guardIndex,
            "mount(main, ...) must appear after the live-mount guard's return, or a poll tick would reach it");
    }
}
