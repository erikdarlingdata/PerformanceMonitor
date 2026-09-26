/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
}
