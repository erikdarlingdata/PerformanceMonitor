/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the fleet page's Silence This Server / Unsilence control (#4843): the read and write
/// names, the shape it writes, the marker Unsilence matches on, the can_edit gate and the module-scope state.</summary>
public sealed class FleetSilencePageTests
{
    private static string Fleet() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "fleet.js").ReplaceLineEndings("\n");

    [Fact]
    public void SilenceWritesThroughTheMuteRulesCreateRoute_AndUnsilenceThroughItsDeleteRoute()
    {
        var fleet = Fleet();
        Assert.Contains("import { createRule, deleteRule } from \"./mute-rules.js\";", fleet);
        Assert.Contains("await createRule(silenceBody(card))", fleet);
        Assert.Contains("await deleteRule(r.id)", fleet);
        Assert.Contains("readTool(\"get_mute_rules\", { enabled_only: false })", fleet);

        var rules = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "mute-rules.js").ReplaceLineEndings("\n");
        Assert.Contains("export const createRule = (body) => send(\"POST\", \"/api/mute-rules\", body);", rules);
        Assert.Contains("export const deleteRule = (id) => send(\"DELETE\", \"/api/mute-rules/\" + encodeURIComponent(id));", rules);
    }

    [Fact]
    public void TheBodyIsTheDesktopSilenceShape_KeyedOnTheServerIdWithItsMarkerReason()
    {
        var fleet = Fleet();
        Assert.Contains("{ server_id: card.server_id, server_name: card.display_name, reason: SILENCE_REASON }", fleet);
        Assert.Contains("export const SILENCE_REASON = \"" + PerformanceMonitor.Darling.Viewer.ViewerDataService.ServerSilenceReason + "\";", fleet);
    }

    [Fact]
    public void UnsilenceMatchesOnlyTheRuleTheSilenceCreated()
    {
        var fleet = Fleet();
        Assert.Contains("rule.reason === SILENCE_REASON", fleet);
        Assert.Contains("rule.server_id === serverId", fleet);
        foreach (var field in new[] { "metric_name", "database_pattern", "query_text_pattern", "wait_type_pattern", "job_name_pattern" })
        {
            Assert.Contains("rule." + field + " == null", fleet);
        }
        Assert.Contains(".filter((r) => isOwnSilenceRule(r, id))", fleet);
    }

    [Fact]
    public void TheButtonIsGatedOnCanEdit_AndTheActionsConfirm()
    {
        var fleet = Fleet();
        Assert.Contains("canEditSilence = !!(await api.getSession()).can_edit;", fleet);
        Assert.Contains("if (!canEditSilence) return null;", fleet);
        Assert.Contains("globalThis.confirm(ask)", fleet);
    }

    [Fact]
    public void InFlightAndOptimisticStateLiveAtModuleScope_KeyedByServer()
    {
        var fleet = Fleet();
        Assert.Contains("\nconst silencePending = new Set();", fleet);
        Assert.Contains("\nconst silenceOverride = new Map();", fleet);
        Assert.Contains("if (silencePending.has(id)) return;", fleet);
    }

    [Fact]
    public void ThePageTextNeverGoesThroughInnerHtml_AndTheCssIsInAppCss()
    {
        Assert.DoesNotContain("innerHTML", Fleet());
        var app = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css");
        var theme = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "theme.css");
        Assert.Contains(".silence-btn", app);
        Assert.DoesNotContain("silence-btn", theme);
    }
}
