/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the Mute Rules page: its read, the four write routes and their methods, the can_edit gate, the nav entry and the route.</summary>
public sealed class MuteRulesPageTests
{
    private static string Page() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "mute-rules.js").ReplaceLineEndings("\n");

    private static string Wwwroot(params string[] parts) =>
        ReadRepoFile([.. new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot" }, .. parts]).ReplaceLineEndings("\n");

    [Fact]
    public void ThePageReadsGetMuteRules_IncludingPausedAndExpiredRules()
    {
        Assert.Contains("readTool(\"get_mute_rules\", { enabled_only: false })", Page());
    }

    [Fact]
    public void EachWriteUsesItsRouteAndMethod()
    {
        var page = Page();
        Assert.Contains("send(\"POST\", \"/api/mute-rules\", body)", page);
        Assert.Contains("send(\"PATCH\", \"/api/mute-rules/\" + encodeURIComponent(id), body)", page);
        Assert.Contains("send(\"PUT\", \"/api/mute-rules/\" + encodeURIComponent(id) + \"/enabled\", { enabled })", page);
        Assert.Contains("send(\"DELETE\", \"/api/mute-rules/\" + encodeURIComponent(id))", page);
    }

    [Fact]
    public void ThePatchPathNeverNamesEnabled()
    {
        var page = Page();
        var start = page.IndexOf("export function buildPatch", System.StringComparison.Ordinal);
        var end = page.IndexOf("export function interpretWrite", System.StringComparison.Ordinal);
        Assert.InRange(start, 0, end);
        Assert.DoesNotContain("enabled", page[start..end]);
        Assert.DoesNotContain("\"enabled\"", page[page.IndexOf("const EDITABLE_KEYS", System.StringComparison.Ordinal)..][..120]);
    }

    [Fact]
    public void EditAffordancesAreGatedOnCanEdit()
    {
        var page = Page();
        Assert.Contains("const canEdit = !!session.can_edit;", page);
        Assert.Contains("const newBtn = canEdit ?", page);
        Assert.Contains("if (canEdit) {\n    cells.push(", page);
        Assert.Contains("if (canEdit && query && query !== lastPrefillQuery)", page);
        Assert.Contains("window.confirm(", page);
    }

    [Fact]
    public void AlertHistoryOffersMuteActionsOnlyToAnEditingSeat()
    {
        var alerts = Wwwroot("js", "pages", "alerts.js");
        Assert.Contains("canMute = !!(await getSession()).can_edit;", alerts);
        Assert.Contains("return canMute ? ALERT_COLUMNS.concat([MUTE_COLUMN]) : ALERT_COLUMNS;", alerts);
        Assert.Contains("link(\"Mute this alert\", mutePrefillParams(a))", alerts);
        Assert.DoesNotContain("server_name: a.server_name", alerts);
        Assert.Contains("stored_server_name = r.StoredServerName", File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")));
    }

    [Fact]
    public void TheNavEntryAndTheRouteAreWired()
    {
        Assert.Contains("<a data-route=\"mute-rules\" href=\"#/mute-rules\">Mute Rules</a>", Wwwroot("index.html"));
        var app = Wwwroot("js", "app.js");
        Assert.Contains("h === \"#/mute-rules\" || h.startsWith(\"#/mute-rules?\")", app);
        Assert.Contains("else if (r.name === \"muteRules\") renderMuteRules(main, r.query);", app);
        Assert.Contains("if (r.name === \"muteRules\") return \"mute-rules\";", app);
    }
}
