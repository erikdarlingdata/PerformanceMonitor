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

/// <summary>Source pins for the Manage Tags page: the fleet read it reuses, the five write routes and their methods, the can_edit gate, the delete confirm and the nav wiring.</summary>
public sealed class ManageTagsPageTests
{
    private static string Page() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "manage-tags.js").ReplaceLineEndings("\n");

    private static string Wwwroot(params string[] parts) =>
        ReadRepoFile([.. new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot" }, .. parts]).ReplaceLineEndings("\n");

    [Fact]
    public void ThePageReusesTheFleetRead_AndAddsNoReadRoute()
    {
        var page = Page();
        Assert.Contains("const res = await apiGetFleet();", page);
        Assert.Contains("state.forest = Array.isArray(d.tags) ? d.tags : [];", page);
        Assert.Contains("state.cards = Array.isArray(d.cards) ? d.cards : [];", page);
        Assert.DoesNotContain("get_fleet_overview", page);
        Assert.DoesNotContain("fetch(\"/api/", page);
    }

    [Fact]
    public void EachWriteUsesItsRouteAndMethod()
    {
        var page = Page();
        Assert.Contains("const createTag = (body) => send(\"POST\", \"/api/server-tags\", body);", page);
        Assert.Contains("const patchTag = (id, body) => send(\"PATCH\", tagPath(id), body);", page);
        Assert.Contains("send(\"DELETE\", tagPath(id) + (confirm ? \"?confirm=true\" : \"\"))", page);
        Assert.Contains("send(\"POST\", tagPath(id) + \"/servers\", { server_ids: ids })", page);
        Assert.Contains("send(\"DELETE\", tagPath(id) + \"/servers\", { server_ids: ids })", page);
        /* #5240: the write transport moved to util.js's apiWrite, so the JSON content type the server demands of every
           mutation (a 415 otherwise) is declared there, once. The pin follows it: the page must send through apiWrite,
           and apiWrite itself (not apiSend, which carries the same header line) must declare the header. */
        Assert.Contains("const res = await apiWrite(method, path, body);", page);
        // #5356: a 2xx that is JSON but not an object is an error answer here, not a saved change and not an expired session.
        Assert.Contains("if (res.unexpected) return { kind: \"error\"", page);
        Assert.DoesNotContain("fetch(", page);
        var util = Wwwroot("js", "util.js");
        var write = util.IndexOf("export async function apiWrite(method, path, body) {", System.StringComparison.Ordinal);
        Assert.True(write >= 0, "util.js no longer exports apiWrite");
        var writeBody = util.Substring(write, util.IndexOf("\n}\n", write, System.StringComparison.Ordinal) - write);
        Assert.Contains("\"Content-Type\": \"application/json\"", writeBody);
    }

    [Fact]
    public void TheDeleteConfirmBranchesOnTheStatusWord_NotOnMessageText()
    {
        var page = Page();
        Assert.Contains("if (status === 409 && b.status === \"confirm_required\") return { kind: \"confirm\"", page);
        Assert.Contains("const res = await deleteTag(tag.id, false);", page);
        Assert.Contains("const res = await deleteTag(pending.id, true);", page);
        Assert.DoesNotContain(".includes(\"already exists\")", page);
        Assert.DoesNotContain("message.match", page);
    }

    [Fact]
    public void EditAffordancesAreGatedOnCanEdit()
    {
        var page = Page();
        Assert.Contains("const canEdit = !!session.can_edit;", page);
        Assert.Contains("const newRoot = canEdit ?", page);
        Assert.Contains("const actions = state.canEdit\n", page);
        Assert.Contains("box.disabled = !state.canEdit;", page);
        Assert.Contains("if (state.canEdit) {\n    nodes.push(", page);
    }

    [Fact]
    public void TheStateLivesAtModuleScope_AndNoMarkupOrInlineHandlerIsBuilt()
    {
        var page = Page();
        Assert.Contains("let selectedId = null;", page);
        Assert.Contains("const edits = new Map();", page);
        Assert.DoesNotContain("innerHTML", page);
        Assert.DoesNotContain("onclick=", page);
    }

    [Fact]
    public void TheNavEntryAndTheRouteAreWired()
    {
        Assert.Contains("<a data-route=\"manage-tags\" href=\"#/manage-tags\">Manage Tags</a>", Wwwroot("index.html"));
        var app = Wwwroot("js", "app.js");
        Assert.Contains("import { renderManageTags } from \"./pages/manage-tags.js\";", app);
        Assert.Contains("if (h === \"#/manage-tags\") return { name: \"manageTags\" };", app);
        Assert.Contains("else if (r.name === \"manageTags\") renderManageTags(main);", app);
        Assert.Contains("if (r.name === \"manageTags\") return \"manage-tags\";", app);
    }
}
