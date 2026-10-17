/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Manage Query Tags page (#5986) run under Node (<c>manage-query-tags-harness.mjs</c>) against a fake DOM and a fake
/// fetch: the tree and the rule list, each write's request, the delete confirm, the Test button, the repaint on the 60 s
/// poll, the double click and the read-only sign-in. Skipped when Node is not installed.
/// </summary>
public sealed class ManageQueryTagsBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "manage-query-tags-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the manage query tags harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the manage query tags harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString() ?? "null").ToArray();

    [Fact]
    public void ThePage_ShowsTheTagsWithTheirHandAssignments_AndTheRulesInTheOrderTheServerSent()
    {
        if (!TryRun("render", out var r)) return;

        Assert.Equal(new[] { "Reports (2 hand, 1 rules)", "Batch<b> (0 hand, 0 rules)" }, Strings(r.GetProperty("tree")));
        Assert.Equal(new[] { "hash 0xAB12 — every serverRemove", "object db<i>.dbo.proc1 — srv-gRemove" }, Strings(r.GetProperty("assignments")));
        Assert.Equal(new[] { "5", "6" }, Strings(r.GetProperty("rules")));
        Assert.Contains("The tag map is current.", r.GetProperty("text").GetString());
    }

    [Fact]
    public void ACreate_PostsTheTrimmedName_AndAnAnswerOf400_KeepsTheFormOpenWithTheServersText()
    {
        if (!TryRun("createTag", out var created)) return;
        var call = created.GetProperty("calls")[0];
        Assert.Equal("POST", call.GetProperty("method").GetString());
        Assert.Equal("/api/query-tags", call.GetProperty("url").GetString());
        Assert.Equal("application/json", call.GetProperty("contentType").GetString());
        Assert.Equal("New", call.GetProperty("body").GetProperty("name").GetString());

        if (!TryRun("invalid", out var invalid)) return;
        Assert.Contains("A tag name must not be blank.", invalid.GetProperty("text").GetString());
        Assert.Equal(1, invalid.GetProperty("formOpen").GetInt32());
    }

    [Fact]
    public void ADeleteAnswering409ConfirmRequired_ShowsTheCounts_AndSendsConfirmOnlyWhenAskedAgain()
    {
        if (!TryRun("delete", out var r)) return;

        var first = r.GetProperty("first");
        Assert.Equal(1, first.GetProperty("calls").GetInt32());
        Assert.Contains("1 child tag(s), 2 hand assignment(s), 1 rule(s) would go with it.", first.GetProperty("text").GetString());
        Assert.Equal(1, first.GetProperty("anyway").GetInt32());
        var calls = r.GetProperty("calls");
        Assert.Equal(2, calls.GetArrayLength());
        Assert.Equal("/api/query-tags/1", calls[0].GetProperty("url").GetString());
        Assert.Equal("/api/query-tags/1?confirm=true", calls[1].GetProperty("url").GetString());
    }

    [Fact]
    public void AssignAndUnassign_SendOnlyTheKeyPartsOfTheirKind_WithTheServerScope()
    {
        if (!TryRun("assignHash", out var hash)) return;
        var hashBody = hash.GetProperty("calls")[0].GetProperty("body");
        Assert.Equal("/api/query-tags/2/assignments", hash.GetProperty("calls")[0].GetProperty("url").GetString());
        Assert.Equal("0xCD34", hashBody.GetProperty("query_hash").GetString());
        Assert.Equal(7, hashBody.GetProperty("server_id").GetInt32());
        Assert.False(hashBody.TryGetProperty("database_name", out _));

        if (!TryRun("assignObject", out var obj)) return;
        var objBody = obj.GetProperty("calls")[0].GetProperty("body");
        Assert.Equal("object", objBody.GetProperty("kind").GetString());
        Assert.Equal("dbo", objBody.GetProperty("schema_name").GetString());
        Assert.False(objBody.TryGetProperty("query_hash", out _));
        Assert.Contains("Assignment moved to this tag.", obj.GetProperty("text").GetString());

        if (!TryRun("unassign", out var gone)) return;
        var call = gone.GetProperty("calls")[0];
        Assert.Equal("DELETE", call.GetProperty("method").GetString());
        Assert.Equal("db<i>", call.GetProperty("body").GetProperty("database_name").GetString());
        Assert.Equal(7, call.GetProperty("body").GetProperty("server_id").GetInt32());
    }

    [Fact]
    public void TheRuleForm_CreatesEditsAndTests_AndTheTestShowsTheCountsAndSamplesAsText()
    {
        if (!TryRun("ruleCreate", out var create)) return;
        var body = create.GetProperty("calls")[0].GetProperty("body");
        Assert.Equal("/api/query-tag-rules", create.GetProperty("calls")[0].GetProperty("url").GetString());
        Assert.Equal("application", body.GetProperty("field").GetString());
        Assert.Equal("rep*", body.GetProperty("pattern").GetString());
        Assert.Equal(1, body.GetProperty("tag_id").GetInt32());
        Assert.Equal(50, body.GetProperty("priority").GetInt32());

        if (!TryRun("ruleEdit", out var edit)) return;
        var patch = edit.GetProperty("calls")[0];
        Assert.Equal("PATCH", patch.GetProperty("method").GetString());
        Assert.Equal("/api/query-tag-rules/6", patch.GetProperty("url").GetString());
        Assert.Single(patch.GetProperty("body").EnumerateObject());
        Assert.True(patch.GetProperty("body").GetProperty("enabled").GetBoolean());

        if (!TryRun("ruleTest", out var test)) return;
        Assert.StartsWith("/api/query-tag-rules/test?field=application&pattern=rep", test.GetProperty("calls")[0].GetProperty("url").GetString());
        var text = test.GetProperty("text").GetString();
        Assert.Contains("12 key(s) examined, 5 matched: 3 would be tagged, 1 held by a hand assignment, 1 held by a higher rule.", text);
        Assert.Contains("would_tag: 0xAA (application = <script>x</script>)", text);
    }

    [Fact]
    public void TheOpenForm_SurvivesThePoll_AndADoubleClickOnSave_SendsOneRequest()
    {
        if (!TryRun("pollKeepsForm", out var poll)) return;
        Assert.True(poll.GetProperty("sameInput").GetBoolean());
        Assert.Equal("Half typed", poll.GetProperty("value").GetString());
        Assert.Equal(1, poll.GetProperty("saves").GetInt32());

        if (!TryRun("doubleClick", out var dbl)) return;
        Assert.Equal(1, dbl.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void AReadOnlySignIn_SeesTheTagsAndRules_ButNoWriteControl()
    {
        if (!TryRun("readonly", out var r)) return;

        Assert.All(r.GetProperty("writes").EnumerateArray(), n => Assert.Equal(0, n.GetInt32()));
        Assert.Equal(2, r.GetProperty("tree").GetInt32());
        Assert.Equal(2, r.GetProperty("rules").GetInt32());
    }
}
