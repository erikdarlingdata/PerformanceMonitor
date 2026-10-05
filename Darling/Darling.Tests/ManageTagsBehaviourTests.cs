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
/// The Manage Tags page run under Node (<c>manage-tags-harness.mjs</c>) against a fake DOM and a fake fetch: the
/// tree, each write's request, the server's refusal text, the delete confirm, the assignment diff, the repaint and
/// the read-only sign-in. Skipped when Node is not installed.
/// </summary>
public sealed class ManageTagsBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "manage-tags-harness.mjs"));
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
                Assert.Fail("the manage tags harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the manage tags harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString() ?? "null").ToArray();

    [Fact]
    public void TheTreeShowsTheForestInServerOrder_WithSwatchesAndIndent_AndOnlyAWellFormedColour()
    {
        if (!TryRun("render", out var r)) return;

        Assert.Equal(new[] { "East", "Prod", "Edge<b>", "West" }, Strings(r.GetProperty("order")));
        Assert.Equal(new[] { "padding-left:0px", "padding-left:18px", "padding-left:36px", "padding-left:0px" }, Strings(r.GetProperty("padding")));
        Assert.Equal(new[] { "background:#4E79A7", "null", "null", "background:#E15759" }, Strings(r.GetProperty("swatches")));
        Assert.Equal(1, r.GetProperty("newRoot").GetInt32());
    }

    [Fact]
    public void ACreate_PostsTrimmedFieldsAsJson_AndListsTheRulesItChanged()
    {
        if (!TryRun("create", out var r)) return;

        var call = r.GetProperty("calls")[0];
        Assert.Equal("POST", call.GetProperty("method").GetString());
        Assert.Equal("/api/server-tags", call.GetProperty("url").GetString());
        Assert.Equal("application/json", call.GetProperty("contentType").GetString());
        Assert.Equal("{\"name\":\"New\",\"colour\":\"#112233\"}", call.GetProperty("body").GetRawText());
        var text = r.GetProperty("text").GetString();
        Assert.Contains("Tag created.", text);
        Assert.Contains("Disk rule", text);
    }

    [Fact]
    public void AChildCreate_CarriesTheParentId()
    {
        if (!TryRun("child", out var r)) return;

        Assert.Equal("{\"name\":\"Kid\",\"parent_id\":3}", r.GetProperty("calls")[0].GetProperty("body").GetRawText());
    }

    [Fact]
    public void ARenameConflict_ShowsTheServersMessage_AndKeepsTheFormOpen()
    {
        if (!TryRun("rename", out var r)) return;

        var call = r.GetProperty("calls")[0];
        Assert.Equal("PATCH", call.GetProperty("method").GetString());
        Assert.Equal("/api/server-tags/1", call.GetProperty("url").GetString());
        Assert.Equal("{\"name\":\"West\"}", call.GetProperty("body").GetRawText());
        Assert.Contains("A tag named 'West' already exists under this parent.", r.GetProperty("text").GetString());
        Assert.Equal(1, r.GetProperty("formOpen").GetInt32());
    }

    [Fact]
    public void AnEdit_SendsOnlyChangedFields_AMoveToNoParentIsAnExplicitNull_AndTheSubtreeIsNotOfferedAsParent()
    {
        if (!TryRun("patch", out var r)) return;

        Assert.Equal("{\"colour\":\"#aabbcc\",\"parent_id\":null}", r.GetProperty("calls")[0].GetProperty("body").GetRawText());
        Assert.Equal(new[] { "", "1", "3" }, Strings(r.GetProperty("parentOptions")).Select(v => v).ToArray());
    }

    [Fact]
    public void AnInvalidWrite_ShowsTheServersMessageAsIs()
    {
        if (!TryRun("invalid", out var r)) return;

        Assert.Contains("'colour' must be #RRGGBB.", r.GetProperty("text").GetString());
    }

    [Fact]
    public void ADelete_AsksWithoutConfirm_ListsTheRules_AndRepeatsWithConfirmOnlyWhenAskedAgain()
    {
        if (!TryRun("delete", out var r)) return;

        var first = r.GetProperty("afterFirst");
        Assert.Equal(1, first.GetProperty("calls").GetInt32());
        Assert.Contains("West rule", first.GetProperty("text").GetString());
        Assert.Contains("Other rule", first.GetProperty("text").GetString());
        Assert.Equal(1, first.GetProperty("anyway").GetInt32());
        var calls = r.GetProperty("calls");
        Assert.Equal(2, calls.GetArrayLength());
        Assert.Equal("/api/server-tags/3", calls[0].GetProperty("url").GetString());
        Assert.Equal("/api/server-tags/3?confirm=true", calls[1].GetProperty("url").GetString());
        Assert.Contains("Tag deleted.", r.GetProperty("text").GetString());
        Assert.Contains("West rule", r.GetProperty("text").GetString());
        Assert.Equal(0, r.GetProperty("selectedAfter").GetInt32());
    }

    [Fact]
    public void ADeclinedDeleteDialog_SendsNothing()
    {
        if (!TryRun("deleteDeclined", out var r)) return;

        Assert.Equal(0, r.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void Apply_SendsOnePostForTheNewlyChecked_AndOneDeleteForTheNewlyUnchecked()
    {
        if (!TryRun("assign", out var r)) return;

        Assert.Equal(new[] { true, false, true }, r.GetProperty("before").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        var calls = r.GetProperty("calls");
        Assert.Equal(2, calls.GetArrayLength());
        Assert.Equal("POST", calls[0].GetProperty("method").GetString());
        Assert.Equal("/api/server-tags/1/servers", calls[0].GetProperty("url").GetString());
        Assert.Equal("{\"server_ids\":[11]}", calls[0].GetProperty("body").GetRawText());
        Assert.Equal("DELETE", calls[1].GetProperty("method").GetString());
        Assert.Equal("/api/server-tags/1/servers", calls[1].GetProperty("url").GetString());
        Assert.Equal("{\"server_ids\":[12]}", calls[1].GetProperty("body").GetRawText());
        Assert.Contains("Cover rule", r.GetProperty("text").GetString());
    }

    [Fact]
    public void TheRepaint_KeepsTheSelectedTag_AndTheUncheckedEdits()
    {
        if (!TryRun("repaint", out var r)) return;

        Assert.Equal(new[] { "1" }, Strings(r.GetProperty("pressed")));
        Assert.Equal(new[] { true, true, true }, r.GetProperty("checked").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.Equal(0, r.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void AReadOnlySignIn_RendersTheTree_WithEveryWriteControlAbsentAndTheBoxesDisabled()
    {
        if (!TryRun("readonly", out var r)) return;

        Assert.Equal(4, r.GetProperty("tree").GetInt32());
        Assert.Equal(0, r.GetProperty("newRoot").GetInt32());
        Assert.All(r.GetProperty("writes").EnumerateArray(), e => Assert.Equal(0, e.GetInt32()));
        Assert.All(r.GetProperty("disabled").EnumerateArray(), e => Assert.True(e.GetBoolean()));
    }

    private static string[] Methods(JsonElement calls) => calls.EnumerateArray().Select(c => c.GetProperty("method").GetString() + " " + c.GetProperty("url").GetString()).ToArray();

    [Fact]
    public void ACheckedBoxIsAnIntent_SoAServerAssignedElsewhereIsNeitherUncheckedNorUnassigned()
    {
        if (!TryRun("staleEdit", out var r)) return;

        Assert.Equal(new[] { true, true, true, true }, r.GetProperty("checkedAfterTick").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        var calls = r.GetProperty("calls");
        Assert.Equal(new[] { "POST /api/server-tags/1/servers" }, Methods(calls));
        Assert.Equal("{\"server_ids\":[11]}", calls[0].GetProperty("body").GetRawText());
    }

    [Fact]
    public void ARefresh_KeepsTheOpenFormsInputElementAndItsTypedValue()
    {
        if (!TryRun("tickForm", out var r)) return;

        Assert.True(r.GetProperty("same").GetBoolean());
        Assert.Equal("Typed", r.GetProperty("value").GetString());
        Assert.Equal(1, r.GetProperty("forms").GetInt32());
    }

    [Fact]
    public void ARefresh_UpdatesTheFormsParentOptions_WithoutReplacingTheTypedField()
    {
        if (!TryRun("tickParents", out var r)) return;

        Assert.True(r.GetProperty("same").GetBoolean());
        Assert.Contains("5", Strings(r.GetProperty("options")));
    }

    [Fact]
    public void ADoubleClickOnDeleteAnyway_SendsOneConfirmedDelete_AndListsTheRules()
    {
        if (!TryRun("dblDelete", out var r)) return;

        Assert.Equal(new[] { "DELETE /api/server-tags/3", "DELETE /api/server-tags/3?confirm=true" }, Methods(r.GetProperty("calls")));
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
        Assert.Contains("West rule", r.GetProperty("text").GetString());
    }

    [Fact]
    public void ADoubleClickOnSave_SendsOnePost_WithNoError()
    {
        if (!TryRun("dblSave", out var r)) return;

        Assert.Equal(new[] { "POST /api/server-tags" }, Methods(r.GetProperty("calls")));
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
    }

    [Fact]
    public void ADoubleClickOnApply_SendsOnePostAndOneDelete()
    {
        if (!TryRun("dblApply", out var r)) return;

        Assert.Equal(new[] { "POST /api/server-tags/1/servers", "DELETE /api/server-tags/1/servers" }, Methods(r.GetProperty("calls")));
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
    }

    [Fact]
    public void AnEditAnswered404_ClosesTheFormAndReReadsTheTree()
    {
        if (!TryRun("editGone", out var r)) return;

        Assert.Equal(0, r.GetProperty("formOpen").GetInt32());
        Assert.Equal(0, r.GetProperty("node").GetInt32());
        Assert.Contains("No such tag.", r.GetProperty("text").GetString());
    }

    [Fact]
    public void WhenThePostLandsAndTheDeleteFails_TheRulesGatheredSoFarAreShownWithTheError()
    {
        if (!TryRun("partial", out var r)) return;

        Assert.Equal(2, r.GetProperty("calls").GetInt32());
        Assert.Contains("Store unavailable.", r.GetProperty("text").GetString());
        Assert.Contains("Gained rule", r.GetProperty("text").GetString());
    }

    [Theory]
    [InlineData(403, "This session is read-only, so the change was not made.")]
    [InlineData(404, "No such tag.")] // an unknown parent, not a gone tag
    [InlineData(415, "Content-Type must be application/json.")]
    [InlineData(500, "Boom.")]
    [InlineData(0, "Network error: connection refused")]
    public void AFailedWrite_ShowsTheHouseMessage_AndKeepsTheForm(int status, string message)
    {
        if (!TryRun("refusal:" + status, out var r)) return;

        Assert.Contains(message, r.GetProperty("text").GetString());
        Assert.Equal(1, r.GetProperty("formOpen").GetInt32());
        Assert.Equal("Keep me", r.GetProperty("kept").GetString());
    }

    [Fact]
    public void AnEditAnsweredUnknownParent_KeepsTheFormTheNameAndTheTicks_AndResetsTheParent()
    {
        if (!TryRun("editParent", out var r)) return;

        Assert.Contains("Parent tag 2 does not exist.", r.GetProperty("text").GetString());
        Assert.Equal(1, r.GetProperty("formOpen").GetInt32());
        Assert.Equal("Renamed", r.GetProperty("kept").GetString());
        Assert.Equal("", r.GetProperty("parent").GetString());
        Assert.Equal(1, r.GetProperty("sel").GetInt32());
        Assert.Equal(1, r.GetProperty("apply").GetInt32());
    }

    [Fact]
    public void WhileAWriteIsInFlight_CancelAndTheFormOpenersAreDisabled_AndComeBackAfter()
    {
        if (!TryRun("busyCancel", out var r)) return;

        Assert.True(r.GetProperty("during").GetProperty("cancel").GetBoolean());
        Assert.True(r.GetProperty("during").GetProperty("opener").GetBoolean());
        Assert.False(r.GetProperty("after").GetBoolean());
    }

    [Fact]
    public void AfterARefusalSettles_ASecondSaveSendsASecondRequest()
    {
        if (!TryRun("secondSave", out var r)) return;

        Assert.Equal(2, r.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void WhenTheBaselineGainsTheTickedServerElsewhere_ApplySendsNothing()
    {
        if (!TryRun("baselineAgrees", out var r)) return;

        Assert.Equal(0, r.GetProperty("requests").GetInt32());
        Assert.Contains("No assignment changes", r.GetProperty("text").GetString());
    }

    [Theory]
    [InlineData(403, "This session is read-only, so the change was not made.")]
    [InlineData(500, "Refused 500.")]
    [InlineData(415, "Refused 415.")]
    public void AFailedDelete_ShowsTheHouseMessage_AndKeepsTheSelection(int status, string message)
    {
        if (!TryRun("deleteRefusal:" + status, out var r)) return;

        Assert.Contains(message, r.GetProperty("text").GetString());
        Assert.Equal(1, r.GetProperty("selected").GetInt32());
    }
}
