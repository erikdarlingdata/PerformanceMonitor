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
/// The proposal page and the route page's nobody-can-approve strips (#7109), run under Node (<c>web-proposals-harness.mjs</c>) on a fake DOM
/// and a scripted fetch: the two presses a mapped person makes and what each sends, a superseded press with what changed, the administrator
/// note before a press, a session that cannot press, and one strip for each route the check names. Node is skipped when it is not installed.
/// </summary>
public sealed class WebProposalsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-proposals-harness.mjs"));
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
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the proposals harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the proposals harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void AMappedPerson_ReadsTheFacts_PressesFirst_ThenConfirms_AndEachPressSendsAnEmptyObject()
    {
        var r = Run("person");

        var reason = r.GetProperty("reason").GetString()!;
        Assert.Contains("The agent's reason", reason);
        Assert.Contains("<b>blocks</b> everyone", reason);
        Assert.Equal(0, r.GetProperty("markupNodes").GetInt32());
        Assert.Contains("application_name: <img src=x onerror=alert(1)>", r.GetProperty("binding").GetString());
        Assert.Equal("#/investigations?request=aa000000-0000-4000-8000-000000000001", r.GetProperty("requestLink").GetString());
        Assert.Equal(["first-press"], r.GetProperty("buttonsBefore").EnumerateArray().Select(e => e.GetString()));
        Assert.Contains("proposal-actions", r.GetProperty("actionsClass").GetString());
        Assert.Contains("These are the facts now.", r.GetProperty("afterFirst").EnumerateArray().Select(e => e.GetString()));
        Assert.Contains("pid: 87", r.GetProperty("pressFacts").GetString());
        /* #7109: the first press reads as the person's name, never the stored key. */
        Assert.Contains("Ann at", r.GetProperty("firstPressText").GetString());
        Assert.DoesNotContain("person:", r.GetProperty("firstPressText").GetString());
        Assert.Equal(["first-press", "confirm"], r.GetProperty("buttonsAfter").EnumerateArray().Select(e => e.GetString()));
        var posts = r.GetProperty("posts").EnumerateArray().ToArray();
        Assert.Equal(2, posts.Length);
        Assert.Equal("/api/proposals/0b9a3f4e-1111-4222-8333-444455556666/first-press", posts[0].GetProperty("path").GetString());
        Assert.Equal("/api/proposals/0b9a3f4e-1111-4222-8333-444455556666/confirm", posts[1].GetProperty("path").GetString());
        Assert.All(posts, p => Assert.Empty(p.GetProperty("body").EnumerateObject()));
    }

    [Fact]
    public void ASupersededPress_ShowsItsMessageAsAnError_AndWhatChanged()
    {
        var r = Run("superseded");

        Assert.Contains(r.GetProperty("strips").EnumerateArray(), s =>
            s.GetProperty("kind").GetString()!.Contains("error") && s.GetProperty("text").GetString()!.Contains("The facts changed"));
        Assert.Equal("What changed: state, state_change", r.GetProperty("changed").GetString());
    }

    [Fact]
    public void AnAdministratorPath_SeesWhyATwoPersonRouteNeedsAMappedPerson_BeforeThePress()
    {
        var r = Run("administrator");

        Assert.Contains(r.GetProperty("strips").EnumerateArray(), s => s.GetString()!.Contains("A route that needs two people"));
        Assert.Equal(["first-press"], r.GetProperty("buttons").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void ASessionThatCannotPress_ReadsTheProposalAndTheList_WithTheNote_AndNoButton()
    {
        var r = Run("readOnly");

        Assert.Equal("State: Waiting for a person", r.GetProperty("state").GetString());
        Assert.Contains(r.GetProperty("strips").EnumerateArray(), s => s.GetString()!.Contains("Sign in to the web as an admin to press"));
        Assert.Equal(0, r.GetProperty("buttons").GetInt32());
        Assert.Equal(1, r.GetProperty("listRows").GetInt32());
        Assert.Equal("#/proposals?proposal=0b9a3f4e-1111-4222-8333-444455556666", r.GetProperty("listLink").GetString());
        Assert.Equal("data proposals", r.GetProperty("tableClass").GetString());
        Assert.Equal("table-wrap", r.GetProperty("tableWrapClass").GetString());
    }

    [Fact]
    public void TheRoutePage_ShowsTheSentence_ForEachRouteTheCheckNames_AndNothingForAFailedRead()
    {
        var r = Run("routeSentence");

        Assert.Equal(
            ["Route 4: Nobody can approve this. Map a person, or turn on single-person approval.",
             "Route 9: Nobody can approve this. Map a person, or turn on single-person approval."],
            r.GetProperty("strips").EnumerateArray().Select(e => e.GetString()));
        Assert.Equal(0, r.GetProperty("failed").GetInt32());
    }
}
