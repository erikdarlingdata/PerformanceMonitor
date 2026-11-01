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
/// The Admin "People" tab (#7107), run under Node (<c>web-people-harness.mjs</c>) on a fake DOM and a scripted fetch: the session's own sign-in
/// key with its copy button, "Add my own sign-in", the shared web token's missing key, a viewer's own key with no list, and the args each form
/// sends. Node is skipped when it is not installed.
/// </summary>
public sealed class WebPeopleBehaviourTests
{
    private const string Key = "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789-_abcde";

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-people-harness.mjs"));
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
                Assert.Fail("the people harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the people harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ASessionWithASignInKey_SeesItWithACopyButton_AndAddMyOwnSignIn_SendsExactlyThatKeyAsAWebSignIn()
    {
        var r = Run("ownKey");

        Assert.Equal(Key, r.GetProperty("keyShown").GetString());
        Assert.Equal(Key, r.GetProperty("copied").GetString());
        Assert.True(r.GetProperty("detailShown").GetBoolean());
        var put = Assert.Single(r.GetProperty("puts").EnumerateArray());
        Assert.Equal("/api/people/add_person_identity", put.GetProperty("path").GetString());
        Assert.Equal(5, put.GetProperty("body").GetProperty("person_id").GetInt32());
        Assert.Equal("web", put.GetProperty("body").GetProperty("kind").GetString());
        Assert.Equal(Key, put.GetProperty("body").GetProperty("external_id").GetString());
    }

    [Fact]
    public void TheSharedWebToken_ShowsNoKey_SaysWhy_AndHasNoAddMyOwnSignIn()
    {
        var r = Run("sharedToken");

        Assert.False(r.GetProperty("keyShown").GetBoolean());
        Assert.True(r.GetProperty("noKeySentence").GetBoolean());
        Assert.False(r.GetProperty("addOwnPresent").GetBoolean());
        // The admin can still paste someone else's key.
        Assert.True(r.GetProperty("addSignInPresent").GetBoolean());
    }

    [Fact]
    public void AViewerSignIn_SeesOnlyItsOwnKeyAndTheCopyButton_AndNoList()
    {
        var r = Run("viewer");

        Assert.Equal(Key, r.GetProperty("keyShown").GetString());
        Assert.True(r.GetProperty("copyPresent").GetBoolean());
        Assert.False(r.GetProperty("listShown").GetBoolean());
        Assert.False(r.GetProperty("addPersonPresent").GetBoolean());
        Assert.Empty(r.GetProperty("otherButtons").EnumerateArray());
        Assert.Equal(0, r.GetProperty("strips").GetInt32());
    }

    [Fact]
    public void TheAdminForms_SendTheStoreArgs_SlackUpperCased_AndAnApproveGrantNeedsAKind()
    {
        var r = Run("forms");

        Assert.Equal(6, r.GetProperty("kindBoxes").GetInt32());
        Assert.True(r.GetProperty("noKindsRefused").GetBoolean());
        var puts = r.GetProperty("puts").EnumerateArray().Select(p => p.GetProperty("path").GetString() + " " + p.GetProperty("body").GetRawText()).ToArray();
        Assert.Equal(new[]
        {
            """/api/people/add_person {"display_name":"Bob"}""",
            """/api/people/add_person_identity {"person_id":5,"kind":"slack","external_id":"T9:U7"}""",
            """/api/people/add_person_grant {"person_id":5,"capability":"approve","scope_tag_id":7,"kinds":["evict_plan"]}""",
            """/api/people/set_person_enabled {"person_id":5,"enabled":false}""",
        }, puts);
    }
}
