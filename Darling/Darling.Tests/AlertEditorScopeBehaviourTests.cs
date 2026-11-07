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
/// The web alert rule editor's reading of a saved scope (#8077), run in Node: a scope with a servers list and no mode loads as the
/// servers mode and saves it back, a tagId with no mode loads as the tag mode, and the two shapes the editor cannot read without
/// guessing (both keys, or an explicit mode beside another mode's key) are left as they are for the validator to refuse. Behaviour
/// only; the spelling of the editor's source is not pinned.
/// </summary>
public sealed class AlertEditorScopeBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-editor-scope-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped editor script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the alert editor scope harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the alert editor scope harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void AServersListWithNoMode_LoadsAsServers_AndSavesThatMode()
    {
        var shape = Run().GetProperty("serversOnly");
        Assert.Equal("servers", shape.GetProperty("scopeMode").GetString());
        Assert.Equal(new[] { "alpha", "beta" }, Strings(shape.GetProperty("scopeServers")));
        var saved = shape.GetProperty("saved");
        Assert.Equal("servers", saved.GetProperty("mode").GetString());
        Assert.Equal(new[] { "alpha", "beta" }, Strings(saved.GetProperty("servers")));
    }

    [Fact]
    public void ATagIdWithNoMode_LoadsAsTag_AndSavesThatMode()
    {
        var shape = Run().GetProperty("tagOnly");
        Assert.Equal("tag", shape.GetProperty("scopeMode").GetString());
        Assert.Equal(7, shape.GetProperty("scopeTagId").GetInt32());
        var saved = shape.GetProperty("saved");
        Assert.Equal("tag", saved.GetProperty("mode").GetString());
        Assert.Equal(7, saved.GetProperty("tagId").GetInt32());
    }

    [Fact]
    public void BothKeysWithNoMode_IsNotGuessed_TheEditorStaysOnAll()
    {
        // The validator refuses the stored rule and says so; the editor does not pick one of the two keys for the author.
        var shape = Run().GetProperty("both");
        Assert.Equal("all", shape.GetProperty("scopeMode").GetString());
    }

    [Fact]
    public void AnExplicitMode_IsKept_BesideAKeyFromAnotherMode()
    {
        // An explicit mode is the author's statement; the editor shows it and the validator's refusal of the stray key stands.
        var shape = Run().GetProperty("explicitAllWithServers");
        Assert.Equal("all", shape.GetProperty("scopeMode").GetString());
    }
}
