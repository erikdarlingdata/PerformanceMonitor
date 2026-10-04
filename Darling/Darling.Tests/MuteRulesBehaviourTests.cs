/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Mute Rules page's form logic, from the shipped <c>mute-rules.js</c> run under Node
/// (<c>mute-rules-harness.mjs</c>): which fields a PATCH carries, the explicit-null clear, the empty-create
/// warning and each server response the page handles. Skipped when Node is not installed.
/// </summary>
public sealed class MuteRulesBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "mute-rules-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "mute-rules.js"));
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the mute rules harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the mute rules harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void APatchCarriesOnlyTheChangedFields_AndNeverEnabled()
    {
        if (!TryRun("patch", out var r)) return;

        Assert.Equal("{\"reason\":\"now quiet\"}", r.GetProperty("onlyChanged").GetRawText());
        Assert.Equal("{}", r.GetProperty("nothing").GetRawText());
        Assert.False(r.GetProperty("withEnabled").GetBoolean());
    }

    [Fact]
    public void ABlankedField_IsSentAsAnExplicitNull()
    {
        if (!TryRun("clear", out var r)) return;

        Assert.Equal("{\"server_name\":null}", r.GetProperty("cleared").GetRawText());
        Assert.Equal("{\"expires_at_utc\":null}", r.GetProperty("expiryCleared").GetRawText());
    }

    [Fact]
    public void AnEmptyCreate_IsRecognised_AndATrimmedBodyHoldsOnlyFilledFields()
    {
        if (!TryRun("empty", out var r)) return;

        Assert.True(r.GetProperty("blank").GetBoolean());
        Assert.True(r.GetProperty("reasonOnly").GetBoolean());
        Assert.False(r.GetProperty("scoped").GetBoolean());
        Assert.Equal("{\"server_name\":\"A\",\"metric_name\":\"M\"}", r.GetProperty("body").GetRawText());
        Assert.Contains("mute EVERY alert", r.GetProperty("warning").GetString());
    }

    [Fact]
    public void EachServerResponse_IsInterpretedForThePage()
    {
        if (!TryRun("writes", out var r)) return;

        var exists = r.GetProperty("exists");
        Assert.Equal("exists", exists.GetProperty("kind").GetString());
        Assert.Equal("abc", exists.GetProperty("existingId").GetString());
        var invalid = r.GetProperty("invalid");
        Assert.Equal("invalid", invalid.GetProperty("kind").GetString());
        Assert.Equal("expires_at_utc", invalid.GetProperty("field").GetString());
        Assert.Equal("notfound", r.GetProperty("notfound").GetProperty("kind").GetString());
        Assert.Equal("readonly", r.GetProperty("readonly").GetProperty("kind").GetString());
        Assert.Equal("n1", r.GetProperty("created").GetProperty("rule").GetProperty("id").GetString());
    }
}
