/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7565: when the service saves a new view with a warning, the composer stays on the page and the view is now the one being edited, so
/// pressing Save again must UPDATE it (a PUT to its id) and not create a second copy (a second POST). Run as the shipped
/// <c>notebook.js</c> and <c>editor.js</c> under Node (<c>view-editor-save-harness.mjs</c>) against a scripted service. Node is skipped
/// when it is not installed.
/// </summary>
public sealed class ViewEditorSaveBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(RepoFile.PathTo("Darling", "Darling.Tests", "view-editor-save-harness.mjs"));
        psi.ArgumentList.Add(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the view editor save harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the view editor save harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    [Theory]
    [InlineData("editor")]
    [InlineData("notebook")]
    public void SavingAWarnedNewViewAgain_UpdatesTheSavedView_AndDoesNotCreateATwin(string composer)
    {
        var run = Run().GetProperty(composer);

        Assert.Equal(new[] { "POST /api/views" }, run.GetProperty("afterFirst").EnumerateArray().Select(x => x.GetString()!).ToArray());
        /* The second press is a PUT to the id the first save returned, at the version it returned: no second POST. */
        Assert.Equal(new[] { "POST /api/views", "PUT /api/views/41" }, run.GetProperty("all").EnumerateArray().Select(x => x.GetString()!).ToArray());
        Assert.Equal(1, run.GetProperty("versions")[1].GetInt32());
    }
}
