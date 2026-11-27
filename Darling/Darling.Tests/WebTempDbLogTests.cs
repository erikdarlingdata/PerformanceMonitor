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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8881 second step, the web page's half: the File I/O tab's tempdb chart gets a "Log used" series from get_tempdb_trend's peak_log_used_mb
/// and one plain line under the chart from log_holder_note. A reply with no log reading anywhere draws no "Log used" entry, and a null note draws
/// no line. Runs the shipped page scripts on a small fake DOM through web-tempdb-log-harness.mjs.
/// </summary>
public sealed class WebTempDbLogTests
{
    private const string Note = "Oldest tempdb log transaction: session 87, login app_user, program MyApp, open 14 minutes, 3.2 GB of log used.";

    private static string Reply(string logUsed, string? note) =>
        "{\"server\":\"s\",\"trend\":["
        + "{\"time\":\"2026-11-16T10:00:00\",\"total_reserved_mb\":10,\"user_objects_mb\":5,\"internal_objects_mb\":3,\"version_store_mb\":2,\"peak_log_used_mb\":null},"
        + "{\"time\":\"2026-11-16T10:05:00\",\"total_reserved_mb\":11,\"user_objects_mb\":5,\"internal_objects_mb\":3,\"version_store_mb\":2,\"peak_log_used_mb\":" + logUsed + "}],"
        + "\"log_holder_note\":" + (note is null ? "null" : JsonSerializer.Serialize(note)) + "}";

    private static JsonElement Run(params string[] replies)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-tempdb-log-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        foreach (var reply in replies)
        {
            psi.ArgumentList.Add(reply);
        }

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the tempdb log harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the tempdb log harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void AReplyWithALogReading_DrawsALogUsedEntry_AndTheHolderLine()
    {
        var shown = Run(Reply("500", Note))[0];

        Assert.True(shown.GetProperty("found").GetBoolean());
        var text = shown.GetProperty("text").GetString()!;
        Assert.Equal(1, shown.GetProperty("logUsedMentions").GetInt32());
        Assert.EndsWith(Note, text, StringComparison.Ordinal);
    }

    [Fact]
    public void AReplyWithNoLogReadingAndNoHolder_DrawsNeitherTheSeriesNorALine()
    {
        var shown = Run(Reply("null", null))[0];

        Assert.True(shown.GetProperty("found").GetBoolean());
        Assert.Equal(0, shown.GetProperty("logUsedMentions").GetInt32());
        Assert.DoesNotContain("Oldest tempdb log transaction", shown.GetProperty("text").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void ALogReadingWithNoHolder_DrawsTheSeriesButNoLine()
    {
        var shown = Run(Reply("500", null))[0];

        Assert.Equal(1, shown.GetProperty("logUsedMentions").GetInt32());
        Assert.DoesNotContain("Oldest tempdb log transaction", shown.GetProperty("text").GetString()!, StringComparison.Ordinal);
    }
}
