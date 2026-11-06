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
using PerformanceMonitor.Collectors;
using Xunit;
using static Darling.Tests.RepoFile;
using static PerformanceMonitor.Collectors.CollectorRuntimePrecondition;

namespace Darling.Tests;

/// <summary>
/// #7813: the Automatic Tuning panel prints the whole sentence the tool chose for an empty reply, so a person on the page reads which case
/// applies. The shipped page scripts run under Node (<c>automatic-tuning-empty-harness.mjs</c>) with the three real sentences; Node is
/// skipped when it is not installed.
/// </summary>
public sealed class AutomaticTuningEmptyHarnessTests
{
    private static readonly string[] Sentences =
    {
        PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }, null),
        PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 }, new[] { new QueryStoreDatabaseState("DbA", "OFF") }),
        PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 }, new[] { new QueryStoreDatabaseState("DbA", "READ_WRITE") }),
    };

    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "automatic-tuning-empty-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        foreach (var sentence in Sentences)
            psi.ArgumentList.Add(sentence);
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the automatic tuning empty harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the automatic tuning empty harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ThePanelPrintsEachSentenceWhole_WithNothingBehindMoreDetail()
    {
        var shown = Result.Value.EnumerateArray().Select(s => (s.GetProperty("text").GetString(), s.GetProperty("detail").GetBoolean())).ToArray();

        Assert.Equal(Sentences.Select(s => ((string?)s, false)).ToArray(), shown);
    }

    [Fact]
    public void TheThreeSentencesDiffer_SoThePageNamesTheCase()
    {
        Assert.Equal(3, Sentences.Distinct(StringComparer.Ordinal).Count());
        Assert.Contains("has no automatic tuning", Sentences[0], StringComparison.Ordinal);
        Assert.Contains("no database has Query Store on", Sentences[1], StringComparison.Ordinal);
        Assert.Contains("nothing has been recommended", Sentences[2], StringComparison.Ordinal);
    }
}
