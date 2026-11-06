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
using PerformanceMonitor.Collectors;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7813: on the Queries tab, Plan Corrections and Automatic Tuning come from one fan-out over get_plan_corrections, so for an empty
/// reply Plan Corrections prints the whole sentence the tool chose and Automatic Tuning points at it. The shipped page scripts run
/// under Node (<c>automatic-tuning-empty-harness.mjs</c>) through the real Queries tab with the two real sentences; Node is skipped
/// when it is not installed.
/// </summary>
public sealed class AutomaticTuningEmptyHarnessTests
{
    private static readonly string[] Sentences =
    {
        PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }),
        PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 }),
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
    public void PlanCorrectionsPrintsEachSentenceWhole_AndAutomaticTuningPointsAtIt()
    {
        var shown = Result.Value;
        Assert.Equal(Sentences.Length, shown.GetArrayLength());

        for (var i = 0; i < Sentences.Length; i++)
        {
            var planCorrections = shown[i].GetProperty("planCorrections");
            Assert.Equal(Sentences[i], planCorrections.GetProperty("text").GetString());
            Assert.Equal(0, planCorrections.GetProperty("details").GetInt32());

            var automaticTuning = shown[i].GetProperty("automaticTuning");
            Assert.Equal("Empty for the same reason as Plan Corrections.", automaticTuning.GetProperty("text").GetString());
            Assert.Equal(0, automaticTuning.GetProperty("details").GetInt32());
        }
    }

    [Fact]
    public void TheTwoSentencesDiffer_SoThePageNamesTheCase()
    {
        Assert.NotEqual(Sentences[0], Sentences[1]);
        Assert.Contains("has no automatic tuning", Sentences[0], StringComparison.Ordinal);
        Assert.Contains("has not stored a cycle", Sentences[1], StringComparison.Ordinal);
        Assert.Contains("ollection health", Sentences[1], StringComparison.Ordinal);
    }
}
