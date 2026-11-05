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
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7675: the Query Store Health grid marks a store in the ERROR state and shows the one-line meaning. These run the shipped <c>sql-config.js</c> and
/// <c>panels.js</c> under Node (<c>web-qs-health-error-harness.mjs</c>); skipped when Node is not installed. The pure rules the service and the
/// Viewer share (<see cref="QueryStoreReadonlyReason.DecodeWithState"/>, <see cref="CollectorRuntimePrecondition.QueryStoreErrorNote"/>) are here too.
/// </summary>
public sealed class WebQueryStoreHealthErrorBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-qs-health-error-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the Query Store health harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the Query Store health harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void AnErrorRow_IsMarked_AndCarriesTheLine_AndAReadWriteRowIsPlain()
    {
        var rows = Run();
        var error = rows[0];
        Assert.Equal("! ERROR", error.GetProperty("actual").GetProperty("text").GetString());
        Assert.Contains("sev-High", error.GetProperty("actual").GetProperty("classes").EnumerateArray().Select(c => c.GetString()));
        Assert.Contains("stopped on an internal error", error.GetProperty("reason").GetProperty("text").GetString(), StringComparison.Ordinal);

        var ok = rows[1];
        Assert.Equal("READ_WRITE", ok.GetProperty("actual").GetProperty("text").GetString());
        Assert.Empty(ok.GetProperty("actual").GetProperty("classes").EnumerateArray());
    }

    [Theory]
    [InlineData("ERROR", 0, true)]
    [InlineData("error", 0, true)]
    [InlineData("ERROR", 1, true)]
    [InlineData("READ_WRITE", 0, false)]
    [InlineData("OFF", 0, false)]
    public void TheReasonCell_GetsTheErrorLine_OnlyForAnErrorStore(string state, int reason, bool hasLine)
    {
        var text = QueryStoreReadonlyReason.DecodeWithState(state, reason);
        Assert.Equal(hasLine, text.Contains(QueryStoreReadonlyReason.ErrorStateNote, StringComparison.Ordinal));
        if (!hasLine) Assert.Equal(QueryStoreReadonlyReason.Decode(reason), text);
    }

    [Fact]
    public void TheNoteOnRows_NamesOnlyTheErrorDatabasesTheRowsCameFrom()
    {
        CollectorRuntimePrecondition.QueryStoreDatabaseState[] states =
        [
            new("ErrDb", "ERROR", 60), new("OtherErr", "ERROR", 60), new("RwDb", "READ_WRITE", 60),
        ];
        var note = CollectorRuntimePrecondition.QueryStoreErrorNote(states, null, ["ErrDb", "RwDb"]);
        Assert.Contains("ErrDb", note, StringComparison.Ordinal);
        Assert.DoesNotContain("OtherErr", note, StringComparison.Ordinal);
        Assert.Null(CollectorRuntimePrecondition.QueryStoreErrorNote(states, null, ["RwDb"]));
    }
}
