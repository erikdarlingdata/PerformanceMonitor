/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5602: every test that needs WPF runs its body on the one shared STA thread (<c>StaTestThread.Run</c>), not on a thread of its
/// own. About fifty test files used to start their own STA thread behind a gate (#5559): each left a dispatcher and a weak-event
/// table behind (#5596's 300 ms per thread at process exit), and a body that pumped could still overlap another. A new test that
/// starts its own thread, defines its own <c>OnStaThread</c>-style helper, or runs <c>Dispatcher.Run()</c> brings that back, so
/// this fails and names the file and the rule. A test that really must own its thread goes on <see cref="Allowed"/> with the
/// reason, and must take <c>StaTestThread.EnterExclusive()</c> around the thread so it cannot overlap a body on the shared one.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class StaThreadCensusTests
{
    /* Built from parts so this file does not contain the text it scans for. */
    private static readonly string s_sta = "ApartmentState" + ".STA";
    private static readonly string s_run = "Dispatcher" + ".Run(";
    private const string Exclusive = "StaTestThread.EnterExclusive()";

    /// <summary>
    /// A test that must own a thread, with the reason. None today: the host (StaTestThread.cs) and its own tests are in Darling.Tests,
    /// which this project links the host from. Lite.Tests has no test that needs a thread of its own.
    /// </summary>
    private static readonly Dictionary<string, string> Allowed = new(StringComparer.Ordinal)
    {
        ["StaThreadCensusTests.cs"] = "this census names the text it scans for",
    };

    /// <summary>Anti-vacuity: the suite has well over this many files that use the host, so a scan that stopped seeing them fails.</summary>
    private const int HostUserFloor = 20;

    /// <summary>A method named like the per-file helpers this replaced: OnStaThread, OnStaThread&lt;T&gt;, RunOnStaThread, OnDispatcher.</summary>
    private static readonly Regex s_helperDefinition = new(
        @"\bstatic\s+[\w<>\[\]?,\.\s()]+?\b(?:Run)?On(?:Sta(?:Thread)?|Dispatcher|UiThread)\s*(?:<\w+>)?\s*\(", RegexOptions.Compiled);

    [Fact]
    public void NoTestFileOutsideTheAllowList_StartsItsOwnStaThread_DefinesAnStaHelper_OrRunsADispatcher()
    {
        var violations = new List<string>();
        var hostUsers = 0;
        foreach (var (rel, text) in TestFiles())
        {
            if (text.Contains("StaTestThread.Run", StringComparison.Ordinal))
            {
                hostUsers++;
            }

            if (Allowed.ContainsKey(Path.GetFileName(rel)))
            {
                continue;
            }

            /* Code only: a comment or a string that names the old helper, the dispatcher or the apartment state is not a use of it. */
            var code = Darling.Tests.CSharpSourceWalker.StripCommentsAndStrings(text);
            if (code.Contains(s_sta, StringComparison.Ordinal))
            {
                violations.Add(rel + ": starts its own STA thread; run the body in StaTestThread.Run(...) instead");
            }

            if (s_helperDefinition.IsMatch(code))
            {
                violations.Add(rel + ": defines its own OnStaThread-style helper; call StaTestThread.Run(...) directly");
            }

            if (code.Contains(s_run, StringComparison.Ordinal))
            {
                violations.Add(rel + ": runs its own dispatcher; use the async overload StaTestThread.Run(async () => ...)");
            }
        }

        Assert.True(
            violations.Count == 0,
            "WPF tests share ONE STA thread per test process (#5602). Fix, or add the file to StaThreadCensusTests.Allowed with the reason it must "
            + "own a thread:" + Environment.NewLine + string.Join(Environment.NewLine, violations));
        Assert.True(hostUsers >= HostUserFloor, $"only {hostUsers} test files use StaTestThread.Run; the scan has stopped seeing them");
    }

    [Fact]
    public void ATestThatOwnsItsThread_TakesTheExclusiveGateAroundIt()
    {
        var missing = new List<string>();
        foreach (var (rel, text) in TestFiles())
        {
            var name = Path.GetFileName(rel);
            if (Allowed.ContainsKey(name) && name != "StaThreadCensusTests.cs"
                && !text.Contains(Exclusive, StringComparison.Ordinal))
            {
                missing.Add(rel);
            }
        }

        Assert.True(
            missing.Count == 0,
            "These tests own their STA thread but do not call " + Exclusive + " first, so they can overlap a body on the shared thread (#5559's race): "
            + string.Join(", ", missing));
    }

    [Fact]
    public void EveryAllowListEntry_SaysWhy()
    {
        Assert.All(Allowed, entry => Assert.True(entry.Value.Length >= 20, entry.Key + " is on the allow list without a reason"));
    }

    private static IEnumerable<(string Rel, string Text)> TestFiles()
    {
        var dir = ProjectDir();
        foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(dir, file);
            if (rel.StartsWith("bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || rel.StartsWith("obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            {
                continue;
            }

            yield return (rel, File.ReadAllText(file));
        }
    }

    private static string ProjectDir([CallerFilePath] string thisFile = "") => Path.GetDirectoryName(thisFile)!;
}
