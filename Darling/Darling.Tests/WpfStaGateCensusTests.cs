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
using System.Threading;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5559: every Darling.Tests test that starts its own STA thread takes <see cref="WpfStaGate"/> first. WPF's
/// <c>DependencyPropertyDescriptor</c> cache hands every STA thread in the process the same descriptor, and that descriptor's
/// <c>AddValueChanged</c> writes an unsynchronised dictionary, so two test classes in parallel corrupted it in CI. A new
/// test file that starts an STA thread without the gate brings the flake back, so this fails and names the file.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class WpfStaGateCensusTests
{
    /* Built from parts so this file does not contain the text it scans for. */
    private static readonly string s_sta = "ApartmentState" + ".STA";
    private const string Gate = "WpfStaGate.Enter()";

    /// <summary>Anti-vacuity: the suite has well over this many STA files, so a scan that stopped seeing them fails.</summary>
    private const int StaFileFloor = 20;

    [Fact]
    public void EveryStaThreadStart_IsPrecededByItsOwnGateEntry()
    {
        var missing = new List<string>();
        var staFiles = 0;
        foreach (var file in Directory.EnumerateFiles(ProjectDir(), "*.cs", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(ProjectDir(), file);
            if (rel.StartsWith("bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || rel.StartsWith("obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || Path.GetFileName(file) is "WpfStaGate.cs" or "WpfStaGateCensusTests.cs")
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (!text.Contains(s_sta, StringComparison.Ordinal))
            {
                continue;
            }

            staFiles++;

            /* Each STA start needs its OWN gate entry after the previous start: Enter, thread, STA, Enter, thread, STA. */
            var starts = Regex.Matches(text, Regex.Escape(s_sta)).Select(m => m.Index).ToList();
            var gates = Regex.Matches(text, Regex.Escape(Gate)).Select(m => m.Index).ToList();
            var previous = -1;
            foreach (var start in starts)
            {
                if (!gates.Any(g => g > previous && g < start))
                {
                    missing.Add(rel);
                    break;
                }

                previous = start;
            }
        }

        Assert.True(
            missing.Count == 0,
            "These test files start an STA thread without taking WpfStaGate first (put `using var staGate = WpfStaGate.Enter();` "
            + "before the helper creates the thread, #5559): " + string.Join(", ", missing));
        Assert.True(staFiles >= StaFileFloor, $"only {staFiles} STA test files were found; the scan has stopped seeing them");
    }

    [Fact]
    public void TheGate_LetsOnlyOneHolderIn_AtATime()
    {
        var inside = 0;
        var most = 0;
        var threads = Enumerable.Range(0, 3).Select(_ => new Thread(() =>
        {
            for (var i = 0; i < 10; i++)
            {
                using var staGate = WpfStaGate.Enter();
                var now = Interlocked.Increment(ref inside);
                InterlockedMax(ref most, now);
                Thread.Sleep(1);
                Interlocked.Decrement(ref inside);
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => Assert.True(t.Join(TimeSpan.FromSeconds(30)), "a gate holder never finished"));

        Assert.Equal(1, most);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while ((seen = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }

    private static string ProjectDir([CallerFilePath] string thisFile = "") => Path.GetDirectoryName(thisFile)!;
}
