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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which command-timeout pins route their deadline judgement through the shared
/// <see cref="CommandDeadlineScanner"/> — asserted, rather than stated in prose and left to rot.
///
/// <para><b>This exists because that number was wrong three times in one day.</b> #2938 extracted the
/// judgement and described five pins as asking it; #2940 made it six, #2966 seven, #2972 nine. Every one of
/// those drifts was invisible, because a count written in a doc comment has nothing checking it. The
/// enumerated LIST beside the count rotted the same way, which is the worse half: a reader looking for the
/// adopters found the pins named at extraction time and no hint that others had joined. So the set is
/// written down once, here, and the build re-derives it from the tree.</para>
///
/// <para><b>Both halves are declared, and the second is what earns this test.</b> Listing the adopters
/// catches a pin that quietly STOPS calling the scanner. Listing the abstainers catches the likelier case:
/// a new <c>*CommandTimeoutTests.cs</c> arriving with its own private copy of the rule, which is exactly
/// how the two holdouts #2972 consolidated came to exist — each had drifted a little from the others before
/// anyone noticed they were the same judgement. A pin that lands in neither list fails asking which it is,
/// instead of joining the family by adjacency or skipping it by omission.</para>
/// </summary>
public sealed class CommandDeadlineScannerAdoptionTests
{
    /// <summary>
    /// The pins that ask <see cref="CommandDeadlineScanner.SetsAnExplicitDeadline"/> whether a site set a
    /// deadline at all.
    /// </summary>
    private static readonly string[] s_adopters =
    {
        "AlertPassCommandTimeoutTests.cs",
        "AnalysisPassCommandTimeoutTests.cs",
        "CollectionSweepCommandTimeoutTests.cs",
        "CommandPlaneCommandTimeoutTests.cs",
        "FactCollectorCommandTimeoutTests.cs",
        "McpReadCommandTimeoutTests.cs",
        "StorageCommandTimeoutTests.cs",
        "StragglerCommandTimeoutTests.cs",
        "ViewerCommandTimeoutTests.cs",
    };

    /// <summary>
    /// The pins that deliberately do NOT, with the reason — because an unexplained abstention is
    /// indistinguishable from an oversight, and this family has already produced both.
    ///
    /// <para><c>StartupCommandTimeoutTests</c> judges every site RELATIONALLY: each must carry the named
    /// <see cref="PerformanceMonitor.Darling.Service.ServiceCommandDeadlines"/> constant for its own
    /// bootstrap regime. The shared scanner cannot express WHICH constant a site must take, only that some
    /// deadline is set, so routing that pin through it would answer a weaker question than the pin already
    /// answers. The same reasoning keeps the value-bound halves of the straggler and command-plane pins in
    /// place alongside their scanner calls.</para>
    /// </summary>
    private static readonly string[] s_abstainers =
    {
        "StartupCommandTimeoutTests.cs",
    };

    [Fact]
    public void EveryCommandTimeoutPin_EitherAdoptsTheSharedScanner_OrIsADeclaredAbstainer()
    {
        var directory = TestDirectory();

        var pins = Directory
            .EnumerateFiles(directory, "*CommandTimeoutTests.cs", SearchOption.TopDirectoryOnly)
            .Select(p => Path.GetFileName(p)!)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();

        /* A floor rather than an equality, for the reason the rest of this family gives: it catches a sweep
           that read the wrong directory and reported clean on nothing. The claims that matter are the two
           set equalities below, which move whenever a pin is added, removed or switched. */
        Assert.True(
            pins.Length >= 10,
            $"the sweep found only {pins.Length} *CommandTimeoutTests.cs files — it is not reading the test project");

        var adopters = new List<string>();
        var abstainers = new List<string>();

        foreach (var pin in pins)
        {
            /* STRIPPED, so prose about the scanner does not count as routing through it. Every file in this
               family discusses the shared judgement at length in doc comments, and several name the method;
               read raw, this test would pass by finding the commentary rather than the call. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(directory, pin)));
            var adopts = code.Contains("CommandDeadlineScanner.SetsAnExplicitDeadline", StringComparison.Ordinal);

            (adopts ? adopters : abstainers).Add(pin);
        }

        Assert.Equal(s_adopters.OrderBy(f => f, StringComparer.Ordinal).ToArray(), adopters.ToArray());
        Assert.Equal(s_abstainers.OrderBy(f => f, StringComparer.Ordinal).ToArray(), abstainers.ToArray());
    }

    private static string TestDirectory([CallerFilePath] string thisFile = "")
        => Path.GetDirectoryName(thisFile)!;
}
