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
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8364: <see cref="ViewerTimeHelper"/> keeps the display mode and the active server's clock in process-wide statics
/// (<c>CurrentDisplayMode</c>, <c>ActiveServerClock</c>, and <c>UtcOffsetMinutes</c>, whose setter installs a fixed-offset
/// clock). A test class that writes one of them carries <c>[Collection("viewer-time-statics")]</c>. That collection runs with
/// parallelization disabled, so no class in another collection is mid-assertion while a static is swapped. A writer outside it
/// runs beside the rest of the assembly, and either side can fail on a time text that has nothing to do with the code it tests.
/// A class that only reads the statics needs no collection: the collection running alone is what keeps them still while it
/// reads. Lite's <c>ServerTimeHelperCollectionTests</c> is the twin for <c>ServerTimeHelper</c>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ViewerTimeStaticsCollectionTests
{
    private const string CollectionName = "viewer-time-statics";

    /// <summary>
    /// An assignment to one of the statics: the member name, then a single <c>=</c>. <c>==</c> is excluded by the lookahead, and
    /// <c>!=</c>, <c>&lt;=</c> and <c>&gt;=</c> put their operator character between the name and the <c>=</c>, where only
    /// whitespace may sit. A read of the same member matches nothing here.
    /// </summary>
    private static readonly Regex AssignsAStatic = new(
        @"\bViewerTimeHelper\s*\.\s*(ActiveServerClock|UtcOffsetMinutes|CurrentDisplayMode)\s*=(?!=)",
        RegexOptions.CultureInvariant);

    /// <summary>A line that applies the collection attribute to the class below it.</summary>
    private static readonly Regex CollectionAttributeLine = new(
        @"^\s*\[Collection\(""" + CollectionName + @"""\)",
        RegexOptions.CultureInvariant | RegexOptions.Multiline);

    /// <summary>Anti-vacuity: well over this many files write the statics, so a scan that stops seeing the sources fails.</summary>
    private const int WriterFloor = 10;

    /// <summary>Whether a test source writes one of the statics, in code (comments and string literals do not count).</summary>
    internal static bool WritesTheStatics(string source) =>
        AssignsAStatic.IsMatch(CSharpSourceWalker.StripCommentsAndStrings(source));

    /// <summary>Whether a test source applies the collection attribute.</summary>
    internal static bool CarriesTheCollection(string source) => CollectionAttributeLine.IsMatch(source);

    /// <summary>
    /// The shapes the scan must read as a write: a direct assignment, the shell's constructor (<c>MainWindow()</c> applies the stored
    /// display mode through <c>ApplyTimeDisplayMode</c>), and the two test helpers that set and restore the mode for their caller
    /// (<c>QueryGridSeed.ReadBanner</c>, <c>DataStartBannerReadout.For</c> and <c>.Since</c>). And the shapes it must not: a read,
    /// a comparison that ends a switch arm's guard before its <c>=&gt;</c>, and an assignment inside a comment or a string.
    /// </summary>
    [Fact]
    public void TheScanSeesEveryWriterShape_AndNoReadShape()
    {
        Assert.True(WritesTheStatics("class C { void M() { ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC; } }"), "a direct assignment");
        Assert.True(WritesTheStatics("class C { void M() { ViewerTimeHelper.ActiveServerClock = ServerClock.Utc; } }"), "a direct assignment of the clock");
        Assert.True(WritesTheStatics("class C { void M() { var w = new MainWindow(); } }"), "the shell's constructor applies the stored display mode");
        Assert.True(WritesTheStatics("class C { void M() { QueryGridSeed.ReadBanner(banner => Task.CompletedTask); } }"), "QueryGridSeed.ReadBanner sets the mode for its caller");
        Assert.True(WritesTheStatics("class C { string M(DateTime utc) => DataStartBannerReadout.Since(utc); }"), "DataStartBannerReadout.Since sets the mode for its caller");
        Assert.True(WritesTheStatics("class C { string? M() => DataStartBannerReadout.For(1, 2); }"), "DataStartBannerReadout.For sets the mode for its caller");

        Assert.False(WritesTheStatics("class C { bool M(TimeDisplayMode m) => m == ViewerTimeHelper.CurrentDisplayMode; }"), "a comparison is a read");
        Assert.False(WritesTheStatics("class C { string M(TimeDisplayMode m) => m switch { _ when m == ViewerTimeHelper.CurrentDisplayMode => \"current\", _ => \"other\" }; }"), "the arrow of a switch arm is not an assignment");
        Assert.False(WritesTheStatics("class C { void M() { /* ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC; */ var s = \"ViewerTimeHelper.CurrentDisplayMode = x\"; } }"), "a comment or a string is not a write");
    }

    /// <summary>The attribute counts when its <c>[</c> is code, not when a comment line happens to start with the same text.</summary>
    [Fact]
    public void TheAttributeCountsOnlyInCode()
    {
        Assert.True(CarriesTheCollection("[Collection(\"viewer-time-statics\")]\npublic sealed class C { }"), "the attribute on its own line");
        Assert.True(CarriesTheCollection("/* why */\n[Collection(\"viewer-time-statics\")]\npublic sealed class C { }"), "the attribute after a comment");
        Assert.True(CarriesTheCollection("    [Collection(\"viewer-time-statics\")]\n    public sealed class C { }"), "the attribute indented");
        Assert.False(CarriesTheCollection("/* This class is NOT in\n   [Collection(\"viewer-time-statics\")]. */\npublic sealed class C { }"), "a comment line that starts with the attribute text");
    }

    [Fact]
    public void TheCollection_RunsAlone()
    {
        var definition = typeof(ViewerTimeStaticsCollection).GetCustomAttribute<CollectionDefinitionAttribute>();

        Assert.True(
            definition is not null && definition.Name == CollectionName && definition.DisableParallelization,
            $"[CollectionDefinition(\"{CollectionName}\", DisableParallelization = true)] is what lets a class outside the "
            + "collection read the statics without carrying it; found "
            + $"{(definition is null ? "no [CollectionDefinition]" : $"[CollectionDefinition(\"{definition.Name}\", DisableParallelization = {definition.DisableParallelization})]")}.");
    }

    /// <summary>
    /// Every public static member of <see cref="ViewerTimeHelper"/> that can be assigned is in <see cref="AssignsAStatic"/>.
    /// A settable static added later fails here until the pattern names it, because a class writing only that one would not be
    /// required to carry the collection.
    /// </summary>
    [Fact]
    public void AssignsAStatic_NamesEveryPublicStaticSetterOfViewerTimeHelper()
    {
        var type = typeof(ViewerTimeHelper);
        var settable = type.GetProperties(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(property => property.SetMethod is { IsPublic: true })
            .Select(property => property.Name)
            .Concat(type.GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Where(field => !field.IsInitOnly && !field.IsLiteral)
                .Select(field => field.Name))
            .Distinct()
            .ToList();

        Assert.True(
            settable.Contains("CurrentDisplayMode") && settable.Contains("ActiveServerClock"),
            "Reflection no longer sees the display mode and the server clock as settable statics of ViewerTimeHelper; "
            + $"found: {string.Join(", ", settable)}.");

        var unnamed = settable
            .Where(name => !AssignsAStatic.IsMatch($"ViewerTimeHelper.{name} = value;"))
            .ToList();

        Assert.True(
            unnamed.Count == 0,
            "These settable statics of ViewerTimeHelper are not in AssignsAStatic, so a test class that writes only them would "
            + $"not be required to carry [Collection(\"{CollectionName}\")]: {string.Join(", ", unnamed)}.");
    }

    [Fact]
    public void EveryTestFileThatWritesTheStatics_CarriesTheCollection()
    {
        var repoRoot = FindRepoRoot();
        Assert.True(
            repoRoot is not null,
            $"Could not find the directory holding PerformanceMonitor.sln, walking up from {AppContext.BaseDirectory}.");
        var testsDirectory = Path.Combine(repoRoot!, "Darling", "Darling.Tests");
        Assert.True(Directory.Exists(testsDirectory), $"{testsDirectory} does not exist.");

        var offenders = new List<string>();
        var filesThatWrite = 0;

        foreach (var path in Directory.EnumerateFiles(testsDirectory, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(testsDirectory, path);
            var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (Array.Exists(segments, s => s is "bin" or "obj")
                || string.Equals(Path.GetFileName(path), "ViewerTimeStaticsCollectionTests.cs", StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(path);
            if (!WritesTheStatics(text))
            {
                continue;
            }

            filesThatWrite++;
            if (!CarriesTheCollection(text))
            {
                offenders.Add(relative);
            }
        }

        Assert.True(
            filesThatWrite >= WriterFloor,
            $"Expected the scan to find the many classes that write ViewerTimeHelper's statics, found {filesThatWrite}; "
            + "the scan has stopped seeing the test sources.");
        Assert.True(
            offenders.Count == 0,
            "These test files write ViewerTimeHelper's process-wide statics without "
            + $"[Collection(\"{CollectionName}\")], so they can run beside a class in another collection that is reading a time "
            + $"text: {string.Join(", ", offenders.OrderBy(o => o, StringComparer.Ordinal))}");
    }

    /// <summary>Walks up from the test output directory to the repo root, the directory holding <c>PerformanceMonitor.sln</c>.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
