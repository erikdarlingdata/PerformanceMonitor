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
using System.Windows;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8364: <see cref="ViewerTimeHelper"/> keeps the display mode and the active server's clock in process-wide statics
/// (<c>CurrentDisplayMode</c>, <c>ActiveServerClock</c>, and <c>UtcOffsetMinutes</c>, whose setter installs a fixed-offset
/// clock). A test class that writes one of them carries <c>[Collection("viewer-time-statics")]</c>. That collection runs with
/// parallelization disabled, so no class in another collection is mid-assertion while a static is swapped. A writer outside it
/// runs beside the rest of the assembly, and either side can fail on a time text that has nothing to do with the code it tests.
/// A class that only reads the statics needs no collection: the collection running alone is what keeps them still while it
/// reads. Lite's <c>ServerTimeHelperCollectionTests</c> is the twin for <c>ServerTimeHelper</c>. This class constructs the shell
/// to prove the constructor writes, so it is in the collection too.
/// </summary>
[Collection("viewer-time-statics")]
[Trait("Stage", "Guard")]
public sealed class ViewerTimeStaticsCollectionTests
{
    private const string CollectionName = "viewer-time-statics";

    /// <summary>
    /// An assignment to one of the statics: the member name, then a single <c>=</c>. <c>==</c> and the <c>=&gt;</c> of a switch
    /// arm or a lambda are excluded by the lookahead, and <c>!=</c>, <c>&lt;=</c> and <c>&gt;=</c> put their operator character
    /// between the name and the <c>=</c>, where only whitespace may sit. A read of the same member matches nothing here.
    /// </summary>
    private static readonly Regex AssignsAStatic = new(
        @"\bViewerTimeHelper\s*\.\s*(ActiveServerClock|UtcOffsetMinutes|CurrentDisplayMode)\s*=(?![=>])",
        RegexOptions.CultureInvariant);

    /// <summary>
    /// Calls into product code that write a static on the caller's behalf, so a file that makes one is a writer without naming the
    /// helper: the shell's constructor applies the stored display mode (<see cref="TheShellsConstructor_WritesTheDisplayMode"/>
    /// proves it by constructing the shell), and a server tab's <c>ApplyServerClockToHelper</c> installs its clock (an internal
    /// member that needs a tab with a clock; no class outside the collection calls it).
    /// </summary>
    private static readonly Regex[] ProductWritersByCall =
    {
        new(@"\bnew\s+MainWindow\s*[({]", RegexOptions.CultureInvariant),
        new(@"\bApplyServerClockToHelper\s*\(", RegexOptions.CultureInvariant),
    };

    /// <summary>
    /// Test helpers in other files that set the mode for a read and put it back: <c>QueryGridSeed.ReadBanner</c>, and
    /// <c>DataStartBannerReadout.For</c> and <c>.Since</c>. Each names the test file that holds its assignment and the member around
    /// it, so <see cref="EveryTestHelperWriter_StillAssignsAStatic"/> fails when the rule goes stale.
    /// </summary>
    private static readonly (Regex Call, string File, string Member)[] TestHelperWriters =
    {
        (new Regex(@"\bQueryGridSeed\s*\.\s*ReadBanner\s*\(", RegexOptions.CultureInvariant),
            "Darling/Darling.Tests/ViewerQueryGridsDataStartLiveTests.cs", "ReadBanner"),
        (new Regex(@"\bDataStartBannerReadout\s*\.\s*(For|Since)\s*\(", RegexOptions.CultureInvariant),
            "Darling/Darling.Tests/ViewerConfigChangesDataStartTests.cs", "DataStartBannerReadout"),
    };

    /// <summary>A line that applies the collection attribute to the class below it; group 1 is its <c>[</c>.</summary>
    private static readonly Regex CollectionAttributeLine = new(
        @"^\s*(\[)Collection\(""" + CollectionName + @"""\)",
        RegexOptions.CultureInvariant | RegexOptions.Multiline);

    /// <summary>Anti-vacuity: well over this many files write the statics, so a scan that stops seeing the sources fails.</summary>
    private const int WriterFloor = 10;

    /// <summary>
    /// Whether a test source writes one of the statics, directly or through a call in <see cref="ProductWritersByCall"/> or
    /// <see cref="TestHelperWriters"/>, in code (comments and string literals do not count).
    /// </summary>
    internal static bool WritesTheStatics(string source)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        return AssignsAStatic.IsMatch(code)
            || ProductWritersByCall.Any(call => call.IsMatch(code))
            || TestHelperWriters.Any(writer => writer.Call.IsMatch(code));
    }

    /// <summary>
    /// Whether a test source applies the collection attribute. The name is a string literal, so the line is matched on the raw text,
    /// and the <c>[</c> must fall in code: a comment line that starts with the same text is prose.
    /// </summary>
    internal static bool CarriesTheCollection(string source)
    {
        var code = CSharpSourceWalker.CodeMask(source);
        return CollectionAttributeLine.Matches(source).Any(match => code[match.Groups[1].Index]);
    }

    /// <summary>
    /// The shell's constructor applies the stored display mode to the static, which is why <c>new MainWindow()</c> counts as a
    /// write. The stored value is whatever the machine's settings hold, so the proof presets two different modes and constructs the
    /// shell once from each: a constructor that writes the static lands both on the one stored mode, and one that does not leaves
    /// the two presets apart. The mode and the clock are put back afterwards.
    /// </summary>
    [Fact]
    public void TheShellsConstructor_WritesTheDisplayMode()
    {
        var (afterUtcPreset, afterLocalPreset) = StaTestThread.Run(() =>
        {
            var savedMode = ViewerTimeHelper.CurrentDisplayMode;
            var savedClock = ViewerTimeHelper.ActiveServerClock;
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary>
            {
                new() { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") },
                new() { Source = ThemeManager.SharedStylesUri },
            };
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }

            try
            {
                ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
                _ = new MainWindow();
                var first = ViewerTimeHelper.CurrentDisplayMode;
                ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
                _ = new MainWindow();
                return (first, ViewerTimeHelper.CurrentDisplayMode);
            }
            finally
            {
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }

                ViewerTimeHelper.CurrentDisplayMode = savedMode;
                ViewerTimeHelper.ActiveServerClock = savedClock;
            }
        });

        Assert.True(
            afterUtcPreset == afterLocalPreset,
            $"Constructing the shell from a preset of UTC left the display mode at {afterUtcPreset}, and from a preset of LocalTime "
            + $"at {afterLocalPreset}: the constructor no longer applies the stored mode, so new MainWindow() is not a write and "
            + "should leave ProductWritersByCall.");
    }

    /// <summary>
    /// Each call in <see cref="TestHelperWriters"/> stands for an assignment in another test file. This reads that file and checks
    /// the member is still there and the file still assigns a static in code, so the scan cannot keep treating a call as a write
    /// after the write has moved.
    /// </summary>
    [Fact]
    public void EveryTestHelperWriter_StillAssignsAStatic()
    {
        var repoRoot = FindRepoRoot();
        Assert.True(repoRoot is not null, $"Could not find the directory holding PerformanceMonitor.sln, walking up from {AppContext.BaseDirectory}.");

        var stale = new List<string>();
        foreach (var (call, file, member) in TestHelperWriters)
        {
            var path = Path.Combine(repoRoot!, file.Replace('/', Path.DirectorySeparatorChar));
            if (!File.Exists(path))
            {
                stale.Add($"{call}: {file} does not exist");
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
            if (!code.Contains(member, StringComparison.Ordinal))
            {
                stale.Add($"{call}: {file} no longer has {member}");
            }
            else if (!AssignsAStatic.IsMatch(code))
            {
                stale.Add($"{call}: {file} no longer assigns a ViewerTimeHelper static");
            }
        }

        Assert.True(
            stale.Count == 0,
            "These calls are counted as writes of ViewerTimeHelper's statics, and the file that justified each has changed: "
            + string.Join("; ", stale));
    }

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
