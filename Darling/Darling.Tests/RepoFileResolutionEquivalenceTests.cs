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
/// That <see cref="RepoFile"/> resolves to the SAME absolute path each of the eight resolvers it replaced
/// resolved to, for a live call site of every one of them.
///
/// <para><b>This is the defect a green build hides.</b> Consolidating thirty-four private readers is a
/// mechanical change whose every missed call site is a compile error — which is the good case. What a
/// compile cannot see is a replacement whose PATH RESOLUTION differs from the one it replaced: a
/// <see cref="CallerFilePathAttribute"/> walk and an <c>AppContext.BaseDirectory</c> walk-up do not resolve
/// the same root from the same call site, a probe for the target file resolves a root that varies with the
/// argument, and a marker of <c>PerformanceMonitor.Common</c> is not the marker
/// <c>PerformanceMonitor.sln</c> is. Every pin downstream reads a file and asserts on its text, so a
/// resolver that lands on a DIFFERENT file still returns a string and still runs its assertions — against
/// the wrong source. Nothing else in the suite would say so.</para>
///
/// <para><b>The replaced resolvers are reproduced here rather than described.</b> A sentence claiming they
/// agree is the artefact that rots; the code that used to run, run beside the code that replaced it, is
/// not. These five path shapes are transcriptions of what the thirty-four declarations actually did, and
/// they are the only copies of them left in the tree.</para>
///
/// <para><b>Why one shared answer is safe, asserted and not assumed.</b> Twenty-six of the replaced
/// resolvers walked up from the CALLING pin's own source path, so their answer depended on where that pin
/// lived. <see cref="RepoFile"/> resolves once from its own path instead, which is identical only while
/// every pin sits in the same directory it does — so
/// <see cref="EveryAdoptingPin_IsASiblingOfTheSharedReader"/> asserts that, and a pin moved into a
/// subdirectory reds there rather than silently resolving from a different starting point.</para>
/// </summary>
public sealed class RepoFileResolutionEquivalenceTests
{
    /// <summary>A call site that really exists, paired with the resolver that used to serve it.</summary>
    private sealed record Case(string Shape, string Pin, Func<string, string?> Resolve, string[] Segments, bool Lf);

    private static Case[] Cases(string sourceDirectory) => new[]
    {
        /* 17 of the 34. Walks up from the pin's own source directory PROBING for the relative path itself,
           so the root it resolves varies with the argument. AlertEngineTests is one. */
        new Case(
            "ProbeFromSource-raw",
            "AlertEngineTests",
            r => ProbeFromSourceDirectory(sourceDirectory, r),
            new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DatabaseStates.cs" },
            false),

        /* 4 of the 34. The same walk, normalising CRLF to LF on the way out. */
        new Case(
            "ProbeFromSource-lf",
            "ChartWindowDomainTests",
            r => ProbeFromSourceDirectory(sourceDirectory, r),
            new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "charts.js" },
            true),

        /* 1 of the 34, and the shape the cross-app guard's census calls VariadicSegmentHelper: the same
           walk again, reached through a params segment list with no Path.Combine at the call site. */
        new Case(
            "ProbeFromSource-variadic",
            "FileGrowthAlertStoreTests",
            r => ProbeFromSourceDirectory(sourceDirectory, r),
            new[] { "Lite", "Services", "LocalDataService.FileGrowth.cs" },
            false),

        /* 8 of the 34. Walks up from the TEST BINARY's directory to the solution file, bounded at ten
           levels. This is the shape whose answer depends on where the output landed rather than on where
           the source is, and after this consolidation nothing outside this file resolves that way. */
        new Case(
            "SlnMarkerFromBaseDirectory",
            "DarlingFileSecurityTests",
            MarkerFromBaseDirectory,
            new[] { "Darling", "tools", "install-darling.ps1" },
            false),

        /* 1 of the 34. From the test binary's directory again, but PROBING for the target and unbounded.
           Its live call site spells the path with forward slashes, which is why RepoFile normalises
           separators rather than trusting Path.Combine to. */
        new Case(
            "ProbeFromBaseDirectory",
            "MigrationDataMovingRungCensusPins",
            ProbeFromBaseDirectory,
            new[] { "Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs" },
            false),

        /* 1 of the 34. A marker again, but the marker is a DIRECTORY named PerformanceMonitor.Common. */
        new Case(
            "CommonDirMarkerFromSource-raw",
            "CollectionLogFanoutRollupStoreTests",
            r => MarkerDirectoryFromSourceDirectory(sourceDirectory, r),
            new[] { "Lite", "Mcp", "McpHealthTools.cs" },
            false),

        /* 1 of the 34. The same marker, normalising to LF. */
        new Case(
            "CommonDirMarkerFromSource-lf",
            "ViewerSidebarDotRendersTheCardStatusTests",
            r => MarkerDirectoryFromSourceDirectory(sourceDirectory, r),
            new[] { "PerformanceMonitor.Common", "ServerHealthBands.cs" },
            true),

        /* 1 of the 34. Accepts the solution file OR a .git DIRECTORY as the root marker — which is the one
           replaced resolver that would have walked past the root of a git WORKTREE, where .git is a file.
           RepoFile takes the solution file alone for that reason. */
        new Case(
            "SlnOrGitMarkerFromSource",
            "StartupFailureTriageTests",
            r => SlnOrGitFromSourceDirectory(sourceDirectory, r),
            new[] { "Darling/PerformanceMonitor.Darling.Service/Program.cs" },
            true),
    };

    [Fact]
    public void EveryReplacedResolver_ResolvesTheSameAbsolutePath_AsTheSharedOne()
    {
        var cases = Cases(TestDirectory());

        /* A duplicated shape name is a shape silently dropped while every remaining case still reports a
           pass, so the names are required distinct rather than merely present. The census in
           CrossAppGuardCiGateTests floors itself the same way. */
        Assert.Equal(cases.Length, cases.Select(c => c.Shape).Distinct(StringComparer.Ordinal).Count());

        /* Eight, because eight is what the thirty-four declarations measured out to. A case deleted to make
           a red go away is the cheapest way this test passes while the thing it guards is broken. */
        Assert.Equal(8, cases.Length);

        foreach (var (shape, pin, resolve, segments, lf) in cases)
        {
            var relative = Path.Combine(segments).Replace('/', Path.DirectorySeparatorChar);

            var was = resolve(relative);

            /* The floor that makes the comparison below mean anything. A replaced resolver that finds
               NOTHING agrees with every possible answer once both sides are null, and two of these five
               shapes return null rather than throwing when they run out of parents. */
            Assert.False(
                string.IsNullOrEmpty(was),
                $"the replaced '{shape}' resolver ({pin}) found nothing for {relative}. Either the "
              + "representative call site no longer exists, or — for the two shapes that start at "
              + "AppContext.BaseDirectory — the test binary is no longer inside the repository, in which "
              + "case this pin cannot compare what it exists to compare");

            var now = RepoFile.PathTo(segments);

            Assert.Equal(Path.GetFullPath(was!), Path.GetFullPath(now));

            /* Rooted and present and non-empty: "the same path" is also satisfied by two identical
               relative strings that name nothing, and by a file that exists and is empty — against which
               every Contains assertion downstream passes vacuously. */
            Assert.True(Path.IsPathRooted(now), $"{shape}: {now} is not an absolute path");
            Assert.True(File.Exists(now), $"{shape}: {now} does not exist");

            var text = lf ? RepoFile.ReadRepoFileLf(segments) : RepoFile.ReadRepoFile(segments);
            Assert.NotEmpty(text);

            /* And the CONTENT the pin actually consumes, which is where the raw/LF split lives. Compared
               against the bytes on disk put through the transform that pin's own helper applied, so a
               reader that resolved the right file and then handed back the wrong newlines still reds. */
            var onDisk = File.ReadAllText(was!);
            Assert.Equal(lf ? onDisk.Replace("\r\n", "\n", StringComparison.Ordinal) : onDisk, text);
        }
    }

    [Fact]
    public void TheSharedReader_StillFailsOnAPathThatDoesNotExist()
    {
        /* The discrimination control. Every assertion above compares two answers, and a resolver that
           concatenated its argument onto anything at all and declared success would satisfy all of them —
           both sides being wrong in the same way. This is the case that must NOT resolve. */
        var missing = new[] { "Darling", "Darling.Tests", "NoSuchFileExists.cs" };

        Assert.Throws<FileNotFoundException>(() => RepoFile.ReadRepoFile(missing));
        Assert.Throws<FileNotFoundException>(() => RepoFile.ReadRepoFileLf(missing));

        /* And the same argument through the resolver that probes for its target, which reports absence by
           returning null rather than by throwing. */
        Assert.Null(ProbeFromSourceDirectory(TestDirectory(), Path.Combine(missing)));

        /* The positive half, so "throws" is not merely what this helper does with every input. */
        Assert.NotEmpty(RepoFile.ReadRepoFile("PerformanceMonitor.sln"));
    }

    [Fact]
    public void EveryAdoptingPin_IsASiblingOfTheSharedReader()
    {
        var directory = TestDirectory();

        var elsewhere = Directory
            .EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Where(f =>
                !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
                !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !string.Equals(Path.GetDirectoryName(f), directory, StringComparison.Ordinal))
            .Where(f => CSharpSourceWalker
                .StripCommentsAndStrings(File.ReadAllText(f))
                .Contains("ReadRepoFile", StringComparison.Ordinal))
            .Select(f => Path.GetRelativePath(directory, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();

        Assert.Empty(elsewhere);

        /* The population floor. "No pin outside this directory" is trivially true of a sweep that found no
           pins at all, and this assertion's whole subject is where the pins are. */
        var siblings = Directory
            .EnumerateFiles(directory, "*.cs", SearchOption.TopDirectoryOnly)
            .Count(f => CSharpSourceWalker
                .StripCommentsAndStrings(File.ReadAllText(f))
                .Contains("ReadRepoFile", StringComparison.Ordinal));

        Assert.True(
            siblings >= 30,
            $"only {siblings} files beside RepoFile.cs mention the shared reader, so the emptiness "
          + "asserted above is not evidence that the pins are all siblings");
    }

    /* ---- The resolvers this consolidation replaced, transcribed. ---- */

    /// <summary>Walks up from the pin's own source directory probing for the relative path itself.</summary>
    private static string? ProbeFromSourceDirectory(string sourceDirectory, string relative)
    {
        for (var dir = new DirectoryInfo(sourceDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return null;
    }

    /// <summary>Walks up from the test binary's directory to the solution file, bounded at ten levels.</summary>
    private static string? MarkerFromBaseDirectory(string relative)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return Path.Combine(directory.FullName, relative);
            }

            directory = directory.Parent;
        }

        return null;
    }

    /// <summary>Walks up from the test binary's directory probing for the target, unbounded.</summary>
    private static string? ProbeFromBaseDirectory(string relative)
    {
        var dir = AppContext.BaseDirectory;

        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Directory.GetParent(dir)?.FullName;
        }

        return dir is null ? null : Path.Combine(dir, relative);
    }

    /// <summary>Walks up from the pin's own source directory to a directory named
    /// <c>PerformanceMonitor.Common</c>.</summary>
    private static string? MarkerDirectoryFromSourceDirectory(string sourceDirectory, string relative)
    {
        for (var dir = new DirectoryInfo(sourceDirectory); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Common")))
            {
                return Path.Combine(dir.FullName, relative);
            }
        }

        return null;
    }

    /// <summary>Walks up from the pin's own source directory accepting the solution file or a <c>.git</c>
    /// directory as the marker.</summary>
    private static string? SlnOrGitFromSourceDirectory(string sourceDirectory, string relative)
    {
        var dir = sourceDirectory;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir is null ? null : Path.Combine(dir, relative.Replace('/', Path.DirectorySeparatorChar));
    }

    private static string TestDirectory([CallerFilePath] string thisFile = "")
        => Path.GetDirectoryName(thisFile)!;
}
