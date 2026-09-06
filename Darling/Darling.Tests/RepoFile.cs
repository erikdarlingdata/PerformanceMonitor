/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Darling.Tests;

/// <summary>
/// <para>Reads a file out of this repository by its repo-root-relative path — the one implementation every
/// source-text pin in this project shares. Thirty-four classes each carried their own private
/// <c>ReadRepoFile</c> before this, the same treatment <see cref="CSharpSourceWalker"/> gave the five copies
/// of the source walk in #2913 and <see cref="CommandDeadlineScanner"/> gave the copies of the deadline
/// judgement in #2938. Those copies had already drifted by the time they were consolidated, and this set had
/// drifted further: thirty-four declarations carrying EIGHT different resolution semantics.</para>
///
/// <para><b>The root is found from this file's own compile-time path, not from the test binary's
/// directory.</b> Nine of the copies walked up from <c>AppContext.BaseDirectory</c>, which ties a read of
/// SOURCE to wherever the OUTPUT landed — a coupling with nothing to do with the question being asked, and
/// one that silently changes answer when the runner's working layout changes. The path baked in by
/// <see cref="CallerFilePathAttribute"/> names the tree the assembly was compiled from, which is the tree
/// these pins are reading. Resolved from THIS file rather than from each caller, so there is one answer
/// rather than one per call site; the invariant that keeps those identical — every pin living in this one
/// directory — is asserted in <see cref="RepoFileResolutionEquivalenceTests"/> rather than assumed.</para>
///
/// <para><b>The root is a MARKER, not the target file.</b> Twenty-two of the copies walked up probing for
/// the RELATIVE PATH itself, so the root they resolved varied with the argument, and a path that did not
/// exist anywhere walked to the filesystem root and then dereferenced a null or read the wrong thing. One
/// marker gives one root for every read, and a missing file reports the absolute path it looked for.</para>
///
/// <para><b>Raw and LF-normalised are two methods because they were two behaviours.</b> Twenty-eight copies
/// returned the file as it sits on disk and six normalised CRLF to LF, and each pin's anchors were written
/// against what its own helper returned. This checkout is CRLF (<c>.gitattributes</c> is
/// <c>* text=auto eol=crlf</c>), so folding the two together would change what twenty-eight pins' anchors
/// match — a multi-line <c>DoesNotContain</c> that cannot currently fire would start firing, and a
/// multi-line <c>Contains</c> that cannot currently match would start matching. That is a behaviour change
/// and not a de-duplication, so the choice stays at the call site and stays visible in the method name.</para>
///
/// <para><b><c>params</c> rather than two overloads.</b> The copies split between a single pre-combined
/// relative path and a variadic segment list; one <c>params</c> parameter accepts both spellings with no
/// overload to resolve between, so no call site had to change shape to adopt this. Separators are
/// normalised because three call sites spell their path with forward slashes.</para>
///
/// <para><b>That there is exactly ONE of these is asserted, not stated.</b>
/// <see cref="RepoFileAdoptionTests"/> re-derives the declaring set from the tree, so a thirty-fifth private
/// copy arriving by paste fails the build instead of joining the family silently — which is how these
/// thirty-four came to exist. It carries the boundary of what that scan can and cannot see.</para>
///
/// <para>No xunit dependency, matching <see cref="CSharpSourceWalker"/>: a failure here is a
/// <see cref="FileNotFoundException"/> naming the absolute path, which reads the same in a test runner and
/// leaves this file shareable with <c>Lite.Tests</c> by <c>Compile Include</c> the way #2913 sanctioned.</para>
/// </summary>
internal static class RepoFile
{
    /// <summary>The tracked file that marks the repository root. A file rather than a directory, and the
    /// solution rather than <c>.git</c>, because a git WORKTREE has a <c>.git</c> FILE — so a
    /// <c>Directory.Exists(".git")</c> probe silently walks past the root of every worktree checkout.</summary>
    private const string RootMarker = "PerformanceMonitor.sln";

    /// <summary>The absolute path of the repository root.</summary>
    internal static string Root => ResolveRoot();

    /// <summary>Reads a repo file exactly as it sits on disk.</summary>
    internal static string ReadRepoFile(params string[] segments)
    {
        var full = PathTo(segments);

        if (!File.Exists(full))
        {
            throw new FileNotFoundException(
                $"source not found, this pin would read nothing: {full}", full);
        }

        return File.ReadAllText(full);
    }

    /// <summary>Reads a repo file with CRLF normalised to LF, for pins whose anchors span a line break.
    /// An anchor that embeds the wrong newline matches NOTHING and reads as clean, so the pins that write
    /// multi-line anchors take this one and the rest deliberately do not.</summary>
    internal static string ReadRepoFileLf(params string[] segments) =>
        ReadRepoFile(segments).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The absolute path a repo-root-relative spelling resolves to. Existence is NOT checked here:
    /// callers naming a DIRECTORY use <see cref="Root"/> directly, and the file check belongs with the
    /// read that would otherwise return nothing.
    ///
    /// <para>Internal rather than private so <see cref="RepoFileResolutionEquivalenceTests"/> can compare
    /// the PATH each replaced resolver produced against the path this one produces. Inferring that from
    /// content instead would be satisfied by two different files that happen to hold the same bytes — and
    /// this repository is full of deliberate parity copies, which is the reason that pin exists.</para></summary>
    internal static string PathTo(params string[] segments)
    {
        if (segments is null || segments.Length == 0)
        {
            throw new ArgumentException("at least one path segment is required", nameof(segments));
        }

        /* Both separators, so a path spelled either way resolves on either host. */
        var relative = Path.Combine(segments)
            .Replace('/', Path.DirectorySeparatorChar)
            .Replace('\\', Path.DirectorySeparatorChar);

        return Path.Combine(Root, relative);
    }

    private static string ResolveRoot([CallerFilePath] string thisFile = "")
    {
        var start = Path.GetDirectoryName(thisFile);

        if (string.IsNullOrEmpty(start))
        {
            throw new DirectoryNotFoundException(
                "RepoFile.cs has no compile-time directory to walk up from. A deterministic build that "
              + "rewrites CallerFilePath (PathMap) would do this, and every source-text pin in this project "
              + "resolves through here.");
        }

        for (var dir = new DirectoryInfo(start); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, RootMarker)))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException(
            $"Could not locate {RootMarker} walking up from {thisFile}. This resolves from the compile-time "
          + "path of RepoFile.cs, so it fails when the assembly was built from a source tree that is no "
          + "longer on disk.");
    }
}
