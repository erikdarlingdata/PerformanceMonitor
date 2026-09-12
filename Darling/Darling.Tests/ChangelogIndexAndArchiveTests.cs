/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>CHANGELOG.md</c> is an index and the prose lives in <c>docs/changelog/</c>, and this is what keeps the
/// two halves describing the same history.
///
/// <para>The split happened because GitHub will not render a blob the size the single file had reached
/// (2.06 MB), which made the project's own record unreadable in the web UI. Moving prose out is only
/// acceptable while it is provably the SAME prose, and a size reduction proves nothing: a transform that
/// silently drops an entry and one that silently passes it through unchanged both shrink the file.</para>
///
/// <para><b>What this covers and what it does not.</b> Here: every archive's prose still hashes to what was
/// taken out of <c>CHANGELOG.md</c>, the index carries exactly one compacted line per archived entry, every
/// <c>[#N]</c> resolves in the file that uses it, and every file is under the size a browser will render.
/// The whole-document reconstruction - substituting each archive back into the pre-split file and comparing
/// byte for byte - needs that revision to compare against, so it lives in
/// <c>tools/changelog/changelog_archive.py roundtrip</c> and its <c>--self-test</c>, which corrupts an archive
/// six ways and asserts the comparison reports each one. Neither half covers the other's: the round trip
/// never reads an archive's link definitions, and nothing here reads the pre-split bytes.</para>
///
/// <para>The counts and hashes come from <c>tools/changelog/archive-census.txt</c> rather than being written
/// out again here, because the Python tool reads the same file. A literal mirrored into two places goes stale
/// in one of them and then reports its own staleness as a defect in whatever it was checking.</para>
///
/// <para>Everything else is DERIVED, so a release cut does not have to remember this file: the heading form
/// and ordering, which versions must have an archive, and the size ceilings are all read off the index.
/// Regenerating the census after <c>split</c> is the one step the release process owns.</para>
/// </summary>
public sealed class ChangelogIndexAndArchiveTests
{
    /* Under the ~1 MB where GitHub stops rendering, with room for the release that has not been cut yet.
       The index is held tighter than an archive because every release adds to it and only to it. */
    private const int IndexCeilingBytes = 750 * 1024;
    private const int ArchiveCeilingBytes = 1024 * 1024;

    /* The pre-split census. A FLOOR rather than an equality: [Unreleased] grows with every lane, and
       entries only ever move between sections, so the total can rise but must never fall. */
    private const int PreSplitEntryCount = 1329;

    /* Exactly the form MigrationUpgradeLadderLiveTests.TheMostRecentRelease_HasALadderFixture matches to
       derive which ladder fixture must exist. It takes the FIRST such heading that is not
       Directory.Build.props's version, so the index has to keep every heading in this shape and in
       descending order or that guard silently starts demanding a different fixture. */
    private static readonly Regex ReleaseHeading = new(
        @"^## \[(?<version>\d+\.\d+\.\d+)\] - \d{4}-\d{2}-\d{2}$", RegexOptions.Compiled);

    private static readonly Regex LinkDefinition = new(@"^\[[^\]]+\]: \S+$", RegexOptions.Compiled);
    private static readonly Regex IssueReference = new(@"\[#(?<number>\d+)\]", RegexOptions.Compiled);

    /// <summary>
    /// Every archive still holds, byte for byte, the prose that came out of <c>CHANGELOG.md</c> — and holds
    /// the same number of entries the index shows a line for.
    ///
    /// <para>The hash covers the version heading plus the prose under it, normalised to LF. Not the archive's
    /// header paragraph, which is new text, and not its link-definition block, which is a rebuilt index of
    /// what the prose references rather than content that was moved. Both of those can be rewritten without
    /// touching what was archived, and a hash that covered them would go red for a wording change and teach
    /// people to regenerate it without looking.</para>
    /// </summary>
    [Fact]
    public void EveryArchive_StillHoldsTheProseThatLeftTheIndex()
    {
        var census = Census();
        var sections = IndexSections();
        var archived = sections.Where(s => s.Archived).ToList();

        /* A census that has stopped describing the archives is the failure mode a data-driven pin has, and
           it is indistinguishable from a pass if nobody checks the two sets against each other. */
        Assert.Equal(
            archived.Select(s => s.Version!).OrderBy(v => v, StringComparer.Ordinal).ToList(),
            census.Keys.OrderBy(v => v, StringComparer.Ordinal).ToList());
        Assert.NotEmpty(archived);

        foreach (var section in archived)
        {
            var path = ArchivePath(section.Version!);
            var archive = ReadLines(Path.Combine(RepoRoot(), path.Replace('/', Path.DirectorySeparatorChar)));
            var body = ArchiveBody(archive, path);

            Assert.Equal(section.Heading, body[0]);

            var expected = census[section.Version!];
            var digest = Convert.ToHexStringLower(
                SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("\n", body) + "\n")));
            Assert.Equal(expected.Digest, digest);

            var archiveEntries = TopLevelEntries(body.Skip(1).ToList());
            Assert.Equal(expected.Entries, archiveEntries);

            /* One index line per archived entry. This is the assertion that a compaction dropping an entry
               fails, and it is derived on both sides rather than counted once and trusted. */
            Assert.Equal(archiveEntries, TopLevelEntries(section.Prose));
        }
    }

    /// <summary>
    /// The index keeps every version heading in the form and the order its other readers derive from, links
    /// every archive, and invents an archive for nothing that has no prose to move.
    /// </summary>
    [Fact]
    public void TheIndex_KeepsEveryHeadingAndLinksEveryArchive()
    {
        var indexText = string.Join("\n", ReadLines(Path.Combine(RepoRoot(), "CHANGELOG.md")));
        var sections = IndexSections();
        var releases = sections.Where(s => s.Version is not null).ToList();

        Assert.True(releases.Count >= 24, $"only {releases.Count} release headings — the pre-split file had 24");

        foreach (var section in releases)
        {
            Assert.Matches(ReleaseHeading, section.Heading);
        }

        /* Newest first, strictly. The ladder guard takes the first heading that is not the in-development
           version, so a section that drifted out of order would hand it an older release without failing. */
        var ordered = releases.Select(s => ParseVersion(s.Version!)).ToList();
        for (var i = 1; i < ordered.Count; i++)
        {
            Assert.True(Compare(ordered[i - 1], ordered[i]) > 0,
                $"{releases[i - 1].Version} is not newer than {releases[i].Version} — the index is out of order");
        }

        foreach (var section in releases)
        {
            var path = ArchivePath(section.Version!);
            var file = Path.Combine(RepoRoot(), path.Replace('/', Path.DirectorySeparatorChar));

            if (section.Archived)
            {
                Assert.True(File.Exists(file), $"{section.Version} is compacted in the index but {path} is missing");
                Assert.Contains($"]({path})", indexText, StringComparison.Ordinal);
            }
            else
            {
                /* Pre-3.0 entries are already terse — mean 213 bytes against 1,915 in 3.7.0 — so there is
                   nothing to move, and an empty archive beside them would be a file to maintain that says
                   nothing. Asserted so the rule cannot drift into "archive everything". */
                Assert.False(File.Exists(file), $"{section.Version} keeps its prose in the index but {path} exists");
            }
        }
    }

    /// <summary>
    /// No file in the set carries a <c>[#N]</c> it cannot resolve, and none is large enough that GitHub
    /// refuses to render it — the two properties the split exists to buy.
    ///
    /// <para>Reference-style links resolve per FILE, so moving prose out of <c>CHANGELOG.md</c> without
    /// giving the archive its own definitions renders every issue link as literal bracketed text. That is a
    /// silent failure: the file still renders, and nothing is missing except the links.</para>
    /// </summary>
    [Fact]
    public void EveryFile_ResolvesItsOwnReferencesAndRendersOnGitHub()
    {
        foreach (var (path, ceiling) in Files())
        {
            var file = Path.Combine(RepoRoot(), path.Replace('/', Path.DirectorySeparatorChar));
            var bytes = new FileInfo(file).Length;
            Assert.True(bytes <= ceiling, $"{path} is {bytes:N0} bytes, past the {ceiling:N0} it must stay under");

            var lines = ReadLines(file);
            var defined = lines
                .Where(l => LinkDefinition.IsMatch(l))
                .Select(l => l[1..l.IndexOf(']', StringComparison.Ordinal)])
                .ToHashSet(StringComparer.Ordinal);

            var used = lines
                .Where(l => !LinkDefinition.IsMatch(l))
                .SelectMany(l => IssueReference.Matches(l).Select(m => "#" + m.Groups["number"].Value))
                .ToHashSet(StringComparer.Ordinal);

            /* Non-empty, or "every reference resolves" is satisfied by a file that references nothing. */
            Assert.NotEmpty(used);

            var undefined = used.Where(u => !defined.Contains(u)).OrderBy(u => u, StringComparer.Ordinal).ToList();
            Assert.True(undefined.Count == 0,
                $"{path} references {string.Join(", ", undefined.Take(20))} with no definition in that file");
        }
    }

    /// <summary>
    /// The entries survived the split. Counted over the index and the archives together, because an archived
    /// version's entries are in the archive and every other version's are still in the index.
    /// </summary>
    [Fact]
    public void TheEntryCount_NeverFallsBelowThePreSplitCensus()
    {
        var total = 0;
        foreach (var section in IndexSections())
        {
            if (!section.Archived)
            {
                total += BoldEntries(section.Prose);
                continue;
            }

            var path = ArchivePath(section.Version!).Replace('/', Path.DirectorySeparatorChar);
            var body = ArchiveBody(ReadLines(Path.Combine(RepoRoot(), path)), section.Version!);
            total += BoldEntries(body.Skip(1).ToList());
        }

        Assert.True(total >= PreSplitEntryCount,
            $"{total} bold-titled entries across the index and the archives, below the {PreSplitEntryCount} "
            + "the file held before the split — entries move between sections, they do not disappear");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────────

    private sealed record Row(int Entries, string Digest);

    private sealed class IndexSection
    {
        public required string Heading { get; init; }
        public required List<string> Prose { get; init; }
        public string? Version { get; init; }

        /* 3.x is the archived range: from 3.0.0 on, entries carry the prose the split exists to move. */
        public bool Archived => Version is not null && Version.StartsWith("3.", StringComparison.Ordinal);
    }

    private static Dictionary<string, Row> Census()
    {
        var path = Path.Combine(RepoRoot(), "tools", "changelog", "archive-census.txt");
        var rows = new Dictionary<string, Row>(StringComparer.Ordinal);
        foreach (var line in ReadLines(path))
        {
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            Assert.Equal(3, parts.Length);
            rows[parts[0]] = new Row(int.Parse(parts[1], CultureInfo.InvariantCulture), parts[2]);
        }

        Assert.NotEmpty(rows);
        return rows;
    }

    private static List<IndexSection> IndexSections()
    {
        var lines = ReadLines(Path.Combine(RepoRoot(), "CHANGELOG.md"));
        var starts = Enumerable.Range(0, lines.Count)
            .Where(i => lines[i].StartsWith("## [", StringComparison.Ordinal))
            .ToList();
        Assert.NotEmpty(starts);

        var sections = new List<IndexSection>();
        for (var k = 0; k < starts.Count; k++)
        {
            var start = starts[k];
            var end = k + 1 < starts.Count ? starts[k + 1] : lines.Count;

            /* The trailing run of blank and definition lines. The pre-split file parks its definitions in
               sixteen such runs at section ends rather than one block at the bottom, and the index keeps
               every one of them where it already sat — which is what makes the split reversible without a
               record of where any definition came from. */
            var cut = end;
            while (cut - 1 > start && (lines[cut - 1].Length == 0 || LinkDefinition.IsMatch(lines[cut - 1])))
            {
                cut--;
            }

            var heading = lines[start];
            var match = ReleaseHeading.Match(heading);
            sections.Add(new IndexSection
            {
                Heading = heading,
                Prose = lines.GetRange(start + 1, cut - start - 1),
                Version = match.Success ? match.Groups["version"].Value : null,
            });
        }

        return sections;
    }

    /// <summary>An archive's version heading and the prose under it: the one heading to the end of the file,
    /// less the trailing run of blank and definition lines. Symmetric with the index rule above, so nothing
    /// in the archive's header paragraph can affect what is read as archived content.</summary>
    private static List<string> ArchiveBody(List<string> lines, string what)
    {
        var heads = Enumerable.Range(0, lines.Count).Where(i => ReleaseHeading.IsMatch(lines[i])).ToList();
        Assert.True(heads.Count == 1, $"{what}: an archive must carry exactly one version heading, found {heads.Count}");

        var cut = lines.Count;
        while (cut - 1 > heads[0] && (lines[cut - 1].Length == 0 || LinkDefinition.IsMatch(lines[cut - 1])))
        {
            cut--;
        }

        return lines.GetRange(heads[0], cut - heads[0]);
    }

    private static int TopLevelEntries(List<string> prose) =>
        prose.Count(l => l.StartsWith("- ", StringComparison.Ordinal));

    private static int BoldEntries(List<string> prose) =>
        prose.Count(l => l.StartsWith("- **", StringComparison.Ordinal));

    private static string ArchivePath(string version)
    {
        var parts = version.Split('.');
        return $"docs/changelog/{parts[0]}.{parts[1]}.md";
    }

    private static IEnumerable<(string Path, int Ceiling)> Files()
    {
        yield return ("CHANGELOG.md", IndexCeilingBytes);
        foreach (var section in IndexSections().Where(s => s.Archived))
        {
            yield return (ArchivePath(section.Version!), ArchiveCeilingBytes);
        }
    }

    private static (int Major, int Minor, int Patch) ParseVersion(string version)
    {
        var parts = version.Split('.').Select(p => int.Parse(p, CultureInfo.InvariantCulture)).ToList();
        return (parts[0], parts[1], parts[2]);
    }

    private static int Compare((int Major, int Minor, int Patch) a, (int Major, int Minor, int Patch) b) =>
        a.Major != b.Major ? a.Major - b.Major : a.Minor != b.Minor ? a.Minor - b.Minor : a.Patch - b.Patch;

    /// <summary>Lines without terminators, from either ending form: the working tree is CRLF under
    /// <c>eol=crlf</c> while a blob is LF, and every assertion here is about content rather than form.</summary>
    private static List<string> ReadLines(string path)
    {
        var text = File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal);
        var lines = text.Split('\n').ToList();
        if (lines.Count > 0 && lines[^1].Length == 0)
        {
            lines.RemoveAt(lines.Count - 1);
        }

        return lines;
    }

    /// <summary>Same walk-up idiom as <c>MigrationUpgradeLadderLiveTests.FindRepoRoot</c>.</summary>
    private static string RepoRoot()
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

        Assert.Fail("Could not locate the repository root.");
        return string.Empty;
    }
}
