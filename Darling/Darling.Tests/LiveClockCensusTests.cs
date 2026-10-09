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

namespace Darling.Tests;

/// <summary>
/// A live Postgres test that counts blocks from a plan (<c>Shared Hit Blocks</c> / <c>Shared Read Blocks</c>) seeds its
/// window through <see cref="LiveClock"/> or is on the allow list with its reason (#5608). Block counts depend on where
/// a Timescale chunk boundary falls in the seeded rows, and rows seeded "N hours before the clock" move that boundary
/// with the hour the test runs: the newest-snapshot test measured perfmon old=264 new=120 at 00:50 UTC against
/// new=57 at 01:13, and failed the pull request that happened to run then. <see cref="LiveClock.AnchoredEnd"/> keeps
/// a window inside one chunk; <see cref="LiveClock.Now"/> carries the test-only clock override, so a run at midday can
/// be proved as if it were 00:30.
///
/// <para>The rule is per file, as <see cref="TimingTestCensusTests"/> does it. "Reads a wall clock" is
/// <c>DateTime.UtcNow</c>, <c>DateTime.Now</c>, <c>DateTimeOffset.UtcNow</c> or <c>DateTimeOffset.Now</c> in code
/// (comments and string literals do not count); "counts blocks" is one of the two plan property names above. A file
/// that does both and does not call <c>LiveClock.</c> fails unless it is named in the allow list with a reason, and an
/// allow entry that no longer matches fails, so the list only shrinks.</para>
///
/// <para>Not covered: tests that read plan SHAPE or chunk catalogs (about 70 files use <c>EXPLAIN</c> or the chunk
/// catalog and a wall clock). Their assertions are not block counts, so a boundary does not change the number they
/// compare, but each is a candidate for the same anchor; the PR body lists them.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class LiveClockCensusTests
{
    private static readonly Dictionary<string, string> s_allowed = new(StringComparer.Ordinal)
    {
        // Empty on purpose: every file that counts blocks anchors through LiveClock. Add an entry only with a reason that
        // says why the count cannot depend on where a chunk boundary falls.
    };

    private static readonly Regex s_clock = new(@"\bDateTime(?:Offset)?\s*\.\s*(?:UtcNow|Now)\b", RegexOptions.Compiled);

    private static readonly Regex s_blocks = new(@"Shared (?:Hit|Read) Blocks", RegexOptions.Compiled);

    private static readonly Regex s_live = new(@"DARLING_TEST_PG|ScratchPostgres", RegexOptions.Compiled);

    private static readonly Regex s_anchor = new(@"\bLiveClock\s*\.", RegexOptions.Compiled);

    /// <summary>Anti-vacuity: at least this many files count blocks, so a scan that stopped matching fails.</summary>
    private const int BlockCountingFloor = 2;

    internal sealed record Row(string File, bool Live, bool ReadsClock, bool CountsBlocks, bool UsesAnchor);

    internal static Row ScanText(string file, string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        return new Row(file, s_live.IsMatch(text), s_clock.IsMatch(code), s_blocks.IsMatch(text), s_anchor.IsMatch(code));
    }

    internal static List<string> Problems(IEnumerable<Row> rows, IReadOnlyDictionary<string, string> allowed) =>
        rows.Where(r => r.Live && r.CountsBlocks && r.ReadsClock && !r.UsesAnchor && !allowed.ContainsKey(r.File))
            .Select(r => r.File).ToList();

    internal static List<string> Stale(IEnumerable<Row> rows, IReadOnlyDictionary<string, string> allowed)
    {
        var byFile = rows.ToDictionary(r => r.File, StringComparer.Ordinal);
        return allowed.Keys
            .Where(f => !byFile.TryGetValue(f, out var r) || !(r.Live && r.CountsBlocks && r.ReadsClock && !r.UsesAnchor))
            .ToList();
    }

    private static List<Row> Scan() =>
        Directory.EnumerateFiles(TestsRoot(), "*.cs", SearchOption.TopDirectoryOnly)
            .Select(p => ScanText(Path.GetFileName(p), File.ReadAllText(p)))
            .ToList();

    [Fact]
    public void TheDetector_FlagsAClockedBlockCounter_AndIgnoresAnchoredProseAndStringFiles()
    {
        var bare = ScanText("Bare.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = DateTime.UtcNow; var s = \"Shared Hit Blocks\"; } }");
        var anchored = ScanText("Anchored.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = LiveClock.AnchoredEnd(); var s = \"Shared Hit Blocks\"; var n = DateTime.UtcNow; } }");
        var prose = ScanText("Prose.cs", "// DARLING_TEST_PG DateTime.UtcNow\nclass A { void B() { var s = \"DateTime.UtcNow Shared Hit Blocks\"; } }");
        var noBlocks = ScanText("NoBlocks.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = DateTime.UtcNow; } }");

        Assert.Equal(new[] { "Bare.cs" }, Problems(new[] { bare, anchored, prose, noBlocks }, new Dictionary<string, string>()));
        Assert.Empty(Problems(new[] { bare }, new Dictionary<string, string> { ["Bare.cs"] = "reason" }));
        Assert.Equal(new[] { "Gone.cs" }, Stale(new[] { bare }, new Dictionary<string, string> { ["Bare.cs"] = "reason", ["Gone.cs"] = "reason" }));
    }

    [Fact]
    public void EveryLiveTestThatCountsBlocks_SeedsThroughTheLiveClock_OrIsAllowedWithAReason()
    {
        var rows = Scan();

        var problems = Problems(rows, s_allowed);
        Assert.True(
            problems.Count == 0,
            "A live Postgres test counts plan blocks and reads the wall clock without LiveClock (#5608). The count depends on where a "
          + "chunk boundary falls in the seeded rows, so it changes with the hour the test runs. Seed the end of the window with "
          + "LiveClock.AnchoredEnd() (or LiveClock.Now() for a window that may straddle a boundary the same way every run), or name the "
          + "file in LiveClockCensusTests.s_allowed with the reason the count cannot depend on a boundary:\n  " + string.Join("\n  ", problems));

        var stale = Stale(rows, s_allowed);
        Assert.True(
            stale.Count == 0,
            "These allow-list entries no longer name a live block-counting file that reads the wall clock without LiveClock; delete them:\n  "
          + string.Join("\n  ", stale));

        Assert.True(
            rows.Count(r => r.Live && r.CountsBlocks) >= BlockCountingFloor,
            "the scan found fewer live block-counting files than expected, so it stopped matching and 'no problems' would be vacuous");
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") => Path.GetDirectoryName(thisFile)!;
}
