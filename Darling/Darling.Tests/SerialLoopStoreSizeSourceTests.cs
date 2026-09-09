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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3199. The store disk-pressure check ran <c>SELECT pg_database_size(current_database())</c> every five
/// minutes on the collection loop's serial thread, under
/// <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/> = 5 s. That bound's derivation floored it on
/// <b>6.2 ms measured against a 4.05 GB store</b>. <c>pg_database_size</c> stats every file in the database
/// directory, so its cost follows the store: on a 225 GiB production store — 56x the fixture — the same call
/// measured <b>3,177 ms</b>, 512x the time, leaving 1.57x headroom where the derivation claimed ~806x. The
/// visible symptom was cancelled statements in the store's own PostgreSQL log; the larger cost was every
/// iteration that finished under 5 s, logged nothing, and still spent seconds of the whole fleet's cycle.
///
/// <para><b>What these tests defend is a CATEGORY, not that one instance.</b> The value 5 is not wrong and
/// is not changed here — what was wrong is that a regime floored on millisecond reads had acquired a member
/// whose cost grows with the store, and nothing noticed. <see cref="StartupCommandTimeoutTests"/> counts
/// DEADLINES, so it passed throughout and would pass again the moment a size-scaling read came back. So the
/// invariant asserted here is that <b>no command on the serial loop runs a read whose cost scales with the
/// store</b>, and it is asserted over the population that pin already owns rather than over a hand-copied
/// list of it.</para>
///
/// <para><b>Each test states the scope it is total over</b>, because two of them are source scans and a
/// source scan's blind spot is the whole question:
/// <list type="bullet">
/// <item><see cref="TheDiskCheckExecutesTheRecordedSizeConstant"/> — total for the one member that reads
/// its SQL from another type: it pins the reference in source AND the referent's shipped value at
/// runtime, so neither a retyped copy nor a changed constant can slip past.</item>
/// <item><see cref="NoSerialLoopMemberInlinesAStoreScalingRead"/> — total for SQL written INLINE in any of
/// the ten members. A member that referenced a size-scaling constant declared elsewhere would not match,
/// which is what the tree census below is for.</item>
/// <item><see cref="PgDatabaseSizeRunsOnlyWhereItsCostHasABudget"/> — total for
/// <c>pg_database_size</c> anywhere in shipped source, wherever the string lives, so no indirection
/// evades it. Narrower in function coverage, wider in reach; the two overlap deliberately.</item>
/// </list></para>
/// </summary>
public sealed class SerialLoopStoreSizeSourceTests
{
    /// <summary>
    /// Reads whose cost is a function of the STORE rather than of the row they return — every one of them
    /// walks files or per-relation metadata. This is the family, not just the instance that failed:
    /// <c>pg_total_relation_size</c> over the payload dimensions and <c>hypertable_detailed_size</c> across
    /// every hypertable are the two the self-metrics sweep needed a 300 s budget for (#2317), which is the
    /// direct evidence that these belong nowhere near a 5 s bound.
    /// </summary>
    private static readonly Regex s_storeScalingRead = new(
        @"\b(?:pg_database_size|pg_total_relation_size|pg_table_size|pg_relation_size|pg_indexes_size"
        + @"|hypertable_detailed_size|hypertable_local_size|hypertable_size|chunk_compression_stats"
        + @"|hypertable_approximate_size)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Where <c>pg_database_size</c> is allowed to appear in shipped source, and the reason each one is
    /// allowed to pay for a store-wide walk. Keyed by the file that owns the statement.
    ///
    /// <para>The reasons are the assertion, not decoration — a third entry appearing means someone put a
    /// store-wide walk somewhere new, and the failure message hands them these two so they have to say
    /// which regime theirs is in.</para>
    /// </summary>
    private static readonly (string Relative, int Occurrences, string Why)[] s_pgDatabaseSizeOwners =
    {
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs"), 1,
            "the hourly self-metrics sweep's whole-store row — StoreSelfMetrics.SweepTimeoutSeconds is "
            + "300s, ~94x the 3,177ms measured on a 225GiB store, and #2317 sized it against a production "
            + "store's own sizing queries rather than a fixture"),

        (Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.ServerStatus.cs"), 1,
            "the Viewer status bar's Database field — interactive, user-initiated, on the viewer's own "
            + "fan-out deadline, and nothing waits behind it but the operator who asked"),
    };

    /// <summary>
    /// Shipped source: every <c>.cs</c> under the product projects, excluding tests, the deprecated
    /// suites, the scratch tools and build output. Deliberately the WHOLE product rather than the three
    /// Darling projects — a store-wide walk introduced in a shared collector or in Lite would be the same
    /// defect, and scoping the census to where the defect happened to be found is how a census stops
    /// covering the thing it is named for.
    /// </summary>
    private static IEnumerable<string> ShippedSources()
    {
        var root = RepoRoot();

        var projects = new[]
        {
            Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Service"),
            Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Storage"),
            Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Viewer"),
            Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Analysis"),
            Path.Combine(root, "Lite"),
            Path.Combine(root, "PerformanceMonitor.Alerting"),
            Path.Combine(root, "PerformanceMonitor.Analysis"),
            Path.Combine(root, "PerformanceMonitor.Collectors"),
            Path.Combine(root, "PerformanceMonitor.Common"),
            Path.Combine(root, "PerformanceMonitor.Notifications"),
            Path.Combine(root, "PerformanceMonitor.PlanAnalysis"),
            Path.Combine(root, "PerformanceMonitor.Ui"),
        };

        foreach (var project in projects)
        {
            /* A moved or renamed project must fail loudly rather than silently shrinking the census to the
               directories that still resolve — an empty sweep is how a source scan starts reporting clean. */
            Assert.True(Directory.Exists(project), $"shipped project directory not found: {project}");

            foreach (var file in Directory.EnumerateFiles(project, "*.cs", SearchOption.AllDirectories))
            {
                var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

                if (segments.Contains("bin") || segments.Contains("obj"))
                {
                    continue;
                }

                yield return file;
            }
        }
    }

    /// <summary>
    /// Every string literal in <paramref name="source"/>, joined — so a scan sees the SQL the code actually
    /// sends and CANNOT see an explanatory comment about it.
    ///
    /// <para>The direction matters here more than in most of these pins. This file's own subject is
    /// discussed at length in comments right beside the code — the worker's doc comment names
    /// <c>pg_database_size</c> four times explaining why it no longer runs it — so a scan over raw source
    /// would report the fixed code as the defect. Stripping in the other direction is just as wrong:
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> removes the literals, which is where every
    /// query lives. Literal bodies only is the one reading that answers the question asked.</para>
    /// </summary>
    private static string LiteralsOf(string source) =>
        string.Join("\n", CSharpSourceWalker.StringLiteralBodies(source).Select(l => l.Text));

    /// <summary>
    /// The store-scaling reads one member's body sends, labelled. Extracted so
    /// <see cref="NoSerialLoopMemberInlinesAStoreScalingRead"/> and its positive control run the SAME code
    /// over the real population and over a planted body.
    ///
    /// <para>That is not tidiness. The first form inlined this loop, and deleting its one recording
    /// statement left the whole suite green: a negative census over a population that (correctly) contains
    /// nothing never executes its own loop body, so nothing observes whether that body works. Sharing the
    /// scan with a fixture that DOES contain something is what makes the absence mean anything —
    /// <c>StartupCommandTimeoutTests.TheExclusionScan_CanSeeABootstrapStamp</c> is the same construction
    /// for the same reason.</para>
    /// </summary>
    private static List<string> ScanForStoreScalingReads(string label, string body) =>
        s_storeScalingRead.Matches(LiteralsOf(body))
            .Select(hit => $"{label}: {hit.Value.TrimEnd('(')}")
            .ToList();

    /// <summary>
    /// The one member that reads its SQL from another type, pinned twice: the source says it references
    /// <see cref="StoreSelfMetrics.LatestStoreSizeSql"/>, and the shipped constant itself says what that
    /// resolves to.
    ///
    /// <para>Both halves are load-bearing. Without the source half the member could go back to an inline
    /// literal and the constant would still read clean. Without the runtime half the constant could be
    /// changed to walk the store and the reference would still read clean. This is the "run the shipped
    /// string, not a retyped copy" rule applied to a pin instead of to a probe.</para>
    /// </summary>
    [Fact]
    public void TheDiskCheckExecutesTheRecordedSizeConstant()
    {
        var body = StartupCommandTimeoutTests.SerialLoopMemberBody(
            "DarlingWorker.cs", "ReadStoreSizeBytesAsync");
        var code = CSharpSourceWalker.StripCommentsAndStrings(body);

        Assert.Contains("StoreSelfMetrics.LatestStoreSizeSql", code, StringComparison.Ordinal);

        /* And the deadline regime is unchanged — this member is still one of the ten, so the chain
           arithmetic StartupCommandTimeoutTests defends still describes the same population. Asserted here
           too because the fix would look equally "done" if the site had been moved out of the regime
           instead, and that is a different change with a different justification. */
        Assert.Contains(
            "CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds",
            code,
            StringComparison.Ordinal);

        /* The referent, at runtime. */
        var sql = StoreSelfMetrics.LatestStoreSizeSql;

        Assert.False(
            s_storeScalingRead.IsMatch(sql),
            "StoreSelfMetrics.LatestStoreSizeSql runs a read whose cost scales with the store, on the "
            + "collection loop's serial thread under a 5s bound floored on millisecond reads — the #3199 "
            + $"defect, from the other side: {sql}");

        Assert.Contains("collect.store_metrics", sql, StringComparison.Ordinal);

        /* Bounded and ordered, so "newest" is a property of the query rather than of whatever order the
           heap happened to be in. A LIMIT-less version returns every whole-store row ever recorded — 737
           over 30 days on the dogfood store, ~9,600 at the 400-day horizon — and ExecuteScalarAsync would
           silently take the first, which is the OLDEST under an ascending scan. Correct-looking, wrong
           value, no error. */
        Assert.Contains("ORDER BY metric_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The category invariant, over the ten members
    /// <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/> bounds: none of them may INLINE a read whose
    /// cost scales with the store.
    ///
    /// <para>The population is projected from <see cref="StartupCommandTimeoutTests.SerialLoopMembers"/>
    /// rather than re-listed, so this pin cannot end up asserting over nine members while that one counts
    /// ten. It also inherits that helper's loud failure when a member is renamed or gains an
    /// overload.</para>
    /// </summary>
    [Fact]
    public void NoSerialLoopMemberInlinesAStoreScalingRead()
    {
        var offenders = new List<string>();
        var members = 0;
        var sites = 0;

        foreach (var (file, member, memberSites) in StartupCommandTimeoutTests.SerialLoopMembers)
        {
            members++;
            sites += memberSites;
            offenders.AddRange(ScanForStoreScalingReads(
                $"{file} {member}",
                StartupCommandTimeoutTests.SerialLoopMemberBody(file, member)));
        }

        /* The scan having covered the WHOLE regime, before its result is believed. A projection that came
           back empty — or short by a member — would make the assertion below pass by scanning less than it
           claims, which is the failure this file exists to make impossible for the deadline census.
           Asserted as the site TOTAL rather than as a member count, and tied back to the census's own
           independently-asserted number instead of to a literal: nine members hold the ten commands, so a
           count of members is arithmetic nobody can check by eye. */
        Assert.Equal(StartupCommandTimeoutTests.ExpectedSerialLoopSites, sites);
        Assert.True(members > 0, "the serial-loop member projection came back empty");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command(s) awaited inline on the collection loop's serial thread run a read "
            + "whose cost scales with the store, under ServiceCommandDeadlines.SerialLoopSeconds — a bound "
            + "floored on 16.1ms for five config reads together. #3199 is what that costs: pg_database_size "
            + "measured 6.2ms on the 4.05GB fixture the bound was sized against and 3,177ms on a 225GiB "
            + "production store, so the whole fleet's cycle waited seconds behind it every five minutes. "
            + "Such a read belongs on a cadence with its own budget: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// <c>pg_database_size</c> tree-wide, wherever the string lives, against the two owners that have a
    /// budget for it. This is the half that no indirection evades — a member referencing a constant in
    /// another file is invisible to
    /// <see cref="NoSerialLoopMemberInlinesAStoreScalingRead"/> and counted here.
    ///
    /// <para>The count is a REVIEW GATE rather than a fact worth knowing: a third occurrence fails the
    /// build and the message hands the author the two existing regimes so they have to say which one
    /// theirs is. It fails on a removal too, which is the right direction to be noisy in — losing the
    /// series' own size row would silently blank the disk-pressure alert's only size number.</para>
    /// </summary>
    [Fact]
    public void PgDatabaseSizeRunsOnlyWhereItsCostHasABudget()
    {
        var byFile = new Dictionary<string, int>(StringComparer.Ordinal);
        var scanned = 0;
        var root = RepoRoot();

        foreach (var path in ShippedSources())
        {
            scanned++;
            var hits = Regex.Matches(
                LiteralsOf(File.ReadAllText(path)),
                @"\bpg_database_size\s*\(",
                RegexOptions.CultureInvariant).Count;

            if (hits > 0)
            {
                byFile[Path.GetRelativePath(root, path)] = hits;
            }
        }

        /* The census having covered the product, before its emptiness means anything. */
        Assert.True(
            scanned > 500,
            $"the shipped-source census only reached {scanned} files, so an empty result says nothing "
            + "about where pg_database_size runs");

        var expected = s_pgDatabaseSizeOwners.ToDictionary(
            o => o.Relative, o => o.Occurrences, StringComparer.Ordinal);

        var unexpected = byFile.Keys.Where(f => !expected.ContainsKey(f)).ToList();

        /* Unexpected owners BEFORE the totals, so a genuinely new store-wide walk reports as itself rather
           than as an off-by-one on a count that says nothing about where it is. */
        Assert.True(
            unexpected.Count == 0,
            $"{unexpected.Count} file(s) run pg_database_size, which walks every file in the store "
            + "(measured 3,177ms on a 225GiB store, and it tracks file count rather than bytes). The two "
            + "places that may pay for it, and why, are: "
            + string.Join("; ", s_pgDatabaseSizeOwners.Select(o => $"{o.Relative} — {o.Why}"))
            + ". Say which regime yours is in, or read the recorded value from "
            + "StoreSelfMetrics.LatestStoreSizeSql instead: " + string.Join(", ", unexpected));

        foreach (var (relative, occurrences, why) in s_pgDatabaseSizeOwners)
        {
            Assert.True(
                byFile.TryGetValue(relative, out var actual) && actual == occurrences,
                $"{relative} was expected to run pg_database_size {occurrences}x ({why}) but runs it "
                + $"{(byFile.TryGetValue(relative, out var got) ? got : 0)}x");
        }
    }

    /// <summary>
    /// The reader and the writer cannot disagree about which row is the whole-store row.
    ///
    /// <para>This is the honest-empty trap on a value the shipped store already holds 400 days of: a
    /// reader filtering on a kind the writer stopped writing gets ZERO ROWS, not an error, and
    /// <c>ExecuteScalarAsync</c> renders that as the same null a store that has never swept produces. The
    /// alert text would quietly lose its size number and nothing anywhere would say why. So the kind is
    /// one const with six consumers, and its VALUE is part of the on-disk contract — renaming it orphans
    /// every recorded row.</para>
    /// </summary>
    [Fact]
    public void TheRecordedSizeReaderAndWriterCannotDisagreeAboutTheKind()
    {
        Assert.Equal("store", StoreSelfMetrics.StoreObjectKind);

        var quoted = $"'{StoreSelfMetrics.StoreObjectKind}'";

        Assert.Contains(quoted, StoreSelfMetrics.StoreInsertSql, StringComparison.Ordinal);
        Assert.Contains(quoted, StoreSelfMetrics.LatestStoreSizeSql, StringComparison.Ordinal);

        /* And the C# consumers partition their response on the same const rather than on a retyped copy
           of its value.

           The first draft of this asserted DoesNotContain("ObjectKind == \"") over STRIPPED source, and a
           mutation restoring a bare literal came back green: stripping blanks the literal INCLUDING its
           opening quote, so that pattern cannot appear in stripped source at all and the assertion could
           not fail. Asserting a pattern that the transform it reads has already removed is a pin that
           passes on every input — which is why the mutation table below has a row for the instrument and
           not only for the subject.

           What replaces it asserts the invariant directly: EVERY ObjectKind comparison in the file has the
           const on its right-hand side. Over stripped source a comparison against a literal reads as
           "==" followed by blanks, so the next code token is not the const and the site is reported. */
        var toolsPath = Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp",
            "DarlingMcpStoreMetricsTools.cs");
        Assert.True(File.Exists(toolsPath), $"store-metrics MCP tool source not found: {toolsPath}");

        var toolsRaw = File.ReadAllText(toolsPath);
        var (compared, literal) = ObjectKindComparisons(toolsRaw);

        Assert.True(
            compared > 0,
            "no ObjectKind comparison found in DarlingMcpStoreMetricsTools.cs — the scan this assertion "
            + "rests on matched nothing, so its result says nothing");

        Assert.True(
            literal.Count == 0,
            $"{literal.Count} ObjectKind comparison(s) in DarlingMcpStoreMetricsTools.cs compare against "
            + "something other than StoreSelfMetrics.StoreObjectKind. The kind is one const with six "
            + "consumers because a reader filtering on a kind the writer stopped writing returns zero "
            + $"rows rather than erroring: {string.Join(", ", literal)}");
    }

    /// <summary>
    /// Every <c>.ObjectKind ==</c> / <c>!=</c> comparison in <paramref name="source"/>: how many there are,
    /// and the ones whose right-hand side is not
    /// <see cref="StoreSelfMetrics.StoreObjectKind"/>. Read over STRIPPED source, where a literal
    /// right-hand side survives as blanks — so the next code token after the operator is not the const,
    /// and the site is reported.
    /// </summary>
    private static (int Compared, List<string> NotTheConst) ObjectKindComparisons(string source)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var offenders = new List<string>();
        var compared = 0;

        foreach (Match m in Regex.Matches(
            code, @"\.ObjectKind\s*[!=]=\s*", RegexOptions.CultureInvariant))
        {
            compared++;
            var rest = code[(m.Index + m.Length)..];

            if (!rest.TrimStart().StartsWith("StoreSelfMetrics.StoreObjectKind", StringComparison.Ordinal))
            {
                var line = code.Take(m.Index).Count(c => c == '\n') + 1;
                offenders.Add($"line {line}");
            }
        }

        return (compared, offenders);
    }

    /// <summary>
    /// <see cref="NoSerialLoopMemberInlinesAStoreScalingRead"/>'s own control: the same
    /// <see cref="ScanForStoreScalingReads"/> call, over a body written the way the defect was actually
    /// written — the shape <c>ReadStoreSizeBytesAsync</c> had before this change, comment and all.
    ///
    /// <para>Without this the census is an assertion that happens to hold. Its population correctly
    /// contains no store-scaling read, so its loop body never runs on the real tree, and a mutation
    /// deleting the recording statement left the whole suite green — measured, not assumed. The label is
    /// asserted too, because an offender list that reports the finding without saying WHERE sends the next
    /// person to the wrong file.</para>
    /// </summary>
    [Fact]
    public void TheMemberScan_ReportsAPlantedStoreScalingRead()
    {
        const string planted = """
            private async Task<long?> ReadStoreSizeBytesAsync(CancellationToken cancellationToken)
            {
                /* Context for the alert text; pg_database_size is cheap. */
                using var command = new NpgsqlCommand("SELECT pg_database_size(current_database())", connection)
                    { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
                return (long?)await command.ExecuteScalarAsync(cancellationToken);
            }
            """;

        var offenders = ScanForStoreScalingReads("DarlingWorker.cs ReadStoreSizeBytesAsync", planted);

        Assert.Single(offenders);
        Assert.Contains("ReadStoreSizeBytesAsync", offenders[0], StringComparison.Ordinal);
        Assert.Contains("pg_database_size", offenders[0], StringComparison.Ordinal);

        /* And the clean shape reports nothing, so the control is not simply "always reports". */
        Assert.Empty(ScanForStoreScalingReads(
            "clean",
            """
            using var command = new NpgsqlCommand(StoreSelfMetrics.LatestStoreSizeSql, connection);
            """));
    }

    /// <summary>
    /// The positive controls, run through the IDENTICAL scan the three tests above use. Without these,
    /// every one of them could match nothing and report clean — which is precisely how the deadline census
    /// stayed green across the whole life of this defect.
    ///
    /// <para>The two fixtures are the two directions this scan can be wrong in, and both have occurred in
    /// this repo: SQL in a literal must be SEEN (a scan over stripped source misses every query), and the
    /// same function named in a comment must NOT be (a scan over raw source reports the fix as the
    /// defect — and this change's own doc comments name <c>pg_database_size</c> six times).</para>
    /// </summary>
    [Theory]
    [InlineData("var sql = \"SELECT pg_database_size(current_database())\";\n", true)]
    [InlineData("var sql = @\"SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c\";\n", true)]
    [InlineData("var sql = $@\"SELECT hypertable_detailed_size('{name}'::regclass)\";\n", true)]
    [InlineData("/* This deliberately does NOT call pg_database_size(current_database()). */\n", false)]
    [InlineData("/// <summary>Why pg_total_relation_size(oid) is not used here.</summary>\n", false)]
    [InlineData("var sql = \"SELECT total_bytes FROM collect.store_metrics ORDER BY metric_time DESC\";\n", false)]
    public void TheScanner_ReadsQueriesAndNotProseAboutThem(string source, bool expectedHit)
    {
        Assert.Equal(expectedHit, s_storeScalingRead.IsMatch(LiteralsOf(source)));
    }

    /// <summary>
    /// The positive control for <see cref="ObjectKindComparisons"/>, and it exists because its predecessor
    /// had none and was therefore unfalsifiable — a mutation restoring a bare <c>"store"</c> literal to one
    /// of the four call sites came back GREEN, because the pattern being asserted absent had already been
    /// removed by the transform the assertion read.
    ///
    /// <para>Both counts are asserted, not just the offender count. A scan that found no comparisons at all
    /// would report zero offenders and read exactly like a clean file, which is the same failure one layer
    /// down. The third and fourth fixtures are the mutation that got through; the fifth is the other
    /// direction, where the retired form written in a COMMENT must not be reported.</para>
    /// </summary>
    [Theory]
    [InlineData("if (r.ObjectKind == StoreSelfMetrics.StoreObjectKind) { }\n", 1, 0)]
    [InlineData("if (p.ObjectKind != StoreSelfMetrics.StoreObjectKind) { }\n", 1, 0)]
    [InlineData("if (r.ObjectKind == \"store\") { }\n", 1, 1)]
    [InlineData("if (r.ObjectKind != \"store\") { }\n", 1, 1)]
    [InlineData("/* r.ObjectKind == \"store\" is what this used to do. */\n", 0, 0)]
    [InlineData("var storeLatest = latest.FirstOrDefault();\n", 0, 0)]
    public void TheObjectKindScanner_SeesALiteralRightHandSide(
        string source, int expectedCompared, int expectedOffenders)
    {
        var (compared, offenders) = ObjectKindComparisons(source);

        Assert.Equal(expectedCompared, compared);
        Assert.Equal(expectedOffenders, offenders.Count);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(
            dir is null,
            "could not locate the repository root from " + thisFile);

        return dir!;
    }
}
