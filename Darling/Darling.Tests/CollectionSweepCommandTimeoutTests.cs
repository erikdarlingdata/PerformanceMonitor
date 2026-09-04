/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every store read and write the COLLECTION SWEEP performs must carry an explicit command deadline
/// (#2874) — including the binary COPY that is the collector's actual store write, which no pin in this
/// sweep could previously see.
///
/// <para><b>The fourth construction shape.</b> The three shapes the landed pins match are
/// <c>new NpgsqlCommand(</c>, <c>.CreateCommand(</c> and a bare <c>.CreateCommand</c> method group. A
/// binary COPY is none of them: <c>NpgsqlConnection.BeginBinaryImportAsync</c> returns an
/// <see cref="Npgsql.NpgsqlBinaryImporter"/> whose deadline is its own <c>Timeout</c> property — a
/// <c>TimeSpan</c>, not the <c>int</c> seconds of <c>NpgsqlCommand.CommandTimeout</c> — initialised from
/// the connection's <c>CommandTimeout</c> and therefore inheriting the same undocumented 30 s. There is
/// no <c>NpgsqlCommand</c> and no <c>CreateCommand</c> anywhere in the expression, so every regex in
/// #2810, #2871, #2882, #2888, #2901 and #2905 reported this family clean while all four sites sat on
/// the default. <c>.Service</c> is the only project that has any (Storage 0, Viewer 0, Analysis 0), and
/// one of them — <c>DarlingCollectorRunner.WriteBatchAsync</c> — is the store write of every collector,
/// on every server, on every cycle. <see cref="EveryCopyWriter_SetsTheImporterDeadline"/> is the shape
/// guard, and it is deliberately PROJECT-WIDE rather than scoped to the four known files.</para>
///
/// <para><b>Why the command sweep below is scoped by MEMBER, not by file.</b> Every earlier pin in this
/// sweep could glob a directory, because <c>.Storage</c> and <c>.Viewer</c> are Npgsql-store-only
/// projects whose sites all share one budget. <c>.Service</c> is neither. The collection sweep's sites
/// sit in files that ALSO hold other regimes' sites — <c>DarlingObservability</c> carries the per-cycle
/// <c>collection_log</c> insert next to the daily purge's retention row and the analysis pass's state
/// write; <c>DarlingWorker</c> carries the per-collector statement-text refresh next to two command-plane
/// handlers and an alert-pass read. A file-scoped guard here would either fail on sites this group
/// deliberately left alone or, worse, be relaxed until it guarded nothing. Members are the smallest unit
/// that expresses "runs inside the sweep body".</para>
///
/// <para>The VALUE is pinned as a band below, and derived in
/// <see cref="ServiceCommandDeadlines.CollectionSweepSeconds"/> from this regime's own callers: no
/// enclosing budget, a 15 s launch tick, one of <c>max_concurrent_sweeps</c> permits plus one borrowed
/// store connection held for the body's duration, and a 60 s watchdog that only logs. It is derived at
/// the width the knob is MOVING to (8), not the seeded 4.</para>
/// </summary>
public sealed class CollectionSweepCommandTimeoutTests
{
    /// <summary>
    /// The sweep's store-touching members, as (file, member) pairs. Named rather than globbed for the
    /// reason in the class comment, and each one is reachable from <c>ProcessServerSweepAsync</c> on the
    /// sweep's own cadence.
    ///
    /// <para><b>The site that looked like another regime and was not.</b>
    /// <c>DarlingObservability.WriteAnalysisStateAsync</c> was the seventeenth site and it is IN scope,
    /// after review caught it: it looks like the analysis pass's state write and is not one. The pass's
    /// budget is <c>passCts.CancelAfter(s_analysisTimeout)</c>, whose token is threaded ONLY into
    /// <c>analysisService.AnalyzeAsync</c> — every <c>WriteAnalysisStateAsync</c> call takes the plain
    /// <c>stoppingToken</c> instead, and one of them (<c>DarlingWorker.cs:1877</c>, the PostgreSQL-target
    /// tombstone) is called straight from <c>ProcessServerSweepAsync</c> with no pass around it at all.
    /// Being lexically inside a budgeted method is not the same as being under its budget, and that is
    /// the distinction this group's member scoping exists to make rather than to blur.</para>
    ///
    /// <para><b>What is deliberately absent, and why each is a different budget.</b>
    /// <c>DarlingWorker.ReadLatestCpuAsync</c> is reached from <c>EvaluateAlertsAsync</c> and belongs to
    /// #2882's alert pass (10 s, bounded by a 30 s sweep interval) — it is a site that pin's file-scoped
    /// list missed, not a member of this regime. <c>ReadStoreSizeBytesAsync</c> runs on the disk-check
    /// cadence. <c>RunTestHypotheticalIndexAsync</c> / <c>RunExecuteActualPlanAsync</c> and
    /// <c>DarlingCommandExecutor</c> are the command plane, with a 5-minute claim lease and no heartbeat.
    /// <c>DarlingDeltaCalculator</c>'s four seeds and <c>StoreConfigProvider</c>'s seven seeding sites run
    /// ONCE at startup. <c>StoreConfigProvider.ReadConfigVersionAsync</c> is the 15 s reload beacon and is
    /// the closest call of all — it runs on the sweep's own tick — but it runs on the SERIAL loop thread
    /// ahead of every launch, so its blast radius is the whole fleet and its floor is a single-row lookup
    /// rather than a batch write. Different bounds on both sides; left with the startup group.</para>
    /// </summary>
    private static readonly (string File, string Member)[] s_sweepMembers =
    {
        ("DarlingCollectorRunner.cs", "GetCollectorStateAsync"),
        ("DarlingCollectorRunner.cs", "SaveCollectorStateAsync"),
        ("DarlingCollectorRunner.cs", "DeleteCollectorStateKeyAsync"),
        ("DarlingCollectorRunner.cs", "PruneOrphanedQueryStoreDatabaseStateAsync"),
        ("DarlingCollectorRunner.cs", "PruneForeignQueryStoreDatabaseStateAsync"),
        ("DarlingCollectorRunner.cs", "HasPriorCollectorSuccessAsync"),
        ("DarlingWorker.cs", "TryRefreshPgStatementTextAsync"),
        ("DarlingWorker.cs", "ReadCollectorWatermarksAsync"),
        ("DarlingObservability.cs", "UpsertServerAsync"),
        ("DarlingObservability.cs", "LogCollectionAsync"),
        ("DarlingObservability.cs", "WriteAnalysisStateAsync"),
        ("Targets/RdsCpuIngestor.cs", "GetWatermarkAsync"),
    };

    /// <summary>The sweep's command sites, counted so a member that stops creating commands fails loudly.</summary>
    private const int ExpectedSweepCommandSites = 13;

    /// <summary>The four COPY writers, counted for the same reason.</summary>
    private const int ExpectedCopyWriterSites = 4;

    /// <summary>
    /// All three command shapes the landed pins know about. The bare method group is included even though
    /// #2895's census found ZERO of it in this project, because "absent today" is not a guard.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\s*\(|\.CreateCommand\s*\(|\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The fourth shape. No <c>NpgsqlCommand</c>, no <c>CreateCommand</c>, so nothing above sees it.</summary>
    private static readonly Regex s_copyWriter = new(
        @"BeginBinaryImportAsync\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// <c>NpgsqlBinaryImporter.Timeout</c>, matched on the property rather than on a variable name so a
    /// site that calls its importer something other than <c>importer</c> still counts.
    /// </summary>
    private static readonly Regex s_setsImporterTimeout = new(
        @"\.Timeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EverySweepBodyCommand_SetsTheCollectionSweepDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var (file, member) in s_sweepMembers)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var body = MemberBody(text, member, path);

            foreach (Match ctor in s_commandCtor.Matches(CSharpSourceWalker.StripCommentsAndStrings(body)))
            {
                total++;

                /* Two statements, for the reason #2810's pin records: the CreateCommand shape's method
                   result cannot take an object initializer, so its deadline is the statement AFTER the
                   construction. The span is walked literal- and comment-aware, which is load-bearing
                   rather than defensive here — these members embed verbatim SQL carrying both semicolons
                   and quote characters. */
                var span = CSharpSourceWalker.StatementSpanFrom(body, ctor.Index, statements: 2);

                if (!s_setsTimeout.IsMatch(span))
                {
                    offenders.Add($"{file} {member} +{LineOf(body, ctor.Index)}");
                }
            }
        }

        /* Offenders BEFORE the census, so a real defect reports as a defect. Reversed, an added-and-untimed
           site would fail on the count and say nothing about the deadline it is missing. */
        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} collection-sweep command(s) inherit Npgsql's 30s default instead of setting "
            + $"{nameof(ServiceCommandDeadlines)}.{nameof(ServiceCommandDeadlines.CollectionSweepSeconds)}: "
            + string.Join(", ", offenders));

        /* The census is a tripwire in BOTH directions. Downward it catches a member that stopped creating
           commands, or an extractor that silently returned an empty body — the way a source-walking guard
           starts reporting clean on code it no longer reads. Upward it forces a human to decide whether a
           newly added site really belongs to this budget, rather than inheriting the answer by adjacency. */
        Assert.Equal(ExpectedSweepCommandSites, total);
    }

    /// <summary>
    /// The fourth shape, swept over the WHOLE project rather than the four files that have it today.
    ///
    /// <para>Scoped that way deliberately: the defect this pin exists for is that a COPY writer is
    /// invisible to a command-shaped regex, so the failure mode to guard is a FIFTH one being added
    /// somewhere else and inheriting the default in silence. A list of four filenames would not catch
    /// that, which is the #2786 shape — a guard that names the arm it was written for.</para>
    /// </summary>
    [Fact]
    public void EveryCopyWriter_SetsTheImporterDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in ServiceSources())
        {
            var text = File.ReadAllText(path);

            foreach (Match copy in s_copyWriter.Matches(CSharpSourceWalker.StripCommentsAndStrings(text)))
            {
                total++;

                var span = CSharpSourceWalker.StatementSpanFrom(text, copy.Index, statements: 2);

                if (!s_setsImporterTimeout.IsMatch(span))
                {
                    offenders.Add($"{Path.GetFileName(path)}:{LineOf(text, copy.Index)}");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} binary COPY writer(s) leave NpgsqlBinaryImporter.Timeout unset, so the COPY "
            + "inherits the connection's 30s CommandTimeout — a deadline nobody chose, surfacing as "
            + $"\"Exception while reading from stream\": {string.Join(", ", offenders)}");

        /* A FIFTH COPY writer fails here even when it is correctly timed, and that is the point: this shape
           was invisible to six landed pins, so a new one must be looked at by a person deciding whether it
           shares this budget — not waved through because it happened to set the property. */
        Assert.Equal(ExpectedCopyWriterSites, total);
    }

    /// <summary>
    /// The COPY writers take the SAME constant as the commands beside them, because they are the same
    /// regime — and pinned RELATIONALLY rather than by value so the two can never drift apart. A bare
    /// <c>TimeSpan.FromSeconds(10)</c> at a COPY site would satisfy the shape guard above while quietly
    /// decoupling the busiest write in the process from the number this group derived.
    /// </summary>
    [Fact]
    public void TheCopyWriters_TakeTheSweepConstantRatherThanALiteral()
    {
        var relational = new Regex(
            @"\.Timeout\s*=\s*TimeSpan\.FromSeconds\(\s*ServiceCommandDeadlines\.CollectionSweepSeconds\s*\)",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        var bound = ServiceSources()
            .Select(p => CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(p)))
            .Sum(t => relational.Matches(t).Count);

        Assert.Equal(ExpectedCopyWriterSites, bound);
    }

    /// <summary>
    /// The value, bounded on both sides — see
    /// <see cref="ServiceCommandDeadlines.CollectionSweepSeconds"/> for the derivation. A band rather
    /// than an equality, following the precedent .Storage and .Viewer set: freezing the exact number
    /// makes every future re-derivation a test edit, and the claim being defended is that it sits between
    /// two measured bounds.
    /// </summary>
    [Fact]
    public void TheCollectionSweepDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.CollectionSweepSeconds;

        Assert.True(
            seconds >= 5,
            $"collection-sweep deadline {seconds}s leaves no headroom over the measured worst case — a "
            + "1.53s store write on the busiest server, projected to ~3.1s at max_concurrent_sweeps=8, "
            + "plus 673-893ms of store connection acquisition (#2819)");

        Assert.True(
            seconds < 30,
            $"collection-sweep deadline {seconds}s is at or above Npgsql's inherited 30s default, so it "
            + "buys nothing: nothing encloses the sweep body, and the 60s watchdog only logs");

        Assert.True(
            seconds < DarlingWorker.SweepWatchdogSeconds / 4,
            $"collection-sweep deadline {seconds}s is too close to the {DarlingWorker.SweepWatchdogSeconds}s "
            + "watchdog: real cycles run 13-48s, so one blown deadline on a heavy cycle would cross the "
            + "threshold and report a merely-slow body as a hang — the #1581/#2170 warning herd");
    }

    /// <summary>
    /// The scanners are what sixteen sites' correctness is asserted through, so their own edges are
    /// pinned: a false positive fails a green build on correct code, and a false NEGATIVE reports success
    /// on the defect.
    ///
    /// <para>Every fixture is a shape that occurs. The <c>using (...)</c> block form is how all four COPY
    /// writers are actually written, and its brace arithmetic is the non-obvious case — the span walker
    /// enters at the importer call already inside the <c>using</c>'s parenthesis, so the depth counter
    /// goes NEGATIVE closing it before the block's brace brings it back. The last fixture is the one that
    /// matters most: an untimed COPY whose method is followed by a timed one must NOT borrow its
    /// neighbour's deadline.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "using (var importer = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    importer.Timeout = TimeSpan.FromSeconds(10);\n"
        + "    writer.Importer = importer;\n"
        + "}\n",
        true)]
    [InlineData(
        "await using var importer = await conn.BeginBinaryImportAsync(Copy, ct);\n"
        + "importer.Timeout = TimeSpan.FromSeconds(10);\n",
        true)]
    [InlineData(
        "using (var importer = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    /* no deadline; a comment mentioning Timeout = is not one. */\n"
        + "    writer.Importer = importer;\n"
        + "}\n",
        false)]
    [InlineData(
        "using (var importer = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    writer.Importer = importer;\n"
        + "}\n"
        + "await Other();\n"
        + "using (var second = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    second.Timeout = TimeSpan.FromSeconds(10);\n"
        + "}\n",
        false)]
    public void TheCopyScanner_SeesTheImporterDeadlineWithoutBorrowingANeighbours(string source, bool expectedTimed)
    {
        var copy = s_copyWriter.Match(CSharpSourceWalker.StripCommentsAndStrings(source));
        Assert.True(copy.Success, "the fixture did not contain a COPY writer");

        var span = CSharpSourceWalker.StatementSpanFrom(source, copy.Index, statements: 2);

        Assert.Equal(expectedTimed, s_setsImporterTimeout.IsMatch(span));
    }

    /// <summary>
    /// The member extractor, pinned separately. It is the piece with no precedent in this sweep — every
    /// earlier pin globbed a directory — and a body that resolves to the WRONG span, or to an empty one,
    /// would report clean on whatever it failed to read.
    /// </summary>
    [Fact]
    public void TheMemberExtractor_ReturnsOnlyTheNamedMembersBody()
    {
        const string Source =
            "class C\n"
            + "{\n"
            + "    void Before() { var a = new NpgsqlCommand(X); }\n"
            + "    async Task TargetAsync(int id)\n"
            + "    {\n"
            + "        var command = new NpgsqlCommand(Y);\n"
            + "        if (id > 0) { command.CommandTimeout = 10; }\n"
            + "    }\n"
            + "    void After() { var b = new NpgsqlCommand(Z); }\n"
            + "}\n";

        var body = MemberBody(Source, "TargetAsync", "fixture");

        Assert.Contains("new NpgsqlCommand(Y)", body);
        Assert.DoesNotContain("new NpgsqlCommand(X)", body);
        Assert.DoesNotContain("new NpgsqlCommand(Z)", body);
        Assert.Single(s_commandCtor.Matches(body));
    }

    /// <summary>
    /// The text of one member's body, from its signature to the matching close brace.
    ///
    /// <para>Brace-matched over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output rather
    /// than the raw text, because a brace inside verbatim SQL or a comment would otherwise close the body
    /// early — and these members are full of both. The offsets are then applied to the ORIGINAL text, so
    /// the value-bearing regexes above see what is actually written. That is the same
    /// stripped-walk/raw-span split the shared walker uses for statement spans.</para>
    /// </summary>
    private static string MemberBody(string text, string member, string path)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(text);

        /* The member NAME followed by an optional generic argument list and then its parameter list.
           The generic hole is not decoration: WriteBatchAsync<TRow>( is real in this project, and a
           pattern of `name\s*\(` would miss it — the #2874 trap where a grep encoded the wrong shape
           and read zero hits as clean. */
        var signature = new Regex(
            @"\b" + Regex.Escape(member) + @"\s*(?:<[^<>()]*>)?\s*\(",
            RegexOptions.CultureInvariant);

        var declarations = signature.Matches(stripped)
            .Where(m => BodyStart(stripped, m.Index) >= 0)
            .ToArray();

        Assert.True(
            declarations.Length == 1,
            $"expected exactly one declaration of {member} in {path}, found {declarations.Length} — "
            + "an overload or a rename has moved this member out from under the guard");

        var open = BodyStart(stripped, declarations[0].Index);
        var depth = 0;

        for (var i = open; i < stripped.Length; i++)
        {
            if (stripped[i] == '{')
            {
                depth++;
            }
            else if (stripped[i] == '}' && --depth == 0)
            {
                return text[declarations[0].Index..(i + 1)];
            }
        }

        Assert.Fail($"{member} in {path} has an unbalanced body");
        return string.Empty;
    }

    /// <summary>
    /// Index of the <c>{</c> that opens the body of the declaration starting at <paramref name="at"/>,
    /// or -1 when what follows the parameter list is not a block — which is how a CALL to the member, or
    /// an expression-bodied delegation, is told apart from its declaration. That distinction is load
    /// bearing: <c>DarlingCollectorRunner</c> has two expression-bodied <c>CreateCommand</c> members that
    /// #2874's census counted as offenders and which are not store commands at all.
    /// </summary>
    private static int BodyStart(string stripped, int at)
    {
        var depth = 0;

        for (var i = stripped.IndexOf('(', at); i >= 0 && i < stripped.Length; i++)
        {
            if (stripped[i] == '(')
            {
                depth++;
            }
            else if (stripped[i] == ')' && --depth == 0)
            {
                for (var j = i + 1; j < stripped.Length; j++)
                {
                    if (char.IsWhiteSpace(stripped[j]))
                    {
                        continue;
                    }

                    return stripped[j] == '{' ? j : -1;
                }

                return -1;
            }
        }

        return -1;
    }

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string SourcePath(string relative)
    {
        var path = Path.Combine(
            RepoRoot(),
            "Darling",
            "PerformanceMonitor.Darling.Service",
            relative.Replace('/', Path.DirectorySeparatorChar));

        /* A moved or renamed sweep source must fail loudly rather than silently shrinking the scan to
           the files that still resolve — an empty sweep is how a guard starts reporting clean. */
        Assert.True(File.Exists(path), $"collection-sweep source not found: {path}");

        return path;
    }

    private static IEnumerable<string> ServiceSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        Assert.True(Directory.Exists(dir), $"service project directory not found: {dir}");

        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(dir, p))
            .OrderBy(p => p, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 150, $"the service sweep found only {paths.Length} files — the project has moved");

        return paths;
    }

    private static bool IsBuildOutput(string projectDir, string path)
    {
        var relative = Path.GetRelativePath(projectDir, path);
        var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return segments.Any(s =>
            string.Equals(s, "bin", System.StringComparison.OrdinalIgnoreCase)
            || string.Equals(s, "obj", System.StringComparison.OrdinalIgnoreCase));
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

        Assert.NotNull(dir);
        return dir!;
    }
}
