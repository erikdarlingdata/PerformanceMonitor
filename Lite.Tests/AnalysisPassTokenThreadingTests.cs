/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The analysis pass's token has to reach the reads, not just the gaps between them (#2443).
///
/// <para>#2419 armed a per-pass budget and put a checkpoint ahead of every store-touching stage, which
/// closed the user-visible defect in #2412: a wedged server became loudly stuck instead of silently
/// skipped forever. What it did not do was hand the token to the store layer — its own diff touched no
/// store file — so abandonment happened BETWEEN stages. A collector that had already started ran to
/// completion, and the pass that had given up went on holding the read lock and the connection it was
/// no longer waiting for.</para>
///
/// <para>The threading is mechanical; this file is the part that lasts. A sweep of 203 call sites only
/// stays swept if something counts it, and the failure mode it guards against is specific: adding
/// <c>CancellationToken cancellationToken = default</c> to signatures and stopping there, which leaves
/// every call site passing <see cref="CancellationToken.None"/> while LOOKING threaded. So the pin is on
/// the CALL, not the signature — a no-argument <c>ExecuteReaderAsync()</c> is what goes red, and no
/// default can hide it. The Darling twin is <c>AnalysisPassTokenThreadingTests</c> in Darling.Tests.</para>
/// </summary>
public sealed class AnalysisPassTokenThreadingTests
{
    /// <summary>
    /// The no-argument overloads, plus the read lock. Each has a token-taking sibling, so the empty
    /// parentheses are the whole tell. <c>AcquireReadLock()</c> belongs on this list and not on a list
    /// of its own: every store read on the pass takes that lock BEFORE it opens its connection, so a
    /// pass that reached the reads with a token and the lock without one would still be uninterruptible
    /// behind an archival — which is exactly the gap #2443 called out as Lite-only.
    /// </summary>
    private static readonly Regex s_untokenedStoreCall = new(
        @"\.(?:ExecuteReaderAsync|ExecuteNonQueryAsync|ExecuteScalarAsync|ReadAsync|OpenAsync)\(\s*\)|AcquireReadLock\(\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A member declaration, used only to attribute a call to the method that makes it. Deliberately
    /// crude — it needs to name the enclosing method, not parse C#. The <c>[^=;()]*</c> is what keeps
    /// field initializers and expression-bodied properties out.
    /// </summary>
    private static readonly Regex s_memberDeclaration = new(
        @"^\s*(?:public|private|internal|protected)[^=;()]*\s(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private const string ExemptionMarker = "#2443 exempt";

    /// <summary>
    /// Every method allowed to make an untokened store call, and the reason it is allowed to — the same
    /// five the Darling twin exempts, for the same two reasons. This is an enumeration and not a
    /// wildcard on purpose: the exemption list this repo spent a day removing was a wildcard, and the
    /// way that one grew was that nobody ever had to name a new entry.
    ///
    /// <para>The four read-back surfaces are OFF the pass — the viewer, the MCP and the retention sweep
    /// have no per-pass budget and no wedged analysis to abandon, so there is no token to thread.
    /// <c>InsertFindingAsync</c> is the other kind: ON the pass, token deliberately withheld, because a
    /// finding set cut in half is an outcome that reads like a healthy one (see
    /// <see cref="TheFindingInsertIsAbandonedBeforeItStartsOrNotAtAll"/>).</para>
    /// </summary>
    private static readonly Dictionary<string, string> s_exempt = new(StringComparer.Ordinal)
    {
        ["GetRecentFindingsAsync"] = "read-back: the recommendations reader + MCP findings read",
        ["GetLatestFindingsAsync"] = "read-back: the viewer's latest-run findings",
        ["MuteStoryAsync"] = "off-pass write: the MCP/viewer mute verb",
        ["CleanupOldFindingsAsync"] = "off-pass write: the retention sweep, its own lifetime",
        ["InsertFindingAsync"] = "on-pass write, token withheld: a half-written finding set must not exist"
    };

    /// <summary>
    /// The sweep, and the thing that keeps it swept. Scanned over the whole analysis directory rather
    /// than a file list, because a file list is a thing a seventh <c>DuckDbFactCollector</c> partial can
    /// be added outside of.
    /// </summary>
    [Fact]
    public void NoStoreCallOnTheAnalysisPassRunsWithoutThePassToken()
    {
        var offenders = new List<string>();

        foreach (var (file, lines) in AnalysisSources())
        {
            var enclosing = string.Empty;

            for (var i = 0; i < lines.Length; i++)
            {
                var declaration = s_memberDeclaration.Match(lines[i]);
                if (declaration.Success)
                {
                    enclosing = declaration.Groups["name"].Value;
                }

                if (!s_untokenedStoreCall.IsMatch(lines[i]))
                {
                    continue;
                }

                if (s_exempt.ContainsKey(enclosing)
                    && DocBlockAbove(lines, IndexOfDeclaration(lines, i)).Contains(ExemptionMarker, StringComparison.Ordinal))
                {
                    continue;
                }

                offenders.Add($"{file}:{i + 1} in {enclosing}(): {lines[i].Trim()}");
            }
        }

        Assert.True(offenders.Count == 0,
            "These store calls run without the pass's cancellation token, so the pass can be abandoned "
            + "around them but never inside one. Pass context.CancellationToken (or the method's own token "
            + $"parameter). If the call genuinely must complete, add its method to {nameof(s_exempt)} and put a "
            + $"'{ExemptionMarker}' note in its doc comment saying why:\n"
            + string.Join("\n", offenders));
    }

    /// <summary>
    /// The other half of the agreement: an exemption that no longer corresponds to an untokened call is
    /// a claim the code stopped making, and it must not be allowed to sit there authorising the next one.
    /// </summary>
    [Fact]
    public void EveryExemptionIsStillEarningItsPlace()
    {
        var untokenedBy = new Dictionary<string, int>(StringComparer.Ordinal);
        var markedMethods = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (_, lines) in AnalysisSources())
        {
            var enclosing = string.Empty;
            var enclosingAt = -1;

            for (var i = 0; i < lines.Length; i++)
            {
                var declaration = s_memberDeclaration.Match(lines[i]);
                if (declaration.Success)
                {
                    enclosing = declaration.Groups["name"].Value;
                    enclosingAt = i;
                    if (DocBlockAbove(lines, enclosingAt).Contains(ExemptionMarker, StringComparison.Ordinal))
                    {
                        markedMethods.Add(enclosing);
                    }
                }

                if (s_untokenedStoreCall.IsMatch(lines[i]) && enclosingAt >= 0)
                {
                    untokenedBy[enclosing] = untokenedBy.TryGetValue(enclosing, out var n) ? n + 1 : 1;
                }
            }
        }

        Assert.Equal(s_exempt.Keys.OrderBy(k => k, StringComparer.Ordinal), markedMethods.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(s_exempt.Keys.OrderBy(k => k, StringComparer.Ordinal), untokenedBy.Keys.OrderBy(k => k, StringComparer.Ordinal));

        /* Stated so a NEW untokened call inside an already-exempt method cannot ride in on the
           exemption: 14 read-back calls across four methods, plus the one finding INSERT. */
        Assert.Equal(15, untokenedBy.Values.Sum());
        Assert.Equal(1, untokenedBy["InsertFindingAsync"]);
    }

    /// <summary>
    /// The fact collector is where the token would have been silently useless. Its per-query catches were
    /// bare <c>catch { }</c> — deliberately, so a missing table degrades to "no facts" — which meant an
    /// armed token produced 27 swallowed cancellations and a pass that carried on collecting under a
    /// token that had already fired. Every catch there now lets an abandonment through, which is what
    /// turns the threaded token into an exit rather than 27 wasted lock acquisitions.
    /// </summary>
    [Fact]
    public void EveryCatchOnTheFactCollectorLetsAnAbandonmentThrough()
    {
        var bare = new List<string>();
        var opensCatch = new Regex(@"^\s*catch\b", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        var classified = new Regex(
            @"^\s*catch\s*\(\s*Exception\s+ex\s*\)\s*when\s*\(\s*!AnalysisAbandon\.IsExpected\(",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        foreach (var (file, lines) in AnalysisSources())
        {
            if (!Path.GetFileName(file).StartsWith("DuckDbFactCollector.", StringComparison.Ordinal))
            {
                continue;
            }

            for (var i = 0; i < lines.Length; i++)
            {
                if (opensCatch.IsMatch(lines[i]) && !classified.IsMatch(lines[i]))
                {
                    bare.Add($"{file}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        Assert.True(bare.Count == 0,
            "A fact-collector catch that does not classify swallows the abandonment the token was armed for, "
            + "and the pass keeps collecting under a fired token:\n" + string.Join("\n", bare));

        /* 27 collect methods guard their read this way; the number is stated so deleting one is a
           decision. The other four have no catch at all and never had one — their reads propagate
           straight to the pass, which is the same outcome by a shorter route. */
        Assert.Equal(27, AnalysisSources()
            .Where(s => Path.GetFileName(s.File).StartsWith("DuckDbFactCollector.", StringComparison.Ordinal))
            .SelectMany(s => s.Lines)
            .Count(line => classified.IsMatch(line)));
    }

    /// <summary>
    /// The decision #2443 asked to be made explicitly rather than assumed: what a cancelled PERSIST
    /// means. Every row shares one <c>analysis_time</c> and the latest-findings read takes the newest
    /// <c>analysis_time</c>, so a batch cut in half does not read as truncated; it reads as a complete
    /// analysis that found fewer problems, and the server looks HEALTHIER for having been abandoned.
    /// The lock and the connection open are therefore the last abandonment points: before the first
    /// row, or not at all.
    ///
    /// <para>#2448 closed the other half of that. #2443 could only reason about the CANCELLATION path,
    /// and the same truncated set was still reachable from an ordinary store fault mid-batch, where no
    /// amount of token discipline reaches. The batch is now one transaction, so it is all-or-nothing
    /// against a fault as well. Deliberately the same answer as the Darling twin's, pinned the same
    /// way, because a divergence here would be a parity bug rather than a local choice.</para>
    /// </summary>
    [Fact]
    public void TheFindingInsertIsAbandonedBeforeItStartsOrNotAtAll()
    {
        var store = File.ReadAllText(Path.Combine(AnalysisDirectory(), "FindingStore.cs"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var insertBatch = Between(store,
            "public async Task<List<AnalysisFinding>> InsertFindingsAsync(",
            "public async Task<List<AnalysisFinding>> SaveFindingsAsync(");

        /* The abandonment points: both the lock wait and the connection open observe the pass token. */
        Assert.Contains("_duckDb.AcquireReadLock(context.CancellationToken)", insertBatch, StringComparison.Ordinal);
        Assert.Contains("await connection.OpenAsync(context.CancellationToken)", insertBatch, StringComparison.Ordinal);

        /* And nothing after them does. A ThrowIfCancellationRequested between rows would MAKE the
           partial set rather than prevent it, which is why there is none. */
        Assert.DoesNotContain("ThrowIfCancellationRequested", insertBatch, StringComparison.Ordinal);

        /* #2448: the batch is one transaction, and the rows are enlisted in it. Both halves are pinned
           because either alone is silently useless — a transaction the rows do not join commits nothing
           of theirs, and enlisting in a transaction nobody commits writes nothing at all. */
        Assert.Contains("connection.BeginTransaction();", insertBatch, StringComparison.Ordinal);
        Assert.Contains("transaction.Commit();", insertBatch, StringComparison.Ordinal);
        Assert.Contains("cmd.Transaction = transaction;", store, StringComparison.Ordinal);

        /* The row write states the decision where someone changing it will read it. */
        Assert.Contains(ExemptionMarker,
            Between(store, "/// Inserts one finding on an already-open connection", "private static async Task InsertFindingAsync("),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The classifier's truth table. Both halves of the predicate are load-bearing and the TYPE half
    /// especially so: since #2419 this token fires on an ordinary timeout, so it is signalled during
    /// perfectly normal running, and a filter that asked only "has the token fired?" would relabel any
    /// genuine fault landing after the budget elapsed as an abandonment — swallowing the one line of
    /// evidence it left, at forty-odd catch sites at once.
    /// </summary>
    [Fact]
    public void AnAbandonmentIsAFiredTokenAndACancellationShape_NeverOneWithoutTheOther()
    {
        var fired = new CancellationToken(canceled: true);

        Assert.True(AnalysisAbandon.IsExpected(new OperationCanceledException(), fired));
        Assert.True(AnalysisAbandon.IsExpected(new TaskCanceledException(), fired));

        /* A fault that merely coincides with the budget expiring is still a fault. */
        Assert.False(AnalysisAbandon.IsExpected(new InvalidOperationException("store"), fired));
        Assert.False(AnalysisAbandon.IsExpected(new TimeoutException("deadline"), fired));

        /* And a cancellation shape with nothing cancelled means something threw it for another
           reason, which must keep its error. */
        Assert.False(AnalysisAbandon.IsExpected(new OperationCanceledException(), CancellationToken.None));
    }

    /// <summary>
    /// The Lite-only half of #2443, and the one place cancellation genuinely could not be replaced by
    /// reporting. Every store read on the pass takes the read lock BEFORE opening its connection, and
    /// <c>AcquireReadLock()</c> had no timeout and no token while its <c>AcquireWriteLock(TimeSpan?)</c>
    /// sibling had one — so a pass queued behind a long archival sat in an uninterruptible
    /// <c>EnterReadLock()</c> however carefully its reads were threaded.
    ///
    /// <para>Held behind a real write lock on another thread, which is what an archival looks like from
    /// here. The old overload would block for the full hold; the token'd one gives up when asked.</para>
    /// </summary>
    [Fact]
    public void TheReadLockWaitIsAbandonableWhileAWriterHoldsIt()
    {
        var initializer = new DuckDbInitializer(Path.Combine(Path.GetTempPath(), $"pm-lock-{Guid.NewGuid():N}.db"));

        var writerHasIt = new ManualResetEventSlim(false);
        var releaseWriter = new ManualResetEventSlim(false);

        var writer = Task.Run(() =>
        {
            using var write = initializer.AcquireWriteLock();
            writerHasIt.Set();
            releaseWriter.Wait(TimeSpan.FromSeconds(30));
        });

        try
        {
            Assert.True(writerHasIt.Wait(TimeSpan.FromSeconds(5)), "the writer never took the lock");

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
            var elapsed = Stopwatch.StartNew();

            Assert.Throws<OperationCanceledException>(() =>
            {
                using var read = initializer.AcquireReadLock(cts.Token);
            });

            /* Generously bounded — the assertion is "it gave up while the writer still held it",
               not a latency measurement. The writer holds for up to 30s. */
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(10),
                $"the read lock gave up only after {elapsed.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            releaseWriter.Set();
            writer.Wait(TimeSpan.FromSeconds(30));
        }

        /* And once the writer is gone it is an ordinary read lock again, token or no token. */
        using var after = initializer.AcquireReadLock(CancellationToken.None);
        Assert.NotNull(after);
    }

    private static string Between(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found: {start}");
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor not found after {start}: {end}");
        return source[from..to];
    }

    /// <summary>The declaration line a call at <paramref name="callLine"/> belongs to (scanning up).</summary>
    private static int IndexOfDeclaration(string[] lines, int callLine)
    {
        for (var i = callLine; i >= 0; i--)
        {
            if (s_memberDeclaration.IsMatch(lines[i]))
            {
                return i;
            }
        }

        return 0;
    }

    /// <summary>The contiguous <c>///</c> block immediately above a declaration, as one string.</summary>
    private static string DocBlockAbove(string[] lines, int declarationLine)
    {
        var doc = new List<string>();
        for (var i = declarationLine - 1; i >= 0 && lines[i].TrimStart().StartsWith("///", StringComparison.Ordinal); i--)
        {
            doc.Add(lines[i]);
        }

        return string.Join("\n", doc);
    }

    private static IEnumerable<(string File, string[] Lines)> AnalysisSources()
    {
        var files = Directory.GetFiles(AnalysisDirectory(), "*.cs", SearchOption.TopDirectoryOnly);
        Assert.NotEmpty(files);

        foreach (var file in files.OrderBy(f => f, StringComparer.Ordinal))
        {
            /* The working copy is CRLF; split on the LF so a line never carries a stray CR. */
            yield return (Path.GetFileName(file),
                File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'));
        }
    }

    private static string AnalysisDirectory() => Path.Combine(RepoRoot(), "Lite", "Analysis");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
