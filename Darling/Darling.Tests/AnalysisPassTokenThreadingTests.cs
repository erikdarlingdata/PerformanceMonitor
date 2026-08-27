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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The analysis pass's token has to reach the reads, not just the gaps between them (#2443).
///
/// <para>#2438 armed a per-pass budget and #2430's classifier told a timeout from a stop, so an
/// overrunning server became loudly stuck instead of silently skipped forever. What neither did was
/// hand the token to the store layer: 168 of this project's async store calls took the no-argument
/// overload, so abandonment happened BETWEEN stages. A pass gave up while its wedged query kept a
/// connection and a session alive until the query finished on its own — on a healthy fleet
/// invisible, and on the pathological case these fixes exist for, the monitoring tool holding a
/// session it had already stopped waiting for.</para>
///
/// <para>The threading itself is mechanical; this file is the part that lasts. A sweep this size
/// only stays swept if something counts it, and the failure mode it guards against is specific:
/// adding <c>CancellationToken cancellationToken = default</c> to signatures and stopping there,
/// which leaves every call site passing <see cref="CancellationToken.None"/> while LOOKING threaded.
/// So the pin is on the CALL, not the signature — a no-argument <c>ExecuteReaderAsync()</c> is what
/// goes red, and no default can hide it.</para>
/// </summary>
public sealed class AnalysisPassTokenThreadingTests
{
    /// <summary>
    /// The no-argument overloads. Each has a token-taking sibling, so the empty parentheses are the
    /// whole tell — there is no shape of correct code in this project that needs them.
    /// </summary>
    private static readonly Regex s_untokenedStoreCall = new(
        @"\.(?:ExecuteReaderAsync|ExecuteNonQueryAsync|ExecuteScalarAsync|ReadAsync|OpenAsync|OpenConnectionAsync)\(\s*\)",
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
    /// Every method allowed to make an untokened store call, and the reason it is allowed to. This is
    /// an enumeration and not a wildcard on purpose: the exemption list this repo spent a day removing
    /// was a wildcard, and the way that one grew was that nobody had to name a new entry.
    ///
    /// <para>Two kinds live here and they are not the same kind. The read-back surfaces are OFF the
    /// pass — the viewer, the MCP and the retention sweep have no per-pass budget and no wedged
    /// analysis to abandon, so there is no token to thread and inventing one would be pretending.
    /// <c>InsertFindingAsync</c> is the other kind: it is ON the pass and the token is deliberately
    /// withheld, because a finding set cut in half is a third outcome that reads like neither of the
    /// two the classifier names (see the method's own note, and
    /// <see cref="TheFindingInsertIsAbandonedBeforeItStartsOrNotAtAll"/>).</para>
    /// </summary>
    private static readonly Dictionary<string, string> s_exempt = new(StringComparer.Ordinal)
    {
        ["GetRecentFindingsAsync"] = "read-back: the MCP + viewer findings read, no pass to abandon",
        ["GetLatestFindingsAsync"] = "read-back: the viewer's Recommendations tab, no pass to abandon",
        ["GetMutedStoriesAsync"] = "read-back: the viewer's mute registry read, no pass to abandon",
        ["MuteStoryAsync"] = "off-pass write: the MCP/viewer mute verb, no pass to abandon",
        ["UnmuteStoryAsync"] = "off-pass write: the viewer's unmute verb, no pass to abandon",
        ["CleanupOldFindingsAsync"] = "off-pass write: the worker's retention sweep, its own lifetime",
        ["InsertFindingAsync"] = "on-pass write, token withheld: a half-written finding set must not exist"
    };

    /// <summary>
    /// The sweep, and the thing that keeps it swept. Every store call in the analysis project must
    /// take the pass's token, and the ones that do not must be exempt BY NAME with a marker in their
    /// own doc comment — so the reason travels with the code rather than living only here.
    ///
    /// <para>Scanned over the whole project directory rather than a file list, because a file list is
    /// a thing a new partial can be added outside of. A seventh <c>PgFactCollector</c> partial is
    /// covered the day it exists.</para>
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

                if (s_exempt.ContainsKey(enclosing) && DocBlockAbove(lines, IndexOfDeclaration(lines, i)).Contains(ExemptionMarker, StringComparison.Ordinal))
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
    /// a claim the code stopped making, and it must not be allowed to sit there authorizing the next
    /// one. Every entry must still be earning its place, and every marker in the source must belong to
    /// an entry.
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

        /* The counts are stated so a NEW untokened call inside an already-exempt method cannot ride in
           on the exemption. 15 read-back calls across six methods, and the one finding INSERT. */
        Assert.Equal(16, untokenedBy.Values.Sum());
        Assert.Equal(1, untokenedBy["InsertFindingAsync"]);
    }

    /// <summary>
    /// The fact collector is where the token would have been silently useless. Its per-query catches were
    /// bare <c>catch { }</c> — deliberately, so a missing table degrades to "no facts" — which meant an
    /// armed token produced thirty-three swallowed cancellations and a pass that carried on collecting
    /// under a token that had already fired. Every catch there now lets an abandonment through, which is
    /// what turns the threaded token into an exit rather than thirty-three wasted connection opens.
    /// </summary>
    [Fact]
    public void EveryCatchOnTheFactCollectorLetsAnAbandonmentThrough()
    {
        var bare = new List<string>();
        var opensCatch = new Regex(@"^\s*catch\b", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        var classified = new Regex(
            @"^\s*catch\s*\(\s*Exception\s+ex\s*\)\s*when\s*\(\s*!AnalysisShutdown\.IsExpectedAbandon\(",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        foreach (var (file, lines) in AnalysisSources())
        {
            if (!Path.GetFileName(file).StartsWith("PgFactCollector.", StringComparison.Ordinal))
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

        /* 27 collect methods guard their read this way; the number is stated so deleting one is a decision.
           The other four collect methods have no catch at all and never had one — their reads propagate
           straight to the pass, which is the same outcome by a shorter route. */
        Assert.Equal(27, AnalysisSources()
            .Where(s => Path.GetFileName(s.File).StartsWith("PgFactCollector.", StringComparison.Ordinal))
            .SelectMany(s => s.Lines)
            .Count(line => classified.IsMatch(line)));
    }

    /// <summary>
    /// The behavioural half, and the reason a source pin alone is not enough here. Reverted, this test
    /// fails with an <c>NpgsqlException</c> rather than an <see cref="OperationCanceledException"/>, which
    /// is the whole defect in one line: handed a token that had ALREADY fired,
    /// <see cref="PgFactCollector.CollectFactsAsync"/> still dialled the store. The collectors that carry
    /// a catch then swallowed the failure and the pass carried on to the next one, so a cancelled pass
    /// spent thirty-three connection opens finding out what the token had said before the first.
    ///
    /// <para>No store is needed to run this, precisely because the token is already cancelled — a pass
    /// that observes it never reaches the network.</para>
    /// </summary>
    [Fact]
    public async Task AFiredTokenStopsFactCollectionAtTheFirstReadInsteadOfRunningEveryCollector()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=nobody;Password=nobody;Database=nowhere;Timeout=1;Command Timeout=1");

        var collector = new PgFactCollector(dataSource);
        var context = new AnalysisContext
        {
            ServerId = 1,
            ServerName = "wedged",
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
            CancellationToken = new CancellationToken(canceled: true)
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => collector.CollectFactsAsync(context));
    }

    /// <summary>
    /// The decision this issue asked to be made explicitly rather than assumed: what a cancelled PERSIST
    /// means. Every row shares one <c>analysis_time</c> and the latest-findings read keys on
    /// <c>MAX(analysis_time)</c>, so a batch cut in half does not read as truncated; it reads as a
    /// complete analysis that found fewer problems, and the server looks HEALTHIER for having been
    /// abandoned. That is a third outcome, distinct from the timeout and the shutdown the classifier
    /// names, and it must not exist. The connection open is therefore the last abandonment point: before
    /// the first row, or not at all.
    ///
    /// <para>#2448 closed the other half of that. #2443 could only reason about the CANCELLATION path,
    /// and the same truncated set was still reachable from an ordinary store fault mid-batch, where no
    /// amount of token discipline reaches. The batch is now one transaction, so it is all-or-nothing
    /// against a fault as well — which is why the between-rows assertion below is still right, but no
    /// longer the only thing standing between a store fault and a set that understates a server. Both
    /// halves are pinned here because they are one property.</para>
    /// </summary>
    [Fact]
    public void TheFindingInsertIsAbandonedBeforeItStartsOrNotAtAll()
    {
        var store = ReadSource(Path.Combine(AnalysisDirectory(), "PgFindingStore.cs"));

        /* The abandonment point: the batch's connection open observes the pass token. */
        Assert.Contains(
            "await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);",
            store, StringComparison.Ordinal);

        /* And nothing after it does. A ThrowIfCancellationRequested between rows would MAKE the partial
           set rather than prevent it, which is why there is none. */
        var insertBatch = Between(store, "public async Task<List<AnalysisFinding>> InsertFindingsAsync(", "public async Task<List<AnalysisFinding>> SaveFindingsAsync(");
        Assert.DoesNotContain("ThrowIfCancellationRequested", insertBatch, StringComparison.Ordinal);

        /* #2448: the batch is one transaction, and the rows are enlisted in it. Both halves are pinned
           because either alone is silently useless — a transaction the rows do not join commits nothing
           of theirs, and enlisting in a transaction nobody commits writes nothing at all. */
        Assert.Contains("await connection.BeginTransactionAsync();", insertBatch, StringComparison.Ordinal);
        Assert.Contains("await transaction.CommitAsync();", insertBatch, StringComparison.Ordinal);
        Assert.Contains("new NpgsqlCommand(InsertFindingSql, connection, transaction)", store, StringComparison.Ordinal);

        /* And the row write must not swallow. A per-row catch inside a transaction is worse than useless:
           PostgreSQL refuses every later statement once one fails (25P02), so it buys N-k ERROR lines for
           one event and a CommitAsync that returns normally having written nothing. */
        var rowWrite = Between(store, "private async Task InsertFindingAsync(", "private static AnalysisFinding ReadFinding(");
        Assert.DoesNotContain("catch", rowWrite, StringComparison.Ordinal);

        /* The row write states the decision where someone changing it will read it. */
        Assert.Contains(ExemptionMarker, Between(store, "/// Inserts one finding on an already-open connection", "private async Task InsertFindingAsync("), StringComparison.Ordinal);
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
            yield return (Path.GetFileName(file), ReadSource(file).Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'));
        }
    }

    private static string AnalysisDirectory() =>
        Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis");

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

    private static string ReadSource(string path) => File.ReadAllText(path);
}
