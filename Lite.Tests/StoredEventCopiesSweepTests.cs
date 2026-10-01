using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every read of the four event views whose copies a later batch can store again goes through
/// <see cref="PerformanceMonitorLite.Database.StoredEventCopies"/>, which drops those copies. This sweep finds every
/// literal FROM or JOIN on v_blocked_process_reports, v_long_query_completions, v_system_health_events or v_deadlocks in Lite's
/// source, and every string literal that is exactly one of those names, and compares them with the list below. Comments
/// are skipped by the shared <see cref="CSharpSourceWalker"/>. A new read fails here until it goes through
/// StoredEventCopies or joins the list with its reason.
/// </summary>
public class StoredEventCopiesSweepTests
{
    private static readonly string[] Views = ["v_blocked_process_reports", "v_long_query_completions", "v_system_health_events", "v_deadlocks"];

    /* (file, view) -> the number of reads that file makes of the view outside StoredEventCopies, and why. */
    private static readonly Dictionary<(string File, string View), int> Allowed = new()
    {
        /* StoredEventCopies itself: the one place that reads the views. */
        [("StoredEventCopies.cs", "v_blocked_process_reports")] = 1,
        [("StoredEventCopies.cs", "v_long_query_completions")] = 1,
        [("StoredEventCopies.cs", "v_system_health_events")] = 1,
        [("StoredEventCopies.cs", "v_deadlocks")] = 2,

        /* HasAnyBlockingCaptureAsync's EXISTS over the server's rows, with no time window: a stored copy cannot
           change whether a row exists. */
        [("LocalDataService.BlockingStats.cs", "v_blocked_process_reports")] = 1,
        [("LocalDataService.BlockingStats.cs", "v_deadlocks")] = 1,

        /* The deadlock reads that do not return rows, which count or take a MAX over the plain union (no helper): the
           counts and buckets count COUNT(DISTINCT StoredEventCopies.DeadlockIdentityTuple), pinned by
           EveryDeadlockCountReadsThePlainUnionWithTheSharedIdentity below, and a MAX is unchanged by a copy.
           Blocking.cs: the overview count, the MAX(deadlock_time), the slicer and the trend. */
        [("LocalDataService.Blocking.cs", "v_deadlocks")] = 5,
        [("AnomalyDetector.cs", "v_deadlocks")] = 1,
        [("BaselineProvider.cs", "v_deadlocks")] = 1,
        [("DuckDbFactCollector.Waits.cs", "v_deadlocks")] = 1,
        [("LocalDataService.Overview.cs", "v_deadlocks")] = 1,
        [("LocalDataService.DailySummary.cs", "v_deadlocks")] = 1,

        /* The two last-capture reads, MAX(collection_time) with no time window: a batch that stored only copies of
           events already held still read the session, so its collection_time is a true capture. */
        [("LocalDataService.SystemEvents.cs", "v_system_health_events")] = 2,
    };

    [Fact]
    public void EveryReadOfTheEventViews_GoesThroughStoredEventCopies_OrIsOnTheList()
    {
        var read = new Regex($@"\b(?:FROM|JOIN)\s+(?:main\.)?({string.Join("|", Views)})\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        var found = new Dictionary<(string File, string View), List<int>>();
        foreach (var path in LiteSources())
        {
            /* Comments name these views in prose, so only code and string-literal text are read; a comment is
               blanked to spaces. Every character keeps its offset, so line numbers match the file, and a FROM on
               one line with the view on the next is still one match. */
            var source = File.ReadAllText(path).Replace("\r\n", "\n");
            var keep = CSharpSourceWalker.CodeMask(source);
            var literals = CSharpSourceWalker.StringLiteralBodies(source).ToList();
            foreach (var (start, body) in literals)
            {
                Array.Fill(keep, true, start, body.Length);
            }

            var text = new string(source.Select((c, i) => keep[i] || c == '\n' ? c : ' ').ToArray());
            var reads = read.Matches(text).Select(m => (Index: m.Index, View: m.Groups[1].Value.ToLowerInvariant()))
                .Concat(literals.Where(l => Views.Contains(l.Text, StringComparer.Ordinal)).Select(l => (Index: l.Start, View: l.Text)));

            foreach (var (index, view) in reads)
            {
                var key = (Path.GetFileName(path), view);
                if (!found.TryGetValue(key, out var lines))
                    found[key] = lines = new List<int>();
                lines.Add(source.AsSpan(0, index).Count('\n') + 1);
            }
        }

        var problems = found
            .Where(f => !Allowed.TryGetValue(f.Key, out var allowed) || allowed != f.Value.Count)
            .Select(f => $"{f.Key.File} reads {f.Key.View} {f.Value.Count} time(s) at line(s) {string.Join(", ", f.Value)}; "
                + $"the list allows {(Allowed.TryGetValue(f.Key, out var n) ? n : 0)}")
            .Concat(Allowed.Keys.Where(k => !found.ContainsKey(k))
                .Select(k => $"{k.File} no longer reads {k.View}; remove it from the list"))
            .ToList();

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// A call never puts a collection_time lower bound in its <c>where</c>. The bound goes in <c>collectedFrom</c>,
    /// and the rule then also reads one fallback window behind it, where the first copy of an event re-collected at
    /// the window's start was stored. A bound left in <c>where</c> cuts that look-back off: the copy reads again at
    /// every window start, and the build and the sweep above stay green. A read that windows on event_time needs
    /// no look-back, because a copy keeps its first copy's event_time.
    /// </summary>
    [Fact]
    public void NoCallPutsACollectionTimeLowerBoundInItsWhere()
    {
        var call = new Regex(@"\bStoredEventCopies\.(BlockedProcessReports|LongQueryCompletions|SystemHealthEvents|Deadlocks)\(",
            RegexOptions.CultureInvariant);
        var lowerBound = new Regex(@"\bcollection_time\s*>", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        var methods = new HashSet<string>(StringComparer.Ordinal);
        var problems = new List<string>();
        foreach (var path in LiteSources())
        {
            var source = File.ReadAllText(path).Replace("\r\n", "\n");
            var code = CSharpSourceWalker.CodeMask(source);
            var literals = CSharpSourceWalker.StringLiteralBodies(source).ToList();
            foreach (Match match in call.Matches(source))
            {
                if (!code[match.Index])
                    continue;

                /* The call's arguments run to the parenthesis that closes it; parentheses inside a literal are SQL. */
                var open = match.Index + match.Length - 1;
                var close = open;
                for (var depth = 0; close < source.Length; close++)
                {
                    if (!code[close]) continue;
                    if (source[close] == '(') depth++;
                    else if (source[close] == ')' && --depth == 0) break;
                }

                methods.Add(match.Groups[1].Value);
                if (literals.Any(l => l.Start > open && l.Start < close && lowerBound.IsMatch(l.Text)))
                {
                    problems.Add($"{Path.GetFileName(path)}:{source.AsSpan(0, match.Index).Count('\n') + 1} "
                        + "passes a collection_time lower bound in where; pass it as collectedFrom");
                }
            }
        }

        Assert.Equal(4, methods.Count);
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// A deadlock COUNT counts <c>StoredEventCopies.DeadlockDistinctCount</c> over the plain v_deadlocks, one use per
    /// count read, so no count can use its own identity. This lists those uses per file; the rows reads stay on
    /// <c>StoredEventCopies.Deadlocks</c>. The count SQL never joins back and never selects the graph itself.
    /// </summary>
    [Fact]
    public void EveryDeadlockCountReadsThePlainUnionWithTheSharedIdentity()
    {
        var expected = new Dictionary<string, int>
        {
            ["LocalDataService.Blocking.cs"] = 3,
            ["AnomalyDetector.cs"] = 1,
            ["BaselineProvider.cs"] = 1,
            ["DuckDbFactCollector.Waits.cs"] = 1,
            ["LocalDataService.Overview.cs"] = 1,
            ["LocalDataService.DailySummary.cs"] = 1,
        };
        var actual = LiteSources()
            .Select(p => (File: Path.GetFileName(p), Count: Regex.Matches(File.ReadAllText(p), @"StoredEventCopies\.DeadlockDistinctCount").Count))
            .Where(f => f.Count > 0 && f.File != "StoredEventCopies.cs")
            .ToDictionary(f => f.File, f => f.Count);

        Assert.Equal(expected.OrderBy(e => e.Key), actual.OrderBy(e => e.Key));
    }

    private static IEnumerable<string> LiteSources() =>
        Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories)
            .Where(path => !path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && !path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal));

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new InvalidOperationException("Could not find the repository root from " + thisFile);
    }
}
