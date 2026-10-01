using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every read of the three event views whose copies a later batch can store again goes through
/// <see cref="PerformanceMonitorLite.Database.StoredEventCopies"/>, which drops those copies. This sweep finds every
/// literal FROM or JOIN on v_blocked_process_reports, v_long_query_completions or v_system_health_events in Lite's
/// source, and every string literal that is exactly one of those names, and compares them with the list below. A new
/// read fails here until it goes through StoredEventCopies or joins the list with its reason.
/// </summary>
public class StoredEventCopiesSweepTests
{
    private static readonly string[] Views = ["v_blocked_process_reports", "v_long_query_completions", "v_system_health_events"];

    /* (file, view) -> the number of reads that file makes of the view outside StoredEventCopies, and why. */
    private static readonly Dictionary<(string File, string View), int> Allowed = new()
    {
        /* StoredEventCopies itself: the one place that reads the views. */
        [("StoredEventCopies.cs", "v_blocked_process_reports")] = 1,
        [("StoredEventCopies.cs", "v_long_query_completions")] = 1,
        [("StoredEventCopies.cs", "v_system_health_events")] = 1,

        /* HasAnyBlockingCaptureAsync's EXISTS over the server's rows, with no time window: a stored copy cannot
           change whether a row exists. */
        [("LocalDataService.BlockingStats.cs", "v_blocked_process_reports")] = 1,

        /* The two last-capture reads, MAX(collection_time) with no time window: a batch that stored only copies of
           events already held still read the session, so its collection_time is a true capture. */
        [("LocalDataService.SystemEvents.cs", "v_system_health_events")] = 2,
    };

    /* "/*" followed by white space opens a comment; a glob such as "/*_table.parquet" does not. */
    private static readonly Regex BlockComment = new(@"/\*\s.*?\*/", RegexOptions.Singleline | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryReadOfTheEventViews_GoesThroughStoredEventCopies_OrIsOnTheList()
    {
        var names = string.Join("|", Views);
        var read = new Regex($@"\b(?:FROM|JOIN)\s+(?:main\.)?({names})\b|""({names})""",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        var found = new Dictionary<(string File, string View), List<int>>();
        foreach (var path in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                continue;

            /* Comments name these views in prose. Each comment keeps its line breaks, so line numbers still match
               the file, and the match spans line breaks, so a FROM on one line and the view on the next is caught. */
            var text = BlockComment.Replace(File.ReadAllText(path).Replace("\r\n", "\n"),
                comment => new string('\n', comment.Value.Count(c => c == '\n')));
            text = string.Join("\n", text.Split('\n')
                .Select(line => line.TrimStart().StartsWith("//", StringComparison.Ordinal) ? "" : line));

            foreach (Match match in read.Matches(text))
            {
                var view = (match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value).ToLowerInvariant();
                var key = (Path.GetFileName(path), view);
                if (!found.TryGetValue(key, out var lines))
                    found[key] = lines = new List<int>();
                lines.Add(text.AsSpan(0, match.Index).Count('\n') + 1);
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
