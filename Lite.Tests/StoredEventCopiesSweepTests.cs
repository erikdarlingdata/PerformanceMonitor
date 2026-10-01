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
/// Every read of the three event views whose copies a later batch can store again goes through
/// <see cref="PerformanceMonitorLite.Database.StoredEventCopies"/>, which drops those copies. This sweep finds every
/// literal FROM or JOIN on v_blocked_process_reports, v_long_query_completions or v_system_health_events in Lite's
/// source, and every string literal that is exactly one of those names, and compares them with the list below. Comments
/// are skipped by the shared <see cref="CSharpSourceWalker"/>. A new read fails here until it goes through
/// StoredEventCopies or joins the list with its reason.
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

    [Fact]
    public void EveryReadOfTheEventViews_GoesThroughStoredEventCopies_OrIsOnTheList()
    {
        var read = new Regex($@"\b(?:FROM|JOIN)\s+(?:main\.)?({string.Join("|", Views)})\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        var found = new Dictionary<(string File, string View), List<int>>();
        foreach (var path in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                continue;

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
