using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every table ArchiveService archives keeps only part of its history in the hot table: ordinary archival moves
/// rows older than 7 days to Parquet, and the 512 MB reset moves all of them. A reader that asks about a window
/// or about history must read the table's v_ view, which unions the hot table with the archive. This sweep finds
/// every literal FROM or JOIN on an archivable table's bare name in Lite's source and compares the result with
/// the reads below, which are bare on purpose. A new bare read fails here until it moves to v_ or joins the
/// list with its reason. Reads that build the table name at run time (the collectors' watermark and archive
/// paths, Overview's hot-then-archive read) do not match by design.
/// </summary>
public class ArchivableTableBareReadSweepTests
{
    /* (file, table) -> the number of bare reads that file makes on purpose, and why. */
    private static readonly Dictionary<(string File, string Table), int> BareOnPurpose = new()
    {
        /* Delta seeding: runs once at service start and wants each key's newest stored row inside the seed
           cutoff. The 512 MB reset does not restart the service, so the in-memory baselines carry over; a restart
           onto an empty table seeds nothing, and the first delta is then 0, not a spike. The SQL is mirrored
           verbatim with Darling's seeder and pinned there. */
        [("DeltaCalculator.cs", "wait_stats")] = 2,
        [("DeltaCalculator.cs", "file_io_stats")] = 2,
        [("DeltaCalculator.cs", "perfmon_stats")] = 2,
        [("DeltaCalculator.cs", "memory_grant_stats")] = 2,
        [("DeltaCalculator.cs", "latch_stats")] = 2,
        [("DeltaCalculator.cs", "spinlock_stats")] = 2,
        [("DeltaCalculator.cs", "procedure_stats")] = 1,
        [("DeltaCalculator.cs", "query_stats")] = 1,

        /* Dismiss bookkeeping: the UPDATE marks live rows, the sidecar table covers archived ones, and these
           reads tell the two apart or verify the UPDATE. The alert history itself reads v_config_alert_log. */
        [("LocalDataService.AlertHistory.cs", "config_alert_log")] = 6,

        /* The newest AG snapshot. The alert path skips a snapshot that is not fresh, so an empty hot table right
           after a reset reads as "no data yet", not as healthy. */
        [("LocalDataService.AgTopology.cs", "ag_replica_states")] = 2,
        [("LocalDataService.AgTopology.cs", "ag_database_replica_states")] = 2,
        [("LocalDataService.AvailabilityGroups.cs", "ag_replica_states")] = 2,
        [("LocalDataService.AvailabilityGroups.cs", "ag_database_replica_states")] = 2,

        /* Not a table read: "deadlocks" there is a CTE over v_deadlocks. */
        [("LocalDataService.DailySummary.cs", "deadlocks")] = 2,

        /* The newest database-state snapshot and the one before it. Which source the deviation sweep reads
           right after a reset is picked per sweep in its own change. */
        [("LocalDataService.DatabaseStates.cs", "database_states")] = 16,

        /* A current-state read: whether each collector has run, anchored on the same table's first collection,
           so after a reset all of it restarts together and reads "not run yet". */
        [("LocalDataService.RuntimePrecondition.cs", "collection_log")] = 3,

        /* Collector path. The backfill's candidate databases share one rule with Darling's; its orphan prune
           compares against MAX(collection_time), which is NULL on an empty table, so it deletes nothing then.
           HasPriorCollectorSuccessAsync counts the live log first and v_collection_log when that is 0. */
        [("RemoteCollectorService.QueryStoreBackfill.cs", "query_store_stats")] = 1,
        [("RemoteCollectorService.QueryStoreBackfill.cs", "database_states")] = 3,
        [("RemoteCollectorService.cs", "collection_log")] = 1,
    };

    /* "/*" followed by white space opens a comment; a glob such as "/*_table.parquet" does not. */
    private static readonly Regex BlockComment = new(@"/\*\s.*?\*/", RegexOptions.Singleline | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryBareReadOfAnArchivableTable_IsOnTheBareOnPurposeList()
    {
        var tables = ArchiveService.ArchivableTables.Select(t => t.Table).Distinct(StringComparer.Ordinal)
            .OrderByDescending(t => t.Length).ToList();
        var bare = new Regex(@"\b(?:FROM|JOIN)\s+(?:main\.)?(" + string.Join("|", tables) + @")\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        var found = new Dictionary<(string File, string Table), List<int>>();
        foreach (var path in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                continue;

            /* Comments name these tables in prose. Each comment keeps its line breaks, so line numbers still
               match the file, and the match spans line breaks, so a FROM on one line and the table on the next
               is caught. */
            var text = BlockComment.Replace(File.ReadAllText(path).Replace("\r\n", "\n"),
                comment => new string('\n', comment.Value.Count(c => c == '\n')));
            text = string.Join("\n", text.Split('\n')
                .Select(line => line.TrimStart().StartsWith("//", StringComparison.Ordinal) ? "" : line));

            foreach (Match match in bare.Matches(text))
            {
                var key = (Path.GetFileName(path), match.Groups[1].Value.ToLowerInvariant());
                if (!found.TryGetValue(key, out var lines))
                    found[key] = lines = new List<int>();
                lines.Add(text.AsSpan(0, match.Index).Count('\n') + 1);
            }
        }

        var problems = found
            .Where(f => !BareOnPurpose.TryGetValue(f.Key, out var allowed) || allowed != f.Value.Count)
            .Select(f => $"{f.Key.File} reads {f.Key.Table} bare {f.Value.Count} time(s) at line(s) {string.Join(", ", f.Value)}; "
                + $"the list allows {(BareOnPurpose.TryGetValue(f.Key, out var n) ? n : 0)}")
            .Concat(BareOnPurpose.Keys.Where(k => !found.ContainsKey(k))
                .Select(k => $"{k.File} no longer reads {k.Table} bare; remove it from the list"))
            .ToList();

        Assert.Empty(problems);
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
