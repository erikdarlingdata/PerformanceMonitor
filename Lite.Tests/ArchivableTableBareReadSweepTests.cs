using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Darling.Tests;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every table ArchiveService archives keeps only part of its history in the hot table: ordinary archival moves
/// rows older than 7 days to Parquet, and the 512 MB reset moves all of them. A reader that asks about a window
/// or about history must read the table's v_ view, which unions the hot table with the archive. This sweep finds
/// every literal FROM or JOIN on an archivable table's bare name in Lite's source, skipping comments with the shared
/// <see cref="CSharpSourceWalker"/>, and fails when a file reads a table bare more often than the list below allows.
/// A new bare read fails here until it moves to v_ or joins the list with its reason.
/// <para>Some reads are not seen. Reads that build the table name at run time do not match, by design: the
/// collectors' watermark and archive paths, Overview's hot-then-archive read, and the database-state reads, which
/// pick the hot table or v_database_states per read. The pattern also sees only FROM or JOIN directly before the
/// name, so it misses a comma join (<c>FROM a, query_stats</c>), a quoted name (<c>FROM "query_stats"</c>) and a
/// name that starts the next literal (<c>"FROM " + "query_stats"</c>). None of those three is in Lite today.</para>
/// </summary>
public class ArchivableTableBareReadSweepTests
{
    /* (file, table) -> the number of bare reads that file makes on purpose, and why. */
    private static readonly Dictionary<(string File, string Table), int> BareOnPurpose = new()
    {
        /* A DELETE of exact duplicate rows at start: only the hot table can be deleted from, and the subquery that
           picks the rows reads the same table. A copy that was already archived stays until the archive ages out. */
        [("DeadlockDuplicateCleanup.cs", "deadlocks")] = 2,

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

        /* A current-state read: the collector's last run and the server's last and first collection are read
           together from one table, so they never come from different sources. */
        [("LocalDataService.RuntimePrecondition.cs", "collection_log")] = 4,

        /* Collector path. The backfill's candidate databases share one rule with Darling's; its orphan prune
           compares against MAX(collection_time), which is NULL on an empty table, so it deletes nothing then.
           HasPriorCollectorSuccessAsync counts the live log first and v_collection_log when that is 0. */
        [("RemoteCollectorService.QueryStoreBackfill.cs", "query_store_stats")] = 1,
        [("RemoteCollectorService.QueryStoreBackfill.cs", "database_states")] = 3,
        [("RemoteCollectorService.cs", "collection_log")] = 1,

        /* #4938: the start-up read of each daily collector's last run, so a restart does not make it due again. It
           reads the live log within the longest daily interval plus a day: a collector whose last run is older than
           that is due anyway, and the run it makes is then a live row. */
        [("RemoteCollectorService.RunTime.cs", "collection_log")] = 1,
    };

    private static readonly List<string> Tables = ArchiveService.ArchivableTables.Select(t => t.Table)
        .Distinct(StringComparer.Ordinal).OrderByDescending(t => t.Length).ToList();

    private static readonly Regex Bare = new(@"\b(?:FROM|JOIN)\s+(?:main\.)?(" + string.Join("|", Tables) + @")\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The bare reads in one file's source, as (table, line). Comments name these tables in prose, so only code and
    /// string-literal text are read; a comment is blanked to spaces. Every character keeps its offset, so line
    /// numbers match the file, and a FROM on one line with the table on the next is still one match.
    /// </summary>
    private static List<(string Table, int Line)> BareReads(string source)
    {
        source = source.Replace("\r\n", "\n");
        var keep = CSharpSourceWalker.CodeMask(source);
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            Array.Fill(keep, true, start, body.Length);
        }

        var text = new string(source.Select((c, i) => keep[i] || c == '\n' ? c : ' ').ToArray());
        return Bare.Matches(text)
            .Select(m => (Table: m.Groups[1].Value.ToLowerInvariant(), Line: text.AsSpan(0, m.Index).Count('\n') + 1))
            .ToList();
    }

    [Fact]
    public void EveryBareReadOfAnArchivableTable_IsOnTheBareOnPurposeList()
    {
        var found = new Dictionary<(string File, string Table), List<int>>();
        foreach (var path in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                continue;

            foreach (var (table, line) in BareReads(File.ReadAllText(path)))
            {
                var key = (Path.GetFileName(path), table);
                if (!found.TryGetValue(key, out var lines))
                    found[key] = lines = new List<int>();
                lines.Add(line);
            }
        }

        /* At most, not exactly: a change that moves a listed read to an archive view needs no list edit in the same
           merge, and a new bare read still fails. */
        var problems = found
            .Where(f => f.Value.Count > (BareOnPurpose.TryGetValue(f.Key, out var allowed) ? allowed : 0))
            .Select(f => $"{f.Key.File} reads {f.Key.Table} bare {f.Value.Count} time(s) at line(s) {string.Join(", ", f.Value)}; "
                + $"the list allows {(BareOnPurpose.TryGetValue(f.Key, out var n) ? n : 0)}")
            .ToList();

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /* The sweep passes when it finds nothing, so a mask or a pattern that stopped matching would pass it too. This
       runs the sweep's own reading over fixed source and checks the exact result. The real count has no lower
       bound on purpose: a change that moves a read to an archive view lowers it, in any merge order. */
    [Fact]
    public void TheSweepsReading_FindsSqlReads_AndSkipsCommentsAndViews()
    {
        const string source = """
            class Sample
            {
                const string Reads = @"
            SELECT * FROM wait_stats
            JOIN query_stats ON 1 = 1
            JOIN main.file_io_stats ON 1 = 1";
                // SELECT * FROM wait_stats
                /* SELECT * FROM wait_stats JOIN query_stats */
                const string Views = "SELECT * FROM v_wait_stats JOIN v_query_stats ON 1 = 1";
            }
            """;

        var expected = new List<(string Table, int Line)>
        {
            ("wait_stats", 4), ("query_stats", 5), ("file_io_stats", 6),
        };
        Assert.Equal(expected, BareReads(source));
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
