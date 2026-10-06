/*
 * ParquetCompaction — the parquet merge logic used by ArchiveService.CompactParquetFiles.
 *
 * Extracted into a standalone, dependency-free static class so the standalone
 * reproducer (tools/CompactionRepro) can link this exact source and exercise
 * the *real* production merge path. Before this extraction the reproducer kept
 * its own hand-copied merge loops, which silently drifted from production —
 * fixes "passed the repro" while still OOMing on real installs (see #933).
 *
 * This file must stay free of DI, logging, and project dependencies: it is
 * compiled into both the Lite assembly and the CompactionRepro assembly.
 */

using System.IO;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public static class ParquetCompaction
{
    /* Production tuning for the compaction merge connections (#933).
       memoryLimit/threads/rowGroupSize are exposed as MergeBatchToFile parameters
       so tools/CompactionRepro can sweep them; production callers use the
       defaults below. */
    public const string DefaultMemoryLimit = "4GB";
    public const int DefaultThreads = 2;
    public const int DefaultRowGroupSize = ArchiveRowGroupSize;

    /* Row-group size for every parquet COPY that writes a file the archive views read (#5381).

       DuckDB decodes about a row group's whole column chunk for any read that touches the
       column, whatever the filter. query_text is the wide one: a file written at the default
       122,880 rows is ONE row group, so even a 1 h window read decoded the whole file's text
       (measured on a 68,000-row/day Query Store archive: 335 MB of query_text for a 1 h window,
       against 73 MB at the 8,192 rows compaction used before this change and 17 MB at 2,048).

       Which files this reaches: the archiver runs hourly and writes one file per pass, and every pass ends
       by compacting the month's files, so in steady state the archive is month files that compaction wrote
       (8,192-row groups before this change) plus the newest hourly file. Only files written from now on
       get 2,048-row groups. Compaction rewrites a month only when that month receives a new hourly file, so
       past months keep their 8,192-row groups until retention removes them.

       Time pruning is weaker than the writer's shape suggests. The archiver's COPY keeps rows in
       collection_time order, so each of its groups covers a narrow span. Compaction output does not:
       it merges with preserve_insertion_order = false on 2 threads, so one group can span several hours
       (simulated over 168 hourly merges: 190 of 206 groups wider than 3 h). A 1 h window then touches about
       7% of a compacted file at 2,048 rows against about 27% at 8,192, a gain of about 4x in decoded text,
       not the near-exact pruning of a time-sorted file. Sorting compaction output by collection_time would
       tighten that, at the price of a sort (memory and temp disk) per merge (follow-up issue #5410).
       DuckDB 1.5.5's parallel writer can also place whole row groups in a different order from the source
       (seen from 4,096-row groups up; one group never moves relative to its own rows). Nothing in Lite reads by file
       position: the archive views dedup with an explicit ORDER BY collection_time, and pruning is by
       footer stats.

       2,048 is DuckDB's vector size: it flushes row groups on vector boundaries, so 1,024 writes
       the same 2,048-row groups. File size grew 1 to 3 percent against the default (measured 1.0 to 2.8% on
       the Query Store and query_stats shapes) and write time did not change measurably. ROW_GROUP_SIZE_BYTES
       was rejected: DuckDB 1.5.5 only accepts it with preserve_insertion_order = false (a global setting on
       the shared connection), the rows then come out of order across the scan's row groups (the views' dedup
       and pruning rely on time order), and with an ORDER BY inside the COPY the bound is not honoured at all
       (one 68,708-row group). */
    public const int ArchiveRowGroupSize = 2048;

    /* The one option list for archive COPYs; Lite.Tests' ArchiveCopyOptionsPinTests scans Lite/ so a
       new COPY cannot skip it. */
    public static string BuildArchiveCopyOptions(int rowGroupSize = ArchiveRowGroupSize) =>
        $"FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rowGroupSize}";

    public static readonly string ArchiveCopyOptions = BuildArchiveCopyOptions();

    /* On-disk parquet bytes per compaction merge batch. A group whose files
       exceed this budget is merged in multiple passes, each producing a
       _ptNNN.parquet output file. Parquet compresses only mildly for the numeric
       tables this runs on, so on-disk bytes are a fine proxy for merge memory. */
    public const long DefaultBatchInputBytes = 200L * 1024 * 1024; /* 200 MB */

    /* Tables compaction consolidates per DAY instead of per month (#5393, after #933).

       query_snapshots stores query-plan XML that expands ~30x on read, concentrated in a handful of
       multi-MB values (reporter data: query_plan p50 47 KB, p99 1.1 MB, max 27 MB). Merging it materializes
       gigabytes of strings, so the 200 MB per-batch budget the other tables use OOMs the compaction memory cap,
       and #933 skipped the table. That left one file per archive pass (up to about 24 a day, roughly 2,200 in
       the 3-month window), and every v_query_snapshots read opens each of them at bind.

       It is now merged into YYYYMMDD_query_snapshots.parquet (and _ptNNN parts), one day per group, with its own
       much smaller per-batch input budget (DailyBatchInputBytes) on one thread. Only the day shapes are merged:
       a monthly, legacy or imported_ file of this table is still left alone, as before. */
    private static readonly HashSet<string> DailyCompactionTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "query_snapshots"
    };

    /* Whether compaction merges <paramref name="table"/> one day per group rather than one month per group. */
    public static bool IsDailyTable(string table) => DailyCompactionTables.Contains(table);

    /* Whether compaction leaves the group (<paramref name="period"/>, <paramref name="table"/>) alone: a table
       that is merged per day has no monthly merge, so its 6-digit (YYYYMM) groups are skipped. A day group has
       an 8-digit period. */
    public static bool ShouldSkipCompaction(string table, string period) =>
        IsDailyTable(table) && period.Length != 8;

    /* On-disk parquet bytes per merge batch for a daily table, measured on a copy of a real store's
       query_snapshots files (DuckDB.NET in Lite.Tests, 4 GB memory_limit, one thread, peak process working set;
       directional, a small store):
         - real plans (p50 5.8 KB, max 1.1 MB): 8 MB of input peaked at 281 MB;
         - plan text repeated 8x (p50 46 KB, max 9 MB, about the reporter's p50): 200 MB peaked at 1.3 GB;
         - plan text repeated 24x (p99 about 1 MB, max 27 MB, the reporter's p99 and max but 3x its p50):
           8 MB peaked at 1.23 GB, 13 MB at 2.4 GB, 100 MB at 3.7 GB, so no budget in the hundreds of MB is safe.
       8 MiB keeps the worst measured batch under half the 4 GB cap. Peak memory follows the plan text a row
       group decodes, not the file bytes, so a file whose own size is over the budget is not merged by itself:
       CompactParquetFiles leaves it where it is. */
    public const long DailyBatchInputBytes = 8L * 1024 * 1024; /* 8 MiB */

    /* Threads for a daily table's merge: one thread roughly halves the row-group buffers in flight (24x
       plans, 6.5 MB: 2.0 GB on two threads, 1.3 GB on one) at about twice the time. */
    public static int ThreadsFor(string table) => IsDailyTable(table) ? 1 : DefaultThreads;

    /* Columns to exclude during compaction — dead weight from legacy archives */
    private static readonly Dictionary<string, string[]> CompactionExcludeColumns = new()
    {
        ["query_store_stats"] = ["query_plan_text"]
    };

    /* Deliberately a local copy, NOT a forward to DuckDbInitializer.EscapeSqlPath the way ArchiveService
       does: this file is compiled into tools/CompactionRepro as well (see the header), so it must stay free
       of project dependencies. The DataImportPathEscapingTests source pin covers all three call sites. */
    private static string EscapeSqlPath(string path) => path.Replace("'", "''");

    /* Greedily group <paramref name="sortedPaths"/> (smallest-first) into batches
       whose total on-disk bytes don't exceed <paramref name="maxBytes"/>. A single
       file larger than the cap becomes its own one-element batch — that's the
       degenerate case (the cap can't split an individual file) and the caller
       handles it as a single-file pass-through merge. */
    public static List<List<string>> BuildSizeBudgetedBatches(IReadOnlyList<string> sortedPaths, long maxBytes)
    {
        var batches = new List<List<string>>();
        var current = new List<string>();
        long currentBytes = 0;

        foreach (var p in sortedPaths)
        {
            var size = new FileInfo(p.Replace("/", "\\")).Length;
            if (currentBytes + size > maxBytes && current.Count > 0)
            {
                batches.Add(current);
                current = new List<string>();
                currentBytes = 0;
            }
            current.Add(p);
            currentBytes += size;
        }
        if (current.Count > 0)
        {
            batches.Add(current);
        }

        return batches;
    }

    /* Merge one size-budgeted batch into <paramref name="outputPath"/> with a
       single COPY over the whole batch. DuckDB streams the multi-file parquet
       scan straight into the writer — no growing accumulator, no re-reading
       re-packed row groups.

       #933 replaced an incremental pairwise merge here: on a real 100-file
       backlog the pairwise path was ~5x slower (it re-read an ever-larger
       accumulator file every step) and OOM-prone. A single COPY at the default
       row-group size is both faster and stays within the memory cap for the
       numeric tables this runs on. (query_snapshots, with its query-plan XML,
       is merged one day at a time on a much smaller batch budget — see
       DailyCompactionTables.)

       Pragma tuning:
         - memory_limit = 4GB: parquet COPY makes allocations that bypass the
           buffer manager and can't spill; the cap is a hard ceiling, not a
           spill trigger. Paired with the batch budget (DefaultBatchInputBytes)
           so the working set stays under it.
         - threads = 2: fewer per-thread row-group buffers in flight.
         - preserve_insertion_order = false: lets DuckDB stream.
       The memoryLimit/threads/rowGroupSize parameters let tools/CompactionRepro
       sweep them; production passes the defaults. */
    public static void MergeBatchToFile(
        string table,
        List<string> sourcePaths,
        string outputPath,
        string spillDirSql,
        string memoryLimit = DefaultMemoryLimit,
        int threads = DefaultThreads,
        int rowGroupSize = DefaultRowGroupSize)
    {
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using (var pragmaCmd = con.CreateCommand())
        {
            pragmaCmd.CommandText = BuildPragma(memoryLimit, threads, spillDirSql);
            pragmaCmd.ExecuteNonQuery();
        }

        var selectClause = BuildSelectClause(table, sourcePaths);
        var pathList = string.Join(", ", sourcePaths.Select(p => $"'{EscapeSqlPath(p)}'"));
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pathList}], union_by_name=true)) " +
                          $"TO '{EscapeSqlPath(outputPath)}' ({BuildArchiveCopyOptions(rowGroupSize)})";
        cmd.ExecuteNonQuery();
    }

    private static string BuildPragma(string memoryLimit, int threads, string spillDirSql) =>
        $"SET memory_limit = '{memoryLimit}'; SET threads = {threads}; " +
        $"SET preserve_insertion_order = false; SET temp_directory = '{EscapeSqlPath(spillDirSql)}';";

    /* Build the SELECT clause for a compaction COPY, excluding only the
       CompactionExcludeColumns actually present in THIS set of files.
       Detection must be per-merge-set, not global: archive files predating a
       schema change lack the column, so a globally-computed "* EXCLUDE (col)"
       fails the binder on a pair where neither file has it. query_plan_text
       was added to query_store_stats in migration v13 (2026-02-23), so a
       reporter's pre-v13 archives don't carry it. (#933) */
    private static string BuildSelectClause(string table, IReadOnlyList<string> paths)
    {
        if (!CompactionExcludeColumns.TryGetValue(table, out var excludeCols))
        {
            return "*";
        }

        using var schemaCon = new DuckDBConnection("DataSource=:memory:");
        schemaCon.Open();
        var pathList = string.Join(", ", paths.Select(p => $"'{EscapeSqlPath(p)}'"));
        using var schemaCmd = schemaCon.CreateCommand();
        schemaCmd.CommandText = $"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([{pathList}], union_by_name=true))";
        using var reader = schemaCmd.ExecuteReader();
        var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (reader.Read()) existingCols.Add(reader.GetString(0));

        var colsToExclude = excludeCols.Where(c => existingCols.Contains(c)).ToArray();
        return colsToExclude.Length > 0
            ? $"* EXCLUDE ({string.Join(", ", colsToExclude)})"
            : "*";
    }
}
