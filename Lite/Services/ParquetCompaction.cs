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
       was rejected for the archive writer, and is used only by the daily tables' merge (DailyRowGroupBytes,
       #5393), which already runs with preserve_insertion_order = false: DuckDB 1.5.5 only accepts it with preserve_insertion_order = false (a global setting on
       the shared connection), the rows then come out of order across the scan's row groups (the views' dedup
       and pruning rely on time order), and with an ORDER BY inside the COPY the bound is not honoured at all
       (one 68,708-row group). */
    public const int ArchiveRowGroupSize = 2048;

    /* The one option list for archive COPYs; Lite.Tests' ArchiveCopyOptionsPinTests scans Lite/ so a
       new COPY cannot skip it. */
    public static string BuildArchiveCopyOptions(int rowGroupSize = ArchiveRowGroupSize, long rowGroupBytes = 0) =>
        $"FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rowGroupSize}" +
        (rowGroupBytes > 0 ? $", ROW_GROUP_SIZE_BYTES {rowGroupBytes}" : "");

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

    /* On-disk parquet bytes per merge batch for a daily table (#5393). Measured with tools/CompactionRepro on a
       copy of a real store's query_snapshots files (directional: a small store, 28 files, 9.2 MB), written with
       the archive COPY options (2,048-row groups) and repeated until a run held 157 MB (plan text x24: query_plan
       p50 135 KB, p99 967 KB, max 25.7 MB, the reporter's p99 and max) or 270 MB (x8: p50 45 KB, max 8.6 MB).
       4 GB memory_limit, one thread, one fresh process per run, peak process working set:
         - x24, no row-group byte bound: 8 MiB batches peaked at 3.0 GB, 64 MiB batches ran out of memory;
         - x24 with DailyRowGroupBytes (32 MiB): batches of 8 / 16 / 32 / 64 MiB peaked at 1.17 / 1.15 / 1.26 /
           1.18 GB, so the budget no longer moves the peak: it levels off at about one buffered row group;
         - x8 with it: 0.50 / 0.51 / 0.52 / 0.53 GB; x8 without it: 1.3 GB at 8 MiB, 1.5 GB at 64 MiB.
       64 MiB is the largest budget measured; its worst peak (1.18 GB) is under 2 GB and under half the cap.
       Peak memory follows the plan text a row group decodes, not the file bytes, so a file whose own size is
       over the budget is not merged by itself: CompactParquetFiles leaves it where it is. */
    public const long DailyBatchInputBytes = 64L * 1024 * 1024; /* 64 MiB */

    /* Threads for a daily table's merge: one thread roughly halves the row-group buffers in flight (24x
       plans, 6.5 MB: 2.0 GB on two threads, 1.3 GB on one) at about twice the time. */
    public static int ThreadsFor(string table) => IsDailyTable(table) ? 1 : DefaultThreads;

    /* ROW_GROUP_SIZE_BYTES of a daily table's merge output (#5393): a row group is flushed once its uncompressed
       bytes reach this, whatever its row count. The writer buffers about one row group of plan text, so this, not
       the input budget above, is what bounds the merge's peak (see the measurements there). 32 and 64 MiB peaked
       the same (x24, 64 MiB batches: 1.18 and 1.15 GB); 32 MiB makes smaller groups (341 against 311 for the
       157 MB input), which a time-windowed read prunes better. Usable only because the merge connection sets
       preserve_insertion_order = false (DuckDB 1.5.5 refuses it otherwise). */
    public const long DailyRowGroupBytes = 32L * 1024 * 1024; /* 32 MiB */

    public static long RowGroupBytesFor(string table) => IsDailyTable(table) ? DailyRowGroupBytes : 0;

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
        int rowGroupSize = DefaultRowGroupSize,
        long? rowGroupBytes = null)
    {
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using (var pragmaCmd = con.CreateCommand())
        {
            pragmaCmd.CommandText = BuildPragma(memoryLimit, threads, spillDirSql);
            pragmaCmd.ExecuteNonQuery();
        }

        /* No ORDER BY on this statement, deliberately. #5410 measured ORDER BY collection_time in the merge (4 to
           6 GB peaks on the plan-heavy shape) and it will not be built. The daily tables' batch budget and
           row-group byte bound (DailyBatchInputBytes, DailyRowGroupBytes) were measured UNSORTED: a sort holds the
           whole batch, and DuckDB does not honour ROW_GROUP_SIZE_BYTES under an ORDER BY inside the COPY (one
           68,708-row group), so a future ORDER BY must leave a daily table out of the byte bound and budget
           unless both are measured again with it. */
        var selectClause = BuildSelectClause(table, sourcePaths);
        var pathList = string.Join(", ", sourcePaths.Select(p => $"'{EscapeSqlPath(p)}'"));
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pathList}], union_by_name=true)) " +
                          $"TO '{EscapeSqlPath(outputPath)}' ({BuildArchiveCopyOptions(rowGroupSize, rowGroupBytes ?? RowGroupBytesFor(table))})";
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
