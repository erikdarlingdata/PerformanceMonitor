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
    public const int DefaultRowGroupSize = 8192;

    /* On-disk parquet bytes per compaction merge batch. A group whose files
       exceed this budget is merged in multiple passes, each producing a
       _ptNNN.parquet output file. Parquet compresses only mildly for the numeric
       tables this runs on, so on-disk bytes are a fine proxy for merge memory. */
    public const long DefaultBatchInputBytes = 200L * 1024 * 1024; /* 200 MB */

    /* Tables compaction skips entirely — best-effort (#933).

       query_snapshots stores query-plan XML that expands ~30x on read,
       concentrated in a handful of multi-MB values (reporter data: query_plan p50
       47 KB, p99 1.1 MB, max 27 MB). Merging it materializes gigabytes of strings
       and OOMs the compaction memory cap, and the parquet COPY pre-reserves memory
       in a way that batching can't get under (DuckDB upstream #16482) on memory-
       constrained hosts. It also barely compacts — its per-cycle files are already
       near the largest size that merges safely.

       Rather than retry a doomed multi-minute merge every archival cycle (and log
       an error each time), we skip it. Its per-cycle files are left in place and
       pruned by the normal retention sweep, so every plan is retained for the full
       retention window; only the monthly file-count consolidation is forgone for
       this one table. */
    private static readonly HashSet<string> SkipCompactionTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "query_snapshots"
    };

    /* Whether compaction skips <paramref name="table"/> entirely (best-effort). */
    public static bool ShouldSkipCompaction(string table) => SkipCompactionTables.Contains(table);

    /* Columns to exclude during compaction — dead weight from legacy archives */
    private static readonly Dictionary<string, string[]> CompactionExcludeColumns = new()
    {
        ["query_store_stats"] = ["query_plan_text"]
    };

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
       numeric tables this runs on. (Wide query-plan-XML tables can't be merged
       within the cap at all on constrained hosts and are skipped — see
       SkipCompactionTables.)

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
                          $"TO '{EscapeSqlPath(outputPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rowGroupSize})";
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
