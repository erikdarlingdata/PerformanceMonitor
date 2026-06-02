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
       so tools/CompactionRepro can sweep them; production callers pass the
       per-table values from BatchBudgetFor / RowGroupSizeFor below. */
    public const string DefaultMemoryLimit = "4GB";
    public const int DefaultThreads = 2;
    public const int DefaultRowGroupSize = 8192;

    /* Default on-disk parquet bytes per compaction merge batch. A group whose
       files exceed this budget is merged in multiple passes, each producing a
       _ptNNN.parquet output file. Narrow numeric tables compress only mildly, so
       on-disk bytes are a fine proxy for their merge memory cost. */
    public const long DefaultBatchInputBytes = 200L * 1024 * 1024; /* 200 MB */

    /* Per-table compaction overrides (#933).

       query_snapshots stores query-plan XML that expands ~30x on read, and that
       expansion is concentrated in a handful of very large values (real reporter
       data: query_plan p50 47 KB, p99 1.1 MB, max 27 MB). Two consequences:

         1. On-disk bytes badly under-count the merge's memory cost. The plan XML
            is extremely repetitive, so parquet dictionary-encodes it and ZSTD
            shrinks it ~30x on disk — but the merge has to *materialize* every
            value back into strings, and that materialized size is what blows the
            4 GB cap. (Note: parquet's own "uncompressed_size" footer stat is the
            decoded-but-still-dictionary-encoded size and is nearly as small as
            on disk, so it is NOT a usable proxy either — only the materialized
            VARCHAR byte count is.) Worse, the on-disk budget is *backwards* for
            this table: large files land one-per-batch (safe), while several small
            files get packed into one batch that concentrates the big plans (the
            case that OOM'd a fresh 202606 group of just 3 files). So this table
            budgets by *materialized* VARCHAR bytes instead — see
            GetMaterializedVarcharSizes and BudgetsByMaterializedSize.

         2. A large parquet row group buffers whole rows; a few multi-MB plans in
            one group force unspillable allocations (DuckDB upstream #16482). So
            this table uses a small ROW_GROUP_SIZE (512) to bound how many wide
            rows are in flight at once.

       Validated against the reporter's real data shape via tools/CompactionRepro.
       Numeric tables merge fine at the defaults and are intentionally untouched. */
    private static readonly Dictionary<string, (long BudgetBytes, int RowGroupSize, bool ByMaterialized)> PerTableCompaction = new()
    {
        ["query_snapshots"] = (1024L * 1024 * 1024, 512, true) /* 1 GB materialized, rgs 512 */
    };

    /* Batch budget for <paramref name="table"/> — the per-table override if one
       exists, otherwise the default. The unit is materialized VARCHAR bytes when
       BudgetsByMaterializedSize(table) is true, on-disk bytes otherwise. */
    public static long BatchBudgetFor(string table) =>
        PerTableCompaction.TryGetValue(table, out var c) ? c.BudgetBytes : DefaultBatchInputBytes;

    /* Output ROW_GROUP_SIZE for <paramref name="table"/> — the per-table override
       if one exists, otherwise the default. */
    public static int RowGroupSizeFor(string table) =>
        PerTableCompaction.TryGetValue(table, out var c) ? c.RowGroupSize : DefaultRowGroupSize;

    /* Whether <paramref name="table"/>'s batch budget is measured in materialized
       VARCHAR bytes rather than on-disk bytes. True only for wide-XML tables whose
       dictionary-encoded compression makes on-disk size a poor memory proxy. */
    public static bool BudgetsByMaterializedSize(string table) =>
        PerTableCompaction.TryGetValue(table, out var c) && c.ByMaterialized;

    /* Materialized (decoded-to-string) VARCHAR bytes per file — the sum of
       octet_length over every VARCHAR column. This is the right proxy for a
       merge's peak memory: it's the byte volume DuckDB must hold once the
       dictionary-encoded plan XML is expanded back into strings, which on-disk
       (and even parquet's footer "uncompressed_size") badly under-count. (#933)

       It costs one streaming read per file — seconds across a 100-file backlog,
       and run on a connection with no tight memory_limit so the read itself
       doesn't trip the very cap we're trying to stay under. A file that can't be
       read maps to its on-disk length as a conservative floor rather than
       throwing — compaction should degrade, not abort the archival cycle. */
    public static Dictionary<string, long> GetMaterializedVarcharSizes(IEnumerable<string> paths)
    {
        var sizes = new Dictionary<string, long>();
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        foreach (var p in paths)
        {
            try
            {
                var varcharCols = new List<string>();
                using (var describe = con.CreateCommand())
                {
                    describe.CommandText = $"DESCRIBE SELECT * FROM read_parquet('{EscapeSqlPath(p)}')";
                    using var dr = describe.ExecuteReader();
                    while (dr.Read())
                    {
                        if (dr.GetString(1).Contains("VARCHAR", StringComparison.OrdinalIgnoreCase))
                            varcharCols.Add(dr.GetString(0));
                    }
                }

                if (varcharCols.Count == 0)
                {
                    sizes[p] = FileLength(p);
                    continue;
                }

                var perRow = string.Join(" + ", varcharCols.Select(c =>
                    $"COALESCE(octet_length(encode(\"{c.Replace("\"", "\"\"")}\")), 0)"));
                using var cmd = con.CreateCommand();
                cmd.CommandText = $"SELECT COALESCE(SUM({perRow}), 0)::BIGINT FROM read_parquet('{EscapeSqlPath(p)}')";
                var val = cmd.ExecuteScalar();
                sizes[p] = val is null or DBNull ? FileLength(p) : Convert.ToInt64(val);
            }
            catch
            {
                sizes[p] = FileLength(p);
            }
        }
        return sizes;
    }

    private static long FileLength(string path) => new FileInfo(path.Replace("/", "\\")).Length;

    /* Columns to exclude during compaction — dead weight from legacy archives */
    private static readonly Dictionary<string, string[]> CompactionExcludeColumns = new()
    {
        ["query_store_stats"] = ["query_plan_text"]
    };

    private static string EscapeSqlPath(string path) => path.Replace("'", "''");

    /* Greedily group <paramref name="sortedPaths"/> (smallest-first) into batches
       whose total size doesn't exceed <paramref name="maxBytes"/>. A single file
       larger than the cap becomes its own one-element batch — that's the
       degenerate case (the cap can't split an individual file) and the caller
       handles it as a single-file pass-through merge.

       <paramref name="sizeOf"/> measures each file; it defaults to on-disk length.
       Wide-XML tables pass a map of materialized sizes (GetMaterializedVarcharSizes)
       so the budget reflects the merge's real memory cost, not its compressed size
       (#933). For a meaningful budget the caller should sort by the same metric. */
    public static List<List<string>> BuildSizeBudgetedBatches(
        IReadOnlyList<string> sortedPaths, long maxBytes, Func<string, long>? sizeOf = null)
    {
        sizeOf ??= FileLength;

        var batches = new List<List<string>>();
        var current = new List<string>();
        long currentBytes = 0;

        foreach (var p in sortedPaths)
        {
            var size = sizeOf(p);
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
       accumulator file every step) and OOM-prone. A sweep on the reporter's
       actual data confirmed a single COPY at the per-table row-group size is
       both faster and stays within the memory cap.

       Pragma tuning:
         - memory_limit = 4GB: parquet COPY makes allocations that bypass the
           buffer manager and can't spill; the cap is a hard ceiling, not a
           spill trigger. Pair it with the per-table batch budget (BatchBudgetFor)
           and row-group size (RowGroupSizeFor) so the working set stays under it.
         - threads = 2: fewer per-thread row-group buffers in flight.
         - preserve_insertion_order = false: lets DuckDB stream.
       The memoryLimit/threads/rowGroupSize parameters let tools/CompactionRepro
       sweep them; production passes the per-table values. */
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
