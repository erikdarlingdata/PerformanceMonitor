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
using DuckDB.NET.Native;

namespace PerformanceMonitorLite.Services;

public static class ParquetCompaction
{
    /* Production tuning for the compaction merge connections (#933).
       memoryLimit/threads/rowGroupSize are exposed as MergeBatchToFile parameters
       so tools/CompactionRepro can sweep them; production callers use the
       defaults below. */
    public const string DefaultMemoryLimit = "4GB";

    /* Memory limit of a sorted merge's connection (ServerOrderedTables, #9255) when the caller passes none. */
    public const string SortedMergeMemoryLimit = "512MB";
    public const int DefaultThreads = 2;

    /* Threads of a sorted merge's first attempt (#9320). On one install 7 sorted monthly merges ran out of
       memory at 512MB on two threads and fell back to the unsorted order, whose row groups all mix servers. On
       copies of those files one thread finished all 7, at a peak RSS of 139 to 537 MB. Generated files of the same
       row counts did not reproduce the failure on two threads, so the trigger is in the real files' values, not
       their size or row order. */
    public const int SortedMergeThreads = 1;
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
       a monthly, legacy or imported_ file of this table is still left alone, as before.

       query_stats and query_store_stats join it (#5410), for a different reason: their rows are small, but a
       month file holds every hourly merge's rows in time order across about 150 row groups, so a one-hour read
       (the Queries tab's default window) touched 15-20% of a month's row groups (median; 27 rows decoded per row
       wanted) and a day file touches about 4% (1.05 rows decoded per row wanted). Measured with
       tools/CompactionRepro on a copy of one laptop's hourly files, repeated for size (directional: one laptop,
       DuckDB 1.5.5): the day merge with no sort peaked at 1.16 GB at most and used no temp space; ORDER BY
       collection_time added nothing to the pruning and peaked at 4.2 GB, so the day merge has no sort, except
       query_store_stats' by server then time (ServerOrderedTables, #9255), which has no plan text to hold.
       A month file written before the switch, and an imported_ file, stay as they are: the per-cycle files that
       are waiting at the switch were never merged into the month file, so a day file and a month file never
       hold the same row (neither archive view dedups). */
    private static readonly HashSet<string> DailyCompactionTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "query_snapshots",
        "query_stats",
        "query_store_stats"
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

    /* Threads of a merge attempt: the sorted attempt runs on SortedMergeThreads (#9320), the unsorted merge, the
       unsorted fallback and the sliced export's merge keep ThreadsFor(table). */
    public static int ThreadsFor(string table, bool sortedAttempt) =>
        sortedAttempt ? SortedMergeThreads : ThreadsFor(table);

    /* ROW_GROUP_SIZE_BYTES of a daily table's merge output (#5393): a row group is flushed once its uncompressed
       bytes reach this, whatever its row count. The writer buffers about one row group of plan text, so this, not
       the input budget above, is what bounds the merge's peak (see the measurements there). 32 and 64 MiB peaked
       the same (x24, 64 MiB batches: 1.18 and 1.15 GB); 32 MiB makes smaller groups (341 against 311 for the
       157 MB input), which a time-windowed read prunes better. Usable only because the merge connection sets
       preserve_insertion_order = false (DuckDB 1.5.5 refuses it otherwise). query_store_stats' compaction merge no
       longer uses it (ServerOrderedTables, #9255): with an ORDER BY the bound is not honoured on every DuckDB
       version, and with plan text excluded a 2,048-row group of its rows is far under 32 MiB. The sliced export's
       merge still uses it. */
    public const long DailyRowGroupBytes = 32L * 1024 * 1024; /* 32 MiB */

    public static long RowGroupBytesFor(string table) => IsDailyTable(table) ? DailyRowGroupBytes : 0;

    /* On-disk parquet bytes per merge batch for a monthly table whose merge sorts by server (#9258). A batch's sort
       holds more of it than the streaming copy, so the sorted monthly merge gets its own smaller budget instead of
       DefaultBatchInputBytes: more _ptNNN parts, each sorted on its own and each holding one server per row group.
       Measured with tools/CompactionRepro (measure-sorted-monthly.sh) on 22 seeded hourly files, sorted merge at the
       512MB limit, peak working set (baseline about 60 MB included) and row groups that still mix servers:
         - 60-column index_object_stats shape (655 MB on disk): 32 MiB 474 MB, 88 of 1628 mixed;
           64 MiB 653 MB, 44 mixed; 100 MiB 647 MB, 32 mixed. Unsorted at 200 MB: 343 MB, every group mixed.
         - 37-column procedure_stats shape with seven 200-byte texts (241 MB): 32 MiB 605 MB, 44 of 649 mixed;
           64 MiB 623 MB, 20 mixed; 100 MiB 640 MB, 12 mixed. Unsorted at 200 MB: 304 MB, every group mixed.
       32 MiB is the one budget that stays near 600 MB on both shapes, so no table is taken out of the sorted set.
       The mixed groups left are the seams where a server's rows continue across two parts. */
    public const long SortedBatchInputBytes = 32L * 1024 * 1024; /* 32 MiB */

    /* The per-batch input budget of a table's compaction merge. The one rule, so the production merge and
       tools/CompactionRepro cannot drift apart: the daily budget for a table merged per day (query_snapshots,
       query_stats and query_store_stats, the last of which is also sorted and keeps its day budget as measured in
       #9255), the sorted monthly budget for any other server-ordered table, else the default. The three budgets
       are parameters so a caller with its own settings (ArchiveService's test-settable properties) passes them in. */
    public static long BatchInputBytesFor(
        string table,
        long defaultBytes = DefaultBatchInputBytes,
        long dailyBytes = DailyBatchInputBytes,
        long sortedBytes = SortedBatchInputBytes) =>
        IsDailyTable(table) ? dailyBytes
        : ServerOrderByFor(table) is null ? defaultBytes
        : sortedBytes;

    /* Tables whose compaction merge sorts by server, then by the table's time column (#9255): table -> time column.

       Every archive file mixed every server's rows in each row group (the archiver's COPY and a merge without a sort
       both keep the hot table's time order, which interleaves the servers), so a read filtered to one server decoded
       the other servers' rows too and the footer's server_id min/max could prune nothing. Sorted by server_id, then
       time, a group holds one server (the few at a boundary hold two), and a server-filtered read skips the rest.
       Measured with a seeded store of the field shape (5 servers sharing the rows 33/22.5/17/17/10.5%, hourly files as
       the archiver writes them, one day merged by compaction's own settings: 1 thread, 4 GB; DuckDB 1.5.6): 4 of 141
       groups mixed servers, against all 121 groups without the sort, and a one-server read of 8 such days took 2.5 s
       CPU against 5.3 s on the biggest server (1.2 s against 4.3 s on the smallest), with the same rows read.

       Memory cost, measured on a copy of a field store (query_store_stats day files at the 64 MiB batch budget, each
       its own batch): at a 4 GB limit the sorted merge's peak process memory was over three times the unsorted
       merge's. A 1 GB limit did not lower it. A 512MB limit (SortedMergeMemoryLimit) brings it to two to three times the
       unsorted peak, for one to two seconds more per merge. 512MB, not 250MB: 512MB was measured three times and 250MB
       once, and the hard-ceiling risk below grows as the limit drops. Rows and reads came out the same. At 512MB the
       sort spills a few hundred MB of temp files per merge, and the field day files all finished without the
       fallback below.

       Time spans: on field day files, which hold rows from many collection days, a sorted group spans LESS time than
       an unsorted one (the average fell from about a day or more to a few hours), so a narrow time window decodes no
       more groups. Only on a seeded single-day store did a group span more time (90 against 12 minutes on average, a
       24 h window over all servers read 1.39 s CPU against 1.09 s, one hour no change).

       The cap is a hard ceiling for the parquet writer (see MergeBatchToFile), and a sorted merge has no row-group byte
       bound, so a day of very long statement texts can fill a 2,048-row group past 512MB. MergeBatchToFile then reruns
       that batch unsorted at DefaultMemoryLimit with the byte bound, as the merge ran before #9255, and logs a warning.
       DuckDB's out-of-memory error fires quickly, before anything is spilled. Field compressibility does not reach
       this case, so the fallback guards against unusually long, repetitive statement texts or a DuckDB change.

       Which tables are sorted. At the daily level only query_store_stats: its merge reads no plan text
       (CompactionExcludeColumns drops query_plan_text), so the #5410 peak does not apply, and its day budget
       stays DailyBatchInputBytes. query_snapshots and query_stats carry plan XML, are merged per day and stay
       unsorted. At the monthly level every table listed below is sorted (#9258) and takes SortedBatchInputBytes
       (32 MiB) per batch, not the 200 MB default the unsorted monthly tables keep: a numeric month batch of
       100 MB peaked at 1.2 GB sorted against 290 MB unsorted, so a sorted batch needs its own smaller budget
       (see there for the measurements). A table not listed keeps its merge unsorted at the default budget. The
       archiver's hourly COPY is not sorted: at the field's cadence an hourly file is one row group, so the sort
       clusters nothing, and compaction rewrites the day's files in the same pass. */
    private static readonly Dictionary<string, string> ServerOrderedTables = BuildServerOrderedTables();

    private static Dictionary<string, string> BuildServerOrderedTables()
    {
        var tables = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["query_store_stats"] = "collection_time"
        };

        /* The monthly-merged tables (#9258): every table Lite stores and archives except the three merged per day
           above (query_snapshots and query_stats stay unsorted, see below). Each carries server_id and its own
           time column: capture_time for the config snapshots, alert_time for the alert log, collection_time for
           the rest. A table that is not named here keeps its merge unsorted, whatever it is called (the
           ArchiveService tests merge tables of other names with files that have no server_id). */
        foreach (var table in new[]
        {
            "ag_database_replica_states", "ag_replica_states", "agent_status", "attention_events",
            "blocked_process_reports", "collection_log", "cpu_scheduler_stats", "cpu_utilization_stats",
            "database_size_stats", "database_states", "deadlocks", "default_trace_events",
            "dmv_blocking_snapshots", "file_io_stats", "index_object_stats", "job_history", "latch_stats",
            "long_query_completions", "memory_clerks", "memory_grant_stats", "memory_pressure_events",
            "memory_stats", "perfmon_stats", "plan_cache_stats", "plan_correction", "procedure_stats",
            "pvs_stats", "running_jobs", "server_properties", "session_stats", "session_summary_stats",
            "spinlock_stats", "system_health_events", "tempdb_stats", "wait_stats", "waiting_tasks"
        })
        {
            tables[table] = "collection_time";
        }

        foreach (var table in new[]
        {
            "database_config", "database_scoped_config", "query_store_health", "server_config", "trace_flags"
        })
        {
            tables[table] = "capture_time";
        }

        tables["config_alert_log"] = "alert_time";
        return tables;
    }

    /* The ORDER BY list of the table's compaction merge, or null when the merge keeps no order. */
    public static string? ServerOrderByFor(string table) =>
        ServerOrderedTables.TryGetValue(table, out var timeColumn) ? $"server_id, {timeColumn}" : null;

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
       is merged one day at a time on a much smaller batch budget, and
       query_stats and query_store_stats are merged one day at a time so a
       one-hour read prunes, see DailyCompactionTables. The monthly tables
       that sort by server take the sorted budget, see BatchInputBytesFor.)

       Pragma tuning:
         - memory_limit = 4GB: parquet COPY makes allocations that bypass the
           buffer manager and can't spill; the cap is a hard ceiling, not a
           spill trigger. Paired with the batch budget (DefaultBatchInputBytes)
           so the working set stays under it. A sorted merge (ServerOrderedTables,
           #9255) runs at SortedMergeMemoryLimit (512MB) instead: the sort holds
           more of the batch than the streaming copy, and the lower limit makes
           it spill, which cut its peak from over three times the unsorted
           merge's to two to three times (see ServerOrderedTables). If that merge hits
           the ceiling anyway, the batch is rerun unsorted at 4GB.
         - threads = 2: fewer per-thread row-group buffers in flight. The sorted attempt runs on one (#9320).
         - preserve_insertion_order = false: lets DuckDB stream.
       The memoryLimit/threads/rowGroupSize parameters let tools/CompactionRepro
       sweep them (null memoryLimit means the table's default); production passes
       the defaults. */
    public static void MergeBatchToFile(
        string table,
        List<string> sourcePaths,
        string outputPath,
        string spillDirSql,
        string? memoryLimit = null,
        int? threads = null,
        int rowGroupSize = DefaultRowGroupSize,
        long? rowGroupBytes = null,
        Action<string, int, string>? onSortFallback = null)
    {
        var orderBy = ServerOrderByFor(table);
        try
        {
            RunMerge(table, sourcePaths, outputPath, spillDirSql, EffectiveMemoryLimit(table, memoryLimit),
                threads ?? ThreadsFor(table, sortedAttempt: orderBy is not null), rowGroupSize, rowGroupBytes, orderBy);
        }
        catch (DuckDBException ex) when (orderBy is not null && IsOutOfMemoryOrTempSpace(ex))
        {
            /* The cap is a hard ceiling for the parquet writer and a sorted merge has no row-group byte bound, so a
               batch of very long rows can pass it (#9255). Without this the day's compaction would fail on every
               pass and its hourly files would pile up. Rerun the batch as dev ran it before the sort: unsorted, at
               the default limit, with the byte bound, on a fresh connection (a failed instance may hold memory). */
            if (File.Exists(outputPath))
            {
                File.Delete(outputPath);
            }

            onSortFallback?.Invoke(table, sourcePaths.Count, ex.Message);
            RunMerge(table, sourcePaths, outputPath, spillDirSql, DefaultMemoryLimit,
                threads ?? ThreadsFor(table), rowGroupSize, rowGroupBytes, orderBy: null);
        }
    }

    /* The memory limit a merge of the table runs at: the caller's when it passes one, else the sorted merge's lower
       limit for a ServerOrderedTables table, else the default. */
    internal static string EffectiveMemoryLimit(string table, string? memoryLimit) =>
        memoryLimit ?? (ServerOrderByFor(table) is null ? DefaultMemoryLimit : SortedMergeMemoryLimit);

    /* DuckDB's out-of-memory error type, or the temp folder filling up (the sorted merge spills under its limit). */
    private static bool IsOutOfMemoryOrTempSpace(DuckDBException ex) =>
        ex.ErrorType == DuckDBErrorType.OutOfMemory || IsOutOfTempSpace(ex);

    /* True when a DuckDB failure is the temp folder filling up: its own cap (max_temp_directory_size) or the disk under
       it. Lives here so this file stays free of Lite types, because tools/CompactionRepro compiles it too. */
    internal static bool IsOutOfTempSpace(Exception ex)
    {
        var text = ex.ToString();
        return text.Contains("max_temp_directory_size", StringComparison.OrdinalIgnoreCase)
            || text.Contains("Out of Disk Space", StringComparison.OrdinalIgnoreCase)
            || text.Contains("not enough space", StringComparison.OrdinalIgnoreCase)
            || text.Contains("offload data block", StringComparison.OrdinalIgnoreCase);
    }

    /* Tests only: called with each merge attempt's output path, whether it sorts, and the thread count its pragma sets
       (#9320). Null in production, so the attempt pays one null check. */
    internal static Action<string, bool, int>? MergeAttemptObservedForTests { get; set; }

    private static void RunMerge(
        string table,
        List<string> sourcePaths,
        string outputPath,
        string spillDirSql,
        string memoryLimit,
        int threads,
        int rowGroupSize,
        long? rowGroupBytes,
        string? orderBy)
    {
        MergeAttemptObservedForTests?.Invoke(outputPath, orderBy is not null, threads);

        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using (var pragmaCmd = con.CreateCommand())
        {
            pragmaCmd.CommandText = BuildPragma(memoryLimit, threads, spillDirSql);
            pragmaCmd.ExecuteNonQuery();
        }

        /* No ORDER BY on this statement unless the table is in ServerOrderedTables (#9255). #5410 measured ORDER BY
           collection_time in the merge (4 to 6 GB peaks on the plan-heavy shape) and it will not be built for those
           tables. The daily tables' batch budget and row-group byte bound (DailyBatchInputBytes, DailyRowGroupBytes)
           were measured UNSORTED: a sort holds the whole batch, and DuckDB 1.5.5 did not honour ROW_GROUP_SIZE_BYTES
           under an ORDER BY inside the COPY (one 68,708-row group; 1.5.6 wrote the same 2,048-row groups with and
           without it, #9255), so a sorted merge drops the byte bound whatever the caller passed rather than lean on
           a DuckDB version. */
        var selectClause = BuildSelectClause(table, sourcePaths);
        var pathList = string.Join(", ", sourcePaths.Select(p => $"'{EscapeSqlPath(p)}'"));
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pathList}], union_by_name=true)" +
                          (orderBy is null ? "" : $" ORDER BY {orderBy}") + ") " +
                          $"TO '{EscapeSqlPath(outputPath)}' ({BuildArchiveCopyOptions(rowGroupSize, orderBy is null ? rowGroupBytes ?? RowGroupBytesFor(table) : 0)})";
        cmd.ExecuteNonQuery();
    }

    internal static string BuildPragma(string memoryLimit, int threads, string spillDirSql) =>
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
