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

    /* Row-group size of every UNSORTED parquet COPY that writes a file the archive views read (#5381): the per-cycle
       and reset exports, the unsorted day merges and the unsorted monthly tables. A merge that sorts by server, then
       time (ServerOrderedTables) writes SortedArchiveRowGroupSize, or fewer rows when they are wide, instead (#9407, #9455).

       2,048 was measured on unsorted files, where small groups are the only pruning there is: the rows of a group are
       in collection order, so a short window can skip only the groups it does not touch, and a group decodes about
       its whole column chunk. In a file sorted by server, then time, the footer stats of each group prune by server
       and by time, so a read skips the groups outside the servers and times it asks for, and small groups only add
       per-group overhead (many more groups to open, a larger footer, and a worse compression ratio).


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
       was rejected for the unsorted archive writer, and is used by the daily tables' merge (DailyRowGroupBytes,
       #5393), which already runs with preserve_insertion_order = false, and by the sorted merges (SortedRowGroupBytes,
       #9455): DuckDB 1.5.5 only accepts it with preserve_insertion_order = false (a global setting on
       the shared connection), the rows then come out of order across the scan's row groups (the views' dedup
       and pruning rely on time order), and with an ORDER BY inside the COPY 1.5.5 did not honour the bound at all
       (one 68,708-row group). DuckDB 1.5.6.1 honours it under an ORDER BY and keeps the file sorted. */
    public const int ArchiveRowGroupSize = 2048;

    /* Row-group size of a merge that sorts by server, then time (#9407): DuckDB's default. A day or month file of a
       sorted table is then a handful of groups, each one server's rows in time order, and the footer stats prune the
       rest. Measured on a real archive, the same sorted day files read much faster at this size than at 2,048, with
       identical replies, the same or lower peak memory and a fraction of the disk. */
    public const int SortedArchiveRowGroupSize = 122_880;

    /* ROW_GROUP_SIZE_BYTES of a merge that sorts (#9455): a row group is flushed at this many bytes or at
       SortedArchiveRowGroupSize rows, whichever comes first, so a narrow table keeps 122,880-row groups and a wide one
       gets the largest group that still fits the sorted merge's memory limit. The parquet writer buffers a whole row
       group in memory that does not spill, and 122,880 rows of long text can pass SortedMergeMemoryLimit.

       Measured with tools/CompactionRepro (measure-wide-row-groups.sh) on DuckDB 1.5.6.1, one thread, the 512MB sorted
       merge limit and a sorted COPY of seeded rows with long text that never repeats (one file each, 5 servers):
         - 560 bytes a row (45 numeric columns, 500-byte text): 122,880-row groups fit, and so does every bound up to 128 MiB.
         - 960 bytes a row (45 numeric columns, 900-byte text): 2,048 and 32,768 rows fit, 65,536 and up ran out of memory.
           24 and 32 MiB fit, 48 MiB and up ran out.
         - 942 bytes a row (6 numeric columns, 930-byte XML text): 98,304 rows fit, 122,880 ran out. Up to 96 MiB fit.
       The bound is honoured under the ORDER BY on this DuckDB (groups of about 20,000 to 29,000 rows at 24 MiB), and
       the file stays sorted: no server or time order inversions in a read-back, every row present. The comments in
       RunMerge record that 1.5.5 did not honour it, so a DuckDB bump that stops honouring it is caught by
       ArchiveSortedRowGroupSizeTests, not by a quiet return to the out-of-memory retry. 24 MiB, not the largest that
       fit: the memory cliff is sharp (a width that fits at 32 MiB ran out at 48) and a real archive's rows can be wider
       than the seeded ones. A table that still runs out is sorted again at ArchiveRowGroupSize (MergeBatchToFile). */
    public const long SortedRowGroupBytes = 24L * 1024 * 1024; /* 24 MiB */

    /* The temp spill of a sort, as a multiple of its input: #5410 measured about 4 and 11 times the input size. A re-cut
       starts only when the volume has room for its output plus this many times its source (#9407). */
    public const int RecutTempSpillMultiple = 11;

    /* A sorted table's file whose largest row group is under this many rows, and that has more than one group, was
       written at the old size and is cut again once (#9407). After the cut a file has one group or groups of
       SortedArchiveRowGroupSize, so it never matches again. */
    public const int RecutBelowRowGroupRows = 16_384;

    /* The one option list for archive COPYs; Lite.Tests' ArchiveCopyOptionsPinTests scans Lite/ so a
       new COPY cannot skip it. */
    public static string BuildArchiveCopyOptions(int rowGroupSize = ArchiveRowGroupSize, long rowGroupBytes = 0, bool stampSortedCut = false) =>
        $"FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rowGroupSize}" +
        (rowGroupBytes > 0 ? $", ROW_GROUP_SIZE_BYTES {rowGroupBytes}" : "") +
        (stampSortedCut ? $", KV_METADATA {{{SortedCutStampKey}: '{SortedCutRule}'}}" : "");

    /* The footer key a server-ordered write stamps its file with (#9409), and its value: the rule the file was cut under.
       The rule is a version plus the row and byte targets of a sorted cut, so a change to either target makes every
       stamped file one the re-cut picks up again, and a file stamped with the current rule is never cut again. A
       sorted write is stamped whatever size it ended at: the retry at 2,048 rows is as small as a sorted merge gets. */
    internal const string SortedCutStampKey = "pm_sorted_cut";
    internal static readonly string SortedCutRule = $"1:{SortedArchiveRowGroupSize}:{SortedRowGroupBytes}";

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
       collection_time added nothing to the pruning and peaked at 4.2 GB (on Lite query_stats rows, which carry no
       plan text either) and spilled temp files of about 4 and 11 times the input size, so the day merge had no sort.
       It has one now for query_store_stats (ServerOrderedTables, #9255) and query_stats (#9407), by server then time.
       What bounds that sort is the sorted-merge memory limit (SortedMergeMemoryLimit, 512MB) on one thread
       (SortedMergeThreads), which makes it spill instead of growing, and the free disk a re-cut asks for covers
       the larger of those spills (RecutTempSpillMultiple).
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

       The cap is a hard ceiling for the parquet writer (see MergeBatchToFile), so the writer's whole row group must fit
       in it. A sorted merge flushes a group at SortedRowGroupBytes as well as at SortedArchiveRowGroupSize rows (#9455),
       so a wide table gets the largest group that fits and a narrow one keeps 122,880 rows. Without the byte bound,
       122,880 rows of long texts ran out of memory on every merge of the table. At 2,048 rows a day of very long statement
       texts could pass 512MB. MergeBatchToFile then reruns the batch sorted at 2,048 rows, which keeps the #9255
       clustering, and only when that runs out too reruns it unsorted at DefaultMemoryLimit with the byte bound, as the
       merge ran before #9255. Each logs a warning
       that names which one ran. The re-cut keeps its own rule: a sort that runs out at the sorted size keeps the file.
       DuckDB's out-of-memory error fires quickly, before anything is spilled. Field compressibility does not reach
       this case, so the fallback guards against unusually long, repetitive statement texts or a DuckDB change.

       Which tables are sorted. At the daily level query_store_stats and query_stats. query_store_stats's merge reads
       no plan text (CompactionExcludeColumns drops query_plan_text). query_stats has a query_plan_xml column, but
       Lite's collector never sets CapturePlanXml (Darling does), so the column is NULL on every row Lite writes.
       That is not why query_stats can be sorted: #5410's 4.2 GB peak was measured on query_stats rows, which carry
       no plan text either. The sort is bounded by SortedMergeMemoryLimit (512MB) on one thread, as above (#9407).
       Both keep their day budget, DailyBatchInputBytes. query_snapshots carries plan XML, is merged per day and stays
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
            ["query_store_stats"] = "collection_time",
            ["query_stats"] = "collection_time"
        };

        /* The monthly-merged tables (#9258): every table Lite stores and archives except the three merged per day
           above (query_store_stats and query_stats are added above, query_snapshots stays unsorted). Each carries server_id and its own
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
        int? rowGroupSize = null,
        long? rowGroupBytes = null,
        Action<string, int, string>? onSortFallback = null,
        Action<string, int, string>? onSortedAtSmallGroups = null,
        Action<long>? onMemorySample = null)
    {
        var orderBy = ServerOrderByFor(table);
        var sortedThreads = threads ?? ThreadsFor(table, sortedAttempt: orderBy is not null);
        var sortedGroupRows = rowGroupSize ?? (orderBy is null ? ArchiveRowGroupSize : SortedArchiveRowGroupSize);
        try
        {
            RunMerge(table, sourcePaths, outputPath, spillDirSql, EffectiveMemoryLimit(table, memoryLimit),
                sortedThreads, sortedGroupRows, rowGroupBytes, orderBy, onMemorySample);
        }
        catch (DuckDBException ex) when (orderBy is not null && IsOutOfMemoryOrTempSpace(ex))
        {
            /* The cap is a hard ceiling for the parquet writer, so a batch of very long rows can pass it even with the
               sorted byte bound (SortedRowGroupBytes), and a batch the bound does not cover (a caller's rowGroupBytes of 0,
               or a DuckDB that ignores it) can pass it at the large size (#9255). Without a fallback the day's compaction would fail on every
               pass and its hourly files would pile up. The writer holds one whole row group, so the large sorted size
               (#9407) can pass the cap on a day of long statement texts where 2,048 rows did not: the first retry is
               the same sorted merge at 2,048 rows, which keeps the clustering. Only when that runs out too is the batch
               rerun as dev ran it before the sort: unsorted, at the default limit, with the byte bound. Each rerun is on
               a fresh connection (a failed instance may hold memory). */
            var error = ex;
            if (File.Exists(outputPath))
            {
                File.Delete(outputPath);
            }

            /* Only memory is cured by smaller groups. A temp-space failure would run the same sort and spill again at
               2,048 rows, so it goes straight to the unsorted fallback, which spills nothing (#9407). */
            if (RetriesAtSmallGroups(ex, sortedGroupRows))
            {
                try
                {
                    RunMerge(table, sourcePaths, outputPath, spillDirSql, EffectiveMemoryLimit(table, memoryLimit),
                        sortedThreads, ArchiveRowGroupSize, rowGroupBytes, orderBy, onMemorySample);
                    onSortedAtSmallGroups?.Invoke(table, sourcePaths.Count, ex.Message);
                    return;
                }
                catch (DuckDBException retryEx) when (IsOutOfMemoryOrTempSpace(retryEx))
                {
                    error = retryEx;
                    if (File.Exists(outputPath))
                    {
                        File.Delete(outputPath);
                    }
                }
            }

            onSortFallback?.Invoke(table, sourcePaths.Count, error.Message);
            RunMerge(table, sourcePaths, outputPath, spillDirSql, DefaultMemoryLimit,
                threads ?? ThreadsFor(table), rowGroupSize ?? ArchiveRowGroupSize, rowGroupBytes, orderBy: null, onMemorySample);
        }
    }

    /* Whether a failed sorted attempt is rerun sorted at 2,048 rows: only an out-of-memory failure at a larger size. */
    internal static bool RetriesAtSmallGroups(Exception failure, int attemptedGroupRows) =>
        attemptedGroupRows > ArchiveRowGroupSize && !IsOutOfTempSpace(failure);

    /* The one-file sorted merge of the re-cut (#9407): the file's rows, sorted by the table's server then time order,
       written at the sorted row-group size, with the same settings and sorted attempt as a compaction merge of the
       table. Returns false, with nothing left at the output, when the sort runs out of memory or temp space: the
       caller keeps the original rather than swap in the unsorted order the compaction merge falls back to. Any other
       failure throws. */
    internal static bool TryRecutToFile(
        string table, string sourcePath, string outputPath, string spillDirSql, string? memoryLimit = null, int? rowGroupSize = null,
        Action<long>? onMemorySample = null)
    {
        var orderBy = ServerOrderByFor(table) ?? throw new ArgumentException($"{table} is not merged in server order", nameof(table));
        try
        {
            RunMerge(table, [sourcePath], outputPath, spillDirSql, EffectiveMemoryLimit(table, memoryLimit),
                ThreadsFor(table, sortedAttempt: true), rowGroupSize ?? SortedArchiveRowGroupSize, rowGroupBytes: null, orderBy, onMemorySample);
            return true;
        }
        catch (DuckDBException ex) when (IsOutOfMemoryOrTempSpace(ex))
        {
            if (File.Exists(outputPath))
            {
                File.Delete(outputPath);
            }

            return false;
        }
    }

    /* Row groups and the largest one's row count of a parquet file, read from its footer. */
    internal static (long Groups, long MaxGroupRows) RowGroupShape(DuckDBConnection con, string path)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"SELECT count(DISTINCT row_group_id), coalesce(max(row_group_num_rows), 0) FROM parquet_metadata('{EscapeSqlPath(path)}')";
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return (Convert.ToInt64(reader.GetValue(0)), Convert.ToInt64(reader.GetValue(1)));
    }

    /* The uncompressed bytes of the largest row group of a parquet file, summed over its column chunks (footer only). */
    internal static long MaxRowGroupUncompressedBytes(DuckDBConnection con, string path)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"SELECT coalesce(max(group_bytes), 0) FROM (SELECT sum(total_uncompressed_size)::BIGINT AS group_bytes FROM parquet_metadata('{EscapeSqlPath(path)}') GROUP BY row_group_id)";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    /* A group this large, in uncompressed bytes, was cut by the byte bound: the bound's estimate and the footer's size of
       the same rows differ (measured 0.7 to 1.0 times, see SortedRowGroupBytes), so half the bound is a group the bound
       cut. */
    private const long RecutAtByteTarget = SortedRowGroupBytes / 2;

    /* Whether the file of a sorted table is cut again: more than one row group, none of them over RecutBelowRowGroupRows
       rows, and none that holds half the byte bound. A caller that cuts at a smaller size (tests) never re-cuts a file
       already at that size.

       The target of a file is its row-group size or the byte bound, whichever a cut would reach first (#9455). Rows of a
       wide table fill a group with far fewer than 16,384 rows, so a file whose largest group already holds the byte bound
       is at its target, a file whose target is 2,048 rows (rows of several KB) is never cut, and the cut never picks
       the file it wrote.

       The byte clause alone cannot tell that a file is the cut's own: the footer's size is after dictionary encoding, so
       a group the byte bound cut can read far under half the bound when the text repeats (a 4 KB statement text with a
       few hundred distinct values cut at 6,235 rows and 0.87 MB), and the same file would be sorted again on every pass
       (#9409). A file stamped with the current rule (SortedCutStampKey) is therefore never cut again. A file with no
       stamp or an old one is judged by the row and byte rule, so a file written before the stamp is cut at most once more. */
    internal static bool NeedsRecut(DuckDBConnection con, string path, int targetRows = SortedArchiveRowGroupSize)
    {
        var (groups, maxRows) = RowGroupShape(con, path);
        return NeedsRecut(groups, maxRows, () => SortedCutStamp(con, path), () => MaxRowGroupUncompressedBytes(con, path), targetRows);
    }

    /* The one rule, from a shape already read, so the store health layout read and the re-cut agree on which files are due
       (#9421). The stamp and the byte bound are read lazily: the row checks come first, so a file they already clear costs
       no footer read, and a file stamped with the current rule is never due whatever its byte size. */
    internal static bool NeedsRecut(long groups, long maxGroupRows, Func<string?> stamp, Func<long> maxGroupBytes, int targetRows = SortedArchiveRowGroupSize) =>
        groups > 1
        && maxGroupRows < Math.Min(RecutBelowRowGroupRows, targetRows)
        && !IsStampedWithCurrentRule(stamp())
        && maxGroupBytes() < RecutAtByteTarget;

    internal static bool IsStampedWithCurrentRule(string? stamp) =>
        string.Equals(stamp, SortedCutRule, StringComparison.Ordinal);

    /* The rule a file was stamped with, or null when its footer has none (key and value are BLOB). */
    internal static string? SortedCutStamp(DuckDBConnection con, string path)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"SELECT decode(value) FROM parquet_kv_metadata('{EscapeSqlPath(path)}') WHERE decode(key) = '{SortedCutStampKey}' LIMIT 1";
        return cmd.ExecuteScalar() as string;
    }

    internal static long RowCount(string path)
    {
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"SELECT count(*) FROM read_parquet('{EscapeSqlPath(path)}')";
        return Convert.ToInt64(cmd.ExecuteScalar());
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
        string? orderBy,
        Action<long>? onMemorySample = null)
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
           were measured UNSORTED: a sort holds the whole batch.

           A sorted merge writes SortedRowGroupBytes as its byte bound (#9455). DuckDB 1.5.5 did not honour
           ROW_GROUP_SIZE_BYTES under an ORDER BY inside the COPY (one 68,708-row group) and 1.5.6 wrote the same
           2,048-row groups with and without it (#9255), so the sorted merge dropped it. DuckDB 1.5.6.1 does honour it
           with preserve_insertion_order off, and the output stays sorted (see SortedRowGroupBytes). A caller that
           passes rowGroupBytes sets it, and 0 writes no bound. */
        var groupBytes = orderBy is null ? rowGroupBytes ?? RowGroupBytesFor(table) : rowGroupBytes ?? SortedRowGroupBytes;
        var selectClause = BuildSelectClause(table, sourcePaths);
        var pathList = string.Join(", ", sourcePaths.Select(p => $"'{EscapeSqlPath(p)}'"));
        using var cmd = con.CreateCommand();
        cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pathList}], union_by_name=true)" +
                          (orderBy is null ? "" : $" ORDER BY {orderBy}") + ") " +
                          $"TO '{EscapeSqlPath(outputPath)}' ({BuildArchiveCopyOptions(rowGroupSize, groupBytes, stampSortedCut: orderBy is not null)})";
        using (var sampler = onMemorySample is null ? null : new DuckDbMemorySampler(con, onMemorySample))
        {
            cmd.ExecuteNonQuery();
        }
    }

    /* The highest memory DuckDB reports for a merge's own database while the COPY runs (#9424). DuckDB keeps no peak for a
       statement, so a second connection to the same in-memory database reads duckdb_memory() every SampleMilliseconds (every FastSampleMilliseconds for the first 100 ms, and once more when the merge has ended) and hands
       each total to the callback, which keeps the highest. It is a sample: a spike shorter than the interval can be missed, and
       allocations the parquet writer makes outside DuckDB's buffer manager are not in the figure (see the memory_limit note on
       MergeBatchToFile). The process working set sampled beside it is the figure that sees those. Best effort: a sampler that
       cannot start or read reports nothing and never fails the merge. */
    internal sealed class DuckDbMemorySampler : IDisposable
    {
        public const int SampleMilliseconds = 50;
        private const int FastSampleMilliseconds = 2;
        private const int FastSamples = 50;

        private readonly System.Threading.ManualResetEventSlim _stop = new(false);
        private readonly DuckDBConnection? _probe;
        private readonly System.Threading.Thread? _thread;
        private readonly TimeSpan _joinTimeout;
        private int _handoff;

        /* Tests only (#9421): true once the probe connection and the stop event are disposed. */
        internal bool ResourcesDisposedForTests => System.Threading.Volatile.Read(ref _resourcesDisposed) == 1;
        private int _resourcesDisposed;

        public DuckDbMemorySampler(DuckDBConnection merge, Action<long> onSample, TimeSpan? joinTimeout = null)
        {
            _joinTimeout = joinTimeout ?? TimeSpan.FromSeconds(2);
            try
            {
                _probe = merge.Duplicate();
                _probe.Open();
            }
            catch (Exception)
            {
                _probe?.Dispose();
                _probe = null;
                return;
            }

            var probe = _probe;
            _thread = new System.Threading.Thread(() =>
            {
                try
                {
                    using var cmd = probe.CreateCommand();
                    cmd.CommandText = "SELECT coalesce(sum(memory_usage_bytes), 0)::BIGINT FROM duckdb_memory()";
                    void Sample() => onSample(Convert.ToInt64(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture));

                    /* A merge of a few thousand rows ends in a few milliseconds, before a 50 ms interval fires, and a read
                       taken before the COPY allocates sees 0 (#9421: the cut of a 12,000-row file reported no peak under
                       load). So the first reads come fast, and one more is taken once the merge has ended: DuckDB still
                       holds its parquet caches then, so a merge that ran always reports a figure above 0. */
                    var fast = 0;
                    Sample();
                    while (!_stop.Wait(fast++ < FastSamples ? FastSampleMilliseconds : SampleMilliseconds))
                    {
                        Sample();
                    }

                    Sample();
                }
                catch (Exception)
                {
                    /* A merge that finishes under the sampler, or a read that fails, ends the sampling. */
                }
                finally
                {
                    /* Whoever of this thread and Dispose arrives second disposes the probe: Dispose when the thread has already
                       gone, this thread when Dispose gave up waiting for it (#9421). */
                    if (System.Threading.Interlocked.Exchange(ref _handoff, 1) == 1)
                    {
                        DisposeResources();
                    }
                }
            }) { IsBackground = true, Name = "CompactionDuckDbMemory" };
            _thread.Start();
        }

        public void Dispose()
        {
            _stop.Set();
            if (_thread is null || System.Threading.Interlocked.Exchange(ref _handoff, 1) == 1)
            {
                DisposeResources();
                return;
            }

            /* The sampler may be inside ExecuteScalar on the native handle, so a probe disposed under it is a use after free.
               On a timeout the thread disposes the probe on its way out instead. */
            _thread.Join(_joinTimeout);

            /* No log line on a timeout: this file is also compiled on its own by tools/CompactionRepro, which has no AppLogger. */
        }

        private void DisposeResources()
        {
            _probe?.Dispose();
            _stop.Dispose();
            System.Threading.Volatile.Write(ref _resourcesDisposed, 1);
        }
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
