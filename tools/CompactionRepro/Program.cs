using System.Diagnostics;
using DuckDB.NET.Data;

/*
 * CompactionRepro — standalone reproducer for issue #933.
 *
 * Splits an existing monthly parquet file (like 202604_query_snapshots.parquet)
 * into N per-cycle-shaped chunks, then runs ArchiveService.CompactParquetFiles'
 * merge logic with knobs you can flip on the command line. The split chunks
 * have the exact row shape that caused the user's OOM in #933.
 *
 * Two merge strategies:
 *   accumulator (current prod) — sort smallest-first, fold each next file
 *                                into a growing accumulator. Peak input size
 *                                grows linearly with file count.
 *   tournament  (proposed fix) — pair files in rounds; each round halves the
 *                                file count. Each merge step's inputs stay
 *                                balanced. Final round still merges ~all the
 *                                data but every intermediate step is smaller.
 *
 * Usage:
 *   dotnet run -- --source-file <path> [options]
 *
 * Options:
 *   --source-file <path>     Required. Path to a monthly parquet file to split & merge.
 *   --strategy <str>         accumulator | tournament. Default: tournament
 *   --memory-limit <str>     DuckDB memory_limit (e.g. "1GB", "4GB"). Default: 1GB
 *   --threads <int>          DuckDB threads. 0 = DuckDB default. Default: 1
 *   --row-group-size <int>   Output ROW_GROUP_SIZE. Default: 8192
 *   --num-files <int>        Number of split chunks. Default: 15
 *   --keep                   Don't delete temp dir after run (for inspection)
 *
 * Examples:
 *   # Proposed fix: tournament merge, threads=1
 *   dotnet run -- --source-file "$LOCALAPPDATA/PerformanceMonitorLite/archive/202604_query_snapshots.parquet" \
 *                 --strategy tournament --threads 1
 *
 *   # Reproduce the current OOM: accumulator + threads=2 (matches prod after #942)
 *   dotnet run -- --source-file "$LOCALAPPDATA/PerformanceMonitorLite/archive/202604_query_snapshots.parquet" \
 *                 --strategy accumulator --threads 2
 *
 *   # Isolate the thread-count effect on the existing accumulator strategy
 *   dotnet run -- --source-file "$LOCALAPPDATA/PerformanceMonitorLite/archive/202604_query_snapshots.parquet" \
 *                 --strategy accumulator --threads 1
 */

var sourceFile = GetArg(args, "--source-file", "");
var mergeFilesArg = GetArg(args, "--merge-files", "");
var synthetic = args.Contains("--synthetic");
var syntheticRows = int.Parse(GetArg(args, "--synthetic-rows", "30000"));
var syntheticPlanKb = int.Parse(GetArg(args, "--synthetic-plan-kb", "100"));
if (string.IsNullOrEmpty(sourceFile) && string.IsNullOrEmpty(mergeFilesArg) && !synthetic)
{
    Console.Error.WriteLine("error: --source-file <path> OR --merge-files <a.parquet,...> OR --synthetic required");
    Console.Error.WriteLine("  --source-file:  split the given monthly parquet into chunks, then compact (full repro)");
    Console.Error.WriteLine("  --merge-files:  merge the given comma-separated files directly (skip split)");
    Console.Error.WriteLine("  --synthetic:    generate a query_snapshots-shaped source file (see --synthetic-rows, --synthetic-plan-kb)");
    return 2;
}
if (!string.IsNullOrEmpty(sourceFile) && !File.Exists(sourceFile))
{
    Console.Error.WriteLine($"error: source file not found: {sourceFile}");
    return 2;
}

var strategy = GetArg(args, "--strategy", "tournament").ToLowerInvariant();
if (strategy != "accumulator" && strategy != "tournament")
{
    Console.Error.WriteLine($"error: --strategy must be 'accumulator' or 'tournament', got '{strategy}'");
    return 2;
}
var dbMode = GetArg(args, "--db-mode", "memory").ToLowerInvariant();
if (dbMode != "memory" && dbMode != "file")
{
    Console.Error.WriteLine($"error: --db-mode must be 'memory' or 'file', got '{dbMode}'");
    return 2;
}
var memoryLimit = GetArg(args, "--memory-limit", "1GB");
var threads = int.Parse(GetArg(args, "--threads", "1"));
var rowGroupSize = int.Parse(GetArg(args, "--row-group-size", "8192"));
var numFiles = int.Parse(GetArg(args, "--num-files", "15"));
var cycles = int.Parse(GetArg(args, "--cycles", "1"));
var keep = args.Contains("--keep");

var tempDir = Path.Combine(Path.GetTempPath(), $"CompactionRepro_{Guid.NewGuid():N}");
Directory.CreateDirectory(tempDir);

var mergeFiles = string.IsNullOrEmpty(mergeFilesArg)
    ? new List<string>()
    : mergeFilesArg.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
foreach (var mf in mergeFiles)
{
    if (!File.Exists(mf))
    {
        Console.Error.WriteLine($"error: --merge-files entry not found: {mf}");
        return 2;
    }
}
if (synthetic)
{
    sourceFile = Path.Combine(tempDir, "synthetic_query_snapshots.parquet").Replace("\\", "/");
    Console.WriteLine($"Mode:     synthetic+split+merge");
    Console.WriteLine($"Synthetic: {syntheticRows} rows, ~{syntheticPlanKb} KB plan XML per row");
    Console.WriteLine($"Source:   {sourceFile} (will be generated)");
}
else if (mergeFiles.Count > 0)
{
    Console.WriteLine($"Mode:     merge-files (no split, isolate compaction)");
    Console.WriteLine($"Inputs:");
    foreach (var mf in mergeFiles)
        Console.WriteLine($"  {mf} ({new FileInfo(mf).Length / 1024.0 / 1024.0:F1} MB)");
}
else
{
    Console.WriteLine($"Mode:     split+merge (full repro)");
    Console.WriteLine($"Source:   {sourceFile} ({new FileInfo(sourceFile).Length / 1024.0 / 1024.0:F1} MB)");
}
Console.WriteLine($"Temp dir: {tempDir}");
/* Print engine version so we can correlate with standalone DuckDB CLI tests */
using (var versionCon = new DuckDBConnection("DataSource=:memory:"))
{
    versionCon.Open();
    using var versionCmd = versionCon.CreateCommand();
    versionCmd.CommandText = "SELECT version()";
    Console.WriteLine($"Engine:   DuckDB {versionCmd.ExecuteScalar()}");
}
Console.WriteLine($"Strategy: {strategy}");
Console.WriteLine($"DB mode:  {dbMode}");
Console.WriteLine($"Settings: memory_limit={memoryLimit}, threads={threads}, ROW_GROUP_SIZE={rowGroupSize}");
if (mergeFiles.Count == 0)
    Console.WriteLine($"Splitting source into {numFiles} chunks");
Console.WriteLine();

try
{
    if (synthetic)
    {
        Console.WriteLine($"[0/3] Generating synthetic source ({syntheticRows} rows, ~{syntheticPlanKb} KB plan/row)...");
        var sw = Stopwatch.StartNew();
        GenerateSyntheticSource(sourceFile, syntheticRows, syntheticPlanKb);
        sw.Stop();
        var size = new FileInfo(sourceFile).Length / 1024.0 / 1024.0;
        Console.WriteLine($"      Generated {size:F1} MB in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine();
    }

    List<string> sourcePaths;
    if (mergeFiles.Count > 0)
    {
        Console.WriteLine($"[1/3] Skipping split — using {mergeFiles.Count} provided files");
        sourcePaths = mergeFiles;
    }
    else
    {
        Console.WriteLine($"[1/3] Splitting source file into {numFiles} chunks...");
        var sw = Stopwatch.StartNew();
        sourcePaths = SplitSourceFile(sourceFile, tempDir, numFiles);
        sw.Stop();
        var totalSourceBytes = sourcePaths.Sum(p => new FileInfo(p).Length);
        Console.WriteLine($"      Wrote {sourcePaths.Count} files, {totalSourceBytes / 1024.0 / 1024.0:F1} MB total in {sw.ElapsedMilliseconds} ms");
    }
    Console.WriteLine();

    Console.WriteLine($"[2/3] Running pair-merge compaction (mirrors ArchiveService.CompactParquetFiles), {cycles} cycle(s)...");
    var spillDir = Path.Combine(tempDir, "duckdb_tmp").Replace("\\", "/");
    Directory.CreateDirectory(spillDir);

    var targetPath = Path.Combine(tempDir, "compacted.parquet").Replace("\\", "/");
    var process = Process.GetCurrentProcess();
    var startBytes = GC.GetTotalMemory(forceFullCollection: true);
    var startWorkingSet = process.WorkingSet64;
    Console.WriteLine($"      baseline working set (after GC): {startWorkingSet / 1024.0 / 1024.0:F0} MB");

    var compactionSw = Stopwatch.StartNew();
    var peakWorkingSet = startWorkingSet;
    long compactedFileBytes = 0;
    var perCycleWorkingSet = new List<(long peak, long postGc)>();
    var success = false;
    string? failureMessage = null;

    var stepCounter = 0;
    var intermediates = new List<string>();

    var perStepDbCounter = 0;
    void MergePair(string aPath, string bPath, string outPath, int stepIndex, int totalSteps)
    {
        string dataSource;
        string? dbFile = null;
        if (dbMode == "file")
        {
            /* Each merge gets its own fresh on-disk database so DuckDB has real
               paging room. Deleted after the step to avoid disk bloat. */
            dbFile = Path.Combine(tempDir, $"merge_{++perStepDbCounter}.duckdb").Replace("\\", "/");
            dataSource = $"DataSource={dbFile}";
        }
        else
        {
            dataSource = "DataSource=:memory:";
        }

        try
        {
            using var con = new DuckDBConnection(dataSource);
            con.Open();
            using (var pragma = con.CreateCommand())
            {
                var threadsClause = threads > 0 ? $"SET threads = {threads}; " : "";
                pragma.CommandText =
                    $"SET memory_limit = '{memoryLimit}'; " +
                    $"SET preserve_insertion_order = false; " +
                    $"SET temp_directory = '{spillDir.Replace("'", "''")}'; " +
                    threadsClause;
                pragma.ExecuteNonQuery();
            }

            var pairList = $"'{aPath.Replace("'", "''")}', '{bPath.Replace("'", "''")}'";
            using var cmd = con.CreateCommand();
            cmd.CommandText =
                $"COPY (SELECT * FROM read_parquet([{pairList}], union_by_name=true)) " +
                $"TO '{outPath.Replace("'", "''")}' " +
                $"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rowGroupSize})";
            cmd.ExecuteNonQuery();
        }
        finally
        {
            if (dbFile != null)
            {
                try { File.Delete(dbFile); } catch { }
                try { File.Delete(dbFile + ".wal"); } catch { }
            }
        }

        process.Refresh();
        if (process.WorkingSet64 > peakWorkingSet) peakWorkingSet = process.WorkingSet64;

        var aSize = new FileInfo(aPath).Length / 1024.0 / 1024.0;
        var bSize = new FileInfo(bPath).Length / 1024.0 / 1024.0;
        var outSize = new FileInfo(outPath).Length / 1024.0 / 1024.0;
        Console.WriteLine($"      step {stepIndex}/{totalSteps}: {aSize:F1} + {bSize:F1} -> {outSize:F1} MB | peak WS {peakWorkingSet / 1024.0 / 1024.0:F0} MB");
    }

    string NewIntermediatePath()
    {
        var p = targetPath + $".step{++stepCounter}.tmp";
        intermediates.Add(p);
        return p;
    }

    for (var cycle = 1; cycle <= cycles; cycle++)
    {
        if (cycles > 1) Console.WriteLine($"      --- cycle {cycle}/{cycles} ---");

        /* Reset per-cycle state */
        stepCounter = 0;
        intermediates.Clear();
        perStepDbCounter = 0;
        if (File.Exists(targetPath)) File.Delete(targetPath);

        var cycleStartPeak = peakWorkingSet;

        try
        {
            if (strategy == "accumulator")
            {
                /* Sort smallest-first like current production ArchiveService does */
                var sorted = sourcePaths
                    .OrderBy(p => new FileInfo(p).Length)
                    .ToList();

                var currentPath = sorted[0];
                var totalSteps = sorted.Count - 1;

                for (var i = 1; i < sorted.Count; i++)
                {
                    var isFinal = i == sorted.Count - 1;
                    var stepOutput = isFinal ? targetPath : NewIntermediatePath();

                    MergePair(currentPath, sorted[i], stepOutput, i, totalSteps);

                    if (i >= 2)
                    {
                        var prev = intermediates[^2];
                        try { File.Delete(prev); } catch { }
                    }

                    currentPath = stepOutput;
                }
            }
            else /* tournament */
            {
                var current = new List<string>(sourcePaths);
                var totalSteps = current.Count - 1;

                while (current.Count > 1)
                {
                    var next = new List<string>();
                    var pairs = current.Count / 2;

                    for (var i = 0; i < pairs; i++)
                    {
                        var aPath = current[i * 2];
                        var bPath = current[i * 2 + 1];
                        var isLastMerge = current.Count == 2;
                        var outPath = isLastMerge ? targetPath : NewIntermediatePath();

                        MergePair(aPath, bPath, outPath, stepCounter, totalSteps);
                        next.Add(outPath);

                        if (intermediates.Contains(aPath))
                        {
                            try { File.Delete(aPath); } catch { }
                        }
                        if (intermediates.Contains(bPath))
                        {
                            try { File.Delete(bPath); } catch { }
                        }
                    }

                    if (current.Count % 2 == 1)
                    {
                        next.Add(current[^1]);
                    }

                    current = next;
                }
            }

            compactedFileBytes = new FileInfo(targetPath).Length;
            success = true;
        }
        catch (Exception ex)
        {
            failureMessage = ex.Message;
            break;
        }

        /* Post-cycle measurement: force GC and sample working set after a brief
           pause to let native finalizers run. This is what the user cares about —
           does memory release between archival cycles? */
        var cyclePeak = peakWorkingSet;
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Thread.Sleep(500);
        process.Refresh();
        var postGc = process.WorkingSet64;
        perCycleWorkingSet.Add((cyclePeak - cycleStartPeak, postGc));
        if (cycles > 1)
            Console.WriteLine($"      cycle {cycle} peak +{(cyclePeak - cycleStartPeak) / 1024.0 / 1024.0:F0} MB | post-GC WS {postGc / 1024.0 / 1024.0:F0} MB");
    }
    compactionSw.Stop();

    process.Refresh();
    if (process.WorkingSet64 > peakWorkingSet) peakWorkingSet = process.WorkingSet64;

    Console.WriteLine();
    Console.WriteLine("[3/3] Result:");
    Console.WriteLine($"      Status:           {(success ? "SUCCESS" : "FAILURE")}");
    Console.WriteLine($"      Wall time:        {compactionSw.Elapsed.TotalSeconds:F2}s");
    Console.WriteLine($"      Baseline WS:      {startWorkingSet / 1024.0 / 1024.0:F0} MB");
    Console.WriteLine($"      Peak WS:          {peakWorkingSet / 1024.0 / 1024.0:F0} MB (+{(peakWorkingSet - startWorkingSet) / 1024.0 / 1024.0:F0} MB)");
    if (cycles > 1 && perCycleWorkingSet.Count > 0)
    {
        Console.WriteLine($"      Post-GC WS by cycle:");
        for (var i = 0; i < perCycleWorkingSet.Count; i++)
        {
            var (peak, postGc) = perCycleWorkingSet[i];
            Console.WriteLine($"        cycle {i + 1}: peak +{peak / 1024.0 / 1024.0:F0} MB, post-GC {postGc / 1024.0 / 1024.0:F0} MB");
        }
        var firstPostGc = perCycleWorkingSet[0].postGc;
        var lastPostGc = perCycleWorkingSet[^1].postGc;
        var drift = (lastPostGc - firstPostGc) / 1024.0 / 1024.0;
        Console.WriteLine($"      WS drift (last - first post-GC): {drift:+0;-0;0} MB");
    }
    if (success)
    {
        Console.WriteLine($"      Output size:      {compactedFileBytes / 1024.0 / 1024.0:F1} MB");

        /* Sanity check: row count round-trip — output must match inputs */
        var srcSqlList = mergeFiles.Count > 0
            ? string.Join(", ", mergeFiles.Select(p => $"'{p.Replace("'", "''").Replace("\\", "/")}'"))
            : $"'{sourceFile.Replace("'", "''").Replace("\\", "/")}'";
        using var verifyCon = new DuckDBConnection("DataSource=:memory:");
        verifyCon.Open();
        using var verifyCmd = verifyCon.CreateCommand();
        verifyCmd.CommandText =
            $"SELECT (SELECT COUNT(*) FROM read_parquet('{targetPath.Replace("'", "''")}')) AS out_rows, " +
            $"       (SELECT COUNT(*) FROM read_parquet([{srcSqlList}], union_by_name=true)) AS src_rows";
        using var verifyReader = verifyCmd.ExecuteReader();
        verifyReader.Read();
        var actualRows = verifyReader.GetInt64(0);
        var expectedRows = verifyReader.GetInt64(1);
        Console.WriteLine($"      Row count:        {actualRows} (expected {expectedRows}) {(actualRows == expectedRows ? "OK" : "MISMATCH")}");
    }
    else
    {
        Console.WriteLine($"      Failure:          {failureMessage}");
    }

    /* Spill dir size — non-zero means DuckDB spilled */
    var spillBytes = Directory.Exists(spillDir)
        ? Directory.GetFiles(spillDir, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length)
        : 0;
    Console.WriteLine($"      Spill on disk:    {spillBytes / 1024.0 / 1024.0:F1} MB ({(spillBytes > 0 ? "spilled" : "did not spill")})");

    return success ? 0 : 1;
}
finally
{
    if (!keep)
    {
        try { Directory.Delete(tempDir, recursive: true); } catch { }
    }
    else
    {
        Console.WriteLine();
        Console.WriteLine($"Temp dir retained: {tempDir}");
    }
}

static List<string> SplitSourceFile(string sourceFile, string outDir, int numChunks)
{
    /* Split a real monthly parquet into N chunks using row-number bucketing.
       Each chunk is written as ZSTD parquet (matching the production format).
       Empty chunks are skipped.
       NOTE: This connection deliberately runs with DuckDB defaults (no memory_limit).
       The merge connections set their own memory_limit. If you see the merge OOM
       with "X MiB / 953.6 MiB used" where the high-water mark looks like leftover
       state from this split, that's a clue DuckDB.NET shares buffer state across
       :memory: connections in the same process. */
    var sourceSql = sourceFile.Replace("'", "''").Replace("\\", "/");

    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();

    long totalRows;
    using (var countCmd = con.CreateCommand())
    {
        countCmd.CommandText = $"SELECT COUNT(*) FROM read_parquet('{sourceSql}')";
        totalRows = Convert.ToInt64(countCmd.ExecuteScalar());
    }
    Console.WriteLine($"      Source has {totalRows} rows; splitting into {numChunks} chunks");

    var paths = new List<string>();
    for (var i = 0; i < numChunks; i++)
    {
        var path = Path.Combine(outDir, $"src_{i:D3}.parquet").Replace("\\", "/");
        using var cmd = con.CreateCommand();
        cmd.CommandText =
            $"COPY (SELECT * FROM read_parquet('{sourceSql}') " +
            $"  WHERE (collection_id % {numChunks}) = {i}) " +
            $"TO '{path.Replace("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)";
        cmd.ExecuteNonQuery();
        if (new FileInfo(path).Length > 0) paths.Add(path);
    }
    return paths;
}

static string GetArg(string[] args, string key, string defaultValue)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] == key) return args[i + 1];
    return defaultValue;
}

static void GenerateSyntheticSource(string outputPath, int rows, int planKb)
{
    /* Generate query_snapshots-shaped parquet with high-entropy plan XML so
       ZSTD can't collapse content to a single dictionary entry. We aggregate
       a list of md5() hashes per row — each hash uses (collection_id, op_index)
       as a unique seed, defeating both per-row and cross-row compression.
       This matches the in-memory pain of real plan XML during merge. */
    var sourceSql = outputPath.Replace("'", "''").Replace("\\", "/");
    /* Each "op" tag is roughly '<op id="' + 32-char md5 + '"/>' = ~46 chars.
       Pick op count so the assembled XML hits the target byte size. */
    const int opTagBytes = 46;
    var opsPerPlan = Math.Max(4, (planKb * 1024) / opTagBytes);

    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();

    using var cmd = con.CreateCommand();
    /* list_aggregate builds the plan as the concatenation of {opsPerPlan}
       unique <op id="<md5>"/> tags. md5() is high-entropy and depends on the
       row index + op index, so the per-row content is irreducible. */
    cmd.CommandText = $@"
COPY (
    SELECT
        i AS collection_id,
        TIMESTAMP '2026-04-01 00:00:00' + INTERVAL (i) MINUTE AS collection_time,
        ((i % 4) + 1)::INTEGER AS server_id,
        ('Server' || ((i % 4) + 1)::VARCHAR) AS server_name,
        ((i % 200) + 50)::INTEGER AS session_id,
        ('db_' || ((i % 10) + 1)::VARCHAR) AS database_name,
        '00:00:00' AS elapsed_time_formatted,
        ('SELECT * FROM t_' || (i % 1000)::VARCHAR || ' WHERE c = ''' || md5(i::VARCHAR) || '''') AS query_text,
        ('<plan id=""' || i::VARCHAR || '"">' ||
         list_aggregate(
             list_transform(generate_series(1, {opsPerPlan}),
                            j -> '<op id=""' || md5((i::VARCHAR || ':' || j::VARCHAR)) || '""/>'),
             'string_agg', '') ||
         '</plan>') AS query_plan,
        ('<liveplan id=""' || i::VARCHAR || '"">' ||
         list_aggregate(
             list_transform(generate_series(1, {opsPerPlan}),
                            j -> '<op id=""' || md5(('L:' || i::VARCHAR || ':' || j::VARCHAR)) || '""/>'),
             'string_agg', '') ||
         '</liveplan>') AS live_query_plan,
        CASE (i % 5) WHEN 0 THEN 'running' WHEN 1 THEN 'suspended' WHEN 2 THEN 'sleeping' WHEN 3 THEN 'background' ELSE 'rollback' END AS status,
        CASE WHEN i % 7 = 0 THEN ((i % 200) + 1)::INTEGER ELSE NULL END AS blocking_session_id,
        CASE (i % 4) WHEN 0 THEN 'PAGEIOLATCH_SH' WHEN 1 THEN 'CXPACKET' WHEN 2 THEN 'LCK_M_S' ELSE NULL END AS wait_type,
        ((i * 13) % 5000)::BIGINT AS wait_time_ms,
        ('PAGE: 1:' || (i % 1000000)::VARCHAR) AS wait_resource,
        ((i * 17) % 60000)::BIGINT AS cpu_time_ms,
        ((i * 23) % 120000)::BIGINT AS total_elapsed_time_ms,
        ((i * 31) % 1000000)::BIGINT AS reads,
        ((i * 41) % 10000)::BIGINT AS writes,
        ((i * 43) % 5000000)::BIGINT AS logical_reads,
        ((i % 1000) / 100.0)::DECIMAL(18,2) AS granted_query_memory_gb,
        'READ_COMMITTED' AS transaction_isolation_level,
        ((i % 8) + 1)::INTEGER AS dop,
        ((i % 16) + 1)::INTEGER AS parallel_worker_count,
        ('login_' || (i % 50)::VARCHAR) AS login_name,
        ('HOST-' || (i % 20)::VARCHAR) AS host_name,
        ('Program_' || (i % 30)::VARCHAR) AS program_name,
        (i % 5)::INTEGER AS open_transaction_count,
        ((i % 100))::DECIMAL(5,2) AS percent_complete
    FROM generate_series(1, {rows}) t(i)
) TO '{sourceSql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)";
    cmd.ExecuteNonQuery();
}
