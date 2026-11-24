using System.Diagnostics;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;

/*
 * CompactionRepro — standalone reproducer for issue #933.
 *
 * Runs the *real* production compaction merge code against a set of parquet
 * files and reports memory behavior + pass/fail.
 *
 * IMPORTANT: this tool does NOT reimplement the merge. It links the actual
 * production source (Lite/Services/ParquetCompaction.cs, via a <Compile Link>
 * in the csproj) and calls ParquetCompaction.BuildSizeBudgetedBatches +
 * MergeBatchToFile directly — the same calls ArchiveService.CompactParquetFiles
 * makes. Earlier versions of this repro kept hand-copied merge loops that
 * silently drifted from production; fixes "passed the repro" and still OOM'd on
 * real installs. If you change the merge algorithm, change it in
 * ParquetCompaction.cs and this tool picks it up automatically.
 *
 * Input sources (pick one):
 *   --source-file <path>   Split an existing monthly parquet into N per-cycle
 *                          chunks, then compact. Closest to a real backlog.
 *   --merge-files <a,b,..> Compact the given files (or every *.parquet in the given directory) directly (no split). Use this
 *                          to run against a reporter's actual archive files.
 *   --synthetic            Generate a query_snapshots-shaped source, then split.
 *
 * Every run first prints a read-only [profile] of the source data — the
 * uncompressed (in-memory) size distribution of each column. That profile is
 * the data shape that decides whether compaction OOMs; synthetic data can't
 * fake it. Use --profile-only to get just the profile and skip the merge.
 *
 * Tuning knobs (defaults = ParquetCompaction's non-table-specific defaults):
 *   --memory-limit <str>   DuckDB memory_limit per merge connection. Default: the table's production limit
 *   --threads <int>        DuckDB threads per merge connection. Default: the production pick
 *   --row-group-size <int> Output ROW_GROUP_SIZE. Default: production size of the table (2048, or 122880 for a table that merges in server order)
 *   --row-group-bytes-mb <int>  Output ROW_GROUP_SIZE_BYTES in MiB (0 = none). Default: the table's
 *                          ParquetCompaction.RowGroupBytesFor (#5393: the daily table's byte bound)
 *   (Absent --threads is the production pick: one thread for the sorted attempt, ThreadsFor(--table) for the fallback.)
 *   --max-batch-mb <int>   Per-batch on-disk input budget (MB). Default: the table's ParquetCompaction.BatchInputBytesFor
 *   --table <name>         Table name (drives exclude-column logic). Default: query_snapshots
 *
 * Seeded month (#9258): --seed-files N writes N hourly files of a numeric/text shape (instead of --source-file etc.)
 *   --seed-rows R (150000) --seed-servers S (5) --seed-numeric-cols C (50) --seed-text-cols T (10)
 *   --seed-text-bytes B (24) --seed-noise N (97: range of the random part of each numeric column) --seed-time-column <name> (collection_time). The report adds the row groups that mix
 *   servers, and a sorted merge that falls back to the unsorted order prints one [sort fallback] line.
 *
 * Other options:
 *   --profile-only         Print the source data profile and exit (no merge).
 *   --sweep                Run a matrix of configs (row-group-size x batch budget
 *                          x memory limit) against the same files and print which
 *                          ones survive. Ignores --row-group-size / --max-batch-mb.
 *   --num-files <int>      Chunks to split --source-file/--synthetic into. Default: 15
 *   --synthetic-rows <int> Synthetic row count. Default: 30000
 *   --synthetic-base-ops <n>  RelOps in a normal synthetic plan. Default: 130
 *   --synthetic-tail-ops <n>  RelOps in a tail (huge) synthetic plan. Default: 75000
 *   --synthetic-tail-every <n> Every Nth row gets a tail plan. Default: 500
 *   --copies <int>         Write N renamed copies of every input file so a small store can fill a large
 *                          batch budget. Default: 1 (inputs used as they are).
 *   --repeat-plans <int>   Rewrite every input file with each *plan* VARCHAR column repeated N times
 *                          (repeat(col, N)): real archive files compress about 37 to 1, so this builds
 *                          the reporter-sized plan text a small store lacks. Default: 1 (none).
 *                          Rewritten inputs use the archive COPY options (2,048-row groups), as
 *                          ArchiveService writes them, not DuckDB's 122,880-row default.
 *   --repeat-columns <a,b> The columns --repeat-plans repeats. Default: every VARCHAR column whose name
 *                          contains "plan" (query_plan and live_query_plan for query_snapshots).
 *   --keep                 Don't delete the temp dir after the run.
 *
 * Examples:
 *   # Just profile a reporter's real files — fast, read-only, no merge
 *   dotnet run -c Release -- --merge-files "C:/archive/a.parquet,C:/archive/b.parquet" --profile-only
 *
 *   # Run the real compaction against a reporter's actual failing files
 *   dotnet run -c Release -- --merge-files "C:/archive/a.parquet,C:/archive/b.parquet"
 *
 *   # Sweep configs against a reporter's real files to find one that survives
 *   dotnet run -c Release -- --merge-files "C:/archive/a.parquet,C:/archive/b.parquet" --sweep
 */

var sourceFile = GetArg(args, "--source-file", "");
var mergeFilesArg = GetArg(args, "--merge-files", "");
var synthetic = args.Contains("--synthetic");
var syntheticRows = int.Parse(GetArg(args, "--synthetic-rows", "30000"));
/* Defaults are tuned to the real issue-#933 query_snapshots profile:
   query_plan p50 ~47 KB, p99 ~1.1 MB, max ~27 MB, mean ~100 KB. */
var syntheticBaseOps = int.Parse(GetArg(args, "--synthetic-base-ops", "130"));
var syntheticTailOps = int.Parse(GetArg(args, "--synthetic-tail-ops", "75000"));
var syntheticTailEvery = int.Parse(GetArg(args, "--synthetic-tail-every", "500"));
if (string.IsNullOrEmpty(sourceFile) && string.IsNullOrEmpty(mergeFilesArg) && !synthetic && !args.Contains("--seed-files"))
{
    Console.Error.WriteLine("error: --source-file <path> OR --merge-files <a.parquet,...> OR --synthetic required");
    Console.Error.WriteLine("  --source-file:  split the given monthly parquet into chunks, then compact (full repro)");
    Console.Error.WriteLine("  --merge-files:  compact the given comma-separated files directly (skip split)");
    Console.Error.WriteLine("  --synthetic:    generate a query_snapshots-shaped source file (see --synthetic-rows, --synthetic-plan-kb)");
    return 2;
}
if (!string.IsNullOrEmpty(sourceFile) && !File.Exists(sourceFile))
{
    Console.Error.WriteLine($"error: source file not found: {sourceFile}");
    return 2;
}

var table = GetArg(args, "--table", "query_snapshots");
/* Absent means null, so MergeBatchToFile picks the table's production limit (512MB for the sorted merge). */
var memoryLimitArg = GetArg(args, "--memory-limit", "");
string? memoryLimit = memoryLimitArg.Length > 0 ? memoryLimitArg : null;
int? threads = args.Contains("--threads") ? int.Parse(GetArg(args, "--threads", "")) : null;
/* Absent means null, so MergeBatchToFile picks what production does: 122,880-row groups for a sorted table, and for its
   unsorted fallback 2,048-row groups with the table's byte bound. Passing a size here fixes it for the fallback too. */
int? rowGroupSize = args.Contains("--row-group-size") ? int.Parse(GetArg(args, "--row-group-size", "")) : null;
long? rowGroupBytes = args.Contains("--row-group-bytes-mb") ? long.Parse(GetArg(args, "--row-group-bytes-mb", "")) * 1024L * 1024L : null;
var copies = int.Parse(GetArg(args, "--copies", "1"));
var repeatPlans = int.Parse(GetArg(args, "--repeat-plans", "1"));
var repeatColumns = GetArg(args, "--repeat-columns", "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var maxBatchMb = int.Parse(GetArg(args, "--max-batch-mb", (ParquetCompaction.BatchInputBytesFor(table) / (1024 * 1024)).ToString()));
var maxBatchBytes = maxBatchMb * 1024L * 1024L;
var numFiles = int.Parse(GetArg(args, "--num-files", "15"));
/* A seeded month (#9258): N hourly files of a numeric/text shape the table's merge can sort, instead of a real store's files. */
var seedFiles = int.Parse(GetArg(args, "--seed-files", "0"));
var seedRows = int.Parse(GetArg(args, "--seed-rows", "150000"));
var seedServers = int.Parse(GetArg(args, "--seed-servers", "5"));
var seedNumericCols = int.Parse(GetArg(args, "--seed-numeric-cols", "50"));
var seedTextCols = int.Parse(GetArg(args, "--seed-text-cols", "10"));
var seedTextBytes = int.Parse(GetArg(args, "--seed-text-bytes", "24"));
var seedNoise = int.Parse(GetArg(args, "--seed-noise", "97"));
/* Distinct text values per file: 5,000 by default (compressible), the row count for text that never repeats. */
var seedTextDistinct = int.Parse(GetArg(args, "--seed-text-distinct", "5000"));
var seedTimeColumn = GetArg(args, "--seed-time-column", "collection_time");
var keep = args.Contains("--keep");
var profileOnly = args.Contains("--profile-only");
var sweep = args.Contains("--sweep");

var tempDir = Path.Combine(Path.GetTempPath(), $"CompactionRepro_{Guid.NewGuid():N}");
Directory.CreateDirectory(tempDir);

var mergeFiles = string.IsNullOrEmpty(mergeFilesArg)
    ? new List<string>()
    : Directory.Exists(mergeFilesArg)
        /* a directory: every parquet in it (a command line cannot carry hundreds of paths) */
        ? Directory.GetFiles(mergeFilesArg, "*.parquet").Order(StringComparer.Ordinal).ToList()
        : mergeFilesArg.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
foreach (var mf in mergeFiles)
{
    if (!File.Exists(mf))
    {
        Console.Error.WriteLine($"error: --merge-files entry not found: {mf}");
        return 2;
    }
}

if (seedFiles > 0)
{
    mergeFiles = GenerateSeededMonth(tempDir, seedFiles, seedRows, seedServers, seedNumericCols, seedTextCols, seedTextBytes, seedTimeColumn, seedNoise, seedTextDistinct);
}

/* #9455: does ROW_GROUP_SIZE_BYTES cap a sorted COPY on the DuckDB this build runs, and does the file stay sorted? One
   COPY with the ORDER BY kept at the production pragmas (preserve_insertion_order off, the sorted merge's limit and one
   thread), then the row groups, the order inversions and the row count read back. */
if (args.Contains("--probe-bytes-bound"))
{
    var probeMb = long.Parse(GetArg(args, "--probe-bytes-bound", "64"));
    var probeOut = Path.Combine(tempDir, "probe_out.parquet").Replace("\\", "/");
    var inList = string.Join(", ", mergeFiles.Select(f => $"'{f.Replace("'", "''").Replace("\\", "/")}'"));
    var probeOrderBy = ParquetCompaction.ServerOrderByFor(table) ?? throw new ArgumentException("table is not sorted");
    using var pcon = new DuckDBConnection("DataSource=:memory:");
    pcon.Open();
    void Exec(string sql) { using var c = pcon.CreateCommand(); c.CommandText = sql; c.ExecuteNonQuery(); }
    object? Scalar(string sql) { using var c = pcon.CreateCommand(); c.CommandText = sql; return c.ExecuteScalar(); }
    Directory.CreateDirectory(Path.Combine(tempDir, "spill"));
    Exec(ParquetCompaction.BuildPragma("512MB", 1, Path.Combine(tempDir, "spill").Replace("\\", "/")));
    Console.WriteLine($"Total: DuckDB {Scalar("SELECT version()")}, input {Scalar($"SELECT (sum(total_uncompressed_size) / (SELECT count(*) FROM read_parquet([{inList}], union_by_name=true)))::BIGINT FROM parquet_metadata([{inList}])")} uncompressed bytes/row");
    try
    {
        Exec($"COPY (SELECT * FROM read_parquet([{inList}], union_by_name=true) ORDER BY {probeOrderBy}) TO '{probeOut}' " +
             $"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {GetArg(args, "--probe-rows", "122880")}{(probeMb > 0 ? $", ROW_GROUP_SIZE_BYTES {probeMb * 1024 * 1024}" : "")})");
        var timeCol = probeOrderBy.Split(',')[1].Trim();
        var shape = ParquetCompaction.RowGroupShape(pcon, probeOut);
        var inversions = Scalar($"SELECT count(*) FROM (SELECT server_id, {timeCol}, lag(server_id) OVER w AS ps, lag({timeCol}) OVER w AS pt FROM read_parquet('{probeOut}', file_row_number = true) WINDOW w AS (ORDER BY file_row_number)) WHERE ps > server_id OR (ps = server_id AND pt > {timeCol})");
        Console.WriteLine($"Total: bytes bound {probeMb} MB with ORDER BY: {shape.Groups} row group(s), largest {shape.MaxGroupRows} rows, {inversions} order inversion(s), peak WS {System.Diagnostics.Process.GetCurrentProcess().PeakWorkingSet64 / 1048576} MB, {Scalar($"SELECT count(*) FROM read_parquet('{probeOut}')")} rows");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Total: bytes bound {probeMb} MB with ORDER BY failed: {ex.Message.Replace("\r", " ").Replace("\n", " ")[..Math.Min(160, ex.Message.Length)]}");
    }

    if (!keep) { try { Directory.Delete(tempDir, true); } catch { /* best effort */ } }
    return 0;
}

if (synthetic)
{
    sourceFile = Path.Combine(tempDir, "synthetic_query_snapshots.parquet").Replace("\\", "/");
    Console.WriteLine($"Mode:     synthetic+split+merge");
    Console.WriteLine($"Synthetic: {syntheticRows} rows, {syntheticBaseOps} base ops/plan, " +
                      $"{syntheticTailOps} ops on every {syntheticTailEvery}th row (heavy tail)");
    Console.WriteLine($"Source:   {sourceFile} (will be generated)");
}
else if (mergeFiles.Count > 0)
{
    Console.WriteLine($"Mode:     merge-files (no split — real files)");
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
using (var versionCon = new DuckDBConnection("DataSource=:memory:"))
{
    versionCon.Open();
    using var versionCmd = versionCon.CreateCommand();
    versionCmd.CommandText = "SELECT version()";
    Console.WriteLine($"Engine:   DuckDB {versionCmd.ExecuteScalar()}");
}
Console.WriteLine($"Code:     ParquetCompaction (linked from Lite/Services — real production merge)");
Console.WriteLine($"Table:    {table}");
if (sweep)
{
    Console.WriteLine($"Mode:     sweep (matrix of configs — see [sweep] section below)");
}
else
{
    Console.WriteLine($"Settings: memory_limit={memoryLimit ?? "default"}, threads={(threads?.ToString() ?? "production pick")}, ROW_GROUP_SIZE={(rowGroupSize?.ToString() ?? "production pick")}, ROW_GROUP_SIZE_BYTES={(rowGroupBytes is null ? "production pick" : rowGroupBytes > 0 ? rowGroupBytes / (1024 * 1024) + " MiB" : "none")}, max-batch={maxBatchMb} MB");
}
if (mergeFiles.Count == 0)
    Console.WriteLine($"Splitting source into {numFiles} chunks");
Console.WriteLine();

try
{
    if (synthetic)
    {
        Console.WriteLine($"[0/3] Generating synthetic source ({syntheticRows} rows, heavy-tailed plan XML)...");
        var sw = Stopwatch.StartNew();
        GenerateSyntheticSource(sourceFile, syntheticRows, syntheticBaseOps, syntheticTailOps, syntheticTailEvery);
        sw.Stop();
        var size = new FileInfo(sourceFile).Length / 1024.0 / 1024.0;
        Console.WriteLine($"      Generated {size:F1} MB in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine();
    }

    List<string> sourcePaths;
    if (mergeFiles.Count > 0)
    {
        Console.WriteLine($"[1/3] Skipping split — using {mergeFiles.Count} provided files");
        sourcePaths = mergeFiles.Select(p => p.Replace("\\", "/")).ToList();
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

    if (copies > 1 || repeatPlans > 1)
    {
        var rsw = Stopwatch.StartNew();
        sourcePaths = PrepareInputs(sourcePaths, tempDir, copies, repeatPlans, repeatColumns);
        Console.WriteLine($"      Prepared {sourcePaths.Count} input files ({copies} copies, plans x{repeatPlans}), " +
                          $"{sourcePaths.Sum(p => new FileInfo(p).Length) / 1024.0 / 1024.0:F1} MB on disk, in {rsw.ElapsedMilliseconds} ms");
        Console.WriteLine();
    }

    try
    {
        ProfileSource(sourcePaths);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[profile] scan failed (non-fatal): {ex.Message}");
    }
    Console.WriteLine();

    if (profileOnly)
    {
        Console.WriteLine("[profile-only] --profile-only set; skipping the merge. Done.");
        return 0;
    }

    /* Sort smallest-first — mirrors ArchiveService.CompactParquetFiles. */
    var sorted = sourcePaths
        .OrderBy(p => new FileInfo(p.Replace("/", "\\")).Length)
        .ToList();

    var spillDir = Path.Combine(tempDir, "duckdb_tmp").Replace("\\", "/");
    Directory.CreateDirectory(spillDir);
    var process = Process.GetCurrentProcess();

    /* Run one full compaction (size-budget batching + per-batch merge) for a
       given config. Never throws — a merge OOM is caught and returned as
       Ok=false so a sweep can carry on to the next config. */
    (bool Ok, double StartMb, double PeakMb, double Sec, int Batches, string? Fail, List<string> Outputs)
        RunOneConfig(int? rgs, long batchBytes, string? memLimit, int? thr, bool verbose)
    {
        var batches = ParquetCompaction.BuildSizeBudgetedBatches(sorted, batchBytes);
        if (verbose)
        {
            Console.WriteLine($"      {sorted.Count} files -> {batches.Count} batch(es) at {batchBytes / 1024 / 1024} MB budget");
            for (var i = 0; i < batches.Count; i++)
            {
                var bb = batches[i].Sum(p => new FileInfo(p.Replace("/", "\\")).Length);
                Console.WriteLine($"        batch {i + 1}: {batches[i].Count} files, {bb / 1024.0 / 1024.0:F1} MB on disk");
            }
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        process.Refresh();
        var startWs = process.WorkingSet64;

        /* Background sampler — polls working set during the (opaque) merge calls
           so the reported peak reflects mid-merge pressure. */
        long peakWs = startWs;
        var stop = false;
        var sampler = new Thread(() =>
        {
            var p = Process.GetCurrentProcess();
            while (!Volatile.Read(ref stop))
            {
                p.Refresh();
                var ws = p.WorkingSet64;
                if (ws > peakWs) peakWs = ws;
                Thread.Sleep(75);
            }
        }) { IsBackground = true };
        sampler.Start();

        var sw = Stopwatch.StartNew();
        var outputs = new List<string>();
        var ok = false;
        string? fail = null;
        try
        {
            for (var i = 0; i < batches.Count; i++)
            {
                var outName = batches.Count == 1
                    ? $"out_{table}.parquet"
                    : $"out_{table}_pt{i + 1:D3}.parquet";
                var outPath = Path.Combine(tempDir, outName).Replace("\\", "/");
                if (File.Exists(outPath)) File.Delete(outPath);

                var bsw = Stopwatch.StartNew();
                ParquetCompaction.MergeBatchToFile(table, batches[i], outPath, spillDir, memLimit, thr, rgs, rowGroupBytes,
                    onSortFallback: (fallbackTable, files, error) => Console.WriteLine(
                        $"        [sort fallback] {fallbackTable}: the server-ordered merge of {files} file(s) ran out of memory or temp space, " +
                        $"so this batch fell back to the unsorted order: {(error.Length > 160 ? error[..160] + "..." : error).Replace("\r", " ").Replace("\n", " ")}"),
                    onSortedAtSmallGroups: (retryTable, files, error) => Console.WriteLine(
                        $"        [sorted at {ParquetCompaction.ArchiveRowGroupSize} rows] {retryTable}: the server-ordered merge of {files} file(s) ran out of memory or temp space at the large row-group size, " +
                        $"so this batch was sorted again at {ParquetCompaction.ArchiveRowGroupSize}-row groups: {(error.Length > 160 ? error[..160] + "..." : error).Replace("\r", " ").Replace("\n", " ")}"));
                bsw.Stop();

                process.Refresh();
                if (process.WorkingSet64 > peakWs) peakWs = process.WorkingSet64;
                outputs.Add(outPath);
                if (verbose)
                {
                    var os = new FileInfo(outPath).Length / 1024.0 / 1024.0;
                    Console.WriteLine($"        batch {i + 1}/{batches.Count}: {batches[i].Count} files -> {os:F1} MB " +
                                      $"in {bsw.Elapsed.TotalSeconds:F1}s | peak WS {peakWs / 1024.0 / 1024.0:F0} MB");
                }
            }
            ok = true;
        }
        catch (Exception ex)
        {
            fail = ex.Message.Replace("\r", " ").Replace("\n", " ").Trim();
            if (fail.Length > 200) fail = fail[..200] + "...";
        }
        sw.Stop();
        stop = true;
        sampler.Join(1000);
        process.Refresh();
        if (process.WorkingSet64 > peakWs) peakWs = process.WorkingSet64;

        return (ok, startWs / 1024.0 / 1024.0, peakWs / 1024.0 / 1024.0,
                sw.Elapsed.TotalSeconds, batches.Count, fail, outputs);
    }

    void CleanOutputs()
    {
        foreach (var f in Directory.GetFiles(tempDir, $"out_{table}*"))
        {
            try { File.Delete(f); } catch { /* best effort */ }
        }
    }

    if (sweep)
    {
        /* Matrix of merge configs run against the same source files — varies
           row-group size, batch budget and memory cap to find which combination
           keeps the merge within the cap on real data. (#933) */
        var configs = new (string Label, int Rgs, int BatchMb, string Mem)[]
        {
            ("rgs=8192 batch=200 mem=4GB", 8192, 200, "4GB"),
            ("rgs=2048 batch=100 mem=4GB", 2048, 100, "4GB"),
            ("rgs=512  batch=50  mem=4GB",  512,  50, "4GB"),
            ("rgs=512  batch=25  mem=4GB",  512,  25, "4GB"),
            ("rgs=256  batch=25  mem=2GB",  256,  25, "2GB"),
        };

        Console.WriteLine($"[sweep] Running {configs.Length} configs against the same {sorted.Count} source files.");
        Console.WriteLine();

        var results = new List<(string Label, bool Ok, double PeakDeltaMb, double Sec, int Batches, string? Fail)>();
        foreach (var c in configs)
        {
            Console.WriteLine($"[sweep] {c.Label} ...");
            var r = RunOneConfig(c.Rgs, c.BatchMb * 1024L * 1024L, c.Mem, threads, verbose: false);
            CleanOutputs();
            var peakDelta = r.PeakMb - r.StartMb;
            results.Add((c.Label, r.Ok, peakDelta, r.Sec, r.Batches, r.Fail));
            Console.WriteLine($"        {(r.Ok ? "OK  " : "FAIL")}  peak +{peakDelta:F0} MB  {r.Sec:F0}s  {r.Batches} part file(s)");
            if (!r.Ok) Console.WriteLine($"        {r.Fail}");
            Console.WriteLine();
        }

        Console.WriteLine("[sweep] SUMMARY");
        Console.WriteLine($"  {"config",-38} {"status",-6} {"peak",10} {"time",7} {"parts",6}");
        foreach (var r in results)
            Console.WriteLine($"  {r.Label,-38} {(r.Ok ? "OK" : "FAIL"),-6} {("+" + r.PeakDeltaMb.ToString("F0") + " MB"),10} {(r.Sec.ToString("F0") + "s"),7} {r.Batches,6}");
        Console.WriteLine();
        var firstOk = results.FirstOrDefault(r => r.Ok);
        Console.WriteLine(firstOk.Label != null
            ? $"  First config that survives this data: {firstOk.Label}"
            : "  No config survived — every merge OOM'd. Paste this whole output back.");
        return results.Any(r => r.Ok) ? 0 : 1;
    }

    /* Single-config run. */
    Console.WriteLine($"[3/3] Running merge (real ParquetCompaction code)...");
    var single = RunOneConfig(rowGroupSize, maxBatchBytes, memoryLimit, threads, verbose: true);

    Console.WriteLine();
    Console.WriteLine("Result:");
    Console.WriteLine($"      Status:           {(single.Ok ? "SUCCESS" : "FAILURE")}");
    Console.WriteLine($"      Wall time:        {single.Sec:F2}s");
    Console.WriteLine($"      Peak WS:          {single.PeakMb:F0} MB (baseline {single.StartMb:F0} MB, +{single.PeakMb - single.StartMb:F0} MB)");
    if (single.Ok)
    {
        var outBytes = single.Outputs.Sum(p => new FileInfo(p).Length);
        Console.WriteLine($"      Output:           {single.Outputs.Count} part file(s), {outBytes / 1024.0 / 1024.0:F1} MB total");
        Console.WriteLine($"      Output row groups: {CountRowGroups(single.Outputs)}  (input row groups: {CountRowGroups(sourcePaths)})");
        Console.WriteLine($"      Row groups mixing servers: {MixedServerRowGroups(single.Outputs)}  (input: {MixedServerRowGroups(sourcePaths)})");

        /* Row-count round-trip: total output rows must equal total source rows. */
        var srcSqlList = string.Join(", ", sourcePaths.Select(p => $"'{p.Replace("'", "''").Replace("\\", "/")}'"));
        var outSqlList = string.Join(", ", single.Outputs.Select(p => $"'{p.Replace("'", "''").Replace("\\", "/")}'"));
        using var verifyCon = new DuckDBConnection("DataSource=:memory:");
        verifyCon.Open();
        using var verifyCmd = verifyCon.CreateCommand();
        verifyCmd.CommandText =
            $"SELECT (SELECT COUNT(*) FROM read_parquet([{outSqlList}], union_by_name=true)) AS out_rows, " +
            $"       (SELECT COUNT(*) FROM read_parquet([{srcSqlList}], union_by_name=true)) AS src_rows";
        using var verifyReader = verifyCmd.ExecuteReader();
        verifyReader.Read();
        var actualRows = verifyReader.GetInt64(0);
        var expectedRows = verifyReader.GetInt64(1);
        Console.WriteLine($"      Row count:        {actualRows} (expected {expectedRows}) {(actualRows == expectedRows ? "OK" : "MISMATCH")}");
    }
    else
    {
        Console.WriteLine($"      Failure:          {single.Fail}");
    }

    var spillBytes = Directory.Exists(spillDir)
        ? Directory.GetFiles(spillDir, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length)
        : 0;
    Console.WriteLine($"      Spill on disk:    {spillBytes / 1024.0 / 1024.0:F1} MB ({(spillBytes > 0 ? "spilled" : "did not spill")})");

    return single.Ok ? 0 : 1;
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
       Each chunk is written as ZSTD parquet matching the production per-cycle
       archive format (ArchiveService writes per-cycle files with FORMAT PARQUET,
       COMPRESSION ZSTD and ArchiveRowGroupSize, 2,048 rows, since #5381). Empty chunks are
       skipped. This connection runs with DuckDB defaults (no memory_limit) — the
       merge connections set their own via ParquetCompaction. */
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
            $"TO '{path.Replace("'", "''")}' ({ParquetCompaction.ArchiveCopyOptions})";
        cmd.ExecuteNonQuery();
        if (new FileInfo(path).Length > 0) paths.Add(path);
    }
    return paths;
}

/* Writes `copies` renamed copies of each input, each with every plan VARCHAR column repeated
   `repeatPlans` times, using the archive COPY options. */
static List<string> PrepareInputs(List<string> inputs, string outDir, int copies, int repeatPlans, string[] repeatColumns)
{
    var result = new List<string>();
    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();
    var n = 0;
    foreach (var input in inputs)
    {
        var inSql = input.Replace("\\", "/").Replace("'", "''");
        var select = "*";
        if (repeatPlans > 1)
        {
            var cols = new List<string>();
            using (var d = con.CreateCommand())
            {
                d.CommandText = $"DESCRIBE SELECT * FROM read_parquet('{inSql}')";
                using var dr = d.ExecuteReader();
                while (dr.Read())
                    if (dr.GetString(1).Contains("VARCHAR", StringComparison.OrdinalIgnoreCase)
                        && (repeatColumns.Length > 0
                            ? repeatColumns.Contains(dr.GetString(0), StringComparer.OrdinalIgnoreCase)
                            : dr.GetString(0).Contains("plan", StringComparison.OrdinalIgnoreCase)))
                        cols.Add(dr.GetString(0));
            }
            if (cols.Count > 0)
                select = $"* REPLACE ({string.Join(", ", cols.Select(c => $"repeat(\"{c}\", {repeatPlans}) AS \"{c}\""))})";
        }
        for (var c = 0; c < copies; c++)
        {
            var path = Path.Combine(outDir, $"in_{n++:D4}.parquet").Replace("\\", "/");
            using var cmd = con.CreateCommand();
            cmd.CommandText = $"COPY (SELECT {select} FROM read_parquet('{inSql}')) TO '{path.Replace("'", "''")}' ({ParquetCompaction.ArchiveCopyOptions})";
            cmd.ExecuteNonQuery();
            result.Add(path);
        }
    }
    return result;
}

/* "mixed of total" row groups whose server_id min and max differ (the sort's goal is one server per group), or
   "n/a" when the files carry no server_id. */
static string MixedServerRowGroups(List<string> paths)
{
    try
    {
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        var list = string.Join(", ", paths.Select(p => $"'{p.Replace("'", "''").Replace("\\", "/")}'"));
        using var cmd = con.CreateCommand();
        cmd.CommandText = "SELECT count(*), count(*) FILTER (WHERE stats_min_value <> stats_max_value) " +
                          $"FROM parquet_metadata([{list}]) WHERE path_in_schema = 'server_id'";
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return $"{reader.GetInt64(1)} of {reader.GetInt64(0)}";
    }
    catch (Exception)
    {
        return "n/a";
    }
}

/* N hourly files the way the archiver writes them (archive COPY options, rows in time order, servers interleaved):
   time column, server_id, server_name, numeric counters that grow slowly (so they compress like real counters), and
   text columns drawn from a few thousand distinct values. Returns the file paths. */
static List<string> GenerateSeededMonth(string dir, int files, int rows, int servers, int numericCols, int textCols,
    int textBytes, string timeColumn, int noise = 97, int textDistinct = 5000)
{
    var numeric = string.Concat(Enumerable.Range(1, numericCols).Select(k =>
        $", (((h * {rows} + i) // 50) * {k} + (hash(i, {k}) % {noise}))::BIGINT AS c{k}"));
    var text = string.Concat(Enumerable.Range(1, textCols).Select(k =>
        $", substr(repeat(md5((((h * {rows} + i) % {textDistinct}) + {k})::VARCHAR), {textBytes / 32 + 1}), 1, {textBytes}) AS t{k}"));
    var paths = new List<string>();
    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();
    for (var h = 0; h < files; h++)
    {
        var path = Path.Combine(dir, $"seed_{h:D3}.parquet").Replace("\\", "/");
        using var cmd = con.CreateCommand();
        cmd.CommandText =
            $"COPY (SELECT TIMESTAMP '2026-10-01 00:00:00' + to_seconds(({h} * 3600 + i * 3600 // {rows})::BIGINT) AS {timeColumn}, " +
            $"(1 + i % {servers})::INTEGER AS server_id, 'server' || (1 + i % {servers})::VARCHAR AS server_name{numeric}{text} " +
            $"FROM (SELECT {h} AS h, i FROM range({rows}) t(i))) TO '{path}' ({ParquetCompaction.ArchiveCopyOptions})";
        cmd.ExecuteNonQuery();
        paths.Add(path);
    }

    return paths;
}

static long CountRowGroups(List<string> paths)
{
    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();
    var list = string.Join(", ", paths.Select(p => $"'{p.Replace("'", "''").Replace("\\", "/")}'"));
    using var cmd = con.CreateCommand();
    cmd.CommandText = $"SELECT count(*) FROM (SELECT DISTINCT file_name, row_group_id FROM parquet_metadata([{list}]))";
    return Convert.ToInt64(cmd.ExecuteScalar());
}

static string GetArg(string[] args, string key, string defaultValue)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] == key) return args[i + 1];
    return defaultValue;
}

/* Read-only scan of the source files: reports the in-memory (uncompressed) size
   distribution of every VARCHAR column. This is the data shape that decides
   whether compaction OOMs — a single monster plan-XML cell forces an unspillable
   allocation no matter how small the file is on disk. Synthetic data can't fake
   this, so this profile is the thing we actually need from a real archive. */
static void ProfileSource(List<string> paths)
{
    Console.WriteLine("[profile] Source data shape (read-only scan — no merge):");
    var pathSql = string.Join(", ", paths.Select(p => $"'{p.Replace("'", "''").Replace("\\", "/")}'"));

    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();

    var varcharCols = new List<string>();
    using (var d = con.CreateCommand())
    {
        d.CommandText = $"DESCRIBE SELECT * FROM read_parquet([{pathSql}], union_by_name=true)";
        using var dr = d.ExecuteReader();
        while (dr.Read())
        {
            if (dr.GetString(1).Contains("VARCHAR", StringComparison.OrdinalIgnoreCase))
                varcharCols.Add(dr.GetString(0));
        }
    }
    if (varcharCols.Count == 0)
    {
        Console.WriteLine("      (no VARCHAR columns)");
        return;
    }

    var selects = new List<string> { "count(*) AS row_count" };
    foreach (var c in varcharCols)
    {
        var e = c.Replace("\"", "\"\"");
        selects.Add($"max(octet_length(encode(\"{e}\"))) AS \"{e}__max\"");
        selects.Add($"quantile_cont(octet_length(encode(\"{e}\")), 0.99) AS \"{e}__p99\"");
        selects.Add($"quantile_cont(octet_length(encode(\"{e}\")), 0.50) AS \"{e}__p50\"");
        selects.Add($"sum(octet_length(encode(\"{e}\")))::BIGINT AS \"{e}__sum\"");
    }

    using var cmd = con.CreateCommand();
    cmd.CommandText = $"SELECT {string.Join(", ", selects)} FROM read_parquet([{pathSql}], union_by_name=true)";
    using var r = cmd.ExecuteReader();
    r.Read();

    var rowCount = r.GetInt64(0);
    Console.WriteLine($"      rows: {rowCount:N0}   VARCHAR columns: {varcharCols.Count}");
    Console.WriteLine($"      {"column",-24} {"p50",10} {"p99",12} {"max",14} {"total",12}");

    double Val(int idx) => r.IsDBNull(idx) ? 0 : Convert.ToDouble(r.GetValue(idx));
    long grandTotal = 0;
    for (var i = 0; i < varcharCols.Count; i++)
    {
        var b = 1 + i * 4;
        var max = Val(b);
        var p99 = Val(b + 1);
        var p50 = Val(b + 2);
        var sum = Val(b + 3);
        grandTotal += (long)sum;
        Console.WriteLine($"      {varcharCols[i],-24} {Human(p50),10} {Human(p99),12} {Human(max),14} {Human(sum),12}");
    }

    var onDisk = paths.Sum(p => new FileInfo(p.Replace("/", "\\")).Length);
    Console.WriteLine($"      total uncompressed VARCHAR: {grandTotal / 1024.0 / 1024.0:F1} MB" +
                      $"   (on disk: {onDisk / 1024.0 / 1024.0:F1} MB, ~{(onDisk > 0 ? grandTotal / (double)onDisk : 0):F1}x expansion)");
}

static string Human(double bytes)
{
    if (bytes >= 1024.0 * 1024.0) return $"{bytes / 1024.0 / 1024.0:F1} MB";
    if (bytes >= 1024.0) return $"{bytes / 1024.0:F1} KB";
    return $"{bytes:F0} B";
}

static void GenerateSyntheticSource(string outputPath, int rows, int baseOps, int tailOps, int tailEvery)
{
    /* Generate query_snapshots-shaped parquet with REALISTIC plan XML:

       - Low entropy. Real SQL plan XML is mostly repeated tag/attribute
         structure, so it compresses ~20-40:1 on disk and expands hard in
         memory. (The old generator used md5() hashes — high entropy — which
         compress poorly and barely expand, so it never reproduced the OOM.)
       - Heavy-tailed. Most rows get a small plan (baseOps RelOps); every
         tailEvery-th row gets a huge one (tailOps RelOps); rows divisible by 5
         get a medium plan. A few multi-MB plans clustered into one parquet row
         group is what drives the in-memory spike — uniform-width data can't.

       The merge cost we care about is in-memory VARCHAR bytes, not on-disk
       bytes. A small ZSTD parquet file that decompresses to gigabytes of XML
       is exactly the #933 failure shape. */
    var sourceSql = outputPath.Replace("'", "''").Replace("\\", "/");
    var mediumOps = baseOps * 22;

    using var con = new DuckDBConnection("DataSource=:memory:");
    con.Open();

    using var cmd = con.CreateCommand();
    cmd.CommandText = $@"
COPY (
    WITH r AS (
        SELECT
            i,
            CASE WHEN (i % {tailEvery}) = 0 THEN {tailOps}
                 WHEN (i % 25) = 0 THEN {mediumOps}
                 ELSE {baseOps} END AS ops
        FROM generate_series(1, {rows}) t(i)
    )
    SELECT
        i AS collection_id,
        TIMESTAMP '2026-04-01 00:00:00' + INTERVAL (i) MINUTE AS collection_time,
        ((i % 4) + 1)::INTEGER AS server_id,
        ('Server' || ((i % 4) + 1)::VARCHAR) AS server_name,
        ((i % 200) + 50)::INTEGER AS session_id,
        ('db_' || ((i % 10) + 1)::VARCHAR) AS database_name,
        '00:00:00' AS elapsed_time_formatted,
        ('SELECT * FROM dbo.Orders o JOIN dbo.Customers c ON c.CustomerId = o.CustomerId WHERE o.OrderDate > ''2026-01-01'' /* ' || chr(1090) || chr(1077) || chr(1089) || chr(1090) || ' q' || (i % 1000)::VARCHAR || ' */') AS query_text,
        ('<ShowPlanXML xmlns=""http://schemas.microsoft.com/sqlserver/2004/07/showplan""><BatchSequence><Batch><Statements><StmtSimple><QueryPlan>' ||
         list_aggregate(
             list_transform(generate_series(1, ops),
                 j -> '<RelOp NodeId=""' || j::VARCHAR || '"" PhysicalOp=""Clustered Index Scan"" LogicalOp=""Clustered Index Scan"" EstimateRows=""' || (j * 1.5)::VARCHAR || '"" EstimateIO=""0.003"" EstimateCPU=""0.0001"" AvgRowSize=""' || (40 + (j % 80))::VARCHAR || '"" EstimatedTotalSubtreeCost=""' || (j * 0.017)::VARCHAR || '"" Parallel=""0"" EstimateRebinds=""0"" EstimateRewinds=""0""><OutputList><ColumnReference Database=""[mydb]"" Schema=""[dbo]"" Table=""[Orders]"" Column=""[Col' || (j % 30)::VARCHAR || ']""/></OutputList><IndexScan Ordered=""0"" ForcedIndex=""0""><DefinedValues/><Object Database=""[mydb]"" Schema=""[dbo]"" Table=""[Orders]"" Index=""[PK_Orders_' || (i % 50)::VARCHAR || ']""/></IndexScan></RelOp>'),
             'string_agg', '') ||
         '</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>') AS query_plan,
        ('<ShowPlanXML xmlns=""http://schemas.microsoft.com/sqlserver/2004/07/showplan""><BatchSequence><Batch><Statements><StmtSimple><QueryPlan>' ||
         list_aggregate(
             list_transform(generate_series(1, {baseOps}),
                 j -> '<RelOp NodeId=""' || j::VARCHAR || '"" PhysicalOp=""Hash Match"" LogicalOp=""Aggregate"" Parallel=""1""><OutputList><ColumnReference Database=""[mydb]"" Schema=""[dbo]"" Table=""[Orders]"" Column=""[Col' || (j % 30)::VARCHAR || ']""/></OutputList></RelOp>'),
             'string_agg', '') ||
         '</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>') AS live_query_plan,
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
    FROM r
) TO '{sourceSql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)";
    cmd.ExecuteNonQuery();
}
