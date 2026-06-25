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
 *   --merge-files <a,b,..> Compact the given files directly (no split). Use this
 *                          to run against a reporter's actual archive files.
 *   --synthetic            Generate a query_snapshots-shaped source, then split.
 *
 * Every run first prints a read-only [profile] of the source data — the
 * uncompressed (in-memory) size distribution of each column. That profile is
 * the data shape that decides whether compaction OOMs; synthetic data can't
 * fake it. Use --profile-only to get just the profile and skip the merge.
 *
 * Tuning knobs (defaults = ParquetCompaction's non-table-specific defaults):
 *   --memory-limit <str>   DuckDB memory_limit per merge connection. Default: 4GB
 *   --threads <int>        DuckDB threads per merge connection. Default: 2
 *   --row-group-size <int> Output ROW_GROUP_SIZE. Default: 8192
 *   --max-batch-mb <int>   Per-batch on-disk input budget (MB). Default: 200
 *   --table <name>         Table name (drives exclude-column logic). Default: query_snapshots
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
if (string.IsNullOrEmpty(sourceFile) && string.IsNullOrEmpty(mergeFilesArg) && !synthetic)
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
var memoryLimit = GetArg(args, "--memory-limit", ParquetCompaction.DefaultMemoryLimit);
var threads = int.Parse(GetArg(args, "--threads", ParquetCompaction.DefaultThreads.ToString()));
var rowGroupSize = int.Parse(GetArg(args, "--row-group-size", ParquetCompaction.DefaultRowGroupSize.ToString()));
var maxBatchMb = int.Parse(GetArg(args, "--max-batch-mb", (ParquetCompaction.DefaultBatchInputBytes / (1024 * 1024)).ToString()));
var maxBatchBytes = maxBatchMb * 1024L * 1024L;
var numFiles = int.Parse(GetArg(args, "--num-files", "15"));
var keep = args.Contains("--keep");
var profileOnly = args.Contains("--profile-only");
var sweep = args.Contains("--sweep");

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
    Console.WriteLine($"Settings: memory_limit={memoryLimit}, threads={threads}, ROW_GROUP_SIZE={rowGroupSize}, max-batch={maxBatchMb} MB");
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
        RunOneConfig(int rgs, long batchBytes, string memLimit, int thr, bool verbose)
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
                ParquetCompaction.MergeBatchToFile(table, batches[i], outPath, spillDir, memLimit, thr, rgs);
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
       COMPRESSION ZSTD and the DuckDB default row group size). Empty chunks are
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
            $"TO '{path.Replace("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD)";
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
