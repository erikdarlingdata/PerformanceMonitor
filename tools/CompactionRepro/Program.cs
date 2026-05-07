using System.Diagnostics;
using DuckDB.NET.Data;

/*
 * CompactionRepro — standalone reproducer for issue #933.
 *
 * Splits an existing monthly parquet file (like 202604_query_snapshots.parquet)
 * into N per-cycle-shaped chunks, then runs the same pair-merge compaction
 * logic ArchiveService.CompactParquetFiles uses, with knobs you can flip on
 * the command line. The split chunks have the exact row shape that caused
 * the user's OOM in #933.
 *
 * Compare OLD vs NEW tuning by running the same data shape twice with
 * different --memory-limit / --threads / --row-group-size values.
 *
 * Usage:
 *   dotnet run -- --source-file <path> [options]
 *
 * Options (defaults match the proposed NEW tuning):
 *   --source-file <path>     Required. Path to a monthly parquet file to split & merge.
 *   --memory-limit <str>     DuckDB memory_limit (e.g. "1GB", "4GB"). Default: 1GB
 *   --threads <int>          DuckDB threads. 0 = DuckDB default. Default: 2
 *   --row-group-size <int>   Output ROW_GROUP_SIZE. Default: 8192
 *   --num-files <int>        Number of split chunks. Default: 15
 *   --keep                   Don't delete temp dir after run (for inspection)
 *
 * Examples:
 *   # NEW tuning (the proposed fix) on real query_snapshots data
 *   dotnet run -- --source-file "$LOCALAPPDATA/PerformanceMonitorLite/archive/202604_query_snapshots.parquet" \
 *                 --memory-limit 1GB --threads 2 --row-group-size 8192
 *
 *   # OLD tuning (current production) — should reproduce the OOM
 *   dotnet run -- --source-file "$LOCALAPPDATA/PerformanceMonitorLite/archive/202604_query_snapshots.parquet" \
 *                 --memory-limit 4GB --threads 0 --row-group-size 122880
 */

var sourceFile = GetArg(args, "--source-file", "");
if (string.IsNullOrEmpty(sourceFile))
{
    Console.Error.WriteLine("error: --source-file is required");
    Console.Error.WriteLine("Try: --source-file \"$LOCALAPPDATA/PerformanceMonitorLite/archive/202604_query_snapshots.parquet\"");
    return 2;
}
if (!File.Exists(sourceFile))
{
    Console.Error.WriteLine($"error: source file not found: {sourceFile}");
    return 2;
}

var memoryLimit = GetArg(args, "--memory-limit", "1GB");
var threads = int.Parse(GetArg(args, "--threads", "2"));
var rowGroupSize = int.Parse(GetArg(args, "--row-group-size", "8192"));
var numFiles = int.Parse(GetArg(args, "--num-files", "15"));
var keep = args.Contains("--keep");

var tempDir = Path.Combine(Path.GetTempPath(), $"CompactionRepro_{Guid.NewGuid():N}");
Directory.CreateDirectory(tempDir);

Console.WriteLine($"Source:   {sourceFile} ({new FileInfo(sourceFile).Length / 1024.0 / 1024.0:F1} MB)");
Console.WriteLine($"Temp dir: {tempDir}");
Console.WriteLine($"Settings: memory_limit={memoryLimit}, threads={threads}, ROW_GROUP_SIZE={rowGroupSize}");
Console.WriteLine($"Splitting source into {numFiles} chunks");
Console.WriteLine();

try
{
    Console.WriteLine($"[1/3] Splitting source file into {numFiles} chunks...");
    var sw = Stopwatch.StartNew();
    var sourcePaths = SplitSourceFile(sourceFile, tempDir, numFiles);
    sw.Stop();
    var totalSourceBytes = sourcePaths.Sum(p => new FileInfo(p).Length);
    Console.WriteLine($"      Wrote {sourcePaths.Count} files, {totalSourceBytes / 1024.0 / 1024.0:F1} MB total in {sw.ElapsedMilliseconds} ms");
    Console.WriteLine();

    Console.WriteLine("[2/3] Running pair-merge compaction (mirrors ArchiveService.CompactParquetFiles)...");
    var spillDir = Path.Combine(tempDir, "duckdb_tmp").Replace("\\", "/");
    Directory.CreateDirectory(spillDir);

    var targetPath = Path.Combine(tempDir, "compacted.parquet").Replace("\\", "/");
    var process = Process.GetCurrentProcess();
    var startBytes = GC.GetTotalMemory(forceFullCollection: true);
    var startWorkingSet = process.WorkingSet64;

    var compactionSw = Stopwatch.StartNew();
    var peakWorkingSet = startWorkingSet;
    long compactedFileBytes = 0;
    var success = false;
    string? failureMessage = null;

    try
    {
        /* Sort smallest-first like ArchiveService does */
        var sorted = sourcePaths
            .OrderBy(p => new FileInfo(p).Length)
            .ToList();

        var currentPath = sorted[0];
        var intermediateFiles = new List<string>();

        for (var i = 1; i < sorted.Count; i++)
        {
            var stepOutput = i < sorted.Count - 1
                ? targetPath + $".step{i}.tmp"
                : targetPath;

            using var con = new DuckDBConnection("DataSource=:memory:");
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

            var pairList = $"'{currentPath.Replace("'", "''")}', '{sorted[i].Replace("'", "''")}'";
            using var cmd = con.CreateCommand();
            cmd.CommandText =
                $"COPY (SELECT * FROM read_parquet([{pairList}], union_by_name=true)) " +
                $"TO '{stepOutput.Replace("'", "''")}' " +
                $"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rowGroupSize})";
            cmd.ExecuteNonQuery();

            process.Refresh();
            if (process.WorkingSet64 > peakWorkingSet) peakWorkingSet = process.WorkingSet64;

            if (intermediateFiles.Count > 0)
            {
                var prev = intermediateFiles[^1];
                try { File.Delete(prev); } catch { }
            }

            intermediateFiles.Add(stepOutput);
            currentPath = stepOutput;

            Console.WriteLine($"      step {i}/{sorted.Count - 1}: peak working set {peakWorkingSet / 1024.0 / 1024.0:F0} MB");
        }

        compactedFileBytes = new FileInfo(targetPath).Length;
        success = true;
    }
    catch (Exception ex)
    {
        failureMessage = ex.Message;
    }
    compactionSw.Stop();

    process.Refresh();
    if (process.WorkingSet64 > peakWorkingSet) peakWorkingSet = process.WorkingSet64;

    Console.WriteLine();
    Console.WriteLine("[3/3] Result:");
    Console.WriteLine($"      Status:           {(success ? "SUCCESS" : "FAILURE")}");
    Console.WriteLine($"      Wall time:        {compactionSw.Elapsed.TotalSeconds:F2}s");
    Console.WriteLine($"      Peak working set: {peakWorkingSet / 1024.0 / 1024.0:F0} MB");
    if (success)
    {
        Console.WriteLine($"      Output size:      {compactedFileBytes / 1024.0 / 1024.0:F1} MB");

        /* Sanity check: row count round-trip — output must match source */
        using var verifyCon = new DuckDBConnection("DataSource=:memory:");
        verifyCon.Open();
        using var verifyCmd = verifyCon.CreateCommand();
        verifyCmd.CommandText =
            $"SELECT (SELECT COUNT(*) FROM read_parquet('{targetPath.Replace("'", "''")}')) AS out_rows, " +
            $"       (SELECT COUNT(*) FROM read_parquet('{sourceFile.Replace("'", "''").Replace("\\", "/")}')) AS src_rows";
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
       Empty chunks are skipped. */
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
