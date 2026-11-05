using System.Globalization;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Opens every DuckDB connection the exporter uses, with bounded memory and threads. The exporter runs on a monitoring
/// host beside Darling and its store. DuckDB's defaults (80% of physical memory, one thread per core) let one large
/// table push that host out of memory: the first plan_shape export on a large store grew to tens of GB of private
/// memory. With a limit, DuckDB spills to <c>&lt;database&gt;.spill</c> instead. The default is 4 GB,
/// not less, because a parquet COPY cannot live under a small limit. DuckDB pre-reserves most of memory_limit when a
/// parquet COPY starts, and makes allocations that bypass the buffer manager and never spill. Performance Monitor Lite
/// hit this in its own parquet archive (erikdarlingdata/PerformanceMonitor#933): it calls a limit below 2 GB
/// known-broken and runs its COPYs at 4 GB. At 2 GB (1.86 GiB) a scheduled export failed three times in plan_shape
/// ("1.9 GiB/1.8 GiB used"). A monitoring host usually has far more than that free.
/// </summary>
public static partial class DuckDbSession
{
    public const string DefaultMemoryLimit = "4GB";
    public const int DefaultThreads = 2;

    [GeneratedRegex(@"^\d+(\.\d+)?\s*(KB|MB|GB|KiB|MiB|GiB)$", RegexOptions.IgnoreCase)]
    private static partial Regex MemoryLimitPattern();

    /// <summary>
    /// Options for every COPY ... TO parquet. DuckDB buffers a whole row group in memory before writing it, so row
    /// groups are capped by bytes as well as rows: at 122,880 rows, a row group of ~52 KB plans would be ~6 GB.
    /// </summary>
    public const string ParquetCopyOptions =
        "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880, ROW_GROUP_SIZE_BYTES '64MB'";

    /// <summary>The DuckDB memory_limit applied to every connection, e.g. "4GB".</summary>
    public static string MemoryLimit { get; private set; } = DefaultMemoryLimit;

    /// <summary>The DuckDB thread count applied to every connection.</summary>
    public static int Threads { get; private set; } = DefaultThreads;

    /// <summary>Sets the limits for every connection opened afterwards. Throws on a malformed value.</summary>
    public static void Configure(string memoryLimit, int threads)
    {
        if (!MemoryLimitPattern().IsMatch(memoryLimit.Trim()))
        {
            throw new ArgumentException($"--duckdb-memory-limit must look like 2GB or 512MB, got '{memoryLimit}'");
        }

        if (threads is < 1 or > 64)
        {
            throw new ArgumentException($"--duckdb-threads must be between 1 and 64, got {threads}");
        }

        MemoryLimit = memoryLimit.Trim();
        Threads = threads;
    }

    /// <summary>Opens the DuckDB database at <paramref name="databasePath"/> with the configured limits.</summary>
    public static DuckDBConnection Open(string databasePath) =>
        OpenWithSpillDirectory($"Data Source={databasePath}", SpillDirectoryFor(databasePath));

    /// <summary>Opens a new in-memory DuckDB instance with the configured limits, spilling to <paramref name="spillDirectory"/>.</summary>
    public static DuckDBConnection OpenInMemory(string spillDirectory) =>
        OpenWithSpillDirectory("Data Source=:memory:", spillDirectory);

    private static DuckDBConnection OpenWithSpillDirectory(string connectionString, string spillDirectory)
    {
        Directory.CreateDirectory(spillDirectory);

        var connection = new DuckDBConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        // Every COPY in the exporter carries its own ORDER BY, so insertion order need not be preserved, which lets
        // DuckDB stream the COPY instead of buffering it.
        command.CommandText = string.Create(CultureInfo.InvariantCulture,
            $"SET memory_limit = '{MemoryLimit}'; SET threads = {Threads}; " +
            $"SET temp_directory = '{spillDirectory.Replace("'", "''")}'; SET preserve_insertion_order = false;");
        command.ExecuteNonQuery();
        return connection;
    }

    /// <summary>Deletes a temp database, its WAL and its spill directory. Never throws.</summary>
    public static void DeleteTempDatabase(string databasePath)
    {
        TryDeleteFile(databasePath);
        TryDeleteFile(databasePath + ".wal");
        try
        {
            var spillDirectory = SpillDirectoryFor(databasePath);
            if (Directory.Exists(spillDirectory))
            {
                Directory.Delete(spillDirectory, recursive: true);
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static string SpillDirectoryFor(string databasePath) => databasePath + ".spill";

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
