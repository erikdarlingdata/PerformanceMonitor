using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Writes rows of a wide table (plan XML, query text) to one Parquet file with memory bounded by the chunk size, not
/// by the table size. Each chunk of <see cref="ChunkRows"/> rows goes through the DuckDB appender into its own
/// in-memory DuckDB instance, is written to a part file, and the instance is closed; <see cref="Complete"/> streams
/// the parts into the final file.
///
/// A fresh instance per chunk is what bounds memory. Reusing one instance, even dropping the table and checkpointing
/// after every chunk, grew native memory with the total rows appended (measured: 1.3 GB for 20,000 plans of 51 KB,
/// 3.0 GB for 60,000). A fresh instance per chunk stayed at about 0.5 GB for both.
///
/// The final file is not sorted. Sorting a day of Query Store plans on a large store (hundreds of thousands of plans,
/// tens of GB of XML) cannot be done in bounded memory, and nothing reads these tables in key order: a loader matches keys.
/// </summary>
public sealed class ChunkedParquetWriter : IDisposable
{
    /// <summary>Most rows per part file. At ~52 KB per plan that is ~50 MB of XML held at a time.</summary>
    public const int ChunkRows = 1000;

    /// <summary>
    /// Most bytes per part file, counted from the sizes callers pass to <see cref="RowAppended(long)"/>. A row count
    /// alone does not bound memory: a large store's Query Store plans reach 3.6 MB, so 1,000 of them can be gigabytes,
    /// and the first 1,000-row chunk over DuckDB's 1.8 GiB limit failed a scheduled export three times.
    /// </summary>
    public const long ChunkBytes = 64L * 1024 * 1024;

    /// <summary>
    /// Row groups for these wide tables end at 1,000 rows or 64 MB, whichever comes first, so neither a part nor the
    /// final file ever needs a gigabyte-sized row group in memory. ROW_GROUP_SIZE_BYTES needs preserve_insertion_order
    /// off, which <see cref="DuckDbSession"/> sets on every connection.
    /// </summary>
    private const string WideParquetOptions = "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000, ROW_GROUP_SIZE_BYTES '64MB'";

    private readonly string _table;
    private readonly string _workDirectory;
    private readonly string _createTableSql;
    private readonly List<string> _parts = [];
    private DuckDBConnection? _connection;
    private DuckDBAppender? _appender;
    private int _rowsInChunk;
    private long _bytesInChunk;
    private bool _completed;

    public ChunkedParquetWriter(string tempDbPath, string table, IReadOnlyList<ColumnShape> columns)
    {
        _table = table;
        _workDirectory = tempDbPath + ".parts";
        Directory.CreateDirectory(_workDirectory);
        var columnDefs = string.Join(", ", columns.Select(c => $"{Quote(c.Name)} {c.DuckDbType}"));
        _createTableSql = $"CREATE TABLE {Quote(table)} ({columnDefs})";
    }

    /// <summary>The appender for the current chunk. Call <see cref="RowAppended"/> after each row's EndRow.</summary>
    public DuckDBAppender Appender
    {
        get
        {
            if (_completed)
            {
                throw new InvalidOperationException("The writer is complete.");
            }

            if (_appender is null)
            {
                _connection = DuckDbSession.OpenInMemory(Path.Combine(_workDirectory, "spill"));
                Execute(_connection, _createTableSql);
                _appender = _connection.CreateAppender(_table);
            }

            return _appender;
        }
    }

    /// <summary>
    /// Counts one appended row of about <paramref name="rowBytes"/> bytes (the wide column's size; 0 for narrow rows),
    /// and writes the chunk out when it reaches <see cref="ChunkRows"/> rows or <see cref="ChunkBytes"/> bytes.
    /// </summary>
    public void RowAppended(long rowBytes = 0)
    {
        _rowsInChunk++;
        _bytesInChunk += Math.Max(0, rowBytes);
        if (_rowsInChunk >= ChunkRows || _bytesInChunk >= ChunkBytes)
        {
            FlushChunk();
        }
    }

    /// <summary>Writes the last chunk and streams every part into <paramref name="outputPath"/>.</summary>
    public void Complete(string outputPath)
    {
        FlushChunk();
        _completed = true;

        using var connection = DuckDbSession.OpenInMemory(Path.Combine(_workDirectory, "spill"));
        var escapedOutput = outputPath.Replace("'", "''");
        if (_parts.Count == 0)
        {
            // No rows: an empty file that still carries the table's schema.
            Execute(connection, _createTableSql);
            Execute(connection, $"COPY {Quote(_table)} TO '{escapedOutput}' ({WideParquetOptions})");
            return;
        }

        var partList = string.Join(", ", _parts.Select(p => $"'{p.Replace("'", "''")}'"));
        Execute(connection, $"COPY (SELECT * FROM read_parquet([{partList}])) TO '{escapedOutput}' ({WideParquetOptions})");
    }

    private void FlushChunk()
    {
        if (_appender is null || _connection is null)
        {
            return;
        }

        _appender.Close();
        _appender.Dispose();
        _appender = null;

        if (_rowsInChunk > 0)
        {
            var part = Path.Combine(_workDirectory, $"part-{_parts.Count:D6}.parquet");
            Execute(_connection, $"COPY {Quote(_table)} TO '{part.Replace("'", "''")}' ({WideParquetOptions})");
            _parts.Add(part);
        }

        _connection.Dispose();
        _connection = null;
        _rowsInChunk = 0;
        _bytesInChunk = 0;
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    public void Dispose()
    {
        _appender?.Dispose();
        _connection?.Dispose();
        try
        {
            if (Directory.Exists(_workDirectory))
            {
                Directory.Delete(_workDirectory, recursive: true);
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
