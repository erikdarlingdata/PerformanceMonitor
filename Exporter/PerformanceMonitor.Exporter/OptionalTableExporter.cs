using System.Data.Common;
using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// One optional-table export request. The runner builds this from the registry and the store's own reader; this type is
/// deliberately minimal so the exporter does not depend on the <c>OptionalTables</c> registry or
/// <c>SqlBuilder.BuildOptionalSelect</c>, both being built in parallel.
/// </summary>
/// <param name="Table">The table name, used for the DuckDB temp table and the output file name.</param>
/// <param name="TimeColumn">
/// The column the window is filtered on, e.g. "collection_time" or "capture_time". Null for a table with no time
/// column (there are none of those in the 7a registry, but nothing here requires one).
/// </param>
/// <param name="IsSnapshot">
/// True for a daily-snapshot table (only <c>servers</c> today,. A snapshot row gets a <c>snapshot_time</c> column, and
/// the export runs only when <paramref name="FromUtc"/> is a UTC midnight.
/// </param>
/// <param name="FromUtc">The window start (inclusive), and the snapshot's stamp for a snapshot table.</param>
/// <param name="ToUtc">The window end (exclusive).</param>
public sealed record OptionalExportRequest(string Table, string? TimeColumn, bool IsSnapshot, DateTime FromUtc, DateTime ToUtc)
{
    /// <summary>
    /// True when the request comes from a <see cref="OptionalTableKind.Dimension"/> registry entry. A dimension has
    /// its own exporter (<c>PgStatementTextExporter</c>) and its own key-set logic; it never runs through <see
    /// cref="OptionalTableExporter.ExportAsync"/>'s generic Window/Snapshot select path. Defaults to false so every
    /// other call site (all Window/Snapshot requests) is unaffected.
    /// </summary>
    public bool IsDimension { get; init; }
}

/// <summary>One output Parquet file written for an optional table's export.</summary>
public sealed record OptionalTablePart(string RelativePath, long RowCount, long ByteSize, string Sha256);

/// <summary>
/// The result of one <see cref="OptionalTableExporter.ExportAsync"/> call, shaped so the runner can fold it into the separate
/// optional manifest: a table's total row count, its parts, timings, and (for the snapshot rule) why nothing ran.
/// </summary>
public sealed class OptionalTableExportResult
{
    public required string Table { get; init; }
    public required long RowCount { get; init; }
    public required List<OptionalTablePart> Parts { get; init; }
    public required double StoreQueryElapsedMs { get; init; }
    public required double WriteElapsedMs { get; init; }

    /// <summary>The actual store range read (Daily grain only). Null for every other
    /// grain.</summary>
    public DateTime? ReadFrom { get; init; }

    /// <summary>The actual store range read (Daily grain only). Null for every other grain.</summary>
    public DateTime? ReadTo { get; init; }

    /// <summary>
    /// Set only when the export did not run at all -- today, only the snapshot rule a non-midnight window for a
    /// snapshot table. When this is set, <see cref="RowCount"/> is 0, <see cref="Parts"/> is empty, and the reader
    /// factory passed to <see cref="OptionalTableExporter.ExportAsync"/> was never invoked.
    /// </summary>
    public string? SkippedReason { get; init; }
}

/// <summary>
/// Exports one optional table for one window. This is the byte-bounded writer principle 5 requires: rows stream from
/// the store's reader into DuckDB via the appender, and every row calls <c>RowAppended(rowBytes)</c> so a wide row
/// closes its part early, exactly as <see cref="ChunkedParquetWriter"/> already does for plan XML. It does not reuse
/// <see cref="ChunkedParquetWriter"/> directly, because that type's <c>Complete</c> merges every chunk's part into one
/// final file -- documented there as deliberate, because sorting a whole day of plan XML cannot fit in bounded memory
/// and nothing reads those wide tables in key order. Optional tables are the opposite case: most are narrow, and the
/// loader benefits from row-group pruning by server and time. So each flushed chunk here is written out, sorted, as
/// its own final part file (never merged), reusing <see cref="ChunkedParquetWriter"/>'s constants and its "fresh
/// DuckDB instance per chunk" memory shape, but not its type.
/// </summary>
public static class OptionalTableExporter
{
    /// <summary>
    /// Most rows per part file for optional-table exporters (this class's <see cref="OptionalPartWriter"/> and <see
    /// cref="ChangeOnlyExporter"/>'s narrow-table writer, which shares this bound). Deliberately far above <see
    /// cref="ChunkedParquetWriter.ChunkRows"/>: that 1,000-row cap exists for Query Store plan XML at ~52 KB a row,
    /// but most optional tables are narrow (a real large-store hour of job_history was 27,128 rows and still well
    /// under a megabyte of appended bytes). Without a separate cap, narrow tables split into a part every 1,000 rows
    /// regardless of size -- that hour produced 28 twelve-KB parts, about 670 tiny files a day for one table. <see
    /// cref="ChunkedParquetWriter.ChunkBytes"/> (still 64 MB) remains the real memory bound: wide rows close a chunk
    /// long before a million of them accumulate.
    /// </summary>
    public const int ChunkRows = 1_000_000;

    /// <summary>
    /// Row-group shape for optional-table parts: matches <c>ChunkedParquetWriter</c>'s wide-table options (that
    /// constant is private there), because an optional table's rows are not bounded in width by anything here --
    /// 7b's query_snapshots plans are exactly this shape.
    /// </summary>
    private const string PartParquetOptions = "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000, ROW_GROUP_SIZE_BYTES '64MB'";

    /// <summary>
    /// Exports <paramref name="request"/> for <paramref name="store"/>, writing one or more sorted Parquet parts under
    /// <paramref name="outDir"/>/<paramref name="store"/>/<paramref name="request"/>.Table/. <paramref
    /// name="readerFactory"/> is called at most once, and only when the export actually runs (see snapshot rule): a
    /// skipped snapshot window never touches the store.
    /// </summary>
    public static async Task<OptionalTableExportResult> ExportAsync(
        OptionalExportRequest request,
        string store,
        string outDir,
        Func<CancellationToken, Task<DbDataReader>> readerFactory,
        CancellationToken ct)
    {
        if (request.IsDimension)
        {
            throw new InvalidOperationException(
                $"'{request.Table}' is a Dimension-kind optional table; it must be routed to its own exporter " +
                "(e.g. PgStatementTextExporter), never through the generic OptionalTableExporter Window/Snapshot path.");
        }

        if (request.IsSnapshot && !IsDailySnapshotWindow(request.FromUtc))
        {
            return new OptionalTableExportResult
            {
                Table = request.Table,
                RowCount = 0,
                Parts = [],
                StoreQueryElapsedMs = 0,
                WriteElapsedMs = 0,
                SkippedReason = "skipped: not the daily snapshot window",
            };
        }

        var tableDir = Path.Combine(outDir, store, request.Table);
        Directory.CreateDirectory(tableDir);

        var storeStopwatch = Stopwatch.StartNew();
        await using var reader = await readerFactory(ct).ConfigureAwait(false);

        var columns = BuildColumns(reader, request.IsSnapshot);
        var hasServerName = columns.Any(c => string.Equals(c.Name, "server_name", StringComparison.Ordinal));
        var hasTimeColumn = request.TimeColumn is not null &&
            columns.Any(c => string.Equals(c.Name, request.TimeColumn, StringComparison.Ordinal));
        var orderBy = hasServerName && hasTimeColumn ? $"server_name, {QuoteIdentifier(request.TimeColumn!)}" : null;

        using var partWriter = new OptionalPartWriter(request.Table, columns, outDir, store, request.FromUtc, request.ToUtc, orderBy);
        var readerColumnCount = reader.FieldCount;
        long rowCount = 0;
        while (reader.Read())
        {
            var row = partWriter.Appender.CreateRow();
            row.AppendValue(store);
            long rowBytes = 0;
            for (var i = 0; i < readerColumnCount; i++)
            {
                rowBytes += AppendColumnValue(row, reader, i, columns[i + 1].DuckDbType);
            }

            if (request.IsSnapshot)
            {
                row.AppendValue(DateTime.SpecifyKind(request.FromUtc, DateTimeKind.Unspecified));
            }

            row.EndRow();
            partWriter.RowAppended(rowBytes);
            rowCount++;
        }

        storeStopwatch.Stop();
        var writeStopwatch = Stopwatch.StartNew();
        var parts = partWriter.Complete();
        writeStopwatch.Stop();

        return new OptionalTableExportResult
        {
            Table = request.Table,
            RowCount = rowCount,
            Parts = parts,
            StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
            WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
        };
    }

    /// <summary>True when <paramref name="fromUtc"/> is exactly a UTC midnight -- the fresh window that runs the daily snapshot.</summary>
    internal static bool IsDailySnapshotWindow(DateTime fromUtc) =>
        fromUtc.Hour == 0 && fromUtc.Minute == 0 && fromUtc.Second == 0 && fromUtc.Millisecond == 0;

    internal static List<ColumnShape> BuildColumns(DbDataReader reader, bool isSnapshot)
    {
        var columns = new List<ColumnShape>(reader.FieldCount + 2) { new("source_store", "VARCHAR") };
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var pgType = reader.GetDataTypeName(i);
            columns.Add(new ColumnShape(name, DuckDbTypeMapper.Map(name, pgType)));
        }

        if (isSnapshot)
        {
            columns.Add(new ColumnShape("snapshot_time", "TIMESTAMP"));
        }

        return columns;
    }

    /// <summary>
    /// Appends one reader column to the appender row and returns its byte weight for
    /// <see cref="OptionalPartWriter.RowAppended"/>: the UTF-8 length of a string, the length of a blob, or 8 for
    /// anything else (rule 5). A DBNull contributes 0: there is no value to weigh.
    /// </summary>
    internal static long AppendColumnValue(IDuckDBAppenderRow row, DbDataReader reader, int ordinal, string duckDbType)
    {
        if (reader.IsDBNull(ordinal))
        {
            row.AppendNullValue();
            return 0;
        }

        switch (duckDbType)
        {
            case "BOOLEAN":
                row.AppendValue(reader.GetBoolean(ordinal));
                return 8;
            case "SMALLINT":
                row.AppendValue(Convert.ToInt16(reader.GetValue(ordinal)));
                return 8;
            case "INTEGER":
                row.AppendValue(Convert.ToInt32(reader.GetValue(ordinal)));
                return 8;
            case "BIGINT":
                row.AppendValue(Convert.ToInt64(reader.GetValue(ordinal)));
                return 8;
            case "REAL":
                row.AppendValue(Convert.ToSingle(reader.GetValue(ordinal)));
                return 8;
            case "DOUBLE":
                row.AppendValue(Convert.ToDouble(reader.GetValue(ordinal)));
                return 8;
            case "VARCHAR":
            {
                var value = reader.GetValue(ordinal)?.ToString();
                row.AppendValue(value);
                return value is null ? 0 : System.Text.Encoding.UTF8.GetByteCount(value);
            }
            case "BLOB":
            {
                var bytes = (byte[])reader.GetValue(ordinal);
                row.AppendValue(bytes);
                return bytes.Length;
            }
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                row.AppendValue(reader.GetDateTime(ordinal));
                return 8;
            case "DATE":
                row.AppendValue(DateOnly.FromDateTime(reader.GetDateTime(ordinal)));
                return 8;
            case "UUID":
                row.AppendValue(reader.GetGuid(ordinal));
                return 8;
            case var decimalType when decimalType.StartsWith("DECIMAL(", StringComparison.Ordinal):
                row.AppendValue(reader.GetDecimal(ordinal));
                return 8;
            default:
                throw new NotSupportedException($"No appender case for DuckDB type '{duckDbType}'.");
        }
    }

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>
    /// Writes one optional table's rows as a series of sorted, byte-bounded Parquet parts. A fresh in-memory DuckDB
    /// instance per chunk (via <see cref="DuckDbSession.OpenInMemory"/>) is what bounds memory -- the same shape
    /// <see cref="ChunkedParquetWriter"/> uses and documents. Unlike that writer, each chunk's part IS a final
    /// output file (sorted by server_name and the time column when both exist), never merged.
    /// </summary>
    private sealed class OptionalPartWriter : IDisposable
    {
        private readonly string _table;
        private readonly List<ColumnShape> _columns;
        private readonly string _outDir;
        private readonly string _store;
        private readonly DateTime _fromUtc;
        private readonly DateTime _toUtc;
        private readonly string? _orderBy;
        private readonly string _createTableSql;
        private readonly string _spillRoot;
        private readonly List<OptionalTablePart> _parts = [];
        private DuckDBConnection? _connection;
        private DuckDBAppender? _appender;
        private int _rowsInChunk;
        private long _bytesInChunk;
        private bool _completed;

        public OptionalPartWriter(
            string table, List<ColumnShape> columns, string outDir, string store, DateTime fromUtc, DateTime toUtc, string? orderBy)
        {
            _table = table;
            _columns = columns;
            _outDir = outDir;
            _store = store;
            _fromUtc = fromUtc;
            _toUtc = toUtc;
            _orderBy = orderBy;
            _spillRoot = Path.Combine(Path.GetTempPath(), $"darling-export-opt-{Guid.NewGuid():N}.parts");
            Directory.CreateDirectory(_spillRoot);
            var columnDefs = string.Join(", ", columns.Select(c => $"{QuoteIdentifier(c.Name)} {c.DuckDbType}"));
            _createTableSql = $"CREATE TABLE {QuoteIdentifier(table)} ({columnDefs})";
        }

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
                    _connection = DuckDbSession.OpenInMemory(Path.Combine(_spillRoot, $"spill-{_parts.Count:D6}"));
                    Execute(_connection, _createTableSql);
                    _appender = _connection.CreateAppender(_table);
                }

                return _appender;
            }
        }

        /// <summary>Counts one appended row and flushes the chunk once it reaches the shared chunk bounds.</summary>
        public void RowAppended(long rowBytes)
        {
            _rowsInChunk++;
            _bytesInChunk += Math.Max(0, rowBytes);
            if (_rowsInChunk >= OptionalTableExporter.ChunkRows || _bytesInChunk >= ChunkedParquetWriter.ChunkBytes)
            {
                FlushChunk();
            }
        }

        /// <summary>
        /// Flushes any remaining rows and returns every part written. If no rows were ever appended, writes one
        /// empty, schema-only part -- the same "always one file" convention <see cref="TableExporter"/> and
        /// <see cref="ChunkedParquetWriter"/> both follow, so a downstream loader never has to special-case a table
        /// with no rows in the window.
        /// </summary>
        public List<OptionalTablePart> Complete()
        {
            FlushChunk();
            _completed = true;

            if (_parts.Count == 0)
            {
                using var connection = DuckDbSession.OpenInMemory(Path.Combine(_spillRoot, "spill-empty"));
                Execute(connection, _createTableSql);
                WritePart(connection, partIndex: 0, rowsInPart: 0);
            }

            return _parts;
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
                WritePart(_connection, _parts.Count, _rowsInChunk);
            }

            _connection.Dispose();
            _connection = null;
            _rowsInChunk = 0;
            _bytesInChunk = 0;
        }

        private void WritePart(DuckDBConnection connection, int partIndex, long rowsInPart)
        {
            var relativePath = OutputPaths.RelativeParquetPath(_store, _table, _fromUtc, _toUtc, part: $"p{partIndex:D5}");
            var absolutePath = Path.Combine(_outDir, _store, relativePath.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);

            var select = _orderBy is null
                ? $"SELECT * FROM {QuoteIdentifier(_table)}"
                : $"SELECT * FROM {QuoteIdentifier(_table)} ORDER BY {_orderBy}";
            Execute(connection, $"COPY ({select}) TO '{absolutePath.Replace("'", "''")}' ({PartParquetOptions})");

            var byteSize = new FileInfo(absolutePath).Length;
            var sha256 = ComputeSha256(absolutePath);
            _parts.Add(new OptionalTablePart(relativePath, rowsInPart, byteSize, sha256));
        }

        private static void Execute(DuckDBConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private static string ComputeSha256(string path)
        {
            using var stream = File.OpenRead(path);
            var hash = SHA256.HashData(stream);
            return Convert.ToHexStringLower(hash);
        }

        public void Dispose()
        {
            _appender?.Dispose();
            _connection?.Dispose();
            try
            {
                if (Directory.Exists(_spillRoot))
                {
                    Directory.Delete(_spillRoot, recursive: true);
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
}
