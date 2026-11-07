using System.Collections;
using System.Data;
using System.Data.Common;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// A minimal in-memory <see cref="DbDataReader"/> over a fixed set of rows, used in place of the store's
/// NpgsqlDataReader (O2b builds and tests against the abstract reader shape only). Column values are boxed
/// objects; DBNull.Value marks a null. Postgres type names drive <see cref="DuckDbTypeMapper"/> the same way
/// Npgsql's GetDataTypeName does.
/// </summary>
internal sealed class FakeDbDataReader : DbDataReader
{
    private readonly string[] _names;
    private readonly string[] _pgTypes;
    private readonly List<object[]> _rows;
    private int _index = -1;
    public int ReadCalls { get; private set; }

    public FakeDbDataReader(string[] names, string[] pgTypes, List<object[]> rows)
    {
        _names = names;
        _pgTypes = pgTypes;
        _rows = rows;
    }

    public override int FieldCount => _names.Length;
    public override string GetName(int ordinal) => _names[ordinal];
    public override string GetDataTypeName(int ordinal) => _pgTypes[ordinal];
    public override int GetOrdinal(string name) => Array.IndexOf(_names, name);

    public override bool Read()
    {
        ReadCalls++;
        _index++;
        return _index < _rows.Count;
    }

    public override bool IsDBNull(int ordinal) => _rows[_index][ordinal] is DBNull;
    public override object GetValue(int ordinal) => _rows[_index][ordinal];
    public override bool GetBoolean(int ordinal) => (bool)_rows[_index][ordinal];
    public override DateTime GetDateTime(int ordinal) => (DateTime)_rows[_index][ordinal];
    public override decimal GetDecimal(int ordinal) => (decimal)_rows[_index][ordinal];
    public override double GetDouble(int ordinal) => (double)_rows[_index][ordinal];
    public override float GetFloat(int ordinal) => (float)_rows[_index][ordinal];
    public override Guid GetGuid(int ordinal) => (Guid)_rows[_index][ordinal];
    public override short GetInt16(int ordinal) => (short)_rows[_index][ordinal];
    public override int GetInt32(int ordinal) => (int)_rows[_index][ordinal];
    public override long GetInt64(int ordinal) => (long)_rows[_index][ordinal];
    public override string GetString(int ordinal) => (string)_rows[_index][ordinal];

    // Members required by the abstract base, unused by OptionalTableExporter.
    public override int Depth => 0;
    public override bool HasRows => _rows.Count > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => -1;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
    public override bool NextResult() => false;
    public override Type GetFieldType(int ordinal) => _rows.Count > 0 ? _rows[0][ordinal].GetType() : typeof(object);
    public override byte GetByte(int ordinal) => throw new NotSupportedException();
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
    public override char GetChar(int ordinal) => throw new NotSupportedException();
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
    public override int GetValues(object[] values) => throw new NotSupportedException();
    public override IEnumerator GetEnumerator() => throw new NotSupportedException();
}

public class OptionalTableExporterTests
{
    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-opt-").FullName;

    private static async Task<long> TotalRowsAcrossParts(string outDir, string store, string table, List<OptionalTablePart> parts)
    {
        long total = 0;
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        foreach (var part in parts)
        {
            var absolute = Path.Combine(outDir, store, part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT count(*) FROM read_parquet('{absolute}')";
            total += (long)(await command.ExecuteScalarAsync() ?? 0L);
        }

        return total;
    }

    [Fact]
    public async Task Two_hundred_wide_rows_give_bounded_parts_and_every_row_present()
    {
        const int rows = 200;
        const int rowChars = 2_000_000; // 2 MB of text per row: ~400 MB total, well over the 64 MB chunk bound.
        var outDir = NewOutDir();
        try
        {
            var data = new List<object[]>(rows);
            for (var i = 0; i < rows; i++)
            {
                // Distinct per row: a fixed body plus the row number, built in one string per row (never held
                // as one 400 MB buffer across rows).
                var body = new string('x', rowChars - 10) + i.ToString("D10");
                data.Add([(long)i, body]);
            }

            var reader = new FakeDbDataReader(["k", "payload"], ["int8", "text"], data);
            var request = new OptionalExportRequest("wide_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow.Date, DateTime.UtcNow.Date.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            Assert.Null(result.SkippedReason);
            Assert.Equal(rows, result.RowCount);
            Assert.NotEmpty(result.Parts);
            foreach (var part in result.Parts)
            {
                Assert.True(part.ByteSize < 96 * 1024 * 1024, $"part {part.RelativePath} is {part.ByteSize} bytes");
            }

            var totalRows = await TotalRowsAcrossParts(outDir, "store_a", "wide_opt", result.Parts);
            Assert.Equal(rows, totalRows);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task A_null_in_every_column_type_round_trips()
    {
        var outDir = NewOutDir();
        try
        {
            string[] names = ["a_bool", "a_int", "a_bigint", "a_double", "a_text", "a_timestamp", "a_uuid"];
            string[] types = ["bool", "int4", "int8", "float8", "text", "timestamp", "uuid"];
            var rows = new List<object[]>
            {
                new object[] { DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value },
                new object[] { true, 1, 2L, 3.5, "hi", new DateTime(2026, 1, 1), Guid.NewGuid() },
            };
            var reader = new FakeDbDataReader(names, types, rows);
            var request = new OptionalExportRequest("nullable_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow, DateTime.UtcNow.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            Assert.Equal(2, result.RowCount);
            Assert.Single(result.Parts);
            var absolute = Path.Combine(outDir, "store_a", result.Parts[0].RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT count(*) FILTER (WHERE a_bool IS NULL AND a_int IS NULL AND a_bigint IS NULL " +
                                   $"AND a_double IS NULL AND a_text IS NULL AND a_timestamp IS NULL AND a_uuid IS NULL) " +
                                   $"FROM read_parquet('{absolute}')";
            Assert.Equal(1L, await command.ExecuteScalarAsync());
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task Non_ascii_text_round_trips()
    {
        var outDir = NewOutDir();
        try
        {
            const string text = "caf\u00e9 \u00fcber \u65e5\u672c\u8a9e \ud83d\ude00 \u0645\u0631\u062d\u0628\u0627";
            var reader = new FakeDbDataReader(["v"], ["text"], [[text]]);
            var request = new OptionalExportRequest("nonascii_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow, DateTime.UtcNow.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            var absolute = Path.Combine(outDir, "store_a", result.Parts[0].RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT v FROM read_parquet('{absolute}')";
            Assert.Equal(text, await command.ExecuteScalarAsync());
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task Snapshot_table_stamps_snapshot_time_and_runs_only_at_midnight()
    {
        var outDir = NewOutDir();
        try
        {
            var reader = new FakeDbDataReader(["server_name"], ["text"], [["srv-1"], ["srv-2"]]);
            var midnight = new DateTime(2026, 9, 27, 0, 0, 0, DateTimeKind.Utc);
            var request = new OptionalExportRequest("servers", TimeColumn: null, IsSnapshot: true, midnight, midnight.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            Assert.Null(result.SkippedReason);
            Assert.Equal(2, result.RowCount);
            var absolute = Path.Combine(outDir, "store_a", result.Parts[0].RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT count(*) FILTER (WHERE snapshot_time = TIMESTAMP '2026-09-27 00:00:00') FROM read_parquet('{absolute}')";
            Assert.Equal(2L, await command.ExecuteScalarAsync());
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task Snapshot_table_is_skipped_and_the_reader_factory_is_never_called_off_midnight_once_the_day_is_exported()
    {
        var outDir = NewOutDir();
        try
        {
            var factoryCalled = false;
            Task<DbDataReader> ReaderFactory(CancellationToken _)
            {
                factoryCalled = true;
                return Task.FromResult<DbDataReader>(new FakeDbDataReader(["server_name"], ["text"], [["srv-1"]]));
            }

            var thirteenHundred = new DateTime(2026, 9, 27, 13, 0, 0, DateTimeKind.Utc);
            var request = new OptionalExportRequest("servers", TimeColumn: null, IsSnapshot: true, thirteenHundred, thirteenHundred.AddHours(1))
            {
                DayAlreadyExported = true,
            };

            var result = await OptionalTableExporter.ExportAsync(request, "store_a", outDir, ReaderFactory, CancellationToken.None);

            Assert.False(factoryCalled);
            Assert.Equal("skipped: the daily snapshot for 2026-09-27 is already exported", result.SkippedReason);
            Assert.Equal(0, result.RowCount);
            Assert.Empty(result.Parts);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task An_empty_reader_gives_zero_rows_and_one_empty_schema_only_file()
    {
        // Follows TableExporter/ChunkedParquetWriter's convention: always exactly one output file, even for an
        // empty window, so the loader never special-cases "no file for this table this hour".
        var outDir = NewOutDir();
        try
        {
            var reader = new FakeDbDataReader(["k"], ["int8"], []);
            var request = new OptionalExportRequest("empty_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow, DateTime.UtcNow.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            Assert.Equal(0, result.RowCount);
            Assert.Single(result.Parts);
            Assert.Equal(0, result.Parts[0].RowCount);
            var absolute = Path.Combine(outDir, "store_a", result.Parts[0].RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT count(*) FROM read_parquet('{absolute}')";
            Assert.Equal(0L, await command.ExecuteScalarAsync());
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task Synthetic_wide_rows_split_at_the_byte_cap_not_the_row_cap()
    {
        // Proves the byte bound still fires well under OptionalTableExporter.ChunkRows (1,000,000): 20 rows of 10 MB
        // each is 200 MB total, so it must split into multiple parts on ChunkedParquetWriter.ChunkBytes (64 MB)
        // alone -- nowhere near a million rows.
        const int rows = 20;
        const int rowChars = 10 * 1024 * 1024;
        var outDir = NewOutDir();
        try
        {
            var data = new List<object[]>(rows);
            for (var i = 0; i < rows; i++)
            {
                var body = new string('z', rowChars - 10) + i.ToString("D10");
                data.Add([(long)i, body]);
            }

            var reader = new FakeDbDataReader(["k", "payload"], ["int8", "text"], data);
            var request = new OptionalExportRequest("wide_split_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow.Date, DateTime.UtcNow.Date.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            Assert.Equal(rows, result.RowCount);
            Assert.True(result.Parts.Count > 1, $"expected more than one part for 200 MB of rows, got {result.Parts.Count}");
            foreach (var part in result.Parts)
            {
                Assert.True(part.ByteSize < 96 * 1024 * 1024, $"part {part.RelativePath} is {part.ByteSize} bytes");
            }

            var totalRows = await TotalRowsAcrossParts(outDir, "store_a", "wide_split_opt", result.Parts);
            Assert.Equal(rows, totalRows);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task Peak_memory_stays_bounded_while_writing_400_megabytes()
    {
        const int rows = 200;
        const int rowChars = 2_000_000; // ~400 MB of text across rows, generated and released one row at a time.
        var outDir = NewOutDir();
        try
        {
            var data = new List<object[]>(rows);
            for (var i = 0; i < rows; i++)
            {
                var body = new string('y', rowChars - 10) + i.ToString("D10");
                data.Add([(long)i, body]);
            }

            var reader = new FakeDbDataReader(["k", "payload"], ["int8", "text"], data);
            var request = new OptionalExportRequest("mem_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow, DateTime.UtcNow.AddHours(1));

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var before = GC.GetTotalMemory(forceFullCollection: true);

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var after = GC.GetTotalMemory(forceFullCollection: true);

            Assert.Equal(rows, result.RowCount);
            var growth = after - before;
            Assert.True(growth < 1L * 1024 * 1024 * 1024, $"managed memory grew by {growth} bytes");
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task Twenty_seven_thousand_narrow_rows_give_exactly_one_part()
    {
        // Real store_a finding, 2026-09-28: job_history's 27,128-row hour went out as 28 twelve-KB parts because the
        // exporter hit ChunkedParquetWriter.ChunkRows (1,000) long before the 64 MB byte bound -- narrow rows never
        // needed a new part for memory, only because of the row count. OptionalTableExporter.ChunkRows raises that
        // to 1,000,000 so a narrow table's hour stays in one part; ChunkBytes (64 MB) is still what protects memory.
        const int rows = 27_000;
        var outDir = NewOutDir();
        try
        {
            var data = new List<object[]>(rows);
            for (var i = 0; i < rows; i++)
            {
                data.Add([(long)i, (long)(i % 7), $"row-{i}"]);
            }

            var reader = new FakeDbDataReader(["id", "code", "name"], ["int8", "int4", "text"], data);
            var request = new OptionalExportRequest("narrow_opt", TimeColumn: null, IsSnapshot: false, DateTime.UtcNow.Date, DateTime.UtcNow.Date.AddHours(1));

            var result = await OptionalTableExporter.ExportAsync(
                request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

            Assert.Null(result.SkippedReason);
            Assert.Equal(rows, result.RowCount);
            Assert.Single(result.Parts);
            Assert.Equal(rows, result.Parts[0].RowCount);

            var totalRows = await TotalRowsAcrossParts(outDir, "store_a", "narrow_opt", result.Parts);
            Assert.Equal(rows, totalRows);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task ExportAsync_throws_for_a_dimension_kind_request()
    {
        // pg_statement_text (docs/optional-tables.md §8.2, lane O7) is Kind Dimension and must never go through
        // this generic Window/Snapshot path -- it has its own exporter (PgStatementTextExporter), fed by the
        // same run's pg_statement_stats keys, not by a store-side window read.
        var outDir = NewOutDir();
        try
        {
            var request = new OptionalExportRequest("pg_statement_text", TimeColumn: null, IsSnapshot: false,
                DateTime.UtcNow, DateTime.UtcNow.AddHours(1))
            {
                IsDimension = true,
            };

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                OptionalTableExporter.ExportAsync(
                    request, "store_pg", outDir,
                    _ => Task.FromResult<DbDataReader>(new FakeDbDataReader(["k"], ["int8"], [])),
                    CancellationToken.None));

            Assert.Contains("pg_statement_text", ex.Message);
            Assert.Contains("Dimension", ex.Message);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }
}
