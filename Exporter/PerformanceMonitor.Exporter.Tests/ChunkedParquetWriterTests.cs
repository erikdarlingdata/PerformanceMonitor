using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

// DuckDbSession's memory limit and thread count are process-wide statics. The wide-row test raises the limit to 2 GB;
// DuckDbSessionTests sets it to 512 MB. In different collections xunit runs them in parallel, and the wide-row test then
// fails with "Out of Memory Error ... (487.3 MiB/488.2 MiB used)". One collection runs them one after the other.
[Collection("DuckDbSession statics")]
public class ChunkedParquetWriterTests
{
    private static readonly ColumnShape[] Columns = [new("k", "BIGINT"), new("payload", "VARCHAR")];

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(ChunkedParquetWriter.ChunkRows)]
    [InlineData((ChunkedParquetWriter.ChunkRows * 2) + 500)]
    public void Every_row_reaches_the_final_file_exactly_once(int rows)
    {
        var dir = Directory.CreateTempSubdirectory("darling-export-cpw-").FullName;
        var output = Path.Combine(dir, "out.parquet");
        var tempDb = Path.Combine(dir, "t.duckdb");
        try
        {
            using (var writer = new ChunkedParquetWriter(tempDb, "t", Columns))
            {
                for (var i = 0; i < rows; i++)
                {
                    var row = writer.Appender.CreateRow();
                    row.AppendValue((long)i);
                    row.AppendValue($"payload-{i}");
                    row.EndRow();
                    writer.RowAppended();
                }

                writer.Complete(output);
            }

            Assert.False(Directory.Exists(tempDb + ".parts"));
            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT count(*), count(DISTINCT k), coalesce(sum(k), 0)::BIGINT, " +
                $"count(*) FILTER (WHERE payload = 'payload-' || k) FROM read_parquet('{output}')";
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(rows, reader.GetInt64(0));
            Assert.Equal(rows, reader.GetInt64(1));
            Assert.Equal((long)rows * (rows - 1) / 2, reader.GetInt64(2));
            Assert.Equal(rows, reader.GetInt64(3));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Wide_rows_stay_under_the_memory_limit_because_chunks_and_row_groups_are_bounded_by_bytes()
    {
        // 1,000 distinct rows of 2.5 MB (2.5 GB of text) under the old 2 GB DuckDB limit. With row-count chunks alone
        // this is one 1,000-row chunk and one 1,000-row row group, and it fails the way the scheduled store_a export did
        // on 2026-09-27 ("1.9 GiB/1.8 GiB used"). Every row must differ: identical strings hide the problem. Below
        // about 2 GB a parquet COPY fails whatever the row groups (erikdarlingdata/PerformanceMonitor#933), so the
        // test does not go lower.
        const int rows = 1000;
        const int rowChars = 2_500_000;
        var previousLimit = DuckDbSession.MemoryLimit;
        var previousThreads = DuckDbSession.Threads;
        var dir = Directory.CreateTempSubdirectory("darling-export-cpw-wide-").FullName;
        var output = Path.Combine(dir, "out.parquet");
        var random = new Random(7);
        const string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789<>/= ";
        var body = new char[rowChars];
        for (var c = 0; c < rowChars; c++)
        {
            body[c] = alphabet[random.Next(alphabet.Length)];
        }

        try
        {
            DuckDbSession.Configure("2GB", 2);
            using (var writer = new ChunkedParquetWriter(Path.Combine(dir, "t.duckdb"), "t", Columns))
            {
                for (var i = 0; i < rows; i++)
                {
                    var prefix = i.ToString("D8");
                    prefix.CopyTo(0, body, 0, prefix.Length);
                    var row = writer.Appender.CreateRow();
                    row.AppendValue((long)i);
                    row.AppendValue(new string(body));
                    row.EndRow();
                    writer.RowAppended(rowChars);
                }

                writer.Complete(output);
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT (SELECT count(*) FROM read_parquet('{output}')), " +
                $"(SELECT count(DISTINCT k) FROM read_parquet('{output}')), " +
                $"(SELECT max(row_group_num_rows) FROM parquet_metadata('{output}'))";
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(rows, reader.GetInt64(0));
            Assert.Equal(rows, reader.GetInt64(1));
            // 64 MB / 2.5 MB per row is 26 rows; without the byte bound one row group holds all 1,000 (2.5 GB).
            Assert.True(reader.GetInt64(2) <= 27, $"largest row group has {reader.GetInt64(2)} rows");
        }
        finally
        {
            DuckDbSession.Configure(previousLimit, previousThreads);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void An_empty_file_still_carries_the_schema()
    {
        var dir = Directory.CreateTempSubdirectory("darling-export-cpw-").FullName;
        var output = Path.Combine(dir, "out.parquet");
        try
        {
            using (var writer = new ChunkedParquetWriter(Path.Combine(dir, "t.duckdb"), "t", Columns))
            {
                writer.Complete(output);
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT string_agg(column_name || ' ' || column_type, ', ') FROM (DESCRIBE SELECT * FROM read_parquet('{output}'))";
            Assert.Equal("k BIGINT, payload VARCHAR", command.ExecuteScalar());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}

[Collection("DuckDbSession statics")]
public class DuckDbSessionTests
{
    [Theory]
    [InlineData("2GB")]
    [InlineData("512MB")]
    [InlineData("1.5GiB")]
    public void Configure_accepts_memory_sizes(string limit) => DuckDbSession.Configure(limit, 2);

    [Theory]
    [InlineData("2")]
    [InlineData("2GB'; DROP TABLE x; --")]
    [InlineData("lots")]
    public void Configure_rejects_anything_else(string limit) =>
        Assert.Throws<ArgumentException>(() => DuckDbSession.Configure(limit, 2));

    [Theory]
    [InlineData(0)]
    [InlineData(65)]
    public void Configure_rejects_out_of_range_threads(int threads) =>
        Assert.Throws<ArgumentException>(() => DuckDbSession.Configure("2GB", threads));
}
