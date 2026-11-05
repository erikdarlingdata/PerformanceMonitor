using DuckDB.NET.Data;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Streams a raw window read (<see cref="NpgsqlDataReader"/>) into a DuckDB table through the appender.
/// Shared by the procedure_stats, query_stats and Query Store windows. The store spends its time on these
/// reads waiting for the client to drain rows, so the per-cell work is kept minimal: the reader runs
/// with SequentialAccess and async reads, and the getter for each column is chosen once before the row
/// loop (from the DuckDB column type and the reader's actual field type), so no cell is boxed and no
/// type name is compared per cell. Values are identical to the old boxed path.
/// </summary>
internal static class RawWindowAppender
{
    private enum Kind
    {
        Int32, Int64, Text, Timestamp, Blob, Bool,
        BoxedInt32, BoxedInt64, BoxedText, BoxedBool,
        Int16, Single, Double, Decimal, Date, Guid,
        BoxedInt16, BoxedSingle, BoxedDouble,
    }

    /// <summary>The reader behaviour every raw window read uses.</summary>
    internal const System.Data.CommandBehavior Behavior = System.Data.CommandBehavior.SequentialAccess;

    private static Kind Resolve(string duckDbType, Type fieldType) => duckDbType switch
    {
        "INTEGER" => fieldType == typeof(int) ? Kind.Int32 : Kind.BoxedInt32,
        "BIGINT" => fieldType == typeof(long) ? Kind.Int64 : Kind.BoxedInt64,
        "VARCHAR" => fieldType == typeof(string) ? Kind.Text : Kind.BoxedText,
        "TIMESTAMP" => Kind.Timestamp,
        "BLOB" => Kind.Blob,
        "BOOLEAN" => fieldType == typeof(bool) ? Kind.Bool : Kind.BoxedBool,
        "SMALLINT" => fieldType == typeof(short) ? Kind.Int16 : Kind.BoxedInt16,
        "REAL" => fieldType == typeof(float) ? Kind.Single : Kind.BoxedSingle,
        // DOUBLE also receives unconstrained / wider-than-38 numeric (decimal), converted like the old Convert.ToDouble.
        "DOUBLE" => fieldType == typeof(double) ? Kind.Double : Kind.BoxedDouble,
        "TIMESTAMPTZ" => Kind.Timestamp,
        "DATE" => Kind.Date,
        "UUID" => Kind.Guid,
        var t when t.StartsWith("DECIMAL(", StringComparison.Ordinal) => Kind.Decimal,
        _ => throw new NotSupportedException($"No appender case for DuckDB type '{duckDbType}'."),
    };

    /// <summary>Wall ms the last call spent reading rows off the socket (the time the store waits on us). Benchmark use only.</summary>
    internal static double LastDrainMs;

    /// <summary>Rows per hand-off batch.</summary>
    internal const int BatchRows = 8192;

    /// <summary>Batches that may sit between the drain task and the append task (bounds memory at ~64 x 8192 rows).</summary>
    internal const int QueuedBatches = 64;

    private sealed class Batch
    {
        public int Rows;
        public readonly long[]?[] Longs;
        public readonly int[]?[] Ints;
        public readonly string?[]?[] Strings;
        public readonly DateTime[]?[] Times;
        public readonly byte[]?[]?[] Blobs;
        public readonly bool[]?[] Bools;
        public readonly short[]?[] Shorts;
        public readonly float[]?[] Floats;
        public readonly double[]?[] Doubles;
        public readonly decimal[]?[] Decimals;
        public readonly Guid[]?[] Guids;
        public readonly bool[][] Nulls;

        public Batch(Kind[] kinds)
        {
            var n = kinds.Length;
            Longs = new long[]?[n]; Ints = new int[]?[n]; Strings = new string?[]?[n]; Times = new DateTime[]?[n];
            Blobs = new byte[]?[]?[n]; Bools = new bool[]?[n];
            Shorts = new short[]?[n]; Floats = new float[]?[n]; Doubles = new double[]?[n]; Decimals = new decimal[]?[n]; Guids = new Guid[]?[n];
            Nulls = new bool[n][];
            for (var i = 0; i < n; i++)
            {
                Nulls[i] = new bool[BatchRows];
                switch (kinds[i])
                {
                    case Kind.Int32: case Kind.BoxedInt32: Ints[i] = new int[BatchRows]; break;
                    case Kind.Int64: case Kind.BoxedInt64: Longs[i] = new long[BatchRows]; break;
                    case Kind.Text: case Kind.BoxedText: Strings[i] = new string?[BatchRows]; break;
                    case Kind.Timestamp: case Kind.Date: Times[i] = new DateTime[BatchRows]; break;
                    case Kind.Int16: case Kind.BoxedInt16: Shorts[i] = new short[BatchRows]; break;
                    case Kind.Single: case Kind.BoxedSingle: Floats[i] = new float[BatchRows]; break;
                    case Kind.Double: case Kind.BoxedDouble: Doubles[i] = new double[BatchRows]; break;
                    case Kind.Decimal: Decimals[i] = new decimal[BatchRows]; break;
                    case Kind.Guid: Guids[i] = new Guid[BatchRows]; break;
                    case Kind.Blob: Blobs[i] = new byte[]?[BatchRows]; break;
                    default: Bools[i] = new bool[BatchRows]; break;
                }
            }
        }
    }

    /// <summary>
    /// Reads every remaining row of <paramref name="reader"/> (opened with <see cref="Behavior"/>) into
    /// <paramref name="tableName"/>. The socket is drained on this task into typed column batches while a second task
    /// appends finished batches to DuckDB, so the store never waits on the appender: it only waits on the drain.
    /// <paramref name="duckDbTypes"/> describes the reader's columns; when <paramref name="leadingText"/> is not null
    /// it is appended as an extra first VARCHAR column on every row (the generic export's source_store).
    /// Returns the number of rows appended.
    /// </summary>
    internal static async Task<long> AppendAllAsync(
        NpgsqlDataReader reader, DuckDBConnection duckConnection, string tableName,
        IReadOnlyList<string> duckDbTypes, CancellationToken ct, string? leadingText = null)
    {
        long totalRows = 0;
        var count = duckDbTypes.Count;
        var kinds = new Kind[count];
        for (var i = 0; i < count; i++)
        {
            kinds[i] = Resolve(duckDbTypes[i], reader.GetFieldType(i));
        }

        var channel = System.Threading.Channels.Channel.CreateBounded<Batch>(
            new System.Threading.Channels.BoundedChannelOptions(QueuedBatches) { SingleReader = true, SingleWriter = true });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        double drainMs = 0;
        var swTotal = System.Diagnostics.Stopwatch.StartNew();
        var appendTask = Task.Run(async () =>
        {
            try
            {
                using var appender = duckConnection.CreateAppender(tableName);
                await foreach (var batch in channel.Reader.ReadAllAsync(cts.Token).ConfigureAwait(false))
                {
                    AppendBatch(appender, batch, kinds, leadingText);
                }
            }
            catch
            {
                cts.Cancel();
                throw;
            }
        }, CancellationToken.None);

        try
        {
            var batch = new Batch(kinds);
            while (await reader.ReadAsync(cts.Token).ConfigureAwait(false))
            {
                var r = batch.Rows++;
                totalRows++;
                for (var i = 0; i < count; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        batch.Nulls[i][r] = true;
                        continue;
                    }

                    batch.Nulls[i][r] = false;
                    switch (kinds[i])
                    {
                        case Kind.Int32: batch.Ints[i]![r] = reader.GetInt32(i); break;
                        case Kind.Int64: batch.Longs[i]![r] = reader.GetInt64(i); break;
                        case Kind.Text: batch.Strings[i]![r] = reader.GetString(i); break;
                        case Kind.Timestamp: batch.Times[i]![r] = reader.GetDateTime(i); break;
                        case Kind.Blob: batch.Blobs[i]![r] = reader.GetFieldValue<byte[]>(i); break;
                        case Kind.Bool: batch.Bools[i]![r] = reader.GetBoolean(i); break;
                        case Kind.BoxedInt32: batch.Ints[i]![r] = Convert.ToInt32(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture); break;
                        case Kind.BoxedInt64: batch.Longs[i]![r] = Convert.ToInt64(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture); break;
                        case Kind.BoxedText: batch.Strings[i]![r] = reader.GetValue(i).ToString(); break;
                        case Kind.Int16: batch.Shorts[i]![r] = reader.GetInt16(i); break;
                        case Kind.Single: batch.Floats[i]![r] = reader.GetFloat(i); break;
                        case Kind.Double: batch.Doubles[i]![r] = reader.GetDouble(i); break;
                        case Kind.Decimal: batch.Decimals[i]![r] = reader.GetDecimal(i); break;
                        case Kind.Date: batch.Times[i]![r] = reader.GetDateTime(i); break;
                        case Kind.Guid: batch.Guids[i]![r] = reader.GetGuid(i); break;
                        case Kind.BoxedInt16: batch.Shorts[i]![r] = Convert.ToInt16(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture); break;
                        case Kind.BoxedSingle: batch.Floats[i]![r] = Convert.ToSingle(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture); break;
                        case Kind.BoxedDouble: batch.Doubles[i]![r] = Convert.ToDouble(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture); break;
                        case Kind.BoxedBool: batch.Bools[i]![r] = Convert.ToBoolean(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture); break;
                    }
                }

                if (batch.Rows == BatchRows)
                {
                    await channel.Writer.WriteAsync(batch, cts.Token).ConfigureAwait(false);
                    batch = new Batch(kinds);
                }
            }

            if (batch.Rows > 0)
            {
                await channel.Writer.WriteAsync(batch, cts.Token).ConfigureAwait(false);
            }

            channel.Writer.Complete();
            drainMs = swTotal.Elapsed.TotalMilliseconds;
        }
        catch (Exception ex)
        {
            channel.Writer.TryComplete(ex);
            cts.Cancel();
            // If the append task failed first, it cancelled the read, so ex is only that cancellation:
            // surface the append failure instead. Otherwise the read failure is the one to surface.
            try { await appendTask.ConfigureAwait(false); }
            catch (Exception appendEx) when (ex is OperationCanceledException && !ct.IsCancellationRequested)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(appendEx).Throw();
            }
            catch (Exception) { }
            throw;
        }

        LastDrainMs = drainMs;
        await appendTask.ConfigureAwait(false);
        return totalRows;
    }

    private static void AppendBatch(DuckDBAppender appender, Batch b, Kind[] kinds, string? leadingText)
    {
        for (var r = 0; r < b.Rows; r++)
        {
            var row = appender.CreateRow();
            if (leadingText is not null)
            {
                row.AppendValue(leadingText);
            }

            for (var i = 0; i < kinds.Length; i++)
            {
                if (b.Nulls[i][r])
                {
                    row.AppendNullValue();
                    continue;
                }

                switch (kinds[i])
                {
                    case Kind.Int32: case Kind.BoxedInt32: row.AppendValue(b.Ints[i]![r]); break;
                    case Kind.Int64: case Kind.BoxedInt64: row.AppendValue(b.Longs[i]![r]); break;
                    case Kind.Text: case Kind.BoxedText: row.AppendValue(b.Strings[i]![r]); break;
                    case Kind.Timestamp: row.AppendValue(b.Times[i]![r]); break;
                    case Kind.Date: row.AppendValue(DateOnly.FromDateTime(b.Times[i]![r])); break;
                    case Kind.Int16: case Kind.BoxedInt16: row.AppendValue(b.Shorts[i]![r]); break;
                    case Kind.Single: case Kind.BoxedSingle: row.AppendValue(b.Floats[i]![r]); break;
                    case Kind.Double: case Kind.BoxedDouble: row.AppendValue(b.Doubles[i]![r]); break;
                    case Kind.Decimal: row.AppendValue(b.Decimals[i]![r]); break;
                    case Kind.Guid: row.AppendValue(b.Guids[i]![r]); break;
                    case Kind.Blob: row.AppendValue(b.Blobs[i]![r]); break;
                    default: row.AppendValue(b.Bools[i]![r]); break;
                }
            }
            row.EndRow();
        }
    }
}
