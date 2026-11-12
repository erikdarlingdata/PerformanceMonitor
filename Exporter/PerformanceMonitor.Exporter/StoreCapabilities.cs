using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Detects store-level capabilities that change what the exporter does, without the caller
/// needing to know the underlying information_schema check. Currently only one: whether Query
/// Store data (`collect.query_store_stats`) exists in this store at all, so a store that
/// predates Query Store collection (or has it disabled) behaves exactly as it did before Query
/// Store support was added, instead of erroring on a table that isn't there.
/// </summary>
public static class StoreCapabilities
{
    public static async Task<bool> HasQueryStoreStatsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "/* darling-export:capability.query_store_stats */ SELECT 1 FROM information_schema.tables WHERE table_schema = 'collect' AND table_name = 'query_store_stats'",
            connection);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is not null;
    }

    /// <summary>
    /// Thrown by the pre-flight guard (<see cref="ExportRunner"/>) when one or more core hypertables have a
    /// compressed chunk overlapping their scan window and `--allow-compressed` was not set. Lists every offending
    /// (table, chunk) pair in one message, so the whole run refuses loudly BEFORE any store read of table data, rather
    /// than one table failing after every other table has already been read for nothing.
    /// </summary>
    public sealed class CompressedChunkException(IReadOnlyList<(string Table, string ChunkName)> offenders)
        : Exception(BuildMessage(offenders))
    {
        public IReadOnlyList<(string Table, string ChunkName)> Offenders { get; } = offenders;

        private static string BuildMessage(IReadOnlyList<(string Table, string ChunkName)> offenders)
        {
            var list = string.Join(", ", offenders.Select(o => $"{o.Table} (chunk '{o.ChunkName}')"));
            return $"Compressed chunk(s) overlap the scan window: {list}; pass --allow-compressed for a manual catch-up run.";
        }
    }

    /// <summary>
    /// The compressed-chunk guard: before reading a big hypertable window, checks whether any chunk overlapping
    /// [scanFrom, scanTo) is compressed. Non-hypertables have no rows in `timescaledb_information.chunks` and are
    /// skipped by callers before this is ever invoked.
    /// </summary>
    public static async Task<IReadOnlyList<(string ChunkName, bool IsCompressed)>> CompressedChunksInWindowAsync(
        NpgsqlConnection connection, string schema, string table, DateTime scanFrom, DateTime scanTo, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            /* darling-export:chunk_guard */
            SELECT chunk_name, is_compressed FROM timescaledb_information.chunks
            WHERE hypertable_schema = @schema AND hypertable_name = @table
              AND range_start < @scan_to AND range_end > @scan_from
            """,
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("table", table);
        command.Parameters.Add(new NpgsqlParameter("scan_from", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(scanFrom, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("scan_to", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(scanTo, DateTimeKind.Unspecified),
        });

        var results = new List<(string ChunkName, bool IsCompressed)>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            results.Add((reader.GetString(0), reader.GetBoolean(1)));
        }

        return results;
    }

    /// <summary>
    /// Runs <see cref="CompressedChunksInWindowAsync"/> and throws <see cref="CompressedChunkException"/> naming the
    /// first compressed chunk found, unless <paramref name="allowCompressed"/> is set (the manual catch-up escape
    /// hatch). No-op when the guard finds no compressed chunk.
    /// </summary>
    public static async Task GuardAgainstCompressedChunksAsync(
        NpgsqlConnection connection, string schema, string table, DateTime scanFrom, DateTime scanTo, bool allowCompressed, CancellationToken ct)
    {
        var chunks = await CompressedChunksInWindowAsync(connection, schema, table, scanFrom, scanTo, ct).ConfigureAwait(false);
        if (allowCompressed)
        {
            return;
        }

        var compressed = chunks.FirstOrDefault(c => c.IsCompressed);
        if (compressed.ChunkName is not null)
        {
            throw new CompressedChunkException([(table, compressed.ChunkName)]);
        }
    }
}
