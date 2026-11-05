using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>Discovers a raw table's exportable column list from the store at run time.</summary>
public static class RawColumnDiscovery
{
    /// <summary>
    /// Returns the column names for a raw `collect` table, in ordinal order, with
    /// `server_id` removed. Requires an open connection; used both for real runs and for
    /// `--dry-run`, which is allowed to connect only to discover columns, not to read data.
    /// </summary>
    public static async Task<List<string>> DiscoverAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        var columns = new List<string>();

        await using var command = new NpgsqlCommand(SqlBuilder.RawColumnDiscoverySql, connection);
        command.Parameters.AddWithValue("table", table);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            columns.Add(reader.GetString(0));
        }

        if (columns.Count == 0)
        {
            throw new InvalidOperationException($"No columns found for collect.{table} in information_schema; does the table exist in this store?");
        }

        return columns;
    }
}
