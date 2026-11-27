using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// The ORDER BY every Parquet part is written with. A part is sorted by the columns its contract documents (for
/// example <c>server_name</c> and the time column) and then by every other column, so two rows never tie unless they are
/// identical. Rows that tie on the documented columns alone come out of DuckDB in an order that depends on the order
/// they were read, on thread scheduling and on the hash table layout, which makes a re-export of the same data write
/// different bytes and a different sha256.
/// </summary>
public static class TotalOrder
{
    private static string Q(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>
    /// The ORDER BY list: <paramref name="leading"/> first, then each of <paramref name="allColumns"/> that is not already
    /// in it, in the order given. Names are compared case-sensitively, as DuckDB stores them.
    /// </summary>
    public static string Columns(IEnumerable<string> leading, IEnumerable<string> allColumns)
    {
        var ordered = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var column in leading.Concat(allColumns))
        {
            if (seen.Add(column))
            {
                ordered.Add(Q(column));
            }
        }

        if (ordered.Count == 0)
        {
            throw new ArgumentException("A total order needs at least one column.", nameof(allColumns));
        }

        return string.Join(", ", ordered);
    }

    /// <summary>The ORDER BY list for every column of <paramref name="table"/> in <paramref name="connection"/>, <paramref name="leading"/> first.</summary>
    public static string ForTable(DuckDBConnection connection, string table, params string[] leading)
    {
        var columns = new List<string>();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT column_name FROM (DESCRIBE {Q(table)})";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            columns.Add(reader.GetString(0));
        }

        return Columns(leading, columns);
    }
}
