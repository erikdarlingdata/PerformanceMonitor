using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Tracks the previous per-key value hash for change-only export, in a small on-disk DuckDB file at
/// `&lt;StateDir&gt;/change_state.duckdb`. It is separate from `exported_keys.duckdb` (<see cref="StateStore"/>):
/// losing one never re-baselines the other, and the two files touch disjoint state. `change_state` is
/// append-only and stamped with the window it was written in, so re-running any window is deterministic: a re-run
/// first deletes that window's rows (<see cref="DeleteWindow"/>), then re-records them (<see cref="Record"/>). Store
/// rows are immutable once complete, so the recomputed rows are identical to the ones deleted. <see
/// cref="LoadPrevAsOf"/> copies rather than ATTACHes: this store holds its own connection to `change_state.duckdb`
/// open for the caller's lifetime (read-write, for <see cref="Record"/> and <see cref="DeleteWindow"/>), and
/// DuckDB.NET 1.5.5 does not let the exporter's separate in-memory connection ATTACH that same file concurrently
/// without the two connections stepping on each other's transactions. A plain query on this store's own connection,
/// followed by an appender insert into a temp table on the target connection, avoids that entirely and the row counts
/// here (one row per tracked key per store/table) are small enough that copying costs nothing worth avoiding.
/// </summary>
public sealed class ChangeStateStore : IDisposable
{
    private readonly DuckDBConnection _connection;
    private int _tempTableSeq;

    public bool WasRecreated { get; private set; }

    private ChangeStateStore(DuckDBConnection connection, bool wasRecreated)
    {
        _connection = connection;
        WasRecreated = wasRecreated;
    }

    /// <summary>
    /// Opens (creating if needed) the state file at `&lt;stateDir&gt;/change_state.duckdb` and ensures
    /// its tables exist. If the existing file is corrupt or unreadable, it is renamed to
    /// `change_state.duckdb.corrupt-&lt;utc&gt;` and a fresh file is created in its place, with
    /// <see cref="WasRecreated"/> set to true so callers can force a baseline window.
    /// </summary>
    public static ChangeStateStore Open(string stateDir)
    {
        Directory.CreateDirectory(stateDir);
        var path = Path.Combine(stateDir, "change_state.duckdb");

        var (connection, wasRecreated) = TryOpen(path);
        return new ChangeStateStore(connection, wasRecreated);
    }

    private static (DuckDBConnection Connection, bool WasRecreated) TryOpen(string path)
    {
        var wasRecreated = false;

        if (File.Exists(path))
        {
            try
            {
                var probe = new DuckDBConnection($"Data Source={path}");
                probe.Open();
                EnsureSchema(probe);
                return (probe, wasRecreated);
            }
            catch
            {
                var corruptPath = $"{path}.corrupt-{DateTime.UtcNow:yyyyMMddHHmmssfff}";
                try
                {
                    File.Move(path, corruptPath);
                }
                catch
                {
                    // The file may be locked or already gone; fall through and create a fresh one regardless.
                }
                wasRecreated = true;
            }
        }

        var connection = new DuckDBConnection($"Data Source={path}");
        connection.Open();
        EnsureSchema(connection);
        return (connection, wasRecreated);
    }

    private static void EnsureSchema(DuckDBConnection connection)
    {
        using var createChangeState = connection.CreateCommand();
        createChangeState.CommandText =
            "CREATE TABLE IF NOT EXISTS change_state (source_store VARCHAR, table_name VARCHAR, " +
            "key_hash VARCHAR, value_hash VARCHAR, window_to TIMESTAMP)";
        createChangeState.ExecuteNonQuery();

        using var createChangeMeta = connection.CreateCommand();
        createChangeMeta.CommandText =
            "CREATE TABLE IF NOT EXISTS change_meta (source_store VARCHAR, table_name VARCHAR, " +
            "tracked_set_hash VARCHAR, updated_at TIMESTAMP)";
        createChangeMeta.ExecuteNonQuery();
    }

    private string NextTempTableName(string prefix) => $"{prefix}_{Interlocked.Increment(ref _tempTableSeq)}";

    /// <summary>
    /// Loads the previous value hash per key as of window <paramref name="from"/> (i.e. every row
    /// with `window_to &lt;= from`, collapsed to the latest hash per key via `arg_max`) into a fresh
    /// temp table on <paramref name="target"/>, and returns that temp table's name. The temp table
    /// has columns `(key_hash VARCHAR, value_hash VARCHAR)`.
    /// </summary>
    public string LoadPrevAsOf(string store, string table, DateTime from, DuckDBConnection target)
    {
        var rows = new List<(string KeyHash, string ValueHash)>();
        using (var command = _connection.CreateCommand())
        {
            command.CommandText =
                "SELECT key_hash, arg_max(value_hash, window_to) AS value_hash FROM change_state " +
                "WHERE source_store = $store AND table_name = $table AND window_to <= $from " +
                "GROUP BY key_hash";
            command.Parameters.Add(new DuckDBParameter("store", store));
            command.Parameters.Add(new DuckDBParameter("table", table));
            command.Parameters.Add(new DuckDBParameter("from", from));
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                rows.Add((reader.GetString(0), reader.GetString(1)));
            }
        }

        var tempTable = NextTempTableName("prev_asof");
        using (var createTemp = target.CreateCommand())
        {
            createTemp.CommandText = $"CREATE TEMP TABLE {tempTable} (key_hash VARCHAR, value_hash VARCHAR)";
            createTemp.ExecuteNonQuery();
        }

        if (rows.Count > 0)
        {
            using var appender = target.CreateAppender(tempTable);
            foreach (var (keyHash, valueHash) in rows)
            {
                var row = appender.CreateRow();
                row.AppendValue(keyHash);
                row.AppendValue(valueHash);
                row.EndRow();
            }
        }

        return tempTable;
    }

    /// <summary>Deletes every `change_state` row for (store, table) stamped with the given window, so a re-run can recompute it.</summary>
    public void DeleteWindow(string store, string table, DateTime windowTo)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM change_state WHERE source_store = $store AND table_name = $table AND window_to = $windowTo";
        command.Parameters.Add(new DuckDBParameter("store", store));
        command.Parameters.Add(new DuckDBParameter("table", table));
        command.Parameters.Add(new DuckDBParameter("windowTo", windowTo));
        command.ExecuteNonQuery();
    }

    /// <summary>Bulk-records one `change_state` row per (KeyHash, ValueHash) pair for the given window, through the appender.</summary>
    public void Record(string store, string table, IEnumerable<(string KeyHash, string ValueHash)> rows, DateTime windowTo)
    {
        using var appender = _connection.CreateAppender("change_state");
        foreach (var (keyHash, valueHash) in rows)
        {
            var row = appender.CreateRow();
            row.AppendValue(store);
            row.AppendValue(table);
            row.AppendValue(keyHash);
            row.AppendValue(valueHash);
            row.AppendValue(windowTo);
            row.EndRow();
        }
    }

    /// <summary>Returns the last recorded tracked-set hash for (store, table), or null if none has been recorded yet.</summary>
    public string? GetTrackedSetHash(string store, string table)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT tracked_set_hash FROM change_meta WHERE source_store = $store AND table_name = $table";
        command.Parameters.Add(new DuckDBParameter("store", store));
        command.Parameters.Add(new DuckDBParameter("table", table));
        using var reader = command.ExecuteReader();
        return reader.Read() ? reader.GetString(0) : null;
    }

    /// <summary>Replaces the recorded tracked-set hash for (store, table) with <paramref name="hash"/>.</summary>
    public void SetTrackedSetHash(string store, string table, string hash)
    {
        using var transaction = _connection.BeginTransaction();

        using (var delete = _connection.CreateCommand())
        {
            delete.CommandText = "DELETE FROM change_meta WHERE source_store = $store AND table_name = $table";
            delete.Parameters.Add(new DuckDBParameter("store", store));
            delete.Parameters.Add(new DuckDBParameter("table", table));
            delete.ExecuteNonQuery();
        }

        using (var insert = _connection.CreateCommand())
        {
            insert.CommandText =
                "INSERT INTO change_meta (source_store, table_name, tracked_set_hash, updated_at) VALUES ($store, $table, $hash, $now)";
            insert.Parameters.Add(new DuckDBParameter("store", store));
            insert.Parameters.Add(new DuckDBParameter("table", table));
            insert.Parameters.Add(new DuckDBParameter("hash", hash));
            insert.Parameters.Add(new DuckDBParameter("now", DateTime.UtcNow));
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    /// <summary>
    /// Per (store, table, key), keeps the newest row with `window_to &lt; nowUtc - keepDays` and deletes
    /// the older rows for that key; every row with `window_to &gt;= nowUtc - keepDays` is kept regardless.
    /// This bounds the file's growth while leaving the as-of answer (<see cref="LoadPrevAsOf"/>) unchanged
    /// for any `from` at or after the cutoff.
    /// </summary>
    public void Prune(DateTime nowUtc, int keepDays = 7)
    {
        var cutoff = nowUtc.AddDays(-keepDays);
        using var command = _connection.CreateCommand();
        command.CommandText =
            "DELETE FROM change_state old WHERE old.window_to < $cutoff AND EXISTS (" +
            "  SELECT 1 FROM change_state newer WHERE newer.source_store = old.source_store" +
            "    AND newer.table_name = old.table_name AND newer.key_hash = old.key_hash" +
            "    AND newer.window_to < $cutoff AND newer.window_to > old.window_to" +
            ")";
        command.Parameters.Add(new DuckDBParameter("cutoff", cutoff));
        command.ExecuteNonQuery();
    }

    /// <summary>Returns the earliest recorded `window_to` for (store, table), or null if nothing has been recorded.</summary>
    public DateTime? MinWindowTo(string store, string table)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT min(window_to) FROM change_state WHERE source_store = $store AND table_name = $table";
        command.Parameters.Add(new DuckDBParameter("store", store));
        command.Parameters.Add(new DuckDBParameter("table", table));
        using var reader = command.ExecuteReader();
        if (!reader.Read() || reader.IsDBNull(0))
        {
            return null;
        }
        return reader.GetDateTime(0);
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}
