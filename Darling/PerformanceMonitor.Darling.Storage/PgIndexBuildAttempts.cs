// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9289: the record of a start-path index build that ran out of time. A build that times out rolls back whole, and without a record every
/// later start retried it the same way and held the collectors for the full per-statement timeout (the nightly restart included), so the
/// index never landed. The record holds when the attempt was made, the table's uncompressed heap pages then, and how long the build ran.
/// <see cref="PgTableTuning"/> skips the build while the record is under <see cref="RetryAfter"/> old and the table is no smaller than it
/// was. One row per index name, in a runtime table created beside the first record (like <c>collect.module_map_state</c>; not a migration).
/// A successful build deletes the row.
/// </summary>
internal static class PgIndexBuildAttempts
{
    /// <summary>How long a recorded timeout keeps the build from being tried again, unless the table has shrunk below the recorded pages.</summary>
    internal static readonly TimeSpan RetryAfter = TimeSpan.FromHours(24);

    internal const string CreateTableSql = @"CREATE TABLE IF NOT EXISTS collect.index_build_attempts (
    index_name text PRIMARY KEY,
    attempted_at timestamp NOT NULL,
    heap_pages bigint NOT NULL,
    elapsed_ms bigint NOT NULL
)";

    private const string ExistsSql = "SELECT pg_catalog.to_regclass('collect.index_build_attempts') IS NOT NULL";

    private const string ReadSql =
        "SELECT a.attempted_at, a.heap_pages, a.elapsed_ms FROM collect.index_build_attempts a WHERE a.index_name = $1";

    private const string WriteSql =
        @"INSERT INTO collect.index_build_attempts (index_name, attempted_at, heap_pages, elapsed_ms) VALUES ($1, $2, $3, $4)
ON CONFLICT (index_name) DO UPDATE SET attempted_at = EXCLUDED.attempted_at, heap_pages = EXCLUDED.heap_pages, elapsed_ms = EXCLUDED.elapsed_ms";

    private const string ClearSql = "DELETE FROM collect.index_build_attempts WHERE index_name = $1";

    /// <summary>The catalog read of a plain store: the table's own pages. A never-vacuumed table (relpages -1) counts as 0.</summary>
    private const string PlainPagesSql =
        "SELECT COALESCE(GREATEST(c.relpages, 0), 0)::bigint FROM pg_catalog.pg_class c WHERE c.oid = pg_catalog.to_regclass($1)::oid";

    /// <summary>The catalog read of a hypertable: the pages of every chunk that is not compressed (a compressed chunk's rows sit in another relation).</summary>
    private const string TimescalePagesSql = @"
SELECT COALESCE(SUM(GREATEST(c.relpages, 0)), 0)::bigint
FROM timescaledb_information.chunks AS ch
JOIN pg_catalog.pg_namespace AS n ON n.nspname = ch.chunk_schema
JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = ch.chunk_name
WHERE ch.hypertable_schema = $1 AND ch.hypertable_name = $2 AND NOT ch.is_compressed";

    internal sealed record Attempt(DateTime AttemptedAtUtc, long HeapPages, long ElapsedMs);

    /// <summary>The uncompressed heap pages of <paramref name="table"/> (<c>schema.table</c>, taken from the statement list, never from input).</summary>
    internal static async Task<long> UncompressedHeapPagesAsync(NpgsqlConnection connection, string table, int timeoutSeconds, CancellationToken cancellationToken)
    {
        if (await QueryStoreBackgroundIndexes.IsHypertableAsync(connection, table, cancellationToken, timeoutSeconds).ConfigureAwait(false))
        {
            var dot = table.IndexOf('.', StringComparison.Ordinal);
            await using var chunks = new NpgsqlCommand(TimescalePagesSql, connection) { CommandTimeout = timeoutSeconds };
            chunks.Parameters.AddWithValue(NpgsqlDbType.Text, table.Substring(0, dot));
            chunks.Parameters.AddWithValue(NpgsqlDbType.Text, table.Substring(dot + 1));
            return Convert.ToInt64(await chunks.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0L, CultureInfo.InvariantCulture);
        }

        await using var plain = new NpgsqlCommand(PlainPagesSql, connection) { CommandTimeout = timeoutSeconds };
        plain.Parameters.AddWithValue(NpgsqlDbType.Text, table);
        return Convert.ToInt64(await plain.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0L, CultureInfo.InvariantCulture);
    }

    /// <summary>The recorded attempt for <paramref name="indexName"/>, or null when there is none (or the table does not exist yet).</summary>
    internal static async Task<Attempt?> ReadAsync(NpgsqlConnection connection, string indexName, int timeoutSeconds, CancellationToken cancellationToken)
    {
        await using (var exists = new NpgsqlCommand(ExistsSql, connection) { CommandTimeout = timeoutSeconds })
        {
            if (await exists.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
            {
                return null;
            }
        }

        await using var command = new NpgsqlCommand(ReadSql, connection) { CommandTimeout = timeoutSeconds };
        command.Parameters.AddWithValue(NpgsqlDbType.Text, indexName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new Attempt(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc), reader.GetInt64(1), reader.GetInt64(2))
            : null;
    }

    internal static async Task WriteAsync(NpgsqlConnection connection, string indexName, Attempt attempt, int timeoutSeconds, CancellationToken cancellationToken)
    {
        await using (var create = new NpgsqlCommand(CreateTableSql, connection) { CommandTimeout = timeoutSeconds })
        {
            await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var command = new NpgsqlCommand(WriteSql, connection) { CommandTimeout = timeoutSeconds };
        command.Parameters.AddWithValue(NpgsqlDbType.Text, indexName);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(attempt.AttemptedAtUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, attempt.HeapPages);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, attempt.ElapsedMs);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static async Task ClearAsync(NpgsqlConnection connection, string indexName, int timeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ClearSql, connection) { CommandTimeout = timeoutSeconds };
        command.Parameters.AddWithValue(NpgsqlDbType.Text, indexName);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>True when the recorded attempt still bars a build: it is under <see cref="RetryAfter"/> old and the table has not shrunk below its recorded pages.</summary>
    internal static bool Bars(Attempt attempt, long currentPages, DateTime nowUtc) =>
        nowUtc - attempt.AttemptedAtUtc < RetryAfter && currentPages >= attempt.HeapPages;
}
