/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #4605: seeds the <c>collect.query_stats</c> hour ledger for a live test that plants raw rows with a direct INSERT. The
/// collector's COPY writes the ledger in its own transaction, so a row a test plants by INSERT has no ledger row of its
/// own, and the count guard (<see cref="IntervalRollupCountGuard"/>) compares the ledger with the hourly rollup. The
/// helper makes the ledger what the writer would have made it: <see cref="QueryStatsHourLedger.RecountSql"/> over the
/// hours the test planted into, and the state row's <c>counted_since</c> moved below them, because the V164 rung sets it
/// to the next whole hour after the store migrated and every test here plants into a fixed past anchor.
///
/// <para>A recount SETS a (server, hour) to raw's count, so calling it again after a later INSERT makes the ledger agree
/// with raw again, which is how a late row the real writer would have counted reaches the ledger. An in-place UPDATE
/// changes no count, so the ledger stays as it was, as it would on a real store.</para>
/// </summary>
internal static class QueryStatsLedgerSeed
{
    /// <summary>A <c>counted_since</c> far below every test anchor: the ledger is taken to cover every hour a test plants.</summary>
    internal static readonly DateTime Low = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Recounts <c>[from, to)</c> from raw (widened to whole hours by the SQL) and returns the ledger rows written and deleted.</summary>
    internal static async Task<(long Written, long Deleted)> RecountAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(QueryStatsHourLedger.RecountSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(from, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(to, DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    /// <summary>Moves the state row's <c>counted_since</c>. The row exists on any migrated store (the rung inserts it).</summary>
    internal static async Task SetCountedSinceAsync(NpgsqlConnection connection, DateTime since, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"UPDATE {QueryStatsHourLedger.StateTable} SET counted_since = $1", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(since, DateTimeKind.Unspecified) });
        var rows = await command.ExecuteNonQueryAsync(ct);
        if (rows != 1)
        {
            throw new InvalidOperationException($"The ledger state table should hold exactly one row; the update touched {rows}.");
        }
    }

    /// <summary>Recounts <c>[from, to)</c> and sets <c>counted_since</c> to <see cref="Low"/>: the ledger now covers and matches every raw row a test planted.</summary>
    internal static async Task SeedAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await RecountAsync(connection, from, to, ct);
        await SetCountedSinceAsync(connection, Low, ct);
    }
}
