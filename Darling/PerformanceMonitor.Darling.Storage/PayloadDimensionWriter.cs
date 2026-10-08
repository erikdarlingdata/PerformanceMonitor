/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Flushes one collection batch's diverted payloads into the dimension tables (#1767).
///
/// <para>Runs inside the SAME transaction as the batch's fact COPY. That is the ordering guarantee
/// the design actually needs: not "dims are inserted before facts" as a statement sequence, but
/// "no session ever observes a fact row whose digest has no dim row". A transaction gives the
/// stronger property, and gives it even though the digests are only KNOWN after the COPY has
/// streamed them — the payloads are discovered while writing the rows that reference them, so a
/// literal dims-first pass would mean calling the definition's WritePayload twice, which is not
/// safe: WritePayload computes and CONSUMES the delta state (context.Deltas.CalculateDelta), so a
/// second pass would report every delta as zero.</para>
/// </summary>
public static class PayloadDimensionWriter
{
    /// <summary>
    /// Upserts every distinct payload the batch accumulated. One statement per dimension table,
    /// each carrying the batch as two parallel arrays, so a 200-row batch costs two round trips
    /// rather than 400. Callers pass the batch's own collection time as the last-seen watermark so
    /// it matches the fact rows exactly (and so a backfilled/replayed batch cannot stamp content as
    /// fresher than the rows referencing it).
    ///
    /// <para><paramref name="commandTimeoutSeconds"/> is OPTIONAL and defaults to null, which leaves the
    /// command on Npgsql's own default exactly as before — this method is shared with the general
    /// per-collector payload flush, whose observed store phases run 1.8–11.4s and which #2776 has no
    /// evidence against. Only the Query Store plan-fetch caller opts in, because that is the path where a
    /// 12 MB flush plus map upsert in one transaction was being cancelled at 30s.</para>
    ///
    /// <para>Digests the batch's rows reference WITHOUT content (<see cref="PayloadDimensionBatch.AddTouch"/>)
    /// get one more statement per dimension table in this same transaction, <see cref="PayloadDimensions.TouchSql"/>,
    /// which keeps their <c>last_seen</c> alive under the same <see cref="PayloadDimensions.LastSeenRefreshGuardHours"/>
    /// guard as the insert path, so the dimension GC's liveness invariant is unchanged. The result is the
    /// touched digests that have NO dim row, as upper-case hex (the form <c>Convert.ToHexString</c> gives)
    /// whatever case the known digest was offered in, so a caller must compare case-insensitively or upper-case
    /// its own digests first; empty when there are none, which is every batch that touches nothing.</para>
    /// </summary>
    public static async Task<IReadOnlyList<string>> FlushAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PayloadDimensionBatch batch,
        DateTime collectionTime,
        CancellationToken cancellationToken,
        bool compressPlanContent = true,
        int? commandTimeoutSeconds = null,
        ILogger? logger = null)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (batch is null)
        {
            throw new ArgumentNullException(nameof(batch));
        }

        if (batch.IsEmpty)
        {
            return Array.Empty<string>();
        }

        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
        var lastSeen = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);

        foreach (var dimTable in batch.DimTables)
        {
            var (digests, payloads) = batch.ToArrays(dimTable);
            if (digests.Length == 0)
            {
                continue;
            }

            /* #2069: the plan dim stores gzip bytes (measured 14.0x vs lz4-TOAST's 8.9x on live
               content). Compressed HERE — one seam — with the digest untouched: it was computed
               over the uncompressed text upstream, so content identity is format-stable.
               #2171: plan_xml_compression = 'none' turns the seam off — the plan dim takes the same
               text path as every other dim, and lz4 TOAST carries the compression so direct-SQL
               consumers can read query_plan_xml bare. The digest is identical either way. */
            var compress = compressPlanContent
                && string.Equals(dimTable, PayloadDimensions.CompressedContentDimTable, StringComparison.Ordinal);

            await using var command = new NpgsqlCommand(PayloadDimensions.UpsertSql(dimTable, compress), connection, transaction);
            if (commandTimeoutSeconds is int timeout)
            {
                command.CommandTimeout = timeout;
            }

            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea,
                Value = digests,
            });
            command.Parameters.Add(compress
                ? new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea,
                    Value = payloads.Select(PayloadDimensions.CompressContent).ToArray(),
                }
                : new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
                    Value = payloads,
                });
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Timestamp,
                Value = lastSeen,
            });

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        List<string>? absent = null;
        foreach (var dimTable in batch.TouchedDimTables)
        {
            var touched = batch.TouchedDigests(dimTable);
            if (touched.Length == 0)
            {
                continue;
            }

            await using var command = new NpgsqlCommand(PayloadDimensions.TouchSql(dimTable), connection, transaction);
            if (commandTimeoutSeconds is int timeout)
            {
                command.CommandTimeout = timeout;
            }

            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea,
                Value = touched,
            });
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Timestamp,
                Value = lastSeen,
            });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                (absent ??= new List<string>()).Add(Convert.ToHexString(reader.GetFieldValue<byte[]>(0)));
            }
        }

        if (absent is null)
        {
            return Array.Empty<string>();
        }

        logger?.LogDebug(
            "{AbsentDigests} referenced payload digest(s) have no dimension row; their rows will read without content",
            absent.Count);
        return absent;
    }
}
