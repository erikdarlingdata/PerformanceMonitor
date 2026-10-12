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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Accumulates the distinct payloads diverted out of ONE collection batch, ready to be flushed into
/// the dimension tables (#1767). The write path hands every diverted value here as it streams the
/// binary COPY; this collapses them by digest so a 200-row batch that shares a handful of cached
/// plans presents each plan once.
///
/// <para>The dedup is not only a size optimization — it is REQUIRED for correctness of the upsert.
/// Postgres raises 21000 ("ON CONFLICT DO UPDATE command cannot affect row a second time") when a
/// single statement presents the same conflict key twice, which a raw per-row array would do on
/// essentially every batch.</para>
///
/// <para>Keyed by the digest's hex string rather than the <c>byte[]</c> itself: array keys hash by
/// REFERENCE in .NET, so a <c>Dictionary&lt;byte[], …&gt;</c> would silently never dedup — every
/// row would look distinct and the batch would fail the moment two rows shared a plan.</para>
/// </summary>
public sealed class PayloadDimensionBatch
{
    private readonly Dictionary<string, Dictionary<string, Entry>> _byDimTable = new(StringComparer.Ordinal);

    /* Digests the batch's rows reference WITHOUT carrying the content (a row written from a digest the host
       already holds), per dim table, keyed by hex so repeats collapse. Kept apart from _byDimTable: these have
       no payload to upsert, only a last_seen to keep alive. */
    private readonly Dictionary<string, Dictionary<string, byte[]>> _touchedByDimTable = new(StringComparer.Ordinal);

    private readonly record struct Entry(byte[] Digest, string Payload);

    /// <summary>True when nothing was diverted — the caller skips the flush entirely.</summary>
    public bool IsEmpty => _byDimTable.Count == 0 && _touchedByDimTable.Count == 0;

    /// <summary>
    /// The dimension tables this batch has content for. Enumeration order is unspecified and does
    /// not matter — each table's upsert is independent, and they share a transaction.
    /// </summary>
    public IReadOnlyCollection<string> DimTables => _byDimTable.Keys;

    /// <summary>
    /// Records one diverted payload. Repeat sightings of the same content within the batch collapse
    /// onto the first, which is what makes the flush arrays conflict-free.
    /// </summary>
    public void Add(string dimTable, byte[] digest, string payload)
    {
        if (dimTable is null)
        {
            throw new ArgumentNullException(nameof(dimTable));
        }

        if (digest is null)
        {
            throw new ArgumentNullException(nameof(digest));
        }

        if (payload is null)
        {
            throw new ArgumentNullException(nameof(payload));
        }

        if (!_byDimTable.TryGetValue(dimTable, out var entries))
        {
            entries = new Dictionary<string, Entry>(StringComparer.Ordinal);
            _byDimTable[dimTable] = entries;
        }

        var key = Convert.ToHexString(digest);
        if (!entries.ContainsKey(key))
        {
            entries[key] = new Entry(digest, payload);
        }
    }

    /// <summary>
    /// Records a digest a row references with no content: the dim row is already stored, and the flush must
    /// keep its <c>last_seen</c> alive (see <see cref="PayloadDimensionWriter.FlushAsync"/>).
    /// </summary>
    public void AddTouch(string dimTable, byte[] digest)
    {
        if (dimTable is null)
        {
            throw new ArgumentNullException(nameof(dimTable));
        }

        if (digest is null)
        {
            throw new ArgumentNullException(nameof(digest));
        }

        if (!_touchedByDimTable.TryGetValue(dimTable, out var touched))
        {
            touched = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            _touchedByDimTable[dimTable] = touched;
        }

        touched.TryAdd(Convert.ToHexString(digest), digest);
    }

    /// <summary>The dimension tables holding digests referenced without content.</summary>
    public IReadOnlyCollection<string> TouchedDimTables => _touchedByDimTable.Keys;

    /// <summary>
    /// The distinct touched digests for one dimension table, in digest order: the same total order
    /// <see cref="ToArrays"/> imposes, for the same reason (the touch takes row locks too).
    /// </summary>
    public byte[][] TouchedDigests(string dimTable)
        => _touchedByDimTable.TryGetValue(dimTable, out var touched)
            ? touched.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => e.Value).ToArray()
            : Array.Empty<byte[]>();

    /// <summary>How many distinct payloads this batch holds for one dimension table.</summary>
    public int DistinctCount(string dimTable)
        => _byDimTable.TryGetValue(dimTable, out var entries) ? entries.Count : 0;

    /// <summary>
    /// The two parallel arrays the upsert's <c>unnest($1::bytea[], $2::text[])</c> consumes. Index
    /// alignment between them is the contract, so they are built in one pass over the same source.
    /// </summary>
    public (byte[][] Digests, string[] Payloads) ToArrays(string dimTable)
    {
        if (!_byDimTable.TryGetValue(dimTable, out var entries))
        {
            return (Array.Empty<byte[]>(), Array.Empty<string>());
        }

        var digests = new byte[entries.Count][];
        var payloads = new string[entries.Count];
        var i = 0;
        /* Ordered by digest, and the ORDER is a correctness requirement, not tidiness: the upsert takes a row
           lock per conflict key, so two concurrent sessions whose batches share two digests in OPPOSITE
           relative order each hold the lock the other needs next and deadlock (40P01). Every session emitting
           the same total order makes the cycle impossible. Dictionary enumeration is per-session ARRIVAL
           order, which is exactly the absence of one.

           Sorting the hex key ordinally IS byte-wise digest order: every digest is the same length, and hex
           digits ascend in ASCII in the same sequence as the nibbles they encode. */
        foreach (var entry in entries.OrderBy(e => e.Key, StringComparer.Ordinal))
        {
            digests[i] = entry.Value.Digest;
            payloads[i] = entry.Value.Payload;
            i++;
        }

        return (digests, payloads);
    }
}
