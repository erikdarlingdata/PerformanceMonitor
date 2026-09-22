/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// One analysis pass's collection outcome as the ledger remembers it: when the pass ran, what it could not
/// read, and the family total the count is stated against (#3691, lane 45). <see cref="Failures"/> is empty
/// on a clean pass, which is a RECORDED pass and not an absent one — the only thing that can say a family
/// recovered.
///
/// <para>Deliberately not <see cref="CollectionCaveatState"/>, which is the per-READ value a facts call
/// returns to its own caller. This one is dated, because a remembered pass is only readable beside the
/// instant it ran, and it is the ledger's unit rather than a return value.</para>
/// </summary>
/// <param name="AnalysisTimeUtc">When the pass that produced this snapshot ran.</param>
/// <param name="Failures">The failed family reads, one entry per failed read; empty on a clean pass.</param>
/// <param name="FamiliesTotal">How many families the pass's collector runs — the denominator.</param>
public sealed record CollectionCaveatSnapshot(
    DateTime AnalysisTimeUtc,
    IReadOnlyList<CollectionFailure> Failures,
    int FamiliesTotal);

/// <summary>
/// What the last analysis passes could NOT read, remembered per server for the whole process (#3691, lane 45)
/// — the surface a SCHEDULED pass's collection caveats reach, which until this class was one Warning line in
/// the service log and nowhere an operator or an agent could read.
///
/// <para><b>The gap this closes.</b> A pass records its failed family reads on the context, and the service
/// copies them onto <c>LastCollectionFailures</c> / <c>LastCollectionFamilyCount</c> so the MCP tools that
/// render off that pass can carry <c>collection_caveats</c>. That works for an MCP-triggered pass, because
/// the tool and the pass share the injected service instance. It does NOT work for a scheduled sweep: the
/// worker builds a FRESH analysis service per pass, so the singleton the tools were handed remembers only
/// the passes the tools themselves started. On a fleet whose passes are all scheduled — which is every
/// production install — those properties are permanently empty, and the fact that eleven of twenty families
/// timed out at 03:00 lived in a log line and died there. This ledger is process-wide and static for exactly
/// that reason: it is the one place both the per-pass service instances and the static tool methods can
/// reach without threading a dependency through a worker that constructs its analysis service per server per
/// pass.</para>
///
/// <para><b>A static well-known instance, the <c>AlertReadFailureCounter.Shared</c> shape.</b> Same problem,
/// same answer, deliberately: the value is process-global by nature, the readers are static MCP tool methods
/// in a DI container built separately from the one the pass runs in, and the write side names
/// <see cref="Shared"/> explicitly at its two call sites — so a test constructs its own ledger and cannot
/// pollute the process one, which a <c>public static class</c> would have made impossible.</para>
///
/// <para><b>Bounded, and not persisted.</b> A ring of the last <see cref="MaxPassesPerServer"/> passes per
/// server: enough to say "failed on six of the last six passes" — the difference between one timeout and a
/// family that has been dark all night — and small enough that a fleet of fifty servers holds fifty short
/// arrays. Nothing here is written to a store. That is a design choice, not a shortcut: this is the
/// ANALYSIS pass's own reads of the store, so persisting it would mean writing to the store the pass just
/// failed to read, and the honest report of a process-memory fact is the stamp that dates it. Every payload
/// this class builds therefore carries <see cref="StartedUtc"/> and says outright that a restart forgets —
/// the <c>alert_read_health.counting_since</c> discipline, which exists because a zero with no stamp beside
/// it reads as reassurance.</para>
///
/// <para><b>Every pass is recorded, including a clean one.</b> A clean pass is the evidence that a family
/// RECOVERED; recording only failures would leave the last failure looking current forever, which is
/// <c>last_error</c>'s #3010 defect rebuilt one layer up.</para>
///
/// <para><b>Thread-safety.</b> Passes for different servers run concurrently, and the MCP tools read while
/// they run. The per-server map is a <see cref="ConcurrentDictionary{TKey,TValue}"/> of ring holders so a
/// record never replaces an entry, and each ring is mutated and copied under its OWN lock — per server
/// rather than one global lock, because the contended case is fifty servers' passes landing together and
/// none of them touches another's ring. Readers copy under the same lock and compute outside it, so a
/// reader can never see a half-written ring and a slow serializer cannot hold a pass up.</para>
/// </summary>
public sealed class CollectionCaveatLedger
{
    /// <summary>
    /// The process's ledger — the one every analysis pass writes and the <c>get_collection_health</c> tools
    /// on both SKUs read. See the class remarks for why this is a static instance rather than a container
    /// registration or a static class.
    /// </summary>
    public static CollectionCaveatLedger Shared { get; } = new CollectionCaveatLedger();

    /// <summary>
    /// How many passes per server the ring holds. Twenty-four is chosen against the SHAPE of the question
    /// the tool asks — "is this family always failing, or did it fail once?" — and the cadence that answers
    /// it: the scheduled analysis sweep runs on the order of an hour, so twenty-four passes is about a day
    /// of them, long enough that a nightly maintenance window's timeouts are still visible in the morning
    /// and short enough that a week-old episode cannot be read as current. It is not a measured bar and
    /// claims no lineage; nothing about it is a threshold anything fires on.
    /// </summary>
    public const int MaxPassesPerServer = 24;

    /// <summary>
    /// When this ledger began remembering — the stamp every payload it builds carries, because these are
    /// in-memory counts and a restart takes them to nothing. An empty ledger means "no pass since this
    /// stamp", never "no pass failed".
    /// </summary>
    public DateTime StartedUtc { get; } = DateTime.UtcNow;

    /// <summary>
    /// One server's ring. A fixed array plus a write cursor rather than a queue, so recording a pass
    /// allocates nothing and the oldest pass falls off by being overwritten. <c>_count</c> saturates at the
    /// array length, which is what lets a reader state a denominator honestly on a ledger that has seen
    /// fewer than <see cref="MaxPassesPerServer"/> passes ("2 of 2", not "2 of 24").
    /// </summary>
    private sealed class Ring
    {
        private readonly CollectionCaveatSnapshot?[] _passes = new CollectionCaveatSnapshot?[MaxPassesPerServer];
        private int _next;
        private int _count;

        /// <summary>The display name the newest recorded pass ran under. Held so a reader that has only a
        /// server id can name the server without a store read; the MCP tools resolve their own name and do
        /// not use this, and it exists so the ledger is readable on its own terms.</summary>
        public string ServerName { get; set; } = string.Empty;

        public void Add(CollectionCaveatSnapshot pass)
        {
            _passes[_next] = pass;
            _next = (_next + 1) % _passes.Length;
            if (_count < _passes.Length) _count++;
        }

        /// <summary>The remembered passes, NEWEST FIRST — the order every reader wants, so the reversal
        /// happens once here rather than at each call site.</summary>
        public List<CollectionCaveatSnapshot> Newest()
        {
            var result = new List<CollectionCaveatSnapshot>(_count);
            for (var i = 0; i < _count; i++)
            {
                var index = ((_next - 1 - i) % _passes.Length + _passes.Length) % _passes.Length;
                if (_passes[index] is CollectionCaveatSnapshot pass) result.Add(pass);
            }
            return result;
        }
    }

    private readonly ConcurrentDictionary<int, Ring> _byServer = new ConcurrentDictionary<int, Ring>();

    /// <summary>
    /// Records one analysis pass's collection outcome. Called from the analysis pass beside the assignment
    /// onto the service's own <c>LastCollectionFailures</c>, on EVERY pass — a clean pass carries an empty
    /// <paramref name="failures"/> and is still recorded, because that is the only thing that can say a
    /// family recovered.
    ///
    /// <para>The failures are COPIED. The list handed in is the live context's, which the pass may still be
    /// appending to on a path that continues after this call, and a ledger holding a reference would let a
    /// later failure appear inside an already-recorded pass.</para>
    /// </summary>
    /// <param name="serverId">The server the pass ran for.</param>
    /// <param name="serverName">Its display name, held so a reader has something to name without a store read.</param>
    /// <param name="analysisTimeUtc">When the pass ran.</param>
    /// <param name="failures">The failed family reads; empty on a clean pass.</param>
    /// <param name="familiesTotal">How many families the collector runs.</param>
    public void Record(
        int serverId,
        string serverName,
        DateTime analysisTimeUtc,
        IReadOnlyList<CollectionFailure> failures,
        int familiesTotal)
    {
        var pass = new CollectionCaveatSnapshot(
            analysisTimeUtc,
            failures is null ? [] : failures.ToArray(),
            familiesTotal);

        var ring = _byServer.GetOrAdd(serverId, static _ => new Ring());
        lock (ring)
        {
            ring.ServerName = serverName ?? string.Empty;
            ring.Add(pass);
        }
    }

    /// <summary>
    /// The newest pass remembered for a server, or false when this process has run none for it (a fresh
    /// service, a server whose sweep has not reached its analysis leg yet, or a restart since the last one).
    /// </summary>
    public bool TryGetLatest(int serverId, out CollectionCaveatSnapshot? snapshot)
    {
        snapshot = Newest(serverId).FirstOrDefault();
        return snapshot is not null;
    }

    /// <summary>The display name the server's newest recorded pass ran under, or null when this process has
    /// recorded no pass for it — the ledger's own id-to-name answer, so a reader holding an id is not forced
    /// into a store read to say which server a caveat belongs to.</summary>
    public string? ServerNameOf(int serverId)
    {
        if (!_byServer.TryGetValue(serverId, out var ring)) return null;
        lock (ring)
        {
            return ring.ServerName;
        }
    }

    /// <summary>
    /// How many of the last <paramref name="lastN"/> passes for this server carried a failure for
    /// <paramref name="family"/> — the persistence figure, so the tool can say "failed on 6 of the last 6
    /// passes" instead of leaving a caller to read one entry as one event. A family whose two reads both
    /// failed in one pass counts ONCE: the unit is the pass, not the read.
    /// </summary>
    public int RecentPassCount(int serverId, string family, int lastN)
    {
        if (string.IsNullOrEmpty(family) || lastN <= 0) return 0;
        return Newest(serverId)
            .Take(lastN)
            .Count(p => p.Failures.Any(f => string.Equals(f.Family, family, StringComparison.Ordinal)));
    }

    /// <summary>How many passes this ledger holds for a server, capped at <see cref="MaxPassesPerServer"/> —
    /// the denominator <see cref="RecentPassCount"/>'s figure is stated against.</summary>
    public int PassesRemembered(int serverId) => Newest(serverId).Count;

    /// <summary>The remembered passes for a server, newest first; empty for a server this process has run no
    /// pass for. Copied out under the ring's lock, computed outside it.</summary>
    private List<CollectionCaveatSnapshot> Newest(int serverId)
    {
        if (!_byServer.TryGetValue(serverId, out var ring)) return [];
        lock (ring)
        {
            return ring.Newest();
        }
    }

    /// <summary>
    /// The one-sentence contract that travels with every block this class builds: what layer it describes,
    /// what a family named in it means for the findings, and that it is process memory with a start stamp
    /// rather than a store read.
    /// </summary>
    public string Note() =>
        "The analysis pass's own reads of the collectors' tables — a DIFFERENT layer from the collector rows above, which say whether the data was written. A family here means verdicts about it are absent from analyze_server for that pass, whatever the collector's own row says. Process memory since "
        + StartedUtc.ToString("o", CultureInfo.InvariantCulture)
        + "; a service restart forgets, so an absent block is never evidence that every pass was clean.";

    /// <summary>
    /// The <c>analysis_caveats</c> block for a server, or null when nothing is owed — no pass remembered,
    /// or every remembered pass clean.
    ///
    /// <para><b>Why a clean LATEST pass can still owe a block.</b> The recovery reading is the whole reason
    /// clean passes are recorded: a family that failed on the last five passes and read fine on this one is
    /// news, and a block that vanished the instant it recovered would leave a caller unable to tell a
    /// recovery from a family that never failed. So the block describes the latest pass — <c>analysis_time</c>,
    /// <c>families_failed</c> and <c>families_total</c> are ITS figures, and <c>families_failed</c> is 0 when
    /// it was clean — while <c>entries</c> come from the newest pass that actually failed, dated separately by
    /// <c>entries_from</c> and flagged by <c>recovered</c>. Two stamps on one block is the cost of not lying
    /// in either direction; one stamp would have to either misdate the entries or hide the recovery.</para>
    ///
    /// <para><c>failed_in_last_passes</c> is the persistence figure per family, stated as "n of m" against
    /// the passes this ledger actually holds — never against 24, which would read as a window it may not
    /// have reached yet.</para>
    /// </summary>
    public object? ToPayload(int serverId)
    {
        var passes = Newest(serverId);
        if (passes.Count == 0) return null;

        var latest = passes[0];
        var entriesFrom = passes.FirstOrDefault(p => p.Failures.Count > 0);
        if (entriesFrom is null) return null;

        var remembered = passes.Count;
        return new
        {
            analysis_time = latest.AnalysisTimeUtc.ToString("o", CultureInfo.InvariantCulture),
            families_failed = latest.Failures.Select(f => f.Family).Distinct(StringComparer.Ordinal).Count(),
            families_total = latest.FamiliesTotal,
            passes_remembered = remembered,
            /* True when the latest pass read every family and an older remembered pass did not: the entries
               below are history, and the family they name is being read again. The flag rather than a
               subtraction of the two stamps, because a caller keying on "is this current" must not have to
               compare timestamps to find out. */
            recovered = latest.Failures.Count == 0,
            entries_from = entriesFrom.AnalysisTimeUtc.ToString("o", CultureInfo.InvariantCulture),
            entries = entriesFrom.Failures.Select(f => new
            {
                family = f.Family,
                read = f.Read,
                outcome = CollectionFailure.Label(f.Outcome),
                message = f.Message,
                failed_in_last_passes = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} of {1}",
                    RecentPassCount(serverId, f.Family, MaxPassesPerServer),
                    remembered)
            }).ToList(),
            /* The pass-level sentence, built by the same CollectionCaveats.Describe the Warning log line and
               every analyze_server payload use — so an operator reading this block and an operator grepping
               the service log are reading one sentence rather than two paraphrases that could drift. */
            finding = CollectionCaveats.Describe(entriesFrom.Failures, entriesFrom.FamiliesTotal),
            note = Note()
        };
    }

    /// <summary>
    /// Adds <c>analysis_caveats</c> to a payload object ONLY when a block is owed. With nothing owed the very
    /// same <paramref name="payload"/> reference is returned, so the caller's serializer call is the one it
    /// always made and a payload on a clean ledger cannot move a byte — the
    /// <see cref="CollectionCaveats.Attach"/> contract, and it is load-bearing here for the same reason:
    /// <c>McpHelpers.JsonOptions</c> WRITES nulls, so a literal <c>analysis_caveats = …</c> property would put
    /// a null key on every clean server's <c>get_collection_health</c> for a caveat that was not owed.
    /// </summary>
    public object Attach(object payload, int serverId, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var block = ToPayload(serverId);
        if (block is null) return payload;

        var node = JsonSerializer.SerializeToNode(payload, options)?.AsObject()
            ?? throw new InvalidOperationException("the payload did not serialize to a JSON object");
        node["analysis_caveats"] = JsonSerializer.SerializeToNode(block, options);
        return node;
    }
}
