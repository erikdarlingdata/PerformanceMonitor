/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The I/O chain (filled by lane 11 of #3691, design §3.9): <c>PG_IO_READ_LATENCY_MS</c> ↔ <c>PG_BUFFER_CACHE_PRESSURE</c>
/// (a cold cache reads from disk) and the <c>IO:</c>-type wait standouts; write latency ↔ <c>PG_CHECKPOINT_PRESSURE</c>
/// is stated but not built in v2 (below). Predicates read the destination fact's verdict (<c>BaseSeverity &gt; 0</c>),
/// never a bar of their own.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /// <summary>
    /// The I/O chain, filled by lane 11. Read latency sits BETWEEN the wait a backend felt and the reason the read
    /// happened at all:
    /// <list type="bullet">
    /// <item><description><c>PG_WAIT_IO_DATAFILEREAD</c> → <c>PG_IO_READ_LATENCY_MS</c>: the data-file-read wait
    /// standout leads to the latency fact when the storage was measurably slow per operation — these waits are long
    /// because each read is slow, not merely because there are many. Only the standout has an edge, never the
    /// <c>IO</c> type rollup: lane 5's rule (<c>PgTargetRelationshipGraph.Memory.cs</c>, pinned by
    /// <c>PgTargetWaitTests</c>) is that a rollup roots nothing of its own — it scores 0 whenever its named standout
    /// fired (<c>PgTargetScorer.Waits.cs</c> grades one wait once), so the standout is the root and an edge from the
    /// rollup would be a second door into the same room. The rollup still COUNTS as a corroborator on the latency
    /// fact's amplifiers (<c>PgTargetScorer.Io.cs</c>), which is a predicate, not a story edge.</description></item>
    /// <item><description><c>PG_IO_READ_LATENCY_MS</c> → <c>PG_BUFFER_CACHE_PRESSURE</c>: slow reads matter in
    /// proportion to how many there are, and the composite says the cache is sending them — a working set larger
    /// than <c>shared_buffers</c> is the lever that removes reads rather than speeding them up. That edge continues
    /// through the memory chain to <c>CONFIG_PG_SHARED_BUFFERS</c>.</description></item>
    /// <item><description><c>PG_IO_READ_LATENCY_MS</c> → <c>CONFIG_PG_EFFECTIVE_CACHE_SIZE</c> /
    /// <c>CONFIG_PG_RANDOM_PAGE_COST</c>: CONTEXT edges into the two planner knobs, each firing only when the knob is
    /// at its shipped default (the knob's own verdict, lane 2's engine-defined bar). D5 holds: neither knob is
    /// amplified by latency — the convention card stays at its 0.4 advisory base and is a LEAF of the latency story,
    /// which is where an operator reading "reads are slow AND the planner is costing them as if disks were
    /// spinning rust" wants it. Latency alone never lifts a knob past the incident line.</description></item>
    /// </list>
    /// <para><b>Not built: write latency ↔ checkpoint pressure.</b> <c>PG_IO_WRITE_LATENCY_MS</c> has no v2 bar
    /// (<c>PgTargetScorer.Io.cs</c>) so its <c>BaseSeverity</c> is always 0 and an edge into it could never fire;
    /// an edge FROM it could never be walked. The write fact is context the checkpoint advice may read; the edge
    /// arrives with a measured write bar. <c>ANOMALY_PG_IO_LATENCY</c> reaches the read fact through the
    /// reconciler's fold (<c>PgTargetFactKeys.AnomalyToFamilies</c>), as every PostgreSQL anomaly does — no
    /// anomaly has a graph edge on either engine.</para>
    /// </summary>
    private partial void BuildIoEdges()
    {
        static bool Fired(System.Collections.Generic.IReadOnlyDictionary<string, Fact> facts, string key) =>
            facts.TryGetValue(key, out var fact) && fact.BaseSeverity > 0;

        AddEdge(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), PgTargetFactKeys.IoReadLatencyMs, "io_latency",
            "Data-file reads were slow per operation over the window — these waits are long because each read is slow, not only because there are many",
            facts => Fired(facts, PgTargetFactKeys.IoReadLatencyMs));

        AddEdge(PgTargetFactKeys.IoReadLatencyMs, PgTargetFactKeys.BufferCachePressure, "io_latency",
            "The buffer cache is measurably short for this working set — the misses are what is reaching the slow storage",
            facts => Fired(facts, PgTargetFactKeys.BufferCachePressure));

        AddEdge(PgTargetFactKeys.IoReadLatencyMs, PgTargetFactKeys.ConfigEffectiveCacheSize, "io_latency",
            "effective_cache_size at the compiled default — the planner sizes its cache assumption for a host nobody described",
            facts => Fired(facts, PgTargetFactKeys.ConfigEffectiveCacheSize));

        AddEdge(PgTargetFactKeys.IoReadLatencyMs, PgTargetFactKeys.ConfigRandomPageCost, "io_latency",
            "random_page_cost at the compiled default — the planner is costing random reads as if this storage were spinning disks",
            facts => Fired(facts, PgTargetFactKeys.ConfigRandomPageCost));
    }
}
