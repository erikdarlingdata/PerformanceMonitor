/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// A rendered, tree-shaped view of one or more reconstructed blocking chains. Each root is an apex
    /// (lead/head blocker) with its transitive victims beneath it. Built by <see cref="BlockingChainTreeBuilder"/>
    /// from the per-app projection of the analysis engine's reconstruction; laid out by
    /// <see cref="BlockingChainLayout"/>. App-agnostic (no WPF), so it lives in Common and is shared by
    /// the Dashboard and Lite block-chain viewers.
    /// </summary>
    public sealed class BlockingChainModel
    {
        /// <summary>One node per apex, ranked worst-first (by chain magnitude).</summary>
        public IReadOnlyList<BlockingChainNode> Roots { get; init; } = Array.Empty<BlockingChainNode>();

        /// <summary>A cycle was detected in the source blocking graph (mutual block / deadlock-shaped).</summary>
        public bool CycleDetected { get; init; }

        /// <summary>The reconstruction hit its depth cap; the deepest chains may be truncated.</summary>
        public bool DepthCapped { get; init; }

        /// <summary>The reconstruction exhausted its step budget; some edges may be missing.</summary>
        public bool TraversalTruncated { get; init; }
    }

    /// <summary>
    /// One session in a blocking tree. Keyed internally by (Spid, TranStarted) — a SPID reused by two
    /// sessions in the window is two distinct nodes, never collapsed.
    /// </summary>
    public sealed class BlockingChainNode
    {
        public int Spid { get; init; }

        /// <summary>Execution-context id; with <see cref="Spid"/> forms the node identity (spid:ecid, mirroring
        /// sp_HumanEventsBlockViewer). 0 for the common non-parallel case.</summary>
        public int Ecid { get; init; }

        /// <summary>Transaction start — display only now (identity is spid:ecid). May be null.</summary>
        public DateTime? TranStarted { get; init; }

        /// <summary>True for the lead/head blocker at the root of its tree.</summary>
        public bool IsApex { get; init; }

        /// <summary>True when the apex is sleeping (an abandoned open transaction holding locks).</summary>
        public bool IsApexSleeping { get; init; }

        /// <summary>Chain magnitude for ranking the chain selector. Set on the root only; 0 elsewhere.</summary>
        public double Magnitude { get; init; }

        /// <summary>This node's wait time on its parent, in ms. 0 for the apex (it waits on no one).</summary>
        public long WaitTimeMs { get; init; }

        /// <summary>Lock mode this node is waiting for on its parent. Empty for the apex.</summary>
        public string LockMode { get; init; } = string.Empty;

        /// <summary>The SQL this session was running (its own statement).</summary>
        public string SqlText { get; init; } = string.Empty;

        /// <summary>Database context of the block.</summary>
        public string DatabaseName { get; init; } = string.Empty;

        /// <summary>Who this session is — login, host, and client app. Sourced from the side of the edge
        /// where this node appears (blocked side for a victim/mid-level blocker; blocking side for the apex).
        /// May be empty when the source data doesn't carry it (e.g. the Dashboard apex — see the projection).</summary>
        public string LoginName { get; init; } = string.Empty;
        public string HostName { get; init; } = string.Empty;
        public string ClientApp { get; init; } = string.Empty;

        /// <summary>The object this session is waiting on (the contended resource), resolved to
        /// schema.object where possible. Empty for the apex — it waits on no one.</summary>
        public string ContentiousObject { get; init; } = string.Empty;

        /// <summary>Direct victims of this node, ordered deterministically.</summary>
        public List<BlockingChainNode> Children { get; init; } = new();

        /// <summary>Layout position, assigned by <see cref="BlockingChainLayout"/>.</summary>
        public double X { get; set; }
        public double Y { get; set; }
    }

    /// <summary>
    /// Public input to <see cref="BlockingChainTreeBuilder.Build"/> — one per reconstructed chain. The
    /// per-app launch handler fills this by a mechanical field copy from the analysis engine's
    /// reconstruction (which is internal to the analysis assembly and invisible here). Magnitude and the
    /// sleeping flag live here because they are per-chain, not per-edge.
    /// </summary>
    public sealed class BlockingChainInput
    {
        public int ApexSpid { get; init; }
        public int ApexEcid { get; init; }
        public DateTime? ApexTranStarted { get; init; }
        public bool ApexSleeping { get; init; }
        public double Magnitude { get; init; }

        /// <summary>The chain's level-by-level edges (one per blocker -> blocked relationship).</summary>
        public IReadOnlyList<BlockingEdgeInput> Edges { get; init; } = Array.Empty<BlockingEdgeInput>();
    }

    /// <summary>One blocker -> blocked edge of a chain.</summary>
    public sealed class BlockingEdgeInput
    {
        /// <summary>The reconstructor's BFS level for this edge. Carried for parity/diagnostics only —
        /// <see cref="BlockingChainTreeBuilder"/> re-derives depth structurally via its own BFS and does
        /// not read this, so a reorder of the source edges can't change the built tree.</summary>
        public int Level { get; init; }
        public int BlockingSpid { get; init; }
        public int BlockingEcid { get; init; }
        public DateTime? BlockingTranStarted { get; init; }
        public int BlockedSpid { get; init; }
        public int BlockedEcid { get; init; }
        public DateTime? BlockedTranStarted { get; init; }
        public string LockMode { get; init; } = string.Empty;
        public long WaitTimeMs { get; init; }
        public string DatabaseName { get; init; } = string.Empty;
        public string BlockingSqlText { get; init; } = string.Empty;
        public string BlockedSqlText { get; init; } = string.Empty;
        // Identity of each side, threaded the same way as the SQL text. A node sources its own identity
        // from the side it appears on: the blocked side when it's a victim/mid-level blocker, the blocking
        // side when it's the apex (it only ever appears as a blocker).
        public string BlockedLoginName { get; init; } = string.Empty;
        public string BlockedHostName { get; init; } = string.Empty;
        public string BlockedClientApp { get; init; } = string.Empty;
        public string BlockingLoginName { get; init; } = string.Empty;
        public string BlockingHostName { get; init; } = string.Empty;
        public string BlockingClientApp { get; init; } = string.Empty;
        /// <summary>The contended object on this edge (what the blocked side waits on). The victim node
        /// takes its ContentiousObject from its incoming edge; the apex has none.</summary>
        public string ContentiousObject { get; init; } = string.Empty;
    }
}
