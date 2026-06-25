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
    /// A rendered, app-agnostic view of one deadlock as the <b>waits-for cycle among processes</b>:
    /// process nodes plus waiter -&gt; owner ("waits for") edges carrying the contended resource + lock
    /// modes. Built by <see cref="DeadlockGraphParser"/> from the deadlock graph XML and laid out by
    /// <see cref="DeadlockGraphLayout"/>. Deliberately NOT a process|resource bipartite graph: real
    /// deadlocks here are 5-8 process cycles (often a <i>bundle of independent small cycles</i> — see
    /// <see cref="DeadlockGraphLayout"/>), and in a single-app workload every process shares the same
    /// login/host/app/db, so the statement and the contended resource — not identity — differentiate the
    /// nodes. WPF-free, so it lives in Common and is shared by the Dashboard and Lite deadlock viewers.
    /// </summary>
    public sealed class DeadlockGraphModel
    {
        /// <summary>Every process that took part in the deadlock, in deterministic (spid, id) order.</summary>
        public IReadOnlyList<DeadlockProcessNode> Processes { get; init; } = Array.Empty<DeadlockProcessNode>();

        /// <summary>One edge per (waiter, owner) pair sharing a lock element — "waiter waits for what owner holds."</summary>
        public IReadOnlyList<DeadlockWaitEdge> Edges { get; init; } = Array.Empty<DeadlockWaitEdge>();

        /// <summary>An <c>exchangeEvent</c>/<c>SyncPoint</c> was present — an intra-query (parallel) deadlock.</summary>
        public bool IsParallel { get; init; }

        /// <summary>The deadlock timestamp, when the parsed XML carried an <c>&lt;event&gt;</c> wrapper. Often
        /// null (both apps strip the wrapper before storage); the launch glue supplies the time it knows.</summary>
        public DateTime? EventTime { get; init; }

        /// <summary>
        /// Bounding boxes of the independent cycles (connected components of the waits-for graph), in canvas
        /// coordinates, assigned by <see cref="DeadlockGraphLayout"/>. A single &lt;deadlock&gt; element often
        /// batches several independent cycles that share no resource (the deadlock monitor reports them as one
        /// graph); the viewer draws one labelled box per entry so they read as the distinct sub-graphs they are.
        /// One entry per cycle, biggest first. Layout output, so it follows the same set-after-construction
        /// pattern as <see cref="DeadlockProcessNode.X"/>/<see cref="DeadlockProcessNode.Y"/>.
        /// </summary>
        public IReadOnlyList<DeadlockComponentBox> ComponentBoxes { get; set; } = Array.Empty<DeadlockComponentBox>();

        /// <summary>True when the XML produced no processes (empty range, unparseable, or non-deadlock XML).</summary>
        public bool IsEmpty => Processes.Count == 0;
    }

    /// <summary>
    /// The canvas-space bounding box of one independent cycle (a connected component of the waits-for graph),
    /// produced by <see cref="DeadlockGraphLayout"/>. <see cref="Index"/> is 1-based for display ("Cycle 1").
    /// </summary>
    public sealed class DeadlockComponentBox
    {
        /// <summary>1-based cycle number for display (biggest cycle is 1).</summary>
        public int Index { get; init; }

        /// <summary>How many processes the cycle contains.</summary>
        public int NodeCount { get; init; }

        public double X { get; init; }
        public double Y { get; init; }
        public double Width { get; init; }
        public double Height { get; init; }
    }

    /// <summary>
    /// One SQL Server process (session/thread) caught in a deadlock. Keyed by <see cref="Id"/> (the graph's
    /// <c>process</c> id — stable within one deadlock and distinct even when two parallel threads share a
    /// <see cref="Spid"/>). The card LEADS with <see cref="Spid"/> + <see cref="SqlText"/> + lock/wait; the
    /// identity fields below are usually identical across all nodes in a single-app workload, so they are
    /// shown small / on the properties panel.
    /// </summary>
    public sealed class DeadlockProcessNode
    {
        /// <summary>The graph's process id (e.g. <c>process2d21a0ea088</c>). The node/edge join key.</summary>
        public string Id { get; init; } = string.Empty;

        public int Spid { get; init; }

        /// <summary>Execution-context id. Non-zero distinguishes parallel threads of the same <see cref="Spid"/>.</summary>
        public int Ecid { get; init; }

        /// <summary>True for every process on the victim list (SQL Server can pick more than one victim).</summary>
        public bool IsVictim { get; init; }

        /// <summary>The statement this process was running (its <c>inputbuf</c>). The real differentiator — lead with it.</summary>
        public string SqlText { get; init; } = string.Empty;

        /// <summary>This process's requested lock mode (X, U, S, ...). Empty for the parallel (exchange) case.</summary>
        public string LockMode { get; init; } = string.Empty;

        /// <summary>The resource this process was blocked on (raw <c>waitresource</c> string).</summary>
        public string WaitResource { get; init; } = string.Empty;

        /// <summary>
        /// The resolved contended resource: the friendly lock kind + object name from the first resource this
        /// process waits on (its first non-self "waits for" edge), e.g. "PAGE hammerdb_tpcc.dbo.new_order".
        /// Assigned by <see cref="DeadlockGraphParser"/> after edges are built; SQL Server already resolved the
        /// objectname in the deadlock resource-list, so this needs no DMV lookup. Empty for a process that only
        /// owns (no outgoing wait), waits solely on a parallelism exchange, or whose wait resource carried no
        /// object name (system table / Azure). Mirrors the block-chain viewer's contended-object surface.
        /// </summary>
        public string ContentiousObject { get; set; } = string.Empty;

        /// <summary>How long this process had been waiting, in ms.</summary>
        public long WaitTimeMs { get; init; }

        /// <summary>Deadlock priority (higher survives; the lowest tends to be the victim).</summary>
        public int Priority { get; init; }

        // ── Secondary identity / context (often identical across nodes in a single-app workload) ──
        public string DatabaseName { get; init; } = string.Empty;
        public string LoginName { get; init; } = string.Empty;
        public string ClientApp { get; init; } = string.Empty;
        public string HostName { get; init; } = string.Empty;
        public string IsolationLevel { get; init; } = string.Empty;

        /// <summary>The first real procedure on the execution stack (skips <c>adhoc</c>/<c>unknown</c>); blank for ad hoc.</summary>
        public string ProcName { get; init; } = string.Empty;

        /// <summary>Process status (e.g. <c>suspended</c>).</summary>
        public string Status { get; init; } = string.Empty;

        /// <summary>Layout position (ring placement), assigned by <see cref="DeadlockGraphLayout"/>.</summary>
        public double X { get; set; }
        public double Y { get; set; }
    }

    /// <summary>
    /// A directed "waits for" edge: <see cref="WaiterProcessId"/> is blocked waiting for a resource that
    /// <see cref="OwnerProcessId"/> holds. Emitted once per (waiter, owner) pair sharing a lock element, so
    /// a resource with N owners × M waiters produces the N·M cross product — that is how real &gt;2-process
    /// cycles form. For a parallel <c>exchangeEvent</c> the waiter and owner can be the same process — a
    /// self-edge the layout draws as a small self-loop.
    /// </summary>
    public sealed class DeadlockWaitEdge
    {
        public string WaiterProcessId { get; init; } = string.Empty;
        public string OwnerProcessId { get; init; } = string.Empty;

        /// <summary>The lock element's name: pagelock|keylock|objectlock|ridlock|rowgrouplock|exchangeEvent|SyncPoint.</summary>
        public string ResourceKind { get; init; } = string.Empty;

        /// <summary>Display label: a friendly kind word + the object name (e.g. "PAGE hammerdb_tpcc.dbo.new_order"),
        /// falling back to the kind + hobt/page id when the resource has no object name (Azure / system tables).</summary>
        public string ResourceLabel { get; init; } = string.Empty;

        /// <summary>The waiter's requested mode (X, U, ...). May be empty for an exchange/parallel edge.</summary>
        public string RequestMode { get; init; } = string.Empty;

        /// <summary>The owner's held mode. May be empty for an exchange/parallel edge.</summary>
        public string OwnerMode { get; init; } = string.Empty;

        /// <summary>True when waiter and owner are the same process (a parallel self-edge).</summary>
        public bool IsSelfEdge => string.Equals(WaiterProcessId, OwnerProcessId, StringComparison.Ordinal);
    }
}
