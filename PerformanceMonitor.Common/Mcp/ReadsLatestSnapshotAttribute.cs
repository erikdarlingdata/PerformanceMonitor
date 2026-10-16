/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The three honest shapes a latest-snapshot MCP read may take (#3541 A10).
/// </summary>
public enum LatestSnapshotShape
{
    /// <summary>Reads one snapshot, takes no window: <c>captured_at</c> only.</summary>
    Stamped,

    /// <summary><c>hours_back</c> bounds the SEARCH for the newest snapshot (described in those words) and
    /// <c>as_of</c> anchors it: <c>captured_at</c> + <c>age_seconds</c>.</summary>
    SearchBound,

    /// <summary><c>hours_back</c> is READ: the newest snapshot (<c>captured_at</c> + <c>age_seconds</c>)
    /// beside a <c>window</c> aggregate over every snapshot in it.</summary>
    Windowed,
}

/// <summary>
/// Declares that an MCP tool reads the latest snapshot and says so on its payload: it publishes
/// <c>captured_at</c> (and, for the shapes that take a window, <c>age_seconds</c> / <c>window</c>), because
/// "latest" is a time and an agent that cannot see it reads every such answer as "now" (#3541 A10). The
/// declaration lives with the tool, on both products, so adding a latest read is one attribute on the tool
/// rather than a row in a test-side roster (#5950). <c>McpLatestSnapshotStampTests</c> reflects over it: every
/// tool that carries it must behave as its <see cref="Shape"/> requires, and a tool that publishes a
/// <c>captured_at</c> without carrying it fails by name.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class ReadsLatestSnapshotAttribute : Attribute
{
    public ReadsLatestSnapshotAttribute(LatestSnapshotShape shape) => Shape = shape;

    /// <summary>Which of the three stamped dialects the tool speaks.</summary>
    public LatestSnapshotShape Shape { get; }
}
