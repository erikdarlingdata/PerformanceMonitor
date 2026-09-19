/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// A currently-running query whose elapsed time exceeds the long-running-query alert threshold.
/// Canonical shared copy (Phase-5 A0) — Lite and the Dashboard previously carried member-identical
/// local twins; both apps now alias this type via a global using so call sites are unchanged.
/// </summary>
public class LongRunningQueryInfo
{
    public int SessionId { get; set; }
    public string DatabaseName { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string ProgramName { get; set; } = "";

    /// <summary>The session's <c>login_name</c> as collected (#3653 A5, Q5) — projected so the Long-Running Query
    /// opt-out knob's login arm has the value it matched on beside the row, and so a host that filters rows it
    /// already holds can apply <see cref="LongRunningQueryExclusions.Excludes"/> to the same two handles the
    /// read did. Empty for a producer that does not project it (the deprecated Dashboard's frozen read, every
    /// pre-#3653 fixture); the exclusion itself is applied in the read, so an empty value here never changes a
    /// decision.</summary>
    public string LoginName { get; set; } = "";
    public long ElapsedSeconds { get; set; }
    public long CpuTimeMs { get; set; }
    public long Reads { get; set; }
    public long Writes { get; set; }
    public string? WaitType { get; set; }
    public int? BlockingSessionId { get; set; }
    public string? QueryHash { get; set; }
}
