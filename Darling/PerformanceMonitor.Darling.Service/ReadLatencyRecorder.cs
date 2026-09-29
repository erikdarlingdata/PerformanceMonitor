/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The read-latency seat one server records through (#4782): the <see cref="ReadLatencyAccumulator"/> it was given
/// and the logger a recording failure is reported through, at Debug -- never at a level an operator would see,
/// since a recording failure is never a request failure. <see cref="DarlingWebEndpoints.MapAll"/> builds one per
/// call, and the two places it records close over it: the <c>/api/read/*</c> dispatch loop and the shared
/// composed-panel runner. The MCP host registers its own for <c>run_custom_view_panel</c>, which runs that same
/// runner.
///
/// <para>This replaces two process-wide statics that every <c>MapAll</c> call overwrote: with a second server set
/// up in the same process (parallel test classes do), the first server's samples landed in the second's
/// accumulator. A recorder holding no accumulator records nothing, so recording stays optional, never
/// required.</para>
/// </summary>
public sealed class ReadLatencyRecorder
{
    public ReadLatencyRecorder(ReadLatencyAccumulator? readLatency, ILogger? logger)
    {
        Accumulator = readLatency;
        Logger = logger;
    }

    /// <summary>Where a sample goes; null records nothing.</summary>
    internal ReadLatencyAccumulator? Accumulator { get; }

    /// <summary>Where a recording failure is reported, at Debug; null reports nothing.</summary>
    internal ILogger? Logger { get; }
}
