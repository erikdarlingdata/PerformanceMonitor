/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The three instruments a PostgreSQL target's wait history can come from, and the one token each read
/// discloses so a caller knows what GRAIN they are looking at (#3604).
///
/// <para><b>Why a vocabulary rather than a boolean.</b> Wait accumulation on PostgreSQL had two paths and a
/// hole. Aurora targets get engine-cumulative wait TIME from <c>aurora_stat_system_waits()</c>
/// (<see cref="PgWaitStatsCollector"/>); stock targets get a 10 ms sampled profile IF the
/// <c>pg_wait_sampling</c> extension is loaded (<see cref="PgWaitSamplingCollector"/>'s extension arm); and a
/// stock target WITHOUT the extension — most first installs, and every environment where adding a
/// preload library needs a change ticket — got nothing at all. The same collector now has a third arm that
/// samples <c>pg_stat_activity</c> from the service side, so the hole is a floor. But three sources with
/// three different grains feeding two tables means a number without its instrument is uninterpretable: a
/// count of 300 is 3 seconds of waiting under the extension and 5 minutes under the sampler. Every read
/// that serves any of them names which one it is serving.</para>
///
/// <para><b>The selection is ONE decision, made at target attach, in this order:</b> Aurora native &gt;
/// <c>pg_wait_sampling</c> present &gt; service sampler. <see cref="CollectorTargetInfo.IsAurora"/> routes the
/// first (and gates the other two off — Aurora cannot preload the module, and a second sampled series beside
/// the engine's own counters would be a worse answer, not a redundant one);
/// <see cref="CollectorTargetInfo.HasPgWaitSamplingExtension"/> picks between the last two. Exactly one
/// instrument feeds each target's wait history at any time, and the collector records which one it took in
/// its own per-server state so the reads disclose it from the store rather than re-deriving it.</para>
///
/// <para>Tokens are lower-snake because they travel on the MCP wire as JSON values beside <c>status</c>
/// words of the same shape.</para>
/// </summary>
public static class PgWaitInstrument
{
    /// <summary>Aurora's <c>aurora_stat_system_waits()</c>: every wait, its count and its measured time,
    /// accumulated by the engine since instance start. Parity with <c>sys.dm_os_wait_stats</c>.</summary>
    public const string EngineCumulative = "engine_cumulative";

    /// <summary>The <c>pg_wait_sampling</c> extension's in-engine profiler: every backend's wait event
    /// sampled every <c>profile_period</c> (10 ms by default), attributed to <c>queryid</c>, accumulated
    /// since server start or the last <c>pg_wait_sampling_reset_profile()</c>.</summary>
    public const string ExtensionSampled = "extension_sampled";

    /// <summary>This service polling <c>pg_stat_activity</c> from the outside on a one-second period for a
    /// short window each cycle, accumulated across cycles in the collector's own state. A FLOOR, not parity:
    /// a wait shorter than the period is observed with probability roughly its length over the period, and
    /// nothing between samples or between windows is observed at all.</summary>
    public const string ServiceSampled = "service_sampled";

    /// <summary>
    /// The honest limit of the service tier, in the words every read that serves it appends. One copy, so
    /// the MCP read, the empty-state message and the Viewer cannot drift on what the caveat IS — the same
    /// reason <c>CollectorEngineCapability</c> keeps its epilogue in one place.
    /// </summary>
    public const string ServiceSampledCaveat =
        "service_sampled is a FLOOR, not parity with the pg_wait_sampling extension: this service polls "
        + "pg_stat_activity once a second for a short window each cycle, so a wait shorter than a second is "
        + "seen with probability roughly its length over one second, nothing between samples or between "
        + "windows is seen at all, and a burst that fits inside the gap between two windows is missed "
        + "entirely. Shares of the profile are trustworthy for anything that is a steady fraction of the "
        + "server's time; rare short events are under-counted. estimated_wait_ms is samples multiplied by "
        + "the one-second period. Installing pg_wait_sampling (shared_preload_libraries, then CREATE "
        + "EXTENSION in the monitored database, then let the service reconnect) moves this target to the "
        + "extension_sampled tier with no other change.";

    /// <summary>True for exactly the three tokens above; the reads use it to keep an unrecognised state
    /// value from being echoed as an instrument.</summary>
    public static bool IsKnown(string? instrument) =>
        string.Equals(instrument, EngineCumulative, StringComparison.Ordinal)
        || string.Equals(instrument, ExtensionSampled, StringComparison.Ordinal)
        || string.Equals(instrument, ServiceSampled, StringComparison.Ordinal);
}
