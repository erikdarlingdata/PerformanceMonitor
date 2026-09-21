/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The memory terms of the §4b arithmetic, read out of the LATEST <c>pg_server_config</c> snapshot at or before the
    /// window's end — <c>PgTargetConfigSnapshotSql</c>'s anchor and its three-value <c>source</c> exclusion, copied not
    /// narrowed (the argument is on that constant) — with a closed name list of its own: the five terms the design names
    /// (<c>shared_buffers</c>, <c>work_mem</c>, <c>max_connections</c>, <c>maintenance_work_mem</c>,
    /// <c>autovacuum_max_workers</c>), the two multipliers PostgreSQL's own model adds (<c>max_parallel_workers_per_gather</c>
    /// — each parallel worker gets its own <c>work_mem</c>; <c>autovacuum_work_mem</c> — when set it replaces
    /// <c>maintenance_work_mem</c> for the autovacuum workers), <c>wal_buffers</c> (reported RESOLVED by <c>pg_settings</c>
    /// after startup, so the <c>-1</c> auto value never reaches this read on a running server), and
    /// <c>effective_cache_size</c> for the plausibility line. <c>$1</c> server_id, <c>$2</c> window end.
    ///
    /// <para><b>Why a second read of the snapshot rather than the config family's facts.</b> The config partial emits a
    /// <c>CONFIG_PG_*</c> fact for four of these terms and this collector runs after it (emission order, pinned), so the
    /// facts ARE in the list — but three of the terms have no v1 fact, a term whose text failed to normalise is silently
    /// absent from the list, and an arithmetic whose inputs come half from facts and half from a table has two
    /// provenances to explain when they disagree. One read, one anchor, every term from the same rows; the bytes per
    /// term travel on the fact's metadata so the advice never re-parses text.</para>
    /// </summary>
    public const string PgTargetMemoryConfigSql = @"
SELECT
    c.name,
    c.setting,
    c.unit,
    c.collection_time
FROM pg_server_config AS c
WHERE c.server_id = $1
AND   c.collection_time = (
          SELECT MAX(collection_time)
          FROM pg_server_config
          WHERE server_id = $1
          AND   collection_time <= $2)
AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
AND   c.name IN (
          'shared_buffers', 'work_mem', 'max_connections', 'max_parallel_workers_per_gather',
          'maintenance_work_mem', 'autovacuum_work_mem', 'autovacuum_max_workers',
          'wal_buffers', 'effective_cache_size')";

    /// <summary>
    /// The host's memory over the window, one row per <c>pg_cpu_utilization</c> capture in time order: the five
    /// <c>memory_*_bytes</c> columns and <c>configured_memory_bytes</c> V136 put on the capacity row (Performance Insights
    /// <c>os.memory.*</c>, kilobytes × 1024; Serverless v2 capacity × 2 GiB). Every row in the window is returned,
    /// memory-carrying or not, because the pre-V136 rule is a RATIO — fewer than half the rows carrying
    /// <c>memory_total_bytes</c> is <c>unavailable</c> — and the consecutive-sample gate needs the gaps. <c>$1</c> server_id,
    /// <c>$2</c>/<c>$3</c> window (naive UTC). Bounded by the window and the (server_id, collection_time) index; a
    /// seven-day window at the five-minute grain is ~2,000 narrow rows.
    ///
    /// <para><b>A stock PostgreSQL target has no row here at all</b> — there is no OS memory source from inside the engine,
    /// and <c>PgCpuUtilizationCollector.AppliesTo</c> gates the only source (the AWS API) to Aurora. Zero rows is therefore
    /// the honest arm (<c>reason_no_host_memory_source</c>), never "no memory" and never a fabricated denominator for the
    /// arithmetic.</para>
    /// </summary>
    public const string PgTargetMemoryHostSql = @"
SELECT
    w.collection_time,
    w.memory_total_bytes,
    w.memory_free_bytes,
    w.memory_cached_bytes,
    w.memory_active_bytes,
    w.configured_memory_bytes,
    w.memory_buffers_bytes
FROM pg_cpu_utilization AS w
WHERE w.server_id = $1
AND   w.collection_time >= $2
AND   w.collection_time <= $3
ORDER BY w.collection_time";

    /// <summary>One <c>pg_cpu_utilization</c> row's memory columns; every field nullable because a pre-V136 row (or a
    /// Performance Insights endpoint without <c>os.memory.*</c>) has them NULL.</summary>
    internal readonly record struct HostMemorySample(long? TotalBytes, long? FreeBytes, long? CachedBytes, long? ActiveBytes, long? ConfiguredBytes, long? BuffersBytes = null);

    /// <summary>
    /// The window's host-memory summary, computed from the sample series in one pass. <c>Samples</c> is every row;
    /// <c>SamplesWithMemory</c> those with a positive <c>memory_total_bytes</c> (a zero total is not a measurement). Shares
    /// are computed per sample from that sample's own total, never from a window aggregate, so a Serverless instance that
    /// scaled mid-window is read minute by minute. <c>SustainedMinReclaimableShare</c> is the minimum over every run of
    /// <c>sustain</c> CONSECUTIVE memory-carrying rows of the run's MAXIMUM share — the worst share the host held for that
    /// long — and null when no such run exists (fewer memory rows than the sustain count, or every run broken by a
    /// memory-less row: a gap in the series is a gap in the evidence, not a continuation).
    /// <c>PeakBuffersShare</c> is <c>memory_buffers_bytes / memory_total_bytes</c> at its widest — STATED, not folded into
    /// the reclaimable share: the design defines reclaimable as free + cached, and on a database host the kernel's block
    /// buffers are small; whether they join the share is the calibration's call, and the figure is on the fact for it.
    /// </summary>
    internal readonly record struct HostMemorySummary(
        int Samples,
        int SamplesWithMemory,
        long? TotalMinBytes,
        long? TotalMaxBytes,
        long? ConfiguredMinBytes,
        double? MinReclaimableShare,
        double? MeanReclaimableShare,
        double? SustainedMinReclaimableShare,
        double? PeakActiveShare,
        double? PeakBuffersShare);

    internal static HostMemorySummary SummariseHostMemory(IReadOnlyList<HostMemorySample> samples, int sustain)
    {
        var withMemory = 0;
        long? totalMin = null, totalMax = null, configuredMin = null;
        double? minShare = null, peakActive = null, sustainedMin = null, peakBuffers = null;
        var shareSum = 0.0;
        var run = new List<double>(Math.Max(sustain, 1));

        foreach (var s in samples)
        {
            if (s.TotalBytes is not { } total || total <= 0)
            {
                run.Clear();
                continue;
            }

            withMemory++;
            totalMin = totalMin is { } tmin ? Math.Min(tmin, total) : total;
            totalMax = totalMax is { } tmax ? Math.Max(tmax, total) : total;
            if (s.ConfiguredBytes is { } configured && configured > 0)
                configuredMin = configuredMin is { } cmin ? Math.Min(cmin, configured) : configured;

            /* Reclaimable = free + cached, each NULL read as 0 — a missing cached counter understates what the OS could
               give back, which errs toward reporting pressure, never toward hiding it. Clamped to [0, 1]: PI's minute
               averages of separately sampled counters can sum a hair past the total. */
            var reclaimable = Math.Clamp(((s.FreeBytes ?? 0) + (s.CachedBytes ?? 0)) / (double)total, 0.0, 1.0);
            shareSum += reclaimable;
            minShare = minShare is { } m ? Math.Min(m, reclaimable) : reclaimable;

            if (s.ActiveBytes is { } active)
            {
                var activeShare = Math.Clamp(active / (double)total, 0.0, 1.0);
                peakActive = peakActive is { } p ? Math.Max(p, activeShare) : activeShare;
            }

            if (s.BuffersBytes is { } buffers)
            {
                var buffersShare = Math.Clamp(buffers / (double)total, 0.0, 1.0);
                peakBuffers = peakBuffers is { } pb ? Math.Max(pb, buffersShare) : buffersShare;
            }

            run.Add(reclaimable);
            if (run.Count > sustain) run.RemoveAt(0);
            if (run.Count == sustain)
            {
                var worstHeld = run[0];
                for (var i = 1; i < run.Count; i++) worstHeld = Math.Max(worstHeld, run[i]);
                sustainedMin = sustainedMin is { } sm ? Math.Min(sm, worstHeld) : worstHeld;
            }
        }

        return new HostMemorySummary(
            Samples: samples.Count,
            SamplesWithMemory: withMemory,
            TotalMinBytes: totalMin,
            TotalMaxBytes: totalMax,
            ConfiguredMinBytes: configuredMin,
            MinReclaimableShare: minShare,
            MeanReclaimableShare: withMemory > 0 ? shareSum / withMemory : null,
            SustainedMinReclaimableShare: sustainedMin,
            PeakActiveShare: peakActive,
            PeakBuffersShare: peakBuffers);
    }

    /// <summary>The memory terms as normalised from the snapshot; every optional term nullable so the fact states which
    /// were present rather than substituting a zero that claims a knob is unset.</summary>
    internal readonly record struct MemoryTerms(
        long SharedBuffersBytes,
        long WorkMemBytes,
        double MaxConnections,
        double? ParallelWorkersPerGather,
        long? MaintWorkMemBytes,
        long? AutovacuumWorkMemBytes,
        double? AutovacuumMaxWorkers,
        long? WalBuffersBytes,
        long? EffectiveCacheSizeBytes)
    {
        /// <summary><c>max_connections × work_mem × (1 + max_parallel_workers_per_gather)</c> — each backend may run one sort
        /// or hash at <c>work_mem</c> and each of its parallel workers another; an absent parallel knob multiplies by 1.</summary>
        public double BackendTermBytes
        {
            get { return MaxConnections * WorkMemBytes * (1 + Math.Max(0, ParallelWorkersPerGather ?? 0)); }
        }

        /// <summary><c>autovacuum_max_workers × (autovacuum_work_mem, else maintenance_work_mem)</c> — PostgreSQL's own
        /// substitution rule; 0 when neither per-worker term is known or the worker count is.</summary>
        public double AutovacuumTermBytes
        {
            get
            {
                return AutovacuumMaxWorkers is { } workers && (AutovacuumWorkMemBytes ?? MaintWorkMemBytes) is { } perWorker
                    ? Math.Max(0, workers) * perWorker
                    : 0;
            }
        }

        /// <summary>The whole configured worst case in bytes.</summary>
        public double WorstCaseBytes
        {
            get { return SharedBuffersBytes + BackendTermBytes + AutovacuumTermBytes + Math.Max(0, WalBuffersBytes ?? 0); }
        }
    }

    /// <summary>
    /// <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> and <c>PG_HOST_MEMORY_PRESSURE</c> (filled by lane 32 of #3691 — design §4b, the
    /// review's cross-server-transfer blocker: a configuration is judged against THIS host, never against a number that
    /// was right somewhere else). Two reads: the memory terms from the latest <c>pg_server_config</c> snapshot at or
    /// before the window's end, and the host's memory from every <c>pg_cpu_utilization</c> row in the window.
    ///
    /// <para><b>The three outcomes, in order of what the store had.</b> No <c>pg_cpu_utilization</c> row in the window
    /// (a stock target) ⇒ ONE fact, <c>PG_HOST_MEMORY_PRESSURE</c> in its <c>unavailable</c> shape with
    /// <c>reason_no_host_memory_source</c>, and NO composition fact — the arithmetic has no denominator and a ratio over a
    /// guessed one would be the lie §4b exists to stop; the configured sum still rides that fact's metadata so the
    /// advice can state it. Rows present but fewer than half carrying <c>memory_total_bytes</c> ⇒ the same shape with
    /// <c>reason_memory_columns_sparse</c> (pre-V136 rows, or a Performance Insights endpoint without <c>os.memory.*</c>).
    /// Otherwise the pressure fact is measured (value = the window's minimum reclaimable share; the SUSTAINED minimum the
    /// scorer grades beside it) and, when the snapshot yielded the three mandatory terms, the composition fact is
    /// emitted with value = the ratio of the configured worst case to the window's MINIMUM <c>memory_total_bytes</c>.</para>
    ///
    /// <para><b>Why the minimum total, and not <c>configured_memory_bytes</c>, is the denominator on Serverless.</b>
    /// <c>configured_memory_bytes</c> is the vendor's nominal allocation for the minute's capacity (ACU × 2 GiB);
    /// <c>memory_total_bytes</c> is what the OS actually reported, minute by minute, as the instance scaled. The two track,
    /// but only one is measured, and a Serverless instance is smallest — and an overcommitted configuration most
    /// dangerous — at the LOW end of its scaling range, which is exactly the window's minimum total. The configured
    /// minimum is stated beside it (<c>is_serverless = 1</c>) so the reader sees the range the instance moved through;
    /// on a provisioned class the minimum and maximum total are the same number and the configured figure is absent.</para>
    ///
    /// <para><b>The mandatory terms</b> are <c>shared_buffers</c>, <c>work_mem</c> and <c>max_connections</c>: without any
    /// one of them the sum is not the model, and the fact is not emitted (a partial sum stated as the worst case would
    /// understate it). The other terms are optional and their absence is visible: a key that is not on the fact was not
    /// in the snapshot or did not normalise; no zero stands in for it. Aurora note: the storage tier means the OS page
    /// cache matters less than on stock, but the pressure fact is about the INSTANCE's memory — which is what
    /// <c>shared_buffers</c> and every <c>work_mem</c> allocation come out of — so the reading stands.</para>
    ///
    /// <para>/* filled by lane 32 of #3691 — the marker stays, as v1's did. */ Conventions: every command sets
    /// <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes <c>context.CancellationToken</c>,
    /// <c>$N</c> positional, no bare clock (StoreSqlClockDisciplineTests), the catch is the shared degrade shape around
    /// each read. Both tables are CollectorCatalog targets already, so the FROM/JOIN census admits them with no edit.</para>
    /// </summary>
    private async partial Task CollectMemoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        MemoryTerms? terms = null;
        double? snapshotAgeSeconds = null;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var settings = new Dictionary<string, (string? Setting, string? Unit)>(StringComparer.Ordinal);
            DateTime? snapshotTime = null;
            using (var cmd = new NpgsqlCommand(PgTargetMemoryConfigSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (reader.IsDBNull(0)) continue;
                    settings[reader.GetString(0)] = (reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2));
                    snapshotTime ??= reader.IsDBNull(3) ? null : reader.GetDateTime(3);
                }
            }

            if (snapshotTime is { } at)
            {
                snapshotAgeSeconds = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(at)).TotalSeconds);
                terms = ReadTerms(settings);
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_server_config arrived in V102; a pre-migration store raises 42P01, which the reporter classifies quiet.
               The host read below still runs — a pressure reading needs no snapshot. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var samples = new List<HostMemorySample>();
            using (var cmd = new NpgsqlCommand(PgTargetMemoryHostSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    samples.Add(new HostMemorySample(
                        TotalBytes: reader.IsDBNull(1) ? null : reader.GetInt64(1),
                        FreeBytes: reader.IsDBNull(2) ? null : reader.GetInt64(2),
                        CachedBytes: reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        ActiveBytes: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                        ConfiguredBytes: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                        BuffersBytes: reader.IsDBNull(6) ? null : reader.GetInt64(6)));
                }
            }

            /* Engine off the registry fact emitted first in the pass (emission order: Metadata before everything), through
               MonitoredEngineKind — never a column's presence (#2530). Null when the registry fact is absent: unknown. */
            var registry = facts.Find(f => f.Key == PgTargetFactKeys.ServerMajorVersion);
            bool? isAurora = registry is null ? null : registry.Metadata.GetValueOrDefault("is_aurora") > 0;
            EmitMemoryFacts(context, facts, SummariseHostMemory(samples, PgTargetScorer.HostMemoryPressureSustainSamples), terms, snapshotAgeSeconds, isAurora);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_cpu_utilization arrived at V106 and its memory columns at V136; a pre-migration store raises 42P01 /
               42703 here, which the reporter classifies quiet. Degrades to "no fact" — the D6 disclosure then says the
               family was not collected, and WHY is reported, not assumed (#2826). An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>The snapshot's rows → the terms, or null when a mandatory term is missing or did not normalise. Every
    /// unit goes through <see cref="PgSettingValue"/>; a <c>-1</c> sentinel on <c>autovacuum_work_mem</c> means "inherit"
    /// and is read as absent (so <c>maintenance_work_mem</c> stands in — PostgreSQL's rule); a negative <c>wal_buffers</c>
    /// (the unresolved auto value, which a running server never reports) is read as absent rather than as a negative term.</summary>
    internal static MemoryTerms? ReadTerms(IReadOnlyDictionary<string, (string? Setting, string? Unit)> settings)
    {
        var sharedBuffers = Bytes(settings, "shared_buffers");
        var workMem = Bytes(settings, "work_mem");
        var maxConnections = Number(settings, "max_connections");
        if (sharedBuffers is not { } sb || sb < 0 || workMem is not { } wm || wm < 0 || maxConnections is not { } mc || mc < 0)
            return null;

        var autovacuumWorkMem = Bytes(settings, "autovacuum_work_mem");
        var walBuffers = Bytes(settings, "wal_buffers");
        return new MemoryTerms(
            SharedBuffersBytes: sb,
            WorkMemBytes: wm,
            MaxConnections: mc,
            ParallelWorkersPerGather: Number(settings, "max_parallel_workers_per_gather"),
            MaintWorkMemBytes: Bytes(settings, "maintenance_work_mem") is { } mwm && mwm >= 0 ? mwm : null,
            AutovacuumWorkMemBytes: autovacuumWorkMem is { } avwm && avwm >= 0 ? avwm : null,
            AutovacuumMaxWorkers: Number(settings, "autovacuum_max_workers"),
            WalBuffersBytes: walBuffers is { } wb && wb >= 0 ? wb : null,
            EffectiveCacheSizeBytes: Bytes(settings, "effective_cache_size") is { } ecs && ecs >= 0 ? ecs : null);

        static long? Bytes(IReadOnlyDictionary<string, (string? Setting, string? Unit)> s, string name)
        {
            return s.TryGetValue(name, out var row) ? PgSettingValue.ToBytes(row.Setting, row.Unit) : null;
        }

        static double? Number(IReadOnlyDictionary<string, (string? Setting, string? Unit)> s, string name)
        {
            return s.TryGetValue(name, out var row) ? PgSettingValue.ToNumber(row.Setting) : null;
        }
    }

    /// <summary>
    /// The pure emission step, separated from the reads so the outcomes are executable without a store. The pre-V136
    /// rule is the strict half: <c>2 × samples_with_memory &lt; samples</c> is sparse.
    ///
    /// <para><b>Zero rows: known-absent versus unknown (the kernel family's shape, lane 28).</b> The only writer of
    /// <c>pg_cpu_utilization</c> is gated to Aurora (<c>PgCpuUtilizationCollector.AppliesTo</c>), so on a target the
    /// registry stamps STOCK the absence is known by architecture and the <c>unavailable</c> fact with
    /// <c>reason_no_host_memory_source</c> is emitted — the honesty arm, and the one fact a stock target's memory family
    /// ever carries. On an AURORA target zero rows in the window is UNKNOWN — the CPU collector has not run yet, or
    /// Performance Insights is off — and nothing is emitted: the D6 coverage disclosure already says the CPU family was
    /// not collected, and an "unavailable" that might be "not yet" would be the wrong word. A missing registry fact is
    /// unknown too. The plumbing e2e's quiet stock target therefore carries exactly one more fact than before this lane
    /// (the registry fact plus this one), pinned there deliberately.</para>
    /// </summary>
    internal static void EmitMemoryFacts(AnalysisContext context, List<Fact> facts, HostMemorySummary host, MemoryTerms? terms, double? snapshotAgeSeconds, bool? isAurora)
    {
        if (host.Samples == 0 && isAurora != false) return;

        var pressure = new Fact
        {
            Source = PgTargetSources.MemorySource,
            Key = PgTargetFactKeys.HostMemoryPressure,
            Value = 0,
            ServerId = context.ServerId,
            Metadata =
            {
                [PgTargetScorer.MemorySamplesKey] = host.Samples,
                [PgTargetScorer.MemorySamplesWithMemoryKey] = host.SamplesWithMemory,
                ["observed_ms"] = context.ObservedDurationMs,
            },
        };

        var unavailable = host.Samples == 0 || 2 * host.SamplesWithMemory < host.Samples || host.TotalMinBytes is null;
        if (unavailable)
        {
            pressure.Metadata["unavailable"] = 1;
            pressure.Metadata[host.Samples == 0 ? PgTargetScorer.HostMemoryReasonNoSourceKey : PgTargetScorer.HostMemoryReasonSparseKey] = 1;
            /* The configured sum rides the unavailable fact so the advice can state it beside "no host memory to compare
               against" — the terms are true even when the denominator is not collected. */
            if (terms is { } t) WriteTerms(pressure, t, snapshotAgeSeconds);
            facts.Add(pressure);
            return;
        }

        pressure.Value = host.MinReclaimableShare ?? 0;
        pressure.Metadata[PgTargetScorer.HostMemoryMinReclaimableShareKey] = host.MinReclaimableShare ?? 0;
        pressure.Metadata[PgTargetScorer.HostMemoryMeanReclaimableShareKey] = host.MeanReclaimableShare ?? 0;
        pressure.Metadata[PgTargetScorer.HostMemorySustainSamplesKey] = PgTargetScorer.HostMemoryPressureSustainSamples;
        pressure.Metadata[PgTargetScorer.MemoryTotalMinBytesKey] = host.TotalMinBytes!.Value;
        pressure.Metadata[PgTargetScorer.MemoryTotalMaxBytesKey] = host.TotalMaxBytes ?? host.TotalMinBytes.Value;
        if (host.SustainedMinReclaimableShare is { } sustained) pressure.Metadata[PgTargetScorer.HostMemorySustainedMinReclaimableShareKey] = sustained;
        if (host.PeakActiveShare is { } active) pressure.Metadata[PgTargetScorer.HostMemoryPeakActiveShareKey] = active;
        if (host.PeakBuffersShare is { } buffers) pressure.Metadata[PgTargetScorer.HostMemoryPeakBuffersShareKey] = buffers;
        pressure.Metadata[PgTargetScorer.MemoryIsServerlessKey] = host.ConfiguredMinBytes is null ? 0 : 1;
        if (host.ConfiguredMinBytes is { } configured) pressure.Metadata[PgTargetScorer.MemoryConfiguredMinBytesKey] = configured;
        facts.Add(pressure);

        if (terms is not { } composed) return;

        var denominator = (double)host.TotalMinBytes.Value;
        var overcommit = new Fact
        {
            Source = PgTargetSources.MemorySource,
            Key = PgTargetFactKeys.ConfigMemoryOvercommit,
            Value = composed.WorstCaseBytes / denominator,
            ServerId = context.ServerId,
            Metadata =
            {
                [PgTargetScorer.MemoryOvercommitRatioKey] = composed.WorstCaseBytes / denominator,
                [PgTargetScorer.MemoryTotalMinBytesKey] = host.TotalMinBytes.Value,
                [PgTargetScorer.MemoryTotalMaxBytesKey] = host.TotalMaxBytes ?? host.TotalMinBytes.Value,
                [PgTargetScorer.MemoryIsServerlessKey] = host.ConfiguredMinBytes is null ? 0 : 1,
                [PgTargetScorer.MemorySamplesKey] = host.Samples,
                [PgTargetScorer.MemorySamplesWithMemoryKey] = host.SamplesWithMemory,
                ["observed_ms"] = context.ObservedDurationMs,
            },
        };
        if (host.ConfiguredMinBytes is { } configuredMin) overcommit.Metadata[PgTargetScorer.MemoryConfiguredMinBytesKey] = configuredMin;
        WriteTerms(overcommit, composed, snapshotAgeSeconds);
        /* engine-defined plausibility line, stated not graded: a planner assumption larger than the smallest box the
           window saw is a lie to the planner. Read against the MINIMUM total for the same reason the ratio is. */
        if (composed.EffectiveCacheSizeBytes is { } ecs)
            overcommit.Metadata[PgTargetScorer.MemoryEffectiveCacheExceedsTotalKey] = ecs > host.TotalMinBytes.Value ? 1 : 0;
        facts.Add(overcommit);
    }

    private static void WriteTerms(Fact fact, MemoryTerms terms, double? snapshotAgeSeconds)
    {
        fact.Metadata[PgTargetScorer.MemorySharedBuffersBytesKey] = terms.SharedBuffersBytes;
        fact.Metadata[PgTargetScorer.MemoryWorkMemBytesKey] = terms.WorkMemBytes;
        fact.Metadata[PgTargetScorer.MemoryMaxConnectionsKey] = terms.MaxConnections;
        if (terms.ParallelWorkersPerGather is { } parallel) fact.Metadata[PgTargetScorer.MemoryParallelWorkersPerGatherKey] = parallel;
        if (terms.MaintWorkMemBytes is { } maint) fact.Metadata[PgTargetScorer.MemoryMaintWorkMemBytesKey] = maint;
        if (terms.AutovacuumWorkMemBytes is { } avwm) fact.Metadata[PgTargetScorer.MemoryAutovacuumWorkMemBytesKey] = avwm;
        if (terms.AutovacuumMaxWorkers is { } workers) fact.Metadata[PgTargetScorer.MemoryAutovacuumMaxWorkersKey] = workers;
        if (terms.WalBuffersBytes is { } wal) fact.Metadata[PgTargetScorer.MemoryWalBuffersBytesKey] = wal;
        if (terms.EffectiveCacheSizeBytes is { } ecs) fact.Metadata[PgTargetScorer.MemoryEffectiveCacheSizeBytesKey] = ecs;
        fact.Metadata[PgTargetScorer.MemoryBackendTermBytesKey] = terms.BackendTermBytes;
        fact.Metadata[PgTargetScorer.MemoryAutovacuumTermBytesKey] = terms.AutovacuumTermBytes;
        fact.Metadata[PgTargetScorer.MemoryWorstCaseBytesKey] = terms.WorstCaseBytes;
        if (snapshotAgeSeconds is { } age) fact.Metadata[PgTargetScorer.MemorySnapshotAgeSecondsKey] = age;
    }
}
