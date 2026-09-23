/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The settings the <c>CONFIG_PG_*</c> facts are cut from, read out of the LATEST <c>pg_server_config</c>
    /// snapshot at or before the window's end. One read yields every key in the family: the two knobs
    /// (<c>shared_buffers</c>, <c>max_wal_size</c>), the convention and meta checks the same snapshot gives for
    /// free (<c>effective_cache_size</c>, <c>random_page_cost</c>, <c>track_io_timing</c>), the write-chain
    /// context (<c>checkpoint_timeout</c>, <c>checkpoint_completion_target</c>, <c>min_wal_size</c>,
    /// <c>wal_compression</c>), the bgwriter knobs the buffer composite reads beside <c>shared_buffers</c>
    /// (<c>bgwriter_delay</c>, <c>bgwriter_lru_maxpages</c>), and the keys other lanes score from a fact this
    /// read emits (<c>max_connections</c> / <c>superuser_reserved_connections</c> / <c>reserved_connections</c>
    /// for lane 3's ceiling — the last a PostgreSQL 16+ name, absent from the snapshot before 16 and then simply
    /// not emitted, so the ceiling subtracts 0; <c>work_mem</c> for lane 6, <c>maintenance_work_mem</c> and
    /// <c>autovacuum</c> for lane 4). <c>$1</c> server_id, <c>$2</c> window end, <c>$3</c> the lower bound
    /// <see cref="ConfigSnapshotLowerBounds"/> hands it (#3928).
    ///
    /// <para><b>The shape is <c>DarlingPgServerConfigReader.CurrentConfigSql</c>'s</b>, with two deliberate
    /// differences. The snapshot is anchored on <c>MAX(collection_time)</c> AT OR BEFORE <c>$2</c> rather than
    /// the newest row the server has, because an analysis window can be historical (<c>compare_analysis</c>,
    /// an anchored <c>analyze_server</c>) and the setting that applied THEN is the one the facts should state;
    /// for the ordinary trailing window the two are the same row. And the name list is closed, so the read
    /// returns a dozen rows rather than the ~400 the tool returns — an hourly snapshot of the whole of
    /// <c>pg_settings</c> is the collector's job, not this pass's.</para>
    ///
    /// <para><b>The three-value <c>source</c> exclusion is copied, not narrowed.</b> The reader excludes
    /// <c>client</c>, <c>session</c> and <c>override</c> because those rows describe the collector's OWN
    /// session, not the server (the argument is on <c>SessionScopedSources</c>); none of the names here can
    /// carry a session scope, but a filter that differs from the shipped reader's is a filter someone will one
    /// day have to explain, and the parse-analysis pin over the reader family rests on the list being spelled
    /// inline. <c>is_default</c> is PostgreSQL's own verdict (<c>source = 'default'</c>), never a string
    /// comparison against <c>boot_val</c> — the reader's comment records the three settings that comparison
    /// gets wrong.</para>
    /// </summary>
    public const string PgTargetConfigSnapshotSql = @"
SELECT
    c.name,
    c.setting,
    c.unit,
    c.boot_val,
    (coalesce(c.source, 'default') = 'default') AS is_default,
    coalesce(c.pending_restart, false) AS pending_restart,
    c.collection_time
FROM pg_server_config AS c
WHERE c.server_id = $1
AND   c.collection_time >= $3
AND   c.collection_time = (
          SELECT MAX(collection_time)
          FROM pg_server_config
          WHERE server_id = $1
          AND   collection_time >= $3
          AND   collection_time <= $2)
AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
/* V138 (#3691): the SERVER-WIDE population only. pg_server_config now also holds the per-database and
   per-role overrides out of pg_db_role_setting, which repeat a setting's NAME under a different scope -
   and the C# below loads these rows into a dictionary keyed by name, so an override row would either
   shadow the server-wide value or throw on the duplicate key. The CONFIG_PG_* facts stay server-wide in
   this lane by ruling: a per-database fact family needs a database dimension on the fact and an answer to
   ""which database's work_mem is the server's work_mem"", which is a later brief. Only the OUTER select
   needs this - the MAX(collection_time) subquery above is per SERVER, not per name, so an override row
   cannot move the anchor. */
AND   c.database_name IS NULL
AND   c.role_name IS NULL
AND   c.name IN (
          'shared_buffers', 'max_wal_size', 'min_wal_size', 'effective_cache_size', 'random_page_cost',
          'track_io_timing', 'checkpoint_timeout', 'checkpoint_completion_target', 'wal_compression',
          'bgwriter_delay', 'bgwriter_lru_maxpages',
          'max_connections', 'superuser_reserved_connections', 'reserved_connections',
          'work_mem', 'maintenance_work_mem', 'autovacuum')";

    /// <summary>
    /// #3928: the lower bounds every <c>pg_server_config</c> snapshot read runs with, in order — this family's
    /// four and <c>PgTargetBaselineProvider.PgTargetClockSql</c>. The first is
    /// <see cref="AnalysisContext.LatestValueLookback"/> before the window's end. Bound on both the anchor's
    /// <c>MAX(collection_time)</c> and the outer row scan, it lets TimescaleDB plan the day's chunks instead of
    /// every retained one. The table keeps a year of hourly snapshots in one-day chunks, and a year in, planning
    /// them all cost each read seconds before it touched a row. Bounding only the <c>MAX</c> is not enough: the
    /// outer <c>collection_time = (…)</c> can exclude chunks only at run time.
    ///
    /// <para>The second is no bound at all (<see cref="DateTime.MinValue"/>, which Npgsql sends as
    /// <c>-infinity</c>), run only when the first found nothing. A target whose config collector has been dark
    /// for more than a day therefore still states the newest snapshot it has, the rule these reads had before, at
    /// the cost they had before. The newest snapshot in the day IS the newest snapshot the unbounded anchor
    /// finds, so the two runs cannot disagree; the bound decides only how much the planner has to look at. That
    /// is why these reads take a flat day and a fallback rather than their collector's cadence
    /// (<see cref="AnalysisContext.LatestValueStartFor"/>): they anchor on one snapshot per server, so there is
    /// no stale series for a bound to drop, and a cadence slower than the day costs the fallback, never a
    /// fact.</para>
    /// </summary>
    internal static DateTime[] ConfigSnapshotLowerBounds(DateTime windowEnd) =>
        [AsNaive(windowEnd - AnalysisContext.LatestValueLookback), DateTime.MinValue];

    /// <summary>One <c>pg_settings</c> row as the snapshot stores it: raw text plus its unit.</summary>
    private readonly record struct PgConfigSetting(string? Setting, string? Unit, string? BootVal, bool IsDefault, bool PendingRestart);

    /// <summary>
    /// The latest <c>pg_server_config</c> snapshot → the <c>CONFIG_PG_*</c> facts (filled by lane 2 — #3542
    /// step 2). Every fact's <see cref="Fact.Value"/> is the setting in a DISPLAY unit — megabytes for the
    /// memory knobs, seconds for <c>checkpoint_timeout</c>, the bare number for <c>random_page_cost</c> and
    /// the connection counts, 0/1 for the booleans — and its metadata carries the exact normalised figure
    /// (<c>bytes</c>, <c>ms</c>) beside PostgreSQL's <c>is_default</c> verdict and <c>pending_restart</c>, so a
    /// scorer never re-parses text and an advice block never rounds twice. The normalisation is
    /// <see cref="PgSettingValue"/>'s, the one piece of arithmetic in this family nobody had written before.
    ///
    /// <para><b>What scores and what is context.</b> The two knobs and the three convention checks score
    /// 0.4-when-bad in <c>PgTargetScorer.Config.cs</c> and root advisory cards (D5). <c>checkpoint_timeout</c>,
    /// <c>wal_compression</c>, <c>max_connections</c>, <c>superuser_reserved_connections</c> and
    /// <c>reserved_connections</c> are CONTEXT (base 0): the write chain's advice reads the first two from the
    /// full fact set to say "checkpoint_timeout is 5 min, the default" beside a checkpoint-pressure finding — the
    /// design's "defaults noted alongside checkpoint-pressure findings only" — and lane 3's ceiling reads the
    /// last three at collect time.
    /// <c>work_mem</c>, <c>maintenance_work_mem</c> and <c>autovacuum</c> are emitted here and scored by lanes
    /// 6 and 4; until those land they score 0 through the stub and are context.</para>
    ///
    /// <para><b>What rides which fact.</b> The bgwriter knobs (<c>bgwriter_delay</c>,
    /// <c>bgwriter_lru_maxpages</c>) are metadata on the <c>shared_buffers</c> fact, and
    /// <c>min_wal_size</c> / <c>checkpoint_timeout</c> / <c>checkpoint_completion_target</c> on the
    /// <c>max_wal_size</c> fact: the buffer and write collectors that run after this one read them off the
    /// in-memory fact list rather than re-reading the snapshot, and a knob that only ever qualifies another
    /// knob's advice does not need a key of its own. <c>snapshot_age_s</c> on every fact says how old the
    /// snapshot is relative to the window's end — the config collector runs hourly, and a reader deciding
    /// whether "at the default" is still true deserves to know the stamp is up to an hour behind.</para>
    ///
    /// <para>A setting whose text does not normalise (an unknown unit, a value that is not a number) emits
    /// no fact rather than a zero: an absent knob makes no claim, a fabricated 0 MB claims a misconfiguration
    /// that does not exist. A server with no snapshot at or before the window's end emits nothing — the
    /// config collector has not run for it yet, which the coverage witness and the span gate already
    /// describe.</para>
    /// </summary>
    private async partial Task CollectConfigFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var settings = new Dictionary<string, PgConfigSetting>(StringComparer.Ordinal);
            DateTime? snapshotTime = null;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            /* #3928: the day first, and every retained snapshot only when that found nothing. */
            foreach (var lowerBound in ConfigSnapshotLowerBounds(context.TimeRangeEnd))
            {
                using var cmd = new NpgsqlCommand(PgTargetConfigSnapshotSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
                cmd.Parameters.AddWithValue(lowerBound);

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (reader.IsDBNull(0)) continue;
                    var name = reader.GetString(0);
                    settings[name] = new PgConfigSetting(
                        Setting: reader.IsDBNull(1) ? null : reader.GetString(1),
                        Unit: reader.IsDBNull(2) ? null : reader.GetString(2),
                        BootVal: reader.IsDBNull(3) ? null : reader.GetString(3),
                        IsDefault: !reader.IsDBNull(4) && reader.GetBoolean(4),
                        PendingRestart: !reader.IsDBNull(5) && reader.GetBoolean(5));
                    snapshotTime ??= reader.IsDBNull(6) ? null : reader.GetDateTime(6);
                }

                if (settings.Count > 0) break;
            }

            if (settings.Count == 0 || snapshotTime is null) return;

            var snapshotAgeSeconds = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(snapshotTime.Value)).TotalSeconds);

            /* ── Knob 1: shared_buffers, with the bgwriter knobs the buffer composite reads beside it. ── */
            if (TryBytes(settings, "shared_buffers", out var sharedBuffers, out var sharedBuffersRow))
            {
                var fact = ConfigFact(context, PgTargetFactKeys.ConfigSharedBuffers, ToMb(sharedBuffers), sharedBuffersRow, snapshotAgeSeconds);
                fact.Metadata["bytes"] = sharedBuffers;
                if (TryBytes(settings, "effective_cache_size", out var ecsForRatio, out _))
                    fact.Metadata["effective_cache_size_bytes"] = ecsForRatio;
                if (TryMs(settings, "bgwriter_delay", out var bgwriterDelayMs))
                    fact.Metadata["bgwriter_delay_ms"] = bgwriterDelayMs;
                if (TryNumber(settings, "bgwriter_lru_maxpages", out var lruMaxPages))
                    fact.Metadata["bgwriter_lru_maxpages"] = lruMaxPages;
                facts.Add(fact);
            }

            /* ── Knob 2: max_wal_size, with the checkpoint rhythm the write chain reads beside it. ── */
            if (TryBytes(settings, "max_wal_size", out var maxWalSize, out var maxWalRow))
            {
                var fact = ConfigFact(context, PgTargetFactKeys.ConfigMaxWalSize, ToMb(maxWalSize), maxWalRow, snapshotAgeSeconds);
                fact.Metadata["bytes"] = maxWalSize;
                if (TryBytes(settings, "min_wal_size", out var minWalSize, out _))
                    fact.Metadata["min_wal_size_bytes"] = minWalSize;
                if (TryMs(settings, "checkpoint_timeout", out var checkpointTimeoutMs))
                    fact.Metadata["checkpoint_timeout_s"] = checkpointTimeoutMs / 1000.0;
                if (TryNumber(settings, "checkpoint_completion_target", out var completionTarget))
                    fact.Metadata["checkpoint_completion_target"] = completionTarget;

                /* Lane 15 (#3691 calibration §A4): on aurora-postgres max_wal_size governs nothing — Aurora storage
                   owns checkpointing and the checkpointer counters are synthetic (sixty timed an hour, requested
                   share 0, on all fifty measured clusters) — so "at the shipped default" is not a convention
                   finding there; it is the parameter group's value for a knob the engine does not consult. The
                   fact is still emitted (the write partial reads checkpoint_timeout_s and bytes off it, and the
                   value is true) but stamped not_applicable, and PgTargetScorer.Config.cs scores it 0 off the flag
                   so no advisory card roots and no co-fire can lift it. Engine off the registry fact emitted just
                   before this method (emission order: Metadata before Config), through MonitoredEngineKind — never
                   a column's presence (#2530). */
                var registry = facts.Find(f => f.Key == PgTargetFactKeys.ServerMajorVersion);
                if (registry is not null && registry.Metadata.GetValueOrDefault("is_aurora") > 0)
                {
                    fact.Metadata["not_applicable"] = 1;
                    fact.Metadata["not_applicable_on_aurora"] = 1;
                }
                facts.Add(fact);
            }

            /* ── Convention checks the same snapshot yields for free (D5: advisory band only, never amplified). ── */
            if (TryBytes(settings, "effective_cache_size", out var effectiveCacheSize, out var ecsRow))
            {
                var fact = ConfigFact(context, PgTargetFactKeys.ConfigEffectiveCacheSize, ToMb(effectiveCacheSize), ecsRow, snapshotAgeSeconds);
                fact.Metadata["bytes"] = effectiveCacheSize;
                facts.Add(fact);
            }

            if (TryNumber(settings, "random_page_cost", out var randomPageCost, out var rpcRow))
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigRandomPageCost, randomPageCost, rpcRow, snapshotAgeSeconds));

            if (TryBool(settings, "track_io_timing", out var trackIoTiming, out var tioRow))
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigTrackIoTiming, trackIoTiming ? 1 : 0, tioRow, snapshotAgeSeconds));

            /* ── Write-chain context (base 0): stated in the checkpoint-pressure advice, never a root. ── */
            if (TryMs(settings, "checkpoint_timeout", out var checkpointTimeout, out var ctRow))
            {
                var fact = ConfigFact(context, PgTargetFactKeys.ConfigCheckpointTimeout, checkpointTimeout / 1000.0, ctRow, snapshotAgeSeconds);

                /* #3868: the same stamp max_wal_size takes above, for the same reason and off the same flag.
                   checkpoint_timeout is base 0 here, so nothing was grading it — but the composed advice tells
                   an operator to tune it WITH max_wal_size, and on aurora-postgres that is a knob the storage
                   layer ignores (lane 15 / #3691 §A4: sixty timed "checkpoints" an hour, requested share 0, on
                   all fifty measured clusters). audit_config already rendered the pair not_applicable through an
                   engine-side arm (#3867, rider 1 of Erik's ruling); this is the fact saying it too, so
                   get_analysis_facts and every other reader agree with the tool. The value is still emitted —
                   it is what the parameter group holds, and the write partial reads it off max_wal_size's
                   metadata. Engine off the registry fact emitted before this method, through
                   MonitoredEngineKind — never a column's presence (#2530). */
                var registry = facts.Find(f => f.Key == PgTargetFactKeys.ServerMajorVersion);
                if (registry is not null && registry.Metadata.GetValueOrDefault("is_aurora") > 0)
                {
                    fact.Metadata["not_applicable"] = 1;
                    fact.Metadata["not_applicable_on_aurora"] = 1;
                }
                facts.Add(fact);
            }

            if (settings.TryGetValue("wal_compression", out var walCompression) && walCompression.Setting is not null)
            {
                /* PostgreSQL 15 turned the boolean into an enum (off / pglz / lz4 / zstd; 'on' still means pglz).
                   Anything that is not off is on — the fact says whether full-page images are compressed, not
                   with which codec. */
                var on = PgSettingValue.ToBool(walCompression.Setting) ?? !string.Equals(walCompression.Setting.Trim(), "off", StringComparison.OrdinalIgnoreCase);
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigWalCompression, on ? 1 : 0, walCompression, snapshotAgeSeconds));
            }

            /* ── Lane 3's ceiling (base 0 here; read at score time by the saturation fact). ── */
            if (TryNumber(settings, "max_connections", out var maxConnections, out var mcRow))
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigMaxConnections, maxConnections, mcRow, snapshotAgeSeconds));

            if (TryNumber(settings, "superuser_reserved_connections", out var reserved, out var srRow))
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigSuperuserReserved, reserved, srRow, snapshotAgeSeconds));

            /* PostgreSQL 16+ only (pg_use_reserved_connections); a pre-16 snapshot has no such row and the fact is
               simply not emitted — the ceiling treats absence as 0, which is also the 16+ default. */
            if (TryNumber(settings, "reserved_connections", out var reservedForRole, out var rcRow))
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigReservedConnections, reservedForRole, rcRow, snapshotAgeSeconds));

            /* ── Lanes 6 and 4 score these; emitted here because the read already has them. ── */
            if (TryBytes(settings, "work_mem", out var workMem, out var wmRow))
            {
                var fact = ConfigFact(context, PgTargetFactKeys.ConfigWorkMem, ToMb(workMem), wmRow, snapshotAgeSeconds);
                fact.Metadata["bytes"] = workMem;
                facts.Add(fact);
            }

            if (TryBytes(settings, "maintenance_work_mem", out var maintWorkMem, out var mwmRow))
            {
                var fact = ConfigFact(context, PgTargetFactKeys.ConfigMaintWorkMem, ToMb(maintWorkMem), mwmRow, snapshotAgeSeconds);
                fact.Metadata["bytes"] = maintWorkMem;
                facts.Add(fact);
            }

            if (TryBool(settings, "autovacuum", out var autovacuumOn, out var avRow))
                facts.Add(ConfigFact(context, PgTargetFactKeys.ConfigAutovacuumOff, autovacuumOn ? 0 : 1, avRow, snapshotAgeSeconds));
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_server_config arrived in V102; a pre-migration store raises 42P01 here, which the reporter
               classifies quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    private static Fact ConfigFact(AnalysisContext context, string key, double value, PgConfigSetting row, double snapshotAgeSeconds) =>
        new()
        {
            Source = PgTargetSources.ConfigSource,
            Key = key,
            Value = value,
            ServerId = context.ServerId,
            Metadata =
            {
                ["is_default"] = row.IsDefault ? 1 : 0,
                ["pending_restart"] = row.PendingRestart ? 1 : 0,
                ["snapshot_age_s"] = snapshotAgeSeconds,
            },
        };

    private static double ToMb(long bytes) => bytes / (1024.0 * 1024.0);

    private static bool TryBytes(Dictionary<string, PgConfigSetting> settings, string name, out long bytes, out PgConfigSetting row)
    {
        bytes = 0;
        if (!settings.TryGetValue(name, out row)) return false;
        var parsed = PgSettingValue.ToBytes(row.Setting, row.Unit);
        if (parsed is null) return false;
        bytes = parsed.Value;
        return true;
    }

    private static bool TryMs(Dictionary<string, PgConfigSetting> settings, string name, out double ms) =>
        TryMs(settings, name, out ms, out _);

    private static bool TryMs(Dictionary<string, PgConfigSetting> settings, string name, out double ms, out PgConfigSetting row)
    {
        ms = 0;
        if (!settings.TryGetValue(name, out row)) return false;
        var parsed = PgSettingValue.ToMs(row.Setting, row.Unit);
        if (parsed is null) return false;
        ms = parsed.Value;
        return true;
    }

    private static bool TryNumber(Dictionary<string, PgConfigSetting> settings, string name, out double number) =>
        TryNumber(settings, name, out number, out _);

    private static bool TryNumber(Dictionary<string, PgConfigSetting> settings, string name, out double number, out PgConfigSetting row)
    {
        number = 0;
        if (!settings.TryGetValue(name, out row)) return false;
        var parsed = PgSettingValue.ToNumber(row.Setting);
        if (parsed is null) return false;
        number = parsed.Value;
        return true;
    }

    private static bool TryBool(Dictionary<string, PgConfigSetting> settings, string name, out bool value, out PgConfigSetting row)
    {
        value = false;
        if (!settings.TryGetValue(name, out row)) return false;
        var parsed = PgSettingValue.ToBool(row.Setting);
        if (parsed is null) return false;
        value = parsed.Value;
        return true;
    }
}

/// <summary>
/// Normalises a <c>pg_settings</c> row's <c>setting</c> text by its <c>unit</c> (#3542 step 2) — the one piece
/// of arithmetic in the PostgreSQL-target analysis nobody had written before, because every shipped read
/// returns the raw pair and lets the reader do it by eye. PostgreSQL reports each setting in ITS OWN unit:
/// <c>shared_buffers</c> and <c>effective_cache_size</c> in blocks (<c>8kB</c>), <c>work_mem</c> in
/// <c>kB</c>, <c>max_wal_size</c> in <c>MB</c>, <c>checkpoint_timeout</c> in <c>s</c>, <c>bgwriter_delay</c>
/// in <c>ms</c>, <c>autovacuum_naptime</c> in <c>s</c>; a comparison between two of them, or against a bar,
/// is meaningless until both are in one unit.
///
/// <para><b>Units.</b> Memory: <c>B</c>, <c>kB</c>, <c>MB</c>, <c>GB</c>, <c>TB</c>, and the block-multiple
/// form <c>NkB</c> (<c>8kB</c> is the shipped block size; <c>16kB</c> / <c>32kB</c> are legal builds and
/// <c>wal_segment_size</c>-class settings have reported multiples). Time: <c>us</c>, <c>ms</c>, <c>s</c>,
/// <c>min</c>, <c>h</c>, <c>d</c>. Case-insensitive on the letters, because <c>pg_settings</c> spells
/// <c>kB</c> and humans spell <c>KB</c>. An unknown unit normalises to <c>null</c>, never to the bare
/// number — "unknown" and "bytes" are different claims.</para>
///
/// <para><b>Sentinels survive.</b> <c>-1</c> means "unlimited" or "inherit" on <c>max_slot_wal_keep_size</c>,
/// <c>autovacuum_work_mem</c>, <c>log_min_duration_statement</c>, <c>temp_file_limit</c> and others; a
/// normaliser that multiplied it by 1024 would turn "no limit" into "minus one kilobyte". Any negative
/// value is returned AS THE NUMBER, in no unit, so the caller can test for the sentinel it knows.</para>
///
/// <para><b>An embedded unit is honoured when the row has none.</b> <c>pg_settings.setting</c> is a bare
/// number with the unit in its own column, but <c>SHOW</c> and <c>current_setting()</c> render
/// <c>128MB</c> as one token, and a caller normalising either shape should get the same answer. The unit
/// COLUMN wins when both are present, because it is the catalog's statement and the suffix is a rendering.</para>
/// </summary>
public static class PgSettingValue
{
    /// <summary>The block size a stock build reports as <c>8kB</c>; the unit string carries the multiple, so
    /// a 16 kB build normalises correctly without this constant changing.</summary>
    public const long DefaultBlockBytes = 8192;

    /// <summary>
    /// The setting in BYTES, or <c>null</c> when the text is not a number or the unit is not a memory unit.
    /// A negative value (the <c>-1</c> sentinel) is returned unchanged.
    /// </summary>
    public static long? ToBytes(string? setting, string? unit)
    {
        if (!TrySplit(setting, unit, out var number, out var effectiveUnit)) return null;
        if (number < 0) return (long)Math.Round(number);

        var multiplier = MemoryUnitBytes(effectiveUnit);
        if (multiplier is null) return null;

        var bytes = number * multiplier.Value;
        if (bytes > long.MaxValue) return null;
        return (long)Math.Round(bytes);
    }

    /// <summary>
    /// The setting in MILLISECONDS, or <c>null</c> when the text is not a number or the unit is not a time
    /// unit. A negative value (the <c>-1</c> sentinel) is returned unchanged. Fractional results are real:
    /// <c>autovacuum_vacuum_cost_delay</c> is a real number of milliseconds and <c>us</c> divides.
    /// </summary>
    public static double? ToMs(string? setting, string? unit)
    {
        if (!TrySplit(setting, unit, out var number, out var effectiveUnit)) return null;
        if (number < 0) return number;

        var multiplier = TimeUnitMs(effectiveUnit);
        if (multiplier is null) return null;
        return number * multiplier.Value;
    }

    /// <summary>The setting in SECONDS — <see cref="ToMs"/> over 1000, with the sentinel preserved.</summary>
    public static double? ToSeconds(string? setting, string? unit)
    {
        var ms = ToMs(setting, unit);
        if (ms is null) return null;
        return ms.Value < 0 ? ms.Value : ms.Value / 1000.0;
    }

    /// <summary>A unitless number (<c>random_page_cost</c>, <c>max_connections</c>,
    /// <c>checkpoint_completion_target</c>), invariant-culture, or <c>null</c>.</summary>
    public static double? ToNumber(string? setting)
    {
        if (string.IsNullOrWhiteSpace(setting)) return null;
        return double.TryParse(setting.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var value) && double.IsFinite(value)
            ? value
            : null;
    }

    /// <summary>A PostgreSQL boolean in any of the spellings the engine accepts (<c>on</c> / <c>off</c>,
    /// <c>true</c> / <c>false</c>, <c>yes</c> / <c>no</c>, <c>1</c> / <c>0</c>), or <c>null</c> for anything
    /// else — including the enum values a former boolean grew (<c>wal_compression = lz4</c>), which the caller
    /// decides about.</summary>
    public static bool? ToBool(string? setting)
    {
        if (string.IsNullOrWhiteSpace(setting)) return null;
        return setting.Trim().ToLowerInvariant() switch
        {
            "on" or "true" or "yes" or "1" or "t" or "y" => true,
            "off" or "false" or "no" or "0" or "f" or "n" => false,
            _ => null,
        };
    }

    /// <summary>
    /// A byte count in the unit an operator would write in <c>postgresql.conf</c>: whole gigabytes as
    /// <c>GB</c>, whole megabytes as <c>MB</c>, whole kilobytes as <c>kB</c>, else bytes; one decimal where
    /// the value is not whole in its unit (<c>1.5 GB</c>). Invariant culture — this text lands in advice
    /// prose and persisted story text.
    /// </summary>
    public static string FormatBytes(long bytes)
    {
        if (bytes < 0) return bytes.ToString(CultureInfo.InvariantCulture);
        const long kb = 1024, mb = kb * 1024, gb = mb * 1024;
        if (bytes >= gb) return Scaled(bytes, gb, "GB");
        if (bytes >= mb) return Scaled(bytes, mb, "MB");
        if (bytes >= kb) return Scaled(bytes, kb, "kB");
        return bytes.ToString(CultureInfo.InvariantCulture) + " B";

        static string Scaled(long value, long unit, string suffix)
        {
            var scaled = value / (double)unit;
            return (value % unit == 0
                    ? scaled.ToString("0", CultureInfo.InvariantCulture)
                    : scaled.ToString("0.#", CultureInfo.InvariantCulture))
                + " " + suffix;
        }
    }

    private static bool TrySplit(string? setting, string? unit, out double number, out string? effectiveUnit)
    {
        number = 0;
        effectiveUnit = null;
        if (string.IsNullOrWhiteSpace(setting)) return false;

        var text = setting.Trim();
        var split = text.Length;
        while (split > 0 && (char.IsLetter(text[split - 1]) || text[split - 1] == ' '))
            split--;

        var numberText = text[..split].Trim();
        var suffix = text[split..].Trim();

        if (!double.TryParse(numberText, NumberStyles.Float, CultureInfo.InvariantCulture, out number) || !double.IsFinite(number))
            return false;

        effectiveUnit = !string.IsNullOrWhiteSpace(unit) ? unit.Trim() : (suffix.Length > 0 ? suffix : null);
        return true;
    }

    private static double? MemoryUnitBytes(string? unit)
    {
        if (string.IsNullOrEmpty(unit)) return null;

        /* The block-multiple form: a leading integer and a memory unit — 8kB, 16kB, 32kB, 16MB. */
        var split = 0;
        while (split < unit.Length && char.IsDigit(unit[split])) split++;
        var multiple = 1.0;
        if (split > 0)
        {
            if (!double.TryParse(unit[..split], NumberStyles.None, CultureInfo.InvariantCulture, out multiple)) return null;
            unit = unit[split..].Trim();
        }

        double? baseBytes = unit.ToLowerInvariant() switch
        {
            "b" => 1.0,
            "kb" => 1024.0,
            "mb" => 1024.0 * 1024.0,
            "gb" => 1024.0 * 1024.0 * 1024.0,
            "tb" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
            _ => null,
        };
        return baseBytes is null ? null : baseBytes.Value * multiple;
    }

    private static double? TimeUnitMs(string? unit)
    {
        if (string.IsNullOrEmpty(unit)) return null;
        return unit.ToLowerInvariant() switch
        {
            "us" => 0.001,
            "ms" => 1.0,
            "s" => 1000.0,
            "min" => 60_000.0,
            "h" => 3_600_000.0,
            "d" => 86_400_000.0,
            _ => null,
        };
    }
}
