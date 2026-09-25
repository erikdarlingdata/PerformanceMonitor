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
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The verdict on one sizing-relevant setting (#4214): what the check verb and the once-per-start log line
/// both report. Four states, no fifth — see <see cref="DarlingStoreHostProfile.ClassifyVerdict"/> for how a
/// managed store's file attribution collapses onto these.
/// </summary>
public enum HostSettingVerdict
{
    /// <summary>A managed block sets it, and it equals the value derived for THIS host right now.</summary>
    Matches,

    /// <summary>A managed block sets it, but re-deriving from the CURRENT host (RAM, hypertable count, free
    /// disk) gives a different number — the #2845/#4207/#3802 class of drift this whole issue is about.</summary>
    StaleAfterHardwareChange,

    /// <summary><c>postgresql.auto.conf</c> (an <c>ALTER SYSTEM</c>), or a line placed after every managed
    /// block in <c>postgresql.conf</c>, is what is actually in force.</summary>
    OperatorOverride,

    /// <summary>A bring-your-own store: this class never wrote any block here, so there is nothing to compare
    /// the live value against beyond what <c>pg_settings.source</c> itself says.</summary>
    NotManaged,
}

/// <summary>Where one setting's winning assignment was found, before it is collapsed into a verdict.</summary>
internal enum ConfSettingOrigin
{
    /// <summary>No assignment anywhere PostgreSQL would read one — the compiled-in default is in force. Not
    /// expected on a managed store (every one of the eight checked settings is written by v2/v3/v4/v8/v12 on
    /// the very first bootstrap); kept as its own case so a hand-stripped conf reports something definite
    /// rather than silently reading as a match.</summary>
    Unset,

    /// <summary>The winning line sits inside one of <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/>'s
    /// blocks, in <c>postgresql.conf</c> itself.</summary>
    ManagedBlock,

    /// <summary><c>postgresql.auto.conf</c> set it, or a <c>postgresql.conf</c> line outside every managed
    /// block (an include, or a line spliced in after the last block) is what won.</summary>
    OperatorOverride,
}

/// <summary>One setting's file-based attribution (#4214), read from disk rather than <c>pg_settings.sourcefile</c>
/// — see <see cref="Mcp.DarlingStoreMetricsReader.JobExecutionLoggingSql"/>'s remarks for why: that column needs
/// superuser/<c>pg_read_all_settings</c>, and source attribution must never escalate a role's privileges to get
/// it. The managed data directory is read directly instead, which needs no PostgreSQL privilege at all.</summary>
internal readonly record struct ConfSettingAttribution(ConfSettingOrigin Origin, string? File, int Line, string? RawValue);

/// <summary>Total RAM, the cgroup ceiling (if any), and which of the two governs — the model
/// <see cref="DarlingStoreHostProfile.GatherHostFacts"/> fills from the platform-appropriate read.</summary>
internal readonly record struct HostMemoryProfile(
    long TotalBytes,
    long? CgroupLimitBytes,
    long EffectiveBytes,
    bool IsAuthoritative,
    string SourceDescription);

/// <summary>The volume holding the data directory (managed) or this process's own install location (BYO;
/// see <see cref="DarlingStoreHostProfile.ResolveVolumeAnchor"/> for why that is the best available answer).</summary>
internal readonly record struct HostDataVolumeProfile(
    long TotalBytes,
    long FreeBytes,
    string? Filesystem,
    bool IsReady);

/// <summary>PostgreSQL/TimescaleDB versions and the store-level numbers the issue's Problem section named:
/// size, lifetime buffer hit, lifetime temp bytes, and the #4211 uncompressed-chunks-vs-RAM figure.</summary>
internal readonly record struct HostStoreFacts(
    string PostgresVersion,
    string? TimescaleVersion,
    long? StoreSizeBytes,
    double? BufferHitRatioPercent,
    long? TempBytes,
    long UncompressedChunkBytes,
    int UncompressedChunkCount);

/// <summary>One row of the <c>--check-settings</c> table: a setting's live value, where it came from, what
/// this host would derive for it right now, and the verdict those two facts collapse to.</summary>
internal readonly record struct HostSettingProfile(
    string Name,
    string CurrentValueDisplay,
    long CurrentValueNormalized,
    string SourceDescription,
    string DerivedValueDisplay,
    long DerivedValueNormalized,
    HostSettingVerdict Verdict);

/// <summary>The whole host/store/settings snapshot one <c>--check-settings</c> run or one service-start log
/// line reports (#4214).</summary>
internal sealed class HostProfile
{
    public required string Platform { get; init; }
    public required bool IsContainerized { get; init; }
    public required int ProcessorCount { get; init; }
    public required HostMemoryProfile Memory { get; init; }
    public required HostDataVolumeProfile DataVolume { get; init; }
    public required bool IsManagedStore { get; init; }
    public required HostStoreFacts Store { get; init; }
    public required IReadOnlyList<HostSettingProfile> Settings { get; init; }
}

/// <summary>
/// Gathers and reports the store host profile (#4214, part 1): the cross-platform host facts, the store facts,
/// and a verdict on the eight sizing-relevant settings — everything a remote shell used to be needed for (see
/// the issue's Problem section). Read-only throughout: nothing here ever writes a setting or a conf file; the
/// writer side is <see cref="DarlingManagedPostgres"/>, whose <c>Derive*</c> functions this class calls rather
/// than re-implementing (ruling 1 — a formula lives in exactly one place).
/// </summary>
internal static class DarlingStoreHostProfile
{
    /// <summary>The .NET/container-tooling convention env var naming a containerized process, checked
    /// alongside a finite cgroup memory limit (#4214's ruling 2).</summary>
    public const string DotnetRunningInContainerEnvVar = "DOTNET_RUNNING_IN_CONTAINER";

    /* ============================= Linux RAM / cgroup: pure parse functions ============================= *
     * Pure over already-read file TEXT (not a path), so every branch — meminfo present/absent/unparseable,
     * cgroup v2 "max", v2 a real number, v1's near-long.MaxValue "no limit" sentinel, v1 a real number — has
     * a Windows-runnable test feeding it a literal string, with no Linux filesystem involved (ruling 2/10). */

    /// <summary>Parses <c>/proc/meminfo</c>'s <c>MemTotal:</c> line (kB) to bytes. Null when the text has no
    /// parseable <c>MemTotal</c> line — the caller treats that exactly like a failed
    /// <c>GlobalMemoryStatusEx</c> on Windows: fall back, never size from a zero/garbage reading.</summary>
    internal static long? ParseProcMeminfoTotalBytes(string? meminfoText)
    {
        if (string.IsNullOrEmpty(meminfoText))
        {
            return null;
        }

        foreach (var rawLine in meminfoText.Split('\n'))
        {
            var line = rawLine.TrimEnd('\r').TrimStart();
            if (!line.StartsWith("MemTotal:", StringComparison.Ordinal))
            {
                continue;
            }

            var rest = line["MemTotal:".Length..].Trim();
            var spaceIndex = rest.IndexOf(' ');
            var numberText = spaceIndex >= 0 ? rest[..spaceIndex] : rest;
            return long.TryParse(numberText, NumberStyles.None, CultureInfo.InvariantCulture, out var kb) && kb > 0
                ? kb * 1024L
                : null;
        }

        return null;
    }

    /// <summary>cgroup v2 <c>memory.max</c>: either a byte count, or the literal <c>max</c> meaning no limit
    /// (null). An empty/unparseable reading is also null — "could not tell" and "no limit" are the same
    /// answer to the one question this feeds: is there a ceiling under total RAM to report.</summary>
    internal static long? ParseCgroupV2MemoryMaxBytes(string? memoryMaxText)
    {
        if (string.IsNullOrEmpty(memoryMaxText))
        {
            return null;
        }

        var trimmed = memoryMaxText.Trim();
        if (string.Equals(trimmed, "max", StringComparison.Ordinal))
        {
            return null;
        }

        return long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var bytes) && bytes > 0
            ? bytes
            : null;
    }

    /// <summary>The v1 kernel's "no limit" is not a keyword — <c>memory.limit_in_bytes</c> reads back near
    /// <see cref="long.MaxValue"/> (rounded down to the kernel's page size; historically
    /// 9223372036854771712 on a 4096-byte page), so anything within a GB of it is unlimited, not a real
    /// host's RAM (ruling 2: "so does v1's near-long.MaxValue sentinel").</summary>
    internal const long CgroupV1UnlimitedThreshold = long.MaxValue - 1_073_741_824L;

    /// <summary>cgroup v1 <c>memory.limit_in_bytes</c>: a byte count, or null for unset/unparseable/the
    /// near-<see cref="long.MaxValue"/> "unlimited" sentinel.</summary>
    internal static long? ParseCgroupV1MemoryLimitBytes(string? memoryLimitText)
    {
        if (string.IsNullOrEmpty(memoryLimitText))
        {
            return null;
        }

        if (!long.TryParse(memoryLimitText.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var bytes) || bytes <= 0)
        {
            return null;
        }

        return bytes >= CgroupV1UnlimitedThreshold ? null : bytes;
    }

    /// <summary>The smaller of total RAM and the cgroup limit (null cgroup = no limit) — what the process can
    /// actually use, and the figure the settings' "derived value for this host" is computed from.</summary>
    internal static long ComputeEffectiveMemoryLimitBytes(long memTotalBytes, long? cgroupLimitBytes)
        => cgroupLimitBytes is { } limit && limit < memTotalBytes ? limit : memTotalBytes;

    /// <summary>Containerized when a FINITE cgroup memory limit is in force, or the runtime says so directly.
    /// A bare-metal Linux host has cgroups too but reports "no limit" on them, which is why presence alone is
    /// not the signal (ruling 2) — only a real ceiling, or the explicit env var, counts.</summary>
    internal static bool IsContainerized(long? cgroupMemoryLimitBytes, string? dotnetRunningInContainerEnv)
        => cgroupMemoryLimitBytes.HasValue
           || string.Equals(dotnetRunningInContainerEnv, "true", StringComparison.OrdinalIgnoreCase);

    /* ==================================== Host facts (impure gather) ===================================== */

    /// <summary>Reads the whole file if present, or null on any I/O failure — the one shared "best effort
    /// file read" every Linux path below uses, so a missing/unreadable cgroup file degrades the same way a
    /// missing meminfo line does: fall back, never throw.</summary>
    private static string? TryReadFile(string path)
    {
        try
        {
            return File.Exists(path) ? File.ReadAllText(path) : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <summary>OS platform, containerization, CPU count and RAM for the CURRENT process's host. Windows
    /// reuses <see cref="DarlingManagedPostgres.TryReadWindowsPhysicalMemoryBytes"/> — the SAME
    /// <c>GlobalMemoryStatusEx</c> call the managed store sizes itself from (ruling 2's "reuse the
    /// authoritative read"), not a second P/Invoke. Linux reads <c>/proc/meminfo</c> plus cgroup v2 (falling
    /// back to v1) through the pure functions above. <see cref="Environment.ProcessorCount"/> is already
    /// cgroup-aware on .NET, so no extra work is needed there.</summary>
    internal static (string Platform, bool IsContainerized, int ProcessorCount, HostMemoryProfile Memory) GatherHostFacts()
    {
        var processorCount = Environment.ProcessorCount;

        if (OperatingSystem.IsWindows())
        {
            var authoritative = DarlingManagedPostgres.TryReadWindowsPhysicalMemoryBytes(out var totalBytes, out _, out _);
            var total = authoritative ? totalBytes : GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
            var memory = new HostMemoryProfile(
                total, null, total, authoritative,
                authoritative ? "GlobalMemoryStatusEx" : "GC.GetGCMemoryInfo() fallback (GlobalMemoryStatusEx did not return a reading)");
            return ("Windows", false, processorCount, memory);
        }

        if (OperatingSystem.IsLinux())
        {
            var memTotal = ParseProcMeminfoTotalBytes(TryReadFile("/proc/meminfo"));
            var cgroupLimit = ParseCgroupV2MemoryMaxBytes(TryReadFile("/sys/fs/cgroup/memory.max"))
                ?? ParseCgroupV1MemoryLimitBytes(TryReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
            var authoritative = memTotal.HasValue;
            var total = memTotal ?? GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
            var effective = ComputeEffectiveMemoryLimitBytes(total, cgroupLimit);
            var containerized = IsContainerized(cgroupLimit, Environment.GetEnvironmentVariable(DotnetRunningInContainerEnvVar));
            var memory = new HostMemoryProfile(
                total, cgroupLimit, effective, authoritative,
                authoritative ? "/proc/meminfo MemTotal" : "GC.GetGCMemoryInfo() fallback (/proc/meminfo unreadable)");
            return ("Linux", containerized, processorCount, memory);
        }

        var gcTotal = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        return (
            RuntimeInformation.OSDescription,
            string.Equals(Environment.GetEnvironmentVariable(DotnetRunningInContainerEnvVar), "true", StringComparison.OrdinalIgnoreCase),
            processorCount,
            new HostMemoryProfile(gcTotal, null, gcTotal, false, "GC.GetGCMemoryInfo() fallback (unsupported platform for an authoritative read)"));
    }

    /// <summary>The volume holding <paramref name="anchorPath"/> — same <c>DriveInfo</c>-on-the-path-root
    /// idiom <c>DarlingManagedPostgres.TryReadDataVolumeSpace</c> and three other readers of a Darling volume
    /// already use, plus <c>DriveFormat</c> for the filesystem ruling 2 also asks for.</summary>
    internal static HostDataVolumeProfile GatherDataVolume(string anchorPath)
    {
        try
        {
            var root = Path.GetPathRoot(Path.GetFullPath(anchorPath));
            if (!string.IsNullOrEmpty(root))
            {
                var drive = new DriveInfo(root);
                if (drive.IsReady)
                {
                    return new HostDataVolumeProfile(drive.TotalSize, drive.AvailableFreeSpace, drive.DriveFormat, true);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            /* Fall through to the not-ready result below — a profile read never throws for a disk it cannot see. */
        }

        return new HostDataVolumeProfile(0, 0, null, false);
    }

    /// <summary>
    /// The BYO fallback anchor for the "data volume" fact (#4214): a bring-your-own store has no data
    /// directory this process necessarily knows about — <c>postgres.connectionString</c> may point at a
    /// different host entirely. The smallest reasonable call, made explicitly rather than left unhandled:
    /// report the volume under THIS process's own base directory, clearly labelled by the caller as the
    /// service's disk rather than the store's. A co-located BYO deployment (Docker Compose, same VM — the
    /// common case) gets a real, useful answer; a genuinely remote store gets a labelled figure instead of a
    /// missing one. Part 2 (get_store_host / the web panel) can revisit this once it has a place to say "this
    /// may not be the store's own disk" beyond a CLI comment.
    /// </summary>
    internal static string ResolveVolumeAnchor(PostgresConfig postgres)
        => postgres.Managed ? DarlingManagedPostgres.ResolveDataDirectory(postgres) : AppContext.BaseDirectory;

    /* ======================================== Store facts (SQL) ========================================== */

    public const string PostgresVersionSql = "SHOW server_version";

    public const string TimescaleVersionSql = "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'";

    /// <summary>Store size read LIVE, unlike <c>StoreSelfMetrics.LatestStoreSizeSql</c>'s hourly-sweep figure:
    /// that avoidance is sized against a 5-MINUTE collection-loop cadence (300+ executions/day), and this is a
    /// one-shot, user-invoked <c>--check-settings</c> read, so paying <c>pg_database_size</c>'s real cost here
    /// is the honest trade for a current number instead of a stale one. Ruling 9 (#4214) keeps this OUT of the
    /// once-per-start profile log — <see cref="GatherStartupProfileAsync"/> never calls
    /// <see cref="GatherStoreFactsAsync"/> — so <c>--check-settings</c> and the later MCP read are this SQL's
    /// only callers, both off the serial loop (see <c>SerialLoopStoreSizeSourceTests</c>, #3199).</summary>
    public const string StoreSizeSql = "SELECT pg_database_size(current_database())";

    /// <summary>Lifetime buffer hit ratio and temp bytes for the current database — a single in-memory
    /// <c>pg_stat_database</c> row, effectively free to read live every time.</summary>
    public const string BufferAndTempSql = @"
SELECT
    blks_hit,
    blks_read,
    temp_bytes
FROM pg_stat_database
WHERE datname = current_database()";

    /// <summary>
    /// The #4211 metric: how much of the store's raw ingest sits UNCOMPRESSED right now, summed over every
    /// chunk <c>timescaledb_information.chunks</c> reports as not yet compressed, across every hypertable —
    /// against RAM is the caller's job (<see cref="HostMemoryProfile.EffectiveBytes"/>), this is just the
    /// byte total and chunk count. <c>pg_total_relation_size</c> on the qualified chunk name matches how #4211
    /// itself measured it by hand. See the PR for an EXPLAIN (ANALYZE, BUFFERS) run on the rig.
    /// </summary>
    public const string UncompressedChunkSizeSql = @"
SELECT
    coalesce(sum(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name)::regclass)), 0)::bigint,
    count(*)::integer
FROM timescaledb_information.chunks
WHERE NOT is_compressed";

    internal static async Task<HostStoreFacts> GatherStoreFactsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        string version;
        await using (var cmd = new NpgsqlCommand(PostgresVersionSql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds })
        {
            version = await cmd.ExecuteScalarAsync(cancellationToken) as string ?? "unknown";
        }

        string? timescaleVersion = null;
        try
        {
            await using var cmd = new NpgsqlCommand(TimescaleVersionSql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds };
            timescaleVersion = await cmd.ExecuteScalarAsync(cancellationToken) as string;
        }
        catch (PostgresException)
        {
            /* pg_extension always exists; a failure here means the connection role cannot read it, which is
               not expected of any role this service uses. Left null rather than surfaced — the settings table
               below reports the absence of TimescaleDB just as plainly via the uncompressed-chunks read. */
        }

        long? storeSizeBytes = null;
        try
        {
            await using var cmd = new NpgsqlCommand(StoreSizeSql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds };
            storeSizeBytes = await cmd.ExecuteScalarAsync(cancellationToken) as long?;
        }
        catch (PostgresException)
        {
            /* Least-privilege role without the right on pg_database_size's target -- reported as unavailable
               rather than failing the whole profile. */
        }

        double? hitRatio = null;
        long? tempBytes = null;
        await using (var cmd = new NpgsqlCommand(BufferAndTempSql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds })
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                var hit = reader.GetInt64(0);
                var read = reader.GetInt64(1);
                hitRatio = hit + read > 0 ? 100.0 * hit / (hit + read) : null;
                tempBytes = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            }
        }

        long uncompressedBytes = 0;
        var uncompressedCount = 0;
        try
        {
            await using var cmd = new NpgsqlCommand(UncompressedChunkSizeSql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds };
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                uncompressedBytes = reader.GetInt64(0);
                uncompressedCount = reader.GetInt32(1);
            }
        }
        catch (PostgresException)
        {
            /* timescaledb_information.chunks does not exist on this connection's database -- no TimescaleDB
               extension here (plain PostgreSQL BYO, or preloaded-but-not-CREATE-EXTENSION'd, same shape the
               JobExecutionLoggingSql remarks describe for a different GUC). Zero/zero is the honest answer. */
        }

        return new HostStoreFacts(version, timescaleVersion, storeSizeBytes, hitRatio, tempBytes, uncompressedBytes, uncompressedCount);
    }

    /* ===================================== Settings: attribution + verdict =============================== */

    /// <summary>
    /// Whether 1-based <paramref name="line"/> sits inside some managed block (#4214): walking BACKWARD from
    /// it, the nearest managed marker line must be closer than the nearest blank line. A blank line between
    /// the marker and <paramref name="line"/> means the line is content spliced in AFTER that block's own
    /// blank-line terminator — exactly ruling 6's "a line after the managed blocks sets it". Generic over
    /// EVERY marker in <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/> rather than one per setting,
    /// so it needs no update when a future version block changes which settings it carries.
    /// </summary>
    internal static bool IsLineInsideManagedBlock(string conf, int line)
    {
        var lines = conf.Split('\n');
        if (line < 1 || line > lines.Length)
        {
            return false;
        }

        var markerLine = -1;
        for (var i = line - 1; i >= 0; i--)
        {
            if (Array.IndexOf(DarlingManagedPostgres.AllManagedConfMarkers, lines[i].TrimEnd('\r')) >= 0)
            {
                markerLine = i + 1;
                break;
            }
        }

        if (markerLine < 0)
        {
            return false;
        }

        for (var i = markerLine; i < line - 1; i++)
        {
            if (lines[i].TrimEnd('\r').Length == 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// One setting's file-based attribution on a MANAGED store (#4214): <c>postgresql.auto.conf</c> wins if
    /// it has an assignment (PostgreSQL reads it after all of postgresql.conf); otherwise the LAST assignment
    /// <c>postgresql.conf</c>'s own include chain carries (<see cref="DarlingManagedPostgres.ReadConfAssignments"/>
    /// already returns them in PostgreSQL's own read order), classified managed/override by
    /// <see cref="IsLineInsideManagedBlock"/>. Never touches <c>pg_settings.sourcefile</c> — see
    /// <see cref="Mcp.DarlingStoreMetricsReader.JobExecutionLoggingSql"/>'s remarks for why that column cannot
    /// be relied on without escalating the connection's privileges.
    /// </summary>
    internal static ConfSettingAttribution AttributeManagedSetting(string dataDirectory, string settingName)
    {
        var confPath = Path.GetFullPath(Path.Combine(dataDirectory, "postgresql.conf"));
        var autoConfPath = Path.GetFullPath(Path.Combine(dataDirectory, "postgresql.auto.conf"));

        var autoAssignments = DarlingManagedPostgres.ReadConfAssignments(autoConfPath, settingName);
        if (autoAssignments.Count > 0)
        {
            var last = autoAssignments[^1];
            return new ConfSettingAttribution(ConfSettingOrigin.OperatorOverride, last.File, last.Line, last.Value);
        }

        var confAssignments = DarlingManagedPostgres.ReadConfAssignments(confPath, settingName);
        if (confAssignments.Count == 0)
        {
            return new ConfSettingAttribution(ConfSettingOrigin.Unset, null, 0, null);
        }

        var lastConf = confAssignments[^1];
        if (!string.Equals(Path.GetFullPath(lastConf.File), confPath, StringComparison.OrdinalIgnoreCase))
        {
            /* The winning line lives in an INCLUDED file, not postgresql.conf itself. Every managed block is
               appended directly to postgresql.conf, so an include can never carry one. */
            return new ConfSettingAttribution(ConfSettingOrigin.OperatorOverride, lastConf.File, lastConf.Line, lastConf.Value);
        }

        string confText;
        try
        {
            confText = File.ReadAllText(confPath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return new ConfSettingAttribution(ConfSettingOrigin.OperatorOverride, lastConf.File, lastConf.Line, lastConf.Value);
        }

        var isManaged = IsLineInsideManagedBlock(confText, lastConf.Line);
        return new ConfSettingAttribution(
            isManaged ? ConfSettingOrigin.ManagedBlock : ConfSettingOrigin.OperatorOverride,
            lastConf.File, lastConf.Line, lastConf.Value);
    }

    /// <summary>Collapses one setting's file attribution plus its current-vs-derived comparison into the
    /// verdict + source text the table reports (#4214's ruling 6).</summary>
    internal static (string SourceDescription, HostSettingVerdict Verdict) ClassifyVerdict(
        ConfSettingAttribution attribution, long currentValueNormalized, long derivedValueNormalized)
    {
        switch (attribution.Origin)
        {
            case ConfSettingOrigin.ManagedBlock:
                var matches = currentValueNormalized == derivedValueNormalized;
                return (
                    FormattableString.Invariant($"managed block ({attribution.File}:{attribution.Line})"),
                    matches ? HostSettingVerdict.Matches : HostSettingVerdict.StaleAfterHardwareChange);

            case ConfSettingOrigin.OperatorOverride:
                var where = attribution.File is null
                    ? "operator override"
                    : FormattableString.Invariant($"operator override ({attribution.File}:{attribution.Line})");
                return (where, HostSettingVerdict.OperatorOverride);

            default:
                return ("no managed block or override found; PostgreSQL default in force", HostSettingVerdict.OperatorOverride);
        }
    }

    /// <summary>Converts a <c>pg_settings</c> (setting, unit) pair to the normalized unit each row of the
    /// table compares in: whole MB for a memory/WAL setting, the bare count for a connection/worker setting.
    /// Null for an unrecognized unit — the caller reports the row as unreadable rather than guessing.</summary>
    internal static long? NormalizePgSetting(string setting, string? unit)
    {
        if (!long.TryParse(setting, NumberStyles.Integer, CultureInfo.InvariantCulture, out var raw))
        {
            return null;
        }

        return unit switch
        {
            null or "" => raw,
            "8kB" => raw * 8 / 1024,
            "kB" => raw / 1024,
            "MB" => raw,
            "GB" => raw * 1024,
            _ => null,
        };
    }

    internal static string FormatSettingValue(long value, string unit) =>
        unit.Length == 0 ? value.ToString(CultureInfo.InvariantCulture) : FormattableString.Invariant($"{value}{unit}");

    /// <summary>The eight settings ruling 4 names, each with its unit and the <c>Derive*</c>/constant that
    /// supplies "the value derived for this host" — never a second copy of a formula (ruling 1).</summary>
    private static (string Name, long DerivedValueMb, string Unit)[] BuildDerivedTargets(
        long ramBytesForDerivation, long freeDiskBytesForDerivation)
    {
        var memory = DarlingManagedPostgres.DeriveMemorySettings(DarlingManagedPostgres.QuantizeRam(ramBytesForDerivation));
        var workers = DarlingManagedPostgres.DeriveWorkerSettings(TimescaleSupport.HypertableCount);
        var wal = DarlingManagedPostgres.DeriveWalSettings(freeDiskBytesForDerivation);

        return
        [
            ("shared_buffers", memory.SharedBuffersMb, "MB"),
            ("effective_cache_size", memory.EffectiveCacheSizeMb, "MB"),
            ("maintenance_work_mem", memory.MaintenanceWorkMemMb, "MB"),
            ("work_mem", memory.WorkMemMb, "MB"),
            ("timescaledb.max_background_workers", workers.MaxBackgroundWorkers, ""),
            ("max_worker_processes", workers.MaxWorkerProcesses, ""),
            ("max_connections", DarlingManagedPostgres.TargetMaxConnections, ""),
            ("max_wal_size", wal.MaxWalSizeMb, "MB"),
        ];
    }

    internal static async Task<IReadOnlyList<HostSettingProfile>> GatherSettingProfilesAsync(
        NpgsqlConnection connection,
        PostgresConfig postgres,
        long ramBytesForDerivation,
        long freeDiskBytesForDerivation,
        CancellationToken cancellationToken)
    {
        var targets = BuildDerivedTargets(ramBytesForDerivation, freeDiskBytesForDerivation);
        var names = targets.Select(t => t.Name).ToArray();

        var live = new Dictionary<string, (string Setting, string? Unit, string? Source)>(StringComparer.Ordinal);
        await using (var cmd = new NpgsqlCommand("SELECT name, setting, unit, source FROM pg_settings WHERE name = ANY($1)", connection)
            { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds })
        {
            cmd.Parameters.AddWithValue(names);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                live[reader.GetString(0)] = (
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3));
            }
        }

        var dataDirectory = postgres.Managed ? DarlingManagedPostgres.ResolveDataDirectory(postgres) : null;
        var results = new List<HostSettingProfile>(targets.Length);

        foreach (var (name, derivedValue, unit) in targets)
        {
            var derivedDisplay = FormatSettingValue(derivedValue, unit);

            if (!live.TryGetValue(name, out var pgValue))
            {
                results.Add(new HostSettingProfile(
                    name, "(not visible on this connection)", 0,
                    "unreadable — this connection's role, or this build of TimescaleDB, does not expose it",
                    derivedDisplay, derivedValue, HostSettingVerdict.NotManaged));
                continue;
            }

            var currentValue = NormalizePgSetting(pgValue.Setting, pgValue.Unit);
            var currentDisplay = currentValue.HasValue ? FormatSettingValue(currentValue.Value, unit) : pgValue.Setting;

            if (dataDirectory is null)
            {
                results.Add(new HostSettingProfile(
                    name, currentDisplay, currentValue ?? 0,
                    FormattableString.Invariant($"not-managed (bring-your-own store; pg_settings.source = {pgValue.Source ?? "unknown"})"),
                    derivedDisplay, derivedValue, HostSettingVerdict.NotManaged));
                continue;
            }

            var attribution = AttributeManagedSetting(dataDirectory, name);
            var (sourceDescription, verdict) = ClassifyVerdict(attribution, currentValue ?? long.MinValue, derivedValue);
            results.Add(new HostSettingProfile(name, currentDisplay, currentValue ?? 0, sourceDescription, derivedDisplay, derivedValue, verdict));
        }

        return results;
    }

    /* ========================================== Orchestration ============================================ */

    internal static async Task<HostProfile> GatherAsync(
        PostgresConfig postgres, NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var (platform, containerized, cpuCount, memory) = GatherHostFacts();
        var dataVolume = GatherDataVolume(ResolveVolumeAnchor(postgres));
        var store = await GatherStoreFactsAsync(connection, cancellationToken);
        var settings = await GatherSettingProfilesAsync(connection, postgres, memory.EffectiveBytes, dataVolume.FreeBytes, cancellationToken);

        return new HostProfile
        {
            Platform = platform,
            IsContainerized = containerized,
            ProcessorCount = cpuCount,
            Memory = memory,
            DataVolume = dataVolume,
            IsManagedStore = postgres.Managed,
            Store = store,
            Settings = settings,
        };
    }

    /// <summary>The placeholder <see cref="HostStoreFacts"/> a <see cref="GatherStartupProfileAsync"/> profile
    /// carries — never gathered, never read. <c>string.Empty</c> rather than <c>default</c> so
    /// <see cref="HostStoreFacts.PostgresVersion"/> stays non-null even if some future caller misuses the
    /// profile: a wrong-but-harmless empty string beats a nullability contract broken at runtime.</summary>
    private static readonly HostStoreFacts s_storeFactsNotGatheredAtStartup = new(string.Empty, null, null, null, null, 0, 0);

    /// <summary>
    /// Ruling 9 (#4214): what the once-per-start log gathers — host facts, the live <c>pg_settings</c> read
    /// and the managed conf-file attribution behind each setting's verdict. Deliberately never calls
    /// <see cref="GatherStoreFactsAsync"/>: <c>StoreSizeSql</c> and <c>UncompressedChunkSizeSql</c> are reads
    /// whose cost scales with the store (measured 3,177 ms on a 225 GiB store — see
    /// <c>SerialLoopStoreSizeSourceTests</c>, #3199), and this runs once per process start rather than once
    /// per CLI invocation. Those stay in <see cref="GatherAsync"/> (<c>--check-settings</c>) and the later
    /// MCP read. The returned profile's <see cref="HostProfile.Store"/> is the not-gathered placeholder;
    /// <see cref="FormatStartupProfileText"/> never reads it.
    /// </summary>
    internal static async Task<HostProfile> GatherStartupProfileAsync(
        PostgresConfig postgres, NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var (platform, containerized, cpuCount, memory) = GatherHostFacts();
        var dataVolume = GatherDataVolume(ResolveVolumeAnchor(postgres));
        var settings = await GatherSettingProfilesAsync(connection, postgres, memory.EffectiveBytes, dataVolume.FreeBytes, cancellationToken);

        return new HostProfile
        {
            Platform = platform,
            IsContainerized = containerized,
            ProcessorCount = cpuCount,
            Memory = memory,
            DataVolume = dataVolume,
            IsManagedStore = postgres.Managed,
            Store = s_storeFactsNotGatheredAtStartup,
            Settings = settings,
        };
    }

    /* ============================================ Formatting ============================================= */

    internal static string DescribeVerdict(HostSettingVerdict verdict) => verdict switch
    {
        HostSettingVerdict.Matches => "matches",
        HostSettingVerdict.StaleAfterHardwareChange => "stale-after-hardware-change",
        HostSettingVerdict.OperatorOverride => "operator-override",
        HostSettingVerdict.NotManaged => "not-managed",
        _ => "unknown",
    };

    internal static string FormatBytes(long bytes)
    {
        double value = bytes;
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        var unitIndex = 0;
        while (value >= 1024 && unitIndex < units.Length - 1)
        {
            value /= 1024;
            unitIndex++;
        }

        return FormattableString.Invariant($"{value:0.##} {units[unitIndex]}");
    }

    private static string Truncate(string text, int max) => text.Length <= max ? text : text[..(max - 1)] + "…";

    /// <summary>The host lines both <see cref="FormatProfileText"/> (<c>--check-settings</c>) and
    /// <see cref="FormatStartupProfileText"/> (the once-per-start log) print identically — platform, RAM,
    /// data volume. Extracted so the two never drift (ruling 1): the only difference between the two
    /// formatters is whether a Store section and a blank-line separator follow.</summary>
    private static void AppendHostLines(StringBuilder sb, HostProfile profile)
    {
        sb.Append("Host: ").Append(profile.Platform).Append(profile.IsContainerized ? " (containerized)" : "")
          .Append(", ").Append(profile.ProcessorCount).Append(" CPU(s)").Append('\n');
        sb.Append("RAM: ").Append(FormatBytes(profile.Memory.EffectiveBytes))
          .Append(profile.Memory.IsAuthoritative ? "" : " (unauthoritative reading)")
          .Append(" [").Append(profile.Memory.SourceDescription).Append(']').Append('\n');
        sb.Append("Data volume: ").Append(FormatBytes(profile.DataVolume.TotalBytes)).Append(" total, ")
          .Append(FormatBytes(profile.DataVolume.FreeBytes)).Append(" free")
          .Append(profile.DataVolume.Filesystem is { } fs ? $" ({fs})" : "").Append('\n');
    }

    /// <summary>The per-setting table both formatters print identically — shared for the same reason as
    /// <see cref="AppendHostLines"/>.</summary>
    private static void AppendSettingsTable(StringBuilder sb, IReadOnlyList<HostSettingProfile> settings)
    {
        sb.Append(FormattableString.Invariant($"{"Setting",-38}{"Current",-14}{"Derived",-14}{"Source",-42}Verdict")).Append('\n');
        foreach (var s in settings)
        {
            sb.Append(FormattableString.Invariant(
                $"{s.Name,-38}{s.CurrentValueDisplay,-14}{s.DerivedValueDisplay,-14}{Truncate(s.SourceDescription, 40),-42}{DescribeVerdict(s.Verdict)}")).Append('\n');
        }
    }

    internal static string FormatProfileText(HostProfile profile)
    {
        var sb = new StringBuilder();
        AppendHostLines(sb, profile);
        sb.Append("Store: PostgreSQL ").Append(profile.Store.PostgresVersion)
          .Append(profile.Store.TimescaleVersion is { } tv ? $", TimescaleDB {tv}" : ", TimescaleDB not installed").Append('\n');
        if (profile.Store.StoreSizeBytes is { } size)
        {
            sb.Append("  size ").Append(FormatBytes(size)).Append('\n');
        }

        if (profile.Store.BufferHitRatioPercent is { } hit)
        {
            sb.Append("  lifetime buffer hit ").Append(hit.ToString("0.0", CultureInfo.InvariantCulture)).Append('%').Append('\n');
        }

        if (profile.Store.TempBytes is { } temp)
        {
            sb.Append("  lifetime temp bytes ").Append(FormatBytes(temp)).Append('\n');
        }

        var ramPercent = profile.Memory.EffectiveBytes > 0
            ? 100.0 * profile.Store.UncompressedChunkBytes / profile.Memory.EffectiveBytes
            : 0.0;
        sb.Append("  uncompressed chunks: ").Append(FormatBytes(profile.Store.UncompressedChunkBytes)).Append(" in ")
          .Append(profile.Store.UncompressedChunkCount).Append(" chunk(s), ")
          .Append(ramPercent.ToString("0.0", CultureInfo.InvariantCulture)).Append("% of RAM").Append('\n');
        sb.Append('\n');

        AppendSettingsTable(sb, profile.Settings);

        return sb.ToString();
    }

    /// <summary>The once-per-start log line's text (ruling 9, #4214): host facts and the per-setting verdict
    /// table, with no Store section — a <see cref="GatherStartupProfileAsync"/> profile's
    /// <see cref="HostProfile.Store"/> is a placeholder never gathered from the store, so this formatter
    /// never reads it. Kept as its own function rather than a Store-optional branch inside
    /// <see cref="FormatProfileText"/>: a caller cannot accidentally print a placeholder Store section by
    /// passing a startup profile through the wrong formatter.</summary>
    internal static string FormatStartupProfileText(HostProfile profile)
    {
        var sb = new StringBuilder();
        AppendHostLines(sb, profile);
        sb.Append('\n');
        AppendSettingsTable(sb, profile.Settings);

        return sb.ToString();
    }

    internal static string FormatProfileJson(HostProfile profile)
    {
        var payload = new
        {
            platform = profile.Platform,
            containerized = profile.IsContainerized,
            processorCount = profile.ProcessorCount,
            ramTotalBytes = profile.Memory.TotalBytes,
            ramCgroupLimitBytes = profile.Memory.CgroupLimitBytes,
            ramEffectiveBytes = profile.Memory.EffectiveBytes,
            ramAuthoritative = profile.Memory.IsAuthoritative,
            ramSource = profile.Memory.SourceDescription,
            dataVolumeTotalBytes = profile.DataVolume.TotalBytes,
            dataVolumeFreeBytes = profile.DataVolume.FreeBytes,
            dataVolumeFilesystem = profile.DataVolume.Filesystem,
            managed = profile.IsManagedStore,
            postgresVersion = profile.Store.PostgresVersion,
            timescaleVersion = profile.Store.TimescaleVersion,
            storeSizeBytes = profile.Store.StoreSizeBytes,
            bufferHitRatioPercent = profile.Store.BufferHitRatioPercent,
            tempBytes = profile.Store.TempBytes,
            uncompressedChunkBytes = profile.Store.UncompressedChunkBytes,
            uncompressedChunkCount = profile.Store.UncompressedChunkCount,
            settings = profile.Settings.Select(s => new
            {
                name = s.Name,
                current = s.CurrentValueDisplay,
                derived = s.DerivedValueDisplay,
                source = s.SourceDescription,
                verdict = DescribeVerdict(s.Verdict),
            }),
        };

        return JsonSerializer.Serialize(payload, ProfileJsonOptions);
    }

    private static readonly JsonSerializerOptions ProfileJsonOptions = new() { WriteIndented = true };
}
