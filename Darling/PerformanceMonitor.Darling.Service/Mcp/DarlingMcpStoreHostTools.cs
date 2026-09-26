/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_store_host</c> (#4214 part 2): the store host profile over MCP/web — the same model
/// <c>--check-settings</c> prints (part 1, #4271), reached without a shell on the store's own host. Reuses
/// <see cref="DarlingStoreHostProfile.GatherAsync"/> rather than a second copy (ruling 1): host facts (OS,
/// CPUs, RAM with its cgroup/authoritative reading, the data volume), the store facts (PostgreSQL/TimescaleDB
/// versions, size, lifetime buffer hit, lifetime temp bytes, the #4211 uncompressed-chunks-vs-RAM figure), and
/// a verdict per sizing-relevant setting: <c>matches</c>, <c>stale-after-hardware-change</c>,
/// <c>operator-override</c> or <c>not-managed</c>.
///
/// <para>Deliberately NOT a block on <c>get_store_metrics</c> (ruling 1): that tool is sized to
/// <see cref="McpResponseBudget.DefaultBytes"/> already, and every one of its calls would pay for a read that
/// most questions about the store never need. Lite has no managed PostgreSQL store, so this tool has no Lite
/// twin — see <c>Lite.Tests/CrossAppMcpToolInventoryPinTests.KnownLiteMissingMcpTools</c>.</para>
///
/// <para>The <c>mcp</c> role reads this exactly as least-privilege as every other MCP read (ruling 2) —
/// nothing here widens it. The settings' FILE attribution is read off local disk by the service process
/// itself (<see cref="DarlingStoreHostProfile.AttributeManagedSetting"/>), the same as part 1, needing no
/// PostgreSQL privilege at all; only the LIVE <c>pg_settings</c> values and the store facts travel over the
/// <c>mcp</c> role's connection.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreHostTools
{
    [McpServerTool(Name = "get_store_host"), Description(
        "The monitoring STORE's own host/settings profile - is IT sized right, not a monitored server. No "
        + "server_name, no window: a snapshot. Reads the managed data directory's conf files off local disk for "
        + "settings attribution (needs no elevated PostgreSQL privilege); a bring-your-own store reports every "
        + "setting not-managed. stale_after_hardware_change means a managed block still sets a value this host's "
        + "CURRENT RAM/disk would size differently today - the #4207/#4211 class of drift. <<GUIDE>> Reports the "
        + "host PostgreSQL/TimescaleDB runs on and whether the settings in force still match it: platform "
        + "(OS, containerized, processor_count), cloud (provider - aws or azure - and instance_type, the EC2 "
        + "instance type or Azure VM size; both null when this host is not on either cloud or the metadata "
        + "probe found nothing; the value is whatever the link-local metadata service reported, not verified), "
        + "ram (total_bytes, cgroup_limit_bytes if any, effective_bytes, "
        + "authoritative, source), data_volume (total_bytes, free_bytes, filesystem, ready; on a bring-your-own "
        + "store this is the SERVICE's own disk, not necessarily the store's - data_volume.note says so when "
        + "managed is false), managed, and store (postgres_version, timescale_version, size_bytes, "
        + "buffer_hit_ratio_percent, temp_bytes, uncompressed_chunk_bytes, uncompressed_chunk_count, "
        + "uncompressed_chunk_percent_of_ram - the #4211 metric: how much of the store's raw ingest sits "
        + "uncompressed against the RAM budget). settings is one row per sizing-relevant setting (shared_buffers, "
        + "work_mem, effective_cache_size, maintenance_work_mem, the TimescaleDB worker counts, max_connections, "
        + "max_wal_size): current (the live value), source (which conf block or ALTER SYSTEM set it), derived "
        + "(what the sizing function would compute for THIS host right now) and verdict, one of four: matches "
        + "(a managed block set it and it still agrees with today's derivation), stale_after_hardware_change (a "
        + "managed block set it but re-deriving from the CURRENT host gives a different number - a resize the "
        + "settings never caught up with), operator_override (postgresql.auto.conf, or a hand-edited line after "
        + "every managed block, is what is actually in force), or not_managed (a bring-your-own store; nothing "
        + "here ever wrote a block to compare against, so pg_settings.source is reported as-is). "
        + "any_stale is true when one or more settings verdict is stale_after_hardware_change, so a caller can "
        + "act on the summary without walking the whole table. gathered_at (UTC) is when this snapshot was "
        + "taken; the store/settings facts are cached for up to 5 minutes and shared across callers, so a burst "
        + "of calls costs one live read. This tool never writes a setting or a conf file.")]
    public static async Task<string> GetStoreHost(
        NpgsqlDataSource postgres, PostgresConfig? postgresConfig, StoreHostProfileCache cache,
        CancellationToken cancellationToken = default)
    {
        if (postgresConfig is null)
        {
            /* Only reachable off the direct-call web path when a caller builds the dispatch table without a
               config in scope (BuildReadDispatch's postgresConfig defaults to null for such callers, mirroring
               its logger parameter) — true MCP dispatch always resolves this from DI, registered once at host
               start from the same DarlingConfig every other seat on this host shares. */
            return McpHelpers.Status(
                "unavailable", "The store host profile is not available on this call path (no postgres configuration in scope).");
        }

        try
        {
            /* #4203: the caller's own token is LINKED with the cache's fixed gather deadline, not replaced by
               it — either the caller cancelling (an abandoned web request) or the deadline elapsing (a slow
               store) stops the gather. A gather this ties into is still never cached on cancellation: it
               throws OperationCanceledException out of the gather delegate, and StoreHostProfileCache only
               assigns its cache entry after a gather call that RETURNS, so a cancelled gather leaves the
               cache exactly as StoreHostProfileCacheTests.AFailedGather_IsNotCached already proves for any
               other exception. */
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(ServiceCommandDeadlines.McpStoreHostProfileSeconds));

            /* Round-1 review, Medium 2: the store/settings facts are cached for up to 5 minutes and shared
               across callers, so a burst of calls costs one live read. The connection opens INSIDE the
               gather delegate — a cache hit never calls this delegate at all, so a hit opens no connection. */
            var (profile, gatheredAtUtc) = await cache.GetOrGatherAsync(
                async token =>
                {
                    await using var connection = await postgres.OpenConnectionAsync(token);
                    return await DarlingStoreHostProfile.GatherAsync(postgresConfig, connection, token);
                },
                cts.Token);

            var anyStale = profile.Settings.Any(s => s.Verdict == HostSettingVerdict.StaleAfterHardwareChange);
            var ramPercent = profile.Memory.EffectiveBytes > 0
                ? Math.Round(100.0 * profile.Store.UncompressedChunkBytes / profile.Memory.EffectiveBytes, 1)
                : (double?)null;

            /* Round-1 review, Medium 1: resolved once, reused for every settings row below rather than
               re-resolved per row — same managed data directory GatherSettingProfilesAsync itself resolved
               for this call, so FormatSourceForMcp's containment test agrees with what actually attributed
               each row. Round-1 review, Medium 1 follow-up: calls TryResolveProfileDataDirectory (not the
               raw ResolveDataDirectory) so a UNC-configured managed store agrees with
               GatherSettingProfilesAsync's own refusal (Low 4) instead of silently resolving a path
               GatherSettingProfilesAsync never used. */
            var mcpDataDirectory = postgresConfig.Managed ? DarlingStoreHostProfile.TryResolveProfileDataDirectory(postgresConfig) : null;

            return JsonSerializer.Serialize(new
            {
                platform = profile.Platform,
                containerized = profile.IsContainerized,
                processor_count = profile.ProcessorCount,
                cloud = new
                {
                    provider = profile.Cloud.Provider,
                    instance_type = profile.Cloud.InstanceType,
                },
                ram = new
                {
                    total_bytes = profile.Memory.TotalBytes,
                    cgroup_limit_bytes = profile.Memory.CgroupLimitBytes,
                    effective_bytes = profile.Memory.EffectiveBytes,
                    authoritative = profile.Memory.IsAuthoritative,
                    source = profile.Memory.SourceDescription,
                },
                data_volume = new
                {
                    total_bytes = profile.DataVolume.TotalBytes,
                    free_bytes = profile.DataVolume.FreeBytes,
                    filesystem = profile.DataVolume.Filesystem,
                    ready = profile.DataVolume.IsReady,
                    /* #4214 part 1's own doc comment on ResolveVolumeAnchor flagged this as part 2's to say:
                       a bring-your-own store's "data volume" is this SERVICE's own disk, which may not be the
                       store's disk at all (postgres.connectionString can point anywhere). */
                    note = profile.IsManagedStore
                        ? null
                        : "This is the service's own disk, not necessarily the store's — a bring-your-own postgres.connectionString may point at a different host entirely.",
                },
                managed = profile.IsManagedStore,
                store = new
                {
                    postgres_version = profile.Store.PostgresVersion,
                    timescale_version = profile.Store.TimescaleVersion,
                    size_bytes = profile.Store.StoreSizeBytes,
                    buffer_hit_ratio_percent = profile.Store.BufferHitRatioPercent is { } hit ? Math.Round(hit, 1) : (double?)null,
                    temp_bytes = profile.Store.TempBytes,
                    uncompressed_chunk_bytes = profile.Store.UncompressedChunkBytes,
                    uncompressed_chunk_count = profile.Store.UncompressedChunkCount,
                    uncompressed_chunk_percent_of_ram = ramPercent,
                },
                settings = profile.Settings.Select(s => new
                {
                    name = s.Name,
                    current = s.CurrentValueDisplay,
                    derived = s.DerivedValueDisplay,
                    source = DarlingStoreHostProfile.FormatSourceForMcp(s, mcpDataDirectory),
                    verdict = DarlingStoreHostProfile.DescribeVerdict(s.Verdict).Replace('-', '_'),
                }),
                any_stale = anyStale,
                /* Round-1 review, correcting the original plan: naive-UTC (no trailing Z), matching every
                   other "_at" timestamp on an MCP payload in this codebase (DarlingFleetReader.GeneratedAt,
                   DarlingAgReader, DarlingMcpConfigHistoryTools's NaiveUtc helper, DarlingMcpTrendTools) —
                   not a DateTimeKind.Utc value, which would serialize with a trailing Z instead. */
                gathered_at = DateTime.SpecifyKind(gatheredAtUtc, DateTimeKind.Unspecified),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_host", ex);
        }
    }
}
