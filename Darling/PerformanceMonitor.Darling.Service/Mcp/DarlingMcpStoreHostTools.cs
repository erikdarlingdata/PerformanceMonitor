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
using PerformanceMonitor.Darling.Service.FrontDoor;

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
/// twin.</para>
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
    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_host"), Description(
        "The monitoring STORE's own host/settings profile - is IT sized right, not a monitored server. No "
        + "server_name, no window: a snapshot. Reads the managed data directory's conf files off local disk for "
        + "settings attribution (needs no elevated PostgreSQL privilege); a bring-your-own store reports every "
        + "setting not-managed. stale_after_hardware_change means a managed block still sets a value this host's "
        + "CURRENT RAM/disk would size differently today - that class of drift. <<GUIDE>> Reports the "
        + "host PostgreSQL/TimescaleDB runs on and whether the settings in force still match it: platform "
        + "(OS, containerized, processor_count), cloud (provider - aws or azure - and instance_type, the EC2 "
        + "instance type or Azure VM size; both null when this host is not on either cloud or the metadata "
        + "probe found nothing; the value is whatever the link-local metadata service reported, not verified), "
        + "ram (total_bytes, cgroup_limit_bytes if any, effective_bytes, "
        + "authoritative, source), data_volume (total_bytes, free_bytes, filesystem, ready; on a bring-your-own "
        + "store this is the SERVICE's own disk, not necessarily the store's - data_volume.note says so when "
        + "managed is false), managed, and store (postgres_version, timescale_version, size_bytes, "
        + "buffer_hit_ratio_percent, temp_bytes, uncompressed_chunk_bytes, uncompressed_chunk_count, "
        + "uncompressed_chunk_percent_of_ram - the metric for how much of the store's raw ingest sits "
        + "uncompressed against the RAM budget). settings is one row per sizing-relevant setting (shared_buffers, "
        + "work_mem, effective_cache_size, maintenance_work_mem, the TimescaleDB worker counts, max_connections, "
        + "max_wal_size): current (the live value), source (which conf block or ALTER SYSTEM set it), derived "
        + "(what the sizing function would compute for THIS host right now) and verdict, one of four: matches "
        + "(a managed block set it and it still agrees with today's derivation), stale_after_hardware_change (a "
        + "managed block set it but re-deriving from the CURRENT host gives a different number - a resize the "
        + "settings never caught up with), operator_override (postgresql.auto.conf, or a hand-edited line after "
        + "every managed block, is what is actually in force), or not_managed (a bring-your-own store; nothing "
        + "here ever wrote a block to compare against, so pg_settings.source is reported as-is). Each settings row also "
        + "carries provenance: value (the running value the way SHOW prints it, such as 2GB), file_value (the text the winning line carries, such as 77MB), source (pg_settings.source), source_file and source_line (null when the "
        + "role cannot read them), pending_restart (a restart-only setting whose file line the server could not apply yet; false when the role cannot read pg_file_settings), not_applied (every pg_file_settings line for the setting that is not the value "
        + "in force, with file, line and value, so a managed line hidden by ALTER SYSTEM shows) and summary, one sentence naming them. "
        + "provenance_unreadable is one sentence with the grant that fixes it when the role cannot read the source columns or "
        + "pg_file_settings, else null. "
        + "planner_costs is one row per planner cost setting (random_page_cost, seq_page_cost, effective_io_concurrency): "
        + "current, source (pg_settings.source), source_file (the conf file when the role can read pg_settings.sourcefile, "
        + "else null: the source alone is reported), managed_value and matches_managed (the managed file writes only "
        + "random_page_cost, 1.1; both are null for the other two and on a store that is not managed). "
        + "planner_cost_warning is one sentence when random_page_cost is not the managed value, saying the planner may "
        + "skip covering indexes and the shorter Query Store windows' plans on this store and where the value comes from; "
        + "null otherwise. "
        + "any_stale is true when one or more settings verdict is stale_after_hardware_change, so a caller can "
        + "act on the summary without walking the whole table. gathered_at (UTC) is when this snapshot was "
        + "taken; the store/settings facts are cached for up to 5 minutes and shared across callers, so a burst "
        + "of calls costs one live read. store.size_bytes is the hourly sweep's recorded figure, not a live read: "
        + "size_as_of is when the sweep recorded it and size_age_seconds how old it is as of this response. "
        + "size_estimated is true when that figure is the hourly catalog estimate (page counts the last vacuum or "
        + "analyze recorded, so it lags the files and reads a little low) and false for the exact size the sweep "
        + "writes once a day; null on a row from before the marker. Before "
        + "the first sweep, or when the stored figure cannot be read, size_bytes, size_as_of and size_age_seconds "
        + "are null and size_unavailable_reason says why. The uncompressed_chunk_* figures are read live on every "
        + "call; if that read times out or the role is refused it (any error but TimescaleDB being absent) they are "
        + "null, uncompressed_chunk_percent_of_ram is null with them, and uncompressed_unavailable_reason says why "
        + "(the SQLSTATE for a refusal); a store without TimescaleDB reports 0 chunks. The rest of the profile still "
        + "returns (a slow fact fails alone). --check-settings reads the size live. "
        + "store_certificate is the server certificate the store presents to TLS clients: tls is on, off, "
        + "socket or unavailable. On, source says where it was read: managed_file (the server.crt the service wrote for a "
        + "managed store, beside the data directory) or tls_handshake (the certificate the store presents when this "
        + "service connects, for a store it does not manage; over TCP the handshake ends before any user name or "
        + "password is sent), and server carries subject, issuer, "
        + "sha256 (64 upper-case hex characters, the fingerprint of the certificate's DER bytes), not_before and not_after "
        + "(UTC) and days_to_expiry (whole days, negative once expired, as of this response). root is the issuing "
        + "root's subject and sha256 when the chain carries one, else null. Off means the store's ssl setting is off and "
        + "message says so. Socket means the store is reached over a Unix socket, which has no TLS handshake to read, so "
        + "the service reads nothing and sends nothing. Unavailable means the setting or the certificate could not be read "
        + "(the read has its own short time limit) and message says why; the rest of the profile still returns. A managed "
        + "store's file is read on every call. A handshake result is kept for 10 minutes per store connection, so the "
        + "store's log does not record a failed TLS handshake on every call, and a changed certificate shows within that time. "
        + "postgres_owner says who owns the PostgreSQL "
        + "process: owner is started_by_this_service (stopping the service stops PostgreSQL), adopted_running (it leaves PostgreSQL running), "
        + "not_managed or not_yet_known (still starting), with the sentence in on_service_stop. "
        + "growth_forecast says how long the data volume's free space lasts: growth_bytes_per_day_7d is the store's average "
        + "daily growth over the last 7 full days (the same daily history get_store_metrics returns as daily_growth), free_floor_percent "
        + "is the free-space floor, a parameter with a default of 25 and a range above 0 and below 100, floor_bytes is that share of the "
        + "volume, headroom_bytes is the free space above it and days_until_free_floor is the headroom divided by the 7-day rate. "
        + "days_until_free_floor is 0 when the volume is already at or below the floor. It is null, with growth_note saying why, when "
        + "the history has fewer than 7 full days (growth_days_in_window says how many it has), when the store did not grow over them, "
        + "when the volume could not be read, or when the store is not managed (data_volume is then the service's disk, not the "
        + "store's). It is a straight-line estimate from the net size change, so a compression pass or a retention drop inside the "
        + "window lowers it. This "
        + "tool never writes a setting or a conf file.")]
    public static Task<string> GetStoreHost(
        NpgsqlDataSource postgres, PostgresConfig? postgresConfig, StoreHostProfileCache cache,
        StoreProcessOwnership? ownership = null,
        double free_floor_percent = DefaultFreeFloorPercent,
        CancellationToken cancellationToken = default) =>
        GetStoreHostAt(postgres, postgresConfig, cache, ownership, free_floor_percent, forecastAtUtc: null, cancellationToken);

    /// <summary>
    /// <see cref="GetStoreHost"/> with the growth forecast's "now" given by the caller (#9429), so a test that seeds the history on one
    /// UTC day anchors the reader's window on that same day instead of on a second read of the clock. The tool passes null and gets
    /// the current UTC time.
    /// </summary>
    internal static async Task<string> GetStoreHostAt(
        NpgsqlDataSource postgres, PostgresConfig? postgresConfig, StoreHostProfileCache cache,
        StoreProcessOwnership? ownership,
        double free_floor_percent,
        DateTime? forecastAtUtc,
        CancellationToken cancellationToken)
    {
        if (!(free_floor_percent > 0d && free_floor_percent < 100d))
        {
            return McpHelpers.Refusal("free_floor_percent", "free_floor_percent must be above 0 and below 100 (default 25).");
        }

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
                    /* #5718: this tool (and the web card, which calls it on each first paint, hashchange and span
                       change) takes the store size from the hourly sweep's stored figure and reports its age,
                       rather than paying the live whole-store size walk on every call, which measured 5.8 s mean /
                       30.7 s max on a slow store and failed at the 10 s bound. --check-settings keeps the live read. */
                    return await DarlingStoreHostProfile.GatherAsync(
                        postgresConfig, connection, token, useStoredStoreSize: true);
                },
                cts.Token);

            var owner = ownership?.Current ?? StoreProcessOwnership.Owner.Unknown;
            var anyStale = profile.Settings.Any(s => s.Verdict == HostSettingVerdict.StaleAfterHardwareChange);
            var ramPercent = profile.Memory.EffectiveBytes > 0 && profile.Store.UncompressedChunkBytes is { } uncompressedBytes
                ? Math.Round(100.0 * uncompressedBytes / profile.Memory.EffectiveBytes, 1)
                : (double?)null;

            /* #5718: how old the stored size figure is, as of THIS response (the profile can be up to 5 minutes
               old out of the cache, and the figure was already up to an hour old when it was gathered). */
            var sizeAgeSeconds = profile.Store.StoreSizeAsOfUtc is { } sizeAsOf
                ? Math.Max(0L, (long)Math.Round((DateTime.UtcNow - sizeAsOf).TotalSeconds))
                : (long?)null;

            /* Round-1 review, Medium 1: resolved once, reused for every settings row below rather than
               re-resolved per row — same managed data directory GatherSettingProfilesAsync itself resolved
               for this call, so FormatSourceForMcp's containment test agrees with what actually attributed
               each row. Round-1 review, Medium 1 follow-up: calls TryResolveProfileDataDirectory (not the
               raw ResolveDataDirectory) so a UNC-configured managed store agrees with
               GatherSettingProfilesAsync's own refusal (Low 4) instead of silently resolving a path
               GatherSettingProfilesAsync never used. */
            /* #9430: the certificate is read on every call, outside the profile cache, so days_to_expiry is as of this response and a
               rotated certificate shows at once. A failure or its own short deadline here answers unavailable and never fails the profile; only the caller cancelling does. */
            var certificate = await StoreServerCertificate.ReadBoundedAsync(
                async token =>
                {
                    await using var certificateConnection = await postgres.OpenConnectionAsync(token);
                    return await StoreServerCertificate.ReadAsync(postgresConfig, certificateConnection, token);
                },
                StoreServerCertificate.ReadTimeout,
                cancellationToken);

            var mcpDataDirectory = postgresConfig.Managed ? DarlingStoreHostProfile.TryResolveProfileDataDirectory(postgresConfig) : null;

            /* #9429: the days until the data volume falls to the free-space floor, from the daily history get_store_metrics
               already returns. Read here, outside the profile cache, so the rate is as current as the sweep and a failure of
               this read nulls the forecast without failing the profile. */
            var forecastAt = forecastAtUtc ?? DateTime.UtcNow;
            var (growthPoints, growthFailure) = await ReadStoreGrowthAsync(postgres, forecastAt, cancellationToken);
            var forecast = growthPoints is null
                ? StoreGrowthForecast.Unavailable(free_floor_percent, growthFailure!)
                : ForecastDaysUntilFloor(
                    growthPoints, forecastAt, profile.DataVolume.TotalBytes, profile.DataVolume.FreeBytes,
                    profile.DataVolume.IsReady, profile.IsManagedStore, free_floor_percent);

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
                        : "This is the service's own disk, not necessarily the store's: a bring-your-own postgres.connectionString may point at a different host entirely.",
                },
                managed = profile.IsManagedStore,
                growth_forecast = new
                {
                    free_floor_percent = forecast.FreeFloorPercent,
                    floor_bytes = forecast.FloorBytes,
                    headroom_bytes = forecast.HeadroomBytes,
                    growth_bytes_per_day_7d = forecast.GrowthBytesPerDay,
                    growth_days_in_window = forecast.DaysInWindow,
                    days_until_free_floor = forecast.DaysUntilFloor,
                    growth_note = forecast.Note,
                },
                /* #8787 part D2: who owns the postmaster, from the service's own state (not SQL, and outside the 5-minute profile
                   cache, so it is always current). The answer decides what a service stop does to the store. */
                postgres_owner = new
                {
                    owner = !profile.IsManagedStore ? "not_managed" : StoreProcessOwnership.Name(owner),
                    started_by_this_service = profile.IsManagedStore && owner == StoreProcessOwnership.Owner.StartedByThisService,
                    on_service_stop = StoreProcessOwnership.Describe(profile.IsManagedStore, owner),
                },
                store_certificate = DescribeCertificate(certificate, DateTime.UtcNow),
                store = new
                {
                    postgres_version = profile.Store.PostgresVersion,
                    timescale_version = profile.Store.TimescaleVersion,
                    size_bytes = profile.Store.StoreSizeBytes,
                    /* #5718: the hourly sweep's metric_time (naive UTC) the size above was recorded at, and its age
                       in seconds as of this response; both null, with size_unavailable_reason set, when there is
                       no stored figure or the read failed. */
                    /* #6245: true when the stored figure is the hourly catalog estimate (reads a little low and lags the
                       files), false for the once-a-day exact one, null when read live or on a row from before the marker. */
                    size_estimated = profile.Store.StoreSizeEstimated,
                    size_as_of = profile.Store.StoreSizeAsOfUtc,
                    size_age_seconds = sizeAgeSeconds,
                    size_unavailable_reason = profile.Store.StoreSizeUnavailableReason,
                    buffer_hit_ratio_percent = profile.Store.BufferHitRatioPercent is { } hit ? Math.Round(hit, 1) : (double?)null,
                    temp_bytes = profile.Store.TempBytes,
                    uncompressed_chunk_bytes = profile.Store.UncompressedChunkBytes,
                    uncompressed_chunk_count = profile.Store.UncompressedChunkCount,
                    uncompressed_chunk_percent_of_ram = ramPercent,
                    uncompressed_unavailable_reason = profile.Store.UncompressedUnavailableReason,
                },
                settings = profile.Settings.Select(s => new
                {
                    name = s.Name,
                    current = s.CurrentValueDisplay,
                    derived = s.DerivedValueDisplay,
                    source = DarlingStoreHostProfile.FormatSourceForMcp(s, mcpDataDirectory),
                    verdict = DarlingStoreHostProfile.DescribeVerdict(s.Verdict).Replace('-', '_'),
                    provenance = ShapeProvenance(s.Provenance, mcpDataDirectory),
                }),
                provenance_unreadable = profile.Settings.Select(s => s.ProvenanceUnreadable).FirstOrDefault(note => note is not null),
                /* #9179: the planner cost settings, with the same current/source fields the sizing settings carry plus
                   the file (null when the role cannot read pg_settings.sourcefile) and whether each matches the value
                   the managed file writes. managed_value and matches_managed are null where the managed file sets none
                   and on a store that is not managed. */
                planner_costs = profile.PlannerCosts.Select(c => new
                {
                    name = c.Name,
                    current = c.Current,
                    source = c.Source,
                    source_file = c.SourceFile,
                    managed_value = c.ManagedValue,
                    matches_managed = c.MatchesManaged,
                }),
                planner_cost_warning = DarlingStoreHostProfile.DescribeRandomPageCostMismatch(profile.PlannerCosts),
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
            return McpHelpers.FormatError("get_store_host", FailureFor(ex));
        }
    }

    /// <summary>The default free-space floor for <c>growth_forecast</c>: 25 percent of the data volume free (#9429).</summary>
    public const double DefaultFreeFloorPercent = 25d;

    /// <summary>The days of daily growth the forecast averages, and so the shortest history it answers on (#9429).</summary>
    internal const int ForecastWindowDays = 7;

    /// <summary>
    /// What <c>growth_forecast</c> reports (#9429). Every figure but the floor percent is null when it cannot be stated, with
    /// <see cref="Note"/> saying why.
    /// </summary>
    internal sealed record StoreGrowthForecast(
        double FreeFloorPercent, long? FloorBytes, long? HeadroomBytes, double? GrowthBytesPerDay, int DaysInWindow,
        double? DaysUntilFloor, string? Note)
    {
        internal static StoreGrowthForecast Unavailable(double floorPercent, string note) =>
            new(floorPercent, null, null, null, 0, null, note);
    }

    /// <summary>
    /// The store's daily growth points (<see cref="DarlingStoreMetricsReader.ComputeDailyGrowth"/>, the series
    /// <c>get_store_metrics</c> returns as <c>daily_growth</c>) over the last days, or null with a sentence when the history could not
    /// be read. The window starts nine days back so the 7 full days before today each have a predecessor.
    /// </summary>
    private static async Task<(System.Collections.Generic.List<DarlingStoreMetricsReader.DailyGrowthPoint>? Points, string? Failure)> ReadStoreGrowthAsync(
        NpgsqlDataSource postgres, DateTime nowUtc, CancellationToken cancellationToken)
    {
        try
        {
            /* Only the store's own total feeds the rate, so only that kind is read: the all-kinds read of the same window costs far more on a store with
               a few hundred tables and indexes. */
            var daily = await DarlingStoreMetricsReader.GetDailyForKindAsync(
                postgres, nowUtc.AddDays(-(ForecastWindowDays + 2)), PerformanceMonitor.Darling.Storage.StoreSelfMetrics.StoreObjectKind, cancellationToken);
            var storeDaily = daily.OrderBy(p => p.Day).ToList();
            return (DarlingStoreMetricsReader.ComputeDailyGrowth(storeDaily, nowUtc), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, ex is PostgresException pg
                ? $"The store's size history could not be read (SQLSTATE {pg.SqlState}), so there is no growth forecast."
                : "The store's size history could not be read, so there is no growth forecast.");
        }
    }

    /// <summary>
    /// The days until the data volume's free space falls to the floor at the 7-day growth rate (#9429). Pure, so the arithmetic is
    /// tested without a store.
    ///
    /// <para>The rate is the mean of the net daily byte deltas of the <see cref="ForecastWindowDays"/> full days before today (today's
    /// partial point is left out). A history with fewer such days has no 7-day rate, and the answer says how many days it has, never a
    /// guess from fewer. A volume already at or below the floor answers 0 days whatever the history, because that needs no rate.
    /// A store that did not grow over the window has no days figure: it is not on its way to the floor.</para>
    /// </summary>
    internal static StoreGrowthForecast ForecastDaysUntilFloor(
        System.Collections.Generic.IReadOnlyList<DarlingStoreMetricsReader.DailyGrowthPoint> growth,
        DateTime nowUtc, long volumeTotalBytes, long volumeFreeBytes, bool volumeReady, bool storeIsManaged, double floorPercent)
    {
        var today = nowUtc.Date;
        var window = growth
            .Where(g => !g.Partial && g.Day.Date >= today.AddDays(-ForecastWindowDays) && g.Day.Date < today)
            .ToList();
        var ratePerDay = window.Count >= ForecastWindowDays ? window.Sum(g => (double)g.DeltaBytes) / ForecastWindowDays : (double?)null;

        if (!storeIsManaged)
        {
            return new StoreGrowthForecast(
                floorPercent, null, null, ratePerDay, window.Count, null,
                "This store is not managed by the service, so data_volume is the service's own disk and not the store's. No days figure is given for a disk the store may not be on.");
        }

        if (!volumeReady || volumeTotalBytes <= 0)
        {
            return new StoreGrowthForecast(
                floorPercent, null, null, ratePerDay, window.Count, null, "The data volume's size could not be read, so there is no free-space floor to count to.");
        }

        var floorBytes = (long)Math.Round(volumeTotalBytes * floorPercent / 100d);
        var headroom = volumeFreeBytes - floorBytes;
        if (headroom <= 0)
        {
            return new StoreGrowthForecast(
                floorPercent, floorBytes, headroom, ratePerDay, window.Count, 0d,
                "Free space on the data volume is already at or below the floor.");
        }

        if (ratePerDay is not { } rate)
        {
            return new StoreGrowthForecast(
                floorPercent, floorBytes, headroom, null, window.Count, null,
                $"Only {window.Count} of the last {ForecastWindowDays} full days have a growth figure, so there is no 7-day rate yet.");
        }

        if (rate <= 0d)
        {
            return new StoreGrowthForecast(
                floorPercent, floorBytes, headroom, rate, window.Count, null,
                $"The store did not grow over the last {ForecastWindowDays} full days, so it is not on its way to the floor at that rate.");
        }

        return new StoreGrowthForecast(floorPercent, floorBytes, headroom, rate, window.Count, Math.Round(headroom / rate, 1), null);
    }

    /// <summary>#9430: the <c>store_certificate</c> object. Times are UTC with a trailing Z, because a certificate's validity is a fixed instant.</summary>
    internal static object DescribeCertificate(StoreServerCertificate.Report report, DateTime nowUtc) => new
    {
        tls = report.Tls,
        source = report.Source,
        message = report.Message,
        server = report.Server is { } server
            ? new
            {
                subject = server.Subject,
                issuer = server.Issuer,
                sha256 = server.Sha256,
                not_before = server.NotBeforeUtc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture),
                not_after = server.NotAfterUtc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture),
                days_to_expiry = (int?)StoreServerCertificate.DaysToExpiry(server.NotAfterUtc, nowUtc),
            }
            : null,
        root = report.Root is { } root ? new { subject = root.Subject, sha256 = root.Sha256 } : null,
    };

    /// <summary>One setting's provenance as the MCP payload carries it (#9418), with every file path reduced to what a
    /// remote caller may see. Null when the setting was not visible on the connection.</summary>
    internal static object? ShapeProvenance(SettingProvenance? provenance, string? dataDirectory)
    {
        if (provenance is null)
        {
            return null;
        }

        Func<string, string> redact = file => DarlingStoreHostProfile.SanitizeSourcePathForMcp(file, dataDirectory);
        return new
        {
            value = provenance.ValueDisplay,
            file_value = provenance.AppliedFileValue,
            source = provenance.Source,
            source_file = provenance.SourceFile is null ? null : redact(provenance.SourceFile),
            source_line = provenance.SourceLine,
            pending_restart = provenance.PendingRestart,
            not_applied = provenance.UnappliedFileRows.Select(r => new
            {
                file = r.File is null ? null : redact(r.File),
                line = r.Line,
                value = r.Value,
                error = r.Error,
            }),
            summary = StoreSettingProvenance.Describe(provenance, redact),
        };
    }

    /// <summary>
    /// The exception whose message the error envelope carries for a failure that escaped the gather (#5718).
    /// The two slow size facts fail alone and never get here; what can still arrive is a timeout in one of the
    /// cheap reads (version, buffer hit, <c>pg_settings</c>). Npgsql words that "Exception while reading from
    /// stream", which reads as a dropped connection; this says it timed out and after how long. Anything else
    /// keeps its own message. The envelope itself is still built by <c>McpHelpers.FormatError</c> at the catch.
    /// </summary>
    internal static Exception FailureFor(Exception ex) =>
        DarlingStoreHostProfile.IsReadTimeout(ex)
            ? new TimeoutException(
                $"a store read {DarlingStoreHostProfile.DescribeReadTimeout(ServiceCommandDeadlines.CliStoreReadSeconds)} "
                + "(the store was slow to answer); retry in a moment", ex)
            : ex;
}
