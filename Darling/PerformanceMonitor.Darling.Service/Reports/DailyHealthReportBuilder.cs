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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>
/// Builds the Daily Health Report (#7041) for the 24 hours that end at a given instant, from rows the store already holds:
/// the fleet overview cards (each server's band over the period, the collection state and the database protection bars),
/// the alert history, the Agent job history and the newest disk snapshot in the period. Nothing here connects to a monitored server.
///
/// <para>The title, the time zone and the low-disk bars come from the store's alert settings row, read on every build, so the
/// scheduler and <c>get_daily_health_report</c> always agree about them.</para>
///
/// <para><b>What is a point-in-time read.</b> The fleet cards read their collection and database protection state as the store
/// holds it when the report is built, not as it was at the period's end. For the scheduled send that is the same moment. For
/// <c>get_daily_health_report</c> asked for an earlier day, the band, alert, job and disk sections are that day's and the collection and
/// protection sections are today's.</para>
/// </summary>
internal sealed class DailyHealthReportBuilder : IDailyHealthReportSource
{
    /// <summary>The metric name the report's own delivery is logged under, kept out of the count of alerts fired.</summary>
    internal const string DeliveryMetricName = "Daily Health Report";

    /// <summary>The longest job history and alert history page one build reads; the reports counts are of these rows.</summary>
    private const int HistoryRowCap = 20_000;

    private readonly NpgsqlDataSource _postgres;

    public DailyHealthReportBuilder(NpgsqlDataSource postgres)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
    }

    public async Task<DailyHealthReportRendered> BuildAsync(DateTime periodEndUtc, CancellationToken ct)
    {
        var data = await BuildDataAsync(periodEndUtc, ct).ConfigureAwait(false);
        return Render(data);
    }

    internal static DailyHealthReportRendered Render(DailyHealthReportData data) =>
        new(DailyHealthReportRenderer.Subject(data), DailyHealthReportRenderer.Text(data), DailyHealthReportRenderer.Html(data), data.Servers.Count);

    public async Task<DailyHealthReportData> BuildDataAsync(DateTime periodEndUtc, CancellationToken ct)
    {
        var end = DateTime.SpecifyKind(periodEndUtc, DateTimeKind.Utc);
        var start = end.AddHours(-24);

        var config = await DarlingAlertReader.GetAlertConfigurationAsync(_postgres, ct).ConfigureAwait(false);
        var settings = config.Settings;
        var title = DailyHealthReportSettings.ResolveTitle(settings?.DailyReportTitle);
        var zone = DailyHealthReportSettings.ResolveZone(settings?.DailyReportTimeZone);
        var lowDiskPercent = settings?.LowDiskThresholdPercent ?? 10;
        var lowDiskGb = settings?.LowDiskThresholdGb ?? 5;

        var fleet = await DarlingFleetReader.GetFleetOverviewAsync(
            _postgres, start, end, DateTime.UtcNow, worstCount: int.MaxValue, cancellationToken: ct).ConfigureAwait(false);
        var reasonById = fleet.WorstServers.ToDictionary(w => w.ServerId, w => w.Reason);

        var alertRows = await DarlingAlertReader.GetAlertHistoryPageAsync(
            _postgres, start, end, serverId: null, HistoryRowCap, includeDismissed: true, cancellationToken: ct).ConfigureAwait(false);
        var alertsByServer = alertRows
            .Where(a => !AlertMetricClassifier.IsResolution(a.MetricName)
                        && !string.Equals(a.MetricName, DeliveryMetricName, StringComparison.OrdinalIgnoreCase))
            .GroupBy(a => a.ServerId)
            .ToDictionary(
                g => g.Key,
                g => (IReadOnlyList<DailyHealthAlertCount>)g
                    .GroupBy(a => a.MetricName, StringComparer.OrdinalIgnoreCase)
                    .Select(m => new DailyHealthAlertCount(m.First().MetricName, m.Count()))
                    .OrderByDescending(m => m.Count).ThenBy(m => m.Metric, StringComparer.OrdinalIgnoreCase)
                    .ToList());

        var jobRows = await DarlingJobHistoryReader.GetAsync(
            _postgres, start, serverId: null, HistoryRowCap, McpCommandDeadlines.ReadSeconds,
            new JobHistoryFilter(RunStatus: 0, UntilUtc: end), ct).ConfigureAwait(false);
        var jobsByServer = jobRows
            .Where(j => j.StepId == 0 && j.RunDateTimeUtc is { } at && at >= start && at <= end)
            .GroupBy(j => j.ServerId)
            .ToDictionary(
                g => g.Key,
                g => (IReadOnlyList<DailyHealthFailedJob>)g
                    .GroupBy(j => j.JobName, StringComparer.OrdinalIgnoreCase)
                    .Select(j =>
                    {
                        var last = j.OrderByDescending(r => r.RunDateTimeUtc).First();
                        return new DailyHealthFailedJob(last.JobName, j.Count(), last.RunDateTimeUtc!.Value, last.Message);
                    })
                    .OrderByDescending(j => j.Failures).ThenBy(j => j.JobName, StringComparer.OrdinalIgnoreCase)
                    .ToList());

        var disks = await ReadVolumesAsync(start, end, ct).ConfigureAwait(false);

        var servers = new List<DailyHealthServerReport>(fleet.Cards.Count);
        foreach (var card in fleet.Cards)
        {
            reasonById.TryGetValue(card.ServerId, out var reason);
            alertsByServer.TryGetValue(card.ServerId, out var alerts);
            jobsByServer.TryGetValue(card.ServerId, out var jobs);
            disks.TryGetValue(card.ServerId, out var volumes);
            servers.Add(AssembleServer(card, reason, alerts, jobs, volumes, lowDiskPercent, lowDiskGb));
        }

        servers = servers
            .OrderBy(s => s.BandRank)
            .ThenByDescending(s => s.AlertTotal)
            .ThenBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var localDate = DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(end, zone));
        return new DailyHealthReportData(title, start, end, localDate, zone.Id, servers);
    }

    /// <summary>
    /// One server's section from what was read for it. The section rules live here so a test can drive them without a store:
    /// a PostgreSQL target never gets the Agent job, database protection or disk sections (it has none of those sources, so a row
    /// that reached it is not shown), and a server with nothing in any section is the one-line server.
    /// </summary>
    internal static DailyHealthServerReport AssembleServer(
        FleetServerCard card,
        string? rankedReason,
        IReadOnlyList<DailyHealthAlertCount>? alerts,
        IReadOnlyList<DailyHealthFailedJob>? failedJobs,
        List<VolumeFreeSpaceInfo>? volumes,
        double lowDiskPercent,
        double lowDiskGb)
    {
        ArgumentNullException.ThrowIfNull(card);

        var reasons = new List<string>();
        if (card.Band is not (FleetHealthBand.Healthy or FleetHealthBand.Offline) && !string.IsNullOrWhiteSpace(rankedReason))
        {
            reasons.Add(rankedReason);
        }

        var problems = new List<string>();
        if (card.Band == FleetHealthBand.Offline || card.IsOnline == false)
        {
            problems.Add(card.LastCollectionTime is { } last
                ? "Offline, last collection " + last.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC"
                : "Offline, no collection recorded");
        }
        else if (card.CollectionStale)
        {
            problems.Add("Collection is stale");
        }

        if (card.FailedCollectorCount > 0)
        {
            problems.Add(card.FailedCollectorCount.ToString(CultureInfo.InvariantCulture)
                + (card.FailedCollectorCount == 1 ? " collector failing" : " collectors failing"));
        }

        if (card.RegressedCollectorCount > 0)
        {
            problems.Add(card.RegressedCollectorCount.ToString(CultureInfo.InvariantCulture)
                + (card.RegressedCollectorCount == 1 ? " collector regressed" : " collectors regressed"));
        }

        IReadOnlyList<DailyHealthFailedJob> jobs = Array.Empty<DailyHealthFailedJob>();
        IReadOnlyList<DailyHealthProtectionItem> protection = Array.Empty<DailyHealthProtectionItem>();
        IReadOnlyList<DailyHealthLowDisk> lowDisk = Array.Empty<DailyHealthLowDisk>();
        if (!card.IsPostgres)
        {
            if (failedJobs is not null) jobs = failedJobs;

            if (card.DatabaseProtectionOverdue is { Count: > 0 } overdue)
            {
                protection = overdue
                    .OrderBy(o => o.Kind, StringComparer.Ordinal).ThenBy(o => o.Database, StringComparer.OrdinalIgnoreCase)
                    .Select(o => new DailyHealthProtectionItem(o.Kind, o.Database, o.Text))
                    .ToList();
            }

            if (volumes is not null)
            {
                lowDisk = AlertContextBuilders.GetBreachedVolumes(volumes, lowDiskPercent, lowDiskGb)
                    .Select(v => new DailyHealthLowDisk(v.MountPoint, v.FreeGb, v.TotalMb / 1024.0, v.FreePercent))
                    .ToList();
            }
        }

        return new DailyHealthServerReport(
            card.ServerId,
            string.IsNullOrWhiteSpace(card.DisplayName) ? card.ServerName : card.DisplayName,
            card.IsPostgres,
            card.Band.ToString(),
            reasons,
            alerts ?? Array.Empty<DailyHealthAlertCount>(),
            jobs,
            protection,
            lowDisk,
            problems);
    }

    /// <summary>
    /// Each server's volumes at its newest disk snapshot inside the period (the same per-mount-point shape the Low Disk Space alert
    /// reads), so a day with no snapshot reports no disk section rather than a volume from before the period.
    /// </summary>
    internal const string PeriodVolumesSql = @"
WITH newest AS (
    SELECT server_id, MAX(collection_time) AS collection_time
    FROM database_size_stats
    WHERE collection_time >= $1
    AND   collection_time <= $2
    GROUP BY server_id
)
SELECT
    d.server_id,
    d.volume_mount_point,
    MAX(d.volume_total_mb) AS volume_total_mb,
    MIN(d.volume_free_mb) AS volume_free_mb,
    MAX(d.collection_time) AS newest_collection_time
FROM database_size_stats AS d
JOIN newest AS n ON n.server_id = d.server_id AND n.collection_time = d.collection_time
WHERE d.volume_mount_point IS NOT NULL
AND   d.volume_total_mb > 0
GROUP BY d.server_id, d.volume_mount_point";

    private async Task<Dictionary<int, List<VolumeFreeSpaceInfo>>> ReadVolumesAsync(DateTime start, DateTime end, CancellationToken ct)
    {
        var result = new Dictionary<int, List<VolumeFreeSpaceInfo>>();
        await using var command = _postgres.CreateCommand(PeriodVolumesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddTimestamp(command, start);
        DarlingMcpReadParameters.AddTimestamp(command, end);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var serverId = reader.GetInt32(0);
            if (!result.TryGetValue(serverId, out var list)) result[serverId] = list = new List<VolumeFreeSpaceInfo>();
            list.Add(new VolumeFreeSpaceInfo
            {
                MountPoint = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalMb = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture),
                FreeMb = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3), CultureInfo.InvariantCulture),
                NewestSampleUtc = reader.IsDBNull(4) ? null : DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc),
            });
        }

        return result;
    }
}
