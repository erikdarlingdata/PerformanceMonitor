/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>One metric's alert count for a server over the report period (#7041).</summary>
internal sealed record DailyHealthAlertCount(string Metric, int Count);

/// <summary>A job that failed on a server in the period: how often, when it last failed, and the last message (#7041).</summary>
internal sealed record DailyHealthFailedJob(string JobName, int Failures, DateTime LastFailureUtc, string? Message);

/// <summary>A database with a full backup, log backup or CHECKDB past its bar, as the database protection read states it (#7041).</summary>
internal sealed record DailyHealthProtectionItem(string Kind, string Database, string Text);

/// <summary>A volume under the low-disk bar at the period's newest disk snapshot (#7041).</summary>
internal sealed record DailyHealthLowDisk(string Volume, double FreeGb, double TotalGb, double FreePercent);

/// <summary>
/// One server's section of the Daily Health Report (#7041). Empty lists are the "leave it out" rule: the renderers print a
/// section only when it holds something, and a server with no sections at all is one line.
/// </summary>
internal sealed record DailyHealthServerReport(
    int ServerId,
    string Name,
    bool IsPostgres,
    string Band,
    IReadOnlyList<string> Reasons,
    IReadOnlyList<DailyHealthAlertCount> Alerts,
    IReadOnlyList<DailyHealthFailedJob> FailedJobs,
    IReadOnlyList<DailyHealthProtectionItem> Protection,
    IReadOnlyList<DailyHealthLowDisk> LowDisk,
    IReadOnlyList<string> CollectionProblems)
{
    /// <summary>Nothing to report: the one-line server.</summary>
    public bool IsQuiet =>
        Reasons.Count == 0 && Alerts.Count == 0 && FailedJobs.Count == 0 && Protection.Count == 0
        && LowDisk.Count == 0 && CollectionProblems.Count == 0;

    public int AlertTotal => Alerts.Sum(a => a.Count);

    /// <summary>Worst-first sort key: Critical, then Offline, then Warning, then Healthy.</summary>
    public int BandRank => DailyHealthReportData.BandRankOf(Band);
}

/// <summary>The whole report before rendering: the fleet header and every server, worst first (#7041).</summary>
internal sealed record DailyHealthReportData(
    string Title,
    DateTime PeriodStartUtc,
    DateTime PeriodEndUtc,
    DateOnly LocalDate,
    string TimeZoneId,
    IReadOnlyList<DailyHealthServerReport> Servers)
{
    public int HealthyCount => Servers.Count(s => s.Band == "Healthy");
    public int WarningCount => Servers.Count(s => s.Band == "Warning");
    public int CriticalCount => Servers.Count(s => s.Band == "Critical");
    public int OfflineCount => Servers.Count(s => s.Band == "Offline");
    public int AlertsFired => Servers.Sum(s => s.AlertTotal);

    public static int BandRankOf(string band) => band switch
    {
        "Critical" => 0,
        "Offline" => 1,
        "Warning" => 2,
        _ => 3,
    };
}
