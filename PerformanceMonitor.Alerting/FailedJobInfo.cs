/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// A SQL Agent job run that FAILED within the alert lookback window. Sourced from a live
/// msdb.dbo.sysjobhistory query at alert-check time (failure outcomes are not part of the
/// collected running_jobs snapshot). StepId/StepName/Message describe the actual failing step
/// (correlated from the run's step rows), falling back to the job-outcome row when a job-level
/// failure has no failed step.
/// Canonical shared copy (Phase-5 A0) — Lite and the Dashboard previously carried member-identical
/// local twins; both apps now alias this type via a global using so call sites are unchanged.
/// </summary>
public class FailedJobInfo
{
    public string JobName { get; set; } = "";
    public string JobId { get; set; } = "";

    /// <summary>Server-local time the failed run started (from run_date/run_time).</summary>
    public DateTime RunDateTime { get; set; }
    public int StepId { get; set; }
    public string StepName { get; set; } = "";
    public string Message { get; set; } = "";

    /// <summary>
    /// The monitored server's UTC offset in minutes, measured by
    /// <see cref="FailedJobsQuery.Sql"/> in the same statement that read
    /// <see cref="RunDateTime"/> — negative west of UTC. Null when the row came from somewhere that
    /// supplied no offset, which leaves <see cref="RunDateTime"/> unconvertible rather than assumed.
    /// <para>Measured on the server rather than read from collected <c>server_properties</c> because this
    /// feed is a live msdb query: the server is already on the other end of the connection, so its own
    /// clock answers at the same instant and for the same login, with no dependency on a collector having
    /// run.</para>
    /// </summary>
    public int? UtcOffsetMinutes { get; set; }

    /// <summary>
    /// <see cref="RunDateTime"/> in naive UTC, or null when <see cref="UtcOffsetMinutes"/> is absent. The
    /// frame the alert body and <c>alert_time</c> share.
    /// </summary>
    public DateTime? RunDateTimeUtc => AlertTimestamp.ToUtc(RunDateTime, UtcOffsetMinutes);

    /// <summary>
    /// The failure instant as an alert body renders it: UTC with a <c>Z</c>, or the server's own clock
    /// explicitly marked unconverted when no offset accompanied the row. Never a bare instant — see
    /// <see cref="AlertTimestamp"/> for why the marker rides on the value.
    /// </summary>
    public string RunDateTimeFormatted => AlertTimestamp.ForServerInstant(RunDateTime, UtcOffsetMinutes);
}
