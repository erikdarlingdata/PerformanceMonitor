/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>Builds one rendered Daily Health Report (#7041) for the 24 hours that end at <paramref name="periodEndUtc"/>.</summary>
internal interface IDailyHealthReportSource
{
    Task<DailyHealthReportRendered> BuildAsync(DateTime periodEndUtc, CancellationToken ct);
}

/// <summary>One rendered report: the email subject, the short text for Slack, Teams and the webhook, and the HTML for email.</summary>
internal sealed record DailyHealthReportRendered(string Subject, string Text, string Html, int ServerCount);
