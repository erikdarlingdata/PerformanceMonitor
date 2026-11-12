/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>#6979: one agent change listed in a report notice. <see cref="Object"/> is already masked by the caller.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1720:Identifier contains type name",
    Justification = "The notice names the thing a change touched 'Object', as the agent-changes journal does.")]
public sealed record ReportNoticeChange(string Tool, string Object, string Result);

/// <summary>
/// #6979: what one report notice says. <see cref="RootCauseFirstLine"/> is already masked, and null when the data detail
/// leaves the root cause out. <see cref="Changes"/> holds every change: the renderer shows at most 10 and counts the rest.
/// <see cref="BaseUrl"/> is the web dashboard's public base URL, empty for no links. The request id is a Guid, as in the store.
/// </summary>
public sealed record ReportNoticeContent(
    Guid RequestId, string Verdict, string ServerName, string Metric, string? RootCauseFirstLine,
    IReadOnlyList<ReportNoticeChange> Changes, string BaseUrl);

/// <summary>#6979: sends one report notice to one Slack or Teams destination. Null on success, else the error.</summary>
public interface IReportNoticeSends
{
    Task<string?> TrySendReportNoticeAsync(
        string channel, string destination, ReportNoticeContent content, CancellationToken cancellationToken = default);
}
