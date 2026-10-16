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
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Practice requests and the scores they earn an agent (#5918), in the Investigations window.</summary>
public sealed partial class ViewerDataService
{
    /// <summary>The longest practice-score window, in days; a longer one is refused, not shortened.</summary>
    public const int InvestigationPracticeMaxDays = 365;

    /// <summary>
    /// Opens a practice request replaying the judged case <paramref name="sourceRequestId"/> (a request with a report and
    /// feedback), journaled as opened by the signed-in person. The result's outcome and message say why a case was not opened
    /// (unknown, already a practice request, no report, no feedback, or a live practice request exists). A read-only seat's
    /// 42501 is <see cref="ViewerReadOnlyException"/>; a store below V191 is a <see cref="ViewerSchemaSkewException"/>.
    /// </summary>
    public async Task<InvestigationPracticeResult> OpenInvestigationPracticeAsync(
        Guid sourceRequestId, string login, CancellationToken cancellationToken = default)
    {
        if (ValidateInvestigationLogin(login) is { } problem)
        {
            throw new ArgumentException(problem, nameof(login));
        }

        var store = new InvestigationPracticeStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
        try
        {
            return await SkewAsync(() => store.OpenAsync(sourceRequestId, InvestigationPersonActor(login), cancellationToken));
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    /// <summary>The score per agent over the practice requests opened in the last <paramref name="daysBack"/> days (1 to
    /// <see cref="InvestigationPracticeMaxDays"/>; outside that is an <see cref="ArgumentOutOfRangeException"/>). Advisory.</summary>
    public async Task<IReadOnlyList<InvestigationPracticeScore>> GetInvestigationPracticeScoresAsync(
        int daysBack, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(daysBack, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(daysBack, InvestigationPracticeMaxDays);
        var store = new InvestigationPracticeStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
        return await SkewAsync(() => store.ReadScoresAsync(DateTime.UtcNow.AddDays(-daysBack), cancellationToken));
    }
}
