/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9873: the statement-filter budget the evidence capture's public constructor gives each deadlock graph, blocked-process report and
/// stored plan. Every capture test hands in its own budget (a frozen clock, or one already spent), so these facts are what keep the
/// production default from going wrong unnoticed: a frozen clock would never withhold a slow document, a larger limit would hold the
/// capture on one, and a shared instance would charge one document's reading to the next.
/// </summary>
public sealed class InvestigationEvidenceDefaultBudgetTests
{
    [Fact]
    public void TheDefaultBudget_IsSpentOnlyOnceMoreThanTheReadBudgetOfTimeIsCharged()
    {
        var budget = InvestigationEvidenceCapture.DefaultJudgeBudget();

        Assert.Equal(SensitiveStatements.ReadBudget, budget.Limit);
        Assert.False(budget.Spent);
        budget.AddElapsed(SensitiveStatements.ReadBudget - TimeSpan.FromMilliseconds(1));
        Assert.False(budget.Spent);
        budget.AddElapsed(TimeSpan.FromMilliseconds(2));
        Assert.True(budget.Spent);
    }

    [Fact]
    public void TheDefaultBudget_RunsOnARealClock_SoReadingADocumentIsCharged()
    {
        var clock = InvestigationEvidenceCapture.DefaultJudgeBudget().Clock;
        var first = clock();

        Assert.True(SpinWait.SpinUntil(() => clock() > first, TimeSpan.FromSeconds(10)), "the default budget's clock never moved");
    }

    [Fact]
    public void TheDefaultBudget_IsNewForEachDocument_SoOneDocumentsReadingIsNotChargedToTheNext()
    {
        var first = InvestigationEvidenceCapture.DefaultJudgeBudget();
        var second = InvestigationEvidenceCapture.DefaultJudgeBudget();

        first.AddElapsed(TimeSpan.FromHours(1));

        Assert.NotSame(first, second);
        Assert.True(first.Spent);
        Assert.False(second.Spent);
    }
}
