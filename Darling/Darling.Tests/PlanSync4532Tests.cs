/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4532 — SQL Server caps a statement's copy of its text at about 4,000 characters when it
/// writes showplan XML. Rule 3's MAXDOP message already checked the length, but nothing else
/// said the text was short. Ported from erikdarlingdata/PerformanceStudio@35249e2's rule 39 (the
/// full-text-recovery half of that commit, which needs a captured query buffer PM has no
/// equivalent for, is not part of this port).
/// </summary>
public sealed class PlanSync4532Tests
{
    private static ParsedPlan Analyze(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static bool Has(PlanStatement stmt, string warningType) =>
        stmt.PlanWarnings.Any(w => w.WarningType == warningType);

    [Fact]
    public void IsTextTruncated_TrueAtTheThreshold()
    {
        var stmt = new PlanStatement { StatementText = new string('a', PlanStatement.TruncationLengthThreshold) };
        Assert.True(stmt.IsTextTruncated);
    }

    [Fact]
    public void IsTextTruncated_FalseOneShortOfTheThreshold()
    {
        var stmt = new PlanStatement { StatementText = new string('a', PlanStatement.TruncationLengthThreshold - 1) };
        Assert.False(stmt.IsTextTruncated);
    }

    [Fact]
    public void Rule39_FlagsAStatementThatHitTheCap()
    {
        var stmt = new PlanStatement { StatementText = new string('a', 3995) };
        var plan = Analyze(stmt);
        var result = plan.Batches[0].Statements[0];

        var warning = Assert.Single(result.PlanWarnings.Where(w => w.WarningType == "Truncated Query Text"));
        Assert.Equal(PlanWarningSeverity.Info, warning.Severity);
        Assert.Contains("4,000 characters", warning.Message);
    }

    [Fact]
    public void Rule39_SaysNothingAboutAQueryThatFits()
    {
        var stmt = new PlanStatement { StatementText = new string('a', 3000) };
        var plan = Analyze(stmt);
        var result = plan.Batches[0].Statements[0];

        Assert.False(Has(result, "Truncated Query Text"));
    }
}
