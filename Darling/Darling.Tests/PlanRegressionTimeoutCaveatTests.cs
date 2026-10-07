/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448: the daily-totals read must not turn a PLAN_REGRESSION timeout into silence. The path a timeout takes is
/// <c>ReportCollectionFailure</c> to <c>AnalysisContext.CollectionFailures</c> (family <c>plan_regression</c>, outcome
/// <c>timeout</c>), then the service's caveat store (<c>collect.analysis_collection_caveats</c>, read by
/// <c>get_collection_health</c>'s <c>stored_caveats</c> and the viewer's grid) and the live tools' own
/// <c>collection_caveats</c> block. These pins hold the ends of that path for this family without a store: every command
/// the fact runs carries the 60 s deadline and the fact's catch reports, and a timeout recorded under the fact's method
/// name describes and serializes as <c>plan_regression (timeout)</c> and maps to the stored reason <c>timeout</c>.
/// </summary>
public sealed class PlanRegressionTimeoutCaveatTests
{
    [Fact]
    public void EveryCommandThePlanRegressionFactRuns_CarriesTheFactDeadline_AndItsCatchReports()
    {
        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs"));

        var fact = Slice(source, "private async Task CollectPlanRegressionFactsAsync(", "public const string ProcedureStatsSql");
        var builtDays = Slice(source, "private async Task<List<DateOnly>> ReadPlanRegressionBuiltDaysAsync(", "internal const string PlanRegressionBuiltDaysSql");

        /* The fact's own read and the built-days read: each NpgsqlCommand is built with the deadline. */
        Assert.Single(Regex.Matches(fact, @"new NpgsqlCommand\("));
        Assert.Single(Regex.Matches(fact, @"CommandTimeout = FactCommandTimeoutSeconds"));
        Assert.Single(Regex.Matches(builtDays, @"new NpgsqlCommand\("));
        Assert.Single(Regex.Matches(builtDays, @"CommandTimeout = FactCommandTimeoutSeconds"));

        /* The fact's swallowing catch reports, and so does the built-days read's, under the fact's own name, so a timeout in
           either is a plan_regression caveat; the built-days read then answers "none built", which is the exact-bound read,
           and it does not swallow a cancelled pass. */
        Assert.Contains("ReportCollectionFailure(ex, context);", fact, StringComparison.Ordinal);
        Assert.Contains("when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))", builtDays, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context, nameof(CollectPlanRegressionFactsAsync));", builtDays, StringComparison.Ordinal);
    }

    [Fact]
    public void ATimeoutRecordedForThePlanRegressionFact_DescribesAndSerializesAsATimeoutOfThatFamily()
    {
        var timeout = new NpgsqlException("Exception while reading from stream", new TimeoutException());
        var context = new AnalysisContext { ServerId = 1, ServerName = "s" };

        context.RecordCollectionFailure(
            CollectionFailure.FamilyOf("CollectPlanRegressionFactsAsync"), "CollectPlanRegressionFactsAsync",
            PgFactCollector.ClassifyOutcome(timeout), timeout);

        var failure = Assert.Single(context.CollectionFailures);
        Assert.Equal("plan_regression", failure.Family);
        Assert.Equal(CollectionFailureOutcome.Timeout, failure.Outcome);

        var state = CollectionCaveatState.From(context);
        Assert.Contains("plan_regression (timeout)", state.Describe(), StringComparison.Ordinal);

        var payload = JsonSerializer.SerializeToElement(
            state.Attach(new { facts = 0 }, new JsonSerializerOptions()));
        var entry = payload.GetProperty("collection_caveats").GetProperty("entries").EnumerateArray().Single();
        Assert.Equal("plan_regression", entry.GetProperty("family").GetString());
        Assert.Equal("timeout", entry.GetProperty("outcome").GetString());

        /* What the pass hands the caveat store, the reason the stored caveat (and get_collection_health) shows. */
        Assert.Equal("timeout", CollectionFailure.Label(failure.Outcome));
    }

    private static string Slice(string source, string from, string to)
    {
        var start = source.IndexOf(from, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{from}' is no longer in PgFactCollector.QueryPerf.cs");
        var end = source.IndexOf(to, start, StringComparison.Ordinal);
        Assert.True(end > start, $"'{to}' no longer follows '{from}'");
        return source[start..end];
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
