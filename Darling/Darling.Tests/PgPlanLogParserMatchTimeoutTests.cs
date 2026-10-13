/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every pattern <see cref="PgPlanLogParser"/> holds runs under a match timeout, and a plan whose masking runs out
/// of time is skipped on its own: it is never stored with its text, and it does not fail the plans around it.
/// </summary>
[Collection("timing")]
public sealed class PgPlanLogParserMatchTimeoutTests
{
    private static string PlanWithFilter(string filter) =>
        "{\n  \"Plan\": {\n    \"Node Type\": \"Seq Scan\",\n    \"Relation Name\": \"t\",\n    \"Filter\": \""
        + filter + "\"\n  }\n}";

    private static string LongFilter() => string.Concat(Enumerable.Repeat("(a = 'Kept-5698') AND ", 3_000));

    private static T WithForcedTimeout<T>(Func<T> body)
    {
        var prior = PgSettingRedactor.MatchTimeoutForTest;
        PgSettingRedactor.MatchTimeoutForTest = new TimeSpan(1);
        try
        {
            return body();
        }
        finally
        {
            PgSettingRedactor.MatchTimeoutForTest = prior;
        }
    }

    [Fact]
    public void EveryStaticPatternInThePlanParser_RunsUnderAMatchTimeout()
    {
        var regexes = new List<Regex>();
        foreach (var field in typeof(PgPlanLogParser).GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
        {
            regexes.AddRange(RegexesIn(field.GetValue(null)));
        }

        // The plan-block pattern, the prefix-escape pattern, the fingerprint pair and the stored-text pattern.
        Assert.True(regexes.Count >= 5, $"found {regexes.Count} patterns");
        foreach (var regex in regexes)
        {
            Assert.NotEqual(Regex.InfiniteMatchTimeout, regex.MatchTimeout);
            Assert.True(regex.MatchTimeout <= TimeSpan.FromSeconds(5), $"{regex.MatchTimeout}: {regex}");
        }
    }

    [Fact]
    public void APlanWhoseMaskingRunsOutOfTime_IsSkipped_AndTheNextPlanStillParses()
    {
        var slow = PlanWithFilter(LongFilter());
        var next = PlanWithFilter("(b = 1)");

        // Read normally, the plan parses and its literal is masked.
        var normal = PgPlanLogParser.FromBlock(1, 1, slow);
        Assert.NotNull(normal);
        Assert.DoesNotContain("Kept-5698", normal!.Value.PlanJson, StringComparison.Ordinal);

        var skipped = WithForcedTimeout(() => PgPlanLogParser.FromBlock(1, 1, slow));
        Assert.Null(skipped);

        Assert.NotNull(PgPlanLogParser.FromBlock(2, 1, next));
    }

    [Fact]
    public void ExtractedLogBody_WithAPlanWhoseMaskingRunsOutOfTime_ReturnsWithoutThrowing_AndStoresNoText()
    {
        var body = "2026-10-12 00:00:00.000 UTC [10] 42 LOG:  duration: 1.000 ms  plan:\n"
            + string.Join("\n", PlanWithFilter(LongFilter()).Split('\n').Select(line => "\t" + line)) + "\n";

        Assert.Single(PgPlanLogParser.Extract(body));

        var plans = WithForcedTimeout(() => PgPlanLogParser.Extract(body));

        Assert.Empty(plans);
    }

    [Fact]
    public void PlanRowsFromCsvEntries_APlanWhoseMaskingRunsOutOfTime_YieldsNoRow_AndDoesNotThrow()
    {
        var entry = new PgLogEntry(
            TimestampText: "2026-10-12 00:00:00.000",
            ZoneText: "UTC",
            OccurredAtUtc: new DateTime(2026, 10, 12, 0, 0, 0, DateTimeKind.Utc),
            Pid: 1,
            PrefixRest: string.Empty,
            Severity: "LOG",
            Message: "duration: 0.020 ms  plan:\n" + PlanWithFilter(LongFilter()),
            Detail: null,
            Hint: null,
            Statement: null,
            Context: null,
            UserName: null,
            DatabaseName: null,
            SqlState: null,
            RawText: ",,,,,,,,,,,,,,,,,,,,,,,,,42\n",
            Location: string.Empty,
            QueryIdText: "42");

        Assert.Single(PgPlanCaptureCollector.PlanRowsFromCsvEntries(new[] { entry }, out _));

        var rows = WithForcedTimeout(() => PgPlanCaptureCollector.PlanRowsFromCsvEntries(new[] { entry }, out _));

        Assert.Empty(rows);
    }

    private static IEnumerable<Regex> RegexesIn(object? value)
    {
        switch (value)
        {
            case null or string:
                yield break;
            case Regex regex:
                yield return regex;
                yield break;
            case IEnumerable items:
                foreach (var item in items)
                {
                    foreach (var found in RegexesIn(item))
                    {
                        yield return found;
                    }
                }

                yield break;
        }

        if (value.GetType().DeclaringType != typeof(PgSettingRedactor))
        {
            yield break;
        }

        foreach (var field in value.GetType().GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            foreach (var found in RegexesIn(field.GetValue(value)))
            {
                yield return found;
            }
        }
    }
}
