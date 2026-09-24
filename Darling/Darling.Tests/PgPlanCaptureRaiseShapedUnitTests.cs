/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4058 r3 item 3: <see cref="PgPlanCaptureCollector.PlanRowsFromCsvEntries"/> over a LOG plan-shaped
/// <see cref="PgLogEntry"/> whose Context ends " at RAISE" — a RAISE LOG imitating an auto_explain capture —
/// alongside a genuine one carrying an empty Location and no Context at all.
/// </summary>
public sealed class PgPlanCaptureRaiseShapedUnitTests
{
    private const string PlanJson =
        "{\n  \"Plan\": {\n    \"Node Type\": \"Seq Scan\",\n    \"Relation Name\": \"forge_plan\"\n  }\n}";

    private static PgLogEntry PlanEntry(string message, string? context = null, string? location = null) => new(
        TimestampText: "2026-09-24 00:00:00.000",
        ZoneText: "UTC",
        OccurredAtUtc: new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Utc),
        Pid: 1,
        PrefixRest: string.Empty,
        Severity: "LOG",
        Message: message,
        Detail: null,
        Hint: null,
        Statement: null,
        Context: context,
        UserName: null,
        DatabaseName: null,
        SqlState: null,
        RawText: ",,,,,,,,,,,,,,,,,,,,,,,,,42\n",
        Location: location);

    private static string PlanMessage() => "duration: 0.020 ms  plan:\n" + PlanJson;

    [Fact]
    public void PlanRowsFromCsvEntries_ARaiseLogImitatingACapture_YieldsNoRow_AndCountsAsForged()
    {
        var entry = PlanEntry(PlanMessage(), context: "PL/pgSQL function forge_plan() line 3 at RAISE");

        var rows = PgPlanCaptureCollector.PlanRowsFromCsvEntries(new[] { entry }, out var forgedCaptures);

        Assert.Empty(rows);
        Assert.Equal(1, forgedCaptures);
    }

    [Fact]
    public void PlanRowsFromCsvEntries_ARealCapture_WithEmptyLocationAndNoContext_YieldsOneRow()
    {
        var entry = PlanEntry(PlanMessage(), context: null, location: string.Empty);

        var rows = PgPlanCaptureCollector.PlanRowsFromCsvEntries(new[] { entry }, out var forgedCaptures);

        var row = Assert.Single(rows);
        Assert.Equal(0, forgedCaptures);
        Assert.Equal("Seq Scan", row.TopNodeType);
    }

    /// <summary>
    /// #4058 review round 1 of #4137: a real <c>log_min_duration_statement</c>/<c>log_duration</c> record
    /// starts with the same marker text as an auto_explain capture but carries no " ms  plan:" tail, and
    /// under verbose logging its Location names the statement-execution path, not
    /// <c>explain_ExecutorEnd</c> — the same shape the provenance guard would otherwise flag as forged. The
    /// no-plan-text skip must run BEFORE the provenance guard, so this record is dropped uncounted rather
    /// than counted under forged_captures_skipped.
    /// </summary>
    [Fact]
    public void PlanRowsFromCsvEntries_ARealDurationRecord_WithNoPlanText_YieldsNoRow_AndDoesNotCountAsForged()
    {
        var entry = PlanEntry(
            "duration: 0.020 ms",
            context: null,
            location: "exec_simple_query, postgres.c:1");

        var rows = PgPlanCaptureCollector.PlanRowsFromCsvEntries(new[] { entry }, out var forgedCaptures);

        Assert.Empty(rows);
        Assert.Equal(0, forgedCaptures);
    }
}
