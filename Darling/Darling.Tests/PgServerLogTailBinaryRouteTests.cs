/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The binary-route selection (#4046 part 1c) for the three collectors that share
/// <see cref="PgServerLogTail"/>: <see cref="CollectorContext.PgReadBinaryFileGranted"/> chooses between the
/// text route (<c>pg_read_file</c>, today's byte-for-byte pin lives in
/// <c>PgLogEventsPipelineTests.TheTailerExtraction_LeftBothSiblingsSqlByteIdentical</c>) and the binary route
/// (<c>pg_read_binary_file</c>), pinned here as its own byte-for-byte constant the same way
/// <see cref="PgServerLogTail.TailCteSql"/> already is.
/// </summary>
public sealed class PgServerLogTailBinaryRouteTests
{
    private static CollectorContext TestContext(bool granted) => new()
    {
        LogHashKey = TestLogHashKeys.Fixed,
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 18, 3, 10, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        PgReadBinaryFileGranted = granted,
    };

    /// <summary>
    /// The tailer's binary twin, pinned byte-for-byte the same way <c>TailCteSql</c> is — the same file, the
    /// same offsets, the same gates, <c>pg_read_binary_file</c> in place of <c>pg_read_file</c>.
    /// </summary>
    [Fact]
    public void TailCteBinarySql_IsByteForByteTheTextTailerWithTheBinaryFunctionSwapped()
    {
        const string expected = "\nWITH newest AS (\n    SELECT name, size\n    FROM pg_catalog.pg_ls_logdir()\n    WHERE pg_catalog.current_setting('logging_collector') = 'on'\n      AND name !~* '\\.(csv|json)$'\n      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))\n    ORDER BY modification DESC\n    LIMIT 1\n),\ntail AS (\n    SELECT pg_catalog.pg_read_binary_file(\n               pg_catalog.current_setting('log_directory') || '/' || n.name,\n               greatest(n.size - 4194304, 0),\n               4194304) AS body\n    FROM newest AS n\n)";

        static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Equal(expected, Lf(PgServerLogTail.TailCteBinarySql));

        /* And it differs from the text tailer in exactly the one function name - never in the gates, the
           offsets, or the file selection, which would silently change what the binary route reads. */
        Assert.Equal(
            Lf(PgServerLogTail.TailCteSql).Replace("pg_read_file", "pg_read_binary_file", StringComparison.Ordinal),
            Lf(PgServerLogTail.TailCteBinarySql));
    }

    [Theory]
    [InlineData("pg_log_events")]
    [InlineData("pg_deadlocks")]
    [InlineData("pg_plan_capture")]
    public void Granted_SendsTheBinaryQueryText(string collectorName)
    {
        var text = BuildQueryFor(collectorName, granted: true);

        Assert.Contains("pg_read_binary_file(", text, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_read_file(", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pg_log_events")]
    [InlineData("pg_deadlocks")]
    [InlineData("pg_plan_capture")]
    public void Ungranted_KeepsTheTextRoute(string collectorName)
    {
        var text = BuildQueryFor(collectorName, granted: false);

        Assert.Contains("pg_read_file(", text, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_read_binary_file(", text, StringComparison.Ordinal);
    }

    private static string BuildQueryFor(string collectorName, bool granted) => collectorName switch
    {
        "pg_log_events" => PgLogEventsCollector.Instance.BuildQuery(TestContext(granted)).Text,
        "pg_deadlocks" => PgDeadlocksCollector.Instance.BuildQuery(TestContext(granted)).Text,
        "pg_plan_capture" => PgPlanCaptureCollector.Instance.BuildQuery(TestContext(granted)).Text,
        _ => throw new ArgumentOutOfRangeException(nameof(collectorName)),
    };
}
