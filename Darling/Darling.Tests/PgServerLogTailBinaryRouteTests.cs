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
        const string expected = "\nWITH params AS (\n    SELECT CAST(@log_resume_file AS text) AS file,\n           CAST(@log_resume_offset AS bigint) AS off\n),\nlisting AS MATERIALIZED (\n    SELECT name, size, modification\n    FROM pg_catalog.pg_ls_logdir()\n    WHERE pg_catalog.current_setting('logging_collector') = 'on'\n      AND name !~* '\\.(csv|json)$'\n      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))\n),\nnewest AS (\n    SELECT name, size, modification\n    FROM listing\n    ORDER BY modification DESC, name DESC\n    LIMIT 1\n),\nmarked AS (\n    SELECT l.name, l.size, l.modification, p.off\n    FROM listing AS l\n    JOIN params AS p ON l.name = p.file\n),\nranges AS (\n    SELECT 1 AS part, m.name,\n           CASE WHEN m.size - m.off > 4194304 THEN m.size - 4194304 ELSE m.off END AS read_from,\n           greatest(m.size - 4194304 - m.off, 0) AS skipped_bytes\n    FROM marked AS m\n    JOIN newest AS nw ON m.name <> nw.name\n    WHERE m.size >= m.off\n    UNION ALL\n    SELECT 2, nw.name,\n           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - 4194304)\n                ELSE greatest(nw.size - 4194304, 0) END,\n           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - 4194304 - m.off, 0)\n                WHEN m.size >= m.off THEN greatest(nw.size - 4194304, 0)\n                ELSE 0 END\n    FROM newest AS nw\n    LEFT JOIN marked AS m ON true\n),\ntail AS (\n    SELECT n.part, n.name, n.read_from, n.skipped_bytes,\n           pg_catalog.pg_read_binary_file(\n               pg_catalog.current_setting('log_directory') || '/' || n.name,\n               n.read_from,\n               4194304) AS body\n    FROM ranges AS n\n),\nresume AS (\n    SELECT t.name,\n           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,\n           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m\n             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name\n               AND l.modification >= m.modification) AS skipped_files,\n           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,\n           CASE WHEN p.file IS NULL THEN ''\n                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'\n                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'\n                ELSE '' END AS fallback\n    FROM tail AS t\n    CROSS JOIN params AS p\n    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - 1048576, 0) AS cut) AS c\n    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(t.body, c.cut + 1), '\\x0a'::bytea) AS nl) AS s\n    WHERE t.part = 2\n)";

        static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Equal(expected, Lf(PgServerLogTail.TailCteBinarySql));

        /* And it differs from the text tailer in exactly the read function and the resume newline search, which
           looks at the bytea directly instead of converting the text back to bytes - never in the gates, the
           offsets, or the file selection, which would silently change what the binary route reads. */
        Assert.Equal(
            Lf(PgServerLogTail.TailCteSql)
                .Replace("pg_read_file", "pg_read_binary_file", StringComparison.Ordinal)
                .Replace("pg_catalog.convert_to(t.body, pg_catalog.current_setting('server_encoding'))", "t.body", StringComparison.Ordinal)
                .Replace("pg_catalog.substring(\n               t.body, ", "pg_catalog.substring(t.body, ", StringComparison.Ordinal),
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
