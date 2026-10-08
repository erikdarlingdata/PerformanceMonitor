/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Release walk V8, Darling side: the service's own statements against a monitored SQL Server carry the
/// <c>PerformanceMonitorLite</c> marker the collectors' self-exclusion reads, so they never top a Top Queries or Query Store list.
/// Darling runs the shared Query Store collector in its own shape (plan XML and query text fetched by id), which Lite's census
/// does not build, so the by-id fetches, the database probe and the connect-time detection query are checked here.
/// </summary>
public sealed class DarlingSelfQueryMarkerTests
{
    private static CollectorContext DarlingContext() => new()
    {
        ServerId = 7,
        ServerName = "example-sql-01",
        Deltas = new CollectorDeltaCalculator(),
        CollectionTime = DateTime.UtcNow,
        Target = new CollectorTargetInfo { SqlMajorVersion = 17 },
        HasCollectedBefore = true,
        CurrentDatabaseName = "SomeDatabase",
        CapturePlanXml = true,
        FetchQueryTextSeparately = true,
    };

    [Fact]
    public void TheDetectionQueryAndTheAlertJobReadsCarryTheMarker()
    {
        Assert.Contains(QueryStoreCollector.SelfQueryMarker, DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
        Assert.Contains(QueryStoreCollector.SelfQueryMarker, FailedJobsQuery.Sql, StringComparison.Ordinal);
        Assert.Contains(QueryStoreCollector.SelfQueryMarker, AgentJobStepQuery.BuildSql(2), StringComparison.Ordinal);
    }

    [Fact]
    public void EveryTopLevelSelectOfDarlingsQueryStoreReadsCarriesTheMarker()
    {
        var collector = QueryStoreCollector.Instance;
        var context = DarlingContext();
        var ids = new long[] { 1, 2, 3 };
        var texts = new[]
        {
            collector.BuildEnumerationQuery(context)!.Text,
            collector.BuildPerItemQuery("SomeDatabase", context).Text,
            collector.BuildPlanFetchByIdsQuery("SomeDatabase", context, ids, 1024 * 1024).Text,
            collector.BuildTextFetchByIdsQuery("SomeDatabase", context, ids, 1024 * 1024).Text,
        };

        var checkedSelects = 0;

        foreach (var text in texts)
        {
            /* A top-level statement starts in column 0; the sub-selects and CTE bodies are indented. */
            foreach (Match select in Regex.Matches(text, @"(?m)^SELECT\b(?<rest>[^\r\n]*)"))
            {
                checkedSelects++;
                Assert.Contains(QueryStoreCollector.SelfQueryMarker, select.Groups["rest"].Value, StringComparison.Ordinal);
            }
        }

        Assert.True(checkedSelects >= 8, $"only {checkedSelects} top-level SELECT statements were found");

        /* The probe's per-database statement is built into @sql and runs inside each database, one cached statement per database. */
        var probe = texts[0];
        var sqlAssign = probe.IndexOf("SET @sql = N'", StringComparison.Ordinal);
        Assert.True(sqlAssign > 0);
        Assert.Contains(QueryStoreCollector.SelfQueryMarker, probe.Substring(sqlAssign, 120), StringComparison.Ordinal);
    }
}
