/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// No collector filters Extended Events rows by time with a <c>.value()</c> cast in a WHERE or AND clause.
/// <c>WHERE evt.value('(@timestamp)[1]', 'datetime2') &gt; @cutoff_time</c> shreds and converts every event's
/// timestamp before the filter can discard a single row. The filter belongs inside the XQuery, where the
/// comparison runs without the cast: <c>evt.exist('@timestamp[. &gt; sql:variable("@cutoff_time")]') = 1</c>,
/// the form DarlingData's sp_HumanEventsBlockViewer uses.
///
/// <para>The sweep builds every SQL Server collector's statements for each target shape, with and without
/// plan capture, so a filter assembled from fragments is caught as well as one written out whole.</para>
/// </summary>
public sealed class XeTimeFilterSweepTests
{
    /* WHERE/AND/OR, then a .value() call on the event's own timestamp attribute, then a comparison. Covers
       (@timestamp)[1], a bare @timestamp, and the one-event document's (/event/@timestamp)[1]. */
    private static readonly Regex ValueTimeFilter = new(
        @"\b(?:WHERE|AND|OR)\s+[\w.]*\.value\(\s*N?'\(?\s*/?(?:event/)?@timestamp\s*\)?(?:\[1\])?'\s*,\s*N?'[^']*'\s*\)\s*(?:[<>]=?|<>|!=|=|BETWEEN\b)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static readonly CollectorTargetInfo[] s_targets =
    [
        new CollectorTargetInfo { SqlMajorVersion = 16 },
        new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 },
        new CollectorTargetInfo { IsAzureManagedInstance = true, SqlMajorVersion = 16 },
        new CollectorTargetInfo { IsAwsRds = true, SqlMajorVersion = 15 },
    ];

    private static readonly string[] s_xeCollectors =
    [
        BlockedProcessReportCollector.Instance.Name,
        DeadlocksCollector.Instance.Name,
        LongQueryCompletionsCollector.Instance.Name,
        SystemHealthEventsCollector.Instance.Name,
    ];

    [Fact]
    public void NoCollectorFiltersEventsByTimeWithValue()
    {
        var offenders = new List<string>();
        var built = new HashSet<string>(StringComparer.Ordinal);
        var sqlServer = CollectorCatalog.All.Where(s => s.TargetEngine == CollectorTargetEngine.SqlServer).ToList();

        foreach (var schema in sqlServer)
        {
            foreach (var target in s_targets)
            {
                foreach (var capturePlanXml in new[] { false, true })
                {
                    foreach (var (builder, text) in Statements(schema, target, capturePlanXml))
                    {
                        built.Add(schema.Name);

                        foreach (Match match in ValueTimeFilter.Matches(text))
                        {
                            offenders.Add($"{schema.Name}.{builder}: {Regex.Replace(match.Value, @"\s+", " ")}");
                        }
                    }
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            "Collector SQL filters Extended Events rows by time with a .value() cast, which shreds every event "
            + "before the filter runs. Use evt.exist('@timestamp[. > sql:variable(\"@cutoff_time\")]') = 1 instead: "
            + string.Join(" | ", offenders.Distinct(StringComparer.Ordinal)));

        /* The sweep is only worth having if it reached the Extended Events collectors and most of the catalog. */
        foreach (var name in s_xeCollectors)
        {
            Assert.Contains(name, built);
        }

        Assert.True(
            built.Count > sqlServer.Count * 3 / 4,
            $"Only {built.Count} of {sqlServer.Count} SQL Server collectors produced SQL, so this sweep covers far "
            + "less than it appears to.");
    }

    [Fact]
    public void ThePatternCatchesEveryShapeThisSweepRetired()
    {
        Assert.Matches(ValueTimeFilter, "WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time");
        Assert.Matches(ValueTimeFilter, "AND   tel.evt.value('(/event/@timestamp)[1]', 'datetime2') > @cutoff_time");
        Assert.Matches(ValueTimeFilter, "where evt.value('@timestamp', 'datetime2') >= @cutoff_time");
        Assert.DoesNotMatch(ValueTimeFilter, "WHERE evt.exist('@timestamp[. > sql:variable(\"@cutoff_time\")]') = 1");
        Assert.DoesNotMatch(ValueTimeFilter, "deadlock_time = evt.value('(@timestamp)[1]', 'datetime2'),");
    }

    private static IEnumerable<(string Builder, string Text)> Statements(ICollectorSchemaInfo schema, CollectorTargetInfo target, bool capturePlanXml)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "xe-time-filter-sweep",
            CollectionTime = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = target,
            CapturePlanXml = capturePlanXml,
        };
        var type = schema.GetType();

        foreach (var builder in new[] { "BuildQuery", "BuildSupplementalQuery", "BuildEnumerationQuery", "BuildEnumerationProbe" })
        {
            var text = TextOf(type.GetMethod(builder, [typeof(CollectorContext)]), schema, context);

            if (text is not null)
            {
                yield return (builder, text);
            }
        }

        var perItem = TextOf(type.GetMethod("BuildPerItemQuery", [typeof(string), typeof(CollectorContext)]), schema, "sweep_db", context);

        if (perItem is not null)
        {
            yield return ("BuildPerItemQuery", perItem);
        }
    }

    /* A builder that does not apply to this target shape, or does not enumerate, throws; that is not SQL to sweep. */
    private static string? TextOf(MethodInfo? method, object schema, params object[] args)
    {
        if (method is null)
        {
            return null;
        }

        try
        {
            return (method.Invoke(schema, args) as CollectorQuery)?.Text;
        }
        catch (TargetInvocationException)
        {
            return null;
        }
    }
}
