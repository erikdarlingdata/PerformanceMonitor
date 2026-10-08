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
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every SQL Server collector's own statements carry the self-query marker (#1007 click-through, F14).
///
/// <para><b>Why a census.</b> Top Queries, Query Store and the procedure grids leave out statements whose
/// text contains <c>PerformanceMonitorLite</c>, so the collectors do not fill the grids with Lite watching
/// itself. The plan-correction collector shipped without the marker, and on a quiet server the top rows of
/// Top Queries by Duration and Query Store by Duration were its own <c>sys.dm_db_tuning_recommendations</c>
/// statement, one row per database. A collector added later without the marker would do it again, so this
/// builds every query every SQL Server collector can send, over the target shapes that change the SQL, and
/// fails on any that lacks the marker.</para>
/// </summary>
[Trait("Stage", "Guard")]
public class CollectorSelfQueryMarkerCensusTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static readonly CollectorTargetInfo[] s_targets =
    {
        new() { SqlMajorVersion = 13 },
        new() { SqlMajorVersion = 15 },
        new() { SqlMajorVersion = 17 },
        new() { SqlMajorVersion = 16, IsAzureSqlDb = true },
        new() { SqlMajorVersion = 16, IsAzureManagedInstance = true },
        new() { SqlMajorVersion = 15, IsAwsRds = true },
    };

    private static CollectorContext Context(CollectorTargetInfo target, bool collectedBefore) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = DateTime.UtcNow,
        Deltas = s_deltas,
        Target = target,
        HasCollectedBefore = collectedBefore,
        CurrentDatabaseName = "SomeDatabase",
        LongQuerySessionName = LongQueryCompletionsCollector.XeSessionNameFor("Darling", "0a1b2c3d"),
    };

    private static IEnumerable<(string Where, CollectorQuery Query)> QueriesOf(ICollectorSchemaInfo schema, List<string> unbuildable)
    {
        var type = schema.GetType();

        foreach (var target in s_targets)
        {
            if (!CollectorCatalog.EngineMatches(schema, target))
            {
                continue;
            }

            foreach (var collectedBefore in new[] { false, true })
            {
                foreach (var name in new[] { "BuildQuery", "BuildSupplementalQuery", "BuildEnumerationQuery", "BuildEnumerationProbe", "BuildPerItemQuery" })
                {
                    var method = type.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                        .FirstOrDefault(m => m.Name == name);

                    if (method is null)
                    {
                        continue;
                    }

                    var args = method.GetParameters().Length == 2
                        ? new object?[] { "SomeDatabase", Context(target, collectedBefore) }
                        : new object?[] { Context(target, collectedBefore) };

                    object? result;

                    try
                    {
                        result = method.Invoke(schema, args);
                    }
                    catch (TargetInvocationException ex) when (ex.InnerException is NotSupportedException)
                    {
                        /* The collector says this entry point is not how it runs on this target (it enumerates
                           databases and BuildEnumerationQuery drives it, or it has no per-item query). The
                           per-collector floor below proves every collector still built at least one query. */
                        continue;
                    }
                    catch (TargetInvocationException ex)
                    {
                        unbuildable.Add($"{schema.Name}.{name} (v{target.SqlMajorVersion}): {ex.InnerException?.GetType().Name}: {ex.InnerException?.Message}");
                        continue;
                    }

                    if (result is CollectorQuery q)
                    {
                        yield return ($"{schema.Name}.{name} (v{target.SqlMajorVersion} azure={target.IsAzureSqlDb} mi={target.IsAzureManagedInstance} rds={target.IsAwsRds} before={collectedBefore})", q);
                    }
                }
            }
        }
    }

    [Fact]
    public void EverySqlServerCollectorQueryCarriesTheSelfQueryMarker()
    {
        var missing = new SortedSet<string>(StringComparer.Ordinal);
        var unbuildable = new List<string>();
        var checkedQueries = 0;
        var silent = new List<string>();

        foreach (var schema in CollectorCatalog.All)
        {
            if (schema.TargetEngine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            var built = 0;

            foreach (var (where, query) in QueriesOf(schema, unbuildable))
            {
                checkedQueries++;
                built++;

                if (!query.Text.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal))
                {
                    missing.Add(where.Split(' ')[0]);
                }
            }

            if (built == 0)
            {
                silent.Add(schema.Name);
            }
        }

        Assert.True(checkedQueries > 40, $"only {checkedQueries} queries were built; the census is not reaching the collectors");
        Assert.True(silent.Count == 0, "collectors that built no query at all: " + string.Join(", ", silent));
        Assert.True(unbuildable.Count == 0, "queries that could not be built:\n" + string.Join("\n", unbuildable.Take(20)));
        Assert.True(missing.Count == 0, "collector queries without the " + QueryStoreCollector.SelfQueryMarker + " marker:\n" + string.Join("\n", missing));
    }
}
