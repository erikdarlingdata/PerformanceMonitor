/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V76 per-database Query Store health collector (#2319) — the instrument #2312's investigation was
/// missing: <c>database_config</c>'s single <c>is_query_store_on</c> bit cannot say whether Query Store
/// actually works (desired READ_WRITE with actual READ_ONLY after the cap hit is the classic silent
/// failure) or how close to its cap it sits. These facts pin the rung's place on the ladder, the viewer
/// probe, the enumeration SQL's load-bearing filters, the per-item query's identity-quoting and honesty
/// contract, and the schedule decision.
/// </summary>
public sealed class QueryStoreHealthStoreTests
{
    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* #2312 added V77, so this rung is no longer the top — the "I am the top" claim moves to the
           newest rung's own test (ActivityDrivenPlanFetchStoreTests) and this one keeps the invariants
           that stay true forever: the rung is PRESENT, the ladder is ordered and dense, and the build's
           schema version tracks the maximum. */
        Assert.Contains(76, versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        /* Dense above the one sanctioned historical hole at V45. */
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);

        Assert.Equal("query-store-health", PgMigrations.Scripts.Single(s => s.Version == 76).Name);
    }

    /* ---------------- the viewer probe ---------------- */

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreTo76()
    {
        /* #2312: no longer the top (that claim lives in ActivityDrivenPlanFetchStoreTests) — this fact
           keeps pinning that a store at exactly 76 maps to 76 and one at 75 maps to 75, forever. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        /* 51 positional sentinels then the V76 one by name — the map takes 52 parameters. Present => 76,
           newest-first; absent => the previous arm still answers 75 rather than falling through. */
        var all = Enumerable.Repeat(true, 51).Cast<object>().ToArray();

        Assert.Equal(76, InvokeMap(all, hasQueryStoreHealth: true));
        Assert.Equal(75, InvokeMap(all, hasQueryStoreHealth: false));
    }

    [Fact]
    public void TheProbeAsksForTheTable_AndTheThreePlacesAgree()
    {
        Assert.Contains("table_name = 'query_store_health'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var mapParameters = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetParameters().Length;

        var viewerSource = ReadViewerSource();

        /* The reader must hand over exactly one argument per map parameter: ordinals are 0-based, so the
           highest is Count - 1, and the next one up must NOT appear. */
        Assert.Contains($"reader.GetBoolean({mapParameters - 1})", viewerSource, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({mapParameters})", viewerSource, StringComparison.Ordinal);
    }

    /* ---------------- the definition ---------------- */

    /// <summary>
    /// The enumeration list's load-bearing filters, each of which cost real rounds elsewhere:
    /// HAS_DBACCESS self-skip (#1823 — a least-privilege login without per-db access raised 916 per db
    /// per cycle), the AG filter (a readable-secondary's databases answer for the primary's identity),
    /// ONLINE only (a RESTORING database's catalog views are unreachable), and the house RECOMPILE. And
    /// since #3764 the target that does NOT enumerate: Azure SQL DB, where the three-part reference the
    /// per-item query nests is rejected for every database but the connection's own, so the host connects
    /// per database there and the definition says so with a null enumeration.
    /// </summary>
    [Fact]
    public void TheEnumerationCarriesTheLoadBearingFilters()
    {
        var context = TestContext(isAzure: false);
        var query = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(context)!;

        Assert.Contains("HAS_DBACCESS(d.name) = 1", query.Text, StringComparison.Ordinal);
        Assert.Contains("drs.is_primary_replica = 1", query.Text, StringComparison.Ordinal);
        Assert.Contains("d.state_desc = N'ONLINE'", query.Text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);

        /* #3764: Azure SQL DB does not enumerate at all. The master-side Azure list this arm used to pin fed
           EXECUTE [db].sys.sp_executesql, which Azure rejects for every database that is not the
           connection's own — so on a logical-server registration every item failed and the collector stored
           nothing while reading HEALTHY. The host now connects per database there (RunsPerDatabase, the
           database_scoped_config #3755 shape), the definition returns null from its side, and the dead
           Azure list query is deleted rather than left reachable. The definition-level pins live in
           Lite.Tests/QueryStoreHealthCollectorDefinitionTests. */
        var azureTarget = TestContext(isAzure: true);
        Assert.True(QueryStoreHealthCollector.Instance.RunsPerDatabase(azureTarget.Target));
        Assert.Null(QueryStoreHealthCollector.Instance.BuildEnumerationQuery(azureTarget));
    }

    /// <summary>A database named with a closing bracket must not escape its identifier — the same
    /// quote-doubling every sibling per-database collector carries.</summary>
    [Fact]
    public void ThePerItemQueryDoublesClosingBrackets()
    {
        var query = QueryStoreHealthCollector.Instance.BuildPerItemQuery("we]ird", TestContext(isAzure: false));

        Assert.Contains("EXECUTE [we]]ird].sys.sp_executesql", query.Text, StringComparison.Ordinal);
        Assert.Contains("sys.database_query_store_options", query.Text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", query.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The honesty contract: the database list is deliberately NOT filtered to is_query_store_on — the
    /// options view answers one row even for a QS-off database, so OFF is recorded as OFF and an absent
    /// row can only mean "not collected". Filtering the list would make those two states identical.
    /// </summary>
    [Fact]
    public void TheEnumerationDoesNotFilterToQueryStoreOn()
    {
        var query = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(TestContext(isAzure: false))!;

        Assert.DoesNotContain("is_query_store_on", query.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Hourly, NOT the config family's on-load cadence: actual_state, readonly_reason and the storage
    /// numbers change BY THEMSELVES, and the cap-hit READ_ONLY transition is the point of collecting
    /// this — an on-load snapshot would miss it until the next reconnect.
    /// </summary>
    [Fact]
    public void TheScheduleIsHourlyWithConfigFamilyRetention()
    {
        var schedule = CollectorScheduleDefaults.All["query_store_health"];

        Assert.Equal(60, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
    }

    /// <summary>
    /// The collector gates on 2016+ (review catch: sys.database_query_store_options does not exist
    /// before v13, so an ungated pre-2016 target would error once per database per hour) — the same
    /// condition QueryStoreCollector carries, so Lite and Darling skip identically.
    /// </summary>
    [Theory]
    [InlineData(11, false)]   /* 2012 — no Query Store catalog */
    [InlineData(12, false)]   /* 2014 — no Query Store catalog */
    [InlineData(13, true)]    /* 2016 — Query Store ships */
    [InlineData(16, true)]
    [InlineData(0, true)]     /* version unknown = assume newest */
    public void TheCollectorGatesOnQueryStoresExistence(int majorVersion, bool applies)
        => Assert.Equal(applies, QueryStoreHealthCollector.Instance.AppliesTo(
            new CollectorTargetInfo { SqlMajorVersion = majorVersion }));

    [Fact]
    public void AzureAlwaysApplies()
    {
        Assert.True(QueryStoreHealthCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 11, IsAzureSqlDb = true }));
        Assert.True(QueryStoreHealthCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 11, IsAzureManagedInstance = true }));
    }

    /// <summary>The claim this pin used to make — "every selected column exists from 2016 on, no per-column
    /// gates" — was revisited by V137 (#3796), exactly as it asked: the payload is now twelve columns, the
    /// original ten ungated and the two capture modes appended last, and ONE of them
    /// (<c>wait_stats_capture_mode</c>, 2017+) is gated by <c>QueryStoreHealthCollector.HasWaitStatsCaptureMode</c>
    /// in the <c>DatabaseConfigCollector</c> idiom. The gate itself is pinned in
    /// <c>Lite.Tests/QueryStoreHealthCollectorDefinitionTests</c> and the rung in
    /// <c>QsCaptureModeRouteKnobToastRungTests</c>; what stays here is the order, so a reorder cannot slip past
    /// the positional COPY writer.</summary>
    [Fact]
    public void ThePayloadIsOrdered_WithTheOneGatedColumnLast()
    {
        var columns = QueryStoreHealthCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(new[]
        {
            "database_name", "actual_state", "desired_state", "readonly_reason",
            "current_storage_size_mb", "max_storage_size_mb", "size_based_cleanup_mode",
            "stale_query_threshold_days", "max_plans_per_query", "interval_length_minutes",
            "query_capture_mode", "wait_stats_capture_mode",
        }, columns);

        /* The gated column is the LAST one, so a 2016 target's ten-ordinal reader and a 2017+ target's eleven
           differ only at the tail and every earlier ordinal reads the same on both. */
        Assert.False(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 13 }));
        Assert.True(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 14 }));
    }

    /* ---------------- helpers ---------------- */

    private static CollectorContext TestContext(bool isAzure) => new()
    {
        ServerId = -640001,
        ServerName = "query-store-health-pins",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        Target = new CollectorTargetInfo
        {
            SqlMajorVersion = 16,
            IsAzureSqlDb = isAzure,
        },
    };

    private static int InvokeMap(object[] leading, bool hasQueryStoreHealth)
    {
        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        /* #2312 appended hasQueryStoreTextHash after this rung's parameter — pass it FALSE so these
           facts keep exercising the V76/V75 arms rather than the newer one. */
        /* Parameters appended by LATER rungs are padded FALSE, so this fact keeps exercising its own
           arm rather than a newer one. Derived from the method's arity rather than listed by hand, so
           a future rung does not have to edit this file -- #2357 (V78) was the fourth that would have. */
        var args = leading.Concat(new object[] { hasQueryStoreHealth }).ToArray();
        args = args
            .Concat(Enumerable.Repeat((object)false, method.GetParameters().Length - args.Length))
            .ToArray();
        Assert.Equal(method.GetParameters().Length, args.Length);

        return (int)method.Invoke(null, args)!;
    }

    private static string ReadViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
