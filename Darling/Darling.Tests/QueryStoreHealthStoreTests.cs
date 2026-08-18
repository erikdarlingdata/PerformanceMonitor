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

        Assert.Equal(76, versions.Max());
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
        Assert.Equal(76, StorageVersion.SchemaVersion);
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
    /// ONLINE only (a RESTORING database's catalog views are unreachable), and the house RECOMPILE.
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

        /* Azure lists all online databases — from master, HAS_DBACCESS returns 0 for every user database
           and there is no AG catalog, so the on-prem filters would enumerate NOTHING there. */
        var azure = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(TestContext(isAzure: true))!;
        Assert.DoesNotContain("HAS_DBACCESS", azure.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_hadr_database_replica_states", azure.Text, StringComparison.Ordinal);
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

    /// <summary>WITHIN the view, every selected column exists from 2016 on — no per-column gates;
    /// pinned so a gated column cannot be added without revisiting this claim.</summary>
    [Fact]
    public void ThePayloadIsUngatedAndOrdered()
    {
        var columns = QueryStoreHealthCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(new[]
        {
            "database_name", "actual_state", "desired_state", "readonly_reason",
            "current_storage_size_mb", "max_storage_size_mb", "size_based_cleanup_mode",
            "stale_query_threshold_days", "max_plans_per_query", "interval_length_minutes",
        }, columns);
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

        var args = leading.Concat(new object[] { hasQueryStoreHealth }).ToArray();
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
