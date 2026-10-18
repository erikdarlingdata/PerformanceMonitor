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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 step 1: the forced-plans collector's SQL and registration. It reads only <c>is_forced_plan = 1</c> rows, emits ONE
/// all-NULL marker row per captured database (how a later read tells REMOVED from not captured), carries a command deadline,
/// and its V195 table is the generator's output for the catalog entry.
/// </summary>
public sealed class QueryStoreForcedPlansCollectorTests
{
    /* The collector reads the engine version from the enumeration probe (SERVERPROPERTY PRODUCTVERSION major), the way
       QueryStoreCollector does, so the context carries it as EnumerationProbeResult. Target.SqlMajorVersion alone is not
       what the per-database body gates on. */
    private static CollectorContext Context(int major) => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s", Target = new CollectorTargetInfo { SqlMajorVersion = major },
        Deltas = new CollectorDeltaCalculator(),
        EnumerationProbeResult = major,
    };

    [Fact]
    public void ThePayload_ReadsOnlyForcedPlans_AndEmitsOneMarkerRow_AndSetsACommandTimeout()
    {
        var body = QueryStoreForcedPlansCollector.BuildPayloadBody(Context(16));

        Assert.Contains("WHERE qsp.is_forced_plan = 1", body, StringComparison.Ordinal);
        Assert.Equal(1, body.Split("is_forced_plan", StringSplitOptions.None).Length - 1);
        Assert.Contains("UNION ALL", body, StringComparison.Ordinal);
        Assert.Contains("sys.database_query_store_options", body, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan =", body, StringComparison.Ordinal);
        Assert.True(QueryStoreForcedPlansCollector.Instance.CommandTimeoutSecondsOverride is > 0 and <= 300);
    }

    [Fact]
    public void ThePlanForcingTypeColumn_IsAVersionGate_NullBelow2017()
    {
        Assert.Contains("plan_forcing_type_desc", QueryStoreForcedPlansCollector.BuildPayloadBody(Context(14)), StringComparison.Ordinal);
        Assert.DoesNotContain("plan_forcing_type_desc", QueryStoreForcedPlansCollector.BuildPayloadBody(Context(13)), StringComparison.Ordinal);
    }

    [Fact]
    public void TheEnumeration_AppliesTheServersDatabaseScopeAndExclusions_AndAzureDoesNotEnumerate()
    {
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s", Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            Deltas = new CollectorDeltaCalculator(),
            DatabaseScope = new[] { "KeepA", "KeepB" },
            ExcludedDatabases = new[] { "Skip" },
        };

        var query = QueryStoreForcedPlansCollector.Instance.BuildEnumerationQuery(context)!;

        Assert.Contains("@scope_db_0", query.Text, StringComparison.Ordinal);
        Assert.Contains("@scope_db_1", query.Text, StringComparison.Ordinal);
        Assert.Contains("@excl_db_0", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("KeepA", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);
        Assert.Contains(query.Parameters, p => Equals(p.Value, "KeepA"));
        Assert.Contains(query.Parameters, p => Equals(p.Value, "Skip"));

        var unscoped = QueryStoreForcedPlansCollector.Instance.BuildEnumerationQuery(Context(16))!;
        Assert.DoesNotContain("@scope_db_", unscoped.Text, StringComparison.Ordinal);

        var azure = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s", Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            Deltas = new CollectorDeltaCalculator(),
        };
        Assert.Null(QueryStoreForcedPlansCollector.Instance.BuildEnumerationQuery(azure));
    }

    [Fact]
    public void TheCollector_IsRegistered_EveryFiveMinutes_ThirtyDays_AsAPlainDarlingOnlyTable()
    {
        var instance = QueryStoreForcedPlansCollector.Instance;
        Assert.Contains(instance, CollectorCatalog.All);
        Assert.Equal(5, CollectorScheduleDefaults.All["query_store_forced_plans"].FrequencyMinutes);
        Assert.Equal(30, CollectorScheduleDefaults.All["query_store_forced_plans"].RetentionDays);
        Assert.True(instance.StaysPlainTable);
        Assert.True(instance.IsDarlingOnly);
        Assert.DoesNotContain(TimescaleSupport.HypertableTables, t => t.TargetTable == "query_store_forced_plans");
    }

    [Fact]
    public void TheV195Table_IsTheGeneratorsOutputForTheCatalogEntry()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 195).Sql.Replace("collect.", "", StringComparison.Ordinal);
        var generated = PgSchemaGenerator.CreateTable(QueryStoreForcedPlansCollector.Instance) + "\n\n" + PgSchemaGenerator.CreateIndex(QueryStoreForcedPlansCollector.Instance);

        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        Assert.Equal(Norm(generated), Norm(rung));
    }
}
