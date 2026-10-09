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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3, lane 1: the unit facts of the V173 rung and the builder. No connection: the SQL texts, the order the
/// builder works in, the 3-hour lag, the column map that the compiler and the builder share, and the rung's place in the
/// ladder (the sentinel, the version map, the tick).
/// </summary>
public sealed class QueryStoreComposeStampTests
{
    /// <summary>The probe ordinal of this rung's sentinel in the viewer's version probe.</summary>
    internal const int ProbeOrdinal = 148;

    /* ---- the column map ------------------------------------------------------------------------------- */

    [Fact]
    public void TheMap_HasUniqueColumnsAndExpressions_OverTheFactAlias_NeverDoublePrecision()
    {
        var map = QueryStoreComposeStamp.PartialColumnMap;
        Assert.Equal(map.Count, map.Select(c => c.Column).Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(map.Count, map.Select(c => c.FactExpression).Distinct(StringComparer.Ordinal).Count());
        foreach (var column in map)
        {
            Assert.Contains(column.SqlType, new[] { "bigint", "numeric" });
            Assert.DoesNotContain("double", column.SqlType, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("double", column.FactExpression, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("double", column.RowExpression, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("float", column.RowExpression, StringComparison.OrdinalIgnoreCase);
            if (column.FactExpression != "COUNT(*)")
            {
                Assert.Contains(QueryStoreComposeStamp.FactAlias + ".", column.FactExpression, StringComparison.Ordinal);
            }
        }

        /* The 8-byte columns come first, then the numerics. */
        var types = map.Select(c => c.SqlType).ToList();
        Assert.Equal(types.OrderBy(t => t == "numeric" ? 1 : 0), types);
    }

    [Fact]
    public void TheMap_IsTheTableTheRungCreates_ColumnForColumn()
    {
        var sql = QueryStoreComposeStamp.CreateSql;
        var start = sql.IndexOf("CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp\r\n", StringComparison.Ordinal);
        if (start < 0)
        {
            start = sql.IndexOf("CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp\n", StringComparison.Ordinal);
        }

        Assert.True(start >= 0, "the rung has no CREATE TABLE for the rollup");
        var open = sql.IndexOf('(', start);
        var close = sql.IndexOf(");", open, StringComparison.Ordinal);
        var columns = sql[(open + 1)..close].Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0)
            .Select(l => l.Split(' ', 2)).Select(p => (Name: p[0], Type: p[1])).ToList();

        var expected = new List<(string Name, string Type)> { ("collection_time", "timestamp NOT NULL") };
        expected.AddRange(QueryStoreComposeStamp.PartialColumnMap.Where(c => c.SqlType == "bigint")
            .Select(c => (c.Column, c.Column == "wide_rows" ? "bigint NOT NULL" : "bigint")));
        expected.Add(("server_id", "integer NOT NULL"));
        expected.AddRange(new[] { ("database_name", "text"), ("module_name", "text"), ("query_hash", "text") });
        expected.AddRange(QueryStoreComposeStamp.PartialColumnMap.Where(c => c.SqlType == "numeric").Select(c => (c.Column, "numeric")));

        Assert.Equal(expected, columns);
        Assert.Equal(new[] { "server_id", "database_name", "module_name", "query_hash" }, QueryStoreComposeStamp.DimensionColumns);
    }

    /// <summary>
    /// Every partial the compiler's single-scan builder asks for over a Query Store measure and an aggregate it accepts has a
    /// rollup column: the test calls <c>ComposeCompiler.TryBuildPartialValueExpr</c> itself, for each measure and each
    /// aggregate, and looks up what it added to its <c>PartialColumns</c>. A partial added to the compiler without a column
    /// here would be read through the rollup as a missing column, so the map has to follow it.
    /// </summary>
    [Fact]
    public void TheMap_CoversEveryPartialTheCompilerCanAskFor_OverEveryQueryStoreMeasure()
    {
        var method = typeof(ComposeCompiler).GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "TryBuildPartialValueExpr");
        var partialsType = typeof(ComposeCompiler).GetNestedType("PartialColumns", BindingFlags.NonPublic)!;
        var columnsProperty = partialsType.GetProperty("Columns")!;

        var measures = MeasureCatalog.Measures.Where(m => string.Equals(m.SourceTable, "query_store_stats", StringComparison.Ordinal)).ToList();
        Assert.Equal(7, measures.Count);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var produced = 0;
        foreach (var measure in measures)
        {
            foreach (var aggregate in Enum.GetValues<ComposeAggregate>())
            {
                var partials = Activator.CreateInstance(partialsType, nonPublic: true)!;
                var value = (string?)method.Invoke(null, new[] { measure, aggregate, measure.DefaultUnit, partials });
                if (value is null)
                {
                    continue;
                }

                produced++;
                foreach (var column in (IEnumerable)columnsProperty.GetValue(partials)!)
                {
                    var expression = (string)column.GetType().GetField("Item2")!.GetValue(column)!;
                    seen.Add(expression);
                    Assert.True(QueryStoreComposeStamp.ColumnFor(expression) is not null,
                        $"{measure.Key} / {aggregate} asks for the partial {expression}, which no rollup column holds");
                }
            }
        }

        Assert.True(produced >= 20, $"the walk produced only {produced} partial expressions; the compiler's shape changed");
        /* And every map entry is asked for by something (the map carries no dead column). */
        foreach (var column in QueryStoreComposeStamp.PartialColumnMap)
        {
            Assert.Contains(column.FactExpression, seen);
        }
    }

    /* ---- the SQL texts -------------------------------------------------------------------------------- */

    [Fact]
    public void TheBuilderInsert_IsGeneratedFromTheMap_AtTheStampGrain_InIndexOrder()
    {
        var sql = QueryStoreComposeStamp.BuildHourInsertSql;
        foreach (var column in QueryStoreComposeStamp.PartialColumnMap)
        {
            Assert.Contains(column.FactExpression, sql, StringComparison.Ordinal);
            Assert.Contains(column.Column, sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("double precision", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("::float", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("::numeric", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GROUP BY f.collection_time, f.server_id, f.database_name, f.module_name, f.query_hash", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY f.collection_time, f.server_id", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_store_interval_wide AS f", sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time < $1 + interval '1 hour'", sql, StringComparison.Ordinal);
        /* The floor is the shipped one, and there is no upper bound on first_execution_time (the wide read has none). */
        Assert.Contains("f.first_execution_time >= $1 - " + QueryStoreIntervalWide.PurgeEdgeMarginSql, sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "first_execution_time"));
        /* The target list and the select list have the same number of columns. */
        var targets = Regex.Match(sql, @"INSERT INTO collect\.query_store_compose_stamp \(([^)]*)\)").Groups[1].Value.Split(',').Length;
        Assert.Equal(1 + QueryStoreComposeStamp.PartialColumnMap.Count + QueryStoreComposeStamp.DimensionColumns.Count, targets);
    }

    [Fact]
    public void TheBuildOrder_IsStaleHoursFirst_ThenMissingHoursNewestFirst_AfterTheLag()
    {
        var plan = QueryStoreComposeStamp.PlanSql;
        Assert.Contains("ORDER BY p.stale DESC, p.hour DESC", plan, StringComparison.Ordinal);
        Assert.Contains("LIMIT $2", plan, StringComparison.Ordinal);
        Assert.Contains("b.built_seq IS DISTINCT FROM b.late_seq", plan, StringComparison.Ordinal);
        /* The build lag is read off the STORE's clock, the one the row trigger's WHEN uses; the service's clock is only the retention floor. */
        Assert.Contains("date_trunc('hour', (now() AT TIME ZONE 'UTC') - interval '3 hours')", plan, StringComparison.Ordinal);
        Assert.DoesNotContain("$1::timestamp - interval", plan, StringComparison.Ordinal);
        Assert.Contains("greatest($1::timestamp,", plan, StringComparison.Ordinal);
        Assert.Contains("filled_since", plan, StringComparison.Ordinal);
        Assert.Contains("sv.is_enabled", plan, StringComparison.Ordinal);

        Assert.Equal(TimeSpan.FromHours(3), QueryStoreComposeStamp.BuildLag);
        Assert.Equal(6, QueryStoreComposeStamp.MaxBuildsPerTick);
        Assert.Equal(60, QueryStoreComposeStamp.BuildStatementTimeoutSeconds);
        Assert.Equal(TimeSpan.FromMinutes(10), QueryStoreComposeStamp.MaxTickDuration);
        Assert.Contains("60s", typeof(QueryStoreComposeStamp).GetField("StatementTimeoutSql", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheBuildOrder_InTheCode_IsLockRowsThenDeleteThenInsertThenBuiltThenHours()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreComposeStamp.cs");
        var body = source[source.IndexOf("public static async Task<long?> BuildHourAsync", StringComparison.Ordinal)..];
        var order = new[] { "StatementTimeoutSql", "HourLockSql", "OpenWriterSql", "LockPairsSql", "DeleteHourSql", "BuildHourInsertSql", "SourceRowsSql", "UpsertBuiltSql", "UpsertHourSql", "CommitAsync" };
        var at = order.Select(o => body.IndexOf(o, StringComparison.Ordinal)).ToArray();
        Assert.DoesNotContain(-1, at);
        Assert.Equal(at.OrderBy(i => i), at);

        Assert.Contains("FOR UPDATE", QueryStoreComposeStamp.LockPairsSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id", QueryStoreComposeStamp.LockPairsSql, StringComparison.Ordinal);
        /* The pair's late_seq is never written by the build: built_seq is the value read under the lock. */
        Assert.DoesNotContain("late_seq =", QueryStoreComposeStamp.UpsertBuiltSql, StringComparison.Ordinal);
        Assert.Contains("built_seq = EXCLUDED.built_seq", QueryStoreComposeStamp.UpsertBuiltSql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(l.late_seq, 0)", QueryStoreComposeStamp.UpsertBuiltSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOpenWriterGuard_IsTheWritingTransactionsOfThisDatabase_ThatBeganBeforeTheTriggerMarginEnded()
    {
        var sql = QueryStoreComposeStamp.OpenWriterSql;
        Assert.Contains("FROM pg_stat_activity", sql, StringComparison.Ordinal);
        Assert.Contains("datname = current_database()", sql, StringComparison.Ordinal);
        Assert.Contains("backend_xid IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("pid <> pg_backend_pid()", sql, StringComparison.Ordinal);
        Assert.Contains("(xact_start AT TIME ZONE 'UTC') < $1 + interval '2 hours'", sql, StringComparison.Ordinal);
        /* The 2 hours are the build lag less the trigger's offset: a row of hour H is unmarked only before H + 3 h - 1 h. */
        Assert.Equal(TimeSpan.FromHours(2), QueryStoreComposeStamp.BuildLag - QueryStoreComposeStamp.WhenOffset);
    }

    [Fact]
    public void TheCleanup_IsTheTwoStepShape_DrivenFromTheSmallTables()
    {
        Assert.Contains("RETURNING hour", QueryStoreComposeStamp.GcHoursSql, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_compose_stamp_hours", QueryStoreComposeStamp.GcHoursSql, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_compose_stamp_built", QueryStoreComposeStamp.GcBuiltSql, StringComparison.Ordinal);
        /* Step two probes the (collection_time, server_id) index with the hour's two constants; it never filters the rollup by "< floor". */
        Assert.Contains("collection_time >= $1", QueryStoreComposeStamp.GcRollupSql, StringComparison.Ordinal);
        Assert.Contains("collection_time < $1 + interval '1 hour'", QueryStoreComposeStamp.GcRollupSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFloor_IsTheRetentionHorizonPlusThePurgeEdgeMargin_RoundedUpToTheHour()
    {
        var now = new DateTime(2026, 10, 8, 12, 30, 0, DateTimeKind.Utc);
        var floor = QueryStoreComposeStamp.FloorHour(now, 9);
        var edge = new DateTime(2026, 9, 29, 12, 30, 0) + QueryStoreIntervalWide.PurgeEdgeMargin;
        Assert.True(floor >= edge && floor - edge < TimeSpan.FromHours(1));
        Assert.Equal(0, floor.Minute);
        Assert.Equal(DateTimeKind.Unspecified, floor.Kind);
        /* On an hour edge the floor is the edge itself. */
        var exact = new DateTime(2026, 10, 8, 12, 0, 0) - TimeSpan.FromDays(9) + QueryStoreIntervalWide.PurgeEdgeMargin;
        Assert.Equal(exact.Minute == 0 ? exact : exact.Date.AddHours(exact.Hour + 1), QueryStoreComposeStamp.FloorHour(new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc), 9));
    }

    [Fact]
    public void TheHourNumber_IsWholeHoursSince1970_TheNumberTheTriggerKeyUses()
    {
        Assert.Equal(0, QueryStoreComposeStamp.HourNumber(new DateTime(1970, 1, 1, 0, 0, 0)));
        Assert.Equal(24 * 365, QueryStoreComposeStamp.HourNumber(new DateTime(1971, 1, 1, 0, 0, 0)));
        Assert.Contains("extract(epoch FROM hn)::bigint / 3600", QueryStoreComposeStamp.CreateSql, StringComparison.Ordinal);
    }

    /* ---- the rung ------------------------------------------------------------------------------------- */

    [Fact]
    public void TheRung_IsTheTopOfTheLadder_AndTheStorageVersion()
    {
        Assert.Equal(173, QueryStoreComposeStamp.RungVersion);
        var top = PgMigrations.Scripts[^1];
        Assert.Equal(StorageVersion.SchemaVersion, top.Version);
        Assert.Equal(QueryStoreComposeStamp.RungVersion, top.Version);
        Assert.Equal("query-store-compose-stamp", top.Name);
        Assert.Equal(172, PgMigrations.Scripts[^2].Version);
        Assert.Equal(PgMigrations.Scripts.Count, PgMigrations.Scripts.Select(s => s.Version).Distinct().Count());
        Assert.EndsWith(QueryStoreComposeStamp.CreateSql, top.Sql, StringComparison.Ordinal);
        Assert.StartsWith("SET LOCAL lock_timeout", top.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRungText_HasTheTablesTheIndexTheFunctionAndBothTriggers_OnTheParent_WithTheWriterClock()
    {
        var sql = QueryStoreComposeStamp.CreateSql;
        foreach (var piece in new[]
        {
            "CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp\n",
            "CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp_built",
            "CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp_hours",
            "PRIMARY KEY (server_id, hour)",
            "PRIMARY KEY (hour)",
            "late_seq bigint NOT NULL DEFAULT 0",
            "CREATE INDEX IF NOT EXISTS ix_query_store_compose_stamp_time_server",
            "ON collect.query_store_compose_stamp (collection_time, server_id)",
            "CREATE OR REPLACE FUNCTION collect.query_store_compose_stamp_mark_late() RETURNS trigger",
            "SET search_path = pg_catalog, pg_temp",
            "set_config('darling.query_store_compose_stamp_marked'",
            "AFTER INSERT ON collect.query_store_interval_wide",
            "AFTER UPDATE ON collect.query_store_interval_wide",
            "FOR EACH ROW",
        })
        {
            Assert.Contains(piece, NormalizeEol(sql), StringComparison.Ordinal);
        }

        /* No unique index on the rollup, no DELETE trigger, no SECURITY DEFINER. */
        Assert.DoesNotContain("CREATE UNIQUE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("AFTER DELETE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SECURITY DEFINER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("double precision", sql, StringComparison.OrdinalIgnoreCase);
        /* The WHEN conditions read the writer's clock, never now() (the transaction start), and the offset is the documented one. */
        var whens = Regex.Matches(sql, @"WHEN \((?<cond>.*?)\)\s*EXECUTE", RegexOptions.Singleline).Select(m => m.Groups["cond"].Value).ToList();
        Assert.Equal(2, whens.Count);
        foreach (var when in whens)
        {
            Assert.Contains("clock_timestamp() AT TIME ZONE 'UTC'", when, StringComparison.Ordinal);
            Assert.DoesNotContain("now()", when, StringComparison.Ordinal);
            Assert.Contains("- " + QueryStoreComposeStamp.WhenOffsetSql, when, StringComparison.Ordinal);
        }

        Assert.Contains("OLD.collection_time <", whens[1], StringComparison.Ordinal);
        Assert.Contains("NEW.collection_time <", whens[1], StringComparison.Ordinal);
        Assert.DoesNotContain("now()", sql.Replace("transaction start", string.Empty, StringComparison.Ordinal)[sql.IndexOf("DECLARE", StringComparison.Ordinal)..], StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromHours(1), QueryStoreComposeStamp.WhenOffset);
        Assert.True(QueryStoreComposeStamp.WhenOffset < QueryStoreComposeStamp.BuildLag, "the trigger must mark an hour before the builder reads it");
    }

    private static string NormalizeEol(string value) => value.Replace("\r\n", "\n", StringComparison.Ordinal);

    /* ---- the sentinel, the version map, the tick ---------------------------------------------------- */

    [Fact]
    public void TheProbe_MapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains("to_regclass('collect.query_store_compose_stamp_hours')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(172, (int)method.Invoke(null, behind)!);

        var arm = viewer.IndexOf("if (hasQueryStoreComposeStamp)", StringComparison.Ordinal);
        var below = viewer.IndexOf("if (hasQueryStoreIntervalLatestPartitioned)", StringComparison.Ordinal);
        Assert.True(arm >= 0 && below > arm, "the V173 arm sits above V172's");
        Assert.Contains("return " + QueryStoreComposeStamp.RungVersion.ToString(CultureInfo.InvariantCulture) + ";", viewer[arm..below], StringComparison.Ordinal);
    }

    [Fact]
    public void TheTick_IsANinthTenant_OutsideTheGate_BeforeTheIoRollup_WithItsOwnCatchAll()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);
        var tickStart = code.IndexOf("private async Task RunStoreMaintenanceTickAsync(CancellationToken stoppingToken)", StringComparison.Ordinal);
        Assert.True(tickStart >= 0, "could not locate RunStoreMaintenanceTickAsync");
        var tickEnd = code.IndexOf("private async Task RefreshModuleMapRecentAsync(CancellationToken stoppingToken)", tickStart, StringComparison.Ordinal);
        Assert.True(tickEnd > tickStart, "could not bound RunStoreMaintenanceTickAsync");
        var tick = code[tickStart..tickEnd];

        const string Stamp = "await BuildQueryStoreComposeStampAsync(stoppingToken);";
        const string Io = "await BuildPgIoStatsHourlyAsync(stoppingToken);";
        Assert.Equal(1, tick.Split(Stamp).Length - 1);
        Assert.Equal(1, code.Split(Stamp).Length - 1);
        var stampAt = tick.IndexOf(Stamp, StringComparison.Ordinal);
        var ioAt = tick.IndexOf(Io, StringComparison.Ordinal);
        Assert.True(ioAt > stampAt, "the rollup tick is awaited before the I/O rollup");
        Assert.True(string.IsNullOrWhiteSpace(tick[(stampAt + Stamp.Length)..ioAt]), "nothing sits between the two tenants");
        Assert.True(stampAt > tick.IndexOf("else", StringComparison.Ordinal), "the rollup tick sits after the gated if/else, outside the TimescaleDB gate");

        var builder = code[code.IndexOf("private async Task BuildQueryStoreComposeStampAsync(CancellationToken stoppingToken)", StringComparison.Ordinal)..];
        builder = builder[..builder.IndexOf("private async Task", 60, StringComparison.Ordinal)];
        Assert.Contains("catch (Exception ex) when (ex is not OperationCanceledException)", builder, StringComparison.Ordinal);
        Assert.Contains("QueryStoreComposeStamp.RunTickAsync(", builder, StringComparison.Ordinal);
        Assert.Contains("DarlingRetention.QueryStoreIntervalWideRetentionDays", builder, StringComparison.Ordinal);
        Assert.DoesNotContain("throw", builder, StringComparison.Ordinal);
    }
}
