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
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the plan_correction definition (#1952): the 2017+ gate on both the
/// static and the server-side path, the enumeration shape it shares with database_scoped_config,
/// the Azure per-database branch, and the details-JSON shredding — including the two things that
/// were decided against MS Learn on live metal and would silently rot if nothing asserted them.
/// </summary>
public sealed class PlanCorrectionCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext Context(int sqlMajorVersion = 16, bool isAzureSqlDb = false, bool isAzureManagedInstance = false) =>
        new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                SqlMajorVersion = sqlMajorVersion,
                IsAzureSqlDb = isAzureSqlDb,
                IsAzureManagedInstance = isAzureManagedInstance,
            },
        };

    [Theory]
    /* 2016 is the product's floor and predates automatic plan correction entirely. */
    [InlineData(13, false, false, false)]
    [InlineData(14, false, false, true)]
    [InlineData(17, false, false, true)]
    /* Version unknown = assume newest, the house convention; the enumeration re-checks on the server. */
    [InlineData(0, false, false, true)]
    /* Azure SQL DB and Managed Instance both carry FORCE_LAST_GOOD_PLAN and are never version-gated. */
    [InlineData(0, true, false, true)]
    [InlineData(0, false, true, true)]
    public void AppliesTo_Gates2017AndAzure(int major, bool azureDb, bool azureMi, bool expected) =>
        Assert.Equal(expected, PlanCorrectionCollector.Instance.AppliesTo(
            new CollectorTargetInfo { SqlMajorVersion = major, IsAzureSqlDb = azureDb, IsAzureManagedInstance = azureMi }));

    [Fact]
    public void EnumerationQuery_OnPrem_GatesOnProductVersion_AndMirrorsTheSharedDatabaseFilters()
    {
        var plan = PlanCorrectionCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "SO" },
        });

        Assert.NotNull(plan);

        /* The server-side half of the version gate. It rides in the WHERE clause rather than an IF so
           the batch always returns a result set — the enumeration contract's first result set must
           exist even on an instance that enumerates nothing. */
        Assert.Contains("SERVERPROPERTY('PRODUCTVERSION')", plan!.Text, StringComparison.Ordinal);
        Assert.Contains("@product_major >= 14", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("IF @product_major", plan.Text, StringComparison.Ordinal);

        Assert.Contains("sys.dm_hadr_database_replica_states", plan.Text, StringComparison.Ordinal);
        Assert.Contains("is_primary_replica = 1", plan.Text, StringComparison.Ordinal);
        Assert.Contains("HAS_DBACCESS(d.name) = 1", plan.Text, StringComparison.Ordinal);
        Assert.Contains("AND d.name NOT IN (@excl_db_0)", plan.Text, StringComparison.Ordinal);
        Assert.Equal("SO", Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void EnumerationQuery_DoesNotFilterToQueryStoreEnabledDatabases()
    {
        /* The interesting diagnostic is the database where someone asked for FORCE_LAST_GOOD_PLAN and
           did NOT get it — desired ON / actual OFF, reason QUERY_STORE_OFF. Screening the enumeration
           on Query Store the way query_store does would drop exactly those rows, so this collector
           deliberately enumerates every online user database and lets the engine's own reason_desc
           answer the question. */
        var plan = PlanCorrectionCollector.Instance.BuildEnumerationQuery(Context());

        Assert.NotNull(plan);
        Assert.DoesNotContain("database_query_store_options", plan!.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("is_query_store_on", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void Azure_TakesThePerDatabaseConnectionBranch_InsteadOfEnumerating()
    {
        var azure = Context(isAzureSqlDb: true);

        Assert.True(PlanCorrectionCollector.Instance.RunsPerDatabase(azure.Target));
        Assert.Null(PlanCorrectionCollector.Instance.BuildEnumerationQuery(azure));

        /* Already connected to the database, so the payload body runs bare — no three-part name,
           which Azure SQL DB rejects. */
        var plan = PlanCorrectionCollector.Instance.BuildQuery(azure);
        Assert.Contains("sys.dm_db_tuning_recommendations", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("sp_executesql", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPrem_BuildQueryThrows_BecauseEnumerationDrivesTheCycle()
    {
        Assert.False(PlanCorrectionCollector.Instance.RunsPerDatabase(Context().Target));
        Assert.Throws<NotSupportedException>(() => PlanCorrectionCollector.Instance.BuildQuery(Context()));
    }

    [Fact]
    public void PerItemQuery_EscapesClosingBrackets_AndQuoteDoublesTheBody()
    {
        var plan = PlanCorrectionCollector.Instance.BuildPerItemQuery("we]rd db", Context());

        Assert.Contains("EXECUTE [we]]rd db].sys.sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.database_automatic_tuning_options", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_db_tuning_recommendations", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);

        /* Every literal in the body has to survive nesting inside N'...'. A single un-doubled quote
           terminates the string early and the batch fails at parse time on the target, which no unit
           test that only greps for object names would catch. */
        Assert.Contains("''$.planForceDetails.queryId''", plan.Text, StringComparison.Ordinal);
        Assert.Contains("''FORCE_LAST_GOOD_PLAN''", plan.Text, StringComparison.Ordinal);

        /* The negative has to be anchored on the character BEFORE the quote: a doubled '' trivially
           contains a single ', so searching for the un-doubled literal on its own always matches and
           asserts nothing. Anchoring on ", '" makes the two forms distinguishable — the escaped body
           reads ", ''$." and can never match. */
        Assert.DoesNotContain("(dtr.details, '$.", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void DetailsShredding_UsesJsonValue_NotOpenJson()
    {
        /* OPENJSON is only available at database COMPATIBILITY LEVEL 130+, and a compat-100 database on
           a 2017+ instance is legal. Verified 2026-08-01 on SQL2022 against a compat-100 database:
           JSON_VALUE returned the value, OPENJSON failed to parse at all ("Incorrect syntax near
           '$.queryId'"). Microsoft's own documented shredding query uses OPENJSON and would take out
           the whole collection for such a database, so this is a deliberate divergence from the docs. */
        var text = PlanCorrectionCollector.Instance.BuildQuery(Context(isAzureSqlDb: true)).Text;

        Assert.Contains("JSON_VALUE(dtr.details", text, StringComparison.Ordinal);
        Assert.DoesNotContain("OPENJSON", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DetailsShredding_ReadsBothSpellingsOfTheErrorCounts()
    {
        /* MS Learn's column table documents regressedPlanAbortedCount while every example query on the
           same page shreds regressedPlanErrorCount. Picking the wrong one binds a silent NULL rather
           than raising anything. Live capture on SQL2022 (2026-08-01) showed the engine emits
           ErrorCount — but the COALESCE stays, because reading both cannot be wrong and the docs may
           yet be describing a shape some version does emit. */
        var text = PlanCorrectionCollector.Instance.BuildQuery(Context(isAzureSqlDb: true)).Text;

        foreach (var key in new[]
        {
            "regressedPlanAbortedCount", "regressedPlanErrorCount",
            "recommendedPlanAbortedCount", "recommendedPlanErrorCount",
        })
        {
            Assert.Contains(key, text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PayloadBody_DoesNotCarryRecompile()
    {
        /* #2759/#2760: both statements are static literal T-SQL with no runtime parameters, so
           RECOMPILE has nothing to protect against parameter sniffing on, and
           sys.dm_db_tuning_recommendations has no statistics — a cached plan's cardinality guess is
           the same fixed heuristic a fresh compile produces. Live evidence on
           prod-pos-use1-multi-45/Demo showed 4,237ms avg CPU against 381 logical reads and 18 rows: a
           compile-cost signature paid per database, per server, every cycle, for no plan-quality
           benefit. Caching the plan removes that cost fleet-wide with no functional change. */
        var text = PlanCorrectionCollector.Instance.BuildQuery(Context(isAzureSqlDb: true)).Text;

        Assert.DoesNotContain("OPTION(RECOMPILE)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreJoins_AreLeft_SoAnAgedOutPlanCannotDropTheRecommendation()
    {
        /* Microsoft's Example 3 INNER JOINs sys.query_store_plan twice, which silently drops any
           recommendation whose regressed or recommended plan has already aged out of Query Store. The
           recommendation is still live and still names a real regression, so for a monitoring tool
           that is exactly backwards — it lands with a null query_text instead of vanishing. */
        var text = PlanCorrectionCollector.Instance.BuildQuery(Context(isAzureSqlDb: true)).Text;

        /* #2764 moved these joins off the live views and onto the staged temps; the LEFT semantics
           are what this test protects, so it follows the joins to their new targets. */
        Assert.Contains("LEFT JOIN #pm_plan_correction_queries AS qsq", text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN #pm_plan_correction_query_text AS qsqt", text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN #pm_plan_correction_plans AS qsp", text, StringComparison.Ordinal);
        Assert.DoesNotContain("INNER JOIN", text, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreViews_AreStagedToTemps_NeverJoinedLive()
    {
        /* #2764, the rest of the sp_QuickieStore rule. #2673 staged sys.dm_db_tuning_recommendations
           but the shipping SELECT still LEFT JOINed sys.query_store_query / _query_text / _plan LIVE,
           which is exactly the naive-catalog-view join the pattern exists to avoid. Each view must now
           be read exactly once, into a temp keyed off the already-staged recommendation set, and the
           shipping SELECT must join the temps. This pins that a live view never appears on the JOIN
           side again — a `JOIN sys.query_store_` anywhere in the body is a regression. */
        var text = PlanCorrectionCollector.Instance.BuildQuery(Context(isAzureSqlDb: true)).Text;

        Assert.DoesNotContain("JOIN sys.query_store_query ", text, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN sys.query_store_query_text", text, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN sys.query_store_plan", text, StringComparison.Ordinal);

        /* Each view is staged from exactly one SELECT ... INTO, keyed off the recs (or the queries temp
           for the text, the plan -> query -> text chain). */
        Assert.Equal(1, Count(text, "FROM sys.query_store_plan AS qsp"));
        Assert.Equal(1, Count(text, "FROM sys.query_store_query AS qsq"));
        Assert.Equal(1, Count(text, "FROM sys.query_store_query_text AS qsqt"));
        Assert.Contains("INTO #pm_plan_correction_plans", text, StringComparison.Ordinal);
        Assert.Contains("INTO #pm_plan_correction_queries", text, StringComparison.Ordinal);
        Assert.Contains("INTO #pm_plan_correction_query_text", text, StringComparison.Ordinal);
        Assert.Contains("AND   r.last_good_plan_id = qsp.plan_id", text, StringComparison.Ordinal);
        Assert.Contains("WHERE q.query_text_id = qsqt.query_text_id", text, StringComparison.Ordinal);

        /* Every temp is dropped explicitly, so nothing leaks across the sp_executesql scope. */
        foreach (var temp in new[]
        {
            "#pm_plan_correction_recs",
            "#pm_plan_correction_plans",
            "#pm_plan_correction_queries",
            "#pm_plan_correction_query_text",
        })
        {
            Assert.Contains($"DROP TABLE IF EXISTS {temp};", text, StringComparison.Ordinal);
            Assert.Contains($"DROP TABLE {temp};", text, StringComparison.Ordinal);
        }
    }

    private static int Count(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// One row's worth of reader values in SELECT order, defaulted to the enablement-only shape a
    /// database with no recommendations produces.
    /// </summary>
    private static object[] Row(params (int Ordinal, object Value)[] overrides)
    {
        var values = new object[38];
        Array.Fill(values, DBNull.Value);
        values[0] = "ON";
        values[1] = "ON";

        foreach (var (ordinal, value) in overrides)
        {
            values[ordinal] = value;
        }

        return values;
    }

    [Fact]
    public async Task ReadItemAsync_TagsRowsWithTheEnumeratedDatabase_AndKeepsTheEnablementOnlyShape()
    {
        var rows = new List<PlanCorrectionCollector.Row>();
        var context = Context();

        using (var reader = new FakeCollectorDataReader(Row()))
        {
            await PlanCorrectionCollector.Instance.ReadItemAsync("db1", reader, rows, context, CancellationToken.None);
        }

        using (var reader = new FakeCollectorDataReader(Row((1, "OFF"), (2, "QUERY_STORE_OFF"))))
        {
            await PlanCorrectionCollector.Instance.ReadItemAsync("db2", reader, rows, context, CancellationToken.None);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal("db1", rows[0].DbName);
        Assert.Equal("ON", rows[0].ActualState);
        Assert.Null(rows[0].StateReason);
        Assert.Null(rows[0].RecommendationName);

        /* The row the whole surface exists for: asked for it, did not get it, and the engine says why. */
        Assert.Equal("db2", rows[1].DbName);
        Assert.Equal("ON", rows[1].DesiredState);
        Assert.Equal("OFF", rows[1].ActualState);
        Assert.Equal("QUERY_STORE_OFF", rows[1].StateReason);
    }

    [Fact]
    public async Task ReadAsync_Azure_NamesRowsByTheConnectionsDatabase()
    {
        var context = Context(isAzureSqlDb: true);
        context.CurrentDatabaseName = "azure-db";

        using var reader = new FakeCollectorDataReader(Row());
        var rows = await PlanCorrectionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal("azure-db", Assert.Single(rows).DbName);
    }

    [Fact]
    public async Task ReadItemAsync_MapsEveryOrdinal_FromALiveRecommendation()
    {
        /* Values captured verbatim from the real sys.dm_db_tuning_recommendations row provoked on
           SQL2022 on 2026-08-01 (state Verifying / LastGoodPlanForced, the engine having auto-forced
           plan 1 over regressed plan 2). An off-by-one anywhere in the 38-ordinal read shows up here as
           a value landing on the wrong property, which is the defect this shape is prone to. */
        var validSince = new DateTime(2026, 8, 1, 10, 53, 49, DateTimeKind.Utc);
        var rows = new List<PlanCorrectionCollector.Row>();

        using var reader = new FakeCollectorDataReader(Row(
            (5, "PR_1"),
            (6, "FORCE_LAST_GOOD_PLAN"),
            (7, "Verifying"),
            (8, "LastGoodPlanForced"),
            (9, "Average query CPU time changed from 0.02ms to 105.45ms"),
            (10, validSince),
            (11, validSince),
            (12, 100),
            (13, 1L),
            (14, "SELECT c = COUNT_BIG(*) FROM dbo.skew AS s WHERE s.skew_key = @skew_key"),
            (15, 2L),
            (16, 1L),
            (17, "AUTO"),
            (18, true),
            (20, 15L),
            (21, 105.4473333333333d),
            (22, 0L),
            (23, 600L),
            (24, 0.01629666666666667d),
            (25, 0L),
            (26, 64.84008754999998d),
            (27, false),
            (28, true),
            (29, "System"),
            (30, validSince),
            (31, validSince),
            (37, "exec sp_query_store_force_plan @query_id = 1, @plan_id = 1")));

        await PlanCorrectionCollector.Instance.ReadItemAsync("PlanCorrection1952", reader, rows, Context(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("PlanCorrection1952", row.DbName);
        Assert.Equal("PR_1", row.RecommendationName);
        Assert.Equal("FORCE_LAST_GOOD_PLAN", row.RecommendationType);
        Assert.Equal("Verifying", row.RecommendationState);
        Assert.Equal("LastGoodPlanForced", row.RecommendationStateReason);
        Assert.Equal(100, row.Score);
        Assert.Equal(1L, row.QueryId);
        Assert.Contains("skew_key", row.QueryText, StringComparison.Ordinal);
        Assert.Equal(2L, row.RegressedPlanId);
        Assert.Equal(1L, row.LastGoodPlanId);
        Assert.Equal("AUTO", row.LastGoodPlanForcingType);
        Assert.True(row.LastGoodPlanIsForced);
        Assert.Null(row.LastGoodPlanForceFailureReason);
        Assert.Equal(15L, row.RegressedPlanExecutionCount);
        Assert.Equal(105.4473333333333d, row.RegressedPlanCpuTimeAverageMs);
        Assert.Equal(600L, row.LastGoodPlanExecutionCount);
        Assert.Equal(64.84008754999998d, row.EstimatedGainSeconds);
        Assert.False(row.IsExecutableAction);
        Assert.True(row.IsRevertableAction);
        Assert.Equal("System", row.ExecuteActionInitiatedBy);
        Assert.Equal(validSince, row.ExecuteActionInitiatedTime);
        Assert.Null(row.RevertActionInitiatedBy);
        Assert.Equal("exec sp_query_store_force_plan @query_id = 1, @plan_id = 1", row.ImplementationScript);
    }

    [Fact]
    public void PayloadColumns_MatchWriteOrder_AndTheReadersOrdinalCount()
    {
        var names = PlanCorrectionCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal("database_name", names[0]);
        Assert.Equal("query_text", names[15]);
        Assert.Equal("implementation_script", names[^1]);

        var writer = new RecordingCollectorRowWriter();
        PlanCorrectionCollector.Instance.WritePayload(
            new PlanCorrectionCollector.Row { DbName = "db1", DesiredState = "ON", ActualState = "OFF" },
            writer,
            Context());

        /* WritePayload must emit exactly one value per declared column, in the same order — the
           positional appender in both stores depends on it. */
        Assert.Equal(names.Length, writer.Values.Count);
        Assert.Equal("db1", writer.Values[0]);
        Assert.Equal("ON", writer.Values[1]);
        Assert.Equal("OFF", writer.Values[2]);
        Assert.Null(writer.Values[3]);
    }

    [Fact]
    public void Schedule_SitsOnTheFiveMinuteTier_BesideTheOtherPerDatabaseEnumerator()
    {
        /* Not the on-load (0) tier the sibling per-database CONFIG snapshots use, deliberately:
           sys.dm_db_tuning_recommendations does not survive a restart, so an on-load-only collector
           would capture a database once and miss every Active/Verifying recommendation afterwards —
           rows that exist nowhere else once the engine drops them. */
        Assert.Equal(5, CollectorScheduleDefaults.All["plan_correction"].FrequencyMinutes);
        Assert.Equal(
            CollectorScheduleDefaults.All["query_store"].FrequencyMinutes,
            CollectorScheduleDefaults.All["plan_correction"].FrequencyMinutes);
        Assert.True(CollectorScheduleDefaults.All["plan_correction"].DefaultEnabled);
    }
}
