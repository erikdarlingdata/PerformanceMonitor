/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted procedure_stats definition: the dynamic-SQL
/// standard variant with double-escaped literal exclusions, the Azure single-database variant
/// (token intentionally left unreplaced, as the original did), the plan_handle delta key with
/// its db.schema.object fallback, the 35-column payload, and the #1262 gated whole-module plan
/// capture (off = byte-identical to the no-plan form; on = the text DMV keyed on 0, -1).
/// </summary>
public sealed class ProcedureStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    /// <summary>The drain-time fix (shared with query_stats): a per-row DATALENGTH cap on the captured
    /// plan XML, sourced from QueryPlanXmlCaptureLimits rather than a literal repeated per collector.
    /// Built from the constant so a change to the cap moves this expectation with it.</summary>
    private static readonly string PlanXmlSizeGuardedFragment =
        "query_plan_xml=CASEWHENDATALENGTH(tqp.query_plan)>" + QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + "THENNULLELSEtqp.query_planEND";

    private static string Collapse(string sql) => Regex.Replace(sql, @"\s+", "");

    [Fact]
    public void BuildQuery_Standard_InterpolatesDoubleEscapedLiterals()
    {
        var plan = ProcedureStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "O'Brien" },
        });

        /* Nested-dynamic-SQL escaping: quote doubled twice → N''O''''Brien'' */
        Assert.Contains("AND d.name NOT IN (N''O''''Brien'')", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_trigger_stats", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_function_stats", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildLiteralClause_MatchesLiteOriginal_BothEscapeModes()
    {
        var names = new[] { "O'Brien", "plain" };

        Assert.Equal(
            RemoteCollectorService.BuildDatabaseExclusionLiteralClause(names, "d.name", forNestedDynamicSql: true),
            DatabaseExclusionFilter.BuildLiteralClause(names, "d.name", forNestedDynamicSql: true));
        Assert.Equal(
            RemoteCollectorService.BuildDatabaseExclusionLiteralClause(names, "d.name", forNestedDynamicSql: false),
            DatabaseExclusionFilter.BuildLiteralClause(names, "d.name", forNestedDynamicSql: false));
        Assert.Equal(string.Empty, DatabaseExclusionFilter.BuildLiteralClause(null, "d.name"));
    }

    [Fact]
    public void BuildQuery_Azure_SingleDatabase_TokenLeftInPlace()
    {
        var plan = ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("WHERE s.database_id = DB_ID()", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_exec_trigger_stats", plan.Text, StringComparison.Ordinal);
        /* The original never replaced the token on the Azure variant — pinned as-is. */
        Assert.Contains("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void RunsPerDatabase_OnAzureOnly()
    {
        /* #1833: without the per-database override, the Azure variant ran once on the server
           entry's own connection (master when the Database field is blank), where its
           database_id = DB_ID() filter matched nothing — zero user rows, logged SUCCESS. The
           per-database connection is what makes that predicate mean each user database. */
        Assert.True(ProcedureStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(ProcedureStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(ProcedureStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
    }

    [Fact]
    public void BuildQuery_HandlesConvertToVarchar130_NeverTruncated()
    {
        /* plan_handle/sql_handle are varbinary(64); style-1 hex is '0x' + 128 = 130 chars. The old
           CONVERT(varchar(64), ...) truncated the handle to ~31 bytes, so dm_exec_query_plan rejected the
           stored value and Fetch Live Plan failed for every procedure/trigger/function row. Guards every
           UNION branch in the standard body and the single-proc Azure variant. */
        foreach (var text in new[]
        {
            Collapse(ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text),
            Collapse(ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)).Text),
        })
        {
            Assert.Contains("sql_handle=CONVERT(varchar(130),s.sql_handle,1)", text, StringComparison.Ordinal);
            Assert.Contains("plan_handle=CONVERT(varchar(130),s.plan_handle,1)", text, StringComparison.Ordinal);
            Assert.DoesNotContain("CONVERT(varchar(64),s.sql_handle,1)", text, StringComparison.Ordinal);
            Assert.DoesNotContain("CONVERT(varchar(64),s.plan_handle,1)", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PayloadColumns_MatchSchema_35Columns()
    {
        var names = ProcedureStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.Equal(35, names.Length);
        Assert.Equal("database_name", names[0]);
        Assert.Equal("plan_handle", names[26]);
        Assert.Equal("delta_spills", names[33]);
        Assert.Equal("query_plan_xml", names[34]);   /* trailing plan column (#1262) */
    }

    [Fact]
    public void BuildQuery_PlanCaptureOffByDefault_NoPlanClauses_LiteParity()
    {
        /* Lite never sets CapturePlanXml: the flag defaults false, so no branch gains the plan SELECT
           column or the dm_exec_text_query_plan APPLY, and the placeholders erase without a trace —
           byte-identical to the no-plan form Lite has always shipped (standard AND Azure). */
        var standard = ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));
        var azure = ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        foreach (var text in new[] { standard.Text, azure.Text })
        {
            Assert.DoesNotContain("query_plan_xml", text, StringComparison.Ordinal);
            Assert.DoesNotContain("dm_exec_text_query_plan", text, StringComparison.Ordinal);
            Assert.DoesNotContain("/*PLAN_SELECT*/", text, StringComparison.Ordinal);
            Assert.DoesNotContain("/*PLAN_APPLY*/", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BuildQuery_PlanCaptureOn_Standard_SplicesWholeModuleTextPlanOnceAfterRank()
    {
        /* #1959: the render happens ONCE, OUTSIDE the ranked derived table, against at most 150
           survivors - not once per branch below the TOP, which rendered module-grain plans for every
           candidate the TOP then discarded (the field-measured 25-second overnight tails). The handle
           round-trips from the varchar(130) the payload already carries, so no raw column threads
           through the branches. Whole-module grain stays keyed on literal 0, -1. */
        var plan = ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, capturePlanXml: true));
        var collapsed = Collapse(plan.Text);

        Assert.Single(Regex.Matches(collapsed, Regex.Escape(PlanXmlSizeGuardedFragment)));
        Assert.Single(Regex.Matches(collapsed, Regex.Escape("sys.dm_exec_text_query_plan(CONVERT(varbinary(64),ranked.plan_handle,1),0,-1)AStqp")));
        Assert.DoesNotContain("dm_exec_text_query_plan(s.plan_handle", collapsed, StringComparison.Ordinal);

        /* The apply must sit AFTER the ranked derived table closes - rendering below the TOP is the
           exact defect this shape exists to prevent. */
        Assert.True(
            collapsed.IndexOf("dm_exec_text_query_plan", StringComparison.Ordinal)
                > collapsed.IndexOf(")ASranked", StringComparison.Ordinal),
            "the plan render moved back inside the ranked derived table - below the TOP");

        /* Existing shape untouched: still three DMV branches and the dynamic-SQL body. */
        Assert.Contains("sys.dm_exec_trigger_stats", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_function_stats", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_PlanCaptureOn_Azure_SplicesWholeModuleTextPlan()
    {
        var plan = ProcedureStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, capturePlanXml: true));
        var collapsed = Collapse(plan.Text);

        Assert.Contains(PlanXmlSizeGuardedFragment, collapsed, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_text_query_plan(CONVERT(varbinary(64),ranked.plan_handle,1),0,-1)AStqp", collapsed, StringComparison.Ordinal);
        Assert.True(
            collapsed.IndexOf("dm_exec_text_query_plan", StringComparison.Ordinal)
                > collapsed.IndexOf(")ASranked", StringComparison.Ordinal),
            "the plan render moved back inside the ranked derived table - below the TOP");
        Assert.Contains("WHERE s.database_id = DB_ID()", plan.Text, StringComparison.Ordinal);
        /* Azure keeps its single-proc shape — no trigger/function branches. */
        Assert.DoesNotContain("dm_exec_trigger_stats", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_WritePayload_PlanCaptureOn_CapturesTrailingPlanColumn()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas, capturePlanXml: true);

        /* Flag on = the SELECT carries the trailing query_plan_xml column at ordinal 27. */
        var row = MakeSqlRow(planHandle: "0x0600");
        var row28 = row.Append((object)"<ShowPlanXML>proc</ShowPlanXML>").ToArray();

        using var reader = new FakeCollectorDataReader(row28);
        var rows = await ProcedureStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        ProcedureStatsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(35, writer.Values.Count);
        Assert.Equal("<ShowPlanXML>proc</ShowPlanXML>", writer.Values[34]);   /* query_plan_xml payload slot */
    }

    [Fact]
    public async Task WritePayload_UsesPlanHandleDeltaKey_WithFallback()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);

        using var reader = new FakeCollectorDataReader(
            MakeSqlRow(planHandle: "0x0600"),
            MakeSqlRow(planHandle: null));
        var rows = await ProcedureStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        ProcedureStatsCollector.Instance.WritePayload(rows[0], writer, context);
        Assert.Equal(35, writer.Values.Count);
        Assert.Null(writer.Values[34]);   /* query_plan_xml null when the flag is off */
        Assert.All(deltas.Calls, c => Assert.Equal("0x0600", c.Key));
        Assert.Equal(
            new[] { "proc_stats_exec", "proc_stats_worker", "proc_stats_elapsed", "proc_stats_reads", "proc_stats_writes", "proc_stats_phys_reads", "proc_stats_spills" },
            deltas.Calls.Select(c => c.Group).ToArray());

        deltas.Calls.Clear();
        ProcedureStatsCollector.Instance.WritePayload(rows[1], writer, context);
        Assert.All(deltas.Calls, c => Assert.Equal("SO.dbo.usp_GetUser", c.Key));
    }

    private static object[] MakeSqlRow(string? planHandle) => new object[]
    {
        "SO", "dbo", "usp_GetUser", "PROCEDURE",
        new DateTime(2026, 7, 1, 8, 0, 0, DateTimeKind.Utc), new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        100L, 5000L, 8000L, 900L, 10L, 5L,
        1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L,
        11L, 0L, 4L,
        "0x0200", planHandle is null ? DBNull.Value : planHandle,
    };
}
