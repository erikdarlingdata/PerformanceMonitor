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
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted blocked_process_report definition: the server- vs
/// database-scoped ring-buffer reads, the sp_HumanEventsBlockViewer-mirroring wait_resource →
/// contentious-object resolution batch, the event_time watermark with its 10-minute fallback,
/// the 40-column payload, the report-XML parse (blocked + blocking process attributes,
/// monitorLoop root attribute, null on garbage/missing blocked-process), and the #1262 gated
/// best-effort plan resolution (off = byte-identical; on = frame → dm_exec_query_stats →
/// dm_exec_text_query_plan for both contending sides). Third Name≠TargetTable case
/// (blocked_process_report → blocked_process_reports).
/// </summary>
public sealed class BlockedProcessReportCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Collapse(string sql) => Regex.Replace(sql, @"\s+", "");

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false,
        DateTime? watermark = null,
        DateTime? collectionTime = null,
        bool capturePlanXml = false)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = collectionTime ?? new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            Watermark = watermark,
            CapturePlanXml = capturePlanXml,
        };

    [Fact]
    public void Identity_ThirdNameTargetTableSplit_WithWatermark()
    {
        Assert.Equal("blocked_process_report", BlockedProcessReportCollector.Instance.Name);
        Assert.Equal("blocked_process_reports", BlockedProcessReportCollector.Instance.TargetTable);
        Assert.Equal("event_time", BlockedProcessReportCollector.Instance.WatermarkColumn);
        Assert.Equal("PerformanceMonitor_BlockedProcess", BlockedProcessReportCollector.XeSessionName);
    }

    [Fact]
    public void RunsPerDatabase_OnAzureOnly_WithPerDatabaseWatermark()
    {
        /* #1535: Azure capture is one database-scoped session per monitored database, so the read
           loops databases and each database dedups against its own watermark (database_name is the
           blocked process's currentdbname the report parse has always extracted). Server-scoped
           platforms keep the single run + server-wide watermark. */
        Assert.True(BlockedProcessReportCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(BlockedProcessReportCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(BlockedProcessReportCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.Equal("database_name", BlockedProcessReportCollector.Instance.PerDatabaseWatermarkColumn);
    }

    [Fact]
    public void BuildQuery_ServerScoped_PinsResolutionBatch()
    {
        var plan = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext());

        Assert.Contains("sys.dm_xe_session_targets AS xet", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_xe_database_session_targets", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'PerformanceMonitor_BlockedProcess'", plan.Text, StringComparison.Ordinal);
        Assert.Contains("event[@name=\"blocked_process_report\"]", plan.Text, StringComparison.Ordinal);
        Assert.Contains("INTO #bpr", plan.Text, StringComparison.Ordinal);
        Assert.Contains(".sys.partitions AS p", plan.Text, StringComparison.Ordinal);
        Assert.Contains("IF @product_version >= 15", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_db_page_info(b.resource_database_id, b.resource_file_id, b.resource_page_id, ''LIMITED'')", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'Unresolved: ' +", plan.Text, StringComparison.Ordinal);
        Assert.Contains("> @cutoff_time", plan.Text, StringComparison.Ordinal);
        /* The final projection keeps the original 5 reader columns. */
        Assert.Contains("b.contentious_object\nFROM #bpr AS b", plan.Text.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_UsesDatabaseScopedDmvs()
    {
        var plan = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true));

        Assert.Contains("sys.dm_xe_database_session_targets AS xet", plan.Text, StringComparison.Ordinal);
        Assert.Contains("JOIN sys.dm_xe_database_sessions AS xes", plan.Text, StringComparison.Ordinal);
        Assert.Contains("event[@name=\"blocked_process_report\"]", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_PinsWatermarkCutoff_AndTenMinuteFallback()
    {
        var watermark = new DateTime(2026, 7, 2, 11, 45, 0, DateTimeKind.Utc);
        var withWatermark = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(watermark: watermark));
        /* #4200 added @last_execution_count alongside @cutoff_time. */
        Assert.Equal(2, withWatermark.Parameters.Count);
        var parameter = Assert.Single(withWatermark.Parameters, p => p.Name == "@cutoff_time");
        Assert.Equal(watermark, parameter.Value);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);

        var collectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);
        var fallback = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(collectionTime: collectionTime));
        Assert.Equal(collectionTime.AddMinutes(-10), Assert.Single(fallback.Parameters, p => p.Name == "@cutoff_time").Value);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder_40Columns()
    {
        var names = BlockedProcessReportCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(40, names.Length);
        Assert.Equal("event_time", names[0]);
        Assert.Equal("blocked_spid", names[2]);
        Assert.Equal("wait_time_ms", names[6]);
        Assert.Equal("blocked_sql_text", names[16]);
        Assert.Equal("blocking_sql_text", names[22]);
        Assert.Equal("blocked_process_report_xml", names[33]);
        Assert.Equal("object_id", names[34]);
        Assert.Equal("database_id", names[35]);
        Assert.Equal("contentious_object", names[36]);
        Assert.Equal("monitor_loop", names[37]);
        Assert.Equal("blocked_query_plan_xml", names[38]);    /* trailing best-effort plans (#1262) */
        Assert.Equal("blocking_query_plan_xml", names[39]);
    }

    [Fact]
    public void BuildQuery_PlanCaptureOffByDefault_NoResolution_LiteParity()
    {
        /* Lite never sets CapturePlanXml: no plan columns, no frame shred, no resolution join —
           byte-identical to the no-plan form (server-scoped AND Azure). */
        var serverScoped = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext());
        var azure = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true));

        foreach (var text in new[] { serverScoped.Text, azure.Text })
        {
            Assert.DoesNotContain("query_plan", text, StringComparison.Ordinal);
            Assert.DoesNotContain("dm_exec_text_query_plan", text, StringComparison.Ordinal);
            Assert.DoesNotContain("executionStack/frame", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BuildQuery_PlanCaptureOn_ResolvesBothSidesFromFrameHandles()
    {
        var plan = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(capturePlanXml: true));
        var collapsed = Collapse(plan.Text);

        /* Two projected plan columns and two frame-shredding APPLYs (blocked + blocking). */
        Assert.Contains("blocked_query_plan_xml = bqp.query_plan", plan.Text, StringComparison.Ordinal);
        Assert.Contains("blocking_query_plan_xml = kqp.query_plan", plan.Text, StringComparison.Ordinal);
        Assert.Contains("/blocked-process-report/blocked-process/process/executionStack/frame", plan.Text, StringComparison.Ordinal);
        Assert.Contains("/blocked-process-report/blocking-process/process/executionStack/frame", plan.Text, StringComparison.Ordinal);
        /* The resolution reuses the proven #880 sql_handle + offsets → text-plan lookup. */
        Assert.Equal(2, Regex.Matches(collapsed, Regex.Escape("JOINsys.dm_exec_query_statsASqsONqs.sql_handle=frames.frame_handle")).Count);
        Assert.Contains("sys.dm_exec_text_query_plan", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_PlanCaptureOn_CapturesBothTrailingPlanColumns()
    {
        var context = MakeContext(capturePlanXml: true);
        var eventTime = new DateTime(2026, 7, 2, 11, 59, 30, DateTimeKind.Utc);

        /* Flag on = the projection carries the two plan columns at ordinals 5/6. */
        using var reader = new FakeCollectorDataReader(
            new object[] { eventTime, SampleReportXml, 1234, 7, "dbo.t", "<ShowPlanXML>blocked</ShowPlanXML>", "<ShowPlanXML>blocking</ShowPlanXML>" });
        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        BlockedProcessReportCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(40, writer.Values.Count);
        Assert.Equal("<ShowPlanXML>blocked</ShowPlanXML>", writer.Values[38]);
        Assert.Equal("<ShowPlanXML>blocking</ShowPlanXML>", writer.Values[39]);
    }

    private const string SampleReportXml = @"<blocked-process-report monitorLoop=""332"">
 <blocked-process>
  <process id=""process1"" spid=""55"" ecid=""0"" waittime=""12345"" waitresource=""KEY: 7:72057594046185472 (abc123)"" lockMode=""U"" status=""suspended"" isolationlevel=""read committed (2)"" logused=""100"" trancount=""2"" clientapp=""app1"" hostname=""host1"" loginname=""login1"" transactionname=""UPDATE"" lasttranstarted=""2026-07-02T11:59:00"" lastbatchstarted=""2026-07-02T11:59:01"" lastbatchcompleted=""2026-07-02T11:58:59"" priority=""0"" currentdbname=""SO"">
   <inputbuf> UPDATE t SET x = 1; </inputbuf>
  </process>
 </blocked-process>
 <blocking-process>
  <process spid=""66"" ecid=""0"" status=""sleeping"" isolationlevel=""read committed (2)"" clientapp=""app2"" hostname=""host2"" loginname=""login2"" transactionname=""user_transaction"" lasttranstarted=""2026-07-02T11:58:00"" lastbatchstarted=""2026-07-02T11:58:01"" lastbatchcompleted=""2026-07-02T11:58:02"" priority=""0"">
   <inputbuf> BEGIN TRAN; </inputbuf>
  </process>
 </blocking-process>
</blocked-process-report>";

    [Fact]
    public void ParseReportXml_MapsBothProcesses_AndMonitorLoop()
    {
        var eventTime = new DateTime(2026, 7, 2, 11, 59, 30, DateTimeKind.Utc);
        var row = BlockedProcessReportCollector.ParseReportXml(SampleReportXml, eventTime);

        Assert.NotNull(row);
        Assert.Equal(eventTime, row!.EventTime);
        Assert.Equal(332, row.MonitorLoop);
        Assert.Equal("SO", row.DatabaseName);
        Assert.Equal(55, row.BlockedSpid);
        Assert.Equal(66, row.BlockingSpid);
        Assert.Equal(12345L, row.WaitTimeMs);
        Assert.Equal("KEY: 7:72057594046185472 (abc123)", row.WaitResource);
        Assert.Equal("U", row.LockMode);
        Assert.Equal(100L, row.BlockedLogUsed);
        Assert.Equal(2, row.BlockedTransactionCount);
        Assert.Equal("UPDATE t SET x = 1;", row.BlockedSqlText);
        Assert.Equal("BEGIN TRAN;", row.BlockingSqlText);
        Assert.Equal("UPDATE", row.BlockedTransactionName);
        Assert.Equal("user_transaction", row.BlockingTransactionName);
        Assert.Equal(new DateTime(2026, 7, 2, 11, 59, 0), row.BlockedLastTranStarted);
        Assert.Equal(new DateTime(2026, 7, 2, 11, 58, 2), row.BlockingLastBatchCompleted);
    }

    [Fact]
    public async Task ReadAsync_PerDatabasePath_CaptureDatabaseWinsOverParsedValue()
    {
        /* On the Azure per-database read path the host stamps context.CurrentDatabaseName per
           iteration, and it is authoritative for the per-database watermark key — beating the
           report XML's currentdbname (here "SO") and covering reports that carry none, whose null
           database_name could never advance that database's watermark. Server-scoped platforms
           leave CurrentDatabaseName null, keeping the parsed value (pinned above). */
        var context = MakeContext(isAzureSqlDb: true);
        context.CurrentDatabaseName = "ringdb";

        using var reader = new FakeCollectorDataReader(
            new object[] { new DateTime(2026, 7, 2, 11, 59, 30, DateTimeKind.Utc), SampleReportXml, 1234, 7, "dbo.t" });
        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal("ringdb", Assert.Single(rows).DatabaseName);
    }

    [Fact]
    public void ParseReportXml_NullOnGarbage_NullWithoutBlockedProcess()
    {
        Assert.Null(BlockedProcessReportCollector.ParseReportXml("<not-xml", null));
        Assert.Null(BlockedProcessReportCollector.ParseReportXml("<blocked-process-report/>", null));

        /* A report with only a blocking process is unusable — blocked side drives the row. */
        Assert.Null(BlockedProcessReportCollector.ParseReportXml(
            "<blocked-process-report><blocking-process><process spid=\"66\"/></blocking-process></blocked-process-report>", null));
    }

    [Fact]
    public async Task ReadAsync_SkipsEmptyAndUnparseable_WritePayloadPins40ColumnOrder()
    {
        var context = MakeContext();
        var eventTime = new DateTime(2026, 7, 2, 11, 59, 30, DateTimeKind.Utc);

        using var reader = new FakeCollectorDataReader(
            new object[] { eventTime, SampleReportXml, 1234, 7, "dbo.t" },
            new object[] { eventTime, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value },   /* empty xml -> skipped */
            new object[] { eventTime, "<garbage", DBNull.Value, DBNull.Value, DBNull.Value });    /* unparseable -> skipped */
        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        var writer = new RecordingCollectorRowWriter();
        BlockedProcessReportCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(40, writer.Values.Count);
        Assert.Equal(eventTime, writer.Values[0]);
        Assert.Equal("SO", writer.Values[1]);
        Assert.Equal(55, writer.Values[2]);
        Assert.Equal(66, writer.Values[4]);
        Assert.Equal(12345L, writer.Values[6]);
        Assert.Equal("UPDATE t SET x = 1;", writer.Values[16]);
        Assert.Equal("BEGIN TRAN;", writer.Values[22]);
        Assert.Equal(SampleReportXml, writer.Values[33]);       /* raw XML rides along */
        Assert.Equal(1234, writer.Values[34]);                  /* event object_id (#1140) */
        Assert.Equal(7, writer.Values[35]);
        Assert.Equal("dbo.t", writer.Values[36]);               /* server-side resolved object */
        Assert.Equal(332, writer.Values[37]);                   /* monitorLoop from the root attribute */
        Assert.Null(writer.Values[38]);                         /* plan columns null when flag off */
        Assert.Null(writer.Values[39]);
        Assert.Empty(s_deltas.Calls);
    }
}
