/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the cross-SKU parity contract of the shared system_health_events collector definition (Stage 1
/// raw capture of the built-in system_health Extended Events session): the server-scoped ring-buffer
/// read, the exact six-event capture filter (and the deliberate exclusion of xml_deadlock_report /
/// security / connectivity), the event_time watermark with its 10-minute fallback, the collect-
/// everywhere-but-Azure-SQL-DB AppliesTo, the three-column raw-XML payload, and the null-tolerant
/// ReadAsync/WritePayload. An intentional change to any of these must consciously update this test.
/// </summary>
public sealed class SystemHealthEventsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false,
        bool isAzureManagedInstance = false,
        DateTime? watermark = null,
        DateTime? collectionTime = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = collectionTime ?? new DateTime(2026, 7, 5, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb, IsAzureManagedInstance = isAzureManagedInstance },
            Watermark = watermark,
        };

    [Fact]
    public void Identity_AndSessionNamePinned()
    {
        Assert.Equal("system_health_events", SystemHealthEventsCollector.Instance.Name);
        Assert.Equal("system_health_events", SystemHealthEventsCollector.Instance.TargetTable);
        Assert.Equal("system_health_event_id", SystemHealthEventsCollector.Instance.PrefixIdColumnName);
        Assert.Equal("event_time", SystemHealthEventsCollector.Instance.WatermarkColumn);
        Assert.Equal("system_health", SystemHealthEventsCollector.XeSessionName);
    }

    [Fact]
    public void BuildQuery_ReadsServerScopedSystemHealthRingBuffer()
    {
        var text = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("sys.dm_xe_session_targets", text, StringComparison.Ordinal);
        Assert.Contains("JOIN sys.dm_xe_sessions AS xes", text, StringComparison.Ordinal);
        Assert.Contains("N'system_health'", text, StringComparison.Ordinal);
        Assert.Contains("RingBufferTarget/event", text, StringComparison.Ordinal);
        Assert.Contains("evt.query('.')", text, StringComparison.Ordinal);
        /* The time filter runs inside the XQuery; a .value() cast in WHERE shreds every event first. */
        Assert.Contains("WHERE evt.exist('@timestamp[. > sql:variable(\"@cutoff_time\")]') = 1", text, StringComparison.Ordinal);
        Assert.DoesNotContain("evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time", text, StringComparison.Ordinal);

        /* Server-scoped only — this collector never reaches Azure SQL DB (gated by AppliesTo), so no
           database-scoped DMV variant exists. */
        Assert.DoesNotContain("dm_xe_database_session_targets", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_CapturesExactlyTheSixFeederEvents_AndExcludesDeadlockSecurityConnectivity()
    {
        var text = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext()).Text;

        foreach (var captured in new[]
        {
            "sp_server_diagnostics_component_result",
            "error_reported",
            "memory_broker_ring_buffer_recorded",
            "memory_node_oom_ring_buffer_recorded",
            "scheduler_monitor_system_health_ring_buffer_recorded",
            "wait_info",
        })
        {
            Assert.Contains($"N'{captured}'", text, StringComparison.Ordinal);
        }

        /* xml_deadlock_report is owned by DeadlocksCollector; security/connectivity are outside the
           warning categories this feeds — none may leak into the capture filter. */
        Assert.DoesNotContain("xml_deadlock_report", text, StringComparison.Ordinal);
        Assert.DoesNotContain("security_error_ring_buffer_recorded", text, StringComparison.Ordinal);
        Assert.DoesNotContain("connectivity_ring_buffer_recorded", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_PinsWatermarkCutoff_AndTenMinuteFallback()
    {
        var watermark = new DateTime(2026, 7, 5, 11, 55, 0, DateTimeKind.Utc);
        var withWatermark = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext(watermark: watermark));
        var parameter = Assert.Single(withWatermark.Parameters);
        Assert.Equal("@cutoff_time", parameter.Name);
        Assert.Equal(watermark, parameter.Value);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);

        var collectionTime = new DateTime(2026, 7, 5, 12, 0, 0, DateTimeKind.Utc);
        var fallback = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext(collectionTime: collectionTime));
        Assert.Equal(collectionTime.AddMinutes(-10), Assert.Single(fallback.Parameters).Value);
    }

    [Fact]
    public void AppliesTo_CollectsEverywhereExceptAzureSqlDb()
    {
        /* No built-in system_health session on Azure SQL DB (MS Learn-verified); the captured events
           are server-level with no database-scoped equivalent. Managed Instance and on-prem have the
           full server-scoped session. */
        Assert.False(SystemHealthEventsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(SystemHealthEventsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(SystemHealthEventsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_ThreeRawColumns_InAppendOrder()
    {
        var columns = SystemHealthEventsCollector.Instance.PayloadColumns;

        Assert.Equal(new[] { "event_time", "event_type", "event_xml" }, columns.Select(c => c.Name).ToArray());
        Assert.Equal(CollectorColumnType.Timestamp, columns.Single(c => c.Name == "event_time").Type);
        Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == "event_type").Type);
        Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == "event_xml").Type);
    }

    [Fact]
    public async Task ReadAsync_WritePayload_PinsThreeColumnOrder_AndToleratesNulls()
    {
        var context = MakeContext();
        var eventTime = new DateTime(2026, 7, 5, 11, 58, 0, DateTimeKind.Utc);
        const string rawXml = "<event name=\"error_reported\" package=\"sqlserver\"><data name=\"error_number\"><value>701</value></data></event>";

        using var reader = new FakeCollectorDataReader(
            new object[] { eventTime, "error_reported", rawXml },
            new object[] { DBNull.Value, DBNull.Value, DBNull.Value });
        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);

        var writer = new RecordingCollectorRowWriter();
        SystemHealthEventsCollector.Instance.WritePayload(rows[0], writer, context);

        Assert.Equal(3, writer.Values.Count);
        Assert.Equal(eventTime, writer.Values[0]);
        Assert.Equal("error_reported", writer.Values[1]);
        Assert.Equal(rawXml, writer.Values[2]);   /* raw event XML stored verbatim — no shredding */

        /* An all-NULL row still writes (nothing filters it); capture is a point-in-time event read
           with no delta calculator interaction. */
        var nullWriter = new RecordingCollectorRowWriter();
        SystemHealthEventsCollector.Instance.WritePayload(rows[1], nullWriter, context);
        Assert.Equal(3, nullWriter.Values.Count);
        Assert.All(nullWriter.Values, Assert.Null);
        Assert.Empty(s_deltas.Calls);
    }
}
