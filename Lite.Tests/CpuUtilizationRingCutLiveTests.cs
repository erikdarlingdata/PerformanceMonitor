/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9530: the ring-buffer statement cuts the scheduler ring to its newest records before it converts them to
/// xml. The cut must not change what the collector reads: the same newest 60 samples, with the same values
/// and the same spacing, as converting the whole ring and keeping the newest 60. Runs against a live server
/// when <c>DARLING_TEST_SQL</c> is set (the same variable the other live Lite classes use). Read-only DMV reads.
/// </summary>
public sealed class CpuUtilizationRingCutLiveTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    /* The pre-#9530 read: every record converted, the newest 60 kept. Returns the ring's own timestamp (ms ticks)
       and the process utilization, which are what the collector's sample_time and cpu column derive from. */
    private const string WholeRingReference = @"
SELECT TOP (60)
    t.timestamp,
    x.process_utilization
FROM
(
    SELECT
        dorb.timestamp,
        record = CONVERT(xml, dorb.record)
    FROM sys.dm_os_ring_buffers AS dorb
    WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
) AS t
CROSS APPLY
(
    SELECT
        process_utilization = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer'),
        system_idle = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
) AS x
WHERE x.process_utilization IS NOT NULL
AND   x.system_idle IS NOT NULL
ORDER BY t.timestamp DESC;";

    private const string NewestRingTimestamp = @"
SELECT MAX(dorb.timestamp)
FROM sys.dm_os_ring_buffers AS dorb
WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR';";

    private static SqlConnectionStringBuilder? ConnectionFromEnvironment()
    {
        string? host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
        };
        string? user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        if (string.IsNullOrEmpty(user))
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? "";
        }

        return builder;
    }

    private static async Task<long> ReadNewestTimestampAsync(SqlConnection connection, CancellationToken ct)
    {
        await using var command = new SqlCommand(NewestRingTimestamp, connection);
        object? value = await command.ExecuteScalarAsync(ct);
        return value is long ticks ? ticks : 0;
    }

    [Fact]
    public async Task RingBuffer_ReturnsTheSameNewest60Samples_AsConvertingTheWholeRing()
    {
        var builder = ConnectionFromEnvironment();
        Assert.SkipWhen(builder is null, "Set DARLING_TEST_SQL (and _USER/_PASSWORD for sql auth) to compare the ring read on a live server.");

        var ct = TestContext.Current.CancellationToken;
        string collectorText = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false)).Text;

        await using var connection = new SqlConnection(builder!.ConnectionString);
        await connection.OpenAsync(ct);

        /* The ring gains a record every minute. If one lands between the two reads the lists shift by one, so
           a read pair whose newest ring timestamp moved is taken again. Three pairs is far more than a
           single minute boundary needs. */
        for (int attempt = 0; attempt < 3; attempt++)
        {
            long before = await ReadNewestTimestampAsync(connection, ct);

            var reference = new List<(long Ticks, int Cpu)>();
            await using (var command = new SqlCommand(WholeRingReference, connection))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    reference.Add((reader.GetInt64(0), reader.GetInt32(1)));
                }
            }

            var collected = new List<(DateTime SampleTime, int Cpu)>();
            await using (var command = new SqlCommand(collectorText, connection))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    collected.Add((reader.GetDateTime(0), reader.GetInt32(1)));
                }
            }

            long after = await ReadNewestTimestampAsync(connection, ct);
            if (before != after)
            {
                continue;
            }

            Assert.NotEmpty(reference);
            Assert.Equal(reference.Count, collected.Count);
            for (int i = 0; i < reference.Count; i++)
            {
                Assert.Equal(reference[i].Cpu, collected[i].Cpu);
                if (i > 0)
                {
                    /* sample_time is the poll clock minus the record's age, so two rows differ by exactly
                       the difference of their ring timestamps. */
                    long expectedGapMs = reference[i - 1].Ticks - reference[i].Ticks;
                    long actualGapMs = (long)(collected[i - 1].SampleTime - collected[i].SampleTime).TotalMilliseconds;
                    Assert.Equal(expectedGapMs, actualGapMs);
                }
            }

            return;
        }

        Assert.Fail("The ring advanced during every read pair, so the two reads were never taken on the same ring.");
    }
}
