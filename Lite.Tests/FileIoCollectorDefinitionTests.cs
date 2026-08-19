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
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted file_io_stats definition: the two query variants,
/// the exclusion-filter splice, per-database execution on Azure, and the eight
/// "{database}|{file}" delta groups.
/// </summary>
public sealed class FileIoCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void BuildQuery_OnPrem_SplicesExclusionFilter_WithParameters()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "AdventureWorks", "StackOverflow" },
        });

        Assert.Contains("AND d.name NOT IN (@excl_db_0, @excl_db_1)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.master_files", plan.Text, StringComparison.Ordinal);
        Assert.Equal(2, plan.Parameters.Count);
        Assert.Equal(("@excl_db_0", (object?)"AdventureWorks", CollectorParameterType.NVarChar128),
            (plan.Parameters[0].Name, plan.Parameters[0].Value, plan.Parameters[0].Type));
    }

    [Fact]
    public void BuildQuery_OnPrem_NoExclusions_RemovesToken_NoParameters()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@excl_db_", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildQuery_Azure_ScopesToConnectedDatabase_AndRunsPerDatabase()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("sys.dm_io_virtual_file_stats(DB_ID(), NULL)", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.database_files", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
        Assert.True(FileIoStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(FileIoStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = false }));
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder()
    {
        Assert.Equal(
            new[]
            {
                "database_name", "file_name", "file_type", "physical_name", "size_mb",
                "num_of_reads", "num_of_writes", "read_bytes", "write_bytes",
                "io_stall_read_ms", "io_stall_write_ms", "io_stall_queued_read_ms", "io_stall_queued_write_ms",
                "delta_reads", "delta_writes", "delta_read_bytes", "delta_write_bytes",
                "delta_stall_read_ms", "delta_stall_write_ms", "delta_stall_queued_read_ms", "delta_stall_queued_write_ms",
            },
            FileIoStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAsync_NullRow_MapsDefaults()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value });

        var rows = await FileIoStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("Unknown", row.DatabaseName);
        Assert.Equal("", row.PhysicalName);
        Assert.Equal(0L, row.NumOfReads);
    }

    [Fact]
    public void WritePayload_EmitsSchemaOrder_AndPinsDeltaKeyAndGroups()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var writer = new RecordingCollectorRowWriter();
        var context = CollectorTestContext.Make(deltas);
        var row = new FileIoStatsCollector.Row("SO", "SO_data", "ROWS", @"D:\so.mdf", 100.5m, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 1);

        FileIoStatsCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(21, writer.Values.Count);
        Assert.Equal("SO", writer.Values[0]);
        Assert.Equal(8L, writer.Values[12]);
        Assert.Equal(10L, writer.Values[13]);   /* delta_reads = 1 * 10 */
        Assert.Equal(80L, writer.Values[20]);   /* delta_stall_queued_write_ms = 8 * 10 */

        Assert.Equal(8, deltas.Calls.Count);
        Assert.All(deltas.Calls, c => Assert.Equal("SO|SO_data", c.Key));
        Assert.All(deltas.Calls, c => Assert.Equal(CollectorDeltaCalculator.DefaultMaxGapSeconds, c.MaxGap));
        Assert.Equal(
            new[]
            {
                "file_io_reads", "file_io_writes", "file_io_read_bytes", "file_io_write_bytes",
                "file_io_stall_read", "file_io_stall_write", "file_io_stall_queued_read", "file_io_stall_queued_write",
            },
            deltas.Calls.Select(c => c.Group).ToArray());
    }
}

/// <summary>
/// Pins that the shared DatabaseExclusionFilter and Lite's original builder (still used by
/// un-migrated collectors) produce identical SQL — the temporary duplication cannot drift.
/// </summary>
public sealed class DatabaseExclusionFilterTests
{
    [Fact]
    public void Empty_ReturnsEmptyClause_NoParameters()
    {
        var (clause, parameters) = DatabaseExclusionFilter.Build(null, "d.name");
        Assert.Equal(string.Empty, clause);
        Assert.Empty(parameters);
    }

    [Fact]
    public void Names_ProduceParameterizedNotIn()
    {
        var (clause, parameters) = DatabaseExclusionFilter.Build(new[] { "A", "B" }, "d.name");

        Assert.Equal("AND d.name NOT IN (@excl_db_0, @excl_db_1)", clause);
        Assert.Equal(2, parameters.Count);
        Assert.Equal("A", parameters[0].Value);
        Assert.All(parameters, p => Assert.Equal(CollectorParameterType.NVarChar128, p.Type));
    }

    [Fact]
    public void MatchesLiteOriginalBuilder_ClauseAndParameters()
    {
        var names = new[] { "AdventureWorks", "Stack Overflow" };

        var (sharedClause, sharedParams) = DatabaseExclusionFilter.Build(names, "d.name");
        var (liteClause, liteParams) = RemoteCollectorService.BuildDatabaseExclusionFilter(names, "d.name");

        Assert.Equal(liteClause, sharedClause);
        Assert.Equal(liteParams.Count, sharedParams.Count);
        for (int i = 0; i < liteParams.Count; i++)
        {
            Assert.Equal(liteParams[i].ParameterName, sharedParams[i].Name);
            Assert.Equal(liteParams[i].Value, sharedParams[i].Value);
            Assert.Equal(System.Data.SqlDbType.NVarChar, liteParams[i].SqlDbType);
            Assert.Equal(128, liteParams[i].Size);
        }
    }
}
