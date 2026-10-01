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

/// <summary>Pins the parity contracts of the config-family definitions (batch 7).</summary>
public sealed class ServerConfigCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void NamesColumnsAndQuery_MatchSchema()
    {
        Assert.Equal("server_config", ServerConfigCollector.Instance.Name);
        Assert.Equal("server_config", ServerConfigCollector.Instance.TargetTable);
        Assert.Contains("sys.configurations", ServerConfigCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text, StringComparison.Ordinal);
        Assert.Equal(
            new[] { "configuration_name", "value_configured", "value_in_use", "is_dynamic", "is_advanced" },
            ServerConfigCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public void AppliesTo_SkipsAzureSqlDb_CollectsElsewhere()
    {
        /* sys.configurations is not exposed on Azure SQL DB (edition 5). Gate collapsed from Lite's
           IsCollectorSupported into the shared AppliesTo so Darling skips it too (before, Darling threw
           on connect). Managed Instance and on-prem have the view. */
        Assert.False(ServerConfigCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public async Task ReadAndWrite_RoundTrip()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "max degree of parallelism", 8L, 8L, true, true });

        var rows = await ServerConfigCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        ServerConfigCollector.Instance.WritePayload(Assert.Single(rows), writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { "max degree of parallelism", 8L, 8L, true, true }, writer.Values);
    }
}

public sealed class TraceFlagsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void NamesColumnsAndQuery_MatchSchema()
    {
        Assert.Equal("trace_flags", TraceFlagsCollector.Instance.Name);
        var query = TraceFlagsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("DBCC TRACESTATUS(-1)", query, StringComparison.Ordinal);
        Assert.Contains("#trace_flags", query, StringComparison.Ordinal);
        Assert.Equal(
            new[] { "trace_flag", "status", "is_global", "is_session" },
            TraceFlagsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public void AppliesTo_SkipsAzureSqlDb_CollectsElsewhere()
    {
        /* DBCC TRACESTATUS is unsupported on Azure SQL DB (edition 5). Gate collapsed from Lite's
           IsCollectorSupported into the shared AppliesTo so Darling records an explicit skip rather than a
           tolerated 0-row SUCCESS. Managed Instance and on-prem support DBCC. */
        Assert.False(TraceFlagsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public async Task ReadAndWrite_RoundTrip()
    {
        using var reader = new FakeCollectorDataReader(new object[] { 8048, true, true, false });

        var rows = await TraceFlagsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        TraceFlagsCollector.Instance.WritePayload(Assert.Single(rows), writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { 8048, true, true, false }, writer.Values);
    }
}

public sealed class DatabaseConfigCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext Ctx(int major, bool azure = false, string[]? excluded = null) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Deltas = s_deltas,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure, SqlMajorVersion = major },
        ExcludedDatabases = excluded ?? Array.Empty<string>(),
    };

    [Theory]
    [InlineData(13, false, false, false)]  /* 2016: base only */
    [InlineData(15, false, true, false)]   /* 2019: +ADR/memopt */
    [InlineData(17, false, true, true)]    /* 2025: +optimized locking */
    [InlineData(0, false, true, false)]    /* unknown: assume newest 2019 gate, not 2025 */
    [InlineData(13, true, true, true)]     /* Azure SQL Database: 2019 gate and optimized locking regardless of version */
    public void BuildQuery_VersionGates_MatchOriginalRules(int major, bool azure, bool expect2019, bool expect2025)
    {
        var text = DatabaseConfigCollector.Instance.BuildQuery(Ctx(major, azure)).Text;

        Assert.Equal(expect2019, text.Contains("is_accelerated_database_recovery_on", StringComparison.Ordinal));
        Assert.Equal(expect2019, text.Contains("is_memory_optimized_enabled", StringComparison.Ordinal));
        Assert.Equal(expect2025, text.Contains("is_optimized_locking_on", StringComparison.Ordinal));
        Assert.Contains("FROM sys.databases AS d", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_SplicesExclusionFilter()
    {
        var plan = DatabaseConfigCollector.Instance.BuildQuery(Ctx(16, excluded: new[] { "SO" }));

        Assert.Contains("AND d.name NOT IN (@excl_db_0)", plan.Text, StringComparison.Ordinal);
        Assert.Equal("SO", Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder_28Columns()
    {
        Assert.Equal(28, DatabaseConfigCollector.Instance.PayloadColumns.Count);
        Assert.Equal("database_name", DatabaseConfigCollector.Instance.PayloadColumns[0].Name);
        Assert.Equal("delayed_durability", DatabaseConfigCollector.Instance.PayloadColumns[24].Name);
        Assert.Equal("is_optimized_locking_on", DatabaseConfigCollector.Instance.PayloadColumns[27].Name);
    }

    [Fact]
    public async Task ReadAsync_2016Shape_LeavesGatedFieldsNull_AndWritesNulls()
    {
        using var reader = new FakeCollectorDataReader(BaseRow());

        var rows = await DatabaseConfigCollector.Instance.ReadAsync(reader, Ctx(13), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("SO", row.DbName);
        Assert.Equal(160, row.CompatLevel);
        Assert.True(row.Rcsi);
        Assert.Null(row.AcceleratedDatabaseRecovery);
        Assert.Null(row.OptimizedLocking);

        var writer = new RecordingCollectorRowWriter();
        DatabaseConfigCollector.Instance.WritePayload(row, writer, Ctx(13));
        Assert.Equal(28, writer.Values.Count);
        Assert.Null(writer.Values[25]);
        Assert.Null(writer.Values[26]);
        Assert.Null(writer.Values[27]);
    }

    [Fact]
    public async Task ReadAsync_2025Shape_ReadsAllGatedColumns()
    {
        var full = BaseRow().Concat(new object[] { true, false, true }).ToArray();
        using var reader = new FakeCollectorDataReader(full);

        var rows = await DatabaseConfigCollector.Instance.ReadAsync(reader, Ctx(17), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.True(row.AcceleratedDatabaseRecovery);
        Assert.False(row.MemoryOptimized);
        Assert.True(row.OptimizedLocking);

        var writer = new RecordingCollectorRowWriter();
        DatabaseConfigCollector.Instance.WritePayload(row, writer, Ctx(17));
        Assert.Equal(true, writer.Values[25]);
        Assert.Equal(false, writer.Values[26]);
        Assert.Equal(true, writer.Values[27]);
    }

    /// <summary>The 25 base (2016) columns in select order.</summary>
    private static object[] BaseRow() => new object[]
    {
        "SO", "ONLINE", 160, "SQL_Latin1_General_CP1_CI_AS", "FULL",
        false, false, false, true, true, false, true, "OFF", false, true,
        false, false, false, true, false, false, "NOTHING", "CHECKSUM", 60, "DISABLED",
    };
}
