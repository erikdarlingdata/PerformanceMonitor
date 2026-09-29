/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4793: the MCP readers share ONE clock read. It takes the time zone id and the offset from the same newest
/// <c>server_properties</c> row, and a store below the migration that added <c>time_zone_id</c> falls back to a
/// read of the offset alone. The SQL is pinned here without a live database.
/// </summary>
public sealed class DarlingServerClockReaderTests
{
    [Fact]
    public void TheClockSql_ReadsTheZoneAndTheOffset_FromTheNewestRowThatHasAnOffset()
    {
        var sql = DarlingServerClockReader.ServerClockSql;

        Assert.Contains("SELECT sp.utc_offset_minutes, sp.time_zone_id", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties AS sp", sql, StringComparison.Ordinal);
        Assert.Contains("sp.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("sp.utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sp.collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOffsetOnlySql_IsTheSameReadWithoutTheZoneColumn()
    {
        var sql = DarlingServerClockReader.ServerOffsetSql;

        Assert.DoesNotContain("time_zone_id", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT sp.utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("sp.utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sp.collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }
}
