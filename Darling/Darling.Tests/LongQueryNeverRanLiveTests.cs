/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #8571 item 3: get_long_query_completions answered <c>empty</c> for a collector that has never run for the server, while its own hint
/// said nothing was read. A collector that never ran is <c>not_collected</c> with the enable step, as get_schema_changes answers; one
/// that ran and found nothing stays <c>empty</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class LongQueryNeverRanLiveTests
{
    private const string Cleanup =
        "DELETE FROM long_query_completions WHERE server_id = {0}; DELETE FROM collection_log WHERE server_id = {0}";

    [Fact]
    public Task ACollectorThatNeverRan_IsNotCollected_WithTheEnableStep() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("i8571-lq-never", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var json = await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, serverName, 24, 10, cancellationToken: ct);

            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(json));
            var message = JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;
            Assert.Contains("has never run for it", message, StringComparison.Ordinal);
            Assert.Contains("enable_collector (collector long_query_completions)", message, StringComparison.Ordinal);
        }, Cleanup);

    [Fact]
    public Task ACollectorThatRanAndFoundNothing_StaysEmpty() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("i8571-lq-ran", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected) VALUES ($1, $2, $3, 'long_query_completions', $4, 'SUCCESS', 0)",
                CollectionIdGenerator.Next(), serverId, serverName, DarlingMcpTestData.Naive(now.AddMinutes(-5)));

            var json = await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, serverName, 24, 10, cancellationToken: ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
        }, Cleanup);
}
