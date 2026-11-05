/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// V215: the follow-up module read (<see cref="ProcedureStatsModuleDates.WatchedSql"/>, about 60,000 calls a day) asks for one
/// server's reported requests that name a condition. On a restored store copy loaded with one synthetic year of requests at a
/// production store's measured rate it took 127 ms before the indexes and 5.5 ms after. With a year of reported requests seeded
/// and the table analysed, the read must go through V215's partial index on the condition, and must not once that index is gone.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class AgentTableIndexesV215LiveTests
{
    private const string ConditionIndex = "ix_investigation_requests_reported_condition";

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN " + ProcedureStatsModuleDates.WatchedSql.Replace("$1", "1"), connection);
        var text = new StringBuilder();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            text.AppendLine(reader.GetString(0));
        }

        return text.ToString();
    }

    [Fact]
    public async Task TheFollowUpModuleRead_UsesTheConditionIndex_AndNotOnceItIsDropped()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* 73,000 requests over three servers, all reported, as a year at the measured rate. One in 200 names a module, so the partial
           index holds a few hundred rows and the table holds tens of thousands. Each request that names one is unfollowed. */
        await ExecAsync(connection, @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until,
     state, opened_at, expires_at, reported_at, follow_up_at, follow_up_condition)
SELECT gen_random_uuid(), 1, 1 + (n % 3), 'srv' || (n % 3), 'other', 'm',
       now() - interval '1 year' + n * interval '7 minutes', now() - interval '1 year' + n * interval '7 minutes', now(),
       'reported', now() - interval '1 year' + n * interval '7 minutes', now(), now(),
       CASE WHEN n % 200 = 0 THEN now() END,
       CASE WHEN n % 200 = 0 THEN 'module:db.dbo.proc' || n END
FROM generate_series(1, 73000) AS n", ct);
        await ExecAsync(connection, "ANALYZE config.investigation_requests", ct);

        var with = await ExplainAsync(connection, ct);
        Assert.Contains(ConditionIndex, with, StringComparison.Ordinal);

        await ExecAsync(connection, "DROP INDEX config." + ConditionIndex, ct);
        var without = await ExplainAsync(connection, ct);
        Assert.DoesNotContain(ConditionIndex, without, StringComparison.Ordinal);
        /* The control that fails on something: once the index is gone the read falls back to V184's (server_id, family, state)
           index, which reaches every reported request of the server. A plan that names neither means the seed never made the
           condition index load-bearing. Not a Seq Scan: that other index serves the read, so a Seq Scan would fail on no defect. */
        Assert.Contains("ix_investigation_requests_server_family", without, StringComparison.Ordinal);
    }
}
