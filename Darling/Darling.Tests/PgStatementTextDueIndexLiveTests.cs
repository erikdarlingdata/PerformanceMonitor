/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #6043: <see cref="PgStatementText.IsDueSql"/> asks for one server's newest <c>last_seen</c>, but the only index on the
/// column led with <c>last_seen</c> alone, so it walked back through other servers' newer rows. With several servers
/// seeded and the table analysed, the statement must read through the <c>(server_id, last_seen)</c> index once the
/// background step has built it, and must not before.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class PgStatementTextDueIndexLiveTests
{
    private const string NewIndex = "ix_pg_statement_text_server_last_seen";

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN " + PgStatementText.IsDueSql.Replace("$1", "7").Replace("$2::timestamp", "timestamp '2026-01-01'"), connection);
        var text = new System.Text.StringBuilder();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            text.AppendLine(reader.GetString(0));
        }

        return text.ToString();
    }

    [Fact]
    public async Task TheDueCheck_UsesTheServerLastSeenIndex_OnceTheBackgroundStepHasBuiltIt_AndNotBefore()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* 50 servers x 4,000 statements, last_seen interleaved across servers, so the old index's newest entries belong to
           other servers and the planner has a real choice. */
        await ExecAsync(connection, @"
INSERT INTO collect.pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
SELECT s, q, 'select ' || q, timestamp '2026-01-01' - interval '30 days',
       timestamp '2026-01-01' - interval '20 days' + (q * 50 + s) * interval '1 second'
FROM generate_series(1, 50) AS s CROSS JOIN generate_series(1, 4000) AS q", ct);
        await ExecAsync(connection, "ANALYZE collect.pg_statement_text", ct);

        var before = await ExplainAsync(connection, ct);
        Assert.DoesNotContain(NewIndex, before, StringComparison.Ordinal);

        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.StatementTextServerLastSeen, NullLogger.Instance, ct);
        await ExecAsync(connection, "ANALYZE collect.pg_statement_text", ct);

        var after = await ExplainAsync(connection, ct);
        Assert.Contains(NewIndex, after, StringComparison.Ordinal);

        /* The index must serve the server equality as a seek, not as a filter over another index's entries. */
        Assert.Matches(@"Index Cond: .*server_id = 7", after);
    }
}
