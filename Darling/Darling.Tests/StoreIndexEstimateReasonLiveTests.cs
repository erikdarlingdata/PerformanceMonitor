/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The class reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. */

/// <summary>
/// #9105: an index the store health read has no bloat estimate for says why, and the reason matches the index. The estimate covers
/// valid, ready btree indexes; a valid GIN or hash index used to say "The index is not valid or not ready" beside <c>valid: true</c>.
/// The reason names the access method for an index the estimate does not cover, keeps the invalid text for an index that is not valid,
/// and a valid index never carries the invalid text.
/// </summary>
public sealed class StoreIndexEstimateReasonLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnIndexTheEstimateDoesNotCover_NamesItsAccessMethod_AndNeverContradictsValid()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the store index estimate reason test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, @"
CREATE TABLE public.zz_reason_t (id int NOT NULL, dup int NOT NULL, tags int[] NOT NULL, note text NOT NULL);
INSERT INTO public.zz_reason_t
SELECT g, g % 10, ARRAY[g % 50, g % 7], 'note ' || g FROM generate_series(1, 5000) AS g;
CREATE INDEX zz_reason_btree ON public.zz_reason_t (note);
CREATE INDEX zz_reason_gin ON public.zz_reason_t USING gin (tags);
CREATE INDEX zz_reason_hash ON public.zz_reason_t USING hash (id);
ANALYZE public.zz_reason_t;", ct);

        /* A unique index built concurrently over duplicate values fails and leaves an invalid index behind. */
        await Assert.ThrowsAsync<PostgresException>(() =>
            ExecAsync(connection, "CREATE UNIQUE INDEX CONCURRENTLY zz_reason_bad ON public.zz_reason_t (dup)", ct));

        var rows = (await DarlingStoreTableHealthReader.GetIndexesAsync(dataSource, ct))
            .Where(r => r.TableName == "zz_reason_t")
            .ToDictionary(r => r.IndexName, StringComparer.Ordinal);

        var gin = rows["zz_reason_gin"];
        Assert.True(gin.Valid);
        Assert.Equal("The estimate covers btree indexes only; this is a gin index.", gin.EstimateSkippedReason);

        var hash = rows["zz_reason_hash"];
        Assert.True(hash.Valid);
        Assert.Equal("The estimate covers btree indexes only; this is a hash index.", hash.EstimateSkippedReason);

        var bad = rows["zz_reason_bad"];
        Assert.False(bad.Valid);
        Assert.Equal(DarlingStoreTableHealthReader.InvalidIndexReason, bad.EstimateSkippedReason);

        var btree = rows["zz_reason_btree"];
        Assert.True(btree.Valid);
        Assert.NotEqual(DarlingStoreTableHealthReader.InvalidIndexReason, btree.EstimateSkippedReason);
        Assert.DoesNotContain("btree indexes only", btree.EstimateSkippedReason ?? "", StringComparison.Ordinal);

        /* The two fields never disagree, on any index the store holds. */
        foreach (var row in rows.Values)
        {
            Assert.Equal(!row.Valid, row.EstimateSkippedReason == DarlingStoreTableHealthReader.InvalidIndexReason && row.EstimateIndexesUsable == 0);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
