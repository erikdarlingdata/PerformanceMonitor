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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, #5320 (lane R3): the two Darling Query Store writers against a REAL store. The text writer and the plan
/// writer judge each fetched value before it is stored, and a value the filter could not judge (its per-batch
/// budget is spent) is dropped from the batch, not stored as the marker: both stores are write-once per id, and an
/// absent row is exactly what the missing-set probe reads as "fetch me again".
/// </summary>
[Collection("live-postgres")]
public sealed class StatementScrubQueryStoreLiveTests
{
    private const string ServerName = "darling-statement-scrub-qs-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "ScrubDb";
    private const int TestTimeoutSeconds = 30;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>A judge that charges a fixed cost to a fake clock for every value it is asked about.</summary>
    private sealed class FakeJudge(TimeSpan cost)
    {
        public TimeSpan Now { get; private set; }

        public int Calls { get; private set; }

        public Func<TimeSpan> Clock => () => Now;

        public SensitiveStatements.Verdict Judge(string text)
        {
            Calls++;
            Now += cost;
            return SensitiveStatements.Verdict.Clean;
        }
    }

    private static SensitiveStatements.Session SessionSpentAfter(FakeJudge fake, double seconds) =>
        new(fake.Judge, fake.Clock, TimeSpan.FromSeconds(seconds), new SensitiveStatements.TimedOutMemo());

    private static async Task<Dictionary<long, string?>> StoredTextsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var result = new Dictionary<long, string?>();
        using var command = new NpgsqlCommand(
            "SELECT query_id, query_sql_text FROM collect.query_store_text WHERE server_id = $1 AND database_name = $2", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(Db);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetInt64(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        return result;
    }

    private static async Task<Dictionary<long, byte[]?>> MapDigestsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var result = new Dictionary<long, byte[]?>();
        using var command = new NpgsqlCommand(
            "SELECT plan_id, digest FROM collect.query_store_plan_map WHERE server_id = $1 AND database_name = $2", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(Db);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetInt64(0)] = reader.IsDBNull(1) ? null : (byte[])reader.GetValue(1);
        }

        return result;
    }

    private static async Task<string?> DimContentAsync(NpgsqlConnection connection, byte[] digest, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand("SELECT query_plan_gz FROM collect.query_plan_dim WHERE digest = $1", connection);
        command.Parameters.AddWithValue(digest);
        var value = await command.ExecuteScalarAsync(ct);
        return value is byte[] compressed ? PayloadDimensions.DecompressContent(compressed) : null;
    }

    [Fact]
    public async Task QueryStoreText_StoresTheMarkerForANamedStatement_AndTheTextForAPlainOne()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live statement filter test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var landed = await QueryStoreTextWriter.WriteAsync(
                connection, ServerId, Db,
                new[]
                {
                    new FetchedQueryText(1, StatementScrubCanary.CanaryStatement, "0xAA"),
                    new FetchedQueryText(2, StatementScrubCanary.PlainStatement, "0xBB"),
                    new FetchedQueryText(3, null, "0xCC"),
                },
                DateTime.UtcNow, TestTimeoutSeconds, cancellationToken: ct);

            Assert.Equal(new long[] { 1, 2, 3 }, landed);
            var stored = await StoredTextsAsync(connection, ct);
            Assert.Equal(SensitiveStatements.PlaceholderText, stored[1]);
            Assert.Equal(StatementScrubCanary.PlainStatement, stored[2]);
            Assert.Null(stored[3]);
            foreach (var needle in StatementScrubCanary.SecretNeedles)
            {
                Assert.DoesNotContain(stored.Values, v => v is not null && v.Contains(needle, StringComparison.Ordinal));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// r2 M-D: a session that spends its budget after three values leaves ids 4 and up ABSENT (not the marker), the
    /// writer does not return them, and the next cycle's fresh session stores them with their real text.
    /// </summary>
    [Fact]
    public async Task QueryStoreText_DropsTheRowsTheBudgetLeftUnjudged_AndTheNextCycleStoresThem()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live statement filter test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var texts = Enumerable.Range(1, 6)
                .Select(i => new FetchedQueryText(i, "SELECT " + i + " FROM dbo.t", "0x0" + i))
                .ToArray();

            var fake = new FakeJudge(TimeSpan.FromSeconds(1));
            var landed = await QueryStoreTextWriter.WriteAsync(
                connection, ServerId, Db, texts, DateTime.UtcNow, TestTimeoutSeconds, SessionSpentAfter(fake, 3), ct);

            Assert.Equal(new long[] { 1, 2, 3 }, landed);
            var afterFirst = await StoredTextsAsync(connection, ct);
            Assert.Equal(new long[] { 1, 2, 3 }, afterFirst.Keys.OrderBy(k => k).ToArray());
            Assert.DoesNotContain(afterFirst.Values, v => v == SensitiveStatements.PlaceholderText);

            /* The next cycle: the probe sees ids 4-6 as missing, the fetch ships them, a fresh session judges them. */
            var second = await QueryStoreTextWriter.WriteAsync(
                connection, ServerId, Db, texts.Skip(3).ToArray(), DateTime.UtcNow, TestTimeoutSeconds, cancellationToken: ct);

            Assert.Equal(new long[] { 4, 5, 6 }, second);
            var afterSecond = await StoredTextsAsync(connection, ct);
            Assert.Equal(6, afterSecond.Count);
            Assert.Equal("SELECT 5 FROM dbo.t", afterSecond[5]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The plan map's digest resolves to a dimension row holding the FILTERED plan: the digest is of the filtered XML,
    /// the raw plan's digest has no dimension row, the stored plan holds no secret needle and keeps statement 2.
    /// </summary>
    [Fact]
    public async Task QueryStorePlan_DigestResolvesToTheFilteredPlan()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live statement filter test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var canary = StatementScrubCanary.CanaryPlan();
        var filtered = new SensitiveStatements.Session().Xml(canary)!;
        var rawDigest = PayloadDimensions.Digest(canary);
        var filteredDigest = PayloadDimensions.Digest(filtered);
        await DeleteRowsAsync(connection, ct, rawDigest, filteredDigest);

        var bodySucceeded = false;
        try
        {
            Assert.NotEqual(canary, filtered);

            var landed = await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db, new[] { new FetchedPlan(1, canary, "0xAAAA") }, DateTime.UtcNow, TestTimeoutSeconds, cancellationToken: ct);

            Assert.Equal(new long[] { 1 }, landed);
            var map = await MapDigestsAsync(connection, ct);
            Assert.Equal(filteredDigest, map[1]);
            Assert.Null(await DimContentAsync(connection, rawDigest, ct));

            var stored = await DimContentAsync(connection, filteredDigest, ct);
            Assert.Equal(filtered, stored);
            foreach (var needle in StatementScrubCanary.SecretNeedles)
            {
                Assert.DoesNotContain(needle, stored, StringComparison.Ordinal);
            }

            Assert.Contains(SensitiveStatements.PlaceholderText, stored, StringComparison.Ordinal);
            foreach (var needle in StatementScrubCanary.KeptNeedles)
            {
                Assert.Contains(needle, stored, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt, rawDigest, filteredDigest));
        }
    }

    /// <summary>
    /// r2 M-D, plan side: a plan the budget left unjudged gets NO map row (so the probe reads it as missing and
    /// refetches it), is not in the returned list, and the next cycle stores it. A plan with NO XML still gets its
    /// content-less map row.
    /// </summary>
    [Fact]
    public async Task QueryStorePlan_DropsThePlansTheBudgetLeftUnjudged_AndTheNextCycleStoresThem()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live statement filter test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var plans = Enumerable.Range(1, 4)
            .Select(i => new FetchedPlan(i, "<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText=\"SELECT scrub_qs_" + i + "\"/></Statements></Batch></BatchSequence></ShowPlanXML>", "0x0" + i))
            .ToArray();
        var digests = plans.Select(p => PayloadDimensions.Digest(p.PlanXml!)).ToArray();
        await DeleteRowsAsync(connection, ct, digests);

        var bodySucceeded = false;
        try
        {
            /* A fake judge that spends the budget after the first judged value: whatever a plan's document judges, the
               first plan is the only one fully judged; the plans after it reach a spent budget. */
            var fake = new FakeJudge(TimeSpan.FromSeconds(1));
            var probe = SessionSpentAfter(fake, 1);
            probe.TryXml(plans[0].PlanXml, out _);
            var perPlan = fake.Calls;
            Assert.True(perPlan >= 1, "the fake judge must be asked about at least one value per plan");

            var spent = SessionSpentAfter(new FakeJudge(TimeSpan.FromSeconds(1)), perPlan * 2 + 0.5);
            var landed = await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db, plans.Append(new FetchedPlan(5, null, "0x05")).ToArray(),
                DateTime.UtcNow, TestTimeoutSeconds, spent, ct);

            Assert.Equal(new long[] { 1, 2, 5 }, landed);
            var afterFirst = await MapDigestsAsync(connection, ct);
            Assert.Equal(new long[] { 1, 2, 5 }, afterFirst.Keys.OrderBy(k => k).ToArray());
            Assert.Null(afterFirst[5]);

            var second = await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db, plans.Skip(2).ToArray(), DateTime.UtcNow, TestTimeoutSeconds, cancellationToken: ct);

            Assert.Equal(new long[] { 3, 4 }, second);
            var afterSecond = await MapDigestsAsync(connection, ct);
            Assert.Equal(5, afterSecond.Count);
            Assert.Equal(digests[3], afterSecond[4]);
            Assert.NotNull(await DimContentAsync(connection, digests[3], ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt, digests));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, params byte[][] digests)
    {
        if (digests.Length > 0)
        {
            using var dimension = new NpgsqlCommand("DELETE FROM collect.query_plan_dim WHERE digest = ANY($1)", connection);
            dimension.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bytea, digests);
            await dimension.ExecuteNonQueryAsync(ct);
        }

        var sql =
            $"DELETE FROM collect.query_store_plan_map WHERE server_id = {ServerId};" +
            $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
