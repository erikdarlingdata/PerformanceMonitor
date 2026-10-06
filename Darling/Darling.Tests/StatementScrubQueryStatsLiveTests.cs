/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter, lane R2) against a real PostgreSQL (gated on DARLING_TEST_PG): what <c>query_stats</c>
/// leaves in <c>query_text_dim</c> and <c>query_plan_dim</c> after the statement filter ran at collection time.
/// Rows go through the collector's own <c>ReadAsync</c> and <c>WritePayload</c>, the real
/// <see cref="PgCollectorRowWriter"/> and <see cref="PayloadDimensionWriter"/>, in one transaction as the host does.
///
/// <para>The cached digest of the deferred plan fetch is computed exactly as the host computes it (the digest of
/// the filtered plan), so a digest-only row written from it must resolve to the filtered dim row, and a plan the
/// filter withheld whole must resolve to the marker's dim row while the next cycle's filtered plan resolves to its
/// own (no digest-only write is ever left without a dim row).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StatementScrubQueryStatsLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the statement filter store tests.";

    private static readonly DateTime Anchor = new(2031, 4, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    private static Type ColumnType(int ordinal) => ordinal switch
    {
        3 or 4 => typeof(DateTime),
        0 or 1 or 2 or 36 or 37 or 38 or 42 => typeof(string),
        40 or 41 or 43 => typeof(int),
        _ => typeof(long),
    };

    /// <summary>One inline-plan query_stats row, read through the collector (so the filter runs where it does live).</summary>
    private static async Task<QueryStatsCollector.Row> ReadRowAsync(
        string queryHash, string text, string plan, int serverId, string serverName)
    {
        var context = ContextFor(serverId, serverName, Anchor);
        using var table = new DataTable();
        for (var i = 0; i < 44; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), ColumnType(i));
        }

        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        var values = new object[table.Columns.Count];
        for (var i = 0; i < 44; i++)
        {
            values[i] = DBNull.Value;
        }

        values[1] = queryHash;
        values[36] = "0x" + queryHash;
        values[37] = "0x" + queryHash;
        values[38] = text;
        values[44] = plan;
        values[45] = (long)plan.Length;
        table.Rows.Add(values);

        await using var reader = table.CreateDataReader();
        var rows = await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        return Assert.Single(rows);
    }

    private static CollectorContext ContextFor(int serverId, string serverName, DateTime collectionTime) => new()
    {
        ServerId = serverId,
        ServerName = serverName,
        CollectionTime = collectionTime,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
        CapturePlanXml = true,
    };

    /// <summary>The digest the host caches for a plan it fetched: the digest of the filtered plan.</summary>
    private static string CachedDigestOf(string filteredPlan) =>
        Convert.ToHexString(PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(filteredPlan)));

    private static async Task<IReadOnlyList<string>> WriteBatchAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime collectionTime,
        IReadOnlyList<QueryStatsCollector.Row> rows, CancellationToken ct)
    {
        var definition = QueryStatsCollector.Instance;
        var context = ContextFor(serverId, serverName, collectionTime);
        var writer = new PgCollectorRowWriter();
        var dimensions = new PayloadDimensionBatch();
        writer.UseDimensions(PayloadDimensions.DiversionPlanFor(definition), dimensions);

        await using var transaction = await connection.BeginTransactionAsync(ct);
        using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
        {
            writer.Importer = importer;
            foreach (var row in rows)
            {
                await importer.StartRowAsync(ct);
                writer.Value(CollectionIdGenerator.Next());
                writer.Value(collectionTime).Value(serverId).Value(serverName);
                writer.BeginPayload();
                definition.WritePayload(row, writer, context);
                writer.EndPayload(definition.PayloadColumns.Count);
            }

            await importer.CompleteAsync(ct);
        }

        var absent = await PayloadDimensionWriter.FlushAsync(connection, transaction, dimensions, collectionTime, ct);
        await transaction.CommitAsync(ct);
        return absent;
    }

    private static async Task<NpgsqlConnection> OpenMigratedStoreAsync(string connectionString, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        return connection;
    }

    /// <summary>The text and plan a stored row resolves to through the view, the way every reader sees them.</summary>
    private static async Task<(string? Text, string? Plan)> ResolvedAsync(
        NpgsqlConnection connection, int serverId, string queryHash, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT query_text, query_plan_xml, query_plan_gz FROM v_query_stats WHERE server_id = $1 AND query_hash = $2", connection);
        read.Parameters.AddWithValue(serverId);
        read.Parameters.AddWithValue(queryHash);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the row must exist");
        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            PayloadDimensions.ResolveContent(
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetFieldValue<byte[]>(2)));
    }

    private static async Task CleanupAsync(NpgsqlConnection cleanup, int serverId, IEnumerable<byte[]> planDigests, CancellationToken ct)
    {
        await using (var facts = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", cleanup))
        {
            facts.Parameters.AddWithValue(serverId);
            await facts.ExecuteNonQueryAsync(ct);
        }

        foreach (var digest in planDigests)
        {
            await using var dim = new NpgsqlCommand("DELETE FROM query_plan_dim WHERE digest = $1", cleanup);
            dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
            await dim.ExecuteNonQueryAsync(ct);
        }

        await using var text = new NpgsqlCommand("DELETE FROM query_text_dim WHERE digest = $1", cleanup);
        text.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Bytea,
            Value = PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(SensitiveStatements.PlaceholderText)),
        });
        await text.ExecuteNonQueryAsync(ct);
    }

    private static (int ServerId, string ServerName) NewServer() =>
        (-Random.Shared.Next(1_000_000, int.MaxValue), "pm4348-" + Guid.NewGuid().ToString("N")[..12]);

    [Fact]
    public async Task TheDimensionsHoldTheMarker_AndTheCachedDigestResolvesToTheFilteredPlanOnTheNextCycle()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var rawPlan = StatementScrubCanary.CanaryPlan();
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var digests = new List<byte[]>();
        var bodySucceeded = false;
        try
        {
            var first = await ReadRowAsync("0xA1", StatementScrubCanary.CanaryStatement, rawPlan, serverId, serverName);
            var filteredDigest = CachedDigestOf(first.QueryPlanXml!);
            digests.Add(Convert.FromHexString(filteredDigest));
            Assert.Equal(filteredDigest, CachedDigestOf((await ReadRowAsync("0xA2", StatementScrubCanary.CanaryStatement, rawPlan, serverId, serverName)).QueryPlanXml!));

            await WriteBatchAsync(connection, serverId, serverName, Anchor, new[] { first }, ct);

            /* The second cycle: the host has the digest and sends no plan text. */
            var second = await ReadRowAsync("0xB1", StatementScrubCanary.PlainStatement, rawPlan, serverId, serverName);
            second.QueryPlanXml = null;
            second.KnownPlanDigest = filteredDigest;
            var absent = await WriteBatchAsync(connection, serverId, serverName, Anchor.AddMinutes(1), new[] { second }, ct);

            Assert.Empty(absent);
            var (text1, plan1) = await ResolvedAsync(connection, serverId, "0xA1", ct);
            var (text2, plan2) = await ResolvedAsync(connection, serverId, "0xB1", ct);
            Assert.Equal(SensitiveStatements.PlaceholderText, text1);
            Assert.Equal(StatementScrubCanary.PlainStatement, text2);
            Assert.Equal(first.QueryPlanXml, plan1);
            Assert.Equal(first.QueryPlanXml, plan2);
            StatementFilterCensus.AssertPlanFilteredKeepsTheRest(rawPlan, plan2!);

            /* Nothing the raw statement or the raw plan would have stored is in the dimensions: neither raw digest has a
               row, and the stored plan holds no secret needle. */
            Assert.Equal(0L, await CountByDigestAsync(
                connection, "query_text_dim", PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(StatementScrubCanary.CanaryStatement)), ct));
            Assert.Equal(0L, await CountByDigestAsync(
                connection, "query_plan_dim", PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(rawPlan)), ct));
            Assert.Equal(1L, await CountByDigestAsync(
                connection, "query_text_dim", PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(SensitiveStatements.PlaceholderText)), ct));
            Assert.Equal(1L, await CountByDigestAsync(connection, "query_plan_dim", digests[0], ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, digests, cleanupCt));
        }
    }

    /// <summary>r2 Q5: a plan the filter withheld whole stores the marker's dim row; the next cycle's filtered plan stores
    /// its own; a digest-only write resolves for each.</summary>
    [Fact]
    public async Task AWholeMarkerPlan_AndTheNextCyclesFilteredPlan_BothResolveToDimRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var rawPlan = StatementScrubCanary.CanaryPlan();
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var digests = new List<byte[]>();
        var bodySucceeded = false;
        try
        {
            var whole = await ReadRowAsync("0xC1", StatementScrubCanary.PlainStatement, rawPlan, serverId, serverName);
            whole.QueryPlanXml = SensitiveStatements.PlaceholderText;
            var filtered = await ReadRowAsync("0xC2", StatementScrubCanary.PlainStatement, rawPlan, serverId, serverName);
            var wholeDigest = CachedDigestOf(SensitiveStatements.PlaceholderText);
            var filteredDigest = CachedDigestOf(filtered.QueryPlanXml!);
            digests.Add(Convert.FromHexString(wholeDigest));
            digests.Add(Convert.FromHexString(filteredDigest));
            Assert.NotEqual(wholeDigest, filteredDigest);

            Assert.Empty(await WriteBatchAsync(connection, serverId, serverName, Anchor, new[] { whole }, ct));
            Assert.Empty(await WriteBatchAsync(connection, serverId, serverName, Anchor.AddMinutes(1), new[] { filtered }, ct));

            var digestOnly = await ReadRowAsync("0xC3", StatementScrubCanary.PlainStatement, rawPlan, serverId, serverName);
            digestOnly.QueryPlanXml = null;
            digestOnly.KnownPlanDigest = filteredDigest;
            Assert.Empty(await WriteBatchAsync(connection, serverId, serverName, Anchor.AddMinutes(2), new[] { digestOnly }, ct));

            Assert.Equal(SensitiveStatements.PlaceholderText, (await ResolvedAsync(connection, serverId, "0xC1", ct)).Plan);
            Assert.Equal(filtered.QueryPlanXml, (await ResolvedAsync(connection, serverId, "0xC2", ct)).Plan);
            Assert.Equal(filtered.QueryPlanXml, (await ResolvedAsync(connection, serverId, "0xC3", ct)).Plan);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, digests, cleanupCt));
        }
    }

    private static async Task<long> CountByDigestAsync(NpgsqlConnection connection, string table, byte[] digest, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM " + table + " WHERE digest = $1", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}
