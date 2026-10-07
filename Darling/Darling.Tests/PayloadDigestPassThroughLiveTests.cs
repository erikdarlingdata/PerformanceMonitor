/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store side of writing a <c>query_stats</c> row from a plan digest the host already holds (#5158),
/// against a real PostgreSQL (gated on DARLING_TEST_PG): the row's digest column gets the digest with no
/// plan text and no dim insert, the dim row's <c>last_seen</c> is kept alive under the same 6-hour guard as
/// the insert path, and a digest with no dim row is reported back instead of failing the batch.
///
/// <para>Rows are written through the collector definition's own <c>WritePayload</c> and the real
/// <see cref="PgCollectorRowWriter"/> and <see cref="PayloadDimensionWriter"/>, in one transaction as the
/// host does; the only stand-in is <see cref="DigestRoutingWriter"/>, which hands the plan column's write to
/// <c>PayloadOrDigest</c> the way the collector will.</para>
///
/// <para>All times are fixed anchors well ahead of the clock, so no chunk boundary, retention job or
/// wall-clock straddle can move a row under a test.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PayloadDigestPassThroughLiveTests
{
    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the payload digest pass-through store tests.";

    private static readonly DateTime Anchor = new(2031, 3, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private static (int ServerId, string ServerName) NewServer()
    {
        var suffix = Guid.NewGuid().ToString("N")[..12];
        return (-Random.Shared.Next(1_000_000, int.MaxValue), "pm5158-" + suffix);
    }

    private static QueryStatsCollector.Row NewRow(string queryHash, string? planXml)
        => new()
        {
            DatabaseName = "pm5158",
            QueryHash = queryHash,
            QueryPlanHash = "0xPLANHASH",
            SqlHandle = "0x" + queryHash,
            PlanHandle = "0x" + queryHash,
            QueryText = null,
            QueryPlanXml = planXml,
        };

    /// <summary>
    /// Forwards every write, except that the plan column's null goes to PayloadOrDigest with a digest. A
    /// PayloadOrDigest call the collector makes itself at the plan column forwards its own digest when it has one
    /// and this writer's digest otherwise, so the stand-in stays correct once the collector routes its own.
    /// </summary>
    private sealed class DigestRoutingWriter : ICollectorRowWriter
    {
        private readonly PgCollectorRowWriter _inner;
        private readonly int _planOrdinal;
        private readonly string? _knownDigest;
        private int _index;

        public DigestRoutingWriter(PgCollectorRowWriter inner, int planOrdinal, string? knownDigest)
        {
            _inner = inner;
            _planOrdinal = planOrdinal;
            _knownDigest = knownDigest;
        }

        public ICollectorRowWriter Value(string? value)
        {
            if (_index++ == _planOrdinal && _knownDigest is not null)
            {
                _inner.PayloadOrDigest(value, _knownDigest);
                return this;
            }

            _inner.Value(value);
            return this;
        }

        public ICollectorRowWriter PayloadOrDigest(string? content, string? knownDigest)
        {
            var at = _index++;
            _inner.PayloadOrDigest(content, knownDigest ?? (at == _planOrdinal ? _knownDigest : null));
            return this;
        }

        private ICollectorRowWriter Pass(Action write) { _index++; write(); return this; }

        public ICollectorRowWriter Value(long value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(long? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(int value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(int? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(short value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(short? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(double value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(double? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(decimal value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(decimal? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(bool value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(bool? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(DateTime value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter Value(DateTime? value) => Pass(() => _inner.Value(value));
        public ICollectorRowWriter NullValue() => Pass(() => _inner.NullValue());
    }

    /// <summary>
    /// Writes one query_stats batch and commits it, the way the host does: COPY and the dimension flush in
    /// one transaction. A row whose entry in <paramref name="digests"/> is non-null is written from that
    /// digest (its QueryPlanXml must be null); returns the flush's absent-digest list.
    /// </summary>
    private static async Task<IReadOnlyList<string>> WriteBatchAsync(
        NpgsqlConnection connection,
        int serverId,
        string serverName,
        DateTime collectionTime,
        IReadOnlyList<(QueryStatsCollector.Row Row, string? KnownDigest)> rows,
        CancellationToken ct)
    {
        var definition = QueryStatsCollector.Instance;
        var context = new CollectorContext
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
        };

        var planOrdinal = -1;
        for (var i = 0; i < definition.PayloadColumns.Count; i++)
        {
            if (definition.PayloadColumns[i].Name == "query_plan_xml")
            {
                planOrdinal = i;
            }
        }

        Assert.True(planOrdinal >= 0);

        var writer = new PgCollectorRowWriter();
        var dimensions = new PayloadDimensionBatch();
        writer.UseDimensions(PayloadDimensions.DiversionPlanFor(definition), dimensions);

        await using var transaction = await connection.BeginTransactionAsync(ct);
        IReadOnlyList<string> absent;
        using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
        {
            writer.Importer = importer;
            foreach (var (row, knownDigest) in rows)
            {
                await importer.StartRowAsync(ct);
                writer.Value(CollectionIdGenerator.Next());
                writer.Value(collectionTime).Value(serverId).Value(serverName);

                writer.BeginPayload();
                definition.WritePayload(row, new DigestRoutingWriter(writer, planOrdinal, knownDigest), context);
                writer.EndPayload(definition.PayloadColumns.Count);
            }

            await importer.CompleteAsync(ct);
        }

        absent = await PayloadDimensionWriter.FlushAsync(connection, transaction, dimensions, collectionTime, ct);
        await transaction.CommitAsync(ct);
        return absent;
    }

    /// <summary>
    /// The ONE place this file opens a connection. Production names the dimension tables bare
    /// (PayloadDimensions.QueryPlanDimTable is "query_plan_dim") and resolves them through the data source's
    /// search_path (DarlingManagedPostgres.BuildConnectionString sets NpgsqlConnectionStringBuilder.SearchPath).
    /// Every connection opens the same way, so the bare names resolve as they do live; a connection added later
    /// should come through here too.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenWithStoreSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        return connection;
    }

    private static async Task<NpgsqlConnection> OpenMigratedStoreAsync(string connectionString, CancellationToken ct)
    {
        var connection = await OpenWithStoreSearchPathAsync(connectionString, ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<string?> ResolvedPlanAsync(NpgsqlConnection connection, int serverId, string queryHash, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT query_plan_xml, query_plan_gz FROM v_query_stats WHERE server_id = $1 AND query_hash = $2", connection);
        read.Parameters.AddWithValue(serverId);
        read.Parameters.AddWithValue(queryHash);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the row must exist");
        return PayloadDimensions.ResolveContent(
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1));
    }

    private static async Task<DateTime?> LastSeenAsync(NpgsqlConnection connection, byte[] digest, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand("SELECT last_seen FROM query_plan_dim WHERE digest = $1", connection);
        read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
        var value = await read.ExecuteScalarAsync(ct);
        return value is DateTime stamp ? stamp : null;
    }

    private static async Task SetLastSeenAsync(NpgsqlConnection connection, byte[] digest, DateTime lastSeen, CancellationToken ct)
    {
        await using var update = new NpgsqlCommand("UPDATE query_plan_dim SET last_seen = $2 WHERE digest = $1", connection);
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = lastSeen });
        await update.ExecuteNonQueryAsync(ct);
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
    }

    [Fact]
    public async Task ADigestOnlyRow_ResolvesItsPlan_ThroughTheViewExactlyAsAContentRowDoes()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var planXml = $"<ShowPlanXML server=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var digest = PayloadDimensions.Digest(planXml);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteBatchAsync(connection, serverId, serverName, Anchor,
                new[] { (NewRow("0xA", planXml), (string?)null) }, ct);

            var absent = await WriteBatchAsync(connection, serverId, serverName, Anchor.AddMinutes(1),
                new[] { (NewRow("0xB", null), (string?)Convert.ToHexString(digest)) }, ct);

            Assert.Empty(absent);
            Assert.Equal(planXml, await ResolvedPlanAsync(connection, serverId, "0xA", ct));
            Assert.Equal(planXml, await ResolvedPlanAsync(connection, serverId, "0xB", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM query_plan_dim WHERE digest = $1", ct, digest));
            Assert.Equal(2L, await ScalarAsync(
                connection, "SELECT count(*) FROM query_stats WHERE server_id = $1 AND query_plan_digest = $2", ct, serverId, digest));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, new[] { digest }, cleanupCt));
        }
    }

    [Fact]
    public async Task TheTouch_MovesLastSeenOnlyWhenItIsOlderThanTheSixHourGuard()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var planXml = $"<ShowPlanXML server=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var digest = PayloadDimensions.Digest(planXml);
        var hex = Convert.ToHexString(digest);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteBatchAsync(connection, serverId, serverName, Anchor,
                new[] { (NewRow("0xA", planXml), (string?)null) }, ct);

            /* Inside the guard: stored 5h59m behind the touching batch, so it must NOT move. */
            var inside = Anchor.AddHours(10).AddMinutes(-(60 * PayloadDimensions.LastSeenRefreshGuardHours - 1));
            await SetLastSeenAsync(connection, digest, inside, ct);
            await WriteBatchAsync(connection, serverId, serverName, Anchor.AddHours(10),
                new[] { (NewRow("0xB", null), (string?)hex) }, ct);
            Assert.Equal(inside, await LastSeenAsync(connection, digest, ct));

            /* Past the guard: stored 6h01m behind, so it MUST move to the batch's time. */
            var past = Anchor.AddHours(10).AddMinutes(-(60 * PayloadDimensions.LastSeenRefreshGuardHours + 1));
            await SetLastSeenAsync(connection, digest, past, ct);
            await WriteBatchAsync(connection, serverId, serverName, Anchor.AddHours(10),
                new[] { (NewRow("0xC", null), (string?)hex) }, ct);
            Assert.Equal(Anchor.AddHours(10), await LastSeenAsync(connection, digest, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, new[] { digest }, cleanupCt));
        }
    }

    [Fact]
    public async Task AnAbsentDigest_IsReportedBack_AndItsRowReadsANullPlan()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var presentPlan = $"<ShowPlanXML server=\"{serverName}\" present=\"1\"><StmtSimple/></ShowPlanXML>";
        var present = PayloadDimensions.Digest(presentPlan);
        var missing = PayloadDimensions.Digest($"<ShowPlanXML server=\"{serverName}\" missing=\"1\"/>");

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteBatchAsync(connection, serverId, serverName, Anchor,
                new[] { (NewRow("0xA", presentPlan), (string?)null) }, ct);

            var absent = await WriteBatchAsync(connection, serverId, serverName, Anchor.AddMinutes(1),
                new[]
                {
                    (NewRow("0xB", null), (string?)Convert.ToHexString(present)),
                    (NewRow("0xC", null), (string?)Convert.ToHexString(missing)),
                }, ct);

            Assert.Equal(new[] { Convert.ToHexString(missing) }, absent);
            Assert.Equal(presentPlan, await ResolvedPlanAsync(connection, serverId, "0xB", ct));
            Assert.Null(await ResolvedPlanAsync(connection, serverId, "0xC", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM query_plan_dim WHERE digest = $1", ct, missing));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, new[] { present, missing }, cleanupCt));
        }
    }

    [Fact]
    public async Task ARowWithContent_StaysOnTheOrdinaryPath_EvenWhenADigestIsAlsoOffered()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var planXml = $"<ShowPlanXML server=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var digest = PayloadDimensions.Digest(planXml);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            /* A digest naming some OTHER plan, offered beside real content: the content wins, the row carries
               the digest of the content, and nothing is touched or reported. */
            var other = Convert.ToHexString(PayloadDimensions.Digest("<other/>"));
            var absent = await WriteBatchAsync(connection, serverId, serverName, Anchor,
                new[] { (NewRow("0xA", planXml), (string?)other) }, ct);

            Assert.Empty(absent);
            Assert.Equal(planXml, await ResolvedPlanAsync(connection, serverId, "0xA", ct));
            Assert.Equal(1L, await ScalarAsync(
                connection, "SELECT count(*) FROM query_stats WHERE server_id = $1 AND query_plan_digest = $2", ct, serverId, digest));
            Assert.Equal(Anchor, await LastSeenAsync(connection, digest, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, new[] { digest }, cleanupCt));
        }
    }

    [Fact]
    public void AKnownDigest_AtAPositionWithNoDivertedPayload_Throws_InsteadOfWritingANullPlan()
    {
        var hex = Convert.ToHexString(PayloadDimensions.Digest("<plan/>"));

        var noPlan = new PgCollectorRowWriter();
        Assert.Throws<InvalidOperationException>(() => noPlan.PayloadOrDigest(null, hex));

        /* A diversion plan and a batch, but this position is not a diverted one (index 0 is not the plan column). */
        var wrongPosition = new PgCollectorRowWriter();
        wrongPosition.UseDimensions(
            PayloadDimensions.DiversionPlanFor(QueryStatsCollector.Instance), new PayloadDimensionBatch());
        wrongPosition.BeginPayload();
        Assert.Throws<InvalidOperationException>(() => wrongPosition.PayloadOrDigest(null, hex));
    }

    [Fact]
    public async Task ALowerCaseKnownDigest_RoundTrips_AndTheAbsentListReportsItUpperCased()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var presentPlan = $"<ShowPlanXML server=\"{serverName}\" present=\"1\"><StmtSimple/></ShowPlanXML>";
        var present = PayloadDimensions.Digest(presentPlan);
        var missing = PayloadDimensions.Digest($"<ShowPlanXML server=\"{serverName}\" missing=\"1\"/>");

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteBatchAsync(connection, serverId, serverName, Anchor,
                new[] { (NewRow("0xA", presentPlan), (string?)null) }, ct);

            var absent = await WriteBatchAsync(connection, serverId, serverName, Anchor.AddMinutes(1),
                new[]
                {
                    (NewRow("0xB", null), (string?)Convert.ToHexString(present).ToLowerInvariant()),
                    (NewRow("0xC", null), (string?)Convert.ToHexString(missing).ToLowerInvariant()),
                }, ct);

            Assert.Equal(presentPlan, await ResolvedPlanAsync(connection, serverId, "0xB", ct));
            Assert.Equal(new[] { Convert.ToHexString(missing) }, absent);
            Assert.NotEqual(Convert.ToHexString(missing).ToLowerInvariant(), absent[0]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, new[] { present, missing }, cleanupCt));
        }
    }

    /// <summary>
    /// Two batches whose dim upserts and touches cross: each holds one stale digest from its upsert's conflict
    /// arm (the row lock is taken even when the guard is false) and then touches the other's. Run with the real
    /// SQL on two connections, in an interleaved order that needs no timing: the touches skip the rows the other
    /// batch holds, so neither waits (a 2 s lock timeout is armed to make a wait fail fast as 55P03) and both
    /// commit.
    /// </summary>
    [Fact]
    public async Task TwoBatches_ThatUpsertOneStaleDigestEachAndTouchTheOthers_NeitherWaitsNorDeadlocks()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var planA = $"<ShowPlanXML server=\"{serverName}\" which=\"a\"/>";
        var planB = $"<ShowPlanXML server=\"{serverName}\" which=\"b\"/>";
        var digestA = PayloadDimensions.Digest(planA);
        var digestB = PayloadDimensions.Digest(planB);
        var stale = Anchor;
        var now = Anchor.AddHours(10);

        await using var first = await OpenMigratedStoreAsync(connectionString!, ct);
        await using var second = await OpenWithStoreSearchPathAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            /* Both dim rows exist, committed, stored well past the guard behind the batches below. */
            var seed = new PayloadDimensionBatch();
            seed.Add(PayloadDimensions.QueryPlanDimTable, digestA, planA);
            seed.Add(PayloadDimensions.QueryPlanDimTable, digestB, planB);
            await using (var seedTransaction = await first.BeginTransactionAsync(ct))
            {
                await PayloadDimensionWriter.FlushAsync(first, seedTransaction, seed, stale, ct, compressPlanContent: false);
                await seedTransaction.CommitAsync(ct);
            }

            await using var firstTransaction = await first.BeginTransactionAsync(ct);
            await using var secondTransaction = await second.BeginTransactionAsync(ct);

            await UpsertAsync(first, firstTransaction, digestA, planA, now, ct);
            await UpsertAsync(second, secondTransaction, digestB, planB, now, ct);

            await using (var timeout = new NpgsqlCommand("SET LOCAL lock_timeout = '2s'", first, firstTransaction))
            {
                await timeout.ExecuteNonQueryAsync(ct);
            }

            var firstAbsent = await TouchAsync(first, firstTransaction, new[] { digestB }, now, ct);
            var secondAbsent = await TouchAsync(second, secondTransaction, new[] { digestA }, now, ct);

            Assert.Empty(firstAbsent);
            Assert.Empty(secondAbsent);

            await firstTransaction.CommitAsync(ct);
            await secondTransaction.CommitAsync(ct);

            Assert.Equal(now, await LastSeenAsync(first, digestA, ct));
            Assert.Equal(now, await LastSeenAsync(first, digestB, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, serverId, new[] { digestA, digestB }, cleanupCt));
        }
    }

    /// <summary>The dim upsert exactly as the flush sends it (text payload, naive-UTC stamp).</summary>
    private static async Task UpsertAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, byte[] digest, string payload, DateTime stamp, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            PayloadDimensions.UpsertSql(PayloadDimensions.QueryPlanDimTable, compressContent: false), connection, transaction);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = new[] { digest } });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = new[] { payload } });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The keep-alive touch exactly as the flush sends it; returns the digests it reports absent.</summary>
    private static async Task<List<string>> TouchAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, byte[][] digests, DateTime stamp, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            PayloadDimensions.TouchSql(PayloadDimensions.QueryPlanDimTable), connection, transaction);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = digests });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
        var absent = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            absent.Add(Convert.ToHexString(reader.GetFieldValue<byte[]>(0)));
        }

        return absent;
    }

    [Fact]
    public void ADigestThatIsNotAFullHexSha256_IsRefused_BeforeAnythingIsWritten()
    {
        var writer = new PgCollectorRowWriter();
        writer.UseDimensions(
            PayloadDimensions.DiversionPlanFor(QueryStatsCollector.Instance), new PayloadDimensionBatch());
        Assert.Throws<ArgumentException>(() => writer.PayloadOrDigest(null, "not-hex"));
        Assert.Throws<ArgumentException>(() => writer.PayloadOrDigest(null, "ABCD"));
    }

    private static async Task<object?> ScalarAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter is byte[] bytes
                ? new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = bytes }
                : new NpgsqlParameter { Value = parameter });
        }

        return await command.ExecuteScalarAsync(ct);
    }
}
