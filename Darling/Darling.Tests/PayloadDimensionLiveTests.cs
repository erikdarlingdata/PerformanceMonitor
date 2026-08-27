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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store-side half of #1767, against a real PostgreSQL (gated on DARLING_TEST_PG). Plain PostgreSQL is
/// enough — the dimension tables are deliberately NOT hypertables, so nothing here needs TimescaleDB.
///
/// <para>These are the assertions the string pins in <c>PayloadDimensionTests</c> structurally cannot make.
/// The whole change hinges on a claim about what the ENGINE does with the generated artifacts: that the
/// migration's ADD COLUMNs land, that a binary COPY built from the diversion plan writes a 32-byte digest
/// into a bytea column at the payload's ordinal and NULL into the inline one, that the upsert's ON CONFLICT
/// really collapses a repeated plan to one row, and that <c>v_query_stats</c> hands the original strings
/// back through the COALESCE. Every one of those fails SILENTLY if it is wrong — a mis-ordinal COPY
/// succeeds, a non-resolving read returns NULL, a broken dedup just stores more — which is precisely why
/// they are worth the cost of a live rig.</para>
///
/// <para>Each test mints its own server_id/server_name and its own payload content (a per-run GUID), so a
/// shared rig, a repeat run and a parallel run cannot collide, and cleanup is scoped to exactly what the
/// test wrote.</para>
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class PayloadDimensionLiveTests
{
    /// <summary>
    /// The exact line the catalog sweep emits when the #1784 coverage gate holds a tiered drop, pinned in FULL
    /// for the same reason its dimension-GC sibling is: it is a field signature the client's operator reads,
    /// and its TAIL is the actionable half — "resumes by itself once a backfill extends coverage" is what tells
    /// them this is self-correcting rather than a fault to chase. A prefix pin covers only the part that names
    /// the table, which is exactly the shape that let a wrong line ship earlier in this work.
    /// </summary>
    private const string CoverageSkipSignature =
        "Retention purge SKIPPED for query_stats: its rollup does not yet cover the oldest rows, so dropping would delete history no aggregate holds. Resumes by itself once a backfill extends coverage.";

    /// <summary>
    /// The exact line the dimension GC emits when it stands down, pinned in FULL rather than by prefix: it is
    /// a field signature an operator greps for, and a partial pin would let the wording drift to name a cause
    /// the guard does not actually detect -- which is how it came to say "raw purges held" while the shipped
    /// trigger was a purge that did not complete. Since #1795 the ONLY deferral trigger is an UNMEASURABLE
    /// fact floor (the measured bound replaced the blunt purge-failed guard), and the line says so.
    /// </summary>
    private const string DeferralSignature =
        "dimension GC deferred: a dim-feeding table's fact floor was unmeasurable this cycle; dimension content is retained until it can be measured";

    /// <summary>
    /// The exact line the GC emits when the MEASURED bound clamps below the assumed horizon (#1795) —
    /// the greppable signature of a store whose dimension GC is bounded by held fact history (a coverage
    /// clamp or a failed purge) rather than by the nominal horizon. Informational: this state is the fix
    /// working.
    /// </summary>
    private const string BoundedSignature =
        "dimension GC bounded by surviving facts: dimension content newer than the oldest digest-carrying fact row is retained";

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the payload-dimension store tests.";

    /// <summary>A server identity no real store can produce (real ids are storage-name hashes).</summary>
    private static (int ServerId, string ServerName) NewServer()
    {
        var suffix = Guid.NewGuid().ToString("N")[..12];
        return (-Random.Shared.Next(1_000_000, int.MaxValue), "pm1767-" + suffix);
    }

    private static async Task<NpgsqlConnection> OpenMigratedStoreAsync(string connectionString, CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await PgMigrations.MigrateAsync(connection, cancellationToken);
        return connection;
    }

    /// <summary>
    /// Writes one query_stats batch exactly the way <c>DarlingCollectorRunner.WriteBatchAsync</c> does:
    /// diversion plan from the same schema the COPY command is built from, BeginPayload/EndPayload around
    /// the definition's positional write, and the dimension flush inside the SAME transaction as the COPY —
    /// the ordering guarantee the design needs is not "dims first" but "no session ever observes a fact row
    /// whose digest has no dim row".
    ///
    /// <para>Hands back the still-OPEN transaction, because that guarantee is only observable from ANOTHER
    /// session while this one is mid-batch — see
    /// <see cref="AtomicVisibility_NoOtherSessionEverSeesAFactRowWhoseDigestHasNoDimRow"/>. The caller owns
    /// it: commit it, or dispose it to roll the batch back.</para>
    /// </summary>
    private static async Task<NpgsqlTransaction> WriteQueryStatsBatchUncommittedAsync(
        NpgsqlConnection connection,
        int serverId,
        string serverName,
        DateTime collectionTime,
        IReadOnlyList<QueryStatsCollector.Row> rows,
        CancellationToken cancellationToken,
        bool compressPlanContent = true)
    {
        var definition = QueryStatsCollector.Instance;
        var context = new CollectorContext
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
        };

        var writer = new PgCollectorRowWriter();
        var diversionPlan = PayloadDimensions.DiversionPlanFor(definition);
        var dimensions = new PayloadDimensionBatch();
        writer.UseDimensions(diversionPlan, dimensions);

        /* Naive-UTC storage — Npgsql rejects Kind=Utc against `timestamp`. */
        var stored = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);

        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            using (var importer = await connection.BeginBinaryImportAsync(
                PgCollectorRowWriter.CopyCommandFor(definition), cancellationToken))
            {
                writer.Importer = importer;

                foreach (var row in rows)
                {
                    await importer.StartRowAsync(cancellationToken);
                    writer.Value(CollectionIdGenerator.Next());
                    writer.Value(stored)
                          .Value(serverId)
                          .Value(serverName);

                    writer.BeginPayload();
                    definition.WritePayload(row, writer, context);
                    writer.EndPayload(definition.PayloadColumns.Count);
                }

                await importer.CompleteAsync(cancellationToken);
            }

            await PayloadDimensionWriter.FlushAsync(connection, transaction, dimensions, stored, cancellationToken, compressPlanContent);
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// #2171: plan_xml_compression = 'none' - the dim writer stores PLAIN TEXT in query_plan_xml and
    /// leaves query_plan_gz NULL, so a direct-SQL consumer reads the plan bare, and the resolving
    /// view (text-first) returns it unchanged. The digest is codec-independent, so a later gzip-mode
    /// batch carrying the SAME plan is a conflict no-op: the text row STAYS text - flipping the knob
    /// never rewrites existing content in either direction.
    /// </summary>
    [Fact]
    public async Task WritePath_PlanXmlCompressionNone_StoresText_AndAGzipBatchLater_LeavesItText()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var queryText = $"SELECT 'pm2171-{serverName}' AS marker;";
        var planXml = $"<ShowPlanXML server=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var planDigest = PayloadDimensions.Digest(planXml);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteQueryStatsBatchAsync(
                connection, serverId, serverName, DateTime.UtcNow,
                new[] { NewRow("0x2171A", queryText, planXml) }, ct, compressPlanContent: false);

            await using (var check = new NpgsqlCommand(
                "SELECT query_plan_xml, query_plan_gz FROM query_plan_dim WHERE digest = $1", connection))
            {
                check.Parameters.AddWithValue(planDigest);
                await using var reader = await check.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), "the plan dim row must exist");
                Assert.Equal(planXml, reader.GetString(0));
                Assert.True(reader.IsDBNull(1), "'none' mode must leave query_plan_gz NULL - that is the whole contract");
            }

            /* A gzip-mode batch with the SAME plan (hours later, past the last_seen guard) must not
               convert the row - the conflict arm only refreshes last_seen. */
            await WriteQueryStatsBatchAsync(
                connection, serverId, serverName, DateTime.UtcNow.AddHours(2),
                new[] { NewRow("0x2171B", queryText, planXml) }, ct, compressPlanContent: true);

            await using (var still = new NpgsqlCommand(
                "SELECT query_plan_xml IS NOT NULL, query_plan_gz IS NULL FROM query_plan_dim WHERE digest = $1", connection))
            {
                still.Parameters.AddWithValue(planDigest);
                await using var reader = await still.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.GetBoolean(0), "the text content must survive a later gzip-mode batch");
                Assert.True(reader.GetBoolean(1), "the gz column must stay NULL - mode flips never rewrite existing rows");
            }

            /* And the reader contract holds with no new arm: the resolving view returns the plan. */
            await using (var resolved = new NpgsqlCommand(
                "SELECT query_plan_xml FROM v_query_stats WHERE server_id = $1 AND query_hash = '0x2171A'", connection))
            {
                resolved.Parameters.AddWithValue(serverId);
                Assert.Equal(planXml, (string?)await resolved.ExecuteScalarAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    /// <summary>
    /// #2171: the recompression verb's guard - a store set to plan_xml_compression = 'none' refuses
    /// recompression (the live writer keeps producing text; converting would fight it forever), and
    /// 'gzip' proceeds. Tested at the extracted seam against a real migrated store so the SQL and the
    /// V62 column are exercised, not mocked.
    /// </summary>
    [Fact]
    public async Task RecompressGuard_RefusesOnNone_ProceedsOnGzip()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            /* A migrated-but-never-served store has no config_service row — the SERVICE seeds it, not
               the migrations (measured here: the bare UPDATE hit zero rows). Materialize it the way
               the sibling MCP tests do; every column defaults. */
            await using (var seed = new NpgsqlCommand(
                "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", connection))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            await using (var setNone = new NpgsqlCommand(
                "UPDATE config_service SET plan_xml_compression = 'none' WHERE id = 1", connection))
            {
                await setNone.ExecuteNonQueryAsync(ct);
            }

            Assert.True(await DarlingCliCommands.StoreIsSetToPlainTextPlansAsync(connection, ct),
                "a store set to 'none' must be recognized - the verb refusing is the whole guard");

            await using (var setGzip = new NpgsqlCommand(
                "UPDATE config_service SET plan_xml_compression = 'gzip' WHERE id = 1", connection))
            {
                await setGzip.ExecuteNonQueryAsync(ct);
            }

            Assert.False(await DarlingCliCommands.StoreIsSetToPlainTextPlansAsync(connection, ct),
                "the default mode must not trip the guard - recompression is the supported path there");

            bodySucceeded = true;
        }
        finally
        {
            /* The setting is store-global state shared with sibling tests - always restore the default. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, static async (cleanup, cleanupCt) =>
            {
                await using var restore = new NpgsqlCommand(
                    "UPDATE config_service SET plan_xml_compression = 'gzip' WHERE id = 1", cleanup);
                await restore.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>The committing wrapper — what every test that just needs the batch landed uses.</summary>
    private static async Task WriteQueryStatsBatchAsync(
        NpgsqlConnection connection,
        int serverId,
        string serverName,
        DateTime collectionTime,
        IReadOnlyList<QueryStatsCollector.Row> rows,
        CancellationToken cancellationToken,
        bool compressPlanContent = true)
    {
        await using var transaction = await WriteQueryStatsBatchUncommittedAsync(
            connection, serverId, serverName, collectionTime, rows, cancellationToken, compressPlanContent);
        await transaction.CommitAsync(cancellationToken);
    }

    private static QueryStatsCollector.Row NewRow(string queryHash, string? queryText, string? planXml)
        => new()
        {
            DatabaseName = "pm1767",
            QueryHash = queryHash,
            QueryPlanHash = "0xPLANHASH",
            SqlHandle = "0x" + queryHash,
            PlanHandle = "0x" + queryHash,
            QueryText = queryText,
            QueryPlanXml = planXml,
        };

    private static async Task DeleteServerRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken cancellationToken)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(serverId);
        await cleanup.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteDimRowAsync(
        NpgsqlConnection connection, string dimTable, byte[] digest, CancellationToken cancellationToken)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {dimTable} WHERE digest = $1", connection);
        cleanup.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
        await cleanup.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<object?> ScalarAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            if (parameter is byte[] bytes)
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = bytes });
            }
            else
            {
                command.Parameters.AddWithValue(parameter);
            }
        }

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value == DBNull.Value ? null : value;
    }

    /// <summary>
    /// A bytea scalar, typed. Deliberately not left as <c>object</c>: xUnit's Assert.Equal picks its
    /// element-wise sequence overload from the STATIC type, so comparing two boxed <c>byte[]</c> as
    /// <c>object</c> would fall back to reference equality and pass or fail for the wrong reason.
    /// </summary>
    private static async Task<byte[]?> ScalarBytesAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
        => (byte[]?)await ScalarAsync(connection, sql, cancellationToken, parameters);

    // ── (a) the migration ──

    [Fact]
    public async Task Migration_CreatesBothDimensions_AddsEveryDigestColumn_AndIsIdempotent()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);

        /* A fully-migrated store is a no-op on the next run — V38 included. */
        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            var payloadColumn = PayloadDimensions.PayloadColumnOf(dimTable);

            foreach (var (column, type) in new[]
            {
                (PayloadDimensions.DigestColumn, "bytea"),
                (payloadColumn, "text"),
                (PayloadDimensions.LastSeenColumn, "timestamp without time zone"),
            })
            {
                var actualType = await ScalarAsync(
                    connection,
                    "SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                    ct, dimTable, column);
                Assert.Equal(type, actualType);

                var nullable = await ScalarAsync(
                    connection,
                    "SELECT is_nullable FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                    ct, dimTable, column);

                /* #2069: the plan dim's TEXT column is nullable BY DESIGN — new rows carry gzip bytes
                   and leave it NULL (fresh stores create it nullable; upgraded stores get V54's DROP
                   NOT NULL). Digest and last_seen — and the text dim entirely — stay NOT NULL. */
                var expectNullable =
                    string.Equals(dimTable, PayloadDimensions.CompressedContentDimTable, StringComparison.Ordinal) &&
                    string.Equals(column, payloadColumn, StringComparison.Ordinal)
                        ? "YES"
                        : "NO";
                Assert.Equal(expectNullable, nullable);
            }

            /* #2069: the plan dim also carries the gz bytea column, nullable — pre-V54 rows have text
               only, so NOT NULL could never hold. */
            if (string.Equals(dimTable, PayloadDimensions.CompressedContentDimTable, StringComparison.Ordinal))
            {
                Assert.Equal("bytea", await ScalarAsync(
                    connection,
                    "SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                    ct, dimTable, PayloadDimensions.CompressedContentColumn));
                Assert.Equal("YES", await ScalarAsync(
                    connection,
                    "SELECT is_nullable FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                    ct, dimTable, PayloadDimensions.CompressedContentColumn));
            }

            /* The digest is the PRIMARY KEY — that is what makes ON CONFLICT (digest) the dedup mechanism
               rather than a hint. */
            var primaryKeyColumn = await ScalarAsync(
                connection,
                """
                SELECT a.attname
                FROM pg_index AS i
                JOIN pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass
                AND   i.indisprimary
                """,
                ct, dimTable);
            Assert.Equal(PayloadDimensions.DigestColumn, primaryKeyColumn);
        }

        /* All three fact-side digest columns, nullable so the migration rewrote nothing. */
        foreach (var dimension in PayloadDimensions.All)
        {
            var type = await ScalarAsync(
                connection,
                "SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                ct, dimension.TargetTable, dimension.DigestColumn);
            Assert.Equal("bytea", type);

            var nullable = await ScalarAsync(
                connection,
                "SELECT is_nullable FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                ct, dimension.TargetTable, dimension.DigestColumn);
            Assert.Equal("YES", nullable);
        }
    }

    // ── (b) the write path ──

    [Fact]
    public async Task WritePath_StoresDigestsOnTheFactRow_ContentInTheDimensions_AndResolvesThroughTheView()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var queryText = $"SELECT 'pm1767-{serverName}' AS marker;";
        var planXml = $"<ShowPlanXML server=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var textDigest = PayloadDimensions.Digest(queryText);
        var planDigest = PayloadDimensions.Digest(planXml);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteQueryStatsBatchAsync(
                connection, serverId, serverName, DateTime.UtcNow,
                new[] { NewRow("0xAAAA", queryText, planXml) }, ct);

            /* The fact row carries the KEYS only — the inline columns this change exists to empty are NULL. */
            using (var read = new NpgsqlCommand(
                "SELECT query_text, query_plan_xml, query_text_digest, query_plan_digest FROM query_stats WHERE server_id = $1",
                connection))
            {
                read.Parameters.AddWithValue(serverId);
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.IsDBNull(0), "query_text must be NULL on a row written since #1767");
                Assert.True(reader.IsDBNull(1), "query_plan_xml must be NULL on a row written since #1767");
                Assert.Equal(textDigest, reader.GetFieldValue<byte[]>(2));
                Assert.Equal(planDigest, reader.GetFieldValue<byte[]>(3));
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The content is in the dimensions, once. Text keeps its text shape; the plan dim stores
               gzip BYTES with its text column NULL (#2069) — the digest was still computed over the
               UNCOMPRESSED text, so identity is stable across the format change. */
            Assert.Equal(queryText, await ScalarAsync(
                connection, "SELECT query_text FROM query_text_dim WHERE digest = $1", ct, textDigest));
            using (var read = new NpgsqlCommand(
                "SELECT query_plan_xml, query_plan_gz FROM query_plan_dim WHERE digest = $1", connection))
            {
                read.Parameters.AddWithValue(planDigest);
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.IsDBNull(0), "query_plan_dim.query_plan_xml must be NULL on a row written since #2069");
                Assert.Equal(planXml, PayloadDimensions.DecompressContent(reader.GetFieldValue<byte[]>(1)));
            }

            /* And the view hands back exactly what was collected. The TEXT arrives through query_text as
               before; the PLAN arrives as gzip bytes in query_plan_gz with the coalesced query_plan_xml
               NULL — the #2069 disclosed contract change: raw-SQL consumers of the view see NULL plan
               text on new rows and must read the gz column (every product reader resolves text-else-gz). */
            using (var read = new NpgsqlCommand(
                "SELECT query_text, query_plan_xml, query_plan_gz FROM v_query_stats WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(serverId);
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(queryText, reader.GetString(0));
                Assert.True(reader.IsDBNull(1), "v_query_stats.query_plan_xml must be NULL for a post-#2069 plan");
                Assert.Equal(planXml, PayloadDimensions.DecompressContent(reader.GetFieldValue<byte[]>(2)));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryTextDimTable, textDigest, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, planDigest, cleanupCt);
            });
        }
    }

    // ── (c) the dedup that is the whole point ──

    [Fact]
    public async Task Dedup_ManyRowsSharingOnePlan_StoreOneDimRow_AndASecondBatchAddsNone()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var planXml = $"<ShowPlanXML server=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var planDigest = PayloadDimensions.Digest(planXml);
        var textDigests = new List<byte[]>();

        var rows = new List<QueryStatsCollector.Row>();
        for (var i = 0; i < 5; i++)
        {
            var text = $"SELECT {i} FROM pm1767_{serverName};";
            textDigests.Add(PayloadDimensions.Digest(text));
            rows.Add(NewRow($"0xHASH{i}", text, planXml));
        }

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteQueryStatsBatchAsync(connection, serverId, serverName, DateTime.UtcNow, rows, ct);

            /* Five fact rows, one plan row. The batch collapses the repeat sightings BEFORE the upsert
               sees them, which is also what keeps Postgres from raising 21000 on the duplicate key. */
            Assert.Equal(5L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_stats WHERE server_id = $1", ct, serverId));
            Assert.Equal(1L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_plan_dim WHERE digest = $1", ct, planDigest));

            /* A SECOND collection of the same cached plan — the every-cycle repetition that made the inline
               payload 94% of the store — adds nothing. This is the ON CONFLICT path, across batches. */
            await WriteQueryStatsBatchAsync(connection, serverId, serverName, DateTime.UtcNow, rows, ct);

            Assert.Equal(10L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_stats WHERE server_id = $1", ct, serverId));
            Assert.Equal(1L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_plan_dim WHERE digest = $1", ct, planDigest));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, planDigest, cleanupCt);
                foreach (var digest in textDigests)
                {
                    await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryTextDimTable, digest, cleanupCt);
                }
            });
        }
    }

    // ── (d) the transition ──

    [Fact]
    public async Task View_StillReturnsInlinePayloads_ForRowsWrittenBeforeTheDimensionsExisted()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var queryText = $"SELECT 'legacy-{serverName}';";
        var planXml = $"<ShowPlanXML legacy=\"{serverName}\"/>";

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            /* The pre-#1767 shape: payload inline, digests NULL. Migration is ZERO-REWRITE, so every row in
               a live store looks like this until raw retention ages it out — roughly the first four days
               after deploy, during which the COALESCE arm is the ONLY thing keeping the product populated. */
            using (var insert = new NpgsqlCommand(
                """
                INSERT INTO query_stats
                    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, query_plan_xml)
                VALUES ($1, $2, $3, $4, 'pm1767', '0xLEGACY', $5, $6)
                """,
                connection))
            {
                insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
                insert.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                insert.Parameters.AddWithValue(serverId);
                insert.Parameters.AddWithValue(serverName);
                insert.Parameters.AddWithValue(queryText);
                insert.Parameters.AddWithValue(planXml);
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var read = new NpgsqlCommand(
                "SELECT query_text, query_plan_xml, query_text_digest, query_plan_digest FROM v_query_stats WHERE server_id = $1",
                connection))
            {
                read.Parameters.AddWithValue(serverId);
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(queryText, reader.GetString(0));
                Assert.Equal(planXml, reader.GetString(1));
                Assert.True(reader.IsDBNull(2), "a pre-#1767 row carries no text digest");
                Assert.True(reader.IsDBNull(3), "a pre-#1767 row carries no plan digest");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
            });
        }
    }

    // ── (e) the last_seen watermark and its churn guard ──

    [Fact]
    public async Task LastSeen_RefreshesOnlyOncePerHour_SoAHotDimTakesOneUpdateNotOnePerCycle()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var payload = $"<ShowPlanXML watermark=\"{Guid.NewGuid():N}\"/>";
        var digest = PayloadDimensions.Digest(payload);
        var t0 = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            async Task FlushAtAsync(DateTime collectionTime)
            {
                var batch = new PayloadDimensionBatch();
                batch.Add(PayloadDimensions.QueryPlanDimTable, digest, payload);

                await using var transaction = await connection.BeginTransactionAsync(ct);
                await PayloadDimensionWriter.FlushAsync(connection, transaction, batch, collectionTime, ct);
                await transaction.CommitAsync(ct);
            }

            async Task<DateTime> LastSeenAsync()
                => (DateTime)(await ScalarAsync(
                    connection, "SELECT last_seen FROM query_plan_dim WHERE digest = $1", ct, digest))!;

            await FlushAtAsync(t0);
            Assert.Equal(t0, await LastSeenAsync());

            /* Within the hour: NOT refreshed. Every referenced dim row would otherwise take an UPDATE every
               collection cycle — a dead tuple per row per minute on a hot table — for a watermark whose
               only consumer has a multi-day horizon. */
            await FlushAtAsync(t0.AddMinutes(30));
            Assert.Equal(t0, await LastSeenAsync());

            /* Past the hour: refreshed. The guard caps the churn at 60x less, and stays correct as long as
               the GC margin exceeds an hour (it is a full day). */
            await FlushAtAsync(t0.AddHours(2));
            Assert.Equal(t0.AddHours(2), await LastSeenAsync());

            /* And a REPLAYED/backfilled batch cannot stamp content as fresher than it is: the watermark
               never moves backwards, because the guard's comparison is one-directional. */
            await FlushAtAsync(t0.AddMinutes(5));
            Assert.Equal(t0.AddHours(2), await LastSeenAsync());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, digest, cleanupCt);
            });
        }
    }

    // ── (f) the GC ──

    [Fact]
    public async Task Gc_DeletesDimRowsPastTheHorizon_AndKeepsTheOnesInsideIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var run = Guid.NewGuid().ToString("N")[..12];
        var expiredPayload = $"<ShowPlanXML gc=\"expired-{run}\"/>";
        var livePayload = $"<ShowPlanXML gc=\"live-{run}\"/>";
        var expiredDigest = PayloadDimensions.Digest(expiredPayload);
        var liveDigest = PayloadDimensions.Digest(livePayload);

        /* Deliberately ancient timestamps around an ancient cutoff, so the sweep can only ever touch rows
           this test planted — the GC is fleet-wide (a dim row has no server_id) and this class shares a rig. */
        var expiredSeen = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var liveSeen = new DateTime(2000, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var cutoff = new DateTime(2000, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            foreach (var (digest, payload, lastSeen) in new[]
            {
                (expiredDigest, expiredPayload, expiredSeen),
                (liveDigest, livePayload, liveSeen),
            })
            {
                using var insert = new NpgsqlCommand(
                    "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3)",
                    connection);
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
                insert.Parameters.AddWithValue(payload);
                insert.Parameters.AddWithValue(lastSeen);
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var gc = new NpgsqlCommand(
                DarlingRetention.TimeSlicedDeleteSql(
                    PayloadDimensions.QueryPlanDimTable, PayloadDimensions.LastSeenColumn),
                connection))
            {
                gc.Parameters.AddWithValue(cutoff);

                /* Drained the way production drains it: the statement clears ONE time slice per
                   execution, so a single call would only reach this test's row when it happens to sit
                   in the oldest slice on the rig. DrainBatchesAsync is the same loop DarlingRetention
                   runs, so this exercises the real termination condition rather than a test-only one. */
                var swept = await DarlingRetention.DrainBatchesAsync(
                    token => gc.ExecuteNonQueryAsync(token), batchSize: 1, ct);

                /* At least this test's expired row. The GC is fleet-wide, so an exact count would be a
                   flake waiting on a rig that happens to hold another ancient row; the two scoped counts
                   below are the actual pin. */
                Assert.True(swept >= 1, "the expired dim row must be swept");
            }

            Assert.Equal(0L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_plan_dim WHERE digest = $1", ct, expiredDigest));
            Assert.Equal(1L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_plan_dim WHERE digest = $1", ct, liveDigest));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, expiredDigest, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, liveDigest, cleanupCt);
            });
        }
    }

    // ── (g2) the row-capped drain (#2388) ──

    /// <summary>
    /// #2388's batched drain against a REAL table, which is the half nothing covered. The existing drain
    /// tests inject a fake executor and prove the LOOP arithmetic; the SQL those batches actually run had
    /// only ever been asserted as a string. Both halves can be individually correct and still not delete
    /// anything: <c>RowCappedDeleteSql</c> puts the cutoff bound, the ORDER BY and the LIMIT inside a
    /// <c>ctid IN (...)</c> subquery, and that shape has to survive contact with a real planner.
    ///
    /// <para>This is the failure #2386 measured on the dogfood store, so it is worth stating what "works"
    /// means here: not that the statement succeeds, but that repeated bounded statements DRAIN. A single
    /// batch returning rows looked like success in the field for a week while the table grew, because the
    /// purge peeled one slice and reported <c>0 failed</c>. The assertion is therefore on the total and on
    /// the batch COUNT — one batch that deletes something is exactly the bug.</para>
    ///
    /// <para>Runs with a deliberately tiny cap rather than the production 50,000 so the multi-batch path is
    /// exercised in a few hundred rows. The cap is the drain's batch size by construction (a batch at the
    /// cap forces another round), so a small one tests the same control flow the real one does.</para>
    /// </summary>
    [Fact]
    public async Task RowCappedDelete_DrainsAcrossMultipleBatches_AndLeavesFreshRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var run = Guid.NewGuid().ToString("N")[..12];

        const int cap = 25;
        const int expiredCount = 110;   // 4 full batches + a partial one -> 5 executions
        const int liveCount = 7;

        /* Ancient timestamps around an ancient cutoff: the dim GC is fleet-wide (a dim row has no
           server_id) and this class shares a rig, so the window must only ever contain this run's rows. */
        var expiredSeen = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var liveSeen = new DateTime(2001, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var cutoff = new DateTime(2001, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);

        var expiredDigests = new List<byte[]>(expiredCount);
        var liveDigests = new List<byte[]>(liveCount);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            for (var i = 0; i < expiredCount + liveCount; i++)
            {
                var expired = i < expiredCount;
                var payload = $"<ShowPlanXML drain=\"{(expired ? "old" : "new")}-{run}-{i}\"/>";
                var digest = PayloadDimensions.Digest(payload);
                (expired ? expiredDigests : liveDigests).Add(digest);

                using var insert = new NpgsqlCommand(
                    "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3)",
                    connection);
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
                insert.Parameters.AddWithValue(payload);
                insert.Parameters.AddWithValue(expired ? expiredSeen : liveSeen);
                await insert.ExecuteNonQueryAsync(ct);
            }

            var batches = 0;
            using var delete = new NpgsqlCommand(
                DarlingRetention.RowCappedDeleteSql(
                    PayloadDimensions.QueryPlanDimTable, PayloadDimensions.LastSeenColumn, cap),
                connection);
            delete.Parameters.AddWithValue(cutoff);

            var deleted = await DarlingRetention.DrainBatchesAsync(
                async batchCt =>
                {
                    batches++;
                    return await delete.ExecuteNonQueryAsync(batchCt);
                },
                cap,
                ct);

            /* Every expired row, and only those. */
            Assert.Equal(expiredCount, deleted);

            /* The point of the fix: it took more than one statement to get there. A single execution
               deleting 110 rows would mean the LIMIT never bound, which is the unbounded statement
               #2388 replaced — the one that times out on a real backlog. */
            Assert.Equal(5, batches);

            Assert.Equal(0L, await ScalarAsync(
                connection, "SELECT COUNT(*) FROM query_plan_dim WHERE last_seen < $1", ct, cutoff));
            Assert.Equal((long)liveCount, await ScalarAsync(
                connection,
                "SELECT COUNT(*) FROM query_plan_dim WHERE last_seen = $1", ct, liveSeen));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var digest in expiredDigests.Concat(liveDigests))
                {
                    await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, digest, cleanupCt);
                }
            });
        }
    }

    // ── (g) the recompression verb (#2076) ──

    [Fact]
    public async Task Recompression_ConvertsTextRowsToVerifiedGzip_LeavesLastSeenAndGzRowsAlone_AndConverges()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var run = Guid.NewGuid().ToString("N")[..12];

        /* Three pre-V54-shaped text rows (one carrying non-ASCII, the codec's UTF-8 leg) and one
           already-gz row the run must not touch. The repeated operator block gives each plan realistic
           bulk (~6 KB): a payload smaller than gzip's own framing would compress LARGER and turn the
           smaller-than-text assertion below into a lie about real plans. */
        var bulk = string.Concat(Enumerable.Repeat(
            "<RelOp NodeId=\"1\" PhysicalOp=\"Index Seek\" LogicalOp=\"Index Seek\" EstimateRows=\"1\"/>", 80));
        var textPlans = new[]
        {
            $"<ShowPlanXML recompress=\"a-{run}\">{bulk}<StmtSimple/></ShowPlanXML>",
            $"<ShowPlanXML recompress=\"b-{run}\">{bulk}<StmtSimple StatementText=\"SELECT N'Ærø — 数据'\"/></ShowPlanXML>",
            $"<ShowPlanXML recompress=\"c-{run}\">{bulk}<StmtSimple/></ShowPlanXML>",
        };
        var textDigests = textPlans.Select(PayloadDimensions.Digest).ToArray();
        var gzPlan = $"<ShowPlanXML recompress=\"gz-{run}\"/>";
        var gzDigest = PayloadDimensions.Digest(gzPlan);
        var gzBytes = PayloadDimensions.CompressContent(gzPlan);
        var t0 = new DateTime(2026, 4, 1, 12, 0, 0, DateTimeKind.Unspecified);

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            for (var i = 0; i < textPlans.Length; i++)
            {
                using var insert = new NpgsqlCommand(
                    "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3)",
                    connection);
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = textDigests[i] });
                insert.Parameters.AddWithValue(textPlans[i]);
                insert.Parameters.AddWithValue(t0);
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var insert = new NpgsqlCommand(
                "INSERT INTO query_plan_dim (digest, query_plan_gz, last_seen) VALUES ($1, $2, $3)",
                connection))
            {
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = gzDigest });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = gzBytes });
                insert.Parameters.AddWithValue(t0);
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* The survey counts the whole table (fleet-wide on a shared rig), so pin relatively. */
            var survey = await PlanDimRecompression.SurveyAsync(connection, ct);
            Assert.True(survey.Pending >= textPlans.Length, "the seeded text rows must count as pending");

            var result = await PlanDimRecompression.ConvertAsync(connection, progress: null, ct);
            Assert.True(result.Rows >= textPlans.Length, "the seeded text rows must convert");
            Assert.Equal(0, result.VerifyFailures);
            Assert.True(result.TextBytes > result.GzipBytes, "gzip must actually be smaller than the text it replaced");

            /* Each converted row: text NULL, gz decompresses to the ORIGINAL text, and last_seen is
               UNTOUCHED — recompression is not a sighting (stamping it would push GC-eligible rows a
               full retention window into the future). */
            for (var i = 0; i < textPlans.Length; i++)
            {
                using var read = new NpgsqlCommand(
                    "SELECT query_plan_xml, query_plan_gz, last_seen FROM query_plan_dim WHERE digest = $1",
                    connection);
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = textDigests[i] });
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.IsDBNull(0), "converted row must have NULL text");
                Assert.Equal(textPlans[i], PayloadDimensions.DecompressContent(reader.GetFieldValue<byte[]>(1)));
                Assert.Equal(t0, reader.GetDateTime(2));
            }

            /* The already-gz row's bytes are untouched (the fetch predicate never selects it). */
            var gzAfter = (byte[])(await ScalarAsync(
                connection, "SELECT query_plan_gz FROM query_plan_dim WHERE digest = $1", ct, gzDigest))!;
            Assert.Equal(gzBytes, gzAfter);

            /* Convergence: a second run finds nothing left to do. */
            var second = await PlanDimRecompression.ConvertAsync(connection, progress: null, ct);
            Assert.Equal(0, second.Rows);
            Assert.Equal(0, second.VerifyFailures);

            /* The closing compaction (#2076): VACUUM FULL runs against a live store and the content
               survives it byte-for-byte — the estimate is sane (positive, no bigger than the current
               relation plus slack) and a converted row still decompresses to its original text after
               the rewrite. */
            var estimate = await PlanDimRecompression.EstimateCompactedBytesAsync(connection, ct);
            Assert.True(estimate > 0, "compacted-size estimate must be positive");
            await PlanDimRecompression.VacuumFullAsync(connection, ct);
            Assert.Equal(textPlans[0], PayloadDimensions.DecompressContent((byte[])(await ScalarAsync(
                connection, "SELECT query_plan_gz FROM query_plan_dim WHERE digest = $1", ct, textDigests[0]))!));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var digest in textDigests)
                {
                    await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, digest, cleanupCt);
                }

                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, gzDigest, cleanupCt);
            });
        }
    }

    // ── (h) NUL handling ──

    [Fact]
    public async Task NulLadenPayload_RoundTripsStripped_AndItsDigestKeysTheStoredBytes()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();

        /* SQL Server NVARCHAR allows embedded NUL; Postgres text rejects the raw byte with 22021 and fails
           the whole COPY batch (#1614). The strip has to happen BEFORE the hash, or the digest keys bytes
           that were never stored and the dim lookup silently misses on every re-collection. */
        var raw = $"SELECT '\0nul-{serverName}\0' AS marker;";
        var stripped = PgCollectorRowWriter.StripEmbeddedNuls(raw);
        Assert.DoesNotContain('\0', stripped);
        var strippedDigest = PayloadDimensions.Digest(stripped);
        Assert.NotEqual(strippedDigest, PayloadDimensions.Digest(raw));

        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await WriteQueryStatsBatchAsync(
                connection, serverId, serverName, DateTime.UtcNow,
                new[] { NewRow("0xNUL", raw, null) }, ct);

            /* The digest on the fact row is the digest of what the dim actually holds. */
            Assert.Equal(strippedDigest, await ScalarBytesAsync(
                connection, "SELECT query_text_digest FROM query_stats WHERE server_id = $1", ct, serverId));
            Assert.Equal(stripped, await ScalarAsync(
                connection, "SELECT query_text FROM query_text_dim WHERE digest = $1", ct, strippedDigest));
            Assert.Equal(stripped, await ScalarAsync(
                connection, "SELECT query_text FROM v_query_stats WHERE server_id = $1", ct, serverId));

            /* A NULL payload stays NULL — nothing to dedupe and no dim row to point at. */
            Assert.Null(await ScalarAsync(
                connection, "SELECT query_plan_digest FROM query_stats WHERE server_id = $1", ct, serverId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryTextDimTable, strippedDigest, cleanupCt);
            });
        }
    }

    // ── (h) the atomicity the whole ordering argument rests on ──

    /// <summary>
    /// The ordering guarantee, proved from the only vantage point that can see it: a SECOND session.
    ///
    /// <para>The guarantee is NOT "dims are inserted before facts". It cannot be — the digests are only
    /// known once the COPY has streamed the payloads, and a dims-first pass would mean running the
    /// definition's <c>WritePayload</c> twice, which is unsafe because <c>WritePayload</c> CONSUMES the
    /// delta state (<c>context.Deltas.CalculateDelta</c>) and a second pass would report every delta as
    /// zero. The guarantee is the STRONGER one a shared transaction gives for free: <b>no other session
    /// ever observes a fact row whose digest has no dimension row</b>.</para>
    ///
    /// <para>So this test opens the window a non-transactional implementation would leak through.
    /// Connection A runs the whole batch — COPY, importer completion, dimension flush — and stops short of
    /// COMMIT. Connection B, a separate connection at the default READ COMMITTED (what the viewer, the MCP
    /// readers and the analysis engine all are), must see NOTHING: not the facts, not the dims. An
    /// implementation that let the importer's own completion be the commit and upserted the dimensions
    /// afterwards would, at this exact instant, be showing every reader fact rows whose digests resolve to
    /// nothing — and <c>v_query_stats</c> would hand those readers NULL text and NULL plan with no error
    /// anywhere, which reads as a collection outage rather than as a bug. After the commit both sides
    /// appear together, and the anti-join a reader would run comes back empty.</para>
    /// </summary>
    [Fact]
    public async Task AtomicVisibility_NoOtherSessionEverSeesAFactRowWhoseDigestHasNoDimRow()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        var queryText = $"SELECT 'atomic-{serverName}' AS marker;";
        var planXml = $"<ShowPlanXML atomic=\"{serverName}\"><StmtSimple/></ShowPlanXML>";
        var textDigest = PayloadDimensions.Digest(queryText);
        var planDigest = PayloadDimensions.Digest(planXml);

        /* Two rows sharing one text and one plan: the dedup shape a real batch has, so the post-commit
           counts pin BOTH that the facts landed and that they collapsed onto single dim rows. */
        var rows = new[]
        {
            NewRow("0xATOMIC1", queryText, planXml),
            NewRow("0xATOMIC2", queryText, planXml),
        };

        /* The WRITER's session. */
        await using var writer = await OpenMigratedStoreAsync(connectionString!, ct);

        /* The OBSERVER's session. Deliberately a separate NpgsqlConnection and deliberately NOT migrated:
           only the writer touches DDL, so nothing here can serialize behind the writer's own work and
           accidentally turn "sees nothing" into "was blocked from looking".

           It still needs the search_path, and it must SET it rather than inherit one. MigrateAsync
           SETs the path on whatever session it is handed; the store-wide default comes from
           ALTER DATABASE, which reaches only sessions established AFTER it commits — and Npgsql
           POOLS sessions, so a physical connection opened before the first migration keeps the
           default "$user", public for its whole life and resolves bare `query_stats` to nothing
           (42P01). On a long-lived store every session postdates that ALTER by days, which is why
           this passes locally and failed on CI's fresh cluster, where the pool is full of
           connections older than the migration. Setting it here makes the observer independent of
           both the ALTER's timing and the pool's reuse. */
        await using var observer = new NpgsqlConnection(connectionString!);
        await observer.OpenAsync(ct);
        await using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, observer))
        {
            await setPath.ExecuteNonQueryAsync(ct);
        }

        async Task<long> ObservedFactRowsAsync()
            => (long)(await ScalarAsync(
                observer, "SELECT COUNT(*) FROM query_stats WHERE server_id = $1", ct, serverId))!;

        async Task<long> ObservedDimRowsAsync(string dimTable, byte[] digest)
            => (long)(await ScalarAsync(
                observer, $"SELECT COUNT(*) FROM {dimTable} WHERE digest = $1", ct, digest))!;

        /* The property as a reader would state it: fact rows carrying a digest that resolves to nothing.
           Zero is the only acceptable answer at EVERY instant, which is why it is asked from the
           observer's session and not the writer's — inside the writer's transaction it is trivially zero
           however the write path is ordered. */
        async Task<long> DanglingDigestsAsync(string digestColumn, string dimTable)
            => (long)(await ScalarAsync(
                observer,
                $"""
                 SELECT COUNT(*)
                 FROM query_stats AS f
                 WHERE f.server_id = $1
                 AND   f.{digestColumn} IS NOT NULL
                 AND   NOT EXISTS (SELECT 1 FROM {dimTable} AS d WHERE d.digest = f.{digestColumn})
                 """,
                ct, serverId))!;

        var bodySucceeded = false;
        try
        {
            await using (var transaction = await WriteQueryStatsBatchUncommittedAsync(
                writer, serverId, serverName, DateTime.UtcNow, rows, ct))
            {
                /* THE OBSERVATION WINDOW — everything written, nothing committed. */
                Assert.Equal(0L, await ObservedFactRowsAsync());
                Assert.Equal(0L, await ObservedDimRowsAsync(PayloadDimensions.QueryTextDimTable, textDigest));
                Assert.Equal(0L, await ObservedDimRowsAsync(PayloadDimensions.QueryPlanDimTable, planDigest));

                await transaction.CommitAsync(ct);
            }

            /* One commit, both sides visible at once. */
            Assert.Equal(2L, await ObservedFactRowsAsync());
            Assert.Equal(1L, await ObservedDimRowsAsync(PayloadDimensions.QueryTextDimTable, textDigest));
            Assert.Equal(1L, await ObservedDimRowsAsync(PayloadDimensions.QueryPlanDimTable, planDigest));

            Assert.Equal(0L, await DanglingDigestsAsync("query_text_digest", PayloadDimensions.QueryTextDimTable));
            Assert.Equal(0L, await DanglingDigestsAsync("query_plan_digest", PayloadDimensions.QueryPlanDimTable));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryTextDimTable, textDigest, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, planDigest, cleanupCt);
            });
        }
    }

    /// <summary>
    /// The dimension GC must NOT prune while a dim-feeding table's fact floor is UNMEASURABLE (#1795,
    /// narrowing #1782's guard).
    ///
    /// <para>The GC's cutoff is bounded by the oldest surviving digest-carrying fact row. When that floor
    /// cannot be measured at all, the safety boundary is unknown — pruning on the assumed horizon could
    /// dangle digests that still-unmeasurable facts reference, and the resolving view would serve NULL
    /// payload with no error raised anywhere. So an unmeasurable floor defers the GC for the cycle, the
    /// same fail-toward-recoverable choice the old purge-failed guard made.</para>
    ///
    /// <para>The unmeasurable state is injected by RENAMING query_stats for the duration — the floor probe's
    /// relation genuinely does not exist, exercising the real seam rather than a simulated flag. It is
    /// restored in a finally, and the whole live collection is serialized, so no sibling class can observe
    /// the rename.</para>
    /// </summary>
    [Fact]
    public async Task DimensionGc_DefersWhenAFactFloorIsUnmeasurable_ThenPrunesOnceItIs()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);

        /* Far past the ~32-day cutoff, so an unguarded GC would certainly take it. */
        var digest = PayloadDimensions.Digest("pm1782-" + Guid.NewGuid().ToString("N"));
        var ancient = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-400), DateTimeKind.Unspecified);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var renamed = false;
        var bodySucceeded = false;
        /* Snapshot BEFORE the try, so the finally always has a baseline even if setup throws mid-way.
           Restoring against a snapshot when nothing was created is a no-op. */
        var coverageSnapshot = await ExistingCaggsAsync(connection, ct);

        try
        {
            using (var seed = new NpgsqlCommand(
                $"INSERT INTO {PayloadDimensions.QueryPlanDimTable} (digest, query_plan_xml, last_seen) " +
                "VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING", connection))
            {
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
                seed.Parameters.AddWithValue("<ShowPlanXML pm1782=\"1\"/>");
                seed.Parameters.AddWithValue(ancient);
                await seed.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal(1L, await DimRowCountAsync(connection, digest, ct));

            using (var rename = new NpgsqlCommand(
                "ALTER TABLE collect.query_stats RENAME TO query_stats_pm1782", connection))
            {
                await rename.ExecuteNonQueryAsync(ct);
                renamed = true;
            }

            var deferredLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, deferredLog, ct, PurgeDimFeeding(30));

            /* The SUBSTANTIVE property first, deliberately: this is the assertion that distinguishes
               "the guard held" from "the guard logged and pruned anyway", so it is the one a mutation
               must land on. Asserting the log line first would let a broken guard go red on missing
               TEXT while the actual data loss went unexamined. */
            Assert.Equal(1L, await DimRowCountAsync(connection, digest, ct));
            Assert.Contains(DeferralSignature, deferredLog.Joined, StringComparison.Ordinal);

            using (var restore = new NpgsqlCommand(
                "ALTER TABLE collect.query_stats_pm1782 RENAME TO query_stats", connection))
            {
                await restore.ExecuteNonQueryAsync(ct);
                renamed = false;
            }

            /* Coverage satisfied so the control phase exercises the fully-healthy path end to end — no
               clamp skip in the log, floors measurable, purge running. (A clamp skip would no longer
               defer the GC — that is #1795's whole point, pinned by the sibling test below — but the
               control here is about the unmeasurable-floor deferral ENDING.) */
            await SatisfyRollupCoverageAsync(connection, ct);

            /* Control: with the table back the floor is measurable again. No digest-carrying facts exist
               on this rig (the floor is NULL), so the assumed ~32-day horizon applies and the 400-day
               orphan is pruned — the test cannot pass merely because the GC never runs. */
            var prunedLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, prunedLog, ct, PurgeDimFeeding(30));

            Assert.DoesNotContain(DeferralSignature, prunedLog.Joined, StringComparison.Ordinal);
            Assert.Equal(0L, await DimRowCountAsync(connection, digest, ct));

            bodySucceeded = true;
        }
        finally
        {
            /* This finally is the one #1794's CI occurrence landed in — and it is also the highest-stakes
               cleanup in the class: an abandoned rename leaves the store without collect.query_stats for
               every later test and every later RUN on a reused database. It must not depend on the body's
               connection having survived. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                if (renamed)
                {
                    using var restore = new NpgsqlCommand(
                        "ALTER TABLE IF EXISTS collect.query_stats_pm1782 RENAME TO query_stats", cleanup);
                    await restore.ExecuteNonQueryAsync(cleanupCt);
                }

                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, digest, cleanupCt);

                /* LAST, deliberately: the CAGG drops are the one step that can break the session (a
                   drop colliding with a still-running background job — the CI-proven shape), and a
                   break here has no cleanup statements left to strand. */
                await RestoreCaggsAsync(cleanup, coverageSnapshot, cleanupCt);
            });
        }
    }

    private static async Task<long> DimRowCountAsync(NpgsqlConnection connection, byte[] digest, CancellationToken ct)
    {
        using var read = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {PayloadDimensions.QueryPlanDimTable} WHERE digest = $1", connection);
        read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
        return (long)(await read.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// #1815: the floor measurement must survive COMPRESSED history — the exact shape the first field
    /// run of #1795 died on. Compressed chunks carry no btree indexes, so the V39 partial index cannot
    /// serve them and the exact min() probe was a full decompress-scan that hit the driver's timeout;
    /// on a hypertable the floor now comes from chunk catalog metadata instead (instant,
    /// compression-immune, conservative in the safe direction). This test compresses the chunk holding
    /// the digest-carrying fact with the PRODUCT's own compression settings, then proves the sweep
    /// measures (no unmeasurable deferral), bounds (the bounded signature), reclaims the ancient
    /// orphan, keeps the referenced content — and, the path discriminator, keeps a dim row that sits
    /// BETWEEN the chunk floor and the exact digest floor: the conservative catalog bound retains it
    /// where the exact probe's bound would have taken it. Its survival is the proof the catalog path
    /// (not the row probe) set the cutoff.
    /// </summary>
    [Fact]
    public async Task DimensionGc_MeasuresTheFloorFromChunkMetadata_EvenWithCompressedHistory()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the rig is expected to have TimescaleDB");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var (serverId, serverName) = NewServer();

        var heldFactTime = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-45), DateTimeKind.Unspecified);
        var referencedDigest = PayloadDimensions.Digest("pm1815-referenced-" + Guid.NewGuid().ToString("N"));
        var betweenDigest = PayloadDimensions.Digest("pm1815-between-" + Guid.NewGuid().ToString("N"));
        var orphanDigest = PayloadDimensions.Digest("pm1815-orphan-" + Guid.NewGuid().ToString("N"));

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* The held digest-carrying fact, mid-chunk (12:00) so a gap exists between its chunk's
               range_start and the row itself — the discriminator dim lives in that gap. */
            var midChunk = new DateTime(heldFactTime.Year, heldFactTime.Month, heldFactTime.Day, 12, 0, 0);
            using (var seed = new NpgsqlCommand(
                "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_digest) " +
                "VALUES ($1, $2, $3, $4, 'pm1815', '0xPM1815', $5)", connection))
            {
                seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
                seed.Parameters.AddWithValue(midChunk);
                seed.Parameters.AddWithValue(serverId);
                seed.Parameters.AddWithValue(serverName);
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = referencedDigest });
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* Compress that chunk with the product's own settings — the field shape the exact probe
               died on. */
            await ExecAsync(connection, TimescaleSupport.EnableCompressionSql("query_stats"), ct);
            await ExecAsync(connection,
                "SELECT compress_chunk(c, true) FROM show_chunks('collect.query_stats') AS c", ct);

            /* Dims: referenced (last_seen at the fact) survives; the between-dim sits inside the
               chunk-floor .. exact-floor gap and survives ONLY under the catalog bound; the ancient
               orphan goes either way. */
            foreach (var (digest, seen, tag) in new[]
            {
                (referencedDigest, midChunk, "referenced"),
                /* 30 hours below the 12:00 fact = 06:00 the previous day — inside
                   [chunk range_start - 1d, exact digest floor - 1d), the band only the catalog
                   bound retains. */
                (betweenDigest, midChunk.AddHours(-30), "between"),
                (orphanDigest, DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-500), DateTimeKind.Unspecified), "orphan"),
            })
            {
                using var seed = new NpgsqlCommand(
                    $"INSERT INTO {PayloadDimensions.QueryPlanDimTable} (digest, query_plan_xml, last_seen) " +
                    "VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING", connection);
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
                seed.Parameters.AddWithValue($"<ShowPlanXML pm1815=\"{tag}\"/>");
                seed.Parameters.AddWithValue(seen);
                await seed.ExecuteNonQueryAsync(ct);
            }

            var log = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, log, ct, PurgeDimFeeding(30));

            /* Measured, not deferred — the unmeasurable path and its per-table warning must be absent. */
            Assert.DoesNotContain(DeferralSignature, log.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("could not measure", log.Joined, StringComparison.Ordinal);
            Assert.Contains(BoundedSignature, log.Joined, StringComparison.Ordinal);

            /* The substantive trio. The between-dim is the path discriminator: the chunk's range_start
               (midnight) minus the one-day margin retains it; the exact digest floor (12:00) minus the
               margin would have taken it. */
            Assert.Equal(0L, await DimRowCountAsync(connection, orphanDigest, ct));
            Assert.Equal(1L, await DimRowCountAsync(connection, referencedDigest, ct));
            Assert.Equal(1L, await DimRowCountAsync(connection, betweenDigest, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecAsync(cleanup, "SELECT decompress_chunk(c, true) FROM show_chunks('collect.query_stats') AS c", cleanupCt);
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, referencedDigest, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, betweenDigest, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, orphanDigest, cleanupCt);
            });
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The #1795 property, in the exact field shape that motivated it: on a coverage-lagging store the
    /// #1784 clamp holds query_stats' drop EVERY sweep, and before this change that set the #1782 defer
    /// flag — so the dimension GC stood down every cycle and a 400-day orphan survived with nothing
    /// failed anywhere. The GC now prunes against the oldest SURVIVING digest-carrying fact row: the
    /// orphan (older than anything still referencing content) is reclaimed even while the clamp holds,
    /// and content the held facts still reference survives, because pruning it would dangle digests the
    /// clamp deliberately kept alive.
    /// </summary>
    [Fact]
    public async Task DimensionGc_PrunesOrphansButKeepsReferencedContent_WhileTheCoverageClampHoldsFacts()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the rig is expected to have TimescaleDB");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        var (serverId, serverName) = NewServer();

        /* The surviving fact: 45 days back — past the 30-day catalog horizon (the clamp is what keeps
           it alive) and past the ~32-day assumed GC cutoff, so only the MEASURED bound protects the
           content it references. */
        var heldFactTime = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-45), DateTimeKind.Unspecified);
        var referencedDigest = PayloadDimensions.Digest("pm1795-referenced-" + Guid.NewGuid().ToString("N"));
        var orphanDigest = PayloadDimensions.Digest("pm1795-orphan-" + Guid.NewGuid().ToString("N"));
        var orphanSeen = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-400), DateTimeKind.Unspecified);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* INSIDE the try: creating aggregates is the shared-fixture mutation the finally restores.
               Policies removed immediately - see EnsureAggregatesWithoutPoliciesAsync. */
            await EnsureAggregatesWithoutPoliciesAsync(connection, ct);

            /* The held digest-carrying fact... */
            using (var seed = new NpgsqlCommand(
                "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_digest) " +
                "VALUES ($1, $2, $3, $4, 'pm1795', '0xPM1795', $5)", connection))
            {
                seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
                seed.Parameters.AddWithValue(heldFactTime);
                seed.Parameters.AddWithValue(serverId);
                seed.Parameters.AddWithValue(serverName);
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = referencedDigest });
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* ...the content it references (last_seen = its sighting time, 45d — inside the measured
               bound of floor minus one day)... */
            using (var seed = new NpgsqlCommand(
                $"INSERT INTO {PayloadDimensions.QueryPlanDimTable} (digest, query_plan_xml, last_seen) " +
                "VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING", connection))
            {
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = referencedDigest });
                seed.Parameters.AddWithValue("<ShowPlanXML pm1795=\"referenced\"/>");
                seed.Parameters.AddWithValue(heldFactTime);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* ...and the orphan nothing references, older than the oldest surviving fact. */
            using (var seed = new NpgsqlCommand(
                $"INSERT INTO {PayloadDimensions.QueryPlanDimTable} (digest, query_plan_xml, last_seen) " +
                "VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING", connection))
            {
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = orphanDigest });
                seed.Parameters.AddWithValue("<ShowPlanXML pm1795=\"orphan\"/>");
                seed.Parameters.AddWithValue(orphanSeen);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* Coverage LAGS: the aggregate holds recent buckets only, none reaching the 45-day fact, so
               the clamp holds query_stats' drop — the field state. */
            using (var refresh = new NpgsqlCommand(
                TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.QueryStatsHourlyView), connection))
            {
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            Assert.False(
                await TimescaleSupport.IsRawTierDropSafeAsync(connection, "query_stats", ct),
                "the clamp must be holding query_stats for this test to mean anything");

            var log = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, log, ct, PurgeDimFeeding(30));

            /* The clamp held (the fact survived its horizon) — and the GC ran ANYWAY, bounded. */
            Assert.Contains(CoverageSkipSignature, log.Joined, StringComparison.Ordinal);
            Assert.Equal(1L, await AncientRowCountAsync(connection, serverId, ct));
            Assert.DoesNotContain(DeferralSignature, log.Joined, StringComparison.Ordinal);
            Assert.Contains(BoundedSignature, log.Joined, StringComparison.Ordinal);

            /* The substantive pair: the orphan is reclaimed, the referenced content is not. */
            Assert.Equal(0L, await DimRowCountAsync(connection, orphanDigest, ct));
            Assert.Equal(1L, await DimRowCountAsync(connection, referencedDigest, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, referencedDigest, cleanupCt);
                await DeleteDimRowAsync(cleanup, PayloadDimensions.QueryPlanDimTable, orphanDigest, cleanupCt);
                await RestoreCaggsAsync(cleanup, preexistingCaggs, cleanupCt);
            });
        }
    }

    /// <summary>
    /// The catalog sweep must not drop raw chunks the rollup has not captured (#1784).
    ///
    /// <para>Two purges drop these same chunks: the tiered 4-day POLICY, which the #1680 gate holds paused
    /// until the hourly aggregate covers the table's history, and this catalog sweep at the per-collector
    /// 30-day horizon — which had no coverage check at all. On a store where the gate is deliberately holding
    /// the policy, the sweep destroyed exactly the uncovered history the gate exists to protect, silently.</para>
    ///
    /// <para>Both halves are asserted, because a guard that never lets anything drop would pass the first one
    /// alone: with coverage lagging the old row SURVIVES, and once the aggregate reaches back over it the very
    /// same row is dropped. The backstop still backstops.</para>
    /// </summary>
    [Fact]
    public async Task CatalogSweep_SkipsARawDropTheRollupHasNotCovered_AndResumesOnceItHas()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedStoreAsync(connectionString!, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the rig is expected to have TimescaleDB");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Creating aggregates is GLOBAL state on the shared fixture, so snapshot and restore only what this
           test adds — see ExistingCaggsAsync. */
        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        var (serverId, serverName) = NewServer();

        /* Well past the 30-day catalog horizon, so the sweep would certainly take it if allowed. */
        var ancient = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-45), DateTimeKind.Unspecified);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* INSIDE the try: creating aggregates is the shared-fixture mutation the finally restores,
               so it must not happen where a throw would skip that restore. Policies removed immediately -
               their instantly-firing jobs are the 55P03/stranded-aggregate race this class chased in #1795. */
            await EnsureAggregatesWithoutPoliciesAsync(connection, ct);

            using (var seed = new NpgsqlCommand(
                "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash) " +
                "VALUES ($1, $2, $3, $4, 'pm1784', '0xPM1784')", connection))
            {
                seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
                seed.Parameters.AddWithValue(ancient);
                seed.Parameters.AddWithValue(serverId);
                seed.Parameters.AddWithValue(serverName);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* Coverage LAGS: refresh the aggregate only over the recent window, so it holds buckets but none
               reaching back to the ancient row. That is the field state — a rollup that starts later than raw. */
            using (var refresh = new NpgsqlCommand(
                TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.QueryStatsHourlyView), connection))
            {
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            Assert.False(
                await TimescaleSupport.IsRawTierDropSafeAsync(connection, "query_stats", ct),
                "with the rollup starting after raw's oldest row, dropping must NOT be judged safe");

            var skipLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, skipLog, ct, OnlyPurge("query_stats", 40));

            /* THE property: uncovered history survives the sweep. Asserted before the log line so a mutation
               lands on the data loss, not on missing text. */
            Assert.Equal(1L, await AncientRowCountAsync(connection, serverId, ct));
            Assert.Contains(CoverageSkipSignature, skipLog.Joined, StringComparison.Ordinal);

            /* Now let coverage reach back over the ancient row. force => recompute the older buckets. */
            using (var refresh = new NpgsqlCommand(
                TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.QueryStatsHourlyView, force: true), connection))
            {
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(ancient.AddDays(-1), DateTimeKind.Unspecified));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            Assert.True(
                await TimescaleSupport.IsRawTierDropSafeAsync(connection, "query_stats", ct),
                "once the rollup covers raw's oldest row the drop must be judged safe again");

            var dropLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, dropLog, ct, OnlyPurge("query_stats", 40));

            Assert.Equal(0L, await AncientRowCountAsync(connection, serverId, ct));
            Assert.DoesNotContain(CoverageSkipSignature, dropLog.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteServerRowsAsync(cleanup, serverId, cleanupCt);
                await RestoreCaggsAsync(cleanup, preexistingCaggs, cleanupCt);
            });
        }
    }


    /// <summary>
    /// The continuous aggregates present in <c>collect</c> right now, so a test that creates them can drop
    /// exactly the ones it created and leave the shared fixture's shape alone. Mirrors the machinery
    /// TimescaleSupportTests already owns for the same reason: creating aggregates is GLOBAL state — it changes
    /// compose's tier routing, and it makes EnsureBaselineFallbackViewsAsync a no-op for the baseline tests,
    /// which is how CI caught this before it merged.
    /// </summary>
    private static async Task<string[]> ExistingCaggsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect'", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        var names = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }

    /// <summary>
    /// <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/> and then REMOVES every rollup's
    /// refresh policy. The ensure attaches policies whose jobs fire IMMEDIATELY (the #1788 finding), and
    /// those background refreshes race everything this class does next: a manual force-refresh collides
    /// with 55P03 "concurrent refresh", and a restore's DROP can collide with a running job's lock and
    /// silently strand the aggregate in the shared fixture — which is exactly how query_stats_db_hourly
    /// and query_stats_db_daily came to persist across runs and flip the #1784 gate for every later
    /// test. The tests refresh manually and deterministically; they never need the scheduler.
    ///
    /// <para><b>The conversion first is not optional and belongs HERE, not at the call sites (#1862).</b>
    /// TimescaleDB will not build a continuous aggregate over a heap, and the ensure is failure-isolated per
    /// aggregate — so against un-converted tables it creates NOTHING and says so only to a logger this class
    /// passes as null. The next line then force-refreshes a view that was never created and the test dies on
    /// <c>42P01 relation "collect.query_stats_hourly" does not exist</c>, naming a symptom three steps
    /// downstream of the cause. Three of this class's four callers already convert on their own way in, which
    /// is precisely why the fourth survived: on a shared store the conversion is PERSISTENT, so whoever ran
    /// first covered for whoever ran next, and the gap only surfaced on a fresh database in an unlucky order.
    /// Putting it in the helper makes "aggregates exist afterwards" true of the helper rather than true of the
    /// caller's memory, and it costs the other three nothing — <c>if_not_exists</c> makes it a no-op.</para>
    /// </summary>
    private static async Task EnsureAggregatesWithoutPoliciesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* VERIFIED removals (#1873). This step is the one that closes the standing race, so a removal that
           quietly did not happen re-opens it for every aggregate the class goes on to create — and the
           swallow this replaces made that indistinguishable from success. */
        var batch = new LiveCleanupBatch(connection);
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            await batch.RemoveRefreshPolicyAsync(view, ct);
        }
    }

    /// <summary>
    /// Makes the raw tier's rollup coverage reach back over everything raw holds, so the #1784 gate judges a
    /// drop SAFE.
    ///
    /// <para>Deliberately does NOT capture the restore snapshot: the caller must take it BEFORE calling this,
    /// outside its try, so a throw part-way through creation still leaves the finally a baseline to restore
    /// against. Owning the snapshot here made the restore conditional on this method RETURNING, which is
    /// exactly the window in which it leaks aggregates into the shared fixture.</para>
    ///
    /// <para>The refresh retries bounded on 55P03: removing the policies closes the standing race, but a
    /// job that was ALREADY EXECUTING when its policy was removed finishes its run, and one collision can
    /// still land. The product's backfill slices carry the same bounded transient-only retry for the same
    /// reason (#1788); every other SQLSTATE still fails fast.</para>
    /// </summary>
    private static async Task SatisfyRollupCoverageAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await EnsureAggregatesWithoutPoliciesAsync(connection, ct);

        /* Every coverage relation, not just the first: a raw table can have more than one rollup family
           gating its purge (#1849), and the #1784 gate is an AND over all of them — refreshing one would
           leave the gate correctly refusing and the test looking at the wrong cause. */
        foreach (var (relation, _, coverageRelations) in TimescaleSupport.RawTierCoverage)
        foreach (var coverage in coverageRelations)
        {
            _ = relation;
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    using var refresh = new NpgsqlCommand(
                        TimescaleSupport.RefreshContinuousAggregateSql(coverage, force: true), connection);
                    refresh.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-3650), DateTimeKind.Unspecified));
                    await refresh.ExecuteNonQueryAsync(ct);
                    break;
                }
                catch (PostgresException ex) when (ex.SqlState == "55P03" && attempt < 5)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), ct);
                }
            }
        }
    }

    /// <summary>
    /// Puts the shared store's aggregate shape back, and CONFIRMS it (#1873).
    ///
    /// <para>Every removal here is retried until the catalog agrees it is gone, because the collision this
    /// loses to is real and reproducible: a <c>DROP MATERIALIZED VIEW</c> racing a still-executing
    /// <c>refresh_continuous_aggregate</c> fails and leaves the aggregate STANDING — as <c>40P01</c> when the
    /// deadlock detector picks the drop as its victim, and as <c>XX000 tuple concurrently deleted</c> when the
    /// refresh's catalog maintenance beats it to a row. Both were measured on PostgreSQL 18.4 +
    /// TimescaleDB 2.28.1, and the retry won on the following attempt each time. What it does NOT do is
    /// classify the SQLSTATE: <c>XX000</c> is <c>internal_error</c>, so any allow-list that looked reasonable
    /// would have excluded the very failure it needed to survive.</para>
    /// </summary>
    private static async Task RestoreCaggsAsync(NpgsqlConnection connection, string[] preexisting, CancellationToken ct)
    {
        var batch = new LiveCleanupBatch(connection);

        foreach (var (relation, _, coverage) in TimescaleSupport.RawTierCoverage)
        {
            await batch.RemoveRetentionPolicyAsync(relation, ct);
            _ = coverage;
        }

        await batch.DropContinuousAggregatesAsync(
            (await ExistingCaggsAsync(connection, ct)).Except(preexisting, StringComparer.Ordinal), ct);
    }

    /// <summary>
    /// A retention resolver that leaves every table alone except the one under test. PurgeAsync is FLEET-WIDE
    /// and destructive, and the live fixture is shared: a bare call drops any sibling class's rows older than
    /// their horizon. Handing every other collector a decade of retention makes the purge surgical while still
    /// exercising the real sweep.
    /// </summary>
    private static Func<string, int> OnlyPurge(string collectorName, int retentionDays)
        => name => string.Equals(name, collectorName, StringComparison.Ordinal) ? retentionDays : 3650;

    /// <summary>
    /// Like <see cref="OnlyPurge"/> but keeps EVERY dim-feeding table on the given horizon, because the
    /// dimension GC's cutoff is the WIDEST of them plus a margin. Handing one of them a decade — as the
    /// single-table resolver does — pushes that cutoff out by a decade too, and the GC then prunes nothing,
    /// which silently turns a dimension test green for the wrong reason.
    /// </summary>
    private static Func<string, int> PurgeDimFeeding(int retentionDays)
        => name =>
        {
            foreach (var definition in CollectorCatalog.All)
            {
                if (string.Equals(definition.Name, name, StringComparison.Ordinal))
                {
                    return PayloadDimensions.ForTable(definition.TargetTable).Count > 0 ? retentionDays : 3650;
                }
            }

            return 3650;
        };

    private static async Task<long> AncientRowCountAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var read = new NpgsqlCommand("SELECT COUNT(*) FROM query_stats WHERE server_id = $1", connection);
        read.Parameters.AddWithValue(serverId);
        return (long)(await read.ExecuteScalarAsync(ct))!;
    }
}
