/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the pending lists of the two trigram indexes on the Query Store text, on a real store. Every row that reaches either index adds
/// its text's trigrams to the index's pending list, and the insert that takes the list past <c>gin_pending_list_limit</c> merges the whole
/// list inside its own call. A build creates the server-keyed index with the 64 MB limit; the clean sets the limit on an index that lacks
/// it, the server-keyed one and the one on the text alone it replaces, and empties the list of each valid index; a limit change whose lock
/// is not free is skipped at the lock timeout and set by the next clean; an index that carries the limit is not changed again; an INVALID
/// index is left alone; and each index is handled on its own, whatever state the other is in.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it, so it cannot race live collection. Every test uses its own tracker, never
   the process-wide one. */
public sealed class QueryStoreTextTrigramIndexPendingListLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 live test.";

    private const string Table = QueryStoreTextTrigramIndex.TableName;

    private const int Limit = QueryTextTrigramIndex.PendingListLimitKb;

    /* Five times the lock timeout: a limit change that waits for its lock instead of giving up runs past it. */
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(10);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime Now = new(2026, 11, 18, 12, 0, 0, DateTimeKind.Utc);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /* A few thousand distinct texts over two servers. Autovacuum is off on the table first: an autovacuum analyze empties a GIN pending
       list on its own, and these tests read the list before and after a clean. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, $"ALTER TABLE {Table} SET (autovacuum_enabled = off)", ct);
        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pgstattuple", ct);
        await ExecAsync(connection,
            $@"INSERT INTO {Table} (server_id, database_name, query_id, query_sql_text, query_hash, last_seen)
              SELECT 1 + i % 2, 'db_' || (i % 4), i, 'SELECT * FROM dbo.Orders_' || i || ' WHERE OrderId = @p', '0x' || i, timezone('UTC', now())
              FROM generate_series(1, 3000) AS i", ct);
    }

    /* Two hundred more texts. Each existing trigram index takes their trigrams into its pending list. */
    private static async Task AddRowsAsync(NpgsqlConnection connection, int firstQueryId, CancellationToken ct) =>
        await ExecAsync(connection,
            $@"INSERT INTO {Table} (server_id, database_name, query_id, query_sql_text, query_hash, last_seen)
              SELECT 1 + i % 2, 'db_new', {firstQueryId} + i, 'SELECT * FROM dbo.Lines_' || ({firstQueryId} + i) || ' WHERE LineId = @p', '0x' || ({firstQueryId} + i), timezone('UTC', now())
              FROM generate_series(1, 200) AS i", ct);

    private static async Task<string> ExtensionSchemaAsync(NpgsqlConnection connection, string extension, CancellationToken ct) =>
        (string)(await ScalarAsync(
            connection, $"SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = '{extension}'", ct))!;

    /* The named index with no storage options, as a build without the limit makes it. Superuser only. */
    private static async Task CreateWithoutTheLimitAsync(NpgsqlConnection connection, string index, CancellationToken ct)
    {
        if (index == QueryStoreTextTrigramIndex.ReplacedIndexName)
        {
            await QueryStoreTextTrigramIndexLiveTests.CreateReplacedIndexAsync(connection, ct);
        }
        else
        {
            await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pg_trgm", ct);
            await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS btree_gin", ct);
            var text = await ExtensionSchemaAsync(connection, "pg_trgm", ct);
            var key = await ExtensionSchemaAsync(connection, "btree_gin", ct);
            await ExecAsync(connection,
                $"CREATE INDEX {QueryStoreTextTrigramIndex.IndexBareName} ON {Table} USING gin (server_id \"{key}\".int4_ops, query_sql_text \"{text}\".gin_trgm_ops)", ct);
        }

        Assert.NotEqual(Limit, await LimitAsync(connection, index, ct));
    }

    private static string Other(string index) =>
        index == QueryStoreTextTrigramIndex.IndexName ? QueryStoreTextTrigramIndex.ReplacedIndexName : QueryStoreTextTrigramIndex.IndexName;

    private static async Task<bool> ExistsAsync(NpgsqlConnection connection, string index, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT to_regclass('{index}') IS NOT NULL", ct))!;

    /* The limit the index runs under, in kB: its own storage option, else the server's setting. */
    private static async Task<int> LimitAsync(NpgsqlConnection connection, string index, CancellationToken ct) =>
        Convert.ToInt32(await ScalarAsync(connection,
            $"SELECT coalesce((SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'gin_pending_list_limit')::int, (SELECT setting::int FROM pg_settings WHERE name = 'gin_pending_list_limit')) FROM pg_class AS c WHERE c.oid = '{index}'::regclass", ct),
            CultureInfo.InvariantCulture);

    /* The rows in the index's pending list. pgstatginindex reads a valid index only, so a test that stages an INVALID one reads its list
       before the index is marked INVALID and again once it is marked valid. */
    private static async Task<long> PendingAsync(NpgsqlConnection connection, string index, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, $"SELECT pending_tuples FROM pgstatginindex('{index}')", ct), CultureInfo.InvariantCulture);

    /* The version of the index's catalog row. Any ALTER INDEX writes a new one, the clean of the list does not. */
    private static async Task<string> CatalogRowVersionAsync(NpgsqlConnection connection, string index, CancellationToken ct) =>
        (string)(await ScalarAsync(connection, $"SELECT xmin::text FROM pg_class WHERE oid = '{index}'::regclass", ct))!;

    private static Task SetValidAsync(NpgsqlConnection connection, string index, bool valid, CancellationToken ct) =>
        ExecAsync(connection, $"UPDATE pg_index SET indisvalid = {(valid ? "true" : "false")} WHERE indexrelid = '{index}'::regclass", ct);

    private static Task CleanAsync(NpgsqlDataSource data, ILogger logger, CancellationToken ct) =>
        QueryStoreTextTrigramIndex.CleanPendingListsAsync(data, () => true, logger, ct);

    /* A search in an open transaction: it holds ACCESS SHARE on every index the table has at that moment until the transaction ends. */
    private static async Task<NpgsqlConnection> OpenSearchHoldingAsync(ScratchPostgres scratch, NpgsqlConnection admin, string index, CancellationToken ct)
    {
        var reader = new NpgsqlConnection(scratch.ConnectionString);
        await reader.OpenAsync(ct);
        await ExecAsync(reader, "BEGIN", ct);
        await ExecAsync(reader, "SET LOCAL enable_seqscan = off", ct);
        await ExecAsync(reader, $"SELECT count(*) FROM {Table} WHERE server_id = 1 AND query_sql_text LIKE '%Orders_5%'", ct);
        Assert.True((bool)(await ScalarAsync(admin,
            $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = '{index}'::regclass AND mode = 'AccessShareLock' AND granted)", ct))!,
            "the search should hold the index");
        return reader;
    }

    /* True once the limit change is waiting for its ACCESS EXCLUSIVE lock on the index. */
    private static async Task<bool> LimitChangeQueuedAsync(NpgsqlConnection admin, string index, CancellationToken ct)
    {
        for (var i = 0; i < 80; i++)
        {
            if ((bool)(await ScalarAsync(admin,
                $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = '{index}'::regclass AND mode = 'AccessExclusiveLock' AND NOT granted)", ct))!)
            {
                return true;
            }

            await Task.Delay(50, ct);
        }

        return false;
    }

    [Fact]
    public async Task ABuild_CreatesTheIndexWithThePendingListLimit()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);

        await QueryStoreTextTrigramIndex.RunPassAsync(data, true, tracker, Now, new CapturingTestLogger(), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.Equal(Limit, await LimitAsync(admin, QueryStoreTextTrigramIndex.IndexName, ct));

        /* The limit is the index's own, so the clean that follows has nothing to change. */
        var version = await CatalogRowVersionAsync(admin, QueryStoreTextTrigramIndex.IndexName, ct);
        await AddRowsAsync(admin, 100000, ct);
        Assert.True(await PendingAsync(admin, QueryStoreTextTrigramIndex.IndexName, ct) > 0);
        var logger = new CapturingTestLogger();
        await CleanAsync(data, logger, ct);

        Assert.Equal(0L, await PendingAsync(admin, QueryStoreTextTrigramIndex.IndexName, ct));
        Assert.Equal(version, await CatalogRowVersionAsync(admin, QueryStoreTextTrigramIndex.IndexName, ct));
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
    }

    /* An index a build made without the limit, the server-keyed one or the one on the text alone a store still runs on: the next clean
       sets the limit and empties the list, and creates nothing. */
    [Theory]
    [InlineData(QueryStoreTextTrigramIndex.IndexName)]
    [InlineData(QueryStoreTextTrigramIndex.ReplacedIndexName)]
    public async Task ACleanSetsTheLimitOnAnIndexWithoutIt_AndEmptiesItsList(string index)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, index, ct);
        await AddRowsAsync(admin, 100000, ct);
        Assert.True(await PendingAsync(admin, index, ct) > 0);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingTestLogger();

        await CleanAsync(data, logger, ct);

        Assert.Equal(Limit, await LimitAsync(admin, index, ct));
        Assert.Equal(0L, await PendingAsync(admin, index, ct));
        Assert.False(await ExistsAsync(admin, Other(index), ct));
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
    }

    /* The limit is set once. A clean of an index that carries it empties the list and leaves the index's catalog row as it is, so it takes
       no ACCESS EXCLUSIVE lock for a search or an insert to wait behind. */
    [Theory]
    [InlineData(QueryStoreTextTrigramIndex.IndexName)]
    [InlineData(QueryStoreTextTrigramIndex.ReplacedIndexName)]
    public async Task ACleanOfAnIndexThatCarriesTheLimit_EmptiesItsList_AndDoesNotChangeTheIndexAgain(string index)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, index, ct);
        var created = await CatalogRowVersionAsync(admin, index, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await CleanAsync(data, new CapturingTestLogger(), ct);
        Assert.Equal(Limit, await LimitAsync(admin, index, ct));
        var changed = await CatalogRowVersionAsync(admin, index, ct);
        Assert.NotEqual(created, changed);

        await AddRowsAsync(admin, 100000, ct);
        Assert.True(await PendingAsync(admin, index, ct) > 0);
        await CleanAsync(data, new CapturingTestLogger(), ct);

        Assert.Equal(0L, await PendingAsync(admin, index, ct));
        Assert.Equal(changed, await CatalogRowVersionAsync(admin, index, ct));
        Assert.Equal(Limit, await LimitAsync(admin, index, ct));
    }

    /* The limit change takes ACCESS EXCLUSIVE on the index, so with a search holding the index it queues, and every insert queues behind
       it. The change runs under the lock timeout: a timeout is a quiet skip, the clean still empties the list, an insert waits no longer
       than the timeout, and the clean after the search ends sets the limit. */
    [Theory]
    [InlineData(QueryStoreTextTrigramIndex.IndexName)]
    [InlineData(QueryStoreTextTrigramIndex.ReplacedIndexName)]
    public async Task ACleanWithASearchHoldingTheIndex_SkipsTheLimitChangeAtTheLockTimeout_AndTheNextCleanSetsIt(string index)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, index, ct);
        await AddRowsAsync(admin, 100000, ct);
        Assert.True(await PendingAsync(admin, index, ct) > 0);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var inserter = new NpgsqlConnection(scratch.ConnectionString);
        await inserter.OpenAsync(ct);
        await using var reader = await OpenSearchHoldingAsync(scratch, admin, index, ct);
        var logger = new CapturingTestLogger();

        var clean = CleanAsync(data, logger, ct);
        var queued = await LimitChangeQueuedAsync(admin, index, ct);
        var insert = AddRowsAsync(inserter, 200000, ct);
        var window = Task.Delay(Window, ct);
        var insertDone = await Task.WhenAny(insert, window) == insert;
        var cleanDone = await Task.WhenAny(clean, window) == clean;
        if (!insertDone || !cleanDone)
        {
            /* Release the search so a regression fails here instead of hanging on the queued change. */
            await ExecAsync(reader, "ROLLBACK", ct);
            await Task.WhenAll(insert, clean);
        }

        Assert.True(queued, "the limit change should be waiting for the index");
        Assert.True(insertDone, "an insert should not wait past the lock timeout");
        Assert.True(cleanDone, "the clean should not wait past the lock timeout");
        await insert;
        await clean;
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
        Assert.NotEqual(Limit, await LimitAsync(admin, index, ct));

        /* The search is over: the next clean sets the limit and empties the list, the rows the insert added included. */
        await ExecAsync(reader, "COMMIT", ct);
        await CleanAsync(data, logger, ct);

        Assert.Equal(Limit, await LimitAsync(admin, index, ct));
        Assert.Equal(0L, await PendingAsync(admin, index, ct));
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
    }

    /* An INVALID index is a build in progress or its leftover, which the pass drops. The clean neither changes it nor empties its list,
       and takes it up once it is valid. */
    [Theory]
    [InlineData(QueryStoreTextTrigramIndex.IndexName)]
    [InlineData(QueryStoreTextTrigramIndex.ReplacedIndexName)]
    public async Task ACleanLeavesAnInvalidIndexAlone(string index)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, index, ct);
        await AddRowsAsync(admin, 100000, ct);
        var pending = await PendingAsync(admin, index, ct);
        Assert.True(pending > 0);
        /* The test roles are superuser, so the state is staged exactly. */
        await SetValidAsync(admin, index, false, ct);
        var version = await CatalogRowVersionAsync(admin, index, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingTestLogger();

        await CleanAsync(data, logger, ct);

        Assert.NotEqual(Limit, await LimitAsync(admin, index, ct));
        Assert.Equal(version, await CatalogRowVersionAsync(admin, index, ct));
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
        await SetValidAsync(admin, index, true, ct);
        Assert.Equal(pending, await PendingAsync(admin, index, ct));

        await CleanAsync(data, logger, ct);

        Assert.Equal(Limit, await LimitAsync(admin, index, ct));
        Assert.Equal(0L, await PendingAsync(admin, index, ct));
    }

    /* Both indexes exist while the server-keyed one builds. Each is judged on its own state: the valid one gets the limit and an empty
       list while the INVALID one is left alone, whichever of the two that is. */
    [Fact]
    public async Task TheTwoIndexes_AreEachCleanedOnTheirOwnState()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const string keyed = QueryStoreTextTrigramIndex.IndexName;
        const string replaced = QueryStoreTextTrigramIndex.ReplacedIndexName;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, keyed, ct);
        await CreateWithoutTheLimitAsync(admin, replaced, ct);
        await AddRowsAsync(admin, 100000, ct);
        var keyedPending = await PendingAsync(admin, keyed, ct);
        Assert.True(keyedPending > 0);
        Assert.True(await PendingAsync(admin, replaced, ct) > 0);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingTestLogger();

        /* The server-keyed index is still building: only the one on the text alone is cleaned. */
        await SetValidAsync(admin, keyed, false, ct);
        await CleanAsync(data, logger, ct);

        Assert.Equal(Limit, await LimitAsync(admin, replaced, ct));
        Assert.Equal(0L, await PendingAsync(admin, replaced, ct));
        Assert.NotEqual(Limit, await LimitAsync(admin, keyed, ct));
        await SetValidAsync(admin, keyed, true, ct);
        Assert.Equal(keyedPending, await PendingAsync(admin, keyed, ct));

        /* The other way round: the server-keyed index is valid and the one on the text alone is not. */
        await AddRowsAsync(admin, 200000, ct);
        var replacedPending = await PendingAsync(admin, replaced, ct);
        Assert.True(replacedPending > 0);
        await SetValidAsync(admin, replaced, false, ct);
        await CleanAsync(data, logger, ct);

        Assert.Equal(Limit, await LimitAsync(admin, keyed, ct));
        Assert.Equal(0L, await PendingAsync(admin, keyed, ct));
        await SetValidAsync(admin, replaced, true, ct);
        Assert.Equal(replacedPending, await PendingAsync(admin, replaced, ct));

        /* Both valid, both carrying the limit: both lists are emptied. */
        await CleanAsync(data, logger, ct);

        Assert.Equal(0L, await PendingAsync(admin, keyed, ct));
        Assert.Equal(0L, await PendingAsync(admin, replaced, ct));
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
    }

    /* A search holds one index and not the other (the other is created after the search's statement ran). The held one's limit change is
       skipped at the lock timeout, and in the same clean the other gets its limit and both lists are emptied, whichever index is held. */
    [Theory]
    [InlineData(QueryStoreTextTrigramIndex.IndexName)]
    [InlineData(QueryStoreTextTrigramIndex.ReplacedIndexName)]
    public async Task ASearchHoldingOneIndex_DoesNotKeepTheOtherFromItsLimitOrItsClean(string held)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var free = Other(held);

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, held, ct);
        await using var reader = await OpenSearchHoldingAsync(scratch, admin, held, ct);
        await CreateWithoutTheLimitAsync(admin, free, ct);
        Assert.Equal(0L, await ScalarAsync(admin, $"SELECT count(*) FROM pg_locks WHERE relation = '{free}'::regclass", ct));
        await AddRowsAsync(admin, 100000, ct);
        Assert.True(await PendingAsync(admin, held, ct) > 0);
        Assert.True(await PendingAsync(admin, free, ct) > 0);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingTestLogger();

        var clean = CleanAsync(data, logger, ct);
        var queued = await LimitChangeQueuedAsync(admin, held, ct);
        var cleanDone = await Task.WhenAny(clean, Task.Delay(Window, ct)) == clean;
        if (!cleanDone)
        {
            await ExecAsync(reader, "ROLLBACK", ct);
        }

        await clean;
        Assert.True(queued, "the held index's limit change should be waiting for the index");
        Assert.True(cleanDone, "the clean should not wait past the lock timeout");
        Assert.Equal(Limit, await LimitAsync(admin, free, ct));
        Assert.Equal(0L, await PendingAsync(admin, free, ct));
        Assert.NotEqual(Limit, await LimitAsync(admin, held, ct));
        Assert.Equal(0L, await PendingAsync(admin, held, ct));

        await ExecAsync(reader, "COMMIT", ct);
        await CleanAsync(data, logger, ct);

        Assert.Equal(Limit, await LimitAsync(admin, held, ct));
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, logger.Joined);
    }

    private static async Task<bool> EmptiedAsync(NpgsqlConnection admin, string index, CancellationToken ct)
    {
        for (var i = 0; i < 100; i++)
        {
            if (await PendingAsync(admin, index, ct) == 0L)
            {
                return true;
            }

            await Task.Delay(200, ct);
        }

        return false;
    }

    /* The service's background task makes its first clean at once when its first delay is zero, with the option off cleans nothing, and
       ends when the service stops. */
    [Fact]
    public async Task TheBackgroundTask_CleansAnIndexWithoutTheLimitWhenTheOptionIsOn_AndLeavesItWhenOff()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const string index = QueryStoreTextTrigramIndex.IndexName;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, index, ct);
        await AddRowsAsync(admin, 100000, ct);
        var pending = await PendingAsync(admin, index, ct);
        Assert.True(pending > 0);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        await QueryStoreTextTrigramIndex.CleanPendingListsAsync(data, () => false, new CapturingTestLogger(), ct);
        Assert.Equal(pending, await PendingAsync(admin, index, ct));
        Assert.NotEqual(Limit, await LimitAsync(admin, index, ct));

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var run = QueryStoreTextTrigramIndex.RunAsync(
            data, () => true, new QueryTextTrigramIndexTracker(), new CapturingTestLogger(), TimeSpan.Zero, QueryStoreTextTrigramIndex.PassInterval, cts.Token);
        var emptied = await EmptiedAsync(admin, index, ct);

        await cts.CancelAsync();
        await run;
        Assert.True(emptied, "the background task should have emptied the list");
        Assert.Equal(Limit, await LimitAsync(admin, index, ct));
    }

    /* A store that runs on the index on the text alone while the server-keyed build waits (here, inside the wait after a failed build):
       the background task gives that index the limit and empties its list, and builds nothing meanwhile. */
    [Fact]
    public async Task TheBackgroundTask_CleansTheIndexOnTheTextAlone_WhileTheServerKeyedBuildWaits()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const string replaced = QueryStoreTextTrigramIndex.ReplacedIndexName;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateWithoutTheLimitAsync(admin, replaced, ct);
        await AddRowsAsync(admin, 100000, ct);
        Assert.True(await PendingAsync(admin, replaced, ct) > 0);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        tracker.Failed(new InvalidOperationException("a staged failed build"), DateTime.UtcNow, TimeSpan.FromHours(6));

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var run = QueryStoreTextTrigramIndex.RunAsync(
            data, () => true, tracker, new CapturingTestLogger(), TimeSpan.Zero, QueryStoreTextTrigramIndex.PassInterval, cts.Token);
        var emptied = await EmptiedAsync(admin, replaced, ct);

        await cts.CancelAsync();
        await run;
        Assert.True(emptied, "the background task should have emptied the list of the index on the text alone");
        Assert.Equal(Limit, await LimitAsync(admin, replaced, ct));
        Assert.Equal(0, tracker.BuildsStarted);
        Assert.False(await ExistsAsync(admin, QueryStoreTextTrigramIndex.IndexName, ct));
    }
}
