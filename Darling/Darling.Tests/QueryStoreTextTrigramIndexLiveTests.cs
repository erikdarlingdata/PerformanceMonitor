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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the opt-in trigram index on the Query Store text on a real store: with the option off nothing is built, with it on exactly one
/// build happens, keyed by server, and the next pass changes nothing, turning the option off drops the index and the one on the text alone
/// it replaces, the replaced one is dropped once this one is valid and kept while a login cannot create <c>btree_gin</c>, an INVALID
/// leftover is dropped and rebuilt, a failed build is reported and waits before its retry, a login that cannot create the extensions is
/// reported with the statement an operator runs and is not asked again, the plan-cache index's option neither builds nor drops this index,
/// and the health read names each state.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it, so it cannot race live collection. Every test uses its own tracker, never
   the process-wide one. */
public sealed class QueryStoreTextTrigramIndexLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 live test.";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime Now = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);

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

    /* A few thousand distinct texts over two servers, so the build has something to index and the size is not zero. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection,
            @"INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, query_hash, last_seen)
              SELECT 1 + i % 2, 'db_' || (i % 4), i, 'SELECT * FROM dbo.Orders_' || i || ' WHERE OrderId = @p', '0x' || i, timezone('UTC', now())
              FROM generate_series(1, 3000) AS i", ct);
        Assert.Equal(3000L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_text", ct));
    }

    /// <summary>The index earlier builds made for this option, on the text alone, as they made it. Superuser only.</summary>
    internal static async Task CreateReplacedIndexAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pg_trgm", ct);
        var schema = (string)(await ScalarAsync(
            connection, "SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm'", ct))!;
        await ExecAsync(connection, $"CREATE INDEX ix_query_store_text_trgm ON collect.query_store_text USING gin (query_sql_text \"{schema}\".gin_trgm_ops)", ct);
        Assert.True(await ExistsAsync(connection, QueryStoreTextTrigramIndex.ReplacedIndexName, ct));
    }

    private static async Task<bool> ExistsAsync(NpgsqlConnection connection, string index, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT to_regclass('{index}') IS NOT NULL", ct))!;

    private static Task<bool> ExistsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExistsAsync(connection, QueryStoreTextTrigramIndex.IndexName, ct);

    private static async Task<bool> ValidAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreTextTrigramIndex.IndexName}'::regclass", ct))!;

    private static async Task<uint> OidAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToUInt32(await ScalarAsync(connection, $"SELECT '{QueryStoreTextTrigramIndex.IndexName}'::regclass::oid", ct), CultureInfo.InvariantCulture);

    private static Task<bool> PassAsync(NpgsqlDataSource data, bool enabled, QueryTextTrigramIndexTracker tracker, DateTime now, CancellationToken ct) =>
        QueryStoreTextTrigramIndex.RunPassAsync(data, enabled, tracker, now, NullLogger.Instance, ct);

    private static async Task<TrigramIndexHealth> HealthAsync(NpgsqlDataSource data, QueryTextTrigramIndexTracker tracker, CancellationToken ct)
    {
        await using var connection = await data.OpenConnectionAsync(ct);
        return await QueryStoreTextTrigramIndex.ReadHealthAsync(connection, tracker, ct);
    }

    [Fact]
    public async Task OptionOff_BuildsNoIndex_AndTheHealthReadSaysOff()
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
        tracker.Configure(false);

        var settled = await PassAsync(data, false, tracker, Now, ct);

        Assert.True(settled);
        Assert.False(await ExistsAsync(admin, ct));
        Assert.Equal(0, tracker.BuildsStarted);
        var health = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Off, health.Status);
        Assert.False(health.Exists);
        Assert.False(health.Enabled);
        Assert.Contains(QueryStoreTextTrigramIndex.OptionName, QueryStoreTextTrigramIndex.Note(health), StringComparison.Ordinal);
    }

    [Fact]
    public async Task OptionOn_BuildsOnce_AndTheNextPassChangesNothing()
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

        var before = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Pending, before.Status);
        Assert.False(before.Exists);

        await PassAsync(data, true, tracker, Now, ct);

        Assert.True(await ExistsAsync(admin, ct));
        Assert.True(await ValidAsync(admin, ct));
        Assert.Equal(1, tracker.BuildsStarted);
        /* The server is the first key (btree_gin's int4_ops), the text the second. */
        Assert.Equal(
            "server_id int4_ops, query_sql_text gin_trgm_ops",
            await ScalarAsync(admin, $@"SELECT string_agg(a.attname || ' ' || opc.opcname, ', ' ORDER BY k.n)
                FROM pg_index AS i
                CROSS JOIN LATERAL unnest(i.indkey::int2[], i.indclass::oid[]) WITH ORDINALITY AS k (attnum, opclass, n)
                JOIN pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                JOIN pg_opclass AS opc ON opc.oid = k.opclass
                WHERE i.indexrelid = '{QueryStoreTextTrigramIndex.IndexName}'::regclass", ct));
        var oid = await OidAsync(admin, ct);
        var built = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Ready, built.Status);
        Assert.True(built.Valid);
        Assert.True(built.ExtensionInstalled);
        Assert.True(built.SizeBytes > 0);
        Assert.Contains("find_queries_by_text", QueryStoreTextTrigramIndex.Note(built), StringComparison.Ordinal);

        await PassAsync(data, true, tracker, Now.AddHours(1), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.Equal(oid, await OidAsync(admin, ct));
    }

    [Fact]
    public async Task TurningTheOptionOff_DropsTheIndex()
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
        await PassAsync(data, true, tracker, Now, ct);
        Assert.True(await ValidAsync(admin, ct));

        /* The service restarts with the option off. */
        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        Assert.Equal(TrigramIndexStatus.DropPending, (await HealthAsync(data, off, ct)).Status);

        var settled = await PassAsync(data, false, off, Now.AddHours(2), ct);

        Assert.False(settled);
        Assert.False(await ExistsAsync(admin, ct));
        Assert.Equal(TrigramIndexStatus.Off, (await HealthAsync(data, off, ct)).Status);
        Assert.True(await PassAsync(data, false, off, Now.AddHours(3), ct));
    }

    /// <summary>Point by point: the option off drops this index and the one on the text alone it replaces, each on its own, and a store
    /// holding only the replaced one (an earlier build's) is DropPending until the pass drops it.</summary>
    [Fact]
    public async Task TurningTheOptionOff_DropsTheReplacedIndexToo()
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
        await PassAsync(data, true, tracker, Now, ct);
        Assert.True(await ValidAsync(admin, ct));

        /* Both names at once: an earlier build ran with the option on after this one built its index. */
        await CreateReplacedIndexAsync(admin, ct);
        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        var both = await HealthAsync(data, off, ct);
        Assert.Equal(TrigramIndexStatus.DropPending, both.Status);
        Assert.True(both.ReplacedIndexExists);

        Assert.False(await PassAsync(data, false, off, Now.AddHours(1), ct));

        Assert.False(await ExistsAsync(admin, ct));
        Assert.False(await ExistsAsync(admin, QueryStoreTextTrigramIndex.ReplacedIndexName, ct));
        Assert.Equal(TrigramIndexStatus.Off, (await HealthAsync(data, off, ct)).Status);

        /* Only the replaced one: a store an earlier build indexed, with the option now off. */
        await CreateReplacedIndexAsync(admin, ct);
        var only = await HealthAsync(data, off, ct);
        Assert.Equal(TrigramIndexStatus.DropPending, only.Status);
        Assert.Null(only.Valid);
        Assert.True(only.ReplacedIndexExists);

        Assert.False(await PassAsync(data, false, off, Now.AddHours(2), ct));

        Assert.False(await ExistsAsync(admin, QueryStoreTextTrigramIndex.ReplacedIndexName, ct));
        Assert.Equal(TrigramIndexStatus.Off, (await HealthAsync(data, off, ct)).Status);
        Assert.True(await PassAsync(data, false, off, Now.AddHours(3), ct));
        Assert.Equal(0, off.BuildsStarted);
    }

    /// <summary>A store an earlier build indexed on the text alone: the replaced index serves until this one is valid, and the pass that
    /// builds this one drops it. One build, and the next pass changes nothing.</summary>
    [Fact]
    public async Task TheIndexOnTheTextAlone_IsDroppedOnceThisOneIsValid()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateReplacedIndexAsync(admin, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);

        var before = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Pending, before.Status);
        Assert.Null(before.Valid);
        Assert.True(before.ReplacedIndexExists);
        Assert.Contains(QueryStoreTextTrigramIndex.ReplacedIndexName, QueryStoreTextTrigramIndex.Note(before), StringComparison.Ordinal);

        await PassAsync(data, true, tracker, Now, ct);

        Assert.True(await ValidAsync(admin, ct));
        Assert.False(await ExistsAsync(admin, QueryStoreTextTrigramIndex.ReplacedIndexName, ct));
        Assert.Equal(1, tracker.BuildsStarted);
        var ready = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Ready, ready.Status);
        Assert.False(ready.ReplacedIndexExists);
        Assert.DoesNotContain(QueryStoreTextTrigramIndex.ReplacedIndexName, QueryStoreTextTrigramIndex.Note(ready), StringComparison.Ordinal);
        var oid = await OidAsync(admin, ct);

        await PassAsync(data, true, tracker, Now.AddHours(1), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.Equal(oid, await OidAsync(admin, ct));
    }

    [Fact]
    public async Task AnInvalidLeftover_IsDroppedAndRebuilt()
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
        await PassAsync(data, true, tracker, Now, ct);
        var oldOid = await OidAsync(admin, ct);

        /* An interrupted CREATE INDEX CONCURRENTLY leaves the index INVALID; the test roles are superuser, so the state is staged exactly. */
        await ExecAsync(admin, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{QueryStoreTextTrigramIndex.IndexName}'::regclass", ct);
        Assert.False(await ValidAsync(admin, ct));
        var leftover = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Pending, leftover.Status);
        Assert.True(leftover.Exists);
        Assert.False(leftover.Valid);

        await PassAsync(data, true, tracker, Now.AddHours(1), ct);

        Assert.True(await ValidAsync(admin, ct));
        Assert.NotEqual(oldOid, await OidAsync(admin, ct));
        Assert.Equal(2, tracker.BuildsStarted);
        Assert.Equal(TrigramIndexStatus.Ready, (await HealthAsync(data, tracker, ct)).Status);
    }

    [Fact]
    public async Task AFailedBuild_IsReported_LeavesNothingBehind_AndWaitsBeforeTheRetry()
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

        /* A lock the build cannot get makes it fail on its one-second command deadline, with nothing built. */
        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await ExecAsync(holder, "BEGIN; LOCK TABLE collect.query_store_text IN ACCESS EXCLUSIVE MODE", ct);

        await QueryStoreTextTrigramIndex.RunPassAsync(data, true, tracker, Now, NullLogger.Instance, 1, ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.False(await ExistsAsync(admin, ct));
        var failed = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Failed, failed.Status);
        Assert.Equal(1, failed.FailedAttempts);
        Assert.False(string.IsNullOrEmpty(failed.LastError));
        Assert.Equal(Now + QueryTextTrigramIndex.Backoff(1), failed.NextAttemptUtc);
        Assert.Contains("next_attempt_at", QueryStoreTextTrigramIndex.Note(failed), StringComparison.Ordinal);

        await ExecAsync(holder, "ROLLBACK", ct);

        /* Inside the wait nothing is tried; past it the build runs and the failure is forgotten. */
        await PassAsync(data, true, tracker, Now.AddMinutes(1), ct);
        Assert.Equal(1, tracker.BuildsStarted);
        Assert.False(await ExistsAsync(admin, ct));

        await PassAsync(data, true, tracker, Now + QueryTextTrigramIndex.Backoff(1) + TimeSpan.FromMinutes(1), ct);
        Assert.Equal(2, tracker.BuildsStarted);
        Assert.True(await ValidAsync(admin, ct));
        var recovered = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Ready, recovered.Status);
        Assert.Equal(0, recovered.FailedAttempts);
        Assert.Null(recovered.LastError);
    }

    [Fact]
    public async Task ALoginThatCannotCreateTheExtension_IsReportedWithTheStatement_AndNotAskedAgain()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        var role = "qstrgm_nocreate_" + Guid.NewGuid().ToString("N")[..8];
        const string password = "Nocreate-8570-pw";
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        try
        {
            await RunWithLimitedLoginAsync(scratch, role, password, ct);
        }
        finally
        {
            /* Roles belong to the cluster, not the scratch database: drop it once the database and its grants are gone. */
            await scratch.DisposeAsync();
            await using var cluster = new NpgsqlConnection(baseCs);
            await cluster.OpenAsync(CancellationToken.None);
            await ExecAsync(cluster, $"DROP ROLE IF EXISTS {role}", CancellationToken.None);
        }
    }

    private static async Task RunWithLimitedLoginAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await ExecAsync(admin, "DROP EXTENSION IF EXISTS pg_trgm", ct);
        await ExecAsync(admin, "DROP EXTENSION IF EXISTS btree_gin", ct);
        Assert.Equal(0L, await ScalarAsync(admin, "SELECT count(*) FROM pg_extension WHERE extname IN ('pg_trgm', 'btree_gin')", ct));
        await ExecAsync(admin, $"CREATE ROLE {role} LOGIN PASSWORD '{password}'", ct);
        await ExecAsync(admin, $"GRANT USAGE ON SCHEMA collect TO {role}", ct);

        var limited = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password };
        await using var limitedData = NpgsqlDataSource.Create(limited.ConnectionString);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);

        await PassAsync(limitedData, true, tracker, Now, ct);
        await PassAsync(limitedData, true, tracker, Now.AddHours(1), ct);

        Assert.Equal(0, tracker.BuildsStarted);
        Assert.False(await ExistsAsync(admin, ct));
        var waiting = await HealthAsync(limitedData, tracker, ct);
        Assert.Equal(TrigramIndexStatus.WaitingOnExtension, waiting.Status);
        Assert.Equal(QueryStoreTextTrigramIndex.OperatorStatement, waiting.OperatorStatement);
        Assert.True(waiting.ExtensionAvailable);
        Assert.False(waiting.ExtensionInstalled);
        Assert.False(string.IsNullOrEmpty(waiting.LastError));
        Assert.Contains("operator_statement", QueryStoreTextTrigramIndex.Note(waiting), StringComparison.Ordinal);

        /* An operator creates the extensions: the next pass builds, and the wait is over. */
        await ExecAsync(admin, QueryStoreTextTrigramIndex.OperatorStatement, ct);
        await PassAsync(data, true, tracker, Now.AddHours(2), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.True(await ValidAsync(admin, ct));
        var ready = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Ready, ready.Status);
        Assert.Null(ready.OperatorStatement);
    }

    /// <summary>pg_trgm is there (an earlier build used it) and the login cannot create btree_gin: the store reports it is waiting on the
    /// extension with the statement to run, keeps the index on the text alone serving, and is not asked again; once an operator creates
    /// btree_gin the next pass builds this index and drops the replaced one.</summary>
    [Fact]
    public async Task ALoginThatCannotCreateBtreeGin_KeepsTheIndexOnTheTextAlone()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        var role = "qstrgm_nokey_" + Guid.NewGuid().ToString("N")[..8];
        const string password = "Nokey-8570-pw";
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        try
        {
            await RunWithoutBtreeGinAsync(scratch, role, password, ct);
        }
        finally
        {
            await scratch.DisposeAsync();
            await using var cluster = new NpgsqlConnection(baseCs);
            await cluster.OpenAsync(CancellationToken.None);
            await ExecAsync(cluster, $"DROP ROLE IF EXISTS {role}", CancellationToken.None);
        }
    }

    private static async Task RunWithoutBtreeGinAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await CreateReplacedIndexAsync(admin, ct);
        await ExecAsync(admin, "DROP EXTENSION IF EXISTS btree_gin", ct);
        Assert.Equal(0L, await ScalarAsync(admin, "SELECT count(*) FROM pg_extension WHERE extname = 'btree_gin'", ct));
        await ExecAsync(admin, $"CREATE ROLE {role} LOGIN PASSWORD '{password}'", ct);
        await ExecAsync(admin, $"GRANT USAGE ON SCHEMA collect TO {role}", ct);

        var limited = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password };
        await using var limitedData = NpgsqlDataSource.Create(limited.ConnectionString);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);

        await PassAsync(limitedData, true, tracker, Now, ct);
        await PassAsync(limitedData, true, tracker, Now.AddHours(1), ct);

        Assert.Equal(0, tracker.BuildsStarted);
        Assert.False(await ExistsAsync(admin, ct));
        Assert.True(await ExistsAsync(admin, QueryStoreTextTrigramIndex.ReplacedIndexName, ct));
        var waiting = await HealthAsync(limitedData, tracker, ct);
        Assert.Equal(TrigramIndexStatus.WaitingOnExtension, waiting.Status);
        Assert.Equal(QueryStoreTextTrigramIndex.OperatorStatement, waiting.OperatorStatement);
        Assert.False(waiting.ExtensionInstalled);
        Assert.True(waiting.ExtensionAvailable);
        Assert.True(waiting.ReplacedIndexExists);
        Assert.False(string.IsNullOrEmpty(waiting.LastError));
        var note = QueryStoreTextTrigramIndex.Note(waiting);
        Assert.Contains("btree_gin", note, StringComparison.Ordinal);
        Assert.Contains(QueryStoreTextTrigramIndex.ReplacedIndexName, note, StringComparison.Ordinal);

        await ExecAsync(admin, "CREATE EXTENSION btree_gin", ct);
        await PassAsync(data, true, tracker, Now.AddHours(2), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.True(await ValidAsync(admin, ct));
        Assert.False(await ExistsAsync(admin, QueryStoreTextTrigramIndex.ReplacedIndexName, ct));
        Assert.Equal(TrigramIndexStatus.Ready, (await HealthAsync(data, tracker, ct)).Status);
    }

    /// <summary>The two trigram options are independent: each pass builds and drops only its own index, and each tracker counts only its
    /// own builds.</summary>
    [Fact]
    public async Task ThePlanCacheOption_NeitherBuildsNorDropsTheQueryStoreTextIndex()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var queryStoreText = new QueryTextTrigramIndexTracker();
        queryStoreText.Configure(true);
        var planCacheOff = new QueryTextTrigramIndexTracker();
        planCacheOff.Configure(false);

        await PassAsync(data, true, queryStoreText, Now, ct);
        Assert.True(await QueryTextTrigramIndex.RunPassAsync(data, false, planCacheOff, Now, NullLogger.Instance, ct));

        Assert.True(await ValidAsync(admin, ct));
        Assert.False(await ExistsAsync(admin, QueryTextTrigramIndex.IndexName, ct));
        Assert.Equal(0, planCacheOff.BuildsStarted);

        /* The plan-cache option on, this one off: the plan-cache index is built and stays, this one is dropped. */
        var planCacheOn = new QueryTextTrigramIndexTracker();
        planCacheOn.Configure(true);
        await QueryTextTrigramIndex.RunPassAsync(data, true, planCacheOn, Now.AddHours(1), NullLogger.Instance, ct);
        var queryStoreTextOff = new QueryTextTrigramIndexTracker();
        queryStoreTextOff.Configure(false);
        await PassAsync(data, false, queryStoreTextOff, Now.AddHours(1), ct);

        Assert.True(await ExistsAsync(admin, QueryTextTrigramIndex.IndexName, ct));
        Assert.False(await ExistsAsync(admin, ct));
        Assert.Equal(0, queryStoreTextOff.BuildsStarted);
        Assert.Equal(TrigramIndexStatus.Off, (await HealthAsync(data, queryStoreTextOff, ct)).Status);
    }
}
