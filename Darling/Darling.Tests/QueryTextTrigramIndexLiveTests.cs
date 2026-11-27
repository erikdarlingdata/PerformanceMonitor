/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The opt-in trigram index on the plan-cache text (#7674) on a real store: with the option off nothing is built, with it on exactly one
/// build happens and the next pass changes nothing, turning the option off drops the index, an INVALID leftover is dropped and rebuilt, a
/// failed build is reported and waits before its retry, a login that cannot create <c>pg_trgm</c> is reported with the statement an
/// operator runs and is not asked again, and the health read names each state.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it, so it cannot race live collection. */
public sealed class QueryTextTrigramIndexLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);

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

    /* A few thousand distinct texts, so the build has something to index and the size is not zero. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              SELECT sha256(i::text::bytea), 'SELECT * FROM dbo.Orders_' || i || ' WHERE OrderId = @p', timezone('UTC', now())
              FROM generate_series(1, 3000) AS i", ct);
        Assert.Equal(3000L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_text_dim", ct));
    }

    private static async Task<bool> ExistsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT to_regclass('{QueryTextTrigramIndex.IndexName}') IS NOT NULL", ct))!;

    private static async Task<bool> ValidAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryTextTrigramIndex.IndexName}'::regclass", ct))!;

    private static async Task<uint> OidAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToUInt32(await ScalarAsync(connection, $"SELECT '{QueryTextTrigramIndex.IndexName}'::regclass::oid", ct), CultureInfo.InvariantCulture);

    private static Task<bool> PassAsync(NpgsqlDataSource data, bool enabled, QueryTextTrigramIndexTracker tracker, DateTime now, CancellationToken ct) =>
        QueryTextTrigramIndex.RunPassAsync(data, enabled, tracker, now, NullLogger.Instance, ct);

    private static async Task<TrigramIndexHealth> HealthAsync(NpgsqlDataSource data, QueryTextTrigramIndexTracker tracker, CancellationToken ct)
    {
        await using var connection = await data.OpenConnectionAsync(ct);
        return await QueryTextTrigramIndex.ReadHealthAsync(connection, tracker, ct);
    }

    [Fact]
    public async Task OptionOff_BuildsNoIndex_AndTheHealthReadSaysOff()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 live test.");
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
    }

    [Fact]
    public async Task OptionOn_BuildsOnce_AndTheNextPassChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 live test.");
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
        var oid = await OidAsync(admin, ct);
        var built = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Ready, built.Status);
        Assert.True(built.Valid);
        Assert.True(built.ExtensionInstalled);
        Assert.True(built.SizeBytes > 0);

        await PassAsync(data, true, tracker, Now.AddHours(1), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.Equal(oid, await OidAsync(admin, ct));
    }

    private static async Task<int> PendingListLimitAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToInt32(await ScalarAsync(connection,
            $"SELECT coalesce((SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'gin_pending_list_limit')::int, (SELECT setting::int FROM pg_settings WHERE name = 'gin_pending_list_limit')) FROM pg_class AS c WHERE c.oid = '{QueryTextTrigramIndex.IndexName}'::regclass", ct),
            CultureInfo.InvariantCulture);

    private static async Task<long> PendingTuplesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, $"SELECT pending_tuples FROM pgstatginindex('{QueryTextTrigramIndex.IndexName}')", ct), CultureInfo.InvariantCulture);

    /* Every collector batch adds its texts' trigrams to the index's pending list, and the insert that takes the list past the limit
       flushes it inside its own call. A new build carries the 64 MB limit, and an index an earlier build made without it gets the limit
       and an empty list from the next clean, so the insert rarely reaches the limit. */
    [Fact]
    public async Task ACleanSetsThePendingListLimitOnAnOlderIndex_AndEmptiesTheList_AndANewBuildCarriesTheLimit()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        await ExecAsync(admin, "CREATE EXTENSION IF NOT EXISTS pg_trgm", ct);
        await ExecAsync(admin, "CREATE EXTENSION IF NOT EXISTS pgstattuple", ct);
        var schema = (string)(await ScalarAsync(
            admin, "SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm'", ct))!;
        await ExecAsync(admin, $"CREATE INDEX ix_query_text_dim_trgm ON collect.query_text_dim USING gin (query_text \"{schema}\".gin_trgm_ops)", ct);
        await ExecAsync(admin,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              SELECT sha256(('new' || i)::bytea), 'SELECT * FROM dbo.Lines_' || i || ' WHERE LineId = @p', timezone('UTC', now())
              FROM generate_series(1, 200) AS i", ct);
        Assert.NotEqual(QueryTextTrigramIndex.PendingListLimitKb, await PendingListLimitAsync(admin, ct));
        Assert.True(await PendingTuplesAsync(admin, ct) > 0);

        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        await QueryTextTrigramIndex.CleanPendingListAsync(data, () => true, NullLogger.Instance, ct);

        Assert.Equal(QueryTextTrigramIndex.PendingListLimitKb, await PendingListLimitAsync(admin, ct));
        Assert.Equal(0L, await PendingTuplesAsync(admin, ct));

        await ExecAsync(admin, $"DROP INDEX {QueryTextTrigramIndex.IndexName}", ct);
        await PassAsync(data, true, tracker, Now.AddHours(1), ct);
        Assert.Equal(1, tracker.BuildsStarted);
        Assert.Equal(QueryTextTrigramIndex.PendingListLimitKb, await PendingListLimitAsync(admin, ct));
    }

    private static async Task<NpgsqlDataSource> OlderIndexWithPendingRowsAsync(ScratchPostgres scratch, NpgsqlConnection admin, CancellationToken ct)
    {
        await SeedAsync(admin, ct);
        await ExecAsync(admin, "CREATE EXTENSION IF NOT EXISTS pg_trgm", ct);
        await ExecAsync(admin, "CREATE EXTENSION IF NOT EXISTS pgstattuple", ct);
        var schema = (string)(await ScalarAsync(
            admin, "SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm'", ct))!;
        await ExecAsync(admin, $"CREATE INDEX ix_query_text_dim_trgm ON collect.query_text_dim USING gin (query_text \"{schema}\".gin_trgm_ops)", ct);
        await ExecAsync(admin,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              SELECT sha256(('new' || i)::bytea), 'SELECT * FROM dbo.Lines_' || i || ' WHERE LineId = @p', timezone('UTC', now())
              FROM generate_series(1, 200) AS i", ct);
        Assert.True(await PendingTuplesAsync(admin, ct) > 0);
        return NpgsqlDataSource.Create(scratch.ConnectionString);
    }

    /* #8570: the clean's result (pages cleaned) and timing reach the service log, so a clean can be tied to its index. With the option off
       nothing is logged. */
    [Fact]
    public async Task ACleanLogsTheIndexAndThePagesItCleaned_AndACleanWithTheOptionOffLogsNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await using var data = await OlderIndexWithPendingRowsAsync(scratch, admin, ct);

        var off = new StructuredSink();
        await QueryTextTrigramIndex.CleanPendingListAsync(data, () => false, off, ct);
        Assert.Empty(off.Entries);
        Assert.True(await PendingTuplesAsync(admin, ct) > 0);

        var on = new StructuredSink();
        await QueryTextTrigramIndex.CleanPendingListAsync(data, () => true, on, ct);
        var cleaned = on.Entries.Where(e => e.Fields.ContainsKey("Pages")).ToList();
        var line = Assert.Single(cleaned);
        Assert.Equal(QueryTextTrigramIndex.IndexName, (string)line.Fields["Index"]!);
        Assert.True(Convert.ToInt64(line.Fields["Pages"], CultureInfo.InvariantCulture) > 0, "the clean should report the pages it cleaned");
        Assert.Equal(LogLevel.Debug, line.Level);
        Assert.Equal(0L, await PendingTuplesAsync(admin, ct));
    }

    /// <summary>A logger that keeps each line's level and structured fields, so a test reads the values rather than the formatted text.</summary>
    private sealed class StructuredSink : ILogger
    {
        public List<(LogLevel Level, Dictionary<string, object?> Fields)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var fields = new Dictionary<string, object?>();
            if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                foreach (var pair in pairs)
                {
                    fields[pair.Key] = pair.Value;
                }
            }

            lock (Entries)
            {
                Entries.Add((logLevel, fields));
            }
        }
    }

    /* The limit change takes ACCESS EXCLUSIVE on the index, so with a long search holding the index it queues, and every insert queues
       behind it. The change runs under a 2 second lock timeout, a timeout is a quiet skip, and the clean still empties the list. With the
       lock timeout removed the clean waits for the search and the insert waits behind it, so neither finishes inside the window. The insert
       is released at the same lock timeout as the clean, so it can land in the pending list after the clean emptied it: the 200 seeded rows
       are gone and at most that one row is pending. */
    [Fact]
    public async Task ACleanWithASearchHoldingTheIndex_SkipsTheLimitChangeAtTheLockTimeout_AndNeverBlocksAnInsertPastIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await using var data = await OlderIndexWithPendingRowsAsync(scratch, admin, ct);

        await using var reader = new NpgsqlConnection(scratch.ConnectionString);
        await reader.OpenAsync(ct);
        await using var inserter = new NpgsqlConnection(scratch.ConnectionString);
        await inserter.OpenAsync(ct);
        await ExecAsync(reader, "BEGIN", ct);
        await ExecAsync(reader, "SET enable_seqscan = off", ct);
        await ExecAsync(reader, "SELECT count(*) FROM collect.query_text_dim WHERE query_text LIKE '%Lines_5%'", ct);

        var clean = QueryTextTrigramIndex.CleanPendingListAsync(data, () => true, NullLogger.Instance, ct);
        var queued = false;
        for (var i = 0; i < 50 && !queued; i++)
        {
            queued = (bool)(await ScalarAsync(admin,
                $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = '{QueryTextTrigramIndex.IndexName}'::regclass AND mode = 'AccessExclusiveLock' AND NOT granted)", ct))!;
            if (!queued)
            {
                await Task.Delay(100, ct);
            }
        }

        var insert = ExecAsync(inserter,
            "INSERT INTO collect.query_text_dim (digest, query_text, last_seen) VALUES (sha256('blocked'::bytea), 'SELECT 1', timezone('UTC', now()))", ct);
        var window = Task.Delay(TimeSpan.FromSeconds(8), ct);
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

        Assert.NotEqual(QueryTextTrigramIndex.PendingListLimitKb, await PendingListLimitAsync(admin, ct));
        Assert.InRange(await PendingTuplesAsync(admin, ct), 0L, 1L);

        await ExecAsync(reader, "COMMIT", ct);
        await QueryTextTrigramIndex.CleanPendingListAsync(data, () => true, NullLogger.Instance, ct);
        Assert.Equal(QueryTextTrigramIndex.PendingListLimitKb, await PendingListLimitAsync(admin, ct));
    }

    /* The service's background task makes its first clean at once when its first delay is zero, and with the option off cleans nothing. */
    [Fact]
    public async Task TheBackgroundTask_CleansAnOlderIndexWhenTheOptionIsOn_AndLeavesItWhenOff()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await using var data = await OlderIndexWithPendingRowsAsync(scratch, admin, ct);

        await QueryTextTrigramIndex.CleanPendingListAsync(data, () => false, NullLogger.Instance, ct);
        Assert.True(await PendingTuplesAsync(admin, ct) > 0);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var run = QueryTextTrigramIndex.RunAsync(
            data, () => true, new QueryTextTrigramIndexTracker(), NullLogger.Instance, TimeSpan.Zero, QueryTextTrigramIndex.PassInterval, cts.Token);
        var emptied = false;
        for (var i = 0; i < 100 && !emptied; i++)
        {
            emptied = await PendingTuplesAsync(admin, ct) == 0L;
            if (!emptied)
            {
                await Task.Delay(200, ct);
            }
        }

        await cts.CancelAsync();
        await run;
        Assert.True(emptied, "the background task should have emptied the list");
        Assert.Equal(QueryTextTrigramIndex.PendingListLimitKb, await PendingListLimitAsync(admin, ct));
    }

    [Fact]
    public async Task TurningTheOptionOff_DropsTheIndex()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 live test.");
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

    [Fact]
    public async Task AnInvalidLeftover_IsDroppedAndRebuilt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 live test.");
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
        await ExecAsync(admin, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{QueryTextTrigramIndex.IndexName}'::regclass", ct);
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
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 live test.");
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
        await ExecAsync(holder, "BEGIN; LOCK TABLE collect.query_text_dim IN ACCESS EXCLUSIVE MODE", ct);

        await QueryTextTrigramIndex.RunPassAsync(data, true, tracker, Now, NullLogger.Instance, 1, ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.False(await ExistsAsync(admin, ct));
        var failed = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Failed, failed.Status);
        Assert.Equal(1, failed.FailedAttempts);
        Assert.False(string.IsNullOrEmpty(failed.LastError));
        Assert.Equal(Now + QueryTextTrigramIndex.Backoff(1), failed.NextAttemptUtc);

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
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 live test.");
        var ct = TestContext.Current.CancellationToken;

        var role = "trgm_nocreate_" + Guid.NewGuid().ToString("N")[..8];
        const string password = "Nocreate-7674-pw";
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
        Assert.Equal(0L, await ScalarAsync(admin, "SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm'", ct));
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
        Assert.Equal(QueryTextTrigramIndex.OperatorStatement, waiting.OperatorStatement);
        Assert.True(waiting.ExtensionAvailable);
        Assert.False(waiting.ExtensionInstalled);
        Assert.False(string.IsNullOrEmpty(waiting.LastError));

        /* An operator creates the extension: the next pass builds, and the wait is over. */
        await ExecAsync(admin, QueryTextTrigramIndex.OperatorStatement, ct);
        await PassAsync(data, true, tracker, Now.AddHours(2), ct);

        Assert.Equal(1, tracker.BuildsStarted);
        Assert.True(await ValidAsync(admin, ct));
        var ready = await HealthAsync(data, tracker, ct);
        Assert.Equal(TrigramIndexStatus.Ready, ready.Status);
        Assert.Null(ready.OperatorStatement);
    }

    [Fact]
    public void Classify_NamesBuildingAndDropping_AndTheBackoffDoublesToItsCap()
    {
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        tracker.BeginBuild(Now);
        var none = new TrigramCatalogState(true, null, null, null, true, false);
        Assert.Equal(TrigramIndexStatus.Building, QueryTextTrigramIndex.Classify(none, tracker.Read()));
        tracker.EndPhase();
        Assert.Equal(TrigramIndexStatus.Building, QueryTextTrigramIndex.Classify(none with { BuildRunning = true }, tracker.Read()));
        Assert.Equal(TrigramIndexStatus.Pending, QueryTextTrigramIndex.Classify(none, tracker.Read()));

        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        off.BeginDrop();
        var present = new TrigramCatalogState(true, true, 8192, "public", true, false);
        Assert.Equal(TrigramIndexStatus.Dropping, QueryTextTrigramIndex.Classify(present, off.Read()));

        Assert.Equal(TimeSpan.FromMinutes(30), QueryTextTrigramIndex.Backoff(1));
        Assert.Equal(TimeSpan.FromHours(1), QueryTextTrigramIndex.Backoff(2));
        Assert.Equal(TimeSpan.FromHours(12), QueryTextTrigramIndex.Backoff(40));
    }
}
