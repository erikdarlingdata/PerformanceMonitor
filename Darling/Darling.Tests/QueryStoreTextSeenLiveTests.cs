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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #9043 (V281): the Query Store text liveness stamp lives in <c>collect.query_store_text_seen</c>, one narrow row per
/// text, so the fetch probe's touch does not rewrite a wide text row under the trigram index. These run the shipped
/// statements against a migrated store and read what each one wrote:
/// <list type="bullet">
/// <item>the touch stamps the seen row and writes nothing to <c>collect.query_store_text</c> while the text row's own stamp
/// is inside <see cref="QueryStoreTextSeen.TextRowRefreshHours"/> and its hash is set, restamps the text row past that
/// interval, and adopts a missing hash at once;</item>
/// <item>the touch gives a text with no seen row one, and none to a statement the store holds no text for;</item>
/// <item>the prune keeps a text either stamp says is live, retires the rest, and judges a text with no seen row by the
/// text row's own stamp;</item>
/// <item>the retention pass's adopt step counts as no purged table and no deleted row, a failure of it is one failed
/// table and the prune still runs, and a pass whose time budget is spent starts no adopt and records the stop at the prune;</item>
/// <item>a touch that commits while the prune waits for its text keeps the text and its seen row;</item>
/// <item>a deleted text takes its seen row with it, whatever deleted it;</item>
/// <item>the rung fills the table from the text rows it finds, arms the trigger, and can run again;</item>
/// <item><c>find_queries_by_text</c> returns the same rows, with the liveness stamp as <c>last_seen</c>.</item>
/// </list>
/// What a statement wrote is read from <c>pg_stat_xact_user_tables</c> inside the touch's own transaction and from the
/// row's <c>ctid</c> and <c>xmin</c>, never from a timing.
/// </summary>
public sealed class QueryStoreTextSeenLiveTests
{
    private const string Skip =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store text liveness tests (each mints its own scratch database).";

    private const int RungVersion = 281;
    private const int ServerId = 7;
    private const string Db = "db";
    private const int TimeoutSeconds = 30;

    /* Past every horizon a shared collector's retention can set, and well inside all of them: the same two ages the other
       live retention tests use for "certainly expired" and "certainly kept". */
    private const int StaleDays = 400;
    private const int FreshHours = 1;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ATouch_StampsTheSeenRow_AndWritesNothingToTheTextRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        /* Past the guard, so the seen row is due, and inside the refresh interval, so the text row is not. Both ages are
           derived from the constants, so a moved width moves the fixture with it. */
        var now = WholeSecondUtcNow();
        var landedAt = now.AddHours(-(QueryStoreLivenessTouchGuard.GuardHours + 1));
        Assert.True(QueryStoreLivenessTouchGuard.GuardHours + 1 < QueryStoreTextSeen.TextRowRefreshHours);

        await QueryStoreTextWriter.WriteAsync(
            connection, ServerId, Db, new[] { new FetchedQueryText(1, "SELECT 1", "0xA1") }, landedAt, TimeoutSeconds, cancellationToken: ct);

        /* The upsert's trigger wrote the seen row, at the text row's stamp. */
        Assert.Equal(QueryStorePlanMap.Naive(landedAt), await SeenAsync(connection, ServerId, 1, ct));
        var textBefore = await TextVersionAsync(connection, 1, ct);

        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            var textWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text", ct);
            var seenWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct);
            var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db, new[] { (1L, (string?)"0xA1") }, now, TimeoutSeconds, ct);

            Assert.Equal(new FetchProbeVerdict(1, Resolved: true, HashStale: false), verdicts.Single());
            Assert.Equal(new Writes(0, 0, 0), (await WritesInThisTransactionAsync(connection, "query_store_text", ct)).Since(textWritesBefore));
            Assert.Equal(new Writes(0, 1, 0), (await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct)).Since(seenWritesBefore));
            await transaction.CommitAsync(ct);
        }

        Assert.Equal(textBefore, await TextVersionAsync(connection, 1, ct));
        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(connection, ServerId, 1, ct));

        /* Five minutes later the seen row is inside the guard: the touch writes nothing anywhere and still answers. */
        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            var textWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text", ct);
            var seenWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct);
            var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db, new[] { (1L, (string?)"0xA1") }, now.AddMinutes(5), TimeoutSeconds, ct);

            Assert.Equal(new FetchProbeVerdict(1, Resolved: true, HashStale: false), verdicts.Single());
            Assert.Equal(new Writes(0, 0, 0), (await WritesInThisTransactionAsync(connection, "query_store_text", ct)).Since(textWritesBefore));
            Assert.Equal(new Writes(0, 0, 0), (await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct)).Since(seenWritesBefore));
            await transaction.CommitAsync(ct);
        }

        Assert.Equal(textBefore, await TextVersionAsync(connection, 1, ct));
        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(connection, ServerId, 1, ct));
    }

    [Fact]
    public async Task ATouch_RestampsTheTextRowPastTheRefreshInterval_AndAdoptsAMissingHashAtOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var now = WholeSecondUtcNow();
        var pastTheInterval = now.AddHours(-(QueryStoreTextSeen.TextRowRefreshHours + 1));
        var insideTheInterval = now.AddHours(-(QueryStoreTextSeen.TextRowRefreshHours - 1));
        var anHourAgo = now.AddHours(-1);
        Assert.True(QueryStoreTextSeen.TextRowRefreshHours - 1 > QueryStoreLivenessTouchGuard.GuardHours);
        Assert.True(QueryStoreLivenessTouchGuard.GuardHours > 1);

        /* 1: hash set, the text row's stamp past the interval. 2: hash set, inside it. 3: no hash (a row from before the
           hash was stored), landed an hour ago, so nothing about its age asks for a write. */
        await QueryStoreTextWriter.WriteAsync(
            connection, ServerId, Db, new[] { new FetchedQueryText(1, "SELECT 1", "0xA1") }, pastTheInterval, TimeoutSeconds, cancellationToken: ct);
        await QueryStoreTextWriter.WriteAsync(
            connection, ServerId, Db, new[] { new FetchedQueryText(2, "SELECT 2", "0xA2") }, insideTheInterval, TimeoutSeconds, cancellationToken: ct);
        await QueryStoreTextWriter.WriteAsync(
            connection, ServerId, Db, new[] { new FetchedQueryText(3, "SELECT 3", QueryHash: null) }, anHourAgo, TimeoutSeconds, cancellationToken: ct);

        var secondBefore = await TextVersionAsync(connection, 2, ct);

        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            var textWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text", ct);
            var seenWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct);
            var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db,
                new[] { (1L, (string?)"0xA1"), (2L, (string?)"0xA2"), (3L, (string?)"0xA3") }, now, TimeoutSeconds, ct);

            Assert.Equal(
                new[]
                {
                    new FetchProbeVerdict(1, Resolved: true, HashStale: false),
                    new FetchProbeVerdict(2, Resolved: true, HashStale: false),
                    new FetchProbeVerdict(3, Resolved: true, HashStale: false),
                },
                verdicts);

            /* Text rows 1 (restamped) and 3 (hash adopted). Seen rows 1 and 2 (past the guard); 3's is an hour old. */
            Assert.Equal(new Writes(0, 2, 0), (await WritesInThisTransactionAsync(connection, "query_store_text", ct)).Since(textWritesBefore));
            Assert.Equal(new Writes(0, 2, 0), (await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct)).Since(seenWritesBefore));
            await transaction.CommitAsync(ct);
        }

        var first = await TextVersionAsync(connection, 1, ct);
        Assert.Equal(QueryStorePlanMap.Naive(now), first.LastSeen);
        Assert.Equal("0xA1", first.QueryHash);

        Assert.Equal(secondBefore, await TextVersionAsync(connection, 2, ct));

        var third = await TextVersionAsync(connection, 3, ct);
        Assert.Equal("0xA3", third.QueryHash);

        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(connection, ServerId, 1, ct));
        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(connection, ServerId, 2, ct));
        Assert.Equal(QueryStorePlanMap.Naive(anHourAgo), await SeenAsync(connection, ServerId, 3, ct));
    }

    [Fact]
    public async Task ATouch_GivesATextWithNoSeenRowOne_AndNoneToAStatementTheStoreHasNoTextFor()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var now = WholeSecondUtcNow();
        await PlantAsync(connection, ServerId, 1, "0xA1", QueryStorePlanMap.Naive(now.AddHours(-1)), seenStamp: null, ct);
        Assert.Null(await SeenAsync(connection, ServerId, 1, ct));
        var textBefore = await TextVersionAsync(connection, 1, ct);

        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            var textWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text", ct);
            var seenWritesBefore = await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct);
            var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db, new[] { (1L, (string?)"0xA1"), (2L, (string?)"0xA2") }, now, TimeoutSeconds, ct);

            Assert.Equal(
                new[]
                {
                    new FetchProbeVerdict(1, Resolved: true, HashStale: false),
                    new FetchProbeVerdict(2, Resolved: false, HashStale: false),
                },
                verdicts);
            Assert.Equal(new Writes(0, 0, 0), (await WritesInThisTransactionAsync(connection, "query_store_text", ct)).Since(textWritesBefore));
            Assert.Equal(new Writes(1, 0, 0), (await WritesInThisTransactionAsync(connection, "query_store_text_seen", ct)).Since(seenWritesBefore));
            await transaction.CommitAsync(ct);
        }

        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(connection, ServerId, 1, ct));
        Assert.Null(await SeenAsync(connection, ServerId, 2, ct));
        Assert.Equal(textBefore, await TextVersionAsync(connection, 1, ct));
    }

    [Fact]
    public async Task TheUpsert_WritesOneSeenRowWithANewText_AndARefetchAddsNone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var now = WholeSecondUtcNow();
        var earlier = now.AddHours(-2);

        await QueryStoreTextWriter.WriteAsync(
            connection, ServerId, Db,
            new[] { new FetchedQueryText(1, "SELECT 1", "0xA1"), new FetchedQueryText(2, "SELECT 2", "0xA2") },
            earlier, TimeoutSeconds, cancellationToken: ct);

        Assert.Equal(QueryStorePlanMap.Naive(earlier), await SeenAsync(connection, ServerId, 1, ct));
        Assert.Equal(QueryStorePlanMap.Naive(earlier), await SeenAsync(connection, ServerId, 2, ct));

        /* Query Store renumbered: id 1 names another statement, so its text is fetched again and overwritten. Id 3 is new. */
        await QueryStoreTextWriter.WriteAsync(
            connection, ServerId, Db,
            new[] { new FetchedQueryText(1, "SELECT 'renumbered'", "0xB1"), new FetchedQueryText(3, "SELECT 3", "0xA3") },
            now, TimeoutSeconds, cancellationToken: ct);

        Assert.Equal(QueryStorePlanMap.Naive(now), (await TextVersionAsync(connection, 1, ct)).LastSeen);
        Assert.Equal(new long[] { 1, 2, 3 }, await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct));
        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(connection, ServerId, 3, ct));
    }

    [Fact]
    public async Task ThePrune_KeepsATextEitherStampSaysIsLive_AndRetiresTheRestWithTheirSeenRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var utcNow = QueryStorePlanMap.Naive(WholeSecondUtcNow());
        var stale = utcNow.AddDays(-StaleDays);
        var fresh = utcNow.AddHours(-FreshHours);

        /* 1: the touch kept the seen row fresh and the text row's own stamp is old. Kept.
           2: both stamps old. Retired.
           3: the seen row is old and the text row fresh, which is what an older build's touch leaves. Kept.
           4: no seen row, text row old. Retired, on the text row's stamp.
           5: no seen row, text row fresh. Kept, on the text row's stamp. */
        await PlantAsync(connection, ServerId, 1, "0xA1", stale, fresh, ct);
        await PlantAsync(connection, ServerId, 2, "0xA2", stale, stale, ct);
        await PlantAsync(connection, ServerId, 3, "0xA3", fresh, stale, ct);
        await PlantAsync(connection, ServerId, 4, "0xA4", stale, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 5, "0xA5", fresh, seenStamp: null, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var log = new CapturingTestLogger();
        await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

        var texts = await IdsAsync(connection, "collect.query_store_text", ServerId, ct);
        Assert.True(texts.SequenceEqual(new long[] { 1, 3, 5 }), $"text rows left: {string.Join(", ", texts)}\n{log.Joined}");

        /* No seen row outlives its text, and the kept text that had none has one now, at the text row's stamp. */
        var seen = await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct);
        Assert.True(seen.SequenceEqual(new long[] { 1, 3, 5 }), $"seen rows left: {string.Join(", ", seen)}\n{log.Joined}");
        Assert.Equal(fresh, await SeenAsync(connection, ServerId, 5, ct));
        Assert.Equal(fresh, await SeenAsync(connection, ServerId, 1, ct));
        Assert.Equal(stale, await SeenAsync(connection, ServerId, 3, ct));
    }

    /// <summary>
    /// The adopt step writes seen rows and deletes nothing, so the pass counts it as no purged table and its rows as no
    /// deleted rows. The count of purged tables is compared with the same pass held back by the once-a-day gate, which
    /// skips the adopt step, the text prune and the self-statement key prune and runs every other table: the pass that
    /// runs the three purges exactly two more tables.
    /// </summary>
    [Fact]
    public async Task TheAdoptStep_GivesATextWithNoSeenRowOne_AndCountsAsNoPurgedTableAndNoDeletedRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var utcNow = QueryStorePlanMap.Naive(WholeSecondUtcNow());
        var stale = utcNow.AddDays(-StaleDays);
        var fresh = utcNow.AddHours(-FreshHours);

        /* 1, 2, 3: no seen row, text row fresh. Adopted and kept.
           4: no seen row, text row old. Adopted, then retired on the stamp it was adopted with.
           5, 6: both stamps old. Retired.
           7: both stamps fresh. Kept.
           Four rows adopted, three texts retired. */
        await PlantAsync(connection, ServerId, 1, "0xA1", fresh, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 2, "0xA2", fresh, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 3, "0xA3", fresh, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 4, "0xA4", stale, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 5, "0xA5", stale, stale, ct);
        await PlantAsync(connection, ServerId, 6, "0xA6", stale, stale, ct);
        await PlantAsync(connection, ServerId, 7, "0xA7", fresh, fresh, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var log = new CapturingTestLogger();

        var heldBack = await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, log, ct, queryTextPruneGate: AGateThatHoldsTheTextPruneBack());

        /* Nothing else in the store has a row to delete, and the held-back pass leaves the texts as they were planted. */
        Assert.True(0 == heldBack.RowsDeleted, log.Joined);
        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6, 7 }, await IdsAsync(connection, "collect.query_store_text", ServerId, ct));
        Assert.Equal(new long[] { 5, 6, 7 }, await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct));

        var pass = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

        var texts = await IdsAsync(connection, "collect.query_store_text", ServerId, ct);
        Assert.True(texts.SequenceEqual(new long[] { 1, 2, 3, 7 }), $"text rows left: {string.Join(", ", texts)}\n{log.Joined}");
        var seen = await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct);
        Assert.True(seen.SequenceEqual(new long[] { 1, 2, 3, 7 }), $"seen rows left: {string.Join(", ", seen)}\n{log.Joined}");
        Assert.Equal(fresh, await SeenAsync(connection, ServerId, 1, ct));
        Assert.Equal(fresh, await SeenAsync(connection, ServerId, 2, ct));
        Assert.Equal(fresh, await SeenAsync(connection, ServerId, 3, ct));

        /* The three texts the prune retired, and none of the four rows the adopt step wrote. */
        Assert.True(3 == pass.RowsDeleted, log.Joined);

        /* The text table and the self-statement keys, and not the adopt step. */
        Assert.True(heldBack.TablesPurged + 2 == pass.TablesPurged, $"held back {heldBack.TablesPurged}, pass {pass.TablesPurged}\n{log.Joined}");
        Assert.Equal("SUCCESS", (await LastRunRecordAsync(connection, ct)).Status);
    }

    /// <summary>
    /// An adopt step whose statement fails is one failed table in the pass's run record, with a warning that names the seen
    /// table, and the text prune runs all the same. The failure is planted: a trigger on the seen table that refuses every
    /// insert. The adopt step's <c>INSERT</c> is the only insert into that table a pass makes, and the prune's cascade is a
    /// delete, so the same pass held back by the once-a-day gate fails nothing. A text with no seen row is kept until a pass
    /// adopts it, which the pass after the trigger is dropped does.
    /// </summary>
    [Fact]
    public async Task AnAdoptStepThatFails_CountsAsOneFailedTable_AndTheTextPruneStillRuns()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var utcNow = QueryStorePlanMap.Naive(WholeSecondUtcNow());
        var stale = utcNow.AddDays(-StaleDays);
        var fresh = utcNow.AddHours(-FreshHours);

        /* 1: no seen row, text row fresh. 2: no seen row, text row old. 3: both stamps old. 4: both stamps fresh. */
        await PlantAsync(connection, ServerId, 1, "0xA1", fresh, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 2, "0xA2", stale, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 3, "0xA3", stale, stale, ct);
        await PlantAsync(connection, ServerId, 4, "0xA4", fresh, fresh, ct);

        await ExecuteAsync(
            connection,
            "CREATE FUNCTION public.refuse_seen_insert() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN RAISE EXCEPTION 'planted seen row insert failure'; END $f$",
            ct);
        await ExecuteAsync(
            connection,
            "CREATE TRIGGER refuse_seen_insert BEFORE INSERT ON collect.query_store_text_seen FOR EACH ROW EXECUTE FUNCTION public.refuse_seen_insert()",
            ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var heldBackLog = new CapturingTestLogger();

        var heldBack = await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, heldBackLog, ct, queryTextPruneGate: AGateThatHoldsTheTextPruneBack());

        Assert.True("SUCCESS" == (await LastRunRecordAsync(connection, ct)).Status, heldBackLog.Joined);

        var log = new CapturingTestLogger();
        var pass = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

        /* The prune ran: text 3 and its seen row are gone. Nothing was adopted, so text 2 has no seen row for the prune to
           find it by and is kept with text 1. */
        var texts = await IdsAsync(connection, "collect.query_store_text", ServerId, ct);
        Assert.True(texts.SequenceEqual(new long[] { 1, 2, 4 }), $"text rows left: {string.Join(", ", texts)}\n{log.Joined}");
        var seen = await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct);
        Assert.True(seen.SequenceEqual(new long[] { 4 }), $"seen rows left: {string.Join(", ", seen)}\n{log.Joined}");
        Assert.True(1 == pass.RowsDeleted, log.Joined);
        Assert.True(heldBack.TablesPurged + 2 == pass.TablesPurged, $"held back {heldBack.TablesPurged}, pass {pass.TablesPurged}\n{log.Joined}");

        var failed = await LastRunRecordAsync(connection, ct);
        Assert.True("WARNING" == failed.Status, log.Joined);
        Assert.Contains(", 1 failed ", failed.Message, StringComparison.Ordinal);
        Assert.Contains(
            log.Lines,
            l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains(QueryStoreTextSeen.TableName, StringComparison.Ordinal));

        /* The pass after the failure is gone adopts both, and retires text 2 on the stamp it was adopted with. */
        await ExecuteAsync(connection, "DROP TRIGGER refuse_seen_insert ON collect.query_store_text_seen", ct);
        var laterLog = new CapturingTestLogger();
        var later = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, laterLog, ct);

        Assert.Equal(new long[] { 1, 4 }, await IdsAsync(connection, "collect.query_store_text", ServerId, ct));
        Assert.Equal(new long[] { 1, 4 }, await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct));
        Assert.Equal(fresh, await SeenAsync(connection, ServerId, 1, ct));
        Assert.True(1 == later.RowsDeleted, laterLog.Joined);
        Assert.True("SUCCESS" == (await LastRunRecordAsync(connection, ct)).Status, laterLog.Joined);
    }

    /// <summary>
    /// A pass whose time budget is spent when it reaches the adopt step starts no adopt, and that is neither a failure nor a
    /// table the budget kept from starting: the text prune that follows is the first table the pass records as not started.
    /// The clock follows the work, as <see cref="RetentionCollectionYieldLiveTests"/> does it: it reads zero while the plan map
    /// holds a row and past the budget once the map is empty, and the plan map's purge is the statement before the adopt
    /// step. The count of tables not reached is compared with the same pass held back by the once-a-day gate, which skips
    /// the adopt step and the two purges after it: the pass that reaches them leaves exactly two more tables.
    /// </summary>
    [Fact]
    public async Task ABudgetSpentAtTheAdoptStep_StartsNoAdoptAndFailsNothing_AndTheTextPruneRecordsTheStop()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var utcNow = QueryStorePlanMap.Naive(WholeSecondUtcNow());
        var stale = utcNow.AddDays(-StaleDays);
        var fresh = utcNow.AddHours(-FreshHours);
        const int mapRows = 10;

        /* 1: no seen row, which an adopt step that ran would write. 2: both stamps old, which a prune that ran would retire. */
        await PlantAsync(connection, ServerId, 1, "0xA1", fresh, seenStamp: null, ct);
        await PlantAsync(connection, ServerId, 2, "0xA2", stale, stale, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        double Clock()
        {
            using var count = new NpgsqlCommand("SELECT count(*) FROM collect.query_store_plan_map", connection);
            return (long)count.ExecuteScalar()! == 0 ? 100 : 0;
        }

        async Task<(PurgeSummary Summary, RetentionCollectionYield Yield, CapturingTestLogger Log)> StopAfterThePlanMapAsync(QueryTextPruneGate? gate)
        {
            await ExecuteAsync(
                connection,
                "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) "
                + "SELECT 1, 'db', gs, ('\\x' || lpad(to_hex(gs), 8, '0'))::bytea, 'h' || gs, $1 FROM generate_series(1, " + mapRows + ") gs",
                ct, stale);

            var pacer = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask);
            var yield = new RetentionCollectionYield(
                new NeverBehind(), TimeSpan.FromSeconds(50), logger: null, delay: (_, _) => Task.CompletedTask, secondsClock: Clock);
            pacer.Yield = yield;
            var log = new CapturingTestLogger();

            var summary = await DarlingRetention.PurgeWithPacerAsync(
                postgres, timescaleAvailable: false, log, ct, walPacer: pacer, queryTextPruneGate: gate);
            return (summary, yield, log);
        }

        var heldBack = await StopAfterThePlanMapAsync(AGateThatHoldsTheTextPruneBack());
        Assert.True(heldBack.Yield.StoppedOnBudget, heldBack.Log.Joined);
        Assert.True(mapRows == heldBack.Summary.RowsDeleted, heldBack.Log.Joined);

        var stopped = await StopAfterThePlanMapAsync(gate: null);

        /* The plan map was drained and nothing after it ran: no row adopted, no text retired. */
        Assert.True(mapRows == stopped.Summary.RowsDeleted, stopped.Log.Joined);
        Assert.Null(await SeenAsync(connection, ServerId, 1, ct));
        Assert.Equal(new long[] { 1, 2 }, await IdsAsync(connection, "collect.query_store_text", ServerId, ct));
        Assert.Equal(new long[] { 2 }, await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct));

        /* The stop is the text prune's, at its entry. The text table and the self-statement keys are the two more tables
           not reached, and the adopt step is not a third. */
        Assert.True("before " + QueryStoreTextStore.TableName == stopped.Yield.StoppedPlace, stopped.Log.Joined);
        Assert.True(
            heldBack.Yield.TablesNotReached + 2 == stopped.Yield.TablesNotReached,
            $"held back {heldBack.Yield.TablesNotReached}, pass {stopped.Yield.TablesNotReached}\n{stopped.Log.Joined}");

        var record = await LastRunRecordAsync(connection, ct);
        Assert.True("SUCCESS" == record.Status, stopped.Log.Joined);
        Assert.Contains("time budget before " + QueryStoreTextStore.TableName + ",", record.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A touch revives a text while the prune waits to delete it. Both stamps of text 1 are behind the cutoff, so the prune's
    /// scan picks it. The touch, in a transaction that is still open, restamps the text row (a text behind the smallest
    /// cutoff is also behind <see cref="QueryStoreTextSeen.TextRowRefreshHours"/>) and its seen row, and the prune's
    /// <c>DELETE</c> then waits for the text row. When the touch commits, the delete re-checks the row's new version, which
    /// has another <c>ctid</c> and a stamp inside the cutoff, so the text and its seen row are kept. Text 2 has the same
    /// stamps and nothing touches it: the same statement retires it.
    ///
    /// <para>What this can fail on is the touch not rewriting the text row: with the refresh interval outside the cutoff
    /// the prune retires text 1. It cannot tell the two conditions of the re-check apart on a server that tests a
    /// <c>ctid</c> condition again (<see cref="QueryStoreTextSeen.PruneSql"/>), where either one alone keeps the row.</para>
    ///
    /// <para>The order is set by the test, never by a timing: the prune starts after the touch has run, and the touch
    /// commits only once <c>pg_stat_activity</c> shows the prune's backend waiting on a lock the touch's backend holds.</para>
    /// </summary>
    [Fact]
    public async Task ATouchThatCommitsWhileThePruneWaitsForItsText_KeepsTheTextAndItsSeenRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var toucher = await OpenAsync(scratch, ct);
        await using var pruner = new NpgsqlConnection(scratch.ConnectionString);
        await pruner.OpenAsync(ct);
        await using var watcher = new NpgsqlConnection(scratch.ConnectionString);
        await watcher.OpenAsync(ct);

        /* The smallest cutoff a prune can be given (the one-day retention floor plus the prune margin), and stamps one hour
           behind it: the youngest text a prune may retire, which is the one nearest the text row's refresh interval. */
        var now = WholeSecondUtcNow();
        var cutoff = QueryStorePlanMap.Naive(now).AddHours(-QueryStoreTextSeen.OlderBuildCutoffHours);
        var behindTheCutoff = cutoff.AddHours(-1);
        await PlantAsync(toucher, ServerId, 1, "0xA1", behindTheCutoff, behindTheCutoff, ct);
        await PlantAsync(toucher, ServerId, 2, "0xA2", behindTheCutoff, behindTheCutoff, ct);

        /* A prune that still waits after the touch has committed fails with an error instead of holding the test host. */
        await ExecuteAsync(pruner, "SET lock_timeout = '120s'", ct);

        bool waited;
        int retired;
        await using (var transaction = await toucher.BeginTransactionAsync(ct))
        {
            var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                toucher, ServerId, Db, new[] { (1L, (string?)"0xA1") }, now, TimeoutSeconds, ct);
            Assert.Equal(new[] { new FetchProbeVerdict(1, Resolved: true, HashStale: false) }, verdicts);

            var prune = PruneAsync(pruner, cutoff, ct);
            waited = await WaitsOnALockHeldByAsync(watcher, pruner.ProcessID, toucher.ProcessID, prune, ct);
            await transaction.CommitAsync(ct);
            retired = await prune;
        }

        Assert.True(waited, "the prune finished, or ran for a minute, without waiting on a lock the touch's transaction held");

        /* Text 2 only. Text 1 and its seen row carry the touch's stamp. */
        Assert.Equal(1, retired);
        Assert.Equal(new long[] { 1 }, await IdsAsync(toucher, "collect.query_store_text", ServerId, ct));
        Assert.Equal(new long[] { 1 }, await IdsAsync(toucher, "collect.query_store_text_seen", ServerId, ct));
        Assert.Equal(QueryStorePlanMap.Naive(now), (await TextVersionAsync(toucher, 1, ct)).LastSeen);
        Assert.Equal(QueryStorePlanMap.Naive(now), await SeenAsync(toucher, ServerId, 1, ct));
    }

    [Fact]
    public async Task DeletingATextRow_RemovesItsSeenRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var stamp = QueryStorePlanMap.Naive(WholeSecondUtcNow().AddHours(-FreshHours));
        await PlantAsync(connection, ServerId, 1, "0xA1", stamp, stamp, ct);
        await PlantAsync(connection, ServerId, 2, "0xA2", stamp, stamp, ct);
        await PlantAsync(connection, ServerId + 1, 1, "0xB1", stamp, stamp, ct);

        /* A delete that knows nothing of the seen table, the shape every other deleter of text rows has. */
        await ExecuteAsync(connection, "DELETE FROM collect.query_store_text WHERE server_id = $1 AND query_id = 1", ct, ServerId);

        Assert.Equal(new long[] { 2 }, await IdsAsync(connection, "collect.query_store_text_seen", ServerId, ct));
        Assert.Equal(new long[] { 1 }, await IdsAsync(connection, "collect.query_store_text_seen", ServerId + 1, ct));

        await ExecuteAsync(connection, "DELETE FROM collect.query_store_text WHERE server_id = $1", ct, ServerId + 1);

        Assert.Empty(await IdsAsync(connection, "collect.query_store_text_seen", ServerId + 1, ct));
        Assert.Equal(0L, await OrphanSeenRowsAsync(connection, ct));
    }

    [Fact]
    public async Task TheRung_FillsTheSeenTableFromTheTextRows_ArmsTheTrigger_AndRunsAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        /* A store one rung behind, holding text rows of two servers with a different stamp on every row. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        await ExecuteAsync(connection, "DELETE FROM darling_schema_version WHERE version >= $1", ct, RungVersion);
        Assert.False(await ScalarAsync<bool>(connection, "SELECT to_regclass('collect.query_store_text_seen') IS NOT NULL", ct));

        var anchor = QueryStorePlanMap.Naive(WholeSecondUtcNow());
        await ExecuteAsync(
            connection,
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_hash, query_sql_text, last_seen) "
            + "SELECT s, 'db', gs, '0x' || gs, 'SELECT ' || gs, $1::timestamp - ((s * 1000 + gs) || ' minutes')::interval "
            + "FROM generate_series(1, 2) AS s CROSS JOIN generate_series(1, 250) AS gs",
            ct, anchor);

        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));
        Assert.Equal(500L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_store_text_seen", ct));
        Assert.Equal(500L, await MatchingStampsAsync(connection, ct));
        Assert.Equal(0L, await OrphanSeenRowsAsync(connection, ct));

        /* The shape the rung leaves: the key on the three key columns and nothing else indexed, the cascade to the text
           row, and room on the page for the touch's update. */
        Assert.Equal(
            "PRIMARY KEY (server_id, database_name, query_id)",
            await ScalarAsync<string>(
                connection,
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'collect.query_store_text_seen'::regclass AND contype = 'p'",
                ct));
        Assert.Equal(
            1L,
            await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_index WHERE indrelid = 'collect.query_store_text_seen'::regclass", ct));
        Assert.True(
            await ScalarAsync<bool>(
                connection,
                "SELECT confdeltype = 'c' AND confrelid = 'collect.query_store_text'::regclass FROM pg_constraint "
                + "WHERE conrelid = 'collect.query_store_text_seen'::regclass AND contype = 'f'",
                ct),
            "the seen table's foreign key must reference the text table ON DELETE CASCADE");
        Assert.True(
            await ScalarAsync<bool>(
                connection,
                "SELECT coalesce(reloptions @> ARRAY['fillfactor=90'], false) FROM pg_class WHERE oid = 'collect.query_store_text_seen'::regclass",
                ct));

        /* The trigger is armed: a text row inserted with no thought for the seen table gets its seen row. */
        await ExecuteAsync(
            connection,
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES (3, 'db', 1, 'SELECT 1', $1)",
            ct, anchor);
        Assert.Equal(anchor, await SeenAsync(connection, 3, 1, ct));

        /* The rung again, on a store that already has the table, with one text row that has no seen row: it fills that row
           and leaves the rest as they are. */
        await PlantAsync(connection, 3, 2, "0xC2", anchor.AddHours(-3), seenStamp: null, ct);
        await ExecuteAsync(connection, "UPDATE collect.query_store_text_seen SET last_seen = $1 WHERE server_id = 3 AND query_id = 1", ct, anchor.AddHours(1));
        await ExecuteAsync(connection, "DELETE FROM darling_schema_version WHERE version >= $1", ct, RungVersion);

        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(502L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_store_text_seen", ct));
        Assert.Equal(anchor.AddHours(-3), await SeenAsync(connection, 3, 2, ct));
        Assert.Equal(anchor.AddHours(1), await SeenAsync(connection, 3, 1, ct));
        Assert.Equal(
            1L,
            await ScalarAsync<long>(
                connection,
                "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'collect.query_store_text'::regclass AND tgname = 'trg_query_store_text_seen_ins' AND tgenabled = 'O'",
                ct));
    }

    [Fact]
    public async Task FindQueriesByText_ReturnsTheSameRows_WithTheLivenessStampAsLastSeen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var now = QueryStorePlanMap.Naive(WholeSecondUtcNow());

        /* db_a 1: the touch moved the seen row and left the text row. db_a 2: an older build stamped the text row and left
           the seen row. db_b 3: no seen row. db_a 4: another statement. The other server's row matches the pattern. */
        await PlantAsync(connection, ServerId, 1, "0x01", now.AddHours(-40), now.AddHours(-1), ct, "db_a", "SELECT a FROM dbo.Orders WHERE id = 1");
        await PlantAsync(connection, ServerId, 2, "0x02", now.AddHours(-2), now.AddHours(-30), ct, "db_a", "SELECT b FROM dbo.Orders WHERE id = 2");
        await PlantAsync(connection, ServerId, 3, null, now.AddHours(-3), seenStamp: null, ct, "db_b", "SELECT c FROM dbo.Orders WHERE id = 3");
        await PlantAsync(connection, ServerId, 4, "0x04", now.AddHours(-4), now.AddHours(-4), ct, "db_a", "SELECT d FROM dbo.Other");
        await PlantAsync(connection, ServerId + 1, 1, "0x11", now.AddHours(-5), now.AddHours(-5), ct, "db_a", "SELECT e FROM dbo.Orders");

        var rows = await FindAsync(connection, DarlingMcpFindQueriesByTextTools.QueryStoreSql, database: null, limit: 20, ct);

        Assert.Equal(
            new[]
            {
                new Found("db_a", 1, "0x01", now.AddHours(-1), "SELECT a FROM dbo.Orders WHERE id = 1"),
                new Found("db_a", 2, "0x02", now.AddHours(-2), "SELECT b FROM dbo.Orders WHERE id = 2"),
                new Found("db_b", 3, null, now.AddHours(-3), "SELECT c FROM dbo.Orders WHERE id = 3"),
            },
            rows);

        /* The keys, hashes, texts, order and cap are those of the same search on the text table alone. */
        foreach (var (database, limit) in new (string? Database, int Limit)[] { (null, 20), (null, 1), ("db_b", 20), ("db_none", 20) })
        {
            var onTheTextTableAlone = await FindAsync(connection, TextTableAloneSql, database, limit, ct);
            var shipped = await FindAsync(connection, DarlingMcpFindQueriesByTextTools.QueryStoreSql, database, limit, ct);

            Assert.Equal(
                onTheTextTableAlone.Select(r => (r.Database, r.QueryId, r.QueryHash, r.Text)),
                shipped.Select(r => (r.Database, r.QueryId, r.QueryHash, r.Text)));
        }

        Assert.Equal(2, (await FindAsync(connection, DarlingMcpFindQueriesByTextTools.QueryStoreSql, database: null, limit: 1, ct)).Count);
    }

    /// <summary>The search on the text table alone, with the text row's own stamp: the comparison for the rows the shipped
    /// statement returns, written out here so a change to the shipped statement cannot change both sides.</summary>
    private const string TextTableAloneSql = @"
SELECT t.database_name, t.query_id, t.query_hash, t.last_seen, t.query_sql_text
FROM query_store_text AS t
WHERE t.server_id = $1
AND   ($2::text IS NULL OR t.database_name = $2)
AND   t.query_sql_text ILIKE '%' || $3 || '%' ESCAPE '\'
ORDER BY t.database_name, t.query_id
LIMIT $4";

    private sealed record Found(string Database, long QueryId, string? QueryHash, DateTime LastSeen, string Text);

    private readonly record struct Writes(long Inserted, long Updated, long Deleted)
    {
        /// <summary>The writes made after <paramref name="earlier"/> was read in the same transaction. The view also counts
        /// writes of earlier transactions on this session that the statistics collector has not taken yet.</summary>
        public Writes Since(Writes earlier) => new(Inserted - earlier.Inserted, Updated - earlier.Updated, Deleted - earlier.Deleted);
    }

    private readonly record struct TextVersion(string Ctid, string Xmin, DateTime LastSeen, string? QueryHash);

    private sealed class NeverBehind : ICollectionPressure
    {
        public string? BehindReason() => null;
    }

    /// <summary>The once-a-day gate with a prune just finished and a clock that does not move: a pass handed it skips the
    /// adopt step, the text prune and the self-statement key prune, and runs every other table.</summary>
    private static QueryTextPruneGate AGateThatHoldsTheTextPruneBack()
    {
        var at = new DateTime(2026, 11, 18, 0, 0, 0, DateTimeKind.Utc);
        var gate = new QueryTextPruneGate(() => at);
        gate.MarkRan();
        return gate;
    }

    /// <summary>The status and the message of the newest run record a retention pass wrote.</summary>
    private static async Task<(string Status, string Message)> LastRunRecordAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT status, error_message FROM collect.collection_log WHERE collector_name = 'data_retention' ORDER BY collection_time DESC LIMIT 1",
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the retention pass wrote no run record");
        return (reader.GetString(0), reader.GetString(1));
    }

    private static DateTime WholeSecondUtcNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
    }

    private static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>
    /// One text row with the stamps the test names. A null <paramref name="seenStamp"/> leaves the text with no seen row:
    /// the row is inserted with the insert trigger off, which is what a text written by anything that bypasses the
    /// trigger looks like.
    /// </summary>
    private static async Task PlantAsync(
        NpgsqlConnection connection, int serverId, long queryId, string? queryHash, DateTime textStamp, DateTime? seenStamp, CancellationToken ct,
        string database = Db, string? text = null)
    {
        if (seenStamp is null)
        {
            await ExecuteAsync(connection, "ALTER TABLE collect.query_store_text DISABLE TRIGGER trg_query_store_text_seen_ins", ct);
        }

        try
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_hash, query_sql_text, last_seen) "
                + "VALUES ($1, $2, $3, $4, $5, $6)",
                connection);
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)queryHash ?? DBNull.Value });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = text ?? "SELECT " + queryId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = textStamp });
            await insert.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            if (seenStamp is null)
            {
                await ExecuteAsync(connection, "ALTER TABLE collect.query_store_text ENABLE TRIGGER trg_query_store_text_seen_ins", ct);
            }
        }

        if (seenStamp is DateTime stamp && stamp != textStamp)
        {
            await using var update = new NpgsqlCommand(
                "UPDATE collect.query_store_text_seen SET last_seen = $4 WHERE server_id = $1 AND database_name = $2 AND query_id = $3", connection);
            update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
            update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
            update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
            Assert.Equal(1, await update.ExecuteNonQueryAsync(ct));
        }
    }

    /// <summary>One statement of the text prune as the retention pass runs it: the shipped statement at the shipped cap,
    /// bound with the cutoff. Returns the text rows it deleted.</summary>
    private static async Task<int> PruneAsync(NpgsqlConnection connection, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            QueryStoreTextSeen.PruneSql(DarlingRetention.LivenessTouchedTablePruneRowCap), connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(cutoff);
        return await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Polls <c>pg_stat_activity</c> until the backend <paramref name="waiterPid"/> waits on a lock that the backend
    /// <paramref name="holderPid"/> holds. False when <paramref name="statement"/> finishes first or about a minute passes:
    /// either way the statement did not wait on that backend, and the caller fails on it.
    /// </summary>
    private static async Task<bool> WaitsOnALockHeldByAsync(
        NpgsqlConnection watcher, int waiterPid, int holderPid, Task statement, CancellationToken ct)
    {
        for (var i = 0; i < 600 && !statement.IsCompleted; i++)
        {
            await using var command = new NpgsqlCommand(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock' AND $2 = ANY (pg_blocking_pids(pid))",
                watcher);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = waiterPid });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = holderPid });
            if ((long)(await command.ExecuteScalarAsync(ct))! > 0)
            {
                return true;
            }

            await Task.Delay(100, ct);
        }

        return false;
    }

    private static async Task<TextVersion> TextVersionAsync(NpgsqlConnection connection, long queryId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT ctid::text, xmin::text, last_seen, query_hash FROM collect.query_store_text "
            + "WHERE server_id = $1 AND database_name = $2 AND query_id = $3",
            connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = Db });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"no text row for query_id {queryId}");
        return new TextVersion(reader.GetString(0), reader.GetString(1), reader.GetDateTime(2), reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    private static async Task<DateTime?> SeenAsync(NpgsqlConnection connection, int serverId, long queryId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT last_seen FROM collect.query_store_text_seen WHERE server_id = $1 AND query_id = $2", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
        return await command.ExecuteScalarAsync(ct) is DateTime stamp ? stamp : null;
    }

    /// <summary>What the current transaction has written to one table of <c>collect</c>, from the statistics the backend
    /// keeps for its own open transaction.</summary>
    private static async Task<Writes> WritesInThisTransactionAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_xact_user_tables WHERE schemaname = 'collect' AND relname = $1", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = table });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"pg_stat_xact_user_tables has no row for collect.{table}");
        return new Writes(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static async Task<long[]> IdsAsync(NpgsqlConnection connection, string table, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT query_id FROM {table} WHERE server_id = $1 ORDER BY database_name, query_id", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        var ids = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids.ToArray();
    }

    private static Task<long> OrphanSeenRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<long>(
            connection,
            "SELECT count(*) FROM collect.query_store_text_seen AS s WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_text AS t "
            + "WHERE t.server_id = s.server_id AND t.database_name = s.database_name AND t.query_id = s.query_id)",
            ct);

    private static Task<long> MatchingStampsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<long>(
            connection,
            "SELECT count(*) FROM collect.query_store_text AS t JOIN collect.query_store_text_seen AS s "
            + "ON s.server_id = t.server_id AND s.database_name = t.database_name AND s.query_id = t.query_id AND s.last_seen = t.last_seen",
            ct);

    private static async Task<List<Found>> FindAsync(NpgsqlConnection connection, string sql, string? database, int limit, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike("dbo.Orders") });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });

        var rows = new List<Found>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Found(
                reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetDateTime(3), reader.GetString(4)));
        }

        return rows;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.AddWithValue(parameter);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (T)value;
    }
}
