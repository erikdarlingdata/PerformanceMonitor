/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// <c>logging_collector = off</c> is a third state, distinct from a denied grant and from a missing file
/// (#3410): the server writes its log to stderr and whatever started it captures that, so there is no file
/// for the two log readers to read and neither a GRANT nor a path change can make one. Before the gate, the
/// condition surfaced as whatever the directory listing raised — <c>58P01</c> where the log directory was
/// never created — an error every cycle on a server configured that way on purpose; gated WITHOUT a marker
/// it would read as a quiet server, which for the deadlock read is indistinguishable from the healthy
/// answer (#3030's shape). So both queries gate the listing on the setting AND return a marker row in place
/// of log rows, and both <c>ReadAsync</c>s turn that marker into <see cref="PgLoggingCollectorOffException"/>
/// — the not-collected-with-the-reason answer the store's own log read already gives for an empty directory.
///
/// <para>Asserted PER SITE, following <see cref="PgServerLogPathPinTests"/>: the two queries are near-twins,
/// and a total over them is satisfied by one of two identical guards reverting.</para>
/// </summary>
public sealed class PgLoggingCollectorGateTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 9, 14, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170007,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    /// <summary>The gate: a pseudoconstant predicate the planner enforces as a one-time filter above the
    /// function scan, so <c>pg_ls_logdir()</c> is never called when the setting is off and cannot raise
    /// <c>58P01</c> for a directory that legitimately does not exist.</summary>
    private const string GatesTheListing =
        "WHERE pg_catalog.current_setting('logging_collector') = 'on'";

    /// <summary>The marker branch's own predicate — the complement of the gate, so exactly one of the two
    /// branches contributes rows on any server.</summary>
    private const string EmitsTheMarker =
        "WHERE pg_catalog.current_setting('logging_collector') <> 'on'";

    /// <summary>
    /// The shipped SQL of both log readers — what <c>BuildQuery</c> hands the driver — gates the listing
    /// and emits the marker, per site with the collector named, so reverting one of the twin queries fails
    /// by name rather than averaging away.
    /// </summary>
    [Fact]
    public void BothServerLogCollectorsGateTheListingOnTheLoggingCollector()
    {
        var sites = new[]
        {
            (Collector: PgDeadlocksCollector.Instance.Name,
             Sql: PgDeadlocksCollector.Instance.BuildQuery(MakeContext()).Text),
            (Collector: PgPlanCaptureCollector.Instance.Name,
             Sql: PgPlanCaptureCollector.Instance.BuildQuery(MakeContext()).Text),
        };

        foreach (var site in sites)
        {
            Assert.True(
                site.Sql.Contains(GatesTheListing, StringComparison.Ordinal),
                $"{site.Collector} no longer gates its log-directory listing on logging_collector, so a "
                + "server that logs to stderr raises 58P01 every cycle for a directory that is allowed "
                + "not to exist.");

            Assert.True(
                site.Sql.Contains(EmitsTheMarker, StringComparison.Ordinal)
                    && site.Sql.Contains($"'{PgLoggingCollectorOffException.Marker}'", StringComparison.Ordinal),
                $"{site.Collector} gates the listing but returns no marker row when the gate closes, so "
                + "logging_collector = off reads as a quiet server instead of as a named state.");
        }
    }

    /// <summary>
    /// The deadlock read refuses the marker row rather than parsing past it. Skipped, a server that logs
    /// to stderr reads as a server that does not deadlock, and the runner never learns the one fact that
    /// explains every empty cycle.
    /// </summary>
    [Fact]
    public async Task TheDeadlockReadRefusesTheMarkerRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { PgLoggingCollectorOffException.Marker, DBNull.Value, DBNull.Value, DBNull.Value });

        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(
            () => PgDeadlocksCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None).AsTask());
    }

    /// <summary>Same refusal on the plan read, whose marker row is shaped for its typed columns: a real row
    /// always carries a query id (the regexp capture is literal digits cast to bigint), so a NULL first
    /// column beside the marker text is the gate's row and nothing else's.</summary>
    [Fact]
    public async Task ThePlanReadRefusesTheMarkerRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { DBNull.Value, DBNull.Value, PgLoggingCollectorOffException.Marker });

        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(
            () => PgPlanCaptureCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None).AsTask());
    }

    /// <summary>
    /// The control for both refusals: an ordinary row still parses, so the marker check discriminates
    /// rather than tripping on real data. The deadlock row is the shape the query's regexp produces from
    /// the captured report <c>PgDeadlockLogParserTests</c> pins.
    /// </summary>
    [Fact]
    public async Task AnOrdinaryDeadlockRowStillParses()
    {
        var reader = new FakeCollectorDataReader(new object[]
        {
            "2026-08-26 22:25:24.100",
            "UTC",
            "1549",
            "Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
            + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
            + "\tProcess 1549: \n"
            + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n"
            + "\tProcess 1556: \n"
            + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n",
        });

        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(1549, row.VictimPid);
    }

    /// <summary>The plan read's control, through the same shared parser the definition tests use.</summary>
    [Fact]
    public async Task AnOrdinaryPlanRowStillParses()
    {
        var reader = new FakeCollectorDataReader(new object[]
        {
            1L,
            12.5,
            "{ \"Plan\": { \"Node Type\": \"Seq Scan\", \"Relation Name\": \"dl\" } }",
        });

        var rows = await PgPlanCaptureCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(1L, row.QueryId);
    }

    /// <summary>
    /// The refusal's message is the sentence the runner stores and the precondition read quotes back, so
    /// it has to carry the whole answer itself: the setting by name, the restart the fix costs (the
    /// setting is postmaster-context — a reload does not apply it), the explicit denial that this is
    /// 42501's or 58P01's problem, and the statement that nothing was read — because a reader who skips
    /// every other word must still not conclude "no deadlocks".
    /// </summary>
    [Fact]
    public void TheRefusalNamesTheSettingTheRestartAndWhatItIsNot()
    {
        var message = new PgLoggingCollectorOffException().Message;

        Assert.Contains(PgLoggingCollectorOffException.SettingName, message, StringComparison.Ordinal);
        Assert.Contains("RESTART", message, StringComparison.Ordinal);
        Assert.Contains("42501", message, StringComparison.Ordinal);
        Assert.Contains("58P01", message, StringComparison.Ordinal);
        Assert.Contains("NOT 'the log held nothing'", message, StringComparison.Ordinal);

        /* And it must not send anyone after the grant pair — the dead end this state exists to rule out. */
        Assert.DoesNotContain("pg_read_server_files", message, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", message, StringComparison.Ordinal);
    }
}
