/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the session-states read (#2540): that the rollup keys on the reuse-safe backend identity rather than
/// the pid, that the ordering puts a proven horizon holder above a merely long transaction, and — the
/// assertion this whole surface exists for — that the causal claim is gated on the horizon age and REFUSED
/// where the age says the session pinned nothing.
/// </summary>
public class DarlingPgSessionStatesReaderTests
{
    private static string Sql => DarlingPgSessionStatesReader.PgSessionStatesSql;

    /// <summary>
    /// The shipped SQL aligns its projection, so a fragment typed with single spaces will not be found in
    /// it. Structure assertions run against this; exact-rendering assertions keep using <see cref="Sql"/>.
    /// </summary>
    private static string Squeezed => Regex.Replace(Sql, @"\s+", " ");

    private static string CountsSql => DarlingPgSessionStatesReader.PgSessionStatesCaptureCountsSql;

    private static string LongRunningSql => DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql;

    // ── #2711 Long-Running Query — the live-state read ──────────────────────────────────────────────

    [Fact]
    public void LongRunningSql_ScopesToOneServerOneThresholdAndOneRecencyFloor()
    {
        Assert.Contains("server_id = $1", LongRunningSql, StringComparison.Ordinal);
        Assert.Contains("query_duration_ms >= $2", LongRunningSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $3", LongRunningSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", LongRunningSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The mechanism this whole read exists for: "most recent" is computed via <c>max(collection_time)</c>
    /// over the recency-bounded set and then JOINED back, rather than just filtering rows to
    /// <c>collection_time >= $3</c> directly. Without the join, a session whose peak crossed the threshold in
    /// an OLDER capture inside the recency window — but has since finished, so it is absent from the truly
    /// latest capture — would still be reported as currently running. Verified against a real local
    /// PostgreSQL 17 with exactly this shape (a superseded older-but-in-window row correctly excluded, a
    /// short same-cycle row correctly excluded, only the genuinely-latest over-threshold row returned) before
    /// this test was written; this pins the SQL shape that made that hold.
    /// </summary>
    [Fact]
    public void LongRunningSql_FiltersToTheSingleLatestCapture_ViaMaxJoin_NotAWindowFilterAlone()
    {
        Assert.Contains("max(collection_time)", LongRunningSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("s.collection_time = r.latest_capture", LongRunningSql, StringComparison.Ordinal);
    }

    [Fact]
    public void LongRunningSql_TheRowLimitIsParameterisedRatherThanBakedIn()
    {
        Assert.DoesNotMatch(new Regex(@"LIMIT\s+\d+"), LongRunningSql);
    }

    [Fact]
    public void LongRunningSql_ReadsOnlyItsOwnTable()
    {
        Assert.Contains("FROM pg_session_states", LongRunningSql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_blocking_edges", LongRunningSql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_deadlocks", LongRunningSql, StringComparison.Ordinal);
    }

    /// <summary>No query text projected — the collector deliberately stores none (see its class remarks), so
    /// a read that tried to select one would be selecting a column that does not exist. Word-boundary regex
    /// rather than a plain substring check: a naive "s.query" substring search self-matches the legitimate
    /// "s.query_duration_ms" projection (caught by this exact test failing red the first time it was
    /// written), since "query" is a literal prefix of that identifier.</summary>
    [Fact]
    public void LongRunningSql_ProjectsNoQueryText()
    {
        Assert.DoesNotContain("query_text", LongRunningSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotMatch(new Regex(@"\bquery\b", RegexOptions.IgnoreCase), LongRunningSql);
    }

    /// <summary>
    /// Excludes idle-in-transaction sessions. Per PostgreSQL's own semantics, <c>query_duration_ms</c> for a
    /// session sitting idle-in-transaction measures how long ago its LAST query started, not how long a query
    /// has actually been running — that query already finished. Without this filter a session that ran a 5ms
    /// UPDATE and has sat idle-in-transaction for 40 minutes since would read identically to one whose UPDATE
    /// has genuinely been running for 40 minutes, which is a different, differently-actioned incident (an app
    /// connection-pool bug vs. a slow statement). Matches the SQL Server equivalent
    /// (<c>AlertEngine.CheckLongRunningQueriesAsync</c>), which reads <c>sys.dm_exec_requests</c> — a table of
    /// requests actually executing, where an idle session has no row at all.
    /// </summary>
    [Fact]
    public void LongRunningSql_ExcludesIdleInTransactionSessions()
    {
        Assert.Contains("s.is_idle_in_transaction = false", LongRunningSql, StringComparison.Ordinal);
    }

    // ── Scoping and parameterisation ─────────────────────────────────────────────────────────────

    [Fact]
    public void ScopesToOneServerAndOneWindow()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The row limit is a PARAMETER, not a literal. A hardcoded LIMIT shipped once on another PostgreSQL
    /// read and had to be corrected; a caller that raises the limit must actually get more rows.
    /// </summary>
    [Fact]
    public void TheRowLimitIsParameterisedRatherThanBakedIn()
    {
        Assert.DoesNotMatch(new Regex(@"LIMIT\s+\d+"), Sql);
    }

    [Fact]
    public void ReadsOnlyItsOwnTable()
    {
        Assert.Contains("FROM pg_session_states", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_blocking_edges", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_xmin_horizon", Sql, StringComparison.Ordinal);
    }

    // ── The rollup ───────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The rollup keys on backend_id, NOT on pid, and that is the whole reason the collector builds a
    /// synthetic identity. Pids are reused: on a busy instance the same number can be two different backends
    /// inside one retention window, and a pid-keyed group would silently average them into one session that
    /// never existed.
    /// <para>Verified against the shipped SQL on a live store with a fixture where pid 100 is two different
    /// backends: the read returns two rows.</para>
    /// </summary>
    [Fact]
    public void RollsUpPerBackendIdentityRatherThanPerPid()
    {
        Assert.Contains("GROUP BY s.backend_id", Squeezed, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (backend_id)", Squeezed, StringComparison.Ordinal);
        Assert.DoesNotContain("GROUP BY s.pid", Squeezed, StringComparison.Ordinal);
    }

    /// <summary>
    /// Peaks, not averages. The finding is how far this went, and an average over a hundred samples of a
    /// transaction that grew monotonically reports roughly half of what actually happened.
    /// </summary>
    [Theory]
    [InlineData("max(s.state_duration_ms)")]
    [InlineData("max(s.xact_duration_ms)")]
    [InlineData("max(s.horizon_age)")]
    [InlineData("max(s.xmin_age)")]
    [InlineData("max(s.xid_age)")]
    /* backend_duration_ms was collected, stored and given its own V86 column, and the reader never
       selected it - found in review. It is the "captured data that no read reports" category, and the
       repair for that category is an assertion rather than a one-off fix. */
    [InlineData("max(s.backend_duration_ms)")]
    public void ReportsPeaksNotAverages(string fragment)
    {
        Assert.Contains(fragment, Squeezed, StringComparison.Ordinal);
        Assert.DoesNotContain("avg(", Squeezed, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// horizon_holder_samples is a COUNT and not a flag, because the two ends of it are opposite findings:
    /// every write transaction is momentarily the oldest holder, so one sighting in a hundred is ordinary
    /// traffic while ninety-eight is the reason vacuum reclaims nothing. A boolean cannot separate them.
    /// </summary>
    [Fact]
    public void HorizonHoldingIsCountedAcrossSamplesNotFlagged()
    {
        Assert.Contains(
            "count(*) FILTER (WHERE s.is_horizon_holder)", Squeezed, StringComparison.Ordinal);
        Assert.Contains(
            "count(*) FILTER (WHERE s.is_idle_in_transaction)", Squeezed, StringComparison.Ordinal);
    }

    /// <summary>
    /// Worst-first, not newest-first — the same ordering argument the blocking read makes. A newest-first
    /// read under a row limit would return whatever happened at the end of the window and could omit the
    /// incident entirely. Holders lead because theirs is the only row carrying a proven causal claim.
    /// <para>Verified live against an adversarial fixture where the culprit is named 'zzz-worker' and a
    /// decoy with an EQUAL peak transaction duration is named 'aaa-app': the holder still sorts first.</para>
    /// </summary>
    [Fact]
    public void OrdersHorizonHoldersFirstThenTheLongestTransaction()
    {
        var order = Squeezed[Squeezed.LastIndexOf("ORDER BY", StringComparison.Ordinal)..];

        Assert.Contains("(r.horizon_holder_samples > 0) DESC", order, StringComparison.Ordinal);
        Assert.Contains("r.peak_xact_duration_ms DESC", order, StringComparison.Ordinal);

        Assert.True(
            order.IndexOf("horizon_holder_samples", StringComparison.Ordinal)
            < order.IndexOf("peak_xact_duration_ms", StringComparison.Ordinal),
            "a proven horizon holder must outrank a merely long transaction");
    }

    /// <summary>
    /// Nullable join columns compare with IS NOT DISTINCT FROM. Plain <c>=</c> is NULL for a NULL operand,
    /// so it silently DROPS the row rather than matching it.
    /// </summary>
    [Fact]
    public void JoinsOnNullableColumnsUseIsNotDistinctFrom()
    {
        Assert.Contains("l.backend_id IS NOT DISTINCT FROM r.backend_id", Squeezed, StringComparison.Ordinal);
        Assert.Contains("c.collection_id IS NOT DISTINCT FROM s.collection_id", Squeezed, StringComparison.Ordinal);
    }

    /// <summary>
    /// Truncation is derived by comparing the collector's own pre-limit count against the rows it actually
    /// stored, per capture. Without it, an instance parking thousands of sessions reports whatever fitted
    /// under the cap as if it were the whole picture.
    /// </summary>
    [Fact]
    public void TruncationIsDerivedPerCaptureFromThePreLimitCount()
    {
        Assert.Contains("count(*)::bigint AS rows_stored", Squeezed, StringComparison.Ordinal);
        Assert.Contains(
            "bool_or(c.reportable_sessions > c.rows_stored)", Squeezed, StringComparison.Ordinal);
    }

    /// <summary>
    /// EVERY column the collector stores is read back by some read.
    ///
    /// <para><b>An invariant, not a spot fix.</b> Review found <c>backend_duration_ms</c> collected, written
    /// by <c>WritePayload</c> and given its own column in the V86 rung — and never selected by the reader,
    /// so it was written every minute of every day and reachable only by hand-written SQL against the store.
    /// That is the "captured data that no read reports" category, and the repair for a category is an
    /// assertion that closes it rather than one more remembered column.</para>
    ///
    /// <para>Derived from the SHIPPED artifacts on both sides — <c>PayloadColumns</c> and the reader's own
    /// SQL constants — rather than from a list written here, so it cannot keep passing while the two drift
    /// underneath it.</para>
    /// </summary>
    [Fact]
    public void EveryStoredColumn_IsReadBackBySomeRead()
    {
        var unread = PgSessionStatesCollector.Instance.PayloadColumns
            .Select(c => c.Name)
            .Where(name => !Sql.Contains(name, StringComparison.Ordinal)
                           && !CountsSql.Contains(name, StringComparison.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(unread.Count == 0,
            "These columns are collected and stored every cycle and no read selects them, so they are "
            + "reachable only by hand-written SQL against the store. Either surface them or stop storing "
            + "them:\n  " + string.Join("\n  ", unread));
    }

    // ── The denominator ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The capture denominator comes from collection_log and NOT from the data, because this is an EXCEPTION
    /// surface like pg_blocking_edges: the collector stores nothing when every transaction is short, so an
    /// absent capture and a capture that found a healthy instance are byte-identical in the stored rows.
    /// Probing the table itself would report a well-monitored server as uncollected (#2508).
    /// </summary>
    [Fact]
    public void TheDenominatorComesFromCollectionLogNotFromTheData()
    {
        Assert.Contains("FROM collection_log", CountsSql, StringComparison.Ordinal);
        Assert.Contains("l.collector_name = 'pg_session_states'", CountsSql, StringComparison.Ordinal);
        Assert.Contains("l.status = 'SUCCESS'", CountsSql, StringComparison.Ordinal);
        Assert.Contains("count(*) FILTER (WHERE l.rows_collected > 0)", CountsSql, StringComparison.Ordinal);

        Assert.DoesNotContain("FROM pg_session_states", CountsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDenominatorIsScopedToTheSameServerAndWindow()
    {
        Assert.Contains("l.server_id = $1", CountsSql, StringComparison.Ordinal);
        Assert.Contains("l.collection_time >= $2", CountsSql, StringComparison.Ordinal);
        Assert.Contains("l.collection_time <= $3", CountsSql, StringComparison.Ordinal);
    }

    // ── The classifier: where the causal claim is made, and refused ──────────────────────────────

    private static DarlingPgSessionStatesReader.PgSessionStateRow Row(
        int sampleCount = 10,
        int horizonHolderSamples = 0,
        int idleInTransactionSamples = 0,
        long peakHorizonAge = -1,
        long peakStateDurationMs = 1_000,
        bool stateWasRedacted = false)
        => new(
            BackendId: 17_874_796_750_069_283,
            Pid: 69_283,
            DatabaseName: "appdb",
            Username: "app_user",
            ApplicationName: "checkout-worker",
            ClientAddr: null,
            BackendType: "client backend",
            LastState: "idle in transaction",
            LastWaitEventType: "Client",
            LastWaitEvent: "ClientRead",
            LastCommandTag: "UPDATE",
            LastQueryId: 111,
            FirstSeenAt: new DateTime(2026, 8, 23, 10, 0, 0, DateTimeKind.Unspecified),
            LastSeenAt: new DateTime(2026, 8, 23, 10, 10, 0, DateTimeKind.Unspecified),
            SampleCount: sampleCount,
            IdleInTransactionSamples: idleInTransactionSamples,
            HorizonHolderSamples: horizonHolderSamples,
            PeakStateDurationMs: peakStateDurationMs,
            PeakXactDurationMs: peakStateDurationMs,
            PeakQueryDurationMs: peakStateDurationMs,
            PeakBackendDurationMs: peakStateDurationMs + 1_000,
            PeakHorizonAge: peakHorizonAge,
            PeakXminAge: -1,
            PeakXidAge: peakHorizonAge,
            StateWasRedacted: stateWasRedacted,
            TotalSessions: 20,
            ActiveSessions: 2,
            IdleInTransactionSessions: 4,
            ReportableSessions: 4,
            CaptureWasTruncated: false);

    /// <summary>
    /// A sustained holder gets the vacuum argument, and gets pointed at the sibling read that should agree
    /// with it. Cross-checking against get_pg_xmin_horizon is what turns a claim into a corroborated one.
    /// </summary>
    [Fact]
    public void ASustainedHolderIsToldItPinsTheHorizon()
    {
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(
            Row(sampleCount: 10, horizonHolderSamples: 9, idleInTransactionSamples: 10, peakHorizonAge: 40_000));

        Assert.Contains("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);
        Assert.Contains("get_pg_xmin_horizon", finding, StringComparison.Ordinal);
        Assert.Equal("Critical", DarlingMcpPgSessionStatesTools.SessionSeverity(
            Row(sampleCount: 10, horizonHolderSamples: 9, idleInTransactionSamples: 10, peakHorizonAge: 40_000)));
    }

    /// <summary>
    /// One sighting in ten is NOT a finding. Every write transaction is briefly the oldest holder — that is
    /// what a transaction is — so a tool that reported every one of them as pinning the horizon would raise
    /// an alarm on ordinary traffic and be ignored within a day.
    /// </summary>
    [Fact]
    public void APassingSightingIsNotCalledASustainedHold()
    {
        var row = Row(sampleCount: 10, horizonHolderSamples: 1, idleInTransactionSamples: 10, peakHorizonAge: 12);
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(row);

        Assert.Contains("passing sighting", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);
        Assert.NotEqual("Critical", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
    }

    /// <summary>
    /// <b>The assertion the whole feature turns on.</b> An idle-in-transaction session that pinned nothing
    /// must be told so explicitly, and must NOT be given the vacuum argument.
    /// <para>Measured on live PostgreSQL 16.15: a READ COMMITTED transaction that only read, and one whose
    /// UPDATE matched zero rows, both sat idle in transaction indefinitely with backend_xmin AND backend_xid
    /// NULL. Terminating either reclaims not one dead row. A tool reasoning from the state string and the
    /// clock would recommend exactly that.</para>
    /// </summary>
    [Fact]
    public void IdleInTransactionPinningNothing_IsRefusedTheVacuumArgument()
    {
        var row = Row(
            sampleCount: 10, horizonHolderSamples: 0, idleInTransactionSamples: 10,
            peakHorizonAge: -1, peakStateDurationMs: 600_000);
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(row);

        Assert.Contains("pinned NOTHING", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("VACUUM cannot reclaim", finding, StringComparison.Ordinal);

        /* And it is not silently dismissed either: a ten-minute idle transaction is still holding a
           connection and its locks, which is a different argument the finding has to make on its own terms
           rather than borrowing the vacuum one. */
        Assert.Contains("forgotten commit", finding, StringComparison.Ordinal);
        Assert.Equal("Warning", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
    }

    /// <summary>
    /// A SHORT idle transaction pinning nothing is not a finding at all. The floor exists so an ordinary
    /// application does not get told it has a problem it does not have.
    /// </summary>
    [Fact]
    public void AShortIdleTransactionPinningNothingIsNotAFinding()
    {
        var row = Row(
            sampleCount: 2, horizonHolderSamples: 0, idleInTransactionSamples: 2,
            peakHorizonAge: -1, peakStateDurationMs: 15_000);

        Assert.Equal("Healthy", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
        Assert.DoesNotContain("forgotten commit",
            DarlingMcpPgSessionStatesTools.HorizonFinding(row), StringComparison.Ordinal);
    }

    /// <summary>
    /// Redaction outranks every other branch, and gets its own severity band rather than being painted
    /// healthy or critical.
    /// <para>If the state columns came back NULL for want of pg_monitor then every input to every other
    /// finding is an artefact of a missing GRANT rather than an observation about the database. Measured:
    /// PostgreSQL does not refuse the read, it returns the row with everything but pid, application_name,
    /// datname, usename, backend_xid and backend_xmin NULL — so backend_xmin and backend_xid stay VISIBLE
    /// and the horizon
    /// still reads as pinned and nothing can say by what. Same treatment estimate_unavailable gets on the
    /// bloat surface, for the same reason.</para>
    /// </summary>
    [Fact]
    public void ARedactedRowIsCalledUnknown_NotHealthyAndNotCritical()
    {
        var row = Row(
            sampleCount: 5, horizonHolderSamples: 5, idleInTransactionSamples: 0,
            peakHorizonAge: 900, stateWasRedacted: true);
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(row);

        Assert.Contains("CANNOT SAY", finding, StringComparison.Ordinal);
        Assert.Contains("pg_monitor", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);

        Assert.Equal("Unknown", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
        Assert.NotEqual("Critical", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
        Assert.NotEqual("Healthy", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
    }

    /// <summary>
    /// A duration sentinel of -1 must render as words rather than as a number, in every helper that formats
    /// one. "-1ms" read off a screen is a measurement; "not measured" is not.
    /// </summary>
    [Fact]
    public void TheNotMeasuredSentinelNeverRendersAsANumber()
    {
        Assert.Equal("not measured", DarlingMcpPgSessionStatesTools.FormatDuration(-1));
        Assert.DoesNotContain("-1", DarlingMcpPgSessionStatesTools.FormatDuration(-1), StringComparison.Ordinal);
    }
}
