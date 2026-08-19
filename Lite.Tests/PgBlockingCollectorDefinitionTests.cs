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
/// Pins the blocking-chain collector. Two of these tests guard cost rather than correctness, which is
/// unusual and deliberate: <c>pg_blocking_pids()</c> takes ShareLock on the lock manager partitions per
/// call, so an edit that widens where it is evaluated turns the monitor into the contention it reports.
/// That regression would not fail any other test — it would collect perfectly correct data and hurt.
/// </summary>
public class PgBlockingCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 12, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_blocking", PgBlockingCollector.Instance.Name);
        Assert.Equal("pg_blocking_edges", PgBlockingCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgBlockingCollector.Instance.TargetEngine);
    }

    /// <summary>
    /// Any PostgreSQL target, standbys INCLUDED — and the standby half is the part worth asserting, because
    /// the obvious move is to copy <c>pg_autovacuum_stats</c>'s <c>IsInRecovery</c> gate. That gate exists
    /// because <c>pg_stat_user_tables</c> reports zeros on a replica. <c>pg_stat_activity</c> does not: it
    /// reports the standby's own backends, and recovery conflicts are blocking that happens ONLY on a
    /// standby. Inheriting the gate would blind the collector to a condition unique to where it was gated
    /// off.
    /// </summary>
    [Fact]
    public void AppliesToAnyPostgresTarget_IncludingStandbys()
    {
        Assert.True(PgBlockingCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = false }));
        Assert.True(PgBlockingCollector.Instance.AppliesTo(
            new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                IsAurora = true,
                IsInRecovery = true,
            }));

        /* And never against SQL Server, which has its own blocked-process report. */
        Assert.False(CollectorCatalog.AppliesTo(
            PgBlockingCollector.Instance, new CollectorTargetInfo()));
    }

    /// <summary>
    /// THE COST PIN. <c>pg_blocking_pids()</c> must be evaluated only for backends already waiting on a
    /// lock. On a 5,000-connection instance, calling it for every row is the monitoring query that becomes
    /// the incident — it acquires ShareLock on every lock manager partition per call.
    /// <para>Asserted as a gated CASE rather than a WHERE filter because both are needed at once: the
    /// gate bounds the cost, and keeping it in the select list means one query still returns the full
    /// activity snapshot, so the blocker's own state comes back without a second round trip.</para>
    /// </summary>
    [Fact]
    public void CallsBlockingPidsOnlyForLockWaiters()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("pg_blocking_pids", sql, StringComparison.Ordinal);

        /* The gate must be on the same expression as the call, not merely present somewhere in the file. */
        var callAt = sql.IndexOf("pg_blocking_pids", StringComparison.Ordinal);
        var gateAt = sql.IndexOf("wait_event_type, '') = 'Lock'", StringComparison.Ordinal);

        Assert.True(gateAt > 0, "pg_blocking_pids must be gated on wait_event_type = 'Lock'");
        Assert.True(
            gateAt < callAt && callAt - gateAt < 120,
            "the 'Lock' gate must be the CASE guarding this pg_blocking_pids call, not an unrelated "
            + "occurrence elsewhere in the query — ungated, this call runs once per backend and takes "
            + "ShareLock on every lock manager partition each time.");
    }

    /// <summary>
    /// The collector must never attribute blocking to its own backend. Darling's read sits in
    /// <c>pg_stat_activity</c> like any other session; without the filter it can appear as a victim of
    /// whatever it happens to wait on, and "zero rows when healthy" stops being reachable.
    /// </summary>
    [Fact]
    public void ExcludesItsOwnBackend()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("pg_backend_pid()", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// An EDGE LIST, not a rendered tree: the array must be unnested to one row per pair. Storing a
    /// pre-rendered chain would bake in one traversal and turn root-blocker, depth, and fan-out into string
    /// work for every future reader.
    /// </summary>
    [Fact]
    public void UnnestsToOneRowPerEdge()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("unnest(", sql, StringComparison.Ordinal);
        Assert.Contains("blocking_pid", sql, StringComparison.Ordinal);
        Assert.Contains("blocked_pid", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The blocker's own row must be LEFT joined. A blocker that has already exited still leaves a real
    /// edge, and dropping the row would understate the chain — the opposite of what a blocking monitor is
    /// for. It reports the edge with the blocker's columns null instead.
    /// </summary>
    [Fact]
    public void KeepsEdgesWhoseBlockerHasGone()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("LEFT JOIN activity AS blocker", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// No bare <c>::text</c> on a timestamptz, the class of defect
    /// <c>probe_timestamptz_render_nonutc.py</c> exists to prove: <c>ts::text</c> renders in the SESSION
    /// TimeZone, which is byte-identical to UTC on every instance in the fleet and wrong everywhere else.
    /// This collector sidesteps it entirely by shipping durations in milliseconds instead of timestamps —
    /// so the assertion is that no activity timestamp column is selected at all.
    /// </summary>
    [Fact]
    public void ShipsDurationsRatherThanTimestamps()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("clock_timestamp()", sql, StringComparison.Ordinal);

        /* The two timestamptz columns are consumed only inside the subtraction, never emitted. */
        Assert.DoesNotContain("a.xact_start::text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.query_start::text", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>track_activity_query_size</c> must be read through <c>pg_size_bytes()</c>, never cast with
    /// <c>::int</c>.
    /// <para><c>current_setting()</c> renders a memory GUC WITH ITS UNIT: this one comes back as
    /// <c>'8kB'</c> on Aurora 17.7 and <c>'4kB'</c> on 16.11, so <c>::int</c> raises "invalid input syntax
    /// for type integer" and fails the ENTIRE collection, on every cycle, not just this column. The first
    /// draft of this collector had the cast; a live probe against both majors found it, and nothing in the
    /// C# suite could have. Pinned here so the shorter-looking form cannot come back.</para>
    /// </summary>
    [Fact]
    public void ReadsTheQuerySizeGucThroughPgSizeBytes_NotAnIntCast()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains(
            "pg_size_bytes(current_setting('track_activity_query_size'))", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "current_setting('track_activity_query_size')::int", sql, StringComparison.Ordinal);

        /* And the LEFT side of that comparison must be BYTES too — the same unit mistake in a second
           disguise, which is how it shipped one line under a comment about getting units right.
           track_activity_query_size truncates at a byte boundary; length() counts characters, so on
           multi-byte text it undercounts and the flag reads false for a query that really was clipped.
           Measured on live Aurora: repeat('あ',100) is length 100, octet_length 300. */
        Assert.Contains("octet_length(coalesce(blocked.query, ''))", sql, StringComparison.Ordinal);
        Assert.Contains("octet_length(coalesce(blocker.query, ''))", sql, StringComparison.Ordinal);
        /* The negative needs a LEADING SPACE to mean anything: "length(coalesce(" is a substring of
           "octet_length(coalesce(", so the bare form is satisfied by the very code it is meant to reject.
           With the space it matches only a genuine character-length comparison, since the character before
           "length" in the correct form is an underscore. */
        Assert.DoesNotContain(" length(coalesce(blocked.query", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(" length(coalesce(blocker.query", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The synthetic backend id must combine <c>backend_start</c> with the pid. A pid alone is reused, so a
    /// 30-day history keyed on it silently merges two different backends — and the read layer's
    /// "has this been the same stuck backend all along" count is computed from exactly this value.
    /// </summary>
    [Fact]
    public void BuildsAStableBackendIdentityFromBackendStartAndPid()
    {
        var sql = PgBlockingCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("backend_start", sql, StringComparison.Ordinal);
        Assert.Contains("to_char(a.pid, 'FM0000000')", sql, StringComparison.Ordinal);
        /* pg_postmaster_start_time() is the fallback for a backend with no backend_start (background
           workers), so the id is never null and never collides with a real one. */
        Assert.Contains("pg_postmaster_start_time()", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_OrderAndTypes_Pinned()
    {
        var expected = new (string Name, CollectorColumnType Type)[]
        {
            ("blocked_backend_id", CollectorColumnType.BigInt),
            ("blocked_pid", CollectorColumnType.Integer),
            ("blocking_backend_id", CollectorColumnType.BigInt),
            ("blocking_pid", CollectorColumnType.Integer),
            ("database_name", CollectorColumnType.Varchar),
            ("blocked_username", CollectorColumnType.Varchar),
            ("blocked_application_name", CollectorColumnType.Varchar),
            ("blocked_client_addr", CollectorColumnType.Varchar),
            ("blocked_state", CollectorColumnType.Varchar),
            ("blocked_wait_event_type", CollectorColumnType.Varchar),
            ("blocked_wait_event", CollectorColumnType.Varchar),
            ("blocked_query", CollectorColumnType.Varchar),
            ("blocked_xact_duration_ms", CollectorColumnType.BigInt),
            ("blocked_query_duration_ms", CollectorColumnType.BigInt),
            ("blocking_username", CollectorColumnType.Varchar),
            ("blocking_application_name", CollectorColumnType.Varchar),
            ("blocking_client_addr", CollectorColumnType.Varchar),
            ("blocking_state", CollectorColumnType.Varchar),
            ("blocking_wait_event_type", CollectorColumnType.Varchar),
            ("blocking_wait_event", CollectorColumnType.Varchar),
            ("blocking_query", CollectorColumnType.Varchar),
            ("blocking_xact_duration_ms", CollectorColumnType.BigInt),
            ("blocking_query_duration_ms", CollectorColumnType.BigInt),
            ("blocked_pid_count", CollectorColumnType.Integer),
            ("blocking_is_idle_in_transaction", CollectorColumnType.Boolean),
            ("query_text_may_be_truncated", CollectorColumnType.Boolean),
        };

        var actual = PgBlockingCollector.Instance.PayloadColumns;
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Name, actual[i].Name);
            Assert.Equal(expected[i].Type, actual[i].Type);
        }
    }

    /// <summary>
    /// Both sides of the edge must survive the read, in the order the payload declares. A transposed
    /// blocked/blocking pair would invert every remedy the read layer produces — it would send someone to
    /// fix the victim.
    /// </summary>
    [Fact]
    public async Task ReadsBothSidesOfTheEdge()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                1_754_000_001_234_567L, 4242, 1_754_000_009_876_543L, 9999,
                "orders", "app_rw", "checkout-api", "10.0.0.5", "active", "Lock", "transactionid",
                "UPDATE orders SET status = $1 WHERE id = $2", 8_400L, 8_100L,
                /* The root waits on nothing — it is idle in transaction, so both wait columns are NULL.
                   That combination IS the signature: holding locks while waiting for a client that has
                   stopped talking. */
                "app_rw", "nightly-recon", "10.0.0.9", "idle in transaction", DBNull.Value, DBNull.Value,
                "SELECT * FROM orders WHERE id = $1", 240_000L, 239_500L,
                3, true, false,
            });

        var rows = await PgBlockingCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(4242, row.BlockedPid);
        Assert.Equal(9999, row.BlockingPid);
        Assert.Equal("active", row.BlockedState);
        Assert.Equal("idle in transaction", row.BlockingState);
        Assert.True(row.BlockingIsIdleInTransaction);
        Assert.Equal(3, row.BlockedPidCount);
        /* The identities are distinct and non-zero — the read must not collapse them onto the pid. */
        Assert.NotEqual(row.BlockedBackendId, row.BlockingBackendId);
    }

    /// <summary>
    /// A missing duration reads as -1, not 0. A backend with no open transaction has no duration to report,
    /// and 0 would read as "started this instant" — which for a blocking root inverts the diagnosis from
    /// "held for four minutes" to "just arrived".
    /// </summary>
    [Fact]
    public async Task AbsentDurationsBecomeMinusOneNotZero()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                1L, 1, 2L, 2,
                "db", DBNull.Value, DBNull.Value, DBNull.Value, "active", "Lock", "relation", "SELECT 1",
                DBNull.Value, DBNull.Value,
                /* The blocker left pg_stat_activity between the two reads, so its whole side is NULL. The
                   edge is still real and must still be reported — see KeepsEdgesWhoseBlockerHasGone. */
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value,
                1, false, false,
            });

        var rows = await PgBlockingCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(-1, row.BlockedXactDurationMs);
        Assert.Equal(-1, row.BlockedQueryDurationMs);
        Assert.Equal(-1, row.BlockingXactDurationMs);
        Assert.Equal(-1, row.BlockingQueryDurationMs);
    }

    /// <summary>
    /// No blocking is the overwhelmingly common case and the HEALTHY one. Zero rows must not be mistaken
    /// for a failed collection, so nothing here throws or synthesizes a placeholder.
    /// </summary>
    [Fact]
    public async Task NoBlockingYieldsNoRowsRatherThanAPlaceholder()
    {
        var rows = await PgBlockingCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>A blocking snapshot is a state, not a counter — no deltas.</summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 12, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            ExcludedDatabases = Array.Empty<string>(),
        };

        PgBlockingCollector.Instance.WritePayload(
            new PgBlockingCollector.Row(
                1L, 1, 2L, 2, "db", "u", "app", "10.0.0.1", "active", "Lock", "relation", "SELECT 1",
                100L, 90L, "u2", "app2", "10.0.0.2", "idle in transaction", null, null, "SELECT 2",
                200L, 190L, 1, true, false),
            new RecordingCollectorRowWriter(),
            context);

        Assert.Empty(deltas.Calls);
    }

    /// <summary>
    /// Every payload column must be written, in declaration order. The writer is positional, so a payload
    /// column added without a matching <c>.Value()</c> call shifts every later column by one and stores
    /// data that is silently wrong rather than failing.
    /// </summary>
    [Fact]
    public void WritesEveryDeclaredPayloadColumn()
    {
        var writer = new RecordingCollectorRowWriter();

        PgBlockingCollector.Instance.WritePayload(
            new PgBlockingCollector.Row(
                1L, 1, 2L, 2, "db", "u", "app", "10.0.0.1", "active", "Lock", "relation", "SELECT 1",
                100L, 90L, "u2", "app2", "10.0.0.2", "idle in transaction", null, null, "SELECT 2",
                200L, 190L, 1, true, false),
            writer,
            MakeContext());

        Assert.Equal(PgBlockingCollector.Instance.PayloadColumns.Count, writer.Values.Count);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_blocking");

        var schedule = CollectorScheduleDefaults.All["pg_blocking"];
        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }
}
