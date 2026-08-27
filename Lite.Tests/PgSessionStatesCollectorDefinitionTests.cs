/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the session-states collector (#2540): that no raw statement text can reach the store, that the
/// horizon sentinel survives the reader instead of being floored into a claim, that the version floor is
/// handled by substitution rather than by gating the collector off, and that the query text cannot re-enter
/// through the command-tag column.
/// </summary>
public class PgSessionStatesCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(int major = 16, ICollectorDeltaCalculator? deltas = null)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 23, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas ?? s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    /// <summary>
    /// The table name cannot be changed later without a migration, and pg_catalog is searched before
    /// search_path — so a store table named after a catalog object breaks CREATE INDEX with 42809 and makes
    /// unqualified reads resolve to the MONITORING store's own copy.
    /// </summary>
    [Fact]
    public void Identity_Pinned_AndTheTableDoesNotShadowACatalogObject()
    {
        Assert.Equal("pg_session_states", PgSessionStatesCollector.Instance.Name);
        Assert.Equal("pg_session_states", PgSessionStatesCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgSessionStatesCollector.Instance.TargetEngine);

        Assert.NotEqual("pg_stat_activity", PgSessionStatesCollector.Instance.TargetTable);
        Assert.NotEqual("pg_stat_get_activity", PgSessionStatesCollector.Instance.TargetTable);
    }

    /// <summary>
    /// Runs on ANY PostgreSQL target including standbys, deliberately unlike pg_autovacuum_stats.
    /// <para>A standby holds its own transactions exactly like a primary, and with hot_standby_feedback on
    /// its xmin propagates to the PRIMARY — which is one of the four causes pg_xmin_horizon attributes. A
    /// reflex IsInRecovery gate would blind this collector to the place that cause is created.</para>
    /// <para>PostgreSQL 13 is the product's documented floor and is included: query_id is the only column
    /// this reads that 13 lacks, and that is handled by substitution below rather than by refusing the
    /// target.</para>
    /// </summary>
    [Theory]
    [InlineData(13, false, false)]
    [InlineData(13, false, true)]
    [InlineData(16, true, false)]
    [InlineData(17, true, true)]
    [InlineData(18, false, true)]
    public void AppliesToEveryPostgresTarget_IncludingStandbysAndTheVersionFloor(
        int major, bool isAurora, bool inRecovery)
    {
        var target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = major,
            IsAurora = isAurora,
            IsInRecovery = inRecovery,
        };

        Assert.True(PgSessionStatesCollector.Instance.AppliesTo(target));
        Assert.True(CollectorCatalog.AppliesTo(PgSessionStatesCollector.Instance, target));
    }

    /// <summary>
    /// The engine half of the gate. A PostgreSQL definition dispatched at a SQL Server target would send
    /// this dialect at T-SQL every cycle — the #2213 class of defect — so the composed gate has to say no
    /// even though AppliesTo on its own says yes.
    /// </summary>
    [Fact]
    public void TheComposedGateRefusesASqlServerTarget()
    {
        var sqlServer = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16 };

        Assert.False(CollectorCatalog.AppliesTo(PgSessionStatesCollector.Instance, sqlServer));
    }

    /// <summary>
    /// query_id arrived in PostgreSQL 14. Below that the column does not exist and naming it is a parse
    /// error that would take the whole collection down every cycle, so it is substituted with a TYPED null —
    /// the row shape has to stay constant across a mixed-version fleet.
    /// <para>Confirmed by reading pg_attribute for the view on a live PostgreSQL 13.23 instance: 21 columns,
    /// including leader_pid, backend_type, backend_xid and backend_xmin, and no query_id.</para>
    /// </summary>
    [Theory]
    [InlineData(13)]
    [InlineData(12)]
    public void BelowPostgres14_QueryIdIsSubstitutedWithATypedNull(int major)
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext(major)).Text;

        Assert.Contains("NULL::bigint", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.query_id", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(14)]
    [InlineData(16)]
    [InlineData(18)]
    public void OnPostgres14AndAbove_QueryIdIsRead(int major)
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext(major)).Text;

        Assert.Contains("a.query_id", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <b>No column of the query is projected, on any version.</b> pg_stat_activity.query is the statement as
    /// submitted, with literal parameter values inline — verified on a live target, where a probe session's
    /// row came back carrying its literal argument verbatim.
    /// <para>This is the assertion that stops the column being added back by someone reasoning from
    /// pg_blocking, which does store it. That collector fires on an exceptional condition where the text IS
    /// the finding; this one fires on a duration floor an ordinary application crosses, so the same column
    /// would mean routinely accumulating user data to answer a question that does not need it.</para>
    /// </summary>
    [Fact]
    public void NoRawQueryTextIsEverProjectedOrStored()
    {
        foreach (var major in new[] { 13, 14, 16, 17, 18 })
        {
            var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext(major)).Text;

            /* The reference is allowed in the redaction test and in the command-tag whitelist, where the
               text is COMPARED but never emitted. What must not appear is a projection of it.

               A REGEX with a trailing word boundary, not DoesNotContain("AS query"): the plain
               substring also matches "AS query_id", which is the column this collector deliberately
               DOES emit, so the loose form failed against correct code. It failed in the safe
               direction - a guard that cries wolf costs a CI round, where one that stays quiet costs
               a leak - but a guard that cannot tell the thing it forbids from the thing it requires
               is not yet a guard. \b after "query" needs a non-word character next, and "_" is a word
               character, so "AS query_id" no longer matches while "AS query," still does. */
            Assert.DoesNotMatch(new Regex(@"\bAS\s+query\b"), sql);
            Assert.DoesNotContain("a.query AS", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("left(a.query", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("substring(a.query", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("substr(a.query", sql, StringComparison.OrdinalIgnoreCase);
        }

        Assert.DoesNotContain(
            PgSessionStatesCollector.Instance.PayloadColumns,
            c => c.Name.Contains("query_text", StringComparison.Ordinal) || c.Name == "query");
    }

    /// <summary>
    /// The command tag is a CLOSED whitelist, not a substring, and that is a data-protection decision rather
    /// than a formatting one: plenty of ORMs prepend a SQL comment block, so the first token of a real
    /// statement can be a comment opener followed by anything the application chose to put in it, and a
    /// leading-N-characters rule would carry literals straight out of a WHERE clause.
    /// </summary>
    [Fact]
    public void TheCommandTagIsAWhitelistAndFallsBackToAConstant()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("split_part(btrim(a.query), ' ', 1)", sql, StringComparison.Ordinal);
        Assert.Contains("'(other)'", sql, StringComparison.Ordinal);
        Assert.Contains("'(redacted)'", sql, StringComparison.Ordinal);
        Assert.Contains("'(idle)'", sql, StringComparison.Ordinal);

        /* The whitelist itself, spot-checked at both ends so a truncation of the list fails. */
        Assert.Contains("'SELECT'", sql, StringComparison.Ordinal);
        Assert.Contains("'UPDATE'", sql, StringComparison.Ordinal);
        Assert.Contains("'COMMIT'", sql, StringComparison.Ordinal);
        Assert.Contains("'TABLE'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// PostgreSQL block comments NEST, so a literal comment opener written inside a comment in this query
    /// opens a nested one that never closes and the whole collection fails to parse — every cycle, on every
    /// target. The first draft did exactly that and it was caught only by running the shipped string against
    /// a live instance; nothing about reading the C# reveals it.
    /// </summary>
    [Fact]
    public void TheQueryHasBalancedBlockComments()
    {
        foreach (var major in new[] { 13, 16 })
        {
            var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext(major)).Text;

            var depth = 0;
            for (var i = 0; i < sql.Length - 1; i++)
            {
                if (sql[i] == '/' && sql[i + 1] == '*')
                {
                    depth++;
                    i++;
                }
                else if (sql[i] == '*' && sql[i + 1] == '/')
                {
                    depth--;
                    i++;
                    Assert.True(depth >= 0, $"unbalanced comment close on PostgreSQL {major}");
                }
            }

            Assert.Equal(0, depth);
        }
    }

    /// <summary>
    /// The collector's own backend is excluded, or it is a PERMANENT row: Darling's read sits in
    /// pg_stat_activity with an open transaction and a backend_xmin like anything else, so the
    /// zero-rows-when-healthy state becomes unreachable and every denominator is padded by one.
    /// <para>Parallel workers are excluded too, and the guard is deliberately not a bare NULL check —
    /// leader_pid is documented NULL for a plain leader but is set to the process's OWN pid for a leader
    /// participating in its parallel group on newer majors, so excluding on NULL alone would drop those
    /// leaders entirely.</para>
    /// </summary>
    [Fact]
    public void TheCollectorsOwnBackendAndParallelWorkersAreExcluded()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("a.pid <> pg_backend_pid()", sql, StringComparison.Ordinal);
        Assert.Contains("a.leader_pid IS NULL OR a.leader_pid = a.pid", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The redaction flag is derived from the insufficient-privilege literal and NOT from a NULL state.
    /// <para>Measured under full privilege on a live target: checkpointer, walwriter and the autovacuum
    /// launcher all report a NULL state and a NULL query, so testing state alone would flag a perfectly
    /// healthy instance as unprivileged. The literal is unambiguous.</para>
    /// </summary>
    [Fact]
    public void RedactionIsDetectedFromThePrivilegeLiteralNotFromANullState()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("'<insufficient privilege>'", sql, StringComparison.Ordinal);
        Assert.Contains("AS state_is_redacted", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.state IS NULL AS state_is_redacted", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The horizon is read from BOTH backend_xmin and backend_xid, and from the GREATER of the two ages.
    /// <para>Neither column alone sees both cases. Measured on live PostgreSQL 16.15: a READ COMMITTED
    /// transaction that has written holds backend_xid with backend_xmin NULL, while a REPEATABLE READ
    /// transaction holds backend_xmin with backend_xid NULL. Reading only one makes the collector blind to
    /// half of what pins the horizon.</para>
    /// <para>age(), not arithmetic on the raw xid: modular wrap makes naive subtraction wrong exactly at the
    /// boundary where it matters most.</para>
    /// </summary>
    [Fact]
    public void TheHorizonAgeReadsBothColumnsAndTakesTheGreater()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("age(a.backend_xmin)", sql, StringComparison.Ordinal);
        Assert.Contains("age(a.backend_xid)", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(", sql, StringComparison.Ordinal);
        Assert.Contains("THEN -1::bigint", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every duration is computed SERVER-side in milliseconds, so there is no timestamp column in this table
    /// at all. timestamptz::text renders in the SESSION TimeZone and is byte-identical to UTC on a UTC
    /// server, which is why that mistake survives every probe; shipping a duration removes the class rather
    /// than guarding against it.
    /// </summary>
    [Fact]
    public void DurationsAreServerComputed_AndNoTimestampIsShipped()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("clock_timestamp()", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("::text AS state_change", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AS xact_start", sql, StringComparison.Ordinal);

        Assert.DoesNotContain(
            PgSessionStatesCollector.Instance.PayloadColumns,
            c => c.Type == CollectorColumnType.Timestamp);
    }

    /// <summary>
    /// The row cap bounds a pathological instance, and the pre-limit count travels with the rows so a
    /// truncated capture is self-evident rather than silently under-reported.
    /// </summary>
    [Fact]
    public void TheCaptureIsBoundedAndCarriesItsOwnPreLimitCount()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("LIMIT 100", sql, StringComparison.Ordinal);
        Assert.Contains("count(*) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("AS reportable_sessions", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The oldest holder is force-included regardless of the duration floor. That is the entire causal claim
    /// this collector makes, and losing it to a floor would leave the reader exactly where pg_xmin_horizon
    /// already left them — knowing a session holds the horizon and not which.
    /// <para>The winner is computed over the FULL activity set, not the reportable subset: a young
    /// transaction can be the oldest holder, and crowning a filtered survivor would name the wrong session
    /// precisely when the real one fell below the floor.</para>
    /// </summary>
    [Fact]
    public void TheOldestHolderIsIncludedRegardlessOfTheDurationFloor()
    {
        var sql = PgSessionStatesCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("max(horizon_age) FILTER (WHERE horizon_age >= 0)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE r.is_horizon_holder", sql, StringComparison.Ordinal);
        Assert.Contains("FROM activity", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// PayloadColumns order IS the wire format: WritePayload is positional, so a column added without a
    /// matching .Value() shifts everything after it and stores data that is silently WRONG rather than
    /// failing. Both the order and the count are pinned.
    /// </summary>
    [Fact]
    public void PayloadColumns_AreInOrder_AndMatchTheRowArity()
    {
        var expected = new[]
        {
            "backend_id", "pid", "database_name", "username", "application_name", "client_addr",
            "backend_type", "state", "wait_event_type", "wait_event", "command_tag", "query_id",
            "state_duration_ms", "xact_duration_ms", "query_duration_ms", "backend_duration_ms",
            "xmin_age", "xid_age", "horizon_age", "is_idle_in_transaction", "is_horizon_holder",
            "state_is_redacted", "total_sessions", "active_sessions", "idle_in_transaction_sessions",
            "reportable_sessions",
        };

        Assert.Equal(expected, PgSessionStatesCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());

        var writer = new RecordingCollectorRowWriter();
        PgSessionStatesCollector.Instance.WritePayload(SampleRow(), writer, MakeContext());

        Assert.Equal(PgSessionStatesCollector.Instance.PayloadColumns.Count, writer.Values.Count);
    }

    /// <summary>
    /// query_id is BigInt and NULLABLE, with three separate meanings the read has to keep apart: PostgreSQL
    /// 13 has no such column, 14+ reports NULL when compute_query_id is off, and a redacted row reports NULL
    /// along with everything else privileged. A sentinel would collapse all three into a number, and 0 is a
    /// legal query_id.
    /// </summary>
    [Fact]
    public void QueryIdIsNullableRatherThanSentinelled()
    {
        var column = Assert.Single(
            PgSessionStatesCollector.Instance.PayloadColumns.Where(c => c.Name == "query_id"));
        Assert.Equal(CollectorColumnType.BigInt, column.Type);

        var writer = new RecordingCollectorRowWriter();
        PgSessionStatesCollector.Instance.WritePayload(SampleRow() with { QueryId = null }, writer, MakeContext());

        Assert.Null(writer.Values[11]);
    }

    /// <summary>
    /// Every field mapped to its own ordinal, with deliberately distinct values so a transposed pair fails
    /// rather than passing on two equal numbers.
    /// </summary>
    [Fact]
    public async Task ReadsAFullyPopulatedRow_WithEveryFieldOnItsOwnOrdinal()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                17_874_796_750_069_283L, 69_283,            // backend_id, pid
                "appdb", "app_user", "checkout-worker", "10.0.0.7",
                "client backend", "idle in transaction", "Client", "ClientRead",
                "UPDATE", -5_564_491_789_055_112_251L,      // command_tag, query_id
                584_357L, 584_358L, 584_359L, 600_000L,     // state/xact/query/backend durations
                -1L, 1_401L, 1_401L,                        // xmin_age, xid_age, horizon_age
                true, true, false,                          // iit, holder, redacted
                9, 2, 4, 4,                                 // totals
            });

        var rows = await PgSessionStatesCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(17_874_796_750_069_283L, row.BackendId);
        Assert.Equal(69_283, row.Pid);
        Assert.Equal("appdb", row.DatabaseName);
        Assert.Equal("app_user", row.Username);
        Assert.Equal("checkout-worker", row.ApplicationName);
        Assert.Equal("10.0.0.7", row.ClientAddr);
        Assert.Equal("client backend", row.BackendType);
        Assert.Equal("idle in transaction", row.State);
        Assert.Equal("Client", row.WaitEventType);
        Assert.Equal("ClientRead", row.WaitEvent);
        Assert.Equal("UPDATE", row.CommandTag);
        Assert.Equal(-5_564_491_789_055_112_251L, row.QueryId);
        Assert.Equal(584_357L, row.StateDurationMs);
        Assert.Equal(584_358L, row.XactDurationMs);
        Assert.Equal(584_359L, row.QueryDurationMs);
        Assert.Equal(600_000L, row.BackendDurationMs);
        Assert.Equal(-1L, row.XminAge);
        Assert.Equal(1_401L, row.XidAge);
        Assert.Equal(1_401L, row.HorizonAge);
        Assert.True(row.IsIdleInTransaction);
        Assert.True(row.IsHorizonHolder);
        Assert.False(row.StateIsRedacted);
        Assert.Equal(9, row.TotalSessions);
        Assert.Equal(2, row.ActiveSessions);
        Assert.Equal(4, row.IdleInTransactionSessions);
        Assert.Equal(4, row.ReportableSessions);
    }

    /// <summary>
    /// The redacted shape, exactly as PostgreSQL returns it to a role without pg_monitor — measured, not
    /// imagined. The row is NOT refused: every privileged column comes back NULL while backend_xmin and
    /// backend_xid stay visible, so the horizon still reads as pinned and nothing can say by what.
    /// <para>The reader must not turn those NULLs into zeros. -1 for a duration is visibly not a
    /// measurement; 0 would read as "this transaction started this instant".</para>
    /// </summary>
    [Fact]
    public async Task ARedactedRow_KeepsItsSentinels_AndTheHorizonAgeSurvives()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                17_874_378_340_069_283L, 69_283,
                "appdb", "app_user", "checkout-worker", DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "(redacted)", DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                -1L, 900L, 900L,
                false, true, true,
                9, 0, 0, 2,
            });

        var row = Assert.Single(
            await PgSessionStatesCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None));

        Assert.True(row.StateIsRedacted);
        Assert.Null(row.State);
        Assert.Null(row.BackendType);
        Assert.Null(row.QueryId);
        Assert.Equal("(redacted)", row.CommandTag);

        Assert.Equal(-1L, row.StateDurationMs);
        Assert.Equal(-1L, row.XactDurationMs);
        Assert.Equal(-1L, row.QueryDurationMs);
        Assert.Equal(-1L, row.BackendDurationMs);

        /* The cruel part of the redaction, and the reason this row is stored rather than dropped: the xid
           age is NOT redacted, so the horizon is still visibly pinned. */
        Assert.Equal(900L, row.HorizonAge);
        Assert.True(row.IsHorizonHolder);
    }

    /// <summary>
    /// A horizon age of -1 must survive the reader. This is the sentinel the entire feature turns on: an
    /// idle-in-transaction session that holds neither a snapshot nor a transaction id pins NOTHING, and 0
    /// would read as "holds the newest possible xid" — the opposite finding, and the one that talks somebody
    /// into killing a harmless session.
    /// </summary>
    [Fact]
    public async Task IdleInTransactionPinningNothing_KeepsMinusOne_NotZero()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                17_874_796_540_069_234L, 69_234,
                "appdb", "app_user", "reporting", DBNull.Value,
                "client backend", "idle in transaction", "Client", "ClientRead",
                "SELECT", 7_184_301_683_933_573_861L,
                605_419L, 605_420L, 605_421L, 620_000L,
                -1L, -1L, -1L,
                true, false, false,
                9, 0, 4, 4,
            });

        var row = Assert.Single(
            await PgSessionStatesCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None));

        Assert.True(row.IsIdleInTransaction);
        Assert.False(row.IsHorizonHolder);
        Assert.Equal(-1L, row.XminAge);
        Assert.Equal(-1L, row.XidAge);
        Assert.Equal(-1L, row.HorizonAge);
        Assert.NotEqual(0L, row.HorizonAge);

        /* Ten minutes idle inside a transaction and pinning nothing at all. Both halves of that sentence
           have to survive into the store or the read cannot make the distinction. */
        Assert.True(row.StateDurationMs > 600_000L);
    }

    /// <summary>
    /// Zero rows is the HEALTHY state — no session had a transaction open past the floor and nothing held
    /// the horizon — and must never read as a failure or be padded with a placeholder.
    /// </summary>
    [Fact]
    public async Task AnEmptyResultIsHealthy_NotAFailure()
    {
        var rows = await PgSessionStatesCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// No deltas. Every column here is a LEVEL measured at the instant of the sample; differencing two
    /// samples would produce the elapsed time between them, which is a property of the schedule rather than
    /// of the session.
    /// </summary>
    [Fact]
    public async Task TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                1L, 1, "appdb", "u", "a", DBNull.Value, "client backend", "idle in transaction",
                DBNull.Value, DBNull.Value, "SELECT", DBNull.Value,
                1L, 2L, 3L, 4L, -1L, 5L, 5L, true, true, false, 1, 0, 1, 1,
            });

        var rows = await PgSessionStatesCollector.Instance.ReadAsync(
            reader, MakeContext(deltas: deltas), CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        PgSessionStatesCollector.Instance.WritePayload(rows[0], writer, MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    private static PgSessionStatesCollector.Row SampleRow() => new(
        BackendId: 17_874_796_750_069_283,
        Pid: 69_283,
        DatabaseName: "appdb",
        Username: "app_user",
        ApplicationName: "checkout-worker",
        ClientAddr: "10.0.0.7",
        BackendType: "client backend",
        State: "idle in transaction",
        WaitEventType: "Client",
        WaitEvent: "ClientRead",
        CommandTag: "UPDATE",
        QueryId: -5_564_491_789_055_112_251,
        StateDurationMs: 584_357,
        XactDurationMs: 584_358,
        QueryDurationMs: 584_359,
        BackendDurationMs: 600_000,
        XminAge: -1,
        XidAge: 1_401,
        HorizonAge: 1_401,
        IsIdleInTransaction: true,
        IsHorizonHolder: true,
        StateIsRedacted: false,
        TotalSessions: 9,
        ActiveSessions: 2,
        IdleInTransactionSessions: 4,
        ReportableSessions: 4);

    /// <summary>
    /// ONE MINUTE, matching pg_blocking for the same reason rather than by copying it: both read
    /// pg_stat_activity, both are SAMPLES of a view that records nothing on its own, and the cadence IS the
    /// resolution. This one is the cheaper of the two — no pg_blocking_pids() call, no lock-manager
    /// ShareLock, no per-database fan-out.
    /// <para>30 days, also matching pg_blocking: the question is whether this application is parking
    /// transactions more than it used to, and a month covers a release cycle, which is the unit at which
    /// anyone can act on the answer.</para>
    /// </summary>
    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_session_states");

        var schedule = CollectorScheduleDefaults.All["pg_session_states"];

        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);

        /* Asserted against the sibling rather than as a second literal, so the shared sampling argument
           cannot drift into two unrelated numbers. */
        Assert.Equal(CollectorScheduleDefaults.All["pg_blocking"].FrequencyMinutes, schedule.FrequencyMinutes);
        Assert.Equal(CollectorScheduleDefaults.All["pg_blocking"].RetentionDays, schedule.RetentionDays);
    }

    /// <summary>
    /// The capability vocabulary has a noun phrase for this collector, so a SQL Server target asked
    /// get_pg_session_states is told what it does not collect rather than getting the generic fallback.
    /// </summary>
    [Fact]
    public void TheCapabilityMessageNamesWhatIsNotCollected()
    {
        var message = CollectorEngineCapability.NotCollectedMessage(
            "sql-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.SqlServer, "pg_session_states");

        Assert.NotNull(message);
        Assert.Contains("idle in transaction", message, StringComparison.Ordinal);
        Assert.DoesNotContain("the data this read is served from", message, StringComparison.Ordinal);
    }
}
