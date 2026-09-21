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
using Npgsql;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's <see cref="IAlertReadAdapter"/> (Phase-5 slice B): the seven collected alert feeds
/// read from Postgres — each query is Lite's DuckDB read ported dialect-for-dialect against the
/// same table names and columns (the generated PG schema mirrors Lite's — see PgSchemaGenerator),
/// with three deliberate adjustments:
/// (1) no <c>v_</c> views exist here — the raw tables are queried directly;
/// (2) Lite's long-running-query read uses DuckDB's bare <c>NOW()</c>; the PG twin binds a
///     parameterized naive-UTC now instead (the poison-wait read's parameterization style),
///     because a bare <c>now()</c> is <c>timestamptz</c> and comparing it against the naive-UTC
///     <c>timestamp</c> columns would resolve in the server's time zone;
/// (3) DuckDB's <c>N'...'</c> literals become plain <c>'...'</c> (Postgres has no N-prefix).
/// All bound timestamps are naive-UTC Kind-Unspecified, matching the COPY writer's storage
/// discipline. The blocking read reproduces Lite's XE-preferred + DMV-fallback merge via the
/// shared <see cref="BlockedProcessReportMerge"/>.
/// <para>
/// <c>serverKey</c> is the deterministic storage-name hash rendered as a string (the engine's
/// identity across the Phase-5 seams), parsed back to the <c>server_id</c> int here.
/// </para>
/// </summary>
public sealed class DarlingAlertReadAdapter : IAlertReadAdapter
{
    /// <summary>
    /// The explicit command deadline for EVERY read in the alert evaluation pass (#2874).
    ///
    /// <para>All forty-five commands across the six alert-pass types ran with no
    /// <c>CommandTimeout</c>, so every one inherited Npgsql's undocumented 30 s default. Nobody chose
    /// 30 s; it was simply what happened. On 2026-09-04 the forced-plan read failed five times on the
    /// production store, each time surfacing as "Exception while reading from stream" — which is how
    /// Npgsql renders its OWN deadline, and which read literally says the network broke (the same
    /// misdiagnosis #2826 exists to prevent).</para>
    ///
    /// <para><b>Why this pass needed its own number rather than the 60 s #2810 and #2871 chose.</b>
    /// Those two sit under <c>DarlingWorker.s_analysisTimeout</c>, a 120 s <c>CancelAfter</c> that
    /// bounds the whole pass however long an individual command runs. <b>This pass has no enclosing
    /// budget at all</b> — <c>EvaluateAlertsAsync</c> is called with the plain stopping token — so the
    /// per-command deadline IS the pass budget, multiplied by however many reads run in sequence.
    /// At the inherited 30 s that is 45 x 30 s of worst-case exposure while the body holds one of only
    /// <see cref="DarlingWorker.MaxConcurrentServerSweeps"/> fleet permits, and the sweep skips
    /// relaunch for that server the whole time. Copying 60 s here would have doubled it.</para>
    ///
    /// <para><b>Bounded below</b> by measurement: the shipped queries were timed against the
    /// production store on its three busiest servers, cold and warm. The whole pass is dominated by
    /// one read — the forced-plan check at <b>1,744.9 ms</b> cold, scanning ~6.0 GB of
    /// <c>query_store_stats</c>; every other read in the family lands under 3 ms. Ten seconds is 5.7x
    /// that worst case, so it absorbs a substantial stall rather than only the happy path. That margin
    /// has been measured being eaten twice, and both times restored by fixing the read rather than by
    /// moving this number. First the collection-signals read's whole-history top-N sort grew with the
    /// 90-day retention fill until its cold excursions clocked ~12 s — the first measured breach — and
    /// #3496 made that read chunk-orderable. Then the 1,744.9 ms read itself: the ~6.0 GB it was measured
    /// over became 23 GB, and its plan turned out to have been paying a fleet-width tax the whole time —
    /// the planner walked the entire fleet's two-hour slice through the time index and filtered 95% of it
    /// away per server (<c>Rows Removed by Filter: 691,058</c>, 57,307 buffers) rather than take the
    /// <c>(server_id, collection_time)</c> composite that was already there — so the cold tail crossed
    /// this deadline at 10.3 s while the fixed #3496 site sat at zero (#3573). The covering index in
    /// <c>PgTableTuning</c> made that read an Index Only Scan over one server's rows (50 buffers against
    /// 1,514 for the same statement on the rig; see <see cref="ForcePlanFailuresSql"/>). The 1,744.9 ms
    /// figure therefore stands as the measured floor this number was derived from and as the recorded
    /// cost of the access path that has since been replaced, not as a current cost — and 10 s stays: the
    /// cadence bound below has not moved, and a deadline re-fitted to a read that now costs milliseconds
    /// would only mean the next drift is caught later.</para>
    ///
    /// <para><b>Bounded above</b> by the cadence this pass runs on: <c>s_alertSweepInterval</c> is
    /// 30 s, so one stalled read must still leave the pass able to finish inside the interval that
    /// will start it again. Ten seconds keeps a single stall well inside that, and caps the unbudgeted
    /// worst case at 45 x 10 s instead of 45 x 30 s.</para>
    ///
    /// <para><b>The arithmetic after #3848's retry, and why the bound above still holds.</b> A read that
    /// crosses this deadline is now retried ONCE, two seconds later, under this same deadline
    /// (<see cref="ExecuteWithOneRetryAsync"/>), so a stalled read costs at most 22 s rather than 10 s and
    /// the pass's unbudgeted worst case becomes 45 x 22 s. That figure is reached only when EVERY read on
    /// EVERY server stalls twice — a fleet-wide store outage, which is precisely the case where this pass
    /// SHOULD be slow and where a fast pass would only mean it gave up on all forty-five conditions
    /// quickly. The case the retry is FOR costs 12 s on one read of one server: the measured population is
    /// sparse transients inside the store's own write bands (raw-hypertable compression, the daily-tier
    /// materialization's WAL storm, the hourly successor refreshes, timed-checkpoint fsync tails of
    /// 8–25 s), every one of which the monitoring seat's own retry-once discipline survived while this
    /// pass — which had no retry — recorded a blind condition and ran into the same band 30 s later. So the
    /// upper bound moves by a factor the outage case earns and the ordinary case never pays.</para>
    ///
    /// <para>The number itself does NOT move, and that is the point of retrying instead: the asymmetry
    /// below still says err short, because a read that runs long holds a fleet-sweep permit. A retry re-asks
    /// the same question after the band has had two seconds to pass, which is a different lever from
    /// waiting longer inside one attempt — and it is the only one of the two that a 25-second fsync tail
    /// can be survived on without raising this deadline past the 30 s cadence.</para>
    ///
    /// <para><b>Deliberately reader-side, and independent of the three-band grid redesign</b> (#3678 /
    /// #3781 / #3745, which decide together when the store writes). That work changes whether the bands
    /// collide; this changes whether a twelve-second write burst blinds an alert. Both are wanted, neither
    /// waits on the other, and this one carries no coupling to the grid's shape — so a rescheduled band
    /// makes the retry rarer rather than wrong.</para>
    ///
    /// <para><b>The asymmetry is why erring SHORT is right here, and it is the reverse of #2810.</b>
    /// A read that exceeds this deadline skips one alert check and logs it; the next pass runs 30 s
    /// later, so the cost is one cycle of delay on one alert. A read that runs long holds a fleet
    /// sweep permit and delays collection for every other server queued behind it. The recoverable
    /// failure is strictly cheaper than the unrecoverable one, so this is the first value in the
    /// family set BELOW what it inherited rather than above it.</para>
    ///
    /// <para><b>What the data cannot say.</b> Every observed failure was killed AT the 30 s ceiling,
    /// so the record is right-censored: nothing here establishes whether a stalled read wanted 35 s or
    /// 300 s. Ten seconds is chosen from the measured cost and the cadence above, NOT fitted to the
    /// failure distribution — a number claiming to fit that data would be invented.</para>
    ///
    /// <para><b>What this does NOT cover.</b> <c>PgPlanForceActionStore</c> sits beside these
    /// types and is not one of them: its only caller is <c>PlanForceBot.RunAfterAnalysisAsync</c>,
    /// dispatched as the analysis pass's post-pass hook over the plain stopping token. It shares
    /// the unbudgeted shape but runs on the analysis interval, not this pass's 30 s cadence, so the
    /// upper bound derived above does not apply to it and it is left for its own group (#2874).</para>
    /// </summary>
    internal const int AlertPassCommandTimeoutSeconds = 10;

    /// <summary>
    /// How long <see cref="ExecuteWithOneRetryAsync"/> waits between a read's two attempts (#3848).
    ///
    /// <para>Two seconds, and the number is the monitoring seat's measured one rather than a guess: its
    /// retry-once-sequential discipline uses this pause and did not lose a read across the week that
    /// produced this issue, over the same store and the same bands. What the pause has to outlast is a
    /// transient measured in seconds — a checkpoint fsync tail, a compression chunk, one hourly
    /// materialization's WAL burst — and what it must NOT do is re-ask while the band is still on, which
    /// is what a zero-wait retry does: a second attempt issued immediately against a store that just held
    /// a statement for ten seconds spends another ten and buys the same answer, turning one blind
    /// condition into a 20-second permit hold with nothing to show. Two seconds is also small enough that
    /// the pass's worst case stays legible arithmetic (22 s per stalled read against a 30 s cadence);
    /// a pause long enough to outlast a 25 s fsync tail would not be a retry, it would be a second
    /// deadline.</para>
    ///
    /// <para>Not fitted to the failure distribution, deliberately and for the reason the deadline above
    /// states about itself: every observed kill was censored AT the deadline, so nothing in the record
    /// says how much longer any individual read wanted. This is chosen from a working discipline's
    /// measured value and from the cadence, and a number claiming to fit that data would be invented.</para>
    /// </summary>
    internal const int AlertPassRetryDelaySeconds = 2;

    private readonly NpgsqlDataSource _postgres;
    private readonly Func<int, int>? _runningJobsCadenceMinutes;
    private readonly Func<int, int>? _blockingSnapshotCadenceMinutes;
    private readonly AlertReadFailureCounter? _readFailures;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    /// <param name="runningJobsCadenceMinutes">
    /// Resolves a server's EFFECTIVE running_jobs collection cadence (minutes) for the #1812
    /// snapshot-freshness bound — the worker supplies its own schedule resolution (the same
    /// <c>StoreConfigProvider.ResolveSchedule</c> the sweep runs on). Null (test call sites) or a
    /// non-positive answer falls back to the shared <see cref="CollectorScheduleDefaults"/> cadence.
    /// </param>
    /// <param name="blockingSnapshotCadenceMinutes">
    /// The same resolver for the dmv_blocking_snapshot cadence, behind #1839's freshness bound.
    /// </param>
    /// <param name="readFailures">
    /// #3848: the process counter every RETRIED read is tallied on. Passed explicitly by the worker rather
    /// than defaulted to <see cref="AlertReadFailureCounter.Shared"/> inside this type, matching how the
    /// engine takes it — a test constructs its own and cannot pollute the shared one. Null (most test call
    /// sites) retries exactly the same way and counts nothing.
    /// </param>
    /// <param name="delay">
    /// The pause between a read's two attempts, injectable so a pin can assert the seam WAITED
    /// <see cref="AlertPassRetryDelaySeconds"/> without spending two seconds of test time doing it.
    /// Defaults to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>, which is what production runs.
    /// </param>
    public DarlingAlertReadAdapter(
        NpgsqlDataSource postgres,
        Func<int, int>? runningJobsCadenceMinutes = null,
        Func<int, int>? blockingSnapshotCadenceMinutes = null,
        AlertReadFailureCounter? readFailures = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _runningJobsCadenceMinutes = runningJobsCadenceMinutes;
        _blockingSnapshotCadenceMinutes = blockingSnapshotCadenceMinutes;
        _readFailures = readFailures;
        _delay = delay ?? Task.Delay;
    }

    /* ---------------- the retry seam (#3848) ---------------- */

    /// <summary>
    /// Runs one alert-pass read under the <see cref="AlertPassCommandTimeoutSeconds"/> deadline its own
    /// command carries; on a COMMAND-TIMEOUT failure and nothing else, waits
    /// <see cref="AlertPassRetryDelaySeconds"/> and runs it once more (#3848).
    ///
    /// <para><b>One seam rather than fourteen edits.</b> This type issues nineteen commands across
    /// fourteen public reads, and the retry decision is one decision — fourteen copies of it would be free
    /// to disagree the moment any of them grew an arm, which is the argument
    /// <see cref="PostgresTransportFault"/> already makes for the write side's two callers. Each read's
    /// body is passed in whole, so the retry re-runs the read from its CONNECTION OPEN outward rather than
    /// re-executing a half-consumed command: the multi-statement reads (database state's four maintenance
    /// statements ahead of the deviation read; the anomalous-jobs freshness probe ahead of its page) are
    /// re-run from the top, which is sound because every one of those statements is idempotent by
    /// construction — they are <c>INSERT ... ON CONFLICT DO NOTHING</c>, an <c>UPDATE</c> to a fixed value,
    /// a <c>DELETE</c> of rows absent from the newest snapshot, and reads. That property is what makes a
    /// whole-read retry safe here and is NOT a general licence: it holds because this type performs no
    /// accumulating write at all, which is the same test
    /// <see cref="StoreWriteReattempt.IsSafeToReattempt"/> applies one seam over and answers differently.
    /// The store-write precedent is deliberately not reused wholesale — its <c>StoreCopyPhase</c> conjunct
    /// is about a COPY's exactly-once property and has no meaning for a read.</para>
    ///
    /// <para><b>What counts as a command timeout, MEASURED against Npgsql 10.0.3 rather than assumed.</b>
    /// A client-side <c>CommandTimeout</c> expiry arrives as an <see cref="NpgsqlException"/> whose
    /// <see cref="Exception.InnerException"/> is a <see cref="TimeoutException"/> — message "Exception
    /// while reading from stream" over "Timeout during reading attempt", with NO SQLSTATE, which is the
    /// rendering #2826 and #3013 both exist because of. Verified on a rig (postgres:17-alpine,
    /// <c>SELECT pg_sleep(30)</c> at <c>CommandTimeout = 2</c>) in all three places the wait can land:
    /// <c>ExecuteScalarAsync</c>, <c>ExecuteReaderAsync</c>, and <c>ReadAsync</c> mid-stream while rows
    /// are being consumed. All three produce that identical pair, and the connection is left
    /// <c>State = Open</c> and immediately reusable afterwards — measured, including a second
    /// <c>ExecuteReaderAsync</c> on the same connection after a mid-stream kill — with the backend's
    /// statement genuinely cancelled (<c>pg_stat_activity</c> held zero surviving <c>pg_sleep</c> backends
    /// one second later). So the second attempt neither inherits a poisoned connection nor races a
    /// statement that is still running.</para>
    ///
    /// <para><b>And what does NOT, which is the load-bearing half.</b> Three neighbouring shapes were
    /// measured on the same rig, and each is excluded on its own evidence:</para>
    /// <list type="bullet">
    /// <item>The store's OWN <c>statement_timeout</c> cancelling the read arrives as a
    /// <see cref="PostgresException"/>, SQLSTATE <c>57014</c>, with no <see cref="TimeoutException"/>
    /// anywhere in the chain. That is the backend answering, and an identical second attempt gets an
    /// identical answer — <see cref="PostgresTransportFault"/>'s reasoning, and the reason the
    /// <see cref="PostgresException"/> test comes FIRST at every level of the chain walk below.</item>
    /// <item>The pass's stopping token cancelling the read arrives as an
    /// <see cref="OperationCanceledException"/> wrapping a <see cref="PostgresException"/> at
    /// <c>57014</c> ("canceling statement due to user request"). A pre-cancelled token produces a bare
    /// <see cref="OperationCanceledException"/>. Both propagate untouched, and the <c>passToken</c> guard
    /// is checked besides — belt and braces, because a retry that outlives an orderly stop while holding a
    /// sweep permit is the one case where a second attempt is guaranteed useless
    /// (<see cref="StoreWriteReattempt"/>'s reasoning, verbatim).</item>
    /// <item>A CONNECT timeout is an <see cref="NpgsqlException"/> over a
    /// <see cref="TimeoutException"/> too — "Failed to connect to ..." over "Timeout during connection
    /// attempt" — so it is inside this predicate and is retried. That is deliberate and not a leak: a
    /// store that could not be reached inside the connect timeout is exactly a store worth asking again
    /// two seconds later, it is the same transient population, and the cost is bounded by the same
    /// arithmetic. The predicate does not try to tell the two apart because the only discriminator is an
    /// exception MESSAGE, which is what #3013 refuses to key on.</item>
    /// </list>
    ///
    /// <para>Any other exception propagates unchanged, on the first attempt, with no delay: a
    /// <see cref="PostgresException"/> is the store's considered answer and re-asking buys a second round
    /// trip against a backend that just said no, while turning a legible SQLSTATE into a doubled one.</para>
    ///
    /// <para><b>Both truths are counted.</b> A retry that succeeds records
    /// <see cref="AlertReadFailureCounter.RecordRetriedRead"/> and nothing else — the condition WAS judged,
    /// on evidence that arrived late, and the cost stays visible as a count instead of as a blind alert. A
    /// retry that ALSO times out records the retry and then propagates, so the caller's own catch arm
    /// records today's read failure exactly as it did before this change: one failure, not two, and the
    /// caller's elapsed still measures its own last attempt because every counted site restarts its clock
    /// between consecutive awaits. The retry is therefore counted per attempt-PAIR rather than per success,
    /// which is the honest direction — a retry counted only when it worked would understate the write
    /// bands' cost by exactly the episodes where the band was worst.</para>
    /// </summary>
    /// <param name="read">The whole read, run under the caller's token — re-invoked as-is on the retry.</param>
    /// <param name="serverKey">
    /// The alert pass's server key, so a retry lands in the same bucket the read's failure would. Taken as
    /// the caller's raw string (not the parsed <c>server_id</c>) because that is the spelling the counter is
    /// keyed on, and the surface derives its lookup the same way — the silent-zero hazard
    /// <c>AlertReadFailureSurfaceTests</c> pins from source.
    /// </param>
    /// <param name="readName">The read's short constant name, for the counter.</param>
    /// <param name="passToken">
    /// The pass's stopping token. A read cancelled through it is never a retry candidate, and the 2 s wait
    /// honours it — so a service stopping mid-band does not spend two seconds per in-flight read waiting to
    /// re-ask a store it is shutting down.
    /// </param>
    /// <remarks>
    /// <c>internal</c> rather than private so <c>AlertReadRetrySeamTests</c> can drive it with a fake read
    /// that throws the exact exception shapes the rig measured, on a fake delay — the alternative is a pin
    /// that needs a live store to stall on demand, which is the shape that gets skipped in CI and then
    /// stops being evidence. The twelve production callers are all in this file.
    /// </remarks>
    internal async Task<T> ExecuteWithOneRetryAsync<T>(
        Func<CancellationToken, Task<T>> read,
        string serverKey,
        string readName,
        CancellationToken passToken)
    {
        try
        {
            return await read(passToken);
        }
        /* Ahead of the filter rather than relying on it to answer false, exactly as StoreWriteReattempt
           orders its arms: a cancellation must never be reclassified as a retryable stall, and an arm whose
           correctness rests on a predicate NOT matching is one predicate edit away from retrying through a
           shutdown. It also catches the shape the rig measured for a token cancel — an
           OperationCanceledException wrapping PostgresException 57014 — whose inner chain the predicate
           below would otherwise have to reason about at all. */
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception firstAttempt) when (
            !passToken.IsCancellationRequested && IsCommandTimeout(firstAttempt))
        {
            /* Counted HERE, before the second attempt runs, rather than after it returns — so the figure
               means "a read crossed the deadline and was re-asked" whatever the retry then does. Recorded
               after the wait would drop every retry that a shutdown interrupts; recorded only on success
               would understate the bands' cost by exactly the episodes where the band was worst, and the
               surface's own note commits to counting both outcomes here. */
            _readFailures?.RecordRetriedRead(serverKey, readName);

            /* The pause, on the PASS's token: a service stopping mid-band must not spend two seconds per
               in-flight read waiting to re-ask a store it is shutting down. A trip here throws
               TaskCanceledException (measured), which is an OperationCanceledException — so the caller's
               own cancellation arm sees a cancellation, and no second attempt is made. */
            await _delay(TimeSpan.FromSeconds(AlertPassRetryDelaySeconds), passToken);

            /* The SAME read, re-invoked whole, under the SAME deadline its command carries. A failure on
               this attempt propagates: the caller's fault arm records it as today's single read failure,
               which is why a read that stalls twice counts once as a failure and once as a retry. The
               first attempt's exception is not chained onto it — StoreWriteReattempt's reasoning: two
               different faults in one message column is worse than the one that actually ended the read. */
            return await read(passToken);
        }
    }

    /// <summary>
    /// Whether <paramref name="exception"/> is a client-side command (or connect) deadline expiry — the
    /// only fault #3848 retries.
    ///
    /// <para>The chain is WALKED rather than the outermost type tested, because Npgsql wraps: the measured
    /// shape is <see cref="NpgsqlException"/> over <see cref="TimeoutException"/>, and both render as the
    /// same "Exception while reading from stream" text a lost connection does, so neither the outermost
    /// type nor the message can separate them. Same walk shape as
    /// <see cref="PostgresTransportFault.IsTransportFault"/> and deliberately NOT that predicate: the
    /// transport test also admits <see cref="System.Net.Sockets.SocketException"/> and
    /// <see cref="System.IO.IOException"/> — a reset socket and a torn stream — which are connection
    /// faults rather than the read taking too long. Those propagate here, because the population this
    /// retry is sized for is a store that is UP and slow inside a write band, and a broken connection
    /// during an alert pass is a different condition that the count should keep saying out loud.</para>
    ///
    /// <para><b><see cref="PostgresException"/> answers false at EVERY level of the chain</b>, tested
    /// before the timeout test at each step. It means the backend received the statement and replied —
    /// including its own <c>statement_timeout</c> cancelling us at SQLSTATE <c>57014</c>, which the rig
    /// measured as a bare <see cref="PostgresException"/> with no <see cref="TimeoutException"/> in the
    /// chain at all. An identical second attempt gets an identical answer, and retrying past a SQLSTATE
    /// turns a legible error into a doubled one.</para>
    ///
    /// <para>Unlike the transport predicate this does NOT fall back to "any
    /// <see cref="NpgsqlException"/>": a bare one with no <see cref="TimeoutException"/> inside it is some
    /// other client-side fault, and a retry gate that ends in a catch-all would drift into retrying
    /// everything the first time Npgsql introduces a wrapper. The whole value of this gate is that it is
    /// narrow.</para>
    /// </summary>
    internal static bool IsCommandTimeout(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is Npgsql.PostgresException)
            {
                return false;
            }

            if (current is TimeoutException)
            {
                return true;
            }
        }

        return false;
    }

    /* ---------------- blocking (XE-preferred + DMV fallback) ---------------- */

    /// <summary>
    /// The XE blocked-process-report read — Lite's query with the column list trimmed to the
    /// shared alert row's fields (same WHERE / ORDER BY event_time DESC / LIMIT 200 semantics).
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string BlockedProcessReportsSql = @"
SELECT
    event_time,
    database_name,
    blocked_spid,
    blocking_spid,
    wait_time_ms,
    lock_mode,
    blocked_sql_text,
    blocking_sql_text,
    blocked_process_report_xml,
    contentious_object
FROM blocked_process_reports
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY event_time DESC
LIMIT 200";

    /// <summary>
    /// The always-on DMV blocking-snapshot fallback read — Lite's query trimmed the same way
    /// (dmv_blocking_snapshots has no report XML column). Same parameters as
    /// <see cref="BlockedProcessReportsSql"/>.
    /// </summary>
    public const string DmvBlockingSnapshotsSql = @"
SELECT
    event_time,
    database_name,
    blocked_spid,
    blocking_spid,
    wait_time_ms,
    lock_mode,
    blocked_sql_text,
    blocking_sql_text,
    contentious_object
FROM dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY event_time DESC
LIMIT 200";

    public Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetRecentBlockedProcessReportsCoreAsync(serverKey, hoursBack, ct),
            serverKey,
            "blocking",
            cancellationToken);
    }

    private async Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsCoreAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);
        var (startTime, endTime) = Window(hoursBack);

        var items = new List<BlockedProcessAlertRow>();
        var dmvItems = new List<BlockedProcessAlertRow>();

        await using (var connection = await _postgres.OpenConnectionAsync(cancellationToken))
        {
            using (var command = new NpgsqlCommand(BlockedProcessReportsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
            {
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(startTime);
                command.Parameters.AddWithValue(endTime);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    items.Add(new BlockedProcessAlertRow
                    {
                        EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        BlockedSpid = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                        BlockingSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        WaitTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                        LockMode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                        BlockedSqlText = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        BlockingSqlText = reader.IsDBNull(7) ? "" : reader.GetString(7),
                        BlockedProcessReportXml = reader.IsDBNull(8) ? "" : reader.GetString(8),
                        ContentiousObject = reader.IsDBNull(9) ? "" : reader.GetString(9)
                    });
                }
            }

            using (var command = new NpgsqlCommand(DmvBlockingSnapshotsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
            {
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(startTime);
                command.Parameters.AddWithValue(endTime);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    dmvItems.Add(new BlockedProcessAlertRow
                    {
                        EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        BlockedSpid = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                        BlockingSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        WaitTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                        LockMode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                        BlockedSqlText = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        BlockingSqlText = reader.IsDBNull(7) ? "" : reader.GetString(7),
                        ContentiousObject = reader.IsDBNull(8) ? "" : reader.GetString(8),
                        Source = BlockedProcessAlertRow.DmvSnapshotSource
                    });
                }
            }
        }

        /* Lite's XE-preferred fallback semantics, verbatim via the shared merge: keep all BPR
           rows; append a DMV row only where no BPR covers the same SPID pair in the same minute;
           re-cap to the 200 newest. */
        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmvItems);

        return items;
    }

    /* ---------------- current blocking wait (#1839) ---------------- */

    /// <summary>
    /// Lite's latest-blocking-snapshot sum, ported dialect-for-dialect: ONE snapshot selected by
    /// <c>collection_time = MAX(collection_time)</c> (never a window — see
    /// <see cref="CurrentBlockingWaitResult"/>), its <c>wait_time_ms</c> summed and its distinct
    /// blocked SPIDs counted. $1 server_id, used twice.
    /// <para>
    /// ONE statement, deliberately: Npgsql fails SILENTLY on multi-statement commands with positional
    /// parameters, so the freshness probe cannot be batched onto this — the snapshot time comes back as
    /// a column of this same aggregate instead, which is one round trip rather than two anyway.
    /// </para>
    /// </summary>
    public const string CurrentBlockingWaitSql = @"
SELECT
    collection_time,
    CAST(COALESCE(SUM(wait_time_ms), 0) AS bigint) AS total_wait_ms,
    CAST(COUNT(DISTINCT blocked_spid) AS integer) AS blocked_sessions
FROM dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM dmv_blocking_snapshots
    WHERE server_id = $1
)
GROUP BY collection_time";

    public Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetCurrentBlockingWaitCoreAsync(serverKey, ct),
            serverKey,
            "blocking wait time",
            cancellationToken);
    }

    private async Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitCoreAsync(
        string serverKey, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(CurrentBlockingWaitSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        /* The SQL casts pin both aggregates to bigint/integer, so these read directly — PG's SUM(bigint)
           is numeric and COUNT is bigint, neither of which Npgsql would hand back as long/int untyped. */
        var snapshotTime = reader.GetDateTime(0);
        var totalWaitMs = reader.IsDBNull(1) ? 0L : reader.GetInt64(1);
        var blockedSessions = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

        /* #1812 freshness, same rule as Lite's adapter — parity is the point. The stored times are
           naive UTC, so they compare directly against DateTime.UtcNow. */
        var cadence = ResolveCadence(_blockingSnapshotCadenceMinutes, serverId, "dmv_blocking_snapshot");
        bool isFresh = DateTime.UtcNow - snapshotTime <= CurrentBlockingWaitResult.MaxSnapshotAge(cadence);

        /* #3653 (A5): the cadence rides along so the engine's persistence gate can tell a skipped quiet
           collection from an adjacent one — the same resolved number the freshness bound was taken at. */
        return new CurrentBlockingWaitResult(snapshotTime, totalWaitMs, blockedSessions, isFresh, cadence);
    }

    /* ---------------- deadlocks ---------------- */

    /// <summary>Lite's deadlock read (column list trimmed to the shared alert row's fields).</summary>
    public const string DeadlocksSql = @"
SELECT
    victim_process_id,
    victim_sql_text,
    deadlock_graph_xml
FROM deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY deadlock_time DESC
LIMIT 50";

    public Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetRecentDeadlocksCoreAsync(serverKey, hoursBack, ct),
            serverKey,
            "deadlocks",
            cancellationToken);
    }

    private async Task<List<DeadlockAlertRow>> GetRecentDeadlocksCoreAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);
        var (startTime, endTime) = Window(hoursBack);

        var items = new List<DeadlockAlertRow>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(DeadlocksSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(startTime);
        command.Parameters.AddWithValue(endTime);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DeadlockAlertRow
            {
                VictimProcessId = reader.IsDBNull(0) ? "" : reader.GetString(0),
                VictimSqlText = reader.IsDBNull(1) ? "" : reader.GetString(1),
                DeadlockGraphXml = reader.IsDBNull(2) ? "" : reader.GetString(2)
            });
        }

        return items;
    }

    /* ---------------- poison waits ---------------- */

    /// <summary>
    /// Accumulated poison wait per wait type over the alert's window (#3539 A4) — the SQL Server twin of
    /// <see cref="DarlingPostgresAlertReadAdapter.PoisonWaitSql"/>, over <c>wait_stats</c>. $1 server_id,
    /// $2 the parameterized naive-UTC window floor (now minus <see cref="PoisonWaitEvaluator.WindowMinutes"/>;
    /// the parameterization style the long-running-query twin copies — a bare <c>now()</c> is timestamptz
    /// and would compare in the server's time zone against these naive-UTC columns).
    ///
    /// <para><b>What changed from the retired read and why.</b> The old text selected the newest three rows
    /// with <c>delta_waiting_tasks &gt; 0</c> and let the engine judge each row's avg-ms-per-wait; this one
    /// SUMs every row in the window per wait type and applies no threshold. Three consequences, each
    /// deliberate: there is no <c>LIMIT</c>, because a limit on a sum is an undercount; the
    /// <c>delta_waiting_tasks &gt; 0</c> filter is gone, because the measured fleet holds THREADPOOL rows with
    /// hundreds of ms of wait and ZERO completed tasks (a task still waiting across the interval boundary)
    /// and that wait is evidence — the old filter dropped it; and every wait type with any row in the window
    /// comes back whatever its sum, because the engine needs "observed and quiet" as an answer distinct
    /// from "not observed" (an empty result holds a standing alert open; a sub-bar row clears it).</para>
    ///
    /// <para><b>(0, 0) rows.</b> The delta calculator writes <c>delta_wait_time_ms = 0, delta_waiting_tasks
    /// = 0</c> both for a genuinely idle interval and for "no delta is knowable here" (first sighting, counter
    /// reset, a gap past the policy) — the two are indistinguishable in this table today. A SUM treats them
    /// identically and correctly: zero contributes nothing to the accumulated wait, and the evaluator's
    /// denominator is the window's wall clock, not a row count, so a fabricated zero neither inflates nor
    /// deflates the figure. It does count toward <c>observed_intervals</c>, which is honest for the one
    /// question that column answers — did the collector deliver a row — and is why that column is not a
    /// "known quiet" claim. Deltas are never negative (the calculator returns 0 on a reset), so no floor is
    /// applied; a defensive one would only hide a calculator regression.</para>
    ///
    /// <para><b>Cost, under the alert pass's 10-second read deadline while the hourly CAGG refresh runs</b>
    /// (#3597: 4–7 minutes on the largest store). Cheap by SHAPE, not by any index the planner may or may not
    /// pick: the WHERE is predicate-identical to the retired read's — one server_id, the three wait_type
    /// literals, a collection_time floor ten minutes back — so it touches exactly the rows that read touched
    /// and aggregates them instead of ordering them for a <c>LIMIT 3</c>. At the wait_stats collector's
    /// one-minute cadence that is at most ~10 collections × 3 types ≈ 30 rows per server, all inside the
    /// last ten minutes of the current one-day chunk (uncompressed head; chunk exclusion keeps compressed
    /// history out of the plan). The PostgreSQL twin (<see cref="DarlingPostgresAlertReadAdapter.PoisonWaitSql"/>)
    /// has run this exact aggregate shape over pg_wait_stats under the same deadline since #2711.</para>
    ///
    /// <para>The V128 keystone lane is adding <c>sample_interval_seconds</c> to this table and may later
    /// guard readers on it; this text deliberately carries no interval handling so that lane can rebase
    /// onto it cleanly.</para>
    /// </summary>
    public const string PoisonWaitsSql = @"
SELECT
    wait_type,
    SUM(delta_wait_time_ms)::bigint AS accumulated_wait_ms,
    SUM(delta_waiting_tasks)::bigint AS accumulated_waits,
    COUNT(*)::bigint AS observed_intervals,
    MAX(collection_time) AS newest_collection_time
FROM wait_stats
WHERE server_id = $1
AND wait_type IN ('THREADPOOL', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE')
AND collection_time >= $2
GROUP BY wait_type
ORDER BY accumulated_wait_ms DESC";

    public Task<List<PoisonWaitAccumulation>> GetPoisonWaitAccumulationAsync(
        string serverKey, int windowMinutes, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetPoisonWaitAccumulationCoreAsync(serverKey, windowMinutes, ct),
            serverKey,
            "poison waits",
            cancellationToken);
    }

    private async Task<List<PoisonWaitAccumulation>> GetPoisonWaitAccumulationCoreAsync(
        string serverKey, int windowMinutes, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<PoisonWaitAccumulation>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(PoisonWaitsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        /* The engine's window, not a local recency constant: the window IS the denominator the bars
           normalize against, so read and evaluation must agree on it or the "average tasks stuck"
           arithmetic silently means something else (the PostgreSQL twin's reasoning, verbatim). */
        command.Parameters.AddWithValue(NaiveUtcNow().AddMinutes(-windowMinutes));

        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new PoisonWaitAccumulation(
                    reader.GetString(0),
                    reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    reader.IsDBNull(4) ? DateTime.MinValue : reader.GetDateTime(4)));
            }
        }

        return items;
    }

    /* ---------------- long-running queries ---------------- */

    /// <summary>
    /// Lite's long-running-query read with the two PG dialect adjustments: DuckDB's bare
    /// <c>NOW() - INTERVAL '10 MINUTES'</c> becomes the parameterized naive-UTC $4 (a bare
    /// <c>now()</c> is timestamptz — wrong basis against naive-UTC timestamp columns), and the
    /// N'' literals in the opt-out filters (<c>{0}</c> placeholder) lose their N prefix.
    /// $1 server_id, $2 elapsed-ms threshold, $3 max results, $4 staleness floor; <c>{1}</c> and <c>{2}</c> are
    /// the #3653 (A5, Q5) opt-out knob's two arm expressions and <c>{3}</c> is the shared <c>excludedDatabases</c>
    /// list's (#3742) — the shared
    /// <see cref="LongRunningQueryExclusions.BuildSqlPredicates(string, string, string, IReadOnlyList{string}, int)"/>
    /// text (the literal <c>FALSE</c> for an arm with no entries) whose operands bind as $5 onward, program
    /// prefixes first, then logins, then databases.
    ///
    /// <para><b>Why a CTE and three flags rather than one more <c>AND NOT</c>.</b> The knob is applied ahead of
    /// <c>LIMIT</c> (the sessions it removes are the longest-running by construction and would otherwise fill
    /// the cap), and the fire payload wants to know how many it removed and WHICH ARM did it. Flagging every
    /// over-threshold row once per arm lets the outer query keep the unflagged ones under the cap AND count the
    /// flagged ones as uncorrelated scalars over the same CTE — one statement (Npgsql positional parameters
    /// do not survive a batch), one snapshot, so the rows and the counts describe the same instant. The counts
    /// are DISTINCT <c>session_id</c>s, not rows: the read looks at one collection of one server, where the
    /// (server_id, session_id, tran_start_time) session identity collapses to <c>session_id</c>, and a MARS
    /// session with two request rows is one session. The login count is taken <c>AND NOT</c> the program flag so
    /// a session matching both arms counts once, under the program prefix. The outer alias is <c>r</c> like the
    /// inner's so the pins on this text keep reading.</para>
    ///
    /// <para><b>#3742: <c>excludedDatabases</c> is the third flag, for the same reason.</b> Until #3742 this
    /// read applied the database list in C#, AFTER the cap — the very shape the knob's paragraph above refuses
    /// — so an excluded database whose ETL held the five longest sessions consumed the page and the alert came
    /// back short or empty while un-excluded long-running sessions existed. The list now rides the same CTE
    /// as <c>excluded_by_database</c>, the outer <c>WHERE</c> drops it with the knob's two, and its count is
    /// taken <c>AND NOT</c> both knob flags — the database arm is LAST, so a session the knob would have
    /// removed anyway is the knob's whichever database it ran in, and the three counts sum to the sessions
    /// removed (see the builder's doc for why last and not first).</para>
    /// </summary>
    public const string LongRunningQueriesSqlTemplate = @"
WITH candidates AS (
    SELECT
        r.session_id,
        r.database_name,
        SUBSTRING(r.query_text, 1, 300) AS query_text,
        r.total_elapsed_time_ms / 1000 AS elapsed_seconds,
        r.cpu_time_ms,
        r.reads,
        r.writes,
        r.wait_type,
        r.blocking_session_id,
        r.query_hash,
        r.program_name,
        r.login_name,
        r.total_elapsed_time_ms,
        {1} AS excluded_by_program_prefix,
        {2} AS excluded_by_login,
        {3} AS excluded_by_database
    FROM query_snapshots AS r
    WHERE r.server_id = $1
        AND r.collection_time = (SELECT MAX(vqs.collection_time) FROM query_snapshots AS vqs WHERE vqs.server_id = $1)
        AND r.collection_time >= $4
        AND r.session_id > 50
        {0}
        AND r.total_elapsed_time_ms >= $2
)
SELECT
    r.session_id,
    r.database_name,
    r.query_text,
    r.elapsed_seconds,
    r.cpu_time_ms,
    r.reads,
    r.writes,
    r.wait_type,
    r.blocking_session_id,
    r.query_hash,
    r.program_name,
    r.login_name,
    CAST((SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_program_prefix) AS integer) AS excluded_by_program_prefix_count,
    CAST((SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_login AND NOT x.excluded_by_program_prefix) AS integer) AS excluded_by_login_count,
    CAST((SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_database AND NOT (x.excluded_by_program_prefix OR x.excluded_by_login)) AS integer) AS excluded_by_database_count
FROM candidates AS r
WHERE NOT (r.excluded_by_program_prefix OR r.excluded_by_login OR r.excluded_by_database)
ORDER BY r.total_elapsed_time_ms DESC
LIMIT $3";

    /// <summary>The five opt-out noise filters — Lite's clauses with the N'' prefixes dropped.</summary>
    /* sp_server_diagnostics (the AG/FCI health-check session) usually sits in SP_SERVER_DIAGNOSTICS_SLEEP, but it
       also does Extended Events work, so it can be captured in a different wait (e.g. PREEMPTIVE_XE_GETTARGETSTATE)
       where the wait-type match alone misses it and the Long-Running Query alert fires anyway. The query-text
       match (case-insensitive, NULL-safe) catches it regardless of the wait it happens to be in at capture time. */
    public const string SpServerDiagnosticsFilter =
        "AND r.wait_type NOT LIKE '%SP_SERVER_DIAGNOSTICS%'\n    AND (r.query_text IS NULL OR r.query_text NOT ILIKE '%sp_server_diagnostics%')";
    public const string WaitForFilter = "AND r.wait_type NOT IN ('WAITFOR', 'BROKER_RECEIVE_WAITFOR')";
    public const string BackupsFilter = "AND r.wait_type NOT IN ('BACKUPTHREAD', 'BACKUPIO')";
    public const string MiscWaitsFilter = "AND r.wait_type NOT IN ('XE_LIVE_TARGET_TVF')";
    public const string CdcFilter = "AND COALESCE(r.is_cdc_capture, FALSE) = FALSE";

    public Task<LongRunningQueryReadResult> GetLongRunningQueriesAsync(
        string serverKey,
        int thresholdMinutes,
        int maxResults,
        bool excludeSpServerDiagnostics,
        bool excludeWaitFor,
        bool excludeBackups,
        bool excludeMiscWaits,
        bool excludeCdc,
        IReadOnlyList<string> excludedDatabases,
        LongRunningQueryExclusions exclusions,
        CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetLongRunningQueriesCoreAsync(serverKey, thresholdMinutes, maxResults, excludeSpServerDiagnostics, excludeWaitFor, excludeBackups, excludeMiscWaits, excludeCdc, excludedDatabases, exclusions, ct),
            serverKey,
            "long-running queries",
            cancellationToken);
    }

    private async Task<LongRunningQueryReadResult> GetLongRunningQueriesCoreAsync(
        string serverKey,
        int thresholdMinutes,
        int maxResults,
        bool excludeSpServerDiagnostics,
        bool excludeWaitFor,
        bool excludeBackups,
        bool excludeMiscWaits,
        bool excludeCdc,
        IReadOnlyList<string> excludedDatabases,
        LongRunningQueryExclusions exclusions,
        CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);
        var thresholdMs = (long)thresholdMinutes * 60 * 1000;
        maxResults = Math.Clamp(maxResults, 1, 1000);
        ArgumentNullException.ThrowIfNull(exclusions);

        var filters = string.Join("\n    ", new[]
        {
            excludeSpServerDiagnostics ? SpServerDiagnosticsFilter : "",
            excludeWaitFor ? WaitForFilter : "",
            excludeBackups ? BackupsFilter : "",
            excludeMiscWaits ? MiscWaitsFilter : "",
            excludeCdc ? CdcFilter : ""
        }.Where(f => f.Length > 0));

        /* #3653 (A5, Q5): the opt-out knob's two arms, operands binding as $5 onward after the four fixed
           parameters (program prefixes first, then logins — the builder's order is the binding order); #3742:
           the shared excludedDatabases list is the third arm, its operands appended after the logins, so it
           is applied in this statement ahead of LIMIT and no ordinal the knob bound before it existed moves. */
        var exclusionSql = exclusions.BuildSqlPredicates("r.program_name", "r.login_name", "r.database_name", excludedDatabases, firstParameterOrdinal: 5);
        var sql = LongRunningQueriesSqlTemplate
            .Replace("{0}", filters)
            .Replace("{1}", exclusionSql.ProgramPrefixPredicate)
            .Replace("{2}", exclusionSql.LoginPredicate)
            .Replace("{3}", exclusionSql.DatabasePredicate);

        var items = new List<LongRunningQueryInfo>();
        int excludedByProgramPrefix = 0;
        int excludedByLogin = 0;
        int excludedByDatabase = 0;
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(thresholdMs);
        command.Parameters.AddWithValue(maxResults);
        command.Parameters.AddWithValue(NaiveUtcNow().AddMinutes(-10));
        foreach (var operand in exclusionSql.Operands)
        {
            command.Parameters.AddWithValue(operand);
        }

        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new LongRunningQueryInfo
                {
                    SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                    DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    ElapsedSeconds = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    CpuTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    Reads = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                    Writes = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    WaitType = reader.IsDBNull(7) ? null : reader.GetString(7),
                    BlockingSessionId = reader.IsDBNull(8) ? null : (int?)reader.GetInt32(8),
                    QueryHash = reader.IsDBNull(9) ? null : reader.GetString(9),
                    ProgramName = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    LoginName = reader.IsDBNull(11) ? "" : reader.GetString(11)
                });
                /* The same three scalars on every row — read once is enough; a read with no rows has nothing to
                   fire and therefore nothing to render the counts beside. */
                excludedByProgramPrefix = reader.IsDBNull(12) ? 0 : reader.GetInt32(12);
                excludedByLogin = reader.IsDBNull(13) ? 0 : reader.GetInt32(13);
                excludedByDatabase = reader.IsDBNull(14) ? 0 : reader.GetInt32(14);
            }
        }

        /* #3742: no post-read database filter here any more — the list is the CTE's third flag above, applied
           ahead of LIMIT, so the page is a page of MATCHES and the rows it removed are counted, not silently
           consumed. A `.Where` on items at this point would be the defect coming back. */
        return new LongRunningQueryReadResult(items, excludedByProgramPrefix, excludedByLogin, excludedByDatabase);
    }

    /* ---------------- database file growth (#2349) ---------------- */

    /// <summary>
    /// Per-file current size, growth over the lookback window, and the file's volume.
    ///
    /// <para><b>Newest per file, and a baseline from the window's far edge.</b> <c>DISTINCT ON</c> takes the
    /// current row per (database, file); the baseline join takes the OLDEST sample inside the window for the
    /// same key. Growth is the difference, and is 0 when the window holds a single sample — which reads as "no
    /// rise observed" rather than as a rise of the whole file, the wrong answer for a server that just started
    /// collecting.</para>
    ///
    /// <para>Both sides are bounded on <c>collection_time</c>, the partitioning column, so the window prunes
    /// chunks rather than scanning retention. The reported window width is measured rather than assumed, so a
    /// gap in collection cannot make a slow rise look fast.</para>
    ///
    /// <para><b>The newest sample's <c>collection_time</c> travels with the row (#3636)</b> as <c>observed_at</c>,
    /// the last column. The growth is a fact about two COLLECTIONS and reads byte-identical on every alert pass
    /// until the next collection lands — and this collector's cadence is an HOUR against a 5-minute cooldown,
    /// so the engine needs the observation's identity to fire the rise gate once per collection instead of up to
    /// twelve times. <c>current_files</c> already carried <c>collection_time</c> for the window-width arithmetic;
    /// it was simply never projected. Appended rather than inserted so the fourteen ordinals the reader already
    /// binds do not move. #3579's <c>observed_at</c> on the forced-plan read is the same column for the same
    /// reason.</para>
    ///
    /// <para>$1 server_id, $2 window start (naive UTC).</para>
    /// </summary>
    public const string DatabaseFileGrowthSql = @"
WITH current_files AS (
    SELECT DISTINCT ON (database_name, file_name)
        database_name, file_name, physical_name, file_type_desc, collection_time,
        total_size_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb,
        volume_mount_point, volume_total_mb, volume_free_mb
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    ORDER BY database_name, file_name, collection_time DESC
),
baseline AS (
    SELECT DISTINCT ON (database_name, file_name)
        database_name, file_name, collection_time, total_size_mb
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    ORDER BY database_name, file_name, collection_time ASC
)
SELECT
    c.database_name,
    c.file_name,
    COALESCE(c.physical_name, '') AS physical_name,
    COALESCE(c.file_type_desc, '') AS file_type_desc,
    COALESCE(c.total_size_mb, 0) AS total_size_mb,
    COALESCE(c.total_size_mb, 0) - COALESCE(b.total_size_mb, c.total_size_mb, 0) AS growth_mb,
    COALESCE(EXTRACT(EPOCH FROM (c.collection_time - b.collection_time)) / 60.0, 0) AS growth_window_minutes,
    COALESCE(c.volume_mount_point, '') AS volume_mount_point,
    COALESCE(c.volume_total_mb, 0) AS volume_total_mb,
    COALESCE(c.volume_free_mb, 0) AS volume_free_mb,
    c.auto_growth_mb,
    COALESCE(c.is_percent_growth, false) AS is_percent_growth,
    c.growth_pct,
    c.max_size_mb,
    c.collection_time AS observed_at
FROM current_files c
LEFT JOIN baseline b
  ON  b.database_name = c.database_name
  AND b.file_name = c.file_name
WHERE c.total_size_mb IS NOT NULL
ORDER BY c.database_name, c.file_name";

    public Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(
        string serverKey, int lookbackMinutes, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetDatabaseFileGrowthCoreAsync(serverKey, lookbackMinutes, ct),
            serverKey,
            "database file growth",
            cancellationToken);
    }

    private async Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthCoreAsync(
        string serverKey, int lookbackMinutes, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);
        var windowStart = DateTime.SpecifyKind(
            DateTime.UtcNow.AddMinutes(-Math.Max(1, lookbackMinutes)), DateTimeKind.Unspecified);

        var items = new List<DatabaseFileGrowthInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(DatabaseFileGrowthSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(windowStart);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseFileGrowthInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                PhysicalName = reader.GetString(2),
                FileTypeDesc = reader.GetString(3),
                TotalSizeMb = Convert.ToDouble(reader.GetValue(4)),
                GrowthMb = Convert.ToDouble(reader.GetValue(5)),
                GrowthWindowMinutes = Convert.ToDouble(reader.GetValue(6)),
                VolumeMountPoint = reader.GetString(7),
                VolumeTotalMb = Convert.ToDouble(reader.GetValue(8)),
                VolumeFreeMb = Convert.ToDouble(reader.GetValue(9)),
                AutoGrowthMb = reader.IsDBNull(10) ? null : Convert.ToDouble(reader.GetValue(10)),
                IsPercentGrowth = !reader.IsDBNull(11) && reader.GetBoolean(11),
                GrowthPct = reader.IsDBNull(12) ? null : Convert.ToDouble(reader.GetValue(12)),
                MaxSizeMb = reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13)),
                /* #3636: the stored value is naive UTC (the $2 window bound above is built the same way), read
                   back with Kind Unspecified; stamped Utc because that is what it IS and what the property's name
                   says. The engine only ever compares one file's stamps with each other, so the Kind is honesty
                   rather than arithmetic — #3579's forced-plan read does exactly this. */
                ObservedAtUtc = reader.IsDBNull(14) ? null : DateTime.SpecifyKind(reader.GetDateTime(14), DateTimeKind.Utc),
            });
        }

        return items;
    }

    /* ---------------- volume free space ---------------- */

    /// <summary>Lite's per-volume free-space read verbatim (database_size_stats table).</summary>
    public const string VolumeFreeSpaceSql = @"
SELECT
    volume_mount_point,
    MAX(volume_total_mb) AS volume_total_mb,
    MIN(volume_free_mb) AS volume_free_mb
FROM database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM database_size_stats
    WHERE server_id = $1
)
AND   volume_mount_point IS NOT NULL
AND   volume_total_mb > 0
GROUP BY volume_mount_point
ORDER BY MIN(volume_free_mb) / MAX(volume_total_mb)";

    public Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetVolumeFreeSpaceCoreAsync(serverKey, ct),
            serverKey,
            "volume free space",
            cancellationToken);
    }

    private async Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceCoreAsync(
        string serverKey, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<VolumeFreeSpaceInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(VolumeFreeSpaceSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new VolumeFreeSpaceInfo
            {
                MountPoint = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                FreeMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }

        return items;
    }

    /* ---------------- persistent version store (#1984) ---------------- */

    /// <summary>
    /// The newest pvs_stats snapshot's ADR databases, worst (highest PVS share) first — the
    /// latest-collection convention <see cref="VolumeFreeSpaceSql"/> uses. ADR-OFF rows are
    /// excluded here rather than engine-side: a database that cannot have a PVS cannot breach,
    /// and every collected server carries system databases with ADR off.
    /// </summary>
    public const string PvsPressureSql = @"
SELECT
    database_name,
    persistent_version_store_size_mb,
    database_data_size_mb,
    current_aborted_transaction_count,
    oldest_active_transaction_id,
    oldest_aborted_transaction_id,
    aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time
FROM pvs_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM pvs_stats
    WHERE server_id = $1
)
AND   is_accelerated_database_recovery_on
ORDER BY
    CASE WHEN database_data_size_mb > 0
         THEN persistent_version_store_size_mb / database_data_size_mb
         ELSE 0 END DESC";

    public Task<List<PvsPressureInfo>> GetPvsPressureAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetPvsPressureCoreAsync(serverKey, ct),
            serverKey,
            "PVS pressure",
            cancellationToken);
    }

    private async Task<List<PvsPressureInfo>> GetPvsPressureCoreAsync(
        string serverKey, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<PvsPressureInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(PvsPressureSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PvsPressureInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                PvsSizeMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                DatabaseDataSizeMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                CurrentAbortedTransactionCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                OldestActiveTransactionId = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                OldestAbortedTransactionId = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                /* MS's documented shape for "cleanup is ongoing": a start time with no end time. */
                AbortedCleanupOngoing = !reader.IsDBNull(6) && reader.IsDBNull(7)
            });
        }

        return items;
    }

    /* ---------------- tempdb ---------------- */

    /// <summary>Lite's latest-tempdb-snapshot read verbatim (tempdb_stats table). <c>collection_time</c> rides
    /// as the last column since #3653 (A5): it is the row the read already orders by, projected so the engine's
    /// persistence gate can tell a fresh collection from a re-read of the last one — see
    /// <see cref="TempDbSpaceInfo.CollectionTimeUtc"/>. Same shape as before: one row, one index seek.</summary>
    public const string TempDbSpaceSql = @"
SELECT
    total_reserved_mb,
    unallocated_mb,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    top_session_tempdb_mb,
    top_session_id,
    max_size_mb,
    collection_time
FROM tempdb_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    public Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetTempDbSpaceCoreAsync(serverKey, ct),
            serverKey,
            "TempDB space",
            cancellationToken);
    }

    private async Task<TempDbSpaceInfo?> GetTempDbSpaceCoreAsync(
        string serverKey, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(TempDbSpaceSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new TempDbSpaceInfo
            {
                TotalReservedMb = reader.IsDBNull(0) ? 0 : ToDouble(reader.GetValue(0)),
                UnallocatedMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                UserObjectReservedMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                InternalObjectReservedMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                VersionStoreReservedMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TopConsumerMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TopConsumerSessionId = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                /* NULL on every row collected before the V81 rung, and 0 is what "no ceiling measured"
                   is spelled as — so history keeps reporting the percentage it always did rather than
                   dividing by a zero cap. */
                MaxSizeMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                /* #3653 (A5): naive UTC off the `timestamp` column, read back Kind Unspecified and stamped Utc
                   because that is what it IS and what the property's name says — the #3636 file-growth read's
                   idiom. The engine only ever compares one server's stamps with each other, so the Kind is
                   honesty rather than arithmetic. */
                CollectionTimeUtc = reader.IsDBNull(8) ? null : DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Utc)
            };
        }

        return null;
    }

    /* ---------------- anomalous jobs ---------------- */

    /// <summary>
    /// Lite's anomalous-jobs read verbatim (running_jobs table). $2 is the threshold percent
    /// (multiplier x 100, as numeric — percent_of_average is numeric(10,1)).
    /// <para><c>start_time</c> is the monitored server's own clock, so the server's collected UTC offset
    /// is projected beside it — NOT <c>COALESCE(..., 0)</c>: the alert body renders an absent offset as an
    /// explicitly unconverted server-clock instant rather than as UTC (see
    /// <c>AlertTimestamp</c>), and coalescing here would take that choice away from it. Rides on this
    /// statement rather than a second command so the pass's command count is unchanged.</para>
    /// </summary>
    public const string AnomalousJobsSql = @"
SELECT
    job_name,
    job_id,
    current_duration_seconds,
    avg_duration_seconds,
    p95_duration_seconds,
    percent_of_average,
    start_time,
    (
        SELECT sp.utc_offset_minutes
        FROM server_properties AS sp
        WHERE sp.server_id = $1
        AND   sp.utc_offset_minutes IS NOT NULL
        ORDER BY sp.collection_time DESC
        LIMIT 1
    ) AS utc_offset_minutes
FROM running_jobs
WHERE server_id = $1
AND collection_time = (SELECT MAX(collection_time) FROM running_jobs WHERE server_id = $1)
AND avg_duration_seconds >= 60
AND percent_of_average >= $2
ORDER BY percent_of_average DESC
LIMIT 5";

    public Task<AnomalousJobsResult> GetAnomalousJobsAsync(
        string serverKey, int multiplier, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetAnomalousJobsCoreAsync(serverKey, multiplier, ct),
            serverKey,
            "anomalous jobs",
            cancellationToken);
    }

    private async Task<AnomalousJobsResult> GetAnomalousJobsCoreAsync(
        string serverKey, int multiplier, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);
        var thresholdPercent = (decimal)(multiplier * 100);

        var items = new List<AnomalousJobInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        /* #1812: the latest snapshot is only evidence when fresh — a stopped collector, missed cycles,
           or lost msdb access leaves a stale "latest" that would otherwise read as NOW, and the engine's
           per-run cooldown key expires each pass, so a stale snapshot re-fires the same historical run
           every cooldown, forever. Same rule as Lite's adapter; parity is the point. */
        using (var snapshotProbe = new NpgsqlCommand(
            "SELECT MAX(collection_time) FROM running_jobs WHERE server_id = $1", connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            snapshotProbe.Parameters.AddWithValue(serverId);
            var snapshot = await snapshotProbe.ExecuteScalarAsync(cancellationToken);
            var cadence = ResolveRunningJobsCadence(serverId);
            if (snapshot is not DateTime snapshotTime
                || DateTime.UtcNow - snapshotTime > AnomalousJobsResult.MaxSnapshotAge(cadence))
            {
                return AnomalousJobsResult.Stale;
            }
        }

        using var command = new NpgsqlCommand(AnomalousJobsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(thresholdPercent);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new AnomalousJobInfo
            {
                JobName = reader.GetString(0),
                JobId = reader.IsDBNull(1) ? "" : reader.GetString(1),
                CurrentDurationSeconds = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                AvgDurationSeconds = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                P95DurationSeconds = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                PercentOfAverage = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                StartTime = reader.IsDBNull(6) ? DateTime.MinValue : reader.GetDateTime(6),
                UtcOffsetMinutes = reader.IsDBNull(7) ? null : reader.GetInt32(7)
            });
        }

        return new AnomalousJobsResult(SnapshotIsFresh: true, items);
    }

    /* ---------------- database state (baseline deviation) ---------------- */

    /// <summary>
    /// Seeds a first-observation baseline for any database in the latest snapshot that has none
    /// (insert-if-absent; never overwrites an existing baseline or user override). A first observation in
    /// an integrity or transient state is deliberately NOT baselined
    /// (<see cref="DatabaseStateTokens.NeverBaselinedSqlList"/>): onboarding a server mid-outage or
    /// mid-restore must not learn that state as expected — such a database stays pending (no row) until it
    /// settles into a steady state, and the deviation read alerts meanwhile only if the state is critical.
    /// config schema qualified explicitly; database_states resolves to collect through the search_path.
    /// $1 server_id.
    /// </summary>
    public const string SeedDatabaseStateExpectedSql = $@"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END, false, (now() AT TIME ZONE 'UTC')
FROM database_states ds
WHERE ds.server_id = $1
AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
AND   ds.state_desc IS NOT NULL
AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) NOT IN ({DatabaseStateTokens.NeverBaselinedSqlList})
ON CONFLICT (server_id, database_name) DO NOTHING";

    /// <summary>
    /// #2189: re-learns an ILLEGITIMATE inferred baseline as ONLINE once the database reaches ONLINE. The
    /// rule is the seed's own, applied after the fact — an expectation recording a state the seed would
    /// refuse to learn (<see cref="DatabaseStateTokens.NeverBaselinedSqlList"/>) is not a baseline at all,
    /// it is a snapshot of a database mid-something, and the moment that database is demonstrably healthy
    /// the honest move is to learn the steady state rather than page about the improvement forever.
    ///
    /// <para>This is what heals the rows the old seed already poisoned, which the widened exclusion above
    /// cannot: a database baselined RESTORING mid-restore deviated by being healthy forever, and the only
    /// escape was an operator noticing and re-baselining by hand. It also covers the route that is still
    /// open and always will be — "reset to current" pressed during a restore, or during an outage, records
    /// whatever it sees with no state filter at all, and this un-writes it on the next sweep.</para>
    ///
    /// <para>Two gates, and both matter more than they look.</para>
    ///
    /// <para><c>is_user_override = false</c>: an operator who declared an expected state MEANT it, and
    /// #2166's composition contract depends on that — a database parked at expected OFFLINE stays silent
    /// while parked and still alerts the moment it comes back ONLINE. Only the machine's own inference is
    /// second-guessed, never the operator's.</para>
    ///
    /// <para>The state list, which is deliberately NOT "anything that is not ONLINE". OFFLINE and STANDBY
    /// are steady states the seed is happy to learn, and leaving one is real news that must still fire.
    /// A STANDBY secondary that turns up ONLINE has stopped being a secondary — somebody recovered it, log
    /// shipping is broken, and healing it would replace that alert with silence and then fire the moment
    /// the operator FIXED it. An auto-baselined OFFLINE database brought up for an hour and re-parked would
    /// come back deviating forever against a baseline it never had. Both are the reported bug's own shape,
    /// which is why the heal only ever touches states that were never a legitimate baseline.</para>
    ///
    /// <para>Reads the EFFECTIVE state, not <c>state_desc</c> — the same CASE as the seed and the deviation
    /// read. That is load-bearing rather than cosmetic: a standby log-shipping secondary reports
    /// <c>state_desc = 'ONLINE'</c> with <c>is_in_standby</c> set, so matching on the raw column would
    /// re-baseline every such secondary to ONLINE and then alert it forever for being STANDBY — the very
    /// bug being fixed, re-created for the one database family #1986 went out of its way to keep quiet.</para>
    ///
    /// <para>The alerted-state memory is dropped with the baseline it described (#2166). A memory saying
    /// "the operator was told about ONLINE" only meant anything against the stale expectation; carried
    /// past it, it would judge the next episode against an announcement about a baseline that no longer
    /// exists. Clearing is the safe direction — it can cost an extra alert, never a missed one. $1
    /// server_id.</para>
    /// </summary>
    public const string HealDatabaseStateBaselineToOnlineSql = $@"
UPDATE config.database_state_expected e
SET expected_state = 'ONLINE',
    updated_at = (now() AT TIME ZONE 'UTC'),
    last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE e.server_id = $1
AND   e.is_user_override = false
AND   e.expected_state IN ({DatabaseStateTokens.NeverBaselinedSqlList})
AND   EXISTS (
    SELECT 1
    FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
    AND   ds.database_name = e.database_name
    AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) = 'ONLINE'
)";

    /// <summary>
    /// Tidies auto-baselines for databases no longer in the newest snapshot (dropped/renamed); user
    /// overrides are kept. $1 server_id.
    /// </summary>
    public const string PruneDatabaseStateExpectedSql = @"
DELETE FROM config.database_state_expected e
WHERE e.server_id = $1
AND   e.is_user_override = false
AND   NOT EXISTS (
    SELECT 1 FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
    AND   ds.database_name = e.database_name
)";

    /// <summary>
    /// #2166: clears the alerted-state memory for any database the store now shows back AT its expected
    /// state. Runs beside the seed and the prune, on the same connection, for the same reason they do — it
    /// is store maintenance derived from what the store holds, not from anything a process observed.
    ///
    /// <para>That distinction is the whole point. The engine also clears on the falling edge it witnesses,
    /// but that path is reachable only through its in-memory active set, which empties on every restart. A
    /// service restart landing between an alert and the recovery therefore left the persisted
    /// <c>last_alerted_state</c> sticky forever: the database was never in <c>active</c> to be noticed as
    /// recovered, so the next parking read as already-announced and was swallowed. This statement cannot
    /// have that gap, because it asks the store rather than remembering. The engine's clear stays as the
    /// immediate path — a recovery inside one process should not wait for the next cycle's sweep — and this
    /// is what actually owns the invariant.</para>
    ///
    /// <para>One sample at expected is enough, deliberately, where the DEVIATION rule needs two: clearing is
    /// the safe direction (it can only cause an extra alert, never a missed one), and a flap cannot exploit
    /// it because a flap does not survive the two-sample deviation test to alert in the first place. The
    /// "(ignore)" sentinel clears too — an operator silencing a database should not leave a memory behind
    /// that outlives the silence. $1 server_id.</para>
    /// </summary>
    public const string ClearRecoveredDatabaseStateAlertsSql = @"
UPDATE config.database_state_expected e
SET last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE e.server_id = $1
AND   e.last_alerted_state IS NOT NULL
AND   (e.expected_state = '(ignore)'
       OR EXISTS (
           SELECT 1
           FROM database_states ds
           WHERE ds.server_id = $1
           AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
           AND   ds.database_name = e.database_name
           AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) = e.expected_state
       ))";

    /// <summary>
    /// The databases whose state deviates from their expected state in BOTH of the two most recent
    /// collections (a two-sample rule that absorbs restart transients — RECOVERY_PENDING / RECOVERING — and
    /// a standby secondary's per-restore RESTORING flicker), plus databases with no baseline yet whose
    /// effective state is critical in both samples (a pending critical first observation). Uses the
    /// effective state (STANDBY for a log-shipping secondary, else state_desc). Skips the "(ignore)"
    /// sentinel; each row carries current + expected (expected is empty for a pending row). Lite's DuckDB
    /// read ported to Postgres. $1 server_id.
    /// </summary>
    public const string DatabaseStateDeviationsSql = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM database_states WHERE server_id = $1
),
prev AS (
    SELECT MAX(collection_time) AS t FROM database_states
    WHERE server_id = $1 AND collection_time < (SELECT t FROM newest)
),
latest AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM newest)
),
previous AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM prev)
)
SELECT l.database_name, l.eff, COALESCE(e.expected_state, ''), COALESCE(e.last_alerted_state, '')
FROM latest l
JOIN previous p
  ON p.database_name = l.database_name
LEFT JOIN config.database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
WHERE (e.expected_state IS NULL
        AND l.eff IN ({DatabaseStateTokens.CriticalSqlList})
        AND p.eff IN ({DatabaseStateTokens.CriticalSqlList}))
   OR (e.expected_state IS NOT NULL AND e.expected_state <> '(ignore)'
        AND l.eff IS DISTINCT FROM e.expected_state
        AND p.eff IS DISTINCT FROM e.expected_state)
ORDER BY l.database_name";

    public Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetDatabaseStatesCoreAsync(serverKey, ct),
            serverKey,
            "database state",
            cancellationToken);
    }

    private async Task<List<DatabaseStateInfo>> GetDatabaseStatesCoreAsync(
        string serverKey, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<DatabaseStateInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        using (var seed = new NpgsqlCommand(SeedDatabaseStateExpectedSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            seed.Parameters.AddWithValue(serverId);
            await seed.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Beside the seed because it is the same job from the other end (#2189): the seed learns a baseline
           for a database that has none, this un-learns one the database has since outgrown. Both run before
           the read, so a poisoned expectation is corrected on the cycle that notices it rather than firing
           once more first. */
        using (var heal = new NpgsqlCommand(HealDatabaseStateBaselineToOnlineSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            heal.Parameters.AddWithValue(serverId);
            await heal.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var prune = new NpgsqlCommand(PruneDatabaseStateExpectedSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            prune.Parameters.AddWithValue(serverId);
            await prune.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Before the read, so this cycle judges against a memory the store has already healed rather than
           one carried over from a restart (#2166). A database cleared here is one that is back at its
           expected state, so it cannot appear in the deviation read below either way — the ordering matters
           for the NEXT deviation, not this one. */
        using (var clearRecovered = new NpgsqlCommand(ClearRecoveredDatabaseStateAlertsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            clearRecovered.Parameters.AddWithValue(serverId);
            await clearRecovered.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var command = new NpgsqlCommand(DatabaseStateDeviationsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            command.Parameters.AddWithValue(serverId);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new DatabaseStateInfo
                {
                    DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    StateDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    LastAlertedState = reader.IsDBNull(3) ? "" : reader.GetString(3)
                });
            }
        }

        return items;
    }

    /// <summary>How far back <see cref="ForcePlanFailuresSql"/> looks for a plan's two most recent
    /// collections. Bound as a parameter rather than written into the SQL — see that query's remarks.</summary>
    internal static readonly TimeSpan ForcePlanFailureWindow = TimeSpan.FromHours(2);

    /// <summary>
    /// Forced plans whose failure counter ROSE between the two most recent collections that carried the
    /// plan (#2157). $1 server_id.
    ///
    /// <para>Shape notes: query_store_stats holds one row per plan PER INTERVAL per collection, and the
    /// forcing columns are plan-level attributes repeated across those rows — so the CTE collapses each
    /// (plan, collection_time) to one value with MAX before any comparison. The two-hour window bounds
    /// the hypertable scan; a plan not collected within it is by definition not failing right now, and
    /// Query Store's own flush cadence (900s) means an active plan appears several times inside it.</para>
    ///
    /// <para>The window's lower bound is BOUND as <c>$2</c> from <see cref="NaiveUtcNow"/>, never spelled
    /// <c>now() - interval '2 hours'</c> in the SQL. <c>collection_time</c> is
    /// <c>timestamp without time zone</c> holding naive UTC and <c>now()</c> is <c>timestamptz</c>, so the
    /// mixed comparison makes PostgreSQL convert the naive side at the store SESSION's TimeZone — which
    /// initdb takes from the host OS, and which BuildConfAppend does not pin. Measured on
    /// timescaledb:latest-pg17 seeded one row per 52s, a one-hour window returned 70 rows under
    /// <c>TimeZone='UTC'</c> and 347 under <c>'America/New_York'</c>: the same predicate silently spanning
    /// five hours. East of UTC it narrows instead: at <c>'Pacific/Kiritimati'</c> this read returns NOTHING,
    /// so the forced-plan-failure alert never fires there at all. The bound is the service clock's, which is also the clock that
    /// stamped <c>collection_time</c>, so the two cannot disagree about what "two hours ago" means.</para>
    ///
    /// <para>The <c>&gt;</c> comparison is what makes this a delta read: equal counters are silence, and a
    /// LOWER counter (unforce/re-force reset) is silence too rather than a negative delta.</para>
    ///
    /// <para><b>The newer sighting's <c>collection_time</c> travels with the row (#3579)</b> as
    /// <c>observed_at</c>, the last column. The delta is a fact about two COLLECTIONS and stays byte-identical
    /// on every alert pass until the next collection lands — "every row here is a live failure" is true at the
    /// collection instant and stale for the rest of the interval — so the engine needs the observation's
    /// identity to fire once per collection instead of once per cooldown. Appended rather than inserted so
    /// the seven ordinals the reader already binds do not move. It is <c>n.collection_time</c>, already in
    /// <c>per_collection</c>'s GROUP BY and already carried by the covering index: no new <c>qs.</c> column,
    /// so the access path below is untouched (the access-path pins re-derive the list from this text).</para>
    ///
    /// <para><b>The access path is a covering index, and the column list here is what it covers (#3573).</b>
    /// <c>PgTableTuning.ForcePlanFailuresIndexName</c> is <c>(server_id, collection_time DESC) INCLUDE</c>
    /// every other column this statement touches, so it runs as an Index Only Scan over one server's two
    /// hours. That is not a nicety. V1's plain <c>(server_id, collection_time)</c> composite was on the
    /// production store the whole time and the planner refused it: <c>server_id</c>'s physical correlation is
    /// ~0.02 (forty-three servers interleaved by collection pass), so the cost model priced the composite's
    /// heap fetches as one random page per row and preferred streaming the ENTIRE fleet's two-hour slice through
    /// the time index — <c>Rows Removed by Filter: 691,058</c> to keep 37,878, 57,307 buffers, 422 ms warm and
    /// a 10.3 s cold tail against the 10 s deadline. Covering deletes the heap term the model got wrong. The
    /// consequence for anyone editing this SQL: reference a column of <c>qs</c> that the index does not carry
    /// and nothing fails — the plan silently reverts to the fleet-wide scan. <c>ForcePlanFailuresAccessPathTests</c>
    /// pins the two lists against each other and EXPLAINs the shipped statement against a live store; add the
    /// column to the index in the same change or that test tells you.</para>
    /// </summary>
    public const string ForcePlanFailuresSql = @"
WITH per_collection AS (
    SELECT
        qs.database_name,
        qs.query_id,
        qs.plan_id,
        qs.collection_time,
        MAX(COALESCE(qs.force_failure_count, 0)) AS failures,
        MAX(CASE WHEN qs.is_forced_plan THEN 1 ELSE 0 END) AS forced,
        MAX(COALESCE(qs.plan_forcing_type, '')) AS forcing_type,
        MAX(COALESCE(qs.last_force_failure_reason, '')) AS reason
    FROM query_store_stats AS qs
    WHERE qs.server_id = $1
    AND   qs.collection_time > $2
    GROUP BY qs.database_name, qs.query_id, qs.plan_id, qs.collection_time
),
ranked AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.database_name, pc.query_id, pc.plan_id ORDER BY pc.collection_time DESC) AS rn
    FROM per_collection AS pc
)
SELECT
    n.database_name,
    n.query_id,
    n.plan_id,
    n.forcing_type,
    n.reason,
    n.failures - p.failures AS failure_delta,
    n.failures AS total_failures,
    n.collection_time AS observed_at
FROM ranked AS n
JOIN ranked AS p
  ON  p.database_name = n.database_name
  AND p.query_id = n.query_id
  AND p.plan_id = n.plan_id
  AND p.rn = 2
WHERE n.rn = 1
AND   n.forced = 1
AND   n.failures > p.failures
ORDER BY n.database_name, n.query_id, n.plan_id";

    public Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        /* #3848: every read in this type goes out through the one retry seam, which re-asks
           the store once two seconds later when this read crosses its own 10 s command
           deadline — the sparse write-band transient that used to blind this condition for a
           pass. The read below is unchanged and is what runs on both attempts; the name is the
           same one the engine's own catch arm records a failure under, so one read has one name
           across both counts. */
        return ExecuteWithOneRetryAsync(
            ct => GetForcePlanFailuresCoreAsync(serverKey, ct),
            serverKey,
            "forced-plan failures",
            cancellationToken);
    }

    private async Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresCoreAsync(
        string serverKey, CancellationToken cancellationToken)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<ForcePlanFailureInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(ForcePlanFailuresSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NaiveUtcNow() - ForcePlanFailureWindow);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ForcePlanFailureInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                ForcingType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                FailureReason = reader.IsDBNull(4) ? "" : reader.GetString(4),
                FailureDelta = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalFailures = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                /* #3579: the stored value is naive UTC (see the window-bound remarks above), read back with
                   Kind Unspecified; stamped Utc because that is what it IS and what the property's name says.
                   The engine only ever compares one plan's stamps with each other, so the Kind is honesty
                   rather than arithmetic. */
                ObservedAtUtc = reader.IsDBNull(7) ? null : DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)
            });
        }

        return items;
    }

    private int ResolveRunningJobsCadence(int serverId) =>
        ResolveCadence(_runningJobsCadenceMinutes, serverId, "running_jobs");

    /// <summary>
    /// A server's effective cadence for one collector: the worker's resolver when it answers usefully,
    /// otherwise the shipped default, otherwise 2 minutes (an unregistered collector name).
    /// </summary>
    private static int ResolveCadence(Func<int, int>? resolver, int serverId, string collectorName)
    {
        var resolved = resolver?.Invoke(serverId) ?? 0;
        if (resolved > 0)
        {
            return resolved;
        }

        return PerformanceMonitor.Collectors.CollectorScheduleDefaults.All.TryGetValue(collectorName, out var schedule)
            ? schedule.FrequencyMinutes
            : 2;
    }

    /* ---------------- helpers ---------------- */

    /// <summary>Naive-UTC now, Kind-Unspecified — the product's PG timestamp discipline.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    /// <summary>The (start, end) collection_time window Lite's GetTimeRange produces for hoursBack.</summary>
    private static (DateTime StartTime, DateTime EndTime) Window(int hoursBack)
    {
        var endTime = NaiveUtcNow();
        return (endTime.AddHours(-hoursBack), endTime);
    }

    private static int ParseServerKey(string serverKey) =>
        int.Parse(serverKey, CultureInfo.InvariantCulture);

    /// <summary>numeric(p,s) columns read back as decimal — coerce like Lite's ToDouble.</summary>
    private static double ToDouble(object value) =>
        Convert.ToDouble(value, CultureInfo.InvariantCulture);
}
