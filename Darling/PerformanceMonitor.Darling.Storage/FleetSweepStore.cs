/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One completed sweep — the run row of <c>collect.fleet_sweep_runs</c>. Timestamps are
/// UTC on both sides of the store; the store writes them naive and tags them Utc on read.</summary>
public sealed record FleetSweepRun(
    long SweepId,
    DateTime SweptAtUtc,
    DateTime SpanStartUtc,
    DateTime SpanEndUtc,
    long? PreviousSweepId,
    bool AlertsEnabled,
    int ServersExpected,
    int ServersReported,
    bool InstrumentsAlive,
    string InstrumentLivenessJson,
    string ReportJson);

/// <summary>One server's verdict inside one sweep. <c>PreviousBand</c> is null on a first sweep and
/// on a server the previous sweep did not carry; <c>BandReason</c> is the spec's "reason for every
/// non-Healthy card" and is null exactly when the band needs no defending.</summary>
public sealed record FleetSweepServerVerdict(
    int ServerId,
    string ServerName,
    string Band,
    string? PreviousBand,
    string? BandReason,
    string? VerdictJson);

/// <summary>One would-have-paged ledger entry: under <c>alerts_enabled: false</c>, a condition the
/// alert engine would have delivered, carried with its evidence so the mute stays auditable.</summary>
public sealed record FleetSweepWouldHavePagedEntry(
    int ServerId,
    string AlertFamily,
    string EvidenceJson);

/// <summary>One watch item's stored row — the full image, because the engine reads the row, runs
/// <see cref="FleetSweepWatchStateMachine.Advance"/>, and hands the result back whole.</summary>
public sealed record FleetSweepWatchItem(
    int ServerId,
    string ItemKey,
    string Condition,
    string State,
    int ConsecutiveHits,
    int ConsecutiveMisses,
    long FirstSeenSweepId,
    long LastSeenSweepId,
    long? OpenedSweepId,
    long? ClosedSweepId,
    DateTime FirstSeenAtUtc,
    DateTime LastSeenAtUtc,
    string? EvidenceJson);

/// <summary>
/// The fleet sweep's state store (#3466, lane 1 of 4): the sweep-over-sweep memory the alert engine
/// deliberately is not. Four tables in <c>collect</c> — the run, its per-server verdicts, its
/// would-have-paged ledger, and the watch-item worklist — plus the writer the sweep engine calls and
/// the readers the web feed and MCP surface will call. The V123 rung concatenates
/// <see cref="CreateTablesSql"/> (the V38/V121 idiom), so the shape a store gets and the shape these
/// statements address cannot drift.
///
/// <para><b>Why this is data and not a rendered artifact.</b> #3466's decomposition: collect, diff,
/// watch and discriminate are fully deterministic GIVEN persisted previous-sweep state, and the
/// external sweep that produced the spec faked that state by reading its own previous report. The
/// product implements it as rows: a run row per sweep (immutable — sweeps are per-run records, so
/// there is no UPDATE statement against the run or its children in this file), and a watch-item
/// worklist that carries condition state ACROSS sweeps.</para>
///
/// <para><b>The per-server verdicts are a normalized child table, not a payload blob</b>, because
/// every later lane queries them relationally: the engine (lane 2) joins sweep N−1's verdicts to
/// sweep N's per server to prove its diff instead of restating absolutes; the web timeline (lane 3)
/// renders per-server cards for one sweep by key; the daily channel rollup (lane 4) ranks a day's
/// worst episodes across sweeps. A JSON block can be rendered but not joined, and the diff is the
/// acceptance test of the whole feature.</para>
///
/// <para><b>Instrument liveness is a verdict column beside its evidence block</b> — the V114
/// verdict-beside-inputs idiom. <c>instruments_alive</c> is what the timeline and the connect-level
/// reads band on; <c>instrument_liveness_json</c> is which counters were read and whether each was
/// advancing, so a reader can disagree with the boolean rather than believe it. Both are NOT NULL:
/// quiet-is-not-clean is the spec's third hard requirement, so a sweep that did not prove its
/// instruments cannot be written at all — the frozen-counter-beside-a-delivering-path reading is how
/// a real gating defect was caught (#3464), and an empty ledger with a dead reader must render as a
/// loud failure, never as a green sweep.</para>
///
/// <para><b>The would-have-paged ledger is first-class rows, not a count.</b> The muted-mode
/// contract is the spec's strongest argument: under <c>alerts_enabled: false</c> the sweep still
/// runs and carries what WOULD have paged, with evidence, per server and family — on the evidence
/// day a production server sat pinned for eighteen-plus minutes inside an operator mute and the
/// external sweep's report was the only surface that carried it. Rows keyed
/// (sweep_id, server_id, alert_family) let the daily rollup count and rank them in SQL and let an
/// operator audit one afternoon's mute without deserializing every document it covered.</para>
///
/// <para><b>The watch-item table is a worklist, not a series</b> — the oversized-plan backlog
/// precedent. One row per (server_id, item_key): re-sightings advance the row's state machine
/// (<see cref="FleetSweepWatchStateMachine"/>) rather than inserting, first-seen stamps are written
/// once and never touched by the upsert, and per-sweep history lives in the sweep documents the
/// transitions were reported in. <c>server_id</c> 0 is the fleet-scope sentinel for items about the
/// fleet rather than a member — the same synthetic-subject idiom the fleet self-alerts use — and it
/// can sit in the PRIMARY KEY where the mute registry's NULL-means-global cannot.</para>
///
/// <para><b>Plain tables, deliberately — none of the four is a hypertable.</b> They are absent from
/// <c>CollectorCatalog</c>, so <see cref="TimescaleSupport"/>'s catalog-driven conversion and
/// <c>DarlingRetention</c>'s catalog purge never reach them — the <c>collect.store_metrics</c> and
/// <c>collect.oversized_plan_backlog</c> precedents. The run table IS a time series, but at the
/// default hourly cadence it is 24 rows a day against collector tables that take that per server per
/// minute, and its children key on <c>sweep_id</c> — a hypertable's unique constraint must include
/// the partitioning column, which is the same shape rejection the backlog records. The
/// analysis_findings note in <see cref="TimescaleSupport"/> is the standing disposition: convertible
/// later if volume ever warrants, and nothing at this volume does.</para>
///
/// <para><b>Growth is bounded by the horizon, wired with the engine.</b> Retention for these tables
/// lands in lane 2 beside the engine that writes them, the way the backlog's landed beside its
/// sweep: runs and their children prune on <c>swept_at</c> at the base data horizon
/// (<c>DarlingRetention.DataRetentionBaseDays</c> — a sweep outliving the data it summarizes
/// explains nothing), and watch items prune on <c>last_seen_at</c>, the backlog's own rule, so an
/// episode still being carried never loses its row.</para>
///
/// <para><b>No ACL work.</b> The <c>collect</c> schema carries blanket SELECT for admin/viewer/mcp,
/// re-asserted every managed start, plus <c>ALTER DEFAULT PRIVILEGES</c> so new tables inherit —
/// which is exactly what the web feed (viewer role, lane 3) and <c>get_sweep_reports</c> (mcp role,
/// lane 4) need. The engine writes as the service owner, so no new write grant exists to add, and a
/// GRANT appearing in the rung would mean this feature had quietly taken on an ACL decision
/// (<c>FleetSweepStateRungTests</c> pins its absence).</para>
///
/// <para>Postgres discipline throughout (PgCollectorRowWriter/PgFindingStore): timestamps written
/// naive-UTC Kind-Unspecified, tagged Utc on read; "now" is always the caller's bound parameter,
/// never a bare <c>now()</c>; SQL is exposed const so the rung tests pin the dialect ungated.</para>
/// </summary>
public static class FleetSweepStore
{
    /// <summary>
    /// The command deadline for this store's own statements — its own constant, per the
    /// <see cref="OversizedPlanBacklog.CommandTimeoutSeconds"/> reasoning: an inherited Npgsql
    /// default is the defect class <see cref="StorageCommandDeadlines"/> exists to close, and the
    /// MCP-read constant's regime is an interactive read, not a sweep persist. Every statement here
    /// is a key-matched write or a small read on loopback; the one with real work is the run INSERT,
    /// which carries the rendered report document — bounded by the renderer, kilobytes not
    /// megabytes — so 30 s is orders of magnitude above the work while still low enough that a
    /// stalled write is reported rather than held across a sweep slot.
    /// </summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>The run table, schema-qualified — every site here is explicit so a read from a
    /// differently-configured session cannot land somewhere else.</summary>
    public const string RunsTableName = "collect.fleet_sweep_runs";

    /// <summary>See <see cref="RunsTableName"/>.</summary>
    public const string VerdictsTableName = "collect.fleet_sweep_server_verdicts";

    /// <summary>See <see cref="RunsTableName"/>.</summary>
    public const string WouldHavePagedTableName = "collect.fleet_sweep_would_have_paged";

    /// <summary>See <see cref="RunsTableName"/>.</summary>
    public const string WatchItemsTableName = "collect.fleet_sweep_watch_items";

    /// <summary>The fleet-scope sentinel for watch items about the fleet rather than one member —
    /// no monitored server has id 0, the same reasoning the fleet self-alerts' synthetic subject
    /// rests on, and unlike a NULL it can live in the PRIMARY KEY.</summary>
    public const int FleetScopeServerId = 0;

    /// <summary>
    /// The DDL, owned here and concatenated into the V123 rung rather than transcribed there — the
    /// V38/V121 idiom, so the shape the store gets and the shape the statements below address cannot
    /// drift.
    ///
    /// <para><b>One index per table, deliberately: the primary key.</b> The run reads are latest and
    /// by-span over a table bounded at dozens of rows a day; the children's reads lead on
    /// <c>sweep_id</c>, their keys' first column; the watch reads lead on state or scan a worklist
    /// bounded by what the fleet is actually watching. At these sizes every access path is served or
    /// is a scan of a few thousand narrow rows, and an index chosen for an ORDER BY nobody has
    /// measured would be a guess — the backlog's rule, restated. If the by-span read ever stops
    /// being cheap the index to add is <c>(swept_at)</c> on the run table.</para>
    ///
    /// <para><b>No foreign keys from the children to the run</b>, matching every collect-schema
    /// relationship in the store (servers to collector rows, findings to incidents): the writer's
    /// single transaction is what makes a sweep atomic, and retention prunes the tables on the same
    /// horizon, so a constraint would buy enforcement of an invariant the write path already owns at
    /// the price of ordering every future prune.</para>
    /// </summary>
    public const string CreateTablesSql = @"
CREATE TABLE IF NOT EXISTS collect.fleet_sweep_runs
(
    sweep_id bigint NOT NULL,
    swept_at timestamp NOT NULL,
    span_start timestamp NOT NULL,
    span_end timestamp NOT NULL,
    previous_sweep_id bigint,
    alerts_enabled boolean NOT NULL,
    servers_expected integer NOT NULL,
    servers_reported integer NOT NULL,
    instruments_alive boolean NOT NULL,
    instrument_liveness_json text NOT NULL,
    report_json text NOT NULL,
    CONSTRAINT pk_fleet_sweep_runs PRIMARY KEY (sweep_id)
);

CREATE TABLE IF NOT EXISTS collect.fleet_sweep_server_verdicts
(
    sweep_id bigint NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    band text NOT NULL,
    previous_band text,
    band_reason text,
    verdict_json text,
    CONSTRAINT pk_fleet_sweep_server_verdicts PRIMARY KEY (sweep_id, server_id)
);

CREATE TABLE IF NOT EXISTS collect.fleet_sweep_would_have_paged
(
    sweep_id bigint NOT NULL,
    server_id integer NOT NULL,
    alert_family text NOT NULL,
    evidence_json text NOT NULL,
    CONSTRAINT pk_fleet_sweep_would_have_paged PRIMARY KEY (sweep_id, server_id, alert_family)
);

CREATE TABLE IF NOT EXISTS collect.fleet_sweep_watch_items
(
    server_id integer NOT NULL,
    item_key text NOT NULL,
    condition text NOT NULL,
    state text NOT NULL,
    consecutive_hits integer NOT NULL DEFAULT 0,
    consecutive_misses integer NOT NULL DEFAULT 0,
    first_seen_sweep_id bigint NOT NULL,
    last_seen_sweep_id bigint NOT NULL,
    opened_sweep_id bigint,
    closed_sweep_id bigint,
    first_seen_at timestamp NOT NULL,
    last_seen_at timestamp NOT NULL,
    evidence_json text,
    CONSTRAINT pk_fleet_sweep_watch_items PRIMARY KEY (server_id, item_key)
);";

    /// <summary>One completed sweep's run row. INSERT only — a sweep document is immutable once
    /// written, so no statement in this file updates it.</summary>
    public const string InsertRunSql = @"
INSERT INTO collect.fleet_sweep_runs
(sweep_id, swept_at, span_start, span_end, previous_sweep_id, alerts_enabled,
 servers_expected, servers_reported, instruments_alive, instrument_liveness_json, report_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);";

    /// <summary>One server's verdict under a run. INSERT only, same immutability as the run.</summary>
    public const string InsertVerdictSql = @"
INSERT INTO collect.fleet_sweep_server_verdicts
(sweep_id, server_id, server_name, band, previous_band, band_reason, verdict_json)
VALUES ($1, $2, $3, $4, $5, $6, $7);";

    /// <summary>One would-have-paged ledger entry under a run. INSERT only.</summary>
    public const string InsertWouldHavePagedSql = @"
INSERT INTO collect.fleet_sweep_would_have_paged
(sweep_id, server_id, alert_family, evidence_json)
VALUES ($1, $2, $3, $4);";

    /// <summary>
    /// One watch item's advanced state. The ENGINE is authoritative for every mutable column — it
    /// read the row, ran <see cref="FleetSweepWatchStateMachine.Advance"/>, and hands back the full
    /// image — so the conflict arm takes EXCLUDED wholesale rather than COALESCE-merging, with two
    /// deliberate exceptions. The first-seen pair is NEVER in the DO UPDATE: those stamps are the
    /// item's birth record, the backlog's <c>captured_at</c> rule. And <c>evidence_json</c> keeps
    /// the standing evidence when the engine passes null — a MISS sweep has no fresh evidence, and
    /// overwriting the evidence that opened an item with a miss's nothing would leave a carried item
    /// citing no instrument at exactly the moment an operator asks why it is still open.
    /// </summary>
    public const string UpsertWatchItemSql = @"
INSERT INTO collect.fleet_sweep_watch_items AS w
(server_id, item_key, condition, state, consecutive_hits, consecutive_misses,
 first_seen_sweep_id, last_seen_sweep_id, opened_sweep_id, closed_sweep_id,
 first_seen_at, last_seen_at, evidence_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT ON CONSTRAINT pk_fleet_sweep_watch_items DO UPDATE SET
    condition = EXCLUDED.condition,
    state = EXCLUDED.state,
    consecutive_hits = EXCLUDED.consecutive_hits,
    consecutive_misses = EXCLUDED.consecutive_misses,
    last_seen_sweep_id = EXCLUDED.last_seen_sweep_id,
    opened_sweep_id = EXCLUDED.opened_sweep_id,
    closed_sweep_id = EXCLUDED.closed_sweep_id,
    last_seen_at = EXCLUDED.last_seen_at,
    evidence_json = COALESCE(EXCLUDED.evidence_json, w.evidence_json);";

    /// <summary>The shared run column list, written once so the two run reads cannot drift from
    /// each other or from the single reader that maps them.</summary>
    private const string RunColumns = @"
SELECT sweep_id, swept_at, span_start, span_end, previous_sweep_id, alerts_enabled,
       servers_expected, servers_reported, instruments_alive, instrument_liveness_json, report_json
FROM collect.fleet_sweep_runs";

    /// <summary>The newest completed sweep — the engine's diff anchor and the timeline's landing
    /// row. Ordered on the sweep instant with the id as tiebreak, not on the id alone: ids are
    /// generator-issued and time-ordered today, but the ORDER BY states the semantic ("newest
    /// sweep") rather than trusting the generator's implementation detail.</summary>
    public const string GetLatestRunSql = RunColumns + @"
ORDER BY swept_at DESC, sweep_id DESC
LIMIT 1;";

    /// <summary>
    /// The runs inside a caller-supplied span, newest first — the web feed's configurable-span read.
    /// BOTH bounds are the caller's, the #2506 lesson: a read with a start and no end cannot be
    /// anchored, and "now" belongs to the caller's clock, not a bare <c>now()</c> compared in the
    /// store's time zone.
    /// </summary>
    public const string GetRunsBySpanSql = RunColumns + @"
WHERE swept_at >= $1
AND   swept_at <= $2
ORDER BY swept_at DESC, sweep_id DESC;";

    /// <summary>One sweep's per-server verdicts, in a stable render order.</summary>
    public const string GetVerdictsSql = @"
SELECT server_id, server_name, band, previous_band, band_reason, verdict_json
FROM collect.fleet_sweep_server_verdicts
WHERE sweep_id = $1
ORDER BY server_name, server_id;";

    /// <summary>One sweep's would-have-paged ledger.</summary>
    public const string GetWouldHavePagedSql = @"
SELECT server_id, alert_family, evidence_json
FROM collect.fleet_sweep_would_have_paged
WHERE sweep_id = $1
ORDER BY server_id, alert_family;";

    /// <summary>The shared watch-item column list — one list, one reader, the run-read rule.</summary>
    private const string WatchItemColumns = @"
SELECT server_id, item_key, condition, state, consecutive_hits, consecutive_misses,
       first_seen_sweep_id, last_seen_sweep_id, opened_sweep_id, closed_sweep_id,
       first_seen_at, last_seen_at, evidence_json
FROM collect.fleet_sweep_watch_items";

    /// <summary>Watch items in one state — the web/MCP "what is open right now" read.</summary>
    public const string GetWatchItemsByStateSql = WatchItemColumns + @"
WHERE state = $1
ORDER BY last_seen_at DESC, server_id, item_key;";

    /// <summary>
    /// Every watch item the engine must evaluate on a sweep: everything not closed, because a
    /// pending item's entry count and an open item's exit count both advance on sweeps that do NOT
    /// sight them — a miss is an evaluation, not an absence. The state literal is spliced from
    /// <see cref="FleetSweepWatchStateMachine.Closed"/> so the statement and the state machine
    /// cannot disagree about the word.
    /// </summary>
    public const string GetActiveWatchItemsSql = WatchItemColumns + @"
WHERE state <> '" + FleetSweepWatchStateMachine.Closed + @"'
ORDER BY server_id, item_key;";

    /// <summary>
    /// Persists one completed sweep — the run row, its per-server verdicts, its would-have-paged
    /// ledger, and every watch-item transition the engine computed — in ONE transaction.
    ///
    /// <para><b>The transaction is the method's promise, and it THROWS</b> — the
    /// <c>PgFindingStore.InsertFindingsAsync</c> discipline (#2448), restated for sweeps: a sweep is
    /// one indivisible statement about the fleet at one instant. A run row without its verdicts
    /// reads as a fleet with no servers; verdicts without the would-have-paged ledger read as a
    /// clean mute, which is the exact misreading the muted-mode contract exists to prevent; and
    /// watch transitions committed without their run would advance hysteresis counters against a
    /// sweep the store denies happened. Rolling everything back leaves the PREVIOUS sweep as the
    /// newest complete one — stale, stamped with its own instant, incapable of misleading anyone —
    /// and the throw makes the engine report the sweep failed rather than announce a sweep the store
    /// does not hold.</para>
    /// </summary>
    public static async Task RecordSweepAsync(
        NpgsqlDataSource postgres,
        FleetSweepRun run,
        IReadOnlyList<FleetSweepServerVerdict> verdicts,
        IReadOnlyList<FleetSweepWouldHavePagedEntry> wouldHavePaged,
        IReadOnlyList<FleetSweepWatchItem> watchItems,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(verdicts);
        ArgumentNullException.ThrowIfNull(wouldHavePaged);
        ArgumentNullException.ThrowIfNull(watchItems);

        /* The connection open is the last cancellation point — the #2443 shape. Past it the sweep
           persists whole; cancelling between child rows would manufacture exactly the partial
           document the transaction exists to prevent. */
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None).ConfigureAwait(false);

        await using (var command = new NpgsqlCommand(InsertRunSql, connection, transaction)
        {
            CommandTimeout = CommandTimeoutSeconds,
        })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = run.SweepId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = AsNaive(run.SweptAtUtc) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = AsNaive(run.SpanStartUtc) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = AsNaive(run.SpanEndUtc) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)run.PreviousSweepId ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = run.AlertsEnabled });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = run.ServersExpected });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = run.ServersReported });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = run.InstrumentsAlive });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = run.InstrumentLivenessJson });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = run.ReportJson });
            await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }

        if (verdicts.Count > 0)
        {
            await using var command = new NpgsqlCommand(InsertVerdictSql, connection, transaction)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };

            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = run.SweepId });
            var serverId = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var serverName = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var band = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var previousBand = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var bandReason = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var verdictJson = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });

            foreach (var verdict in verdicts)
            {
                serverId.Value = verdict.ServerId;
                serverName.Value = verdict.ServerName;
                band.Value = verdict.Band;
                previousBand.Value = (object?)verdict.PreviousBand ?? DBNull.Value;
                bandReason.Value = (object?)verdict.BandReason ?? DBNull.Value;
                verdictJson.Value = (object?)verdict.VerdictJson ?? DBNull.Value;
                await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        if (wouldHavePaged.Count > 0)
        {
            await using var command = new NpgsqlCommand(InsertWouldHavePagedSql, connection, transaction)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };

            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = run.SweepId });
            var serverId = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var alertFamily = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var evidenceJson = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });

            foreach (var entry in wouldHavePaged)
            {
                serverId.Value = entry.ServerId;
                alertFamily.Value = entry.AlertFamily;
                evidenceJson.Value = entry.EvidenceJson;
                await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        if (watchItems.Count > 0)
        {
            await using var command = new NpgsqlCommand(UpsertWatchItemSql, connection, transaction)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };

            var serverId = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var itemKey = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var condition = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var state = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var hits = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var misses = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var firstSeenSweep = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });
            var lastSeenSweep = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });
            var openedSweep = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });
            var closedSweep = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });
            var firstSeenAt = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp });
            var lastSeenAt = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp });
            var evidenceJson = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });

            foreach (var item in watchItems)
            {
                serverId.Value = item.ServerId;
                itemKey.Value = item.ItemKey;
                condition.Value = item.Condition;
                state.Value = item.State;
                hits.Value = item.ConsecutiveHits;
                misses.Value = item.ConsecutiveMisses;
                firstSeenSweep.Value = item.FirstSeenSweepId;
                lastSeenSweep.Value = item.LastSeenSweepId;
                openedSweep.Value = (object?)item.OpenedSweepId ?? DBNull.Value;
                closedSweep.Value = (object?)item.ClosedSweepId ?? DBNull.Value;
                firstSeenAt.Value = AsNaive(item.FirstSeenAtUtc);
                lastSeenAt.Value = AsNaive(item.LastSeenAtUtc);
                evidenceJson.Value = (object?)item.EvidenceJson ?? DBNull.Value;
                await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// The newest completed sweep, or null when the store holds none — which the caller must treat
    /// as "no previous sweep persisted" and SAY so in its document, never as a quiet first day.
    ///
    /// <para><b>Throws on a store fault, deliberately</b>, unlike the presentation reads below: this
    /// is the ENGINE's diff anchor, and a fault swallowed into null would make sweep N silently diff
    /// against nothing and restate absolutes — a sweep that reads healthier because the store
    /// failed, the #2448 misreading at the read end. The distinction between "empty" and "unreadable"
    /// is the whole quiet-is-not-clean contract, and only the caller can render it.</para>
    /// </summary>
    public static async Task<FleetSweepRun?> GetLatestSweepAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(GetLatestRunSql, connection)
        {
            CommandTimeout = CommandTimeoutSeconds,
        };

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadRun(reader) : null;
    }

    /// <summary>
    /// Every watch item that is not closed — the engine's evaluation set. Throws on a store fault
    /// for <see cref="GetLatestSweepAsync"/>'s reason: an unreadable worklist degraded to empty
    /// would reset every hysteresis counter to "first sighting" on the next sweep, re-opening
    /// nothing and re-confirming everything from scratch, silently.
    /// </summary>
    public static async Task<List<FleetSweepWatchItem>> GetActiveWatchItemsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var items = new List<FleetSweepWatchItem>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(GetActiveWatchItemsSql, connection)
        {
            CommandTimeout = CommandTimeoutSeconds,
        };

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(ReadWatchItem(reader));
        }

        return items;
    }

    /// <summary>
    /// One sweep's per-server verdicts for the ENGINE's diff — the same statement as
    /// <see cref="GetServerVerdictsAsync"/> with the opposite fault posture, and the split is the
    /// point: this read is sweep N's join target for previous-band and transition computation, and a
    /// fault swallowed into empty would make every server read as NEW to the sweep — a diff that
    /// restates absolutes because the store failed, the same misreading <see cref="GetLatestSweepAsync"/>
    /// throws to prevent. The presentation read keeps its log-and-degrade posture for the surfaces
    /// that have their own degraded rendering; the engine has none, so it throws and the sweep fails
    /// loudly instead of publishing a wrong document.
    /// </summary>
    public static async Task<List<FleetSweepServerVerdict>> GetServerVerdictsForEngineAsync(
        NpgsqlDataSource postgres, long sweepId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var verdicts = new List<FleetSweepServerVerdict>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(GetVerdictsSql, connection)
        {
            CommandTimeout = CommandTimeoutSeconds,
        };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sweepId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            verdicts.Add(ReadVerdict(reader));
        }

        return verdicts;
    }

    /// <summary>
    /// The runs inside a span, newest first — the web feed's read (lane 3). Logs and returns empty
    /// on a fault, the presentation-read discipline (PgFindingStore): the surfaces this serves have
    /// their own degraded rendering, and an exception here would take the whole page with it.
    /// </summary>
    public static async Task<List<FleetSweepRun>> GetSweepsBySpanAsync(
        NpgsqlDataSource postgres,
        DateTime spanStartUtc,
        DateTime spanEndUtc,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var runs = new List<FleetSweepRun>();

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(GetRunsBySpanSql, connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = AsNaive(spanStartUtc) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = AsNaive(spanEndUtc) });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                runs.Add(ReadRun(reader));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogError("[FleetSweepStore] GetSweepsBySpanAsync failed: {Message}", ex.Message);
        }

        return runs;
    }

    /// <summary>One sweep's per-server verdicts — presentation read, logs and degrades.</summary>
    public static async Task<List<FleetSweepServerVerdict>> GetServerVerdictsAsync(
        NpgsqlDataSource postgres, long sweepId, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var verdicts = new List<FleetSweepServerVerdict>();

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(GetVerdictsSql, connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sweepId });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                verdicts.Add(ReadVerdict(reader));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogError("[FleetSweepStore] GetServerVerdictsAsync failed: {Message}", ex.Message);
        }

        return verdicts;
    }

    /// <summary>One sweep's would-have-paged ledger — presentation read, logs and degrades.</summary>
    public static async Task<List<FleetSweepWouldHavePagedEntry>> GetWouldHavePagedAsync(
        NpgsqlDataSource postgres, long sweepId, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var entries = new List<FleetSweepWouldHavePagedEntry>();

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(GetWouldHavePagedSql, connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sweepId });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                entries.Add(new FleetSweepWouldHavePagedEntry(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogError("[FleetSweepStore] GetWouldHavePagedAsync failed: {Message}", ex.Message);
        }

        return entries;
    }

    /// <summary>Watch items in one state — presentation read, logs and degrades. The state names
    /// are <see cref="FleetSweepWatchStateMachine"/>'s constants; an unknown state matches nothing,
    /// which is the honest answer for it.</summary>
    public static async Task<List<FleetSweepWatchItem>> GetWatchItemsByStateAsync(
        NpgsqlDataSource postgres, string state, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(state);

        var items = new List<FleetSweepWatchItem>();

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(GetWatchItemsByStateSql, connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = state });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                items.Add(ReadWatchItem(reader));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogError("[FleetSweepStore] GetWatchItemsByStateAsync failed: {Message}", ex.Message);
        }

        return items;
    }

    /// <summary>Maps one run row — ordinals match <see cref="RunColumns"/>, the one list both run
    /// reads share.</summary>
    private static FleetSweepRun ReadRun(NpgsqlDataReader reader)
    {
        return new FleetSweepRun(
            reader.GetInt64(0),
            AsUtc(reader.GetDateTime(1)),
            AsUtc(reader.GetDateTime(2)),
            AsUtc(reader.GetDateTime(3)),
            reader.IsDBNull(4) ? null : reader.GetInt64(4),
            reader.GetBoolean(5),
            reader.GetInt32(6),
            reader.GetInt32(7),
            reader.GetBoolean(8),
            reader.GetString(9),
            reader.GetString(10));
    }

    /// <summary>Maps one verdict row — ordinals match <see cref="GetVerdictsSql"/>, the one statement
    /// both the engine read and the presentation read execute.</summary>
    private static FleetSweepServerVerdict ReadVerdict(NpgsqlDataReader reader)
    {
        return new FleetSweepServerVerdict(
            reader.GetInt32(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5));
    }

    /// <summary>Maps one watch-item row — ordinals match <see cref="WatchItemColumns"/>.</summary>
    private static FleetSweepWatchItem ReadWatchItem(NpgsqlDataReader reader)
    {
        return new FleetSweepWatchItem(
            reader.GetInt32(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetInt32(4),
            reader.GetInt32(5),
            reader.GetInt64(6),
            reader.GetInt64(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8),
            reader.IsDBNull(9) ? null : reader.GetInt64(9),
            AsUtc(reader.GetDateTime(10)),
            AsUtc(reader.GetDateTime(11)),
            reader.IsDBNull(12) ? null : reader.GetString(12));
    }

    /// <summary>Kind-Unspecified for writes — Npgsql 6+ infers timestamptz from Kind=Utc and
    /// silently zone-shifts (see PgCollectorRowWriter).</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Columns are written as naive UTC; tag them Utc on read so the kind is explicit.</summary>
    private static DateTime AsUtc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
