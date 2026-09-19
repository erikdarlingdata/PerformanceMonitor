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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Where the two DOCUMENT-class self-alerts — the collector-cost digest (#3443) and the fleet-sweep daily
/// rollup (#3466) — record that a copy was DELIVERED, so their once-a-day gate can outlive the process
/// that fired them (#3580).
///
/// <para><b>The question this answers is "was one delivered today", not "did I send one today".</b> Both
/// gates lived in process memory — one <c>ConcurrentDictionary</c> each, one fixed key — and a fresh
/// process has empty dictionaries, so the first tick after ANY start delivered both documents again
/// whatever the previous process had delivered an hour earlier. On the v3.8.0 install night three stores
/// restarted once each and the channel carried six re-announcements among ~23 overnight posts: a quarter
/// of the channel was the same two documents, twice. The same night showed the case any fix must keep: a
/// digest/rollup pair whose delivery had FAILED (a transport fault, not a suppression) was correctly
/// re-attempted after the restart and landed. A failed delivery is not a delivery. So the evaluator writes
/// a stamp here only when the deliverer reports a disposition other than
/// <see cref="PerformanceMonitor.Notifications.AlertDelivery.ChannelFailed"/>, reads it back before firing,
/// and skips while the stamp is inside the document's interval — restart or not. A failed delivery writes
/// nothing, so the next tick retries, restart or not.</para>
///
/// <para><b>The store may throw; the evaluator isolates.</b> The <c>FleetSweepStore</c> shape rather than
/// <c>PgAlertStateStore</c>'s catch-inside: the evaluator is the one place that knows what a missing
/// answer means for the gate (fall back to the process-memory gate, warn, count the read into the #3013
/// swallowed-read census), and putting the catch there means a fake that throws exercises the real
/// fallback path rather than a mirror of it.</para>
/// </summary>
public interface ISelfAlertDeliveryStampStore
{
    /// <summary>The UTC instant the document keyed <paramref name="stateKey"/> was last DELIVERED, or
    /// <c>null</c> when no delivery has ever been stamped (a fresh store, or a document that has only ever
    /// failed to deliver).</summary>
    Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken);

    /// <summary>Records that the document keyed <paramref name="stateKey"/> was delivered at
    /// <paramref name="deliveredAtUtc"/>, replacing any earlier stamp.</summary>
    Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken);
}

/// <summary>
/// <see cref="ISelfAlertDeliveryStampStore"/> over <c>collect.collector_state</c> (V44) — the store's
/// general key/value state table, under a fleet-sentinel <c>server_id</c> and an owner name of its own. No
/// migration rung: the table already holds one short row per (server, collector, key), which is exactly
/// two rows of this shape, and #3580's fix shape names an existing key/value table as the natural home.
///
/// <para><b><c>server_id = 0</c> — the fleet sentinel the store already uses, in two places.</b>
/// <c>DarlingObservability.FleetServerId</c> is 0 for the fleet-wide retention purge's
/// <c>collection_log</c> run-record, and <c>FleetSweepStore.FleetScopeServerId</c> is 0 for watch items
/// about the fleet rather than one member; both rest on the same fact, that server_ids are FNV-1a hashes
/// of a storage name and no monitored server is registered at 0. These two documents are fleet-level
/// self-alerts (they fire under the synthetic store label, not a server), so they take the same id rather
/// than mint a third convention. The table's <c>server_id</c> is <c>integer NOT NULL</c> and part of the
/// primary key, which is also why a sentinel and not a NULL.</para>
///
/// <para><b><c>collector_name = 'self_alert'</c> — an owner name that is not a collector definition's.</b>
/// The <c>QueryStoreBackfill</c> precedent: a worker that keeps state in this table under its own name,
/// deliberately outside the definitions' <c>StateKeys</c> machinery, so no collector's declared-key read or
/// per-database prune can reach it. Every prune in <c>DarlingCollectorRunner</c> is scoped to a real
/// <c>server_id</c> AND a collector's own name, and the two migration-time deletes (V77, V114) name their
/// collector; nothing retires rows under this name, which is the point — this is state, not facts, and
/// the table carries no retention by design.</para>
///
/// <para><b>The stamp is the value, as ISO 8601 round-trip text, and <c>updated_at</c> is the write
/// time.</b> Two different instants: the stamp is the evaluator's clock at the fire (the controllable
/// <c>utcNow</c> seam, so a test can place it), and <c>updated_at</c> is the wall clock the row was written
/// at, the column's meaning on every other row of the table. The stamp travels as text because the column
/// is <c>text</c>; the "O" format round-trips to the tick and carries its <c>Z</c>, so the read side gets
/// <see cref="DateTimeKind.Utc"/> back without a <c>SpecifyKind</c> that could lie. A value that does not
/// parse — hand-edited, or written by nothing this build knows — reads as no stamp, with a warning, rather
/// than as a throw: the gate then falls back to memory for that tick, and the next successful delivery
/// overwrites the row.</para>
///
/// <para>Schema-qualified like the V44 DDL and <c>FleetSweepStore</c>, not bare like the runner's own
/// reads: this store is also handed a scratch database in the live test, where the connection string
/// carries no search path and only the migrator's best-effort <c>ALTER DATABASE</c> would resolve a bare
/// name. Naming the schema costs nothing and removes the dependency.</para>
/// </summary>
public sealed class PgSelfAlertDeliveryStampStore : ISelfAlertDeliveryStampStore
{
    /// <summary>See the class remarks: the fleet-wide sentinel both existing fleet-scope writers use.</summary>
    public const int FleetServerId = DarlingObservability.FleetServerId;

    /// <summary>See the class remarks: the owner name, deliberately not a collector definition's.</summary>
    public const string StateCollectorName = "self_alert";

    /// <summary>The digest's stamp key — one fixed row, the document's one fixed in-memory key made
    /// durable.</summary>
    public const string CostDigestStateKey = "digest_delivered_at";

    /// <summary>The fleet-sweep rollup's stamp key.</summary>
    public const string FleetSweepRollupStateKey = "sweep_rollup_delivered_at";

    /// <summary>The analysis singles digest's stamp key (#3712) — the third daily document, same shape.</summary>
    public const string AnalysisSinglesDigestStateKey = "singles_digest_delivered_at";

    /// <summary>The alert pass's own deadline (<c>DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds</c>),
    /// because this read runs inside it: a stamp read that outlives the pass's budget is a stamp read that
    /// should have failed toward the memory gate.</summary>
    internal const int CommandTimeoutSeconds = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

    internal const string GetSql = @"
SELECT state_value
FROM collect.collector_state
WHERE server_id = $1
AND   collector_name = $2
AND   state_key = $3";

    internal const string UpsertSql = @"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at";

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgSelfAlertDeliveryStampStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    public async Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(stateKey);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(GetSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = stateKey });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        if (result is not string text)
        {
            return null;
        }

        if (!DateTime.TryParseExact(text, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var stamp))
        {
            _logger?.LogWarning(
                "Self-alert delivery stamp {Key} holds '{Value}', which is not a round-trip UTC instant; treating it as no stamp",
                stateKey, text);
            return null;
        }

        /* "O" with its Z parses straight to Kind=Utc; a hand-written value carrying an offset lands as Local
           and is converted rather than trusted, so the caller's `now - stamp` is a UTC-to-UTC subtraction
           either way. */
        return stamp.Kind == DateTimeKind.Utc ? stamp : stamp.ToUniversalTime();
    }

    public async Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(stateKey);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(UpsertSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = stateKey });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            /* Stamped Utc before formatting so the text always carries Z — a Kind-Unspecified caller value
               would otherwise format without a zone and read back as Unspecified. */
            Value = DateTime.SpecifyKind(deliveredAtUtc, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture),
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            /* Naive UTC, Kind-Unspecified — the product-wide PG `timestamp` discipline (PgAlertStateStore.
               NaiveUtcNow, the runner's own SaveCollectorStateAsync): binding Kind=Utc against a `timestamp`
               column does not fail, Npgsql infers timestamptz and PostgreSQL casts it into the SERVER's zone,
               so the row lands silently offset while every other timestamp in the store is UTC. */
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
