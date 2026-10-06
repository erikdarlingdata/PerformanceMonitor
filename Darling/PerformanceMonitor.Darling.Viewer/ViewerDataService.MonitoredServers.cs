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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane writes to <c>config.config_monitored_servers</c> — the desired-state twin of
/// the observed <c>collect.servers</c> registry the Stage-1 <c>StoreConfigProvider</c> reads and reconciles
/// its monitored set from on every reload beacon. Adding/editing a server here makes the running Darling
/// service COLLECT it (on its next ~15s sweep); removing stops it; the enable flag drives collection. This
/// is the write half of the Stage-3 rewire that replaces the severed <c>viewer-servers.json</c>.
///
/// <para>Mirrors <see cref="GetMuteRulesAsync"/>'s discipline exactly: public-const SQL (so Darling.Tests
/// pin the dialect + column parity with the service's <c>StoreConfigProvider</c> without a live Postgres),
/// all values bound as <c>$N</c> parameters, writes routed through <see cref="ExecuteWriteAsync"/> so a
/// read-only <c>viewer</c> seat degrades to <see cref="ViewerReadOnlyException"/> (SQLSTATE 42501) rather
/// than a silent no-op. Timestamps are set server-side with <c>now() AT TIME ZONE 'UTC'</c> (naive UTC,
/// matching the DDL defaults and the command executor). The bare table name resolves through the
/// <c>collect,config,public</c> search_path to <c>config.config_monitored_servers</c>.</para>
///
/// <para><b>Identity.</b> <c>server_id</c> is <c>ServerIdHelper.GetDeterministicHashCode(BuildStorageName(
/// host, database, readOnlyIntent))</c> — the SAME identity the collectors stamp and the service's seed uses
/// (<see cref="ComputeServerId"/>), so a viewer-written row JOINs the collected data and the service's
/// reconcile matches it. <b>Secrets.</b> <c>encrypted_password</c> is a DPAPI-LocalMachine blob produced by
/// <see cref="ViewerServerSecret"/> (never plaintext); integrated auth stores none. Azure/Entra auth modes
/// are not written — the service can't honor them (see <see cref="ServerStoreCredential"/>).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /* The full column list, shared by the upsert (Edit) and the insert-if-absent (Add and migrate-in). The
       toggled-boolean-free VALUES bind every field as a parameter; created_at/modified_at are server-side.
       #3499: engine + port joined the list — the V70 columns the MCP add_servers tool and the service seed
       had been writing all along, which the viewer's writes silently defaulted ('sqlserver'/0) and its reads
       never surfaced. That default is exactly why the dialog could not author a PostgreSQL target: the row
       it produced was a SQL Server row whatever the operator meant. */
    private const string MonitoredServerColumns =
        "server_id, name, host, database, auth, username, encrypted_password, encrypt_mode, " +
        "trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases, " +
        "monthly_cost_usd, capture_plans, is_enabled, alert_delivery_mode_override, engine, port";

    private const string MonitoredServerValues =
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, " +
        "(now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC')";

    /// <summary>Upsert by <c>server_id</c> — the Edit save. ON CONFLICT rewrites every field but
    /// <c>created_at</c>, bumping <c>modified_at</c> (and, via the V17 trigger, <c>config_version</c>). An Add does
    /// NOT use it (#4789): two different servers can hash to one <c>server_id</c>, and this statement would
    /// rewrite the one already holding it.</summary>
    public const string MonitoredServerUpsertSql = @"
INSERT INTO config_monitored_servers (" + MonitoredServerColumns + @", created_at, modified_at)
VALUES (" + MonitoredServerValues + @")
ON CONFLICT (server_id) DO UPDATE SET
    name = EXCLUDED.name,
    host = EXCLUDED.host,
    database = EXCLUDED.database,
    auth = EXCLUDED.auth,
    username = EXCLUDED.username,
    encrypted_password = EXCLUDED.encrypted_password,
    encrypt_mode = EXCLUDED.encrypt_mode,
    trust_server_certificate = EXCLUDED.trust_server_certificate,
    read_only_intent = EXCLUDED.read_only_intent,
    multi_subnet_failover = EXCLUDED.multi_subnet_failover,
    excluded_databases = EXCLUDED.excluded_databases,
    monthly_cost_usd = EXCLUDED.monthly_cost_usd,
    capture_plans = EXCLUDED.capture_plans,
    is_enabled = EXCLUDED.is_enabled,
    alert_delivery_mode_override = EXCLUDED.alert_delivery_mode_override,
    engine = EXCLUDED.engine,
    port = EXCLUDED.port,
    modified_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>Insert only when the <c>server_id</c> is absent — the Add save (#4789) and the one-time
    /// <c>viewer-servers.json</c> migrate-in. DO NOTHING so it never overwrites a row the service already seeded
    /// from darling.json (no double-seed), a later viewer edit, or a different server that hashes to the same id.</summary>
    public const string MonitoredServerInsertIfAbsentSql = @"
INSERT INTO config_monitored_servers (" + MonitoredServerColumns + @", created_at, modified_at)
VALUES (" + MonitoredServerValues + @")
ON CONFLICT (server_id) DO NOTHING";

    /// <summary>
    /// The one lock every write that gives a definition an ADDRESS takes (#5240): the service's add and edit
    /// (web and MCP) and this class's <see cref="AddMonitoredServerAsync"/> and <see cref="UpsertMonitoredServerAsync"/>.
    /// A COPY of the service's <c>DarlingMcpServerAdminTools.IdentityLockSql</c>, because this project cannot
    /// reference the service; <c>ServerIdentityLockSourcePinTests</c> reads both files and fails when the two texts
    /// differ, since a viewer locking on another key would serialise with nothing.
    ///
    /// <para><c>server_id</c> is the hash of the storage key, an edit keeps its row's old id, and the table has no
    /// unique index on the key, so nothing in the database stops two writers from each finding an address free and
    /// each taking it. Taken first inside the write's transaction, the lock makes the occupancy check that follows
    /// it a check against a table no other identity write is changing, and the transaction's end (commit or
    /// rollback) releases it. <c>pg_advisory_xact_lock</c> and <c>hashtext</c> are executable by PUBLIC, so the
    /// read-only <c>viewer</c> role needs no grant: its INSERT still fails with SQLSTATE 42501, as it always did.</para>
    /// </summary>
    public const string MonitoredServerIdentityLockSql = "SELECT pg_advisory_xact_lock(hashtext('config_monitored_servers.identity'))";

    /// <summary>Deletes a server definition (the Remove action) — the service drops it from the monitored
    /// set on its next reload. $1 server_id.</summary>
    public const string MonitoredServerDeleteSql = "DELETE FROM config_monitored_servers WHERE server_id = $1";

    /// <summary>Flips just <c>is_enabled</c> without rewriting the other columns (the enable toggle). $1
    /// server_id, $2 enabled.</summary>
    public const string MonitoredServerSetEnabledSql =
        "UPDATE config_monitored_servers SET is_enabled = $2, modified_at = (now() AT TIME ZONE 'UTC') WHERE server_id = $1";

    /// <summary>Rewrites just <c>excluded_databases</c> (the Excluded Databases editor). $1 server_id, $2 text[].</summary>
    public const string MonitoredServerSetExcludedDatabasesSql =
        "UPDATE config_monitored_servers SET excluded_databases = $2, modified_at = (now() AT TIME ZONE 'UTC') WHERE server_id = $1";

    /// <summary>All configured servers, for the Manage Servers list. Ordered by display name. Deliberately
    /// OMITS <c>encrypted_password</c>: the list is a display / sidebar-reconcile read that never needs the
    /// secret, and #1416 revoked the read-only <c>viewer</c> role's SELECT on that column — selecting it here
    /// would fail the whole list with SQLSTATE 42501 for a read-only seat. The Edit dialog reloads the row
    /// (including the DPAPI blob) by id via <see cref="MonitoredServerByIdSql"/>, which is an admin-role action.</summary>
    public const string MonitoredServersSelectSql = @"
SELECT server_id, name, host, database, auth, username, encrypt_mode,
       trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override, engine, port
FROM config_monitored_servers
ORDER BY name";

    /// <summary>One configured server by id (the Edit prefill, incl. the DPAPI blob for the password box). $1 server_id.
    /// An <c>admin</c>-role action — the read-only <c>viewer</c> role is column-denied <c>encrypted_password</c>
    /// (#1416), so a viewer seat uses <see cref="MonitoredServerByIdNoSecretSql"/> instead.</summary>
    public const string MonitoredServerByIdSql = @"
SELECT server_id, name, host, database, auth, username, encrypted_password, encrypt_mode,
       trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override, engine, port
FROM config_monitored_servers
WHERE server_id = $1";

    /// <summary>One configured server by id WITHOUT the <c>encrypted_password</c> secret — the read-only
    /// <c>viewer</c> Edit prefill (D7). The viewer role lost SELECT on that column (#1416), so the full
    /// <see cref="MonitoredServerByIdSql"/> would 42501 for a read-only seat; this projection matches the
    /// secret-free LIST columns (so <see cref="ReadMonitoredServerRowNoSecret"/> reads it), leaving the
    /// password box empty (the read-only seat can't save an edit anyway). $1 server_id.</summary>
    public const string MonitoredServerByIdNoSecretSql = @"
SELECT server_id, name, host, database, auth, username, encrypt_mode,
       trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override, engine, port
FROM config_monitored_servers
WHERE server_id = $1";

    /// <summary>
    /// One configured server by its ADDRESS — host, database and read-only intent (#2158). The collision check
    /// the Add/Edit save runs before writing.
    ///
    /// <para><b>Why by address and not by derived id.</b> The guard used to look the address's
    /// <see cref="ComputeServerId"/> hash up by <c>server_id</c>, which only works while every row's id still
    /// equals the hash of its own address. Once an edit PRESERVES a row's identity — which is the point of
    /// #2158, so a re-addressed server keeps its collected history — that stops being true, and a hash lookup
    /// would miss the very row it is meant to protect: two registrations would end up pointing at one real
    /// instance, which is #2228's shape. Matching the address columns asks the question the guard actually
    /// means.</para>
    ///
    /// <para><c>IS NOT DISTINCT FROM</c> for <c>database</c> because it is nullable and NULL = NULL is unknown
    /// in SQL: a plain <c>=</c> would never match the server-scoped registrations (the common case), so every
    /// one of them would read as "address free". Secret-free projection — the caller only needs to know whether
    /// a row exists and which id it has, so this runs for a read-only seat too.</para>
    ///
    /// <para><b>Engine and port are part of the address (#3499, the #2218 identity applied here).</b> Without
    /// them, the first PostgreSQL target the dialog can author would be refused whenever a SQL Server
    /// registration already lives on the same host — a valid pair rejected because the guard compared a
    /// NARROWER identity than the product keys on, the exact defect #2218 fixed in the MCP tool's dedupe gate.</para>
    ///
    /// <para><b>Engine is compared as a KIND ($4, boolean "is PostgreSQL"), not as the raw string.</b> The
    /// stored spelling is whatever onboarded the row ("postgres" from the dialogs and MCP, but a darling.json
    /// seed persists its raw text — "pg", "aurora-postgresql", "PostgreSQL"), while the IDENTITY folds every
    /// spelling to one token (<c>ServerIdHelper.EngineToken</c>). A raw compare would miss a same-identity row
    /// under a different spelling, and since the upsert lands ON CONFLICT (server_id), the guard's miss is not
    /// a duplicate row but a silent CLOBBER of the existing registration — the exact write this guard exists
    /// to refuse. The IN list is the TargetEngine parse, spelled in SQL; everything else is SQL Server, the
    /// same unrecognized-means-SqlServer rule. <c>port</c> is a plain <c>=</c>: V70 declared both columns NOT
    /// NULL with defaults, so there is no NULL arm to miss.</para>
    /// </summary>
    public const string MonitoredServerByAddressSql = @"
SELECT server_id, name, host, database, auth, username, encrypt_mode,
       trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override, engine, port
FROM config_monitored_servers
WHERE host = $1
AND   database IS NOT DISTINCT FROM $2
AND   read_only_intent = $3
AND   (lower(btrim(engine)) IN ('postgres', 'postgresql', 'pg', 'aurora-postgresql', 'aurora')) = $4
AND   port = $5";

    /// <summary>Row count — the migrate-in / reconcile "is the config-server set seeded yet?" guard.</summary>
    public const string MonitoredServersCountSql = "SELECT COUNT(*) FROM config_monitored_servers";

    /// <summary>
    /// Whether the service has seeded the config store yet — <c>config_service</c> is written LAST in the
    /// Stage-1 seed (its presence marks the seed complete). Distinguishes a genuinely empty managed set (the
    /// user removed every server) from a pre-Stage-1 store the service has never seeded, so the sidebar
    /// reconcile can be config-authoritative in the former and fall back to <c>collect.servers</c> in the
    /// latter (never worse than today).
    /// </summary>
    public const string ConfigSeededSql = "SELECT EXISTS (SELECT 1 FROM config_service WHERE id = 1)";

    /// <summary>
    /// The managed server set the sidebar shows, sourced from the DESIRED-state
    /// <c>config_monitored_servers</c> and enriched with the OBSERVED <c>collect.servers</c> facts (SQL major
    /// version, and the collected server/display names) by the shared <c>server_id</c>. This makes a
    /// viewer-added server appear immediately (before its first collection) and a removed one disappear at
    /// once, instead of waiting for the service's next reconcile. <c>is_enabled</c> and <c>monthly_cost_usd</c>
    /// come from config so a viewer toggle/edit is reflected without a round-trip through the service.
    ///
    /// <para><b>The engine discriminator comes from the OBSERVED side (#2530).</b> <c>engine_kind</c> and
    /// <c>sql_engine_edition</c> are read from <c>collect.servers</c>, not from
    /// <c>config_monitored_servers.engine</c>, which is the DESIRED configuration and cannot carry
    /// Aurora-ness at all - that is probed from <c>aurora_version</c> at connect. A server the operator
    /// added but the service has not connected to yet therefore has NO kind, which is the honest answer:
    /// it gets the SQL Server tab set by default, exactly as it did before this column existed.</para>
    ///
    /// <para>This query, not <c>ServersSql</c>, is what the sidebar uses on any seeded store - i.e. every
    /// real deployment - so the discriminator has to be on BOTH or the viewer would have kept rendering
    /// SQL Server tabs at every PostgreSQL target while a unit test over the other query passed.</para>
    ///
    /// <para><b>And that is exactly how #3145 stayed reachable.</b> The engine discriminator did land on
    /// both queries; <c>postgres_major_version</c> (V100) landed on neither, so the sidebar's version label
    /// had only <c>sql_major_version</c> to work from and rendered "SQL Server v0" at a PostgreSQL target
    /// whose tab set, chip and card were already correct. It is read from the OBSERVED side beside the kind,
    /// for the same reason: a probed fact lives on <c>collect.servers</c>, and the desired-state config plane
    /// cannot carry one.</para>
    ///
    /// <para><c>created_date</c> (#3967) is read from the OBSERVED side for the same reason, and on both
    /// queries for the #3145 one: it is the first successful connect, and a server the operator added that the
    /// service has not reached has none, which keeps its dot on "Awaiting first collection".</para>
    /// </summary>
    public const string ManagedServersSql = @"
SELECT
    c.server_id,
    COALESCE(s.server_name, c.host) AS server_name,
    COALESCE(s.display_name, c.name) AS display_name,
    c.is_enabled,
    s.sql_major_version,
    c.monthly_cost_usd,
    s.engine_kind,
    COALESCE(s.sql_engine_edition, 0) AS sql_engine_edition,
    s.postgres_major_version,
    s.created_date
FROM config_monitored_servers c
LEFT JOIN servers s ON s.server_id = c.server_id
ORDER BY COALESCE(s.display_name, c.name)";

    /// <summary>
    /// The managed server set for the sidebar. Config-authoritative once the service has seeded the store
    /// (added servers appear, removed ones disappear); on a pre-seed store (<c>config_service</c> absent) it
    /// falls back to the observed <see cref="GetServersAsync"/> read so the viewer never shows fewer servers
    /// than it does today.
    /// </summary>
    public async Task<List<DarlingServer>> GetManagedServersAsync(CancellationToken cancellationToken = default)
    {
        return await GetConfigManagedServersAsync(cancellationToken) ?? await GetServersAsync(cancellationToken);
    }

    /// <summary>
    /// The config half of <see cref="GetManagedServersAsync"/>: the managed set when the store is seeded, and
    /// null when <see cref="IsConfigSeededAsync"/> says no. That covers a pre-seed store AND a seeded check
    /// that failed, which the check cannot tell apart. Null rather than the observed fallback, for a caller
    /// that must act only on the config list: the server-list sync compares these ids with the ones loaded,
    /// and the observed list lacks every configured server that has never collected, so comparing it would
    /// read each of those as removed.
    /// </summary>
    public async Task<List<DarlingServer>?> GetConfigManagedServersAsync(CancellationToken cancellationToken = default)
    {
        if (!await IsConfigSeededAsync(cancellationToken))
        {
            return null;
        }

        var servers = new List<DarlingServer>();

        await using var command = _dataSource.CreateCommand(ManagedServersSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverName = reader.GetString(1);
            servers.Add(new DarlingServer(
                reader.GetInt32(0),
                serverName,
                reader.IsDBNull(2) ? serverName : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetDateTime(9)));
        }

        return servers;
    }

    /// <summary>
    /// Whether the service has seeded the config store (the reconcile fallback signal). Fails safe to
    /// <c>false</c> on ANY non-cancellation error — most importantly a pre-V17 store where
    /// <c>config_service</c> does not exist yet (Postgres 42P01): a rolling upgrade where the viewer is newer
    /// than the service must degrade to the observed <see cref="GetServersAsync"/> read, never blank the
    /// sidebar. Mirrors <see cref="DetectReadOnlyAsync"/>'s fail-safe posture.
    /// </summary>
    public async Task<bool> IsConfigSeededAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(ConfigSeededSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// The shared-identity <c>server_id</c> for a server: FNV-1a hash of the canonical storage name
    /// (<c>host[:database][:pg][:port][:RO]</c>). Identical to the service's seed
    /// (<c>ServerIdHelper.GetDeterministicHashCode(server.StorageName)</c>) and the collectors' stamp, so the
    /// row JOINs collected data and the service reconcile matches it. Pure — unit-testable.
    ///
    /// <para>#3499: engine and port are OPTIONAL with the same inert defaults as the shared helper — a SQL
    /// Server caller passing neither derives the byte-identical name it always did (the #2218 protection),
    /// while a PostgreSQL Add derives the same id the MCP <c>add_servers</c> tool would for the same target,
    /// which is the whole parity contract: one server, one identity, whichever door it came in through.</para>
    /// </summary>
    public static int ComputeServerId(string host, string? database, bool readOnlyIntent, string? engine = null, int port = 0) =>
        ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, engine, port));

    /// <summary>All configured servers (Manage Servers list + the sidebar reconcile source), read without the
    /// <c>encrypted_password</c> secret so a read-only <c>viewer</c> seat can list them (#1416). The Edit dialog
    /// reloads the blob by id via <see cref="GetMonitoredServerAsync"/>.</summary>
    public async Task<List<MonitoredServerRow>> GetMonitoredServersAsync(CancellationToken cancellationToken = default)
    {
        var servers = new List<MonitoredServerRow>();

        await using var command = _dataSource.CreateCommand(MonitoredServersSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            servers.Add(ReadMonitoredServerRowNoSecret(reader));
        }

        return servers;
    }

    /// <summary>One configured server by id, or null when it is not in the store (the Edit prefill). A read-only
    /// <c>viewer</c> seat is column-denied <c>encrypted_password</c> (#1416), so it degrades to the secret-free
    /// <see cref="MonitoredServerByIdNoSecretSql"/> projection (D7) — leaving the password box empty — instead
    /// of failing the prefill with 42501.</summary>
    public async Task<MonitoredServerRow?> GetMonitoredServerAsync(int serverId, CancellationToken cancellationToken = default)
    {
        if (IsReadOnly)
        {
            await using var noSecretCommand = _dataSource.CreateCommand(MonitoredServerByIdNoSecretSql);
            noSecretCommand.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            noSecretCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var noSecretReader = await noSecretCommand.ExecuteReaderAsync(cancellationToken);
            return await noSecretReader.ReadAsync(cancellationToken) ? ReadMonitoredServerRowNoSecret(noSecretReader) : null;
        }

        await using var command = _dataSource.CreateCommand(MonitoredServerByIdSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadMonitoredServerRow(reader) : null;
    }

    /// <summary>
    /// The server already registered at this address, or null when the address is free (#2158) — what the
    /// Add/Edit save checks before writing, so one real instance cannot end up under two identities.
    ///
    /// <para>Secret-free by design: the caller compares ids and shows a message, so there is no reason to
    /// read the DPAPI blob, and skipping it means a read-only seat gets the same answer instead of 42501.</para>
    /// </summary>
    public async Task<MonitoredServerRow?> GetMonitoredServerByAddressAsync(
        string host, string? database, bool readOnlyIntent, bool isPostgres = false, int port = 0,
        CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MonitoredServerByAddressSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = host });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)database ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = readOnlyIntent });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = isPostgres });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = port });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadMonitoredServerRowNoSecret(reader) : null;
    }

    /// <summary>How many servers the config-server registry holds (the migrate-in / reconcile guard).</summary>
    public async Task<long> GetMonitoredServerCountAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MonitoredServersCountSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull ? 0 : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Upserts a server definition (the Edit save). An edit keeps its row's <c>server_id</c> even when the address
    /// changes (#2158), so the write lands on the row that already owns it. An ADD does not come through here
    /// (#4789): it goes through <see cref="AddMonitoredServerAsync"/>, which refuses to overwrite a different
    /// server that hashes to the same id.
    ///
    /// <para><b>One transaction, behind the identity lock (#5240).</b> The dialog's address check ran before this
    /// call, so another writer (a web or MCP edit, an add) can claim the address in between, and the upsert's
    /// <c>ON CONFLICT (server_id)</c> arm is no help: an edit keeps its row's old id, so a second row with the same
    /// address is not a conflict on the id. The write therefore takes <see cref="MonitoredServerIdentityLockSql"/>
    /// first, reads every other definition's address again under it, and writes only when the address is still
    /// free. A claimed address throws <see cref="MonitoredServerAddressClaimedException"/> and nothing is written; the
    /// dialog's save handler catches that type on its own and shows its message as it is.</para>
    ///
    /// <para><b>A move never carries the stored password along (#5240).</b> The upsert rewrites
    /// <c>encrypted_password</c> from the row it is given, so a row handed in with the blob it was read with would
    /// send the stored password to a new address. Under the same lock, the write therefore reads the stored host, port
    /// and blob, and when a SQL or service-principal row's host or port differs, it writes only a row that carries a
    /// newly protected password: a missing blob, or the stored one, throws <see cref="MonitoredServerPasswordNeededException"/>
    /// and nothing is written. The rule is the web and MCP edit's (<c>DarlingMcpServerAdminTools.PlanEdit</c> and the
    /// store's edit function), with the same sentence; every other change keeps the stored password as before.</para>
    /// </summary>
    public async Task UpsertMonitoredServerAsync(MonitoredServerRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* The row an edit rewrites is not a claimant of its own address. A refusal throws out of this block and
           the transaction's disposal rolls it back, which releases the lock. */
        var claimant = await TakeIdentityLockAndFindClaimantAsync(connection, transaction, row, row.ServerId, cancellationToken);
        if (claimant is not null)
        {
            throw new MonitoredServerAddressClaimedException(claimant);
        }

        await RefuseStoredPasswordOnMovedReachAsync(connection, transaction, row, cancellationToken);

        await using var command = new NpgsqlCommand(MonitoredServerUpsertSql, connection, transaction);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        BindMonitoredServer(command, row);
        try
        {
            await ExecuteWriteAsync(command, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == StoreMovedKeepingPasswordSqlState)
        {
            /* The store's own rule (a trigger on the table) refused a change of how the server is reached that keeps the
               stored password: the same answer as the check above, for a row that changed between that read and the write. */
            throw new MonitoredServerPasswordNeededException();
        }
        catch (PostgresException ex) when (ex.SqlState == StoreRemediationKeptSqlState)
        {
            /* The store's rule for a row that holds a remediation login: a connection change by this role is refused. */
            throw new MonitoredServerPasswordNeededException(RemediationKeptText);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>The SQLSTATE the store's trigger on <c>config_monitored_servers</c> raises for a change of how a server is
    /// reached that keeps its stored password.</summary>
    internal const string StoreMovedKeepingPasswordSqlState = "PW002";

    /// <summary>The SQLSTATE the store's trigger raises for a change of how a server is reached on a row that holds a
    /// remediation secret, made by a role that does not own the table.</summary>
    internal const string StoreRemediationKeptSqlState = "PW003";

    /// <summary>The sentence a refused change on a row with a remediation login gives: the one in
    /// <see cref="ServerConnectionRule.RemediationKeptText"/>, which the web and MCP edit answer with too.</summary>
    public const string RemediationKeptText = ServerConnectionRule.RemediationKeptText;

    /// <summary>The sentence a refused move gives: the one in <see cref="ServerConnectionRule.PasswordNeededOnMoveText"/>,
    /// which the web and MCP edit answer with too.</summary>
    public const string EditPasswordNeededText = ServerConnectionRule.PasswordNeededOnMoveText;

    /// <summary>The stored connection settings (host, port, engine, database, read-only intent, auth, username, encrypt mode,
    /// trust certificate, multi-subnet failover), whether the stored blob is the one a row carries, and whether the row holds a
    /// remediation secret, for one server id ($1 id, $2 the row's blob). Reads the two secret columns only to compare them
    /// and test for a value, in the write's own transaction: an <c>admin</c>-role write, like the upsert it guards.</summary>
    public const string MonitoredServerStoredReachSql = @"
SELECT host, COALESCE(port, 0), engine, database, read_only_intent, auth, username, encrypt_mode, trust_server_certificate,
       multi_subnet_failover, COALESCE(encrypted_password = $2, false), COALESCE(remediation_encrypted_password, '') <> ''
FROM config_monitored_servers
WHERE server_id = $1";

    /// <summary>The sentence a switch into SQL authentication gives when no new password comes with it: the edit core's
    /// (<c>DarlingMcpServerAdminTools.PlanEdit</c>). The core's sentence is an inline literal, so
    /// <c>ServerEditPasswordRuleViewerTests</c> drives the core and fails if the two texts differ.</summary>
    public const string EditSwitchToSqlNeedsPasswordText = "Switching to SQL authentication needs the password.";

    /// <summary>The service-principal twin of <see cref="EditSwitchToSqlNeedsPasswordText"/>.</summary>
    public const string EditSwitchToServicePrincipalNeedsSecretText =
        "Switching to ServicePrincipal authentication needs the client secret as password.";

    /// <summary>
    /// Refuses (throws <see cref="MonitoredServerPasswordNeededException"/>) a write that changes how a stored server is
    /// reached (any setting <see cref="ServerConnectionRule.ConnectionSettingsDiffer"/> compares: host, port, engine,
    /// database, read-only intent, authentication, username, encrypt mode, trust certificate, multi-subnet failover) before
    /// anything is written. A row that holds a remediation secret refuses any such change with
    /// <see cref="RemediationKeptText"/>, whatever its authentication mode. For a SQL or service-principal row, a change
    /// that carries no newly entered password (the row's blob is missing, or is the stored one) is refused too: a switch
    /// of auth mode gets the core's switch sentence, ahead of the change sentence, as in the core. A Windows or
    /// managed-identity row stores no secret and is otherwise never refused; a row that is not in the store yet has nothing
    /// to reuse. Runs inside the caller's transaction, after the identity lock.
    /// </summary>
    private async Task RefuseStoredPasswordOnMovedReachAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, MonitoredServerRow row, CancellationToken cancellationToken)
    {
        ServerConnectionSettings stored;
        bool carriesStoredBlob;
        bool holdsRemediationSecret;
        try
        {
            await using var command = new NpgsqlCommand(MonitoredServerStoredReachSql, connection, transaction);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.ServerId });
            AddNullableText(command, row.EncryptedPassword);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return;
            }

            stored = ServerConnectionSettings.WithDefaults(
                reader.GetString(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3), !reader.IsDBNull(4) && reader.GetBoolean(4),
                reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7), !reader.IsDBNull(8) && reader.GetBoolean(8),
                !reader.IsDBNull(9) && reader.GetBoolean(9));
            carriesStoredBlob = reader.GetBoolean(10);
            holdsRemediationSecret = reader.GetBoolean(11);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }

        var incoming = ServerConnectionSettings.WithDefaults(
            row.Host, row.Port, row.Engine, row.Database, row.ReadOnlyIntent, row.Auth, row.Username,
            row.EncryptMode, row.TrustServerCertificate, row.MultiSubnetFailover);
        var connectionChanged = ServerConnectionRule.ConnectionSettingsDiffer(incoming, stored);

        if (holdsRemediationSecret && connectionChanged)
        {
            throw new MonitoredServerPasswordNeededException(RemediationKeptText);
        }

        if (!string.Equals(row.Auth, ServerStoreCredential.Sql, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(row.Auth, ServerStoreCredential.ServicePrincipal, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!string.IsNullOrEmpty(row.EncryptedPassword) && !carriesStoredBlob)
        {
            return;
        }

        if (!string.Equals(stored.Auth, row.Auth, StringComparison.OrdinalIgnoreCase))
        {
            throw new MonitoredServerPasswordNeededException(
                string.Equals(row.Auth, ServerStoreCredential.ServicePrincipal, StringComparison.OrdinalIgnoreCase)
                    ? EditSwitchToServicePrincipalNeedsSecretText
                    : EditSwitchToSqlNeedsPasswordText);
        }

        if (connectionChanged)
        {
            throw new MonitoredServerPasswordNeededException();
        }
    }

    /// <summary>The address (storage key) a definition is stored under: the same five columns, folded the same way,
    /// that the collectors, the service's seed and the web and MCP writers key a server on
    /// (<see cref="ComputeServerId"/> hashes it).</summary>
    private static string StorageKeyOf(MonitoredServerRow row) =>
        ServerIdHelper.BuildStorageName(row.Host, row.Database, row.ReadOnlyIntent, row.Engine, row.Port);

    /// <summary>
    /// The first two steps of every identity write (#5240), inside the caller's open transaction: take
    /// <see cref="MonitoredServerIdentityLockSql"/>, then read every definition again (secret-free, the list
    /// projection) and return the one that already holds <paramref name="candidate"/>'s address, or null when the
    /// address is free. The address is compared the way the service's add and edit compare it: the storage key,
    /// case-insensitively. <paramref name="exceptServerId"/> is the row an edit rewrites, which never counts as the
    /// claimant of its own address; an add passes null. The caller ends the transaction, which releases the lock, and a
    /// caller that returns without committing rolls back.
    /// </summary>
    private static async Task<MonitoredServerRow?> TakeIdentityLockAndFindClaimantAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, MonitoredServerRow candidate, int? exceptServerId,
        CancellationToken cancellationToken)
    {
        try
        {
            await using (var identityLock = new NpgsqlCommand(MonitoredServerIdentityLockSql, connection, transaction))
            {
                identityLock.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                await identityLock.ExecuteNonQueryAsync(cancellationToken);
            }

            var candidateKey = StorageKeyOf(candidate);
            await using var definitions = new NpgsqlCommand(MonitoredServersSelectSql, connection, transaction);
            definitions.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            await using var reader = await definitions.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var existing = ReadMonitoredServerRowNoSecret(reader);
                if (existing.ServerId != exceptServerId
                    && string.Equals(StorageKeyOf(existing), candidateKey, StringComparison.OrdinalIgnoreCase))
                {
                    return existing;
                }
            }

            return null;
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            /* The write executors' own translation: a store behind this viewer's schema says so, in words. */
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// Inserts a server definition only when its <c>server_id</c> is absent — the migrate-in. Returns true
    /// when a row was actually written (so the caller can count the imported servers), false when the id
    /// already existed (the service seed or a prior migrate already has it).
    ///
    /// <para>It gives a definition an address, so it is an identity write like <see cref="AddMonitoredServerAsync"/>
    /// (#5240): one transaction behind <see cref="MonitoredServerIdentityLockSql"/>, and an address another
    /// definition already holds, under whatever id, is refused the same way a taken id is: false, nothing written.</para>
    /// </summary>
    public async Task<bool> InsertMonitoredServerIfAbsentAsync(MonitoredServerRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        if (await TakeIdentityLockAndFindClaimantAsync(connection, transaction, row, null, cancellationToken) is not null)
        {
            return false;
        }

        await using var command = new NpgsqlCommand(MonitoredServerInsertIfAbsentSql, connection, transaction);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        BindMonitoredServer(command, row);
        var written = await ExecuteWriteAsync(command, cancellationToken) > 0;
        await transaction.CommitAsync(cancellationToken);
        return written;
    }

    /// <summary>
    /// The Add save (#4789): writes the definition only when its <c>server_id</c> is free, and when it is not,
    /// says what holds the id.
    ///
    /// <para><b>Why not the upsert.</b> The id is a 32-bit hash of the server's identity, so two different
    /// servers can share one. The upsert's <c>ON CONFLICT (server_id) DO UPDATE</c> would rewrite the server
    /// already holding it with the new address: that server stops being monitored and its collected history
    /// shows under the new one. The insert here does nothing on a taken id (<see cref="InsertMonitoredServerIfAbsentAsync"/>),
    /// so the holder is never touched; this method then reads the holder (secret-free, so a read-only seat gets
    /// the same answer) and <see cref="ClassifyAddAgainstOccupant"/> decides whether that is the same server
    /// again or a different one that collides.</para>
    ///
    /// <para>An edit does not come through here: it keeps its row's id when the address changes (#2158) and still
    /// updates in place through <see cref="UpsertMonitoredServerAsync"/>.</para>
    ///
    /// <para><b>One transaction, behind the identity lock (#5240).</b> The id check above only sees a row that holds
    /// THIS id, and an edit keeps its row's old id, so an edit that moved another server onto this address a moment
    /// ago is invisible to it: the insert would land and two servers would hold one address. The add therefore takes
    /// <see cref="MonitoredServerIdentityLockSql"/> first, reads every definition's address again under it, and
    /// answers <see cref="MonitoredServerAddOutcome.Duplicate"/>, naming the definition that holds it, when the address is
    /// claimed: the same meaning the service's add gives a key claimed in the meantime. Only a free address reaches the
    /// insert, in the same transaction, and the holder of a taken id is read in it too.</para>
    /// </summary>
    public async Task<MonitoredServerAddResult> AddMonitoredServerAsync(MonitoredServerRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* A claimed address returns without committing: the transaction's disposal rolls back, which releases the
           lock. A new add has no row of its own to leave out, so no id is excepted. */
        var claimant = await TakeIdentityLockAndFindClaimantAsync(connection, transaction, row, null, cancellationToken);
        if (claimant is not null)
        {
            return new MonitoredServerAddResult(MonitoredServerAddOutcome.Duplicate, claimant);
        }

        int written;
        await using (var insert = new NpgsqlCommand(MonitoredServerInsertIfAbsentSql, connection, transaction))
        {
            insert.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            BindMonitoredServer(insert, row);
            written = await ExecuteWriteAsync(insert, cancellationToken);
        }

        if (written > 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return new MonitoredServerAddResult(MonitoredServerAddOutcome.Added, null);
        }

        MonitoredServerRow? occupant;
        await using (var command = new NpgsqlCommand(MonitoredServerByIdNoSecretSql, connection, transaction))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.ServerId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            occupant = await reader.ReadAsync(cancellationToken) ? ReadMonitoredServerRowNoSecret(reader) : null;
        }

        return new MonitoredServerAddResult(ClassifyAddAgainstOccupant(row, occupant), occupant);
    }

    /// <summary>
    /// What an Add that did not write means (#4789): the row that holds the candidate's <c>server_id</c>
    /// (<paramref name="occupant"/>) is either the same server again (<see cref="MonitoredServerAddOutcome.Duplicate"/>),
    /// a DIFFERENT server whose identity hashes to the same id (<see cref="MonitoredServerAddOutcome.Collides"/>),
    /// or absent (<see cref="MonitoredServerAddOutcome.NotSaved"/>: nothing was written and nothing holds the id,
    /// which is a holder removed between the insert and the read). Never answers
    /// <see cref="MonitoredServerAddOutcome.Added"/>: that is the insert having written. Pure, so it pins without a store.
    ///
    /// <para><b>Same identity is judged the way the dialogs' address check judges it</b>
    /// (<see cref="MonitoredServerByAddressSql"/>): host, database, read-only intent, engine as a KIND
    /// (<see cref="MonitoredServerRow.IsPostgres"/>, so every accepted spelling of PostgreSQL is one engine) and
    /// port. Host and database compare case-sensitively, and a missing database only equals a missing database,
    /// exactly as <c>=</c> and <c>IS NOT DISTINCT FROM</c> do in that query. The display name, credentials and
    /// every other setting are not part of the identity: the same server re-added under a new name is a duplicate.</para>
    /// </summary>
    public static MonitoredServerAddOutcome ClassifyAddAgainstOccupant(MonitoredServerRow candidate, MonitoredServerRow? occupant)
    {
        ArgumentNullException.ThrowIfNull(candidate);

        if (occupant is null)
        {
            return MonitoredServerAddOutcome.NotSaved;
        }

        var sameIdentity =
            string.Equals(candidate.Host, occupant.Host, StringComparison.Ordinal)
            && string.Equals(candidate.Database, occupant.Database, StringComparison.Ordinal)
            && candidate.ReadOnlyIntent == occupant.ReadOnlyIntent
            && candidate.IsPostgres == occupant.IsPostgres
            && candidate.Port == occupant.Port;

        return sameIdentity ? MonitoredServerAddOutcome.Duplicate : MonitoredServerAddOutcome.Collides;
    }

    /// <summary>Removes a server definition by id (the Remove action), and the server's tag assignments in the
    /// same transaction, so a re-added server (same id) does not get its old tags back.</summary>
    public async Task DeleteMonitoredServerAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        foreach (var sql in new[] { MonitoredServerDeleteSql, ServerTagStore.ClearForServerSql })
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await ExecuteWriteAsync(command, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>Toggles a server's <c>is_enabled</c> flag without rewriting its other columns.</summary>
    public async Task SetMonitoredServerEnabledAsync(int serverId, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MonitoredServerSetEnabledSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Rewrites a server's excluded-databases list (the Excluded Databases editor).</summary>
    public async Task SetMonitoredServerExcludedDatabasesAsync(int serverId, IEnumerable<string> excludedDatabases, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MonitoredServerSetExcludedDatabasesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        AddTextArray(command, excludedDatabases);
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Binds the 18 upsert/insert parameters ($1..$18) from a row (created_at/modified_at are server-side).</summary>
    private static void BindMonitoredServer(NpgsqlCommand command, MonitoredServerRow row)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.ServerId });                 // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.Name });                  // $2
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.Host });                  // $3
        AddNullableText(command, row.Database);                                                          // $4
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.Auth });                  // $5
        AddNullableText(command, row.Username);                                                          // $6
        AddNullableText(command, row.EncryptedPassword);                                                 // $7
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.EncryptMode });           // $8
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.TrustServerCertificate });  // $9
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.ReadOnlyIntent });          // $10
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.MultiSubnetFailover });     // $11
        AddTextArray(command, row.ExcludedDatabases);                                                    // $12
        command.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = row.MonthlyCostUsd });       // $13
        AddNullableBool(command, row.CapturePlans);                                                      // $14
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.IsEnabled });               // $15
        /* #1236: per-server delivery override as its enum name, or NULL = "inherit the global". */
        AddNullableText(command, row.AlertDeliveryModeOverride?.ToString());                             // $16
        /* #3499: the engine string rides VERBATIM (like the service seed since V68) so the single parse in
           MonitoredServer.TargetEngine stays the only interpreter; port 0 = the driver's default (5432). */
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.Engine });                 // $17
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.Port });                     // $18
    }

    private static MonitoredServerRow ReadMonitoredServerRow(NpgsqlDataReader reader) => new()
    {
        ServerId = reader.GetInt32(0),
        Name = reader.GetString(1),
        Host = reader.GetString(2),
        Database = reader.IsDBNull(3) ? null : reader.GetString(3),
        Auth = reader.GetString(4),
        Username = reader.IsDBNull(5) ? null : reader.GetString(5),
        EncryptedPassword = reader.IsDBNull(6) ? null : reader.GetString(6),
        EncryptMode = reader.GetString(7),
        TrustServerCertificate = reader.GetBoolean(8),
        ReadOnlyIntent = reader.GetBoolean(9),
        MultiSubnetFailover = reader.GetBoolean(10),
        ExcludedDatabases = reader.IsDBNull(11) ? new List<string>() : reader.GetFieldValue<string[]>(11).ToList(),
        MonthlyCostUsd = reader.GetDecimal(12),
        CapturePlans = reader.IsDBNull(13) ? null : reader.GetBoolean(13),
        IsEnabled = reader.GetBoolean(14),
        CreatedAt = reader.IsDBNull(15) ? null : DateTime.SpecifyKind(reader.GetDateTime(15), DateTimeKind.Utc),
        AlertDeliveryModeOverride = ParseDeliveryOverride(reader.IsDBNull(16) ? null : reader.GetString(16)),
        Engine = reader.GetString(17),
        Port = reader.GetInt32(18),
    };

    /// <summary>
    /// Reads a <see cref="MonitoredServerRow"/> from the secret-free LIST projection
    /// (<see cref="MonitoredServersSelectSql"/>): identical to <see cref="ReadMonitoredServerRow"/> but the
    /// query omits <c>encrypted_password</c> (a read-only <c>viewer</c> seat lost SELECT on it, #1416), so
    /// <see cref="MonitoredServerRow.EncryptedPassword"/> stays null and every column after <c>username</c>
    /// shifts down one ordinal. The Manage Servers grid never displays the secret; the Edit dialog reloads the
    /// blob by id (<see cref="GetMonitoredServerAsync"/>).
    /// </summary>
    private static MonitoredServerRow ReadMonitoredServerRowNoSecret(NpgsqlDataReader reader) => new()
    {
        ServerId = reader.GetInt32(0),
        Name = reader.GetString(1),
        Host = reader.GetString(2),
        Database = reader.IsDBNull(3) ? null : reader.GetString(3),
        Auth = reader.GetString(4),
        Username = reader.IsDBNull(5) ? null : reader.GetString(5),
        EncryptedPassword = null, /* not selected — the LIST read omits the secret column (#1416) */
        EncryptMode = reader.GetString(6),
        TrustServerCertificate = reader.GetBoolean(7),
        ReadOnlyIntent = reader.GetBoolean(8),
        MultiSubnetFailover = reader.GetBoolean(9),
        ExcludedDatabases = reader.IsDBNull(10) ? new List<string>() : reader.GetFieldValue<string[]>(10).ToList(),
        MonthlyCostUsd = reader.GetDecimal(11),
        CapturePlans = reader.IsDBNull(12) ? null : reader.GetBoolean(12),
        IsEnabled = reader.GetBoolean(13),
        CreatedAt = reader.IsDBNull(14) ? null : DateTime.SpecifyKind(reader.GetDateTime(14), DateTimeKind.Utc),
        AlertDeliveryModeOverride = ParseDeliveryOverride(reader.IsDBNull(15) ? null : reader.GetString(15)),
        Engine = reader.GetString(16),
        Port = reader.GetInt32(17),
    };

    private static void AddTextArray(NpgsqlCommand command, IEnumerable<string>? values) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = (values ?? Enumerable.Empty<string>()).ToArray(),
        });

    private static void AddNullableBool(NpgsqlCommand command, bool? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Boolean,
            Value = value.HasValue ? value.Value : DBNull.Value,
        });

    /// <summary>Parses the nullable <c>alert_delivery_mode_override</c> text ("Summary"/"PerEvent") to the enum;
    /// null/empty/unknown = null = "inherit the global delivery mode". Mirrors the service's read parse.</summary>
    private static AlertNotificationMode? ParseDeliveryOverride(string? value) =>
        Enum.TryParse<AlertNotificationMode>(value, ignoreCase: true, out var mode) ? mode : null;
}

/// <summary>
/// A <c>config.config_monitored_servers</c> row as the viewer authors + reads it — the connection definition
/// the Darling service reconstructs a <c>MonitoredServer</c> from. Deliberately carries only the columns the
/// store (and hence the service) has — now INCLUDING the per-server alert-delivery override (#1236, V18, the
/// service honors it at delivery time); the remaining viewer-only cosmetics some Lite fields kept (description,
/// utility DB, the Azure client ids) are NOT part of the service-honored server model and stay out of the store.
/// <see cref="EncryptedPassword"/> is a DPAPI-LocalMachine blob, never plaintext. Favorites remain viewer-local
/// (<see cref="ViewerServerStore"/>).
/// </summary>
public sealed class MonitoredServerRow
{
    /// <summary>The canonical <c>engine</c> tokens the dialogs WRITE — the same two values the MCP
    /// <c>add_servers</c> tool stores. The store may HOLD other accepted spellings (a darling.json seed
    /// persists its raw string, e.g. <c>"aurora-postgresql"</c>), which is why reads go through
    /// <see cref="IsPostgres"/> rather than comparing against these.</summary>
    public const string EngineSqlServer = "sqlserver";
    public const string EnginePostgres = "postgres";

    public int ServerId { get; set; }
    public string Name { get; set; } = "";
    public string Host { get; set; } = "";
    public string? Database { get; set; }

    /// <summary>Which database engine the target runs — <c>config_monitored_servers.engine</c> (V70),
    /// defaulted to SQL Server exactly like <c>MonitoredServer.Engine</c> so every pre-#3499 constructor
    /// call site (the bulk dialog, the migrate-in) keeps writing the row it always wrote. Carried VERBATIM
    /// through an edit — the service's <c>TargetEngine</c> is the one interpreter of the string.</summary>
    public string Engine { get; set; } = EngineSqlServer;

    /// <summary>TCP port for a PostgreSQL target on a non-default port; <c>0</c> (the default) means "the
    /// driver's default", 5432. Unused for SQL Server, which carries a non-default port in the host itself
    /// as <c>host,1433</c> — the same contract as <c>MonitoredServer.Port</c> and the MCP tool's
    /// <c>port</c> field (#2218).</summary>
    public int Port { get; set; }

    /// <summary>True when <see cref="Engine"/> names PostgreSQL under ANY accepted spelling — the same
    /// parse as the service's <c>MonitoredServer.TargetEngine</c> (postgres / postgresql / pg / aurora /
    /// aurora-postgresql, case-insensitive), so the Edit dialog recognizes a row however it was onboarded.
    /// Anything unrecognized reads as SQL Server, mirroring the service: a typo must not flip a row's UI.</summary>
    public bool IsPostgres => Engine?.Trim().ToLowerInvariant() switch
    {
        "postgres" or "postgresql" or "pg" or "aurora-postgresql" or "aurora" => true,
        _ => false,
    };

    /// <summary><see cref="ServerStoreCredential.Integrated"/> or <see cref="ServerStoreCredential.Sql"/>.</summary>
    public string Auth { get; set; } = ServerStoreCredential.Integrated;

    public string? Username { get; set; }

    /// <summary>DPAPI-LocalMachine base64 blob (<see cref="ViewerServerSecret.Protect"/>), or null for integrated auth.</summary>
    public string? EncryptedPassword { get; set; }

    public string EncryptMode { get; set; } = "Mandatory";
    public bool TrustServerCertificate { get; set; }
    public bool ReadOnlyIntent { get; set; }
    public bool MultiSubnetFailover { get; set; }
    public List<string> ExcludedDatabases { get; set; } = new();
    public decimal MonthlyCostUsd { get; set; }

    /// <summary>Per-server plan-capture override; null = follow the global <c>config_service.capture_plans</c>.</summary>
    public bool? CapturePlans { get; set; }

    /// <summary>Per-server deadlock/blocking delivery-mode override (#1236); null = inherit the global
    /// <c>config_alert_settings.delivery_mode</c>. Stored as the enum name in <c>alert_delivery_mode_override</c>.</summary>
    public AlertNotificationMode? AlertDeliveryModeOverride { get; set; }

    public bool IsEnabled { get; set; } = true;

    /// <summary>Server-set creation time (read-only, from the store's <c>created_at</c>); null when not read.</summary>
    public DateTime? CreatedAt { get; set; }
}

/// <summary>How an Add ended (#4789): see <see cref="ViewerDataService.AddMonitoredServerAsync"/>.</summary>
public enum MonitoredServerAddOutcome
{
    /// <summary>The <c>server_id</c> was free and the row was written.</summary>
    Added,

    /// <summary>The SAME server (same host, database, read-only intent, engine and port) is already monitored:
    /// its definition holds this <c>server_id</c>, or holds the address under an older id because an edit moved it
    /// there (#5240), which is what an add that finds the address claimed under the identity lock answers.
    /// Nothing was written.</summary>
    Duplicate,

    /// <summary>The <c>server_id</c> is held by a DIFFERENT server: the two identities hash to one id. Nothing was
    /// written, so the server holding the id is untouched.</summary>
    Collides,

    /// <summary>Nothing was written and nothing holds the id (the holder was removed between the insert and the
    /// read). There is no server to name; the add can simply be tried again.</summary>
    NotSaved,
}

/// <summary>The outcome of <see cref="ViewerDataService.AddMonitoredServerAsync"/> and, when the id was already
/// taken, the secret-free row that holds it (the server to name in the refusal). <see cref="Occupant"/> is null
/// for <see cref="MonitoredServerAddOutcome.Added"/> and <see cref="MonitoredServerAddOutcome.NotSaved"/>.</summary>
public sealed record MonitoredServerAddResult(MonitoredServerAddOutcome Outcome, MonitoredServerRow? Occupant);

/// <summary>
/// <see cref="ViewerDataService.UpsertMonitoredServerAsync"/> refused an edit (#5240): it moves a SQL or
/// service-principal server's connection settings and carries no newly entered password, so the stored one would have been
/// sent to the changed connection, or it changes how a server with a remediation login is reached, or it switches between SQL and service-principal authentication and keeps the stored
/// secret. The message is <see cref="ViewerDataService.EditPasswordNeededText"/> for a change, the switch sentence for
/// a switch, <see cref="ViewerDataService.RemediationKeptText"/> for a remediation row; the dialog's save
/// handler shows it as it is, as it does a claimed address.
/// </summary>
public sealed class MonitoredServerPasswordNeededException : InvalidOperationException
{
    public MonitoredServerPasswordNeededException()
        : base(ViewerDataService.EditPasswordNeededText)
    {
    }

    /// <summary>A refusal with the edit core's switch sentence (<see cref="ViewerDataService.EditSwitchToSqlNeedsPasswordText"/>
    /// or its service-principal twin).</summary>
    public MonitoredServerPasswordNeededException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// <see cref="ViewerDataService.UpsertMonitoredServerAsync"/> refused an edit (#5240): under the identity lock,
/// another definition already holds the address the edit moves this server to, so nothing was written. The same
/// meaning as <see cref="MonitoredServerAddOutcome.Duplicate"/> for an add. The Add/Edit dialog's save handler catches
/// this type before its general catch and shows the message as it is (no "Error saving server:" prefix, nothing
/// logged at error level), so the operator reads why without new UI.
/// </summary>
public sealed class MonitoredServerAddressClaimedException : InvalidOperationException
{
    public MonitoredServerAddressClaimedException(MonitoredServerRow claimant)
        : base(
            "Already monitored: another change to the server list claimed this address (host, database, read-only intent, " +
            "engine and port) while this one was saving, so nothing was changed.")
    {
        Claimant = claimant;
    }

    /// <summary>The secret-free row that holds the address.</summary>
    public MonitoredServerRow Claimant { get; }
}
