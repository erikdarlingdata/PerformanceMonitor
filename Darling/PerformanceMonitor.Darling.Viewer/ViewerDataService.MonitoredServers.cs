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
    /* The full column list, shared by the upsert (Add/Edit) and the insert-if-absent (migrate-in). The
       toggled-boolean-free VALUES bind every field as a parameter; created_at/modified_at are server-side. */
    private const string MonitoredServerColumns =
        "server_id, name, host, database, auth, username, encrypted_password, encrypt_mode, " +
        "trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases, " +
        "monthly_cost_usd, capture_plans, is_enabled, alert_delivery_mode_override";

    private const string MonitoredServerValues =
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, " +
        "(now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC')";

    /// <summary>Upsert by <c>server_id</c> — the Add/Edit save. ON CONFLICT rewrites every field but
    /// <c>created_at</c>, bumping <c>modified_at</c> (and, via the V17 trigger, <c>config_version</c>).</summary>
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
    modified_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>Insert only when the <c>server_id</c> is absent — the one-time <c>viewer-servers.json</c>
    /// migrate-in. DO NOTHING so it never overwrites a row the service already seeded from darling.json (no
    /// double-seed) or a later viewer edit.</summary>
    public const string MonitoredServerInsertIfAbsentSql = @"
INSERT INTO config_monitored_servers (" + MonitoredServerColumns + @", created_at, modified_at)
VALUES (" + MonitoredServerValues + @")
ON CONFLICT (server_id) DO NOTHING";

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
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override
FROM config_monitored_servers
ORDER BY name";

    /// <summary>One configured server by id (the Edit prefill, incl. the DPAPI blob for the password box). $1 server_id.
    /// An <c>admin</c>-role action — the read-only <c>viewer</c> role is column-denied <c>encrypted_password</c>
    /// (#1416), so a viewer seat uses <see cref="MonitoredServerByIdNoSecretSql"/> instead.</summary>
    public const string MonitoredServerByIdSql = @"
SELECT server_id, name, host, database, auth, username, encrypted_password, encrypt_mode,
       trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override
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
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override
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
    /// </summary>
    public const string MonitoredServerByAddressSql = @"
SELECT server_id, name, host, database, auth, username, encrypt_mode,
       trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
       monthly_cost_usd, capture_plans, is_enabled, created_at, alert_delivery_mode_override
FROM config_monitored_servers
WHERE host = $1
AND   database IS NOT DISTINCT FROM $2
AND   read_only_intent = $3";

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
    COALESCE(s.sql_engine_edition, 0) AS sql_engine_edition
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
        if (!await IsConfigSeededAsync(cancellationToken))
        {
            return await GetServersAsync(cancellationToken);
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
                reader.IsDBNull(7) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(7)));
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
    /// (<c>host[:database][:RO]</c>). Identical to the service's seed
    /// (<c>ServerIdHelper.GetDeterministicHashCode(server.StorageName)</c>) and the collectors' stamp, so the
    /// row JOINs collected data and the service reconcile matches it. Pure — unit-testable.
    /// </summary>
    public static int ComputeServerId(string host, string? database, bool readOnlyIntent) =>
        ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(host, database, readOnlyIntent));

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
        string host, string? database, bool readOnlyIntent, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MonitoredServerByAddressSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = host });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)database ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = readOnlyIntent });
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
    /// Upserts a server definition (Add / Edit save). The row's <c>server_id</c> is (re)derived from
    /// host/database/read-only-intent so a definition always lands on its shared identity; the caller passes
    /// a row whose <see cref="MonitoredServerRow.ServerId"/> is already <see cref="ComputeServerId"/>.
    /// </summary>
    public async Task UpsertMonitoredServerAsync(MonitoredServerRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var command = _dataSource.CreateCommand(MonitoredServerUpsertSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        BindMonitoredServer(command, row);
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Inserts a server definition only when its <c>server_id</c> is absent — the migrate-in. Returns true
    /// when a row was actually written (so the caller can count the imported servers), false when the id
    /// already existed (the service seed or a prior migrate already has it).
    /// </summary>
    public async Task<bool> InsertMonitoredServerIfAbsentAsync(MonitoredServerRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var command = _dataSource.CreateCommand(MonitoredServerInsertIfAbsentSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        BindMonitoredServer(command, row);
        return await ExecuteWriteAsync(command, cancellationToken) > 0;
    }

    /// <summary>Removes a server definition by id (the Remove action).</summary>
    public async Task DeleteMonitoredServerAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MonitoredServerDeleteSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await ExecuteWriteAsync(command, cancellationToken);
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

    /// <summary>Binds the 15 upsert/insert parameters ($1..$15) from a row (created_at/modified_at are server-side).</summary>
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
    public int ServerId { get; set; }
    public string Name { get; set; } = "";
    public string Host { get; set; } = "";
    public string? Database { get; set; }

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
