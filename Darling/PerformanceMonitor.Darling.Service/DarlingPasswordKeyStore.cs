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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>What <see cref="DarlingPasswordKeyStore.SnapshotLegacyPinsAsync"/> did.</summary>
internal sealed record LegacyPinSnapshot(string MarkerState, int Pinned);

/// <summary>
/// The service's reads and writes of the password key tables (#5366): the published key, the per-host state row, the
/// legacy pin snapshot and the reset. Every write runs as the store owner (the service's own connection); the
/// owner-only triggers refuse everyone else, and <see cref="CheckTriggersAsync"/> confirms they are all still in place.
/// </summary>
internal static class DarlingPasswordKeyStore
{
    /// <summary>The age after which a service host's state row is deleted.</summary>
    internal const int StaleServiceRowDays = 30;

    /// <summary>The server id the mail-server password's pin carries.</summary>
    internal const int SmtpPinServerId = 0;

    private const string TriggerCatalogSql = @"
SELECT c.relname, t.tgname, t.tgenabled::text
FROM pg_catalog.pg_trigger AS t
JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE NOT t.tgisinternal
  AND n.nspname = 'config'
  AND c.relname IN ('password_key', 'password_key_service', 'legacy_secret_pin', 'legacy_secret_pin_marker');";

    /// <summary>
    /// Checks that the four key tables have exactly the eight owner-only triggers, each enabled for every session
    /// state (<c>tgenabled = 'A'</c>). Returns null when they do, or a plain sentence saying what is not as expected.
    /// </summary>
    internal static async Task<string?> CheckTriggersAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var found = new Dictionary<string, string>(StringComparer.Ordinal);
        var extra = new List<string>();
        await using (var command = new NpgsqlCommand(TriggerCatalogSql, c))
        {
            command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var table = reader.GetString(0);
                var name = reader.GetString(1);
                var enabled = reader.GetString(2);
                var expected = false;
                foreach (var (expectedTable, expectedName) in PasswordKeyTables.OwnerOnlyTriggers)
                {
                    if (string.Equals(expectedTable, table, StringComparison.Ordinal)
                        && string.Equals(expectedName, name, StringComparison.Ordinal))
                    {
                        expected = true;
                        break;
                    }
                }

                if (expected)
                {
                    found[name] = enabled;
                }
                else
                {
                    extra.Add(name);
                }
            }
        }

        foreach (var (_, trigger) in PasswordKeyTables.OwnerOnlyTriggers)
        {
            if (!found.TryGetValue(trigger, out var enabled))
            {
                return $"The password key tables are missing their protection (trigger {trigger} is not there).";
            }

            if (!string.Equals(enabled, "A", StringComparison.Ordinal))
            {
                return $"The password key tables are not fully protected (trigger {trigger} is not enabled in every session state).";
            }
        }

        return extra.Count > 0
            ? $"The password key tables have a trigger that was not part of the store's design ({extra[0]})."
            : null;
    }

    /// <summary>The published key whose state is <c>current</c>, or null when there is none.</summary>
    internal static async Task<PublishedKey?> ReadCurrentAsync(NpgsqlConnection c, int timeoutSeconds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PasswordKeyTables.ReadCurrentSql, c);
        command.CommandTimeout = timeoutSeconds;
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        return new PublishedKey(reader.GetString(0), (byte[])reader.GetValue(1));
    }

    /// <summary>True when the store holds a <c>replaced</c> row for this key id: a key that was reset or rotated away.</summary>
    internal static async Task<bool> IsReplacedAsync(NpgsqlConnection c, string keyId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM config.password_key WHERE key_id = $1 AND state = 'replaced');", c);
        command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
        command.Parameters.AddWithValue(keyId);
        return (bool)(await command.ExecuteScalarAsync(ct).ConfigureAwait(false))!;
    }

    /// <summary>Publishes a key as the current one. Throws a unique-violation <see cref="PostgresException"/> when
    /// another key is already current.</summary>
    internal static async Task PublishAsync(NpgsqlConnection c, string keyId, byte[] spki, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ($1, $2, $3, 'current');", c);
        command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
        command.Parameters.AddWithValue(keyId);
        command.Parameters.AddWithValue(spki);
        command.Parameters.AddWithValue(PasswordSeal.Algorithm);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Writes this service host's row, then deletes the rows no service has updated for
    /// <see cref="StaleServiceRowDays"/> days.</summary>
    internal static async Task WriteServiceStateAsync(
        NpgsqlConnection c, string serviceHost, string? keyId, string state, string? note, int timeoutSeconds, CancellationToken ct)
    {
        await using (var upsert = new NpgsqlCommand(@"
INSERT INTO config.password_key_service (service_host, key_id, state, note, updated_at)
VALUES ($1, $2, $3, $4, now() AT TIME ZONE 'UTC')
ON CONFLICT (service_host) DO UPDATE
SET key_id = EXCLUDED.key_id, state = EXCLUDED.state, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at;", c))
        {
            upsert.CommandTimeout = timeoutSeconds;
            upsert.Parameters.AddWithValue(serviceHost);
            upsert.Parameters.AddWithValue((object?)keyId ?? DBNull.Value);
            upsert.Parameters.AddWithValue(state);
            upsert.Parameters.AddWithValue((object?)note ?? DBNull.Value);
            await upsert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var purge = new NpgsqlCommand(
            "DELETE FROM config.password_key_service WHERE updated_at < (now() AT TIME ZONE 'UTC') - make_interval(days => $1);", c);
        purge.CommandTimeout = timeoutSeconds;
        purge.Parameters.AddWithValue(StaleServiceRowDays);
        await purge.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>How many saved passwords are sealed to a key: the sealed server, remediation and mail-server values.</summary>
    internal static async Task<int> CountSealedPasswordsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT (SELECT count(*) FROM config.config_monitored_servers WHERE starts_with(coalesce(encrypted_password, ''), 'sealed:'))
     + (SELECT count(*) FROM config.config_monitored_servers WHERE starts_with(coalesce(remediation_encrypted_password, ''), 'sealed:'))
     + (SELECT count(*) FROM config.config_notification WHERE starts_with(coalesce(smtp_encrypted_password, ''), 'sealed:'));", c);
        command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Marks the current key replaced, with the reason <c>reset</c>, and returns the key id it replaced, or null when
    /// there was no current key. Key files are never touched here: the next start retires the file and makes a new key.
    /// </summary>
    internal static async Task<string?> MarkCurrentResetAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
UPDATE config.password_key
SET state = 'replaced', replaced_reason = 'reset', replaced_at = now() AT TIME ZONE 'UTC'
WHERE state = 'current'
RETURNING key_id;", c);
        command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
        return (string?)await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The legacy pin snapshot, in one transaction. The marker row is locked first. On <c>pending</c> with
    /// <paramref name="windows"/> true, every non-empty value that is not a reference, not sealed, and that
    /// <paramref name="unprotect"/> opens is pinned to its row and connection, then the marker becomes <c>done</c>.
    /// With <paramref name="windows"/> false the marker becomes <c>skipped</c> and nothing is pinned. A marker that is
    /// not <c>pending</c>, or a missing marker row, changes nothing.
    /// </summary>
    internal static async Task<LegacyPinSnapshot> SnapshotLegacyPinsAsync(
        NpgsqlDataSource postgres, bool windows, Func<string, string?> unprotect, ILogger logger, CancellationToken ct)
    {
        await using var c = await postgres.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var tx = await c.BeginTransactionAsync(ct).ConfigureAwait(false);

        string? marker;
        await using (var lockMarker = new NpgsqlCommand(
            "SELECT state FROM config.legacy_secret_pin_marker WHERE id = 1 FOR UPDATE;", c, tx))
        {
            lockMarker.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            marker = (string?)await lockMarker.ExecuteScalarAsync(ct).ConfigureAwait(false);
        }

        if (marker is null || !string.Equals(marker, "pending", StringComparison.Ordinal))
        {
            await tx.RollbackAsync(ct).ConfigureAwait(false);
            return new LegacyPinSnapshot(marker ?? "none", 0);
        }

        var pinned = 0;
        if (windows)
        {
            pinned = await PinServersAsync(c, tx, unprotect, logger, ct).ConfigureAwait(false);
            pinned += await PinSmtpAsync(c, tx, unprotect, logger, ct).ConfigureAwait(false);
        }

        var next = windows ? "done" : "skipped";
        await using (var update = new NpgsqlCommand(
            "UPDATE config.legacy_secret_pin_marker SET state = $1, changed_at = now() AT TIME ZONE 'UTC' WHERE id = 1;", c, tx))
        {
            update.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            update.Parameters.AddWithValue(next);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await tx.CommitAsync(ct).ConfigureAwait(false);
        return new LegacyPinSnapshot(next, pinned);
    }

    private static async Task<int> PinServersAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, Func<string, string?> unprotect, ILogger logger, CancellationToken ct)
    {
        var rows = new List<(int ServerId, ServerConnectionIdentity Identity, string? Password, string? RemediationUser, string? RemediationPassword)>();
        await using (var read = new NpgsqlCommand(
            $"SELECT server_id, {ServerConnectionIdentity.StoredColumns}, encrypted_password, remediation_username, remediation_encrypted_password " +
            "FROM config.config_monitored_servers ORDER BY server_id;", c, tx))
        {
            read.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            await using var reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var identity = ServerConnectionIdentity.FromStoredColumns(
                    Text(reader, 1), reader.IsDBNull(2) ? null : reader.GetInt32(2), Text(reader, 3), Text(reader, 4),
                    !reader.IsDBNull(5) && reader.GetBoolean(5), Text(reader, 6), Text(reader, 7), Text(reader, 8),
                    !reader.IsDBNull(9) && reader.GetBoolean(9), !reader.IsDBNull(10) && reader.GetBoolean(10));
                rows.Add((reader.GetInt32(0), identity, Text(reader, 11), Text(reader, 12), Text(reader, 13)));
            }
        }

        var pinned = 0;
        foreach (var row in rows)
        {
            if (CanPin(row.Password, unprotect, logger))
            {
                await InsertPinAsync(c, tx, row.ServerId, "server", row.Password!, PasswordBinding.ForServer(row.Identity), ct).ConfigureAwait(false);
                pinned++;
            }

            if (CanPin(row.RemediationPassword, unprotect, logger))
            {
                await InsertPinAsync(
                    c, tx, row.ServerId, "remediation", row.RemediationPassword!,
                    PasswordBinding.ForRemediation(row.Identity, row.RemediationUser), ct).ConfigureAwait(false);
                pinned++;
            }
        }

        return pinned;
    }

    private static async Task<int> PinSmtpAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, Func<string, string?> unprotect, ILogger logger, CancellationToken ct)
    {
        string? host = null;
        int port = 0;
        bool useSsl = false;
        string? username = null;
        string? password = null;
        await using (var read = new NpgsqlCommand(
            "SELECT smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password FROM config.config_notification WHERE id = 1;", c, tx))
        {
            read.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            await using var reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return 0;
            }

            host = Text(reader, 0);
            port = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            useSsl = !reader.IsDBNull(2) && reader.GetBoolean(2);
            username = Text(reader, 3);
            password = Text(reader, 4);
        }

        if (!CanPin(password, unprotect, logger))
        {
            return 0;
        }

        await InsertPinAsync(c, tx, SmtpPinServerId, "smtp", password!, PasswordBinding.ForSmtp(host, port, useSsl, username), ct).ConfigureAwait(false);
        return 1;
    }

    // A value is pinned only when it is a legacy blob this machine can open: not empty, not an env:/file: reference, not
    // already sealed, and one the protector unprotects without throwing.
    private static bool CanPin(string? stored, Func<string, string?> unprotect, ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(stored) || DarlingSecretSource.IsReference(stored) || PasswordSeal.IsSealed(stored))
        {
            return false;
        }

        try
        {
            return unprotect(stored) is not null;
        }
        catch (Exception ex) when (ex is CryptographicException or FormatException or PlatformNotSupportedException)
        {
            logger.LogWarning("A saved password could not be opened on this machine, so it was not pinned: {Message}", ex.Message);
            return false;
        }
    }

    private static async Task InsertPinAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, int serverId, string slot, string storedText, PasswordBinding binding, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, slot) DO NOTHING;", c, tx);
        command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(slot);
        command.Parameters.AddWithValue(SHA256.HashData(Encoding.UTF8.GetBytes(storedText)));
        command.Parameters.AddWithValue(binding.LegacyPinHash());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}
