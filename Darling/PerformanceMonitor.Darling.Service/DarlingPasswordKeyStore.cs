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

/// <summary>
/// What <see cref="DarlingPasswordKeyStore.SnapshotLegacyPinsAsync"/> did. <see cref="MarkerState"/> is <c>done</c> (the
/// step finished: <see cref="Pinned"/> values pinned, possibly none), <c>skipped</c> (a host that is not Windows left the
/// step open for one that is), <c>not-here</c> (values still match the record but this machine opens none of them, so
/// the step stays open for a machine that can), <c>unprotected</c> (a key table's protection is not as expected, so the
/// step did not run), <c>none</c> (no marker row), or the marker's own state when it is not open.
/// </summary>
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
  AND c.relname IN ('password_key', 'password_key_service', 'legacy_secret_pin', 'legacy_secret_pin_marker', 'legacy_secret_pin_candidate');";

    /// <summary>
    /// Checks that the five key tables have exactly the ten owner-only triggers, each enabled for every session
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
                foreach (var (expectedTable, expectedName) in ExpectedTriggers())
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

        foreach (var (_, trigger) in ExpectedTriggers())
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

    // The V165 key tables' eight triggers, then the V166 candidate table's two.
    private static IEnumerable<(string Table, string Trigger)> ExpectedTriggers()
    {
        foreach (var trigger in PasswordKeyTables.OwnerOnlyTriggers)
        {
            yield return trigger;
        }

        foreach (var trigger in LegacyPinCandidateTables.OwnerOnlyTriggers)
        {
            yield return trigger;
        }
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
    /// The legacy pin step, in one transaction. First the catalog must show every owner-only trigger on every key table,
    /// the candidate table's included; if not, nothing runs and the outcome is <c>unprotected</c>. Then the marker row is
    /// locked. On Windows (<paramref name="windows"/>) the step runs only while the marker is <c>pending</c> or
    /// <c>skipped</c> (a Linux host set <c>skipped</c> and left the step to a Windows machine); on anything else it sets
    /// <c>skipped</c> from <c>pending</c> and pins nothing.
    ///
    /// <para>The step pins from the record the V166 rung took of the old-format values present at the upgrade
    /// (<see cref="LegacyPinCandidateTables"/>). A live value matches a candidate only when its stored text and the
    /// connection it is saved for both hash to what the record holds (both compared in fixed time); only a match is
    /// offered to <paramref name="canOpen"/>, which says whether this machine opens it and never returns the value. Four
    /// outcomes, by <see cref="LegacyPinSnapshot.MarkerState"/>: <c>done</c> with every opened match pinned (and none
    /// when nothing matches any more), the record cleared and, when anything was pinned, <c>config_version</c> raised
    /// in the same transaction so every host reloads; <c>not-here</c> when values match but this machine opens none of
    /// them (the transaction is rolled back, the marker and the record stay for a machine that can); <c>unprotected</c>;
    /// and, for a marker that is not open or a missing marker row (<c>none</c>), nothing changes. A failure inside the
    /// transaction changes no pin.</para>
    /// </summary>
    /// <param name="canOpen">True when this machine opens the stored text. May throw
    /// <see cref="CryptographicException"/> / <see cref="FormatException"/> / <see cref="PlatformNotSupportedException"/>
    /// for a value it cannot open (counted as not opened, logged without the value); any other exception escapes.</param>
    /// <param name="timeoutSeconds">The command timeout of every statement the step runs.</param>
    internal static async Task<LegacyPinSnapshot> SnapshotLegacyPinsAsync(
        NpgsqlDataSource postgres, bool windows, Func<string, bool> canOpen, int timeoutSeconds, ILogger logger, CancellationToken ct)
    {
        await using var c = await postgres.OpenConnectionAsync(ct).ConfigureAwait(false);

        var problem = await CheckTriggersAsync(c, ct).ConfigureAwait(false);
        if (problem is not null)
        {
            logger.LogWarning("The legacy password pin step did not run: {Problem}", problem);
            return new LegacyPinSnapshot("unprotected", 0);
        }

        await using var tx = await c.BeginTransactionAsync(ct).ConfigureAwait(false);

        string? marker;
        await using (var lockMarker = new NpgsqlCommand(
            "SELECT state FROM config.legacy_secret_pin_marker WHERE id = 1 FOR UPDATE;", c, tx))
        {
            lockMarker.CommandTimeout = timeoutSeconds;
            marker = (string?)await lockMarker.ExecuteScalarAsync(ct).ConfigureAwait(false);
        }

        var open = string.Equals(marker, "pending", StringComparison.Ordinal)
            || (windows && string.Equals(marker, "skipped", StringComparison.Ordinal));
        if (marker is null || !open)
        {
            await tx.RollbackAsync(ct).ConfigureAwait(false);
            return new LegacyPinSnapshot(marker ?? "none", 0);
        }

        if (!windows)
        {
            await SetMarkerAsync(c, tx, "skipped", timeoutSeconds, ct).ConfigureAwait(false);
            await tx.CommitAsync(ct).ConfigureAwait(false);
            return new LegacyPinSnapshot("skipped", 0);
        }

        var candidates = await ReadCandidatesAsync(c, tx, timeoutSeconds, ct).ConfigureAwait(false);
        var servers = await PinServersAsync(c, tx, candidates, canOpen, timeoutSeconds, logger, ct).ConfigureAwait(false);
        var smtp = await PinSmtpAsync(c, tx, candidates, canOpen, timeoutSeconds, logger, ct).ConfigureAwait(false);
        var matched = servers.Matched + smtp.Matched;
        var pinned = servers.Pinned + smtp.Pinned;

        if (matched > 0 && pinned == 0)
        {
            await tx.RollbackAsync(ct).ConfigureAwait(false);
            return new LegacyPinSnapshot("not-here", 0);
        }

        await SetMarkerAsync(c, tx, "done", timeoutSeconds, ct).ConfigureAwait(false);
        await using (var clear = new NpgsqlCommand("DELETE FROM config.legacy_secret_pin_candidate;", c, tx))
        {
            clear.CommandTimeout = timeoutSeconds;
            await clear.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        if (pinned > 0)
        {
            await using var bump = new NpgsqlCommand(
                "UPDATE config.config_service SET config_version = config_version + 1 WHERE id = 1;", c, tx);
            bump.CommandTimeout = timeoutSeconds;
            await bump.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await tx.CommitAsync(ct).ConfigureAwait(false);
        return new LegacyPinSnapshot("done", pinned);
    }

    private static async Task SetMarkerAsync(NpgsqlConnection c, NpgsqlTransaction tx, string state, int timeoutSeconds, CancellationToken ct)
    {
        await using var update = new NpgsqlCommand(
            "UPDATE config.legacy_secret_pin_marker SET state = $1, changed_at = now() AT TIME ZONE 'UTC' WHERE id = 1;", c, tx);
        update.CommandTimeout = timeoutSeconds;
        update.Parameters.AddWithValue(state);
        await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>One recorded value: the hash of the stored text and the hash of the connection it was recorded for.</summary>
    private sealed record Candidate(byte[] ValueSha256, byte[] BindingSha256);

    // The candidates by (server id, slot). The binding hash is built here from the recorded columns exactly as the live
    // row's is built below, so the two compare as the resolver compares a pin.
    private static async Task<Dictionary<(int ServerId, string Slot), Candidate>> ReadCandidatesAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, int timeoutSeconds, CancellationToken ct)
    {
        var candidates = new Dictionary<(int, string), Candidate>();
        await using var read = new NpgsqlCommand(
            $"SELECT server_id, slot, value_sha256, {ServerConnectionIdentity.StoredColumns}, remediation_username, smtp_use_ssl " +
            "FROM config.legacy_secret_pin_candidate;", c, tx);
        read.CommandTimeout = timeoutSeconds;
        await using var reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var serverId = reader.GetInt32(0);
            var slot = reader.GetString(1);
            var value = (byte[])reader.GetValue(2);
            var host = Text(reader, 3);
            var username = Text(reader, 9);
            PasswordBinding binding;
            if (string.Equals(slot, "smtp", StringComparison.Ordinal))
            {
                binding = PasswordBinding.ForSmtp(
                    host, reader.IsDBNull(4) ? 0 : reader.GetInt32(4), !reader.IsDBNull(14) && reader.GetBoolean(14), username);
            }
            else
            {
                var identity = ServerConnectionIdentity.FromStoredColumns(
                    host, reader.IsDBNull(4) ? null : reader.GetInt32(4), Text(reader, 5), Text(reader, 6),
                    !reader.IsDBNull(7) && reader.GetBoolean(7), Text(reader, 8), username, Text(reader, 10),
                    !reader.IsDBNull(11) && reader.GetBoolean(11), !reader.IsDBNull(12) && reader.GetBoolean(12));
                binding = string.Equals(slot, "server", StringComparison.Ordinal)
                    ? PasswordBinding.ForServer(identity)
                    : PasswordBinding.ForRemediation(identity, Text(reader, 13));
            }

            candidates[(serverId, slot)] = new Candidate(value, binding.LegacyPinHash());
        }

        return candidates;
    }

    private static async Task<(int Matched, int Pinned)> PinServersAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, Dictionary<(int ServerId, string Slot), Candidate> candidates,
        Func<string, bool> canOpen, int timeoutSeconds, ILogger logger, CancellationToken ct)
    {
        var rows = new List<(int ServerId, ServerConnectionIdentity Identity, string? Password, string? RemediationUser, string? RemediationPassword)>();
        await using (var read = new NpgsqlCommand(
            $"SELECT server_id, {ServerConnectionIdentity.StoredColumns}, encrypted_password, remediation_username, remediation_encrypted_password " +
            "FROM config.config_monitored_servers ORDER BY server_id;", c, tx))
        {
            read.CommandTimeout = timeoutSeconds;
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

        var matched = 0;
        var pinned = 0;
        foreach (var row in rows)
        {
            var serverBinding = PasswordBinding.ForServer(row.Identity);
            if (Matches(candidates, row.ServerId, "server", row.Password, serverBinding))
            {
                matched++;
                if (CanPin(row.Password!, canOpen, logger))
                {
                    await InsertPinAsync(c, tx, row.ServerId, "server", row.Password!, serverBinding, timeoutSeconds, ct).ConfigureAwait(false);
                    pinned++;
                }
            }

            var remediationBinding = PasswordBinding.ForRemediation(row.Identity, row.RemediationUser);
            if (Matches(candidates, row.ServerId, "remediation", row.RemediationPassword, remediationBinding))
            {
                matched++;
                if (CanPin(row.RemediationPassword!, canOpen, logger))
                {
                    await InsertPinAsync(c, tx, row.ServerId, "remediation", row.RemediationPassword!, remediationBinding, timeoutSeconds, ct).ConfigureAwait(false);
                    pinned++;
                }
            }
        }

        return (matched, pinned);
    }

    private static async Task<(int Matched, int Pinned)> PinSmtpAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, Dictionary<(int ServerId, string Slot), Candidate> candidates,
        Func<string, bool> canOpen, int timeoutSeconds, ILogger logger, CancellationToken ct)
    {
        string? host = null;
        int port = 0;
        bool useSsl = false;
        string? username = null;
        string? password = null;
        await using (var read = new NpgsqlCommand(
            "SELECT smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password FROM config.config_notification WHERE id = 1;", c, tx))
        {
            read.CommandTimeout = timeoutSeconds;
            await using var reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return (0, 0);
            }

            host = Text(reader, 0);
            port = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            useSsl = !reader.IsDBNull(2) && reader.GetBoolean(2);
            username = Text(reader, 3);
            password = Text(reader, 4);
        }

        var binding = PasswordBinding.ForSmtp(host, port, useSsl, username);
        if (!Matches(candidates, SmtpPinServerId, "smtp", password, binding))
        {
            return (0, 0);
        }

        if (!CanPin(password!, canOpen, logger))
        {
            return (1, 0);
        }

        await InsertPinAsync(c, tx, SmtpPinServerId, "smtp", password!, binding, timeoutSeconds, ct).ConfigureAwait(false);
        return (1, 1);
    }

    // A live value matches its candidate only when both the hash of its stored text and the hash of its connection equal
    // what the record holds. A value with no candidate (written after the record was taken, or moved to another
    // connection) never matches.
    private static bool Matches(
        Dictionary<(int ServerId, string Slot), Candidate> candidates, int serverId, string slot, string? stored, PasswordBinding binding)
    {
        if (string.IsNullOrWhiteSpace(stored) || DarlingSecretSource.IsReference(stored) || PasswordSeal.IsSealed(stored)
            || !candidates.TryGetValue((serverId, slot), out var candidate))
        {
            return false;
        }

        var valueEqual = CryptographicOperations.FixedTimeEquals(candidate.ValueSha256, SHA256.HashData(Encoding.UTF8.GetBytes(stored)));
        var bindingEqual = CryptographicOperations.FixedTimeEquals(candidate.BindingSha256, binding.LegacyPinHash());
        return valueEqual && bindingEqual;
    }

    // A matched value is pinned only when this machine opens it. The question is answered with a yes or a no: the opened
    // value is never returned to this code.
    private static bool CanPin(string stored, Func<string, bool> canOpen, ILogger logger)
    {
        try
        {
            return canOpen(stored);
        }
        catch (Exception ex) when (ex is CryptographicException or FormatException or PlatformNotSupportedException)
        {
            logger.LogWarning("A saved password could not be opened on this machine, so it was not pinned: {Message}", ex.Message);
            return false;
        }
    }

    private static async Task InsertPinAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, int serverId, string slot, string storedText, PasswordBinding binding, int timeoutSeconds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, slot) DO NOTHING;", c, tx);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(slot);
        command.Parameters.AddWithValue(SHA256.HashData(Encoding.UTF8.GetBytes(storedText)));
        command.Parameters.AddWithValue(binding.LegacyPinHash());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}
