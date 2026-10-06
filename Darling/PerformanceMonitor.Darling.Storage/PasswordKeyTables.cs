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
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The public half of the password key the service publishes (<c>config.password_key</c>): the key id, the
/// SubjectPublicKeyInfo bytes and the algorithm name of the one key whose state is <c>current</c>.
/// </summary>
public sealed record PublishedPasswordKey(string KeyId, byte[] Spki, string Algorithm);

/// <summary>
/// One service host's row in <c>config.password_key_service</c>: which key the service holds and whether it is
/// healthy. <see cref="State"/> is one of <c>loading</c>, <c>ok</c>, <c>missing</c>, <c>mismatch</c>, <c>refused</c> and
/// <c>reset_pending</c>. <see cref="UpdatedAtUtc"/> is the row's naive UTC <c>timestamp</c> read as UTC.
/// </summary>
public sealed record PasswordKeyServiceState(string ServiceHost, string? KeyId, string State, string? Note, DateTime UpdatedAtUtc);

/// <summary>
/// The store's tables for the service's password key (V165, #5366), their creating script, and the two reads that
/// writers and viewers share. Nothing in this class writes: the key tables are changed by the store owner only, and
/// the owner-only trigger on each table (<see cref="CreateSql"/>) refuses every other writer.
/// </summary>
public static class PasswordKeyTables
{
    /// <summary>The store schema version of the rung that creates the tables (V165). A store below it has none of them.</summary>
    public const int RungVersion = 165;

    /// <summary>SQLSTATE the owner-only trigger raises when a session that is not the store owner writes a key table.</summary>
    public const string OwnerOnlySqlState = "PW010";

    /// <summary>
    /// The four tables, the one-row marker, the owner-only trigger function and its two triggers per table, as the V165
    /// rung runs them. Idempotent: tables and the unique index are guarded, the marker row is inserted once
    /// (<c>ON CONFLICT DO NOTHING</c>), and the function is replaced and each trigger is dropped and created again. The rung embeds this text, so once V165 has
    /// shipped, editing it edits a shipped rung: add a new rung instead; <c>PasswordKeyRungTests</c> pins the shape.
    ///
    /// <para>The function names the owner by <c>session_user</c> against the owner of the table it fires on, with every
    /// catalog relation schema-qualified and <c>pg_temp</c> last in the function's search path, so a session's own
    /// temporary objects are never consulted. It is not <c>SECURITY DEFINER</c>, and no other function may write these
    /// tables on a caller's behalf. A session user whose membership in the owner role cannot be told (a NULL answer)
    /// is refused. Every trigger is <c>ENABLE ALWAYS</c>, so it also fires when <c>session_replication_role</c> is
    /// <c>replica</c>.</para>
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS config.password_key
(
    key_id text NOT NULL,
    public_key bytea NOT NULL,
    algorithm text NOT NULL,
    state text NOT NULL,
    replaced_reason text,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    replaced_at timestamp,
    CONSTRAINT pk_password_key PRIMARY KEY (key_id),
    CONSTRAINT ck_password_key_id CHECK (key_id ~ '^[0-9a-f]{16}$'),
    CONSTRAINT ck_password_key_state CHECK (state IN ('current', 'replaced')),
    CONSTRAINT ck_password_key_replaced_reason CHECK (replaced_reason IS NULL OR replaced_reason IN ('reset', 'rotated')),
    CONSTRAINT ck_password_key_algorithm CHECK (algorithm IN ('RSA3072-OAEP-SHA256/A256GCM')),
    CONSTRAINT ck_password_key_public_key_size CHECK (octet_length(public_key) BETWEEN 256 AND 2048),
    CONSTRAINT ck_password_key_replaced_columns CHECK (
        (state = 'current' AND replaced_reason IS NULL AND replaced_at IS NULL)
        OR (state = 'replaced' AND replaced_reason IS NOT NULL AND replaced_at IS NOT NULL)),
    CONSTRAINT ck_password_key_id_matches_key CHECK (key_id = left(encode(sha256(public_key), 'hex'), 16))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_password_key_current ON config.password_key ((true)) WHERE state = 'current';

CREATE TABLE IF NOT EXISTS config.password_key_service
(
    service_host text NOT NULL,
    key_id text,
    state text NOT NULL,
    note text,
    updated_at timestamp NOT NULL,
    CONSTRAINT pk_password_key_service PRIMARY KEY (service_host),
    CONSTRAINT ck_password_key_service_state CHECK (state IN ('loading', 'ok', 'missing', 'mismatch', 'refused', 'reset_pending'))
);

CREATE TABLE IF NOT EXISTS config.legacy_secret_pin
(
    server_id integer NOT NULL,
    slot text NOT NULL,
    value_sha256 bytea NOT NULL,
    binding_sha256 bytea NOT NULL,
    pinned_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT pk_legacy_secret_pin PRIMARY KEY (server_id, slot),
    CONSTRAINT ck_legacy_secret_pin_slot CHECK (slot IN ('server', 'remediation', 'smtp'))
);

CREATE TABLE IF NOT EXISTS config.legacy_secret_pin_marker
(
    id integer NOT NULL,
    state text NOT NULL,
    changed_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT pk_legacy_secret_pin_marker PRIMARY KEY (id),
    CONSTRAINT ck_legacy_secret_pin_marker_single_row CHECK (id = 1),
    CONSTRAINT ck_legacy_secret_pin_marker_state CHECK (state IN ('pending', 'done', 'skipped'))
);

INSERT INTO config.legacy_secret_pin_marker (id, state)
VALUES (1, 'pending')
ON CONFLICT (id) DO NOTHING;

-- The two pin tables carry no row-security policy, so only the owner and superusers see their rows.
ALTER TABLE config.legacy_secret_pin ENABLE ROW LEVEL SECURITY;
ALTER TABLE config.legacy_secret_pin_marker ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION config.password_key_owner_only() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $fn$
BEGIN
    IF pg_catalog.pg_has_role(
               session_user,
               (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = TG_RELID),
               'USAGE') IS NOT TRUE THEN
        RAISE EXCEPTION 'Only the store owner can change the password key tables.' USING ERRCODE = 'PW010';
    END IF;

    IF TG_LEVEL = 'STATEMENT' THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$fn$;

REVOKE ALL ON FUNCTION config.password_key_owner_only() FROM PUBLIC;

DROP TRIGGER IF EXISTS trg_password_key_owner_only_row ON config.password_key;
CREATE TRIGGER trg_password_key_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.password_key
    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();
DROP TRIGGER IF EXISTS trg_password_key_owner_only_truncate ON config.password_key;
CREATE TRIGGER trg_password_key_owner_only_truncate
    BEFORE TRUNCATE ON config.password_key
    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();

DROP TRIGGER IF EXISTS trg_password_key_service_owner_only_row ON config.password_key_service;
CREATE TRIGGER trg_password_key_service_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.password_key_service
    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();
DROP TRIGGER IF EXISTS trg_password_key_service_owner_only_truncate ON config.password_key_service;
CREATE TRIGGER trg_password_key_service_owner_only_truncate
    BEFORE TRUNCATE ON config.password_key_service
    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();

DROP TRIGGER IF EXISTS trg_legacy_secret_pin_owner_only_row ON config.legacy_secret_pin;
CREATE TRIGGER trg_legacy_secret_pin_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.legacy_secret_pin
    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();
DROP TRIGGER IF EXISTS trg_legacy_secret_pin_owner_only_truncate ON config.legacy_secret_pin;
CREATE TRIGGER trg_legacy_secret_pin_owner_only_truncate
    BEFORE TRUNCATE ON config.legacy_secret_pin
    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();

DROP TRIGGER IF EXISTS trg_legacy_secret_pin_marker_owner_only_row ON config.legacy_secret_pin_marker;
CREATE TRIGGER trg_legacy_secret_pin_marker_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.legacy_secret_pin_marker
    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();
DROP TRIGGER IF EXISTS trg_legacy_secret_pin_marker_owner_only_truncate ON config.legacy_secret_pin_marker;
CREATE TRIGGER trg_legacy_secret_pin_marker_owner_only_truncate
    BEFORE TRUNCATE ON config.legacy_secret_pin_marker
    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();

ALTER TABLE config.password_key ENABLE ALWAYS TRIGGER trg_password_key_owner_only_row;
ALTER TABLE config.password_key ENABLE ALWAYS TRIGGER trg_password_key_owner_only_truncate;
ALTER TABLE config.password_key_service ENABLE ALWAYS TRIGGER trg_password_key_service_owner_only_row;
ALTER TABLE config.password_key_service ENABLE ALWAYS TRIGGER trg_password_key_service_owner_only_truncate;
ALTER TABLE config.legacy_secret_pin ENABLE ALWAYS TRIGGER trg_legacy_secret_pin_owner_only_row;
ALTER TABLE config.legacy_secret_pin ENABLE ALWAYS TRIGGER trg_legacy_secret_pin_owner_only_truncate;
ALTER TABLE config.legacy_secret_pin_marker ENABLE ALWAYS TRIGGER trg_legacy_secret_pin_marker_owner_only_row;
ALTER TABLE config.legacy_secret_pin_marker ENABLE ALWAYS TRIGGER trg_legacy_secret_pin_marker_owner_only_truncate;";

    /// <summary>
    /// The eight triggers <see cref="CreateSql"/> creates, by table: a row trigger and a truncate trigger on each of the
    /// four key tables. The service checks the catalog against this list and expects every one to be <c>ENABLE ALWAYS</c>.
    /// </summary>
    public static readonly IReadOnlyList<(string Table, string Trigger)> OwnerOnlyTriggers = new[]
    {
        ("password_key", "trg_password_key_owner_only_row"),
        ("password_key", "trg_password_key_owner_only_truncate"),
        ("password_key_service", "trg_password_key_service_owner_only_row"),
        ("password_key_service", "trg_password_key_service_owner_only_truncate"),
        ("legacy_secret_pin", "trg_legacy_secret_pin_owner_only_row"),
        ("legacy_secret_pin", "trg_legacy_secret_pin_owner_only_truncate"),
        ("legacy_secret_pin_marker", "trg_legacy_secret_pin_marker_owner_only_row"),
        ("legacy_secret_pin_marker", "trg_legacy_secret_pin_marker_owner_only_truncate"),
    };

    /// <summary>The one key whose state is <c>current</c>; the unique index allows at most one such row.</summary>
    public const string ReadCurrentSql = @"
SELECT key_id, public_key, algorithm
FROM config.password_key
WHERE state = 'current';";

    /// <summary>
    /// The service host row written most recently. With one service per store this is that service's row; the tie-break
    /// on the host name makes the answer stable when two rows share a timestamp.
    /// </summary>
    public const string ReadNewestServiceStateSql = @"
SELECT service_host, key_id, state, note, updated_at
FROM config.password_key_service
ORDER BY updated_at DESC, service_host
LIMIT 1;";

    /// <summary>Reads the current published key, or null when no key has been published (or the store is below V165).</summary>
    public static async Task<PublishedPasswordKey?> ReadCurrentAsync(NpgsqlConnection c, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);

        await using var command = new NpgsqlCommand(ReadCurrentSql, c);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        return new PublishedPasswordKey(reader.GetString(0), (byte[])reader.GetValue(1), reader.GetString(2));
    }

    /// <summary>Reads the service host row written most recently, or null when no service has written one.</summary>
    public static async Task<PasswordKeyServiceState?> ReadNewestServiceStateAsync(NpgsqlConnection c, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);

        await using var command = new NpgsqlCommand(ReadNewestServiceStateSql, c);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        return new PasswordKeyServiceState(
            reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc));
    }
}
