/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The record of which old-format saved passwords were in the store when the store reached V166 (#5456), its creating
/// script and the statement that fills it. The one-time pin step (<c>DarlingPasswordKeyStore.SnapshotLegacyPinsAsync</c>)
/// works from this record: it pins a saved value only when the value and the connection it is saved for both still equal
/// what was recorded here. Nothing in this class writes: the table is changed by the store owner only, and the
/// owner-only trigger on it (<see cref="CreateSql"/>) refuses every other writer.
/// </summary>
public static class LegacyPinCandidateTables
{
    /// <summary>The store schema version of the rung that creates the table and records the values (V166).</summary>
    public const int RungVersion = 166;

    /// <summary>
    /// The table, its two owner-only triggers and row-level security. The triggers are made before
    /// <see cref="CaptureSql"/> runs, so the table is never writable by anyone else, not even for the moment between
    /// its creation and the provisioning scripts' revoke. The table holds, per saved password, the SHA-256 of the stored
    /// text (never the text) and the connection columns the pin binds the value to, copied as the source row holds them:
    /// for a server's own or remediation password the server's connection settings, for the mail server the host, port,
    /// SSL flag and user name (in <c>host</c>, <c>port</c>, <c>smtp_use_ssl</c> and <c>username</c>). Idempotent. The
    /// rung embeds this text, so once V166 has shipped, editing it edits a shipped rung: add a new rung instead.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS config.legacy_secret_pin_candidate
(
    server_id integer NOT NULL,
    slot text NOT NULL,
    value_sha256 bytea NOT NULL,
    host text,
    port integer,
    engine text,
    database text,
    read_only_intent boolean,
    auth text,
    username text,
    encrypt_mode text,
    trust_server_certificate boolean,
    multi_subnet_failover boolean,
    remediation_username text,
    smtp_use_ssl boolean,
    captured_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT pk_legacy_secret_pin_candidate PRIMARY KEY (server_id, slot),
    CONSTRAINT ck_legacy_secret_pin_candidate_slot CHECK (slot IN ('server', 'remediation', 'smtp'))
);

ALTER TABLE config.legacy_secret_pin_candidate ENABLE ROW LEVEL SECURITY;

DROP TRIGGER IF EXISTS trg_legacy_secret_pin_candidate_owner_only_row ON config.legacy_secret_pin_candidate;
CREATE TRIGGER trg_legacy_secret_pin_candidate_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.legacy_secret_pin_candidate
    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();
DROP TRIGGER IF EXISTS trg_legacy_secret_pin_candidate_owner_only_truncate ON config.legacy_secret_pin_candidate;
CREATE TRIGGER trg_legacy_secret_pin_candidate_owner_only_truncate
    BEFORE TRUNCATE ON config.legacy_secret_pin_candidate
    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();

ALTER TABLE config.legacy_secret_pin_candidate ENABLE ALWAYS TRIGGER trg_legacy_secret_pin_candidate_owner_only_row;
ALTER TABLE config.legacy_secret_pin_candidate ENABLE ALWAYS TRIGGER trg_legacy_secret_pin_candidate_owner_only_truncate;";

    /// <summary>
    /// Records every old-format saved password in the store now: one row per non-blank server, remediation and
    /// mail-server value that is not a reference (<c>env:</c> / <c>file:</c>) and not sealed, with the hash of the
    /// stored text (the same hash the pin step and the resolver take of it) and the connection columns. Runs only while
    /// the pin step is still open (marker <c>pending</c> or <c>skipped</c>), so a store whose step is done records
    /// nothing. The mail server's row is <c>server_id</c> 0. Idempotent: a row already recorded is left as it is.
    /// </summary>
    public const string CaptureSql = @"
INSERT INTO config.legacy_secret_pin_candidate
    (server_id, slot, value_sha256, host, port, engine, database, read_only_intent, auth, username, encrypt_mode,
     trust_server_certificate, multi_subnet_failover, remediation_username, smtp_use_ssl)
SELECT v.server_id, v.slot, pg_catalog.sha256(pg_catalog.convert_to(v.stored, 'UTF8')), v.host, v.port, v.engine, v.database,
       v.read_only_intent, v.auth, v.username, v.encrypt_mode, v.trust_server_certificate, v.multi_subnet_failover,
       v.remediation_username, v.smtp_use_ssl
FROM (
    SELECT s.server_id, 'server'::text AS slot, s.encrypted_password AS stored, s.host, s.port, s.engine, s.database,
           s.read_only_intent, s.auth, s.username, s.encrypt_mode, s.trust_server_certificate, s.multi_subnet_failover,
           NULL::text AS remediation_username, NULL::boolean AS smtp_use_ssl
    FROM config.config_monitored_servers AS s
    UNION ALL
    SELECT s.server_id, 'remediation'::text, s.remediation_encrypted_password, s.host, s.port, s.engine, s.database,
           s.read_only_intent, s.auth, s.username, s.encrypt_mode, s.trust_server_certificate, s.multi_subnet_failover,
           s.remediation_username, NULL::boolean
    FROM config.config_monitored_servers AS s
    UNION ALL
    SELECT 0, 'smtp'::text, n.smtp_encrypted_password, n.smtp_host, n.smtp_port, NULL::text, NULL::text,
           NULL::boolean, NULL::text, n.smtp_username, NULL::text, NULL::boolean, NULL::boolean,
           NULL::text, n.smtp_use_ssl
    FROM config.config_notification AS n
    WHERE n.id = 1
) AS v
WHERE v.stored IS NOT NULL
  AND v.stored !~ '^\s*$'
  AND NOT pg_catalog.starts_with(v.stored, 'sealed:')
  AND NOT pg_catalog.starts_with(v.stored, 'env:')
  AND NOT pg_catalog.starts_with(v.stored, 'file:')
  AND EXISTS (SELECT 1 FROM config.legacy_secret_pin_marker AS m WHERE m.id = 1 AND m.state IN ('pending', 'skipped'))
ON CONFLICT (server_id, slot) DO NOTHING;";

    /// <summary>The two triggers <see cref="CreateSql"/> creates, by table. The service checks the catalog against them and expects both to be <c>ENABLE ALWAYS</c>.</summary>
    public static readonly IReadOnlyList<(string Table, string Trigger)> OwnerOnlyTriggers = new[]
    {
        ("legacy_secret_pin_candidate", "trg_legacy_secret_pin_candidate_owner_only_row"),
        ("legacy_secret_pin_candidate", "trg_legacy_secret_pin_candidate_owner_only_truncate"),
    };
}
