/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store's tables for alerts that another service raises through Darling's channels, routes and mute rules (V180,
/// #5654), and the one column that lets a mute rule name such a source. Only the text lives here; the V180 rung embeds it.
///
/// <para><b>Three tables, all written by the store owner and by the one function in the provisioning batch.</b>
/// <c>config.external_alert_source</c> is the registry that maps a login role to a source name (the function looks the
/// caller up by <c>session_user</c>, so no source can claim another's name). <c>config.external_alert_pending</c> is the
/// queue the function appends to and the service drains on its 15 s pass: one row per fire or clear, with the status the
/// drain moves it through. <c>config.external_alert_state</c> remembers which keys are firing, so a clear pairs with its
/// fire (the shape of the custom alert state store). It is keyed on the submitted server name (<c>server_label</c>: trimmed
/// and lower-cased), and it keeps the server id, key and display name the fire was sent under (<c>server_id</c>,
/// <c>server_key</c>, <c>server_name</c>), so the clear closes that incident even when the server was registered, renamed or
/// removed in between. The registry column is <c>external_source</c> on the mute rule,
/// not <c>source</c>: Lite's alert history view already has a <c>source</c> column that means live or archive.</para>
///
/// <para><b>Privileges are not here.</b> The <c>config</c> schema's blanket SELECT and default privileges cover these
/// tables for the viewer roles, as they cover every plain <c>config</c> table. Taking INSERT, UPDATE and DELETE away from
/// the <c>admin</c> role, and giving a source role its one EXECUTE, belong to the provisioning batch that runs on every
/// start, so they are re-asserted on every start and reach a store that was provisioned before this rung.</para>
///
/// <para>The foreign keys cascade, so removing a source removes its queue and its firing state. The history it already
/// wrote stays in <c>config.config_alert_log</c>, which carries no foreign key to the registry.</para>
/// </summary>
public static class ExternalAlertTables
{
    /// <summary>The store schema version of the rung that creates the tables (V180). A store below it has none of them.</summary>
    public const int RungVersion = 180;

    /// <summary>
    /// The three tables, their two indexes, and the mute-rule column, as the V180 rung runs them. Every statement is
    /// guarded (<c>IF NOT EXISTS</c>), so a re-run changes nothing. The rung embeds this text, so once V180 has shipped,
    /// editing it edits a shipped rung: add a new rung instead.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS config.external_alert_source
(
    source_id integer GENERATED ALWAYS AS IDENTITY,
    source_name text NOT NULL,
    role_name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    max_per_minute integer NOT NULL DEFAULT 60,
    max_pending integer NOT NULL DEFAULT 500,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_external_alert_source PRIMARY KEY (source_id),
    CONSTRAINT uq_external_alert_source_name UNIQUE (source_name),
    CONSTRAINT uq_external_alert_source_role UNIQUE (role_name),
    CONSTRAINT ck_external_alert_source_name CHECK (source_name ~ '^[a-z][a-z0-9_]{1,30}$')
);

CREATE TABLE IF NOT EXISTS config.external_alert_pending
(
    pending_id bigint GENERATED ALWAYS AS IDENTITY,
    source_id integer NOT NULL,
    kind text NOT NULL,
    alert_key text NOT NULL,
    severity text NOT NULL,
    title text,
    detail text,
    server_name text,
    value double precision,
    submitted_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    claimed_at timestamptz,
    service_instance text,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    reason text,
    completed_at timestamptz,
    CONSTRAINT pk_external_alert_pending PRIMARY KEY (pending_id),
    CONSTRAINT fk_external_alert_pending_source FOREIGN KEY (source_id)
        REFERENCES config.external_alert_source (source_id) ON DELETE CASCADE,
    CONSTRAINT ck_external_alert_pending_kind CHECK (kind IN ('fire', 'clear')),
    CONSTRAINT ck_external_alert_pending_severity CHECK (severity IN ('warning', 'critical')),
    CONSTRAINT ck_external_alert_pending_status CHECK (status IN
        ('pending', 'claimed', 'delivered', 'muted', 'no_channel', 'failed', 'expired', 'interrupted', 'ignored'))
);

CREATE INDEX IF NOT EXISTS ix_external_alert_pending_status ON config.external_alert_pending (status, pending_id);

CREATE INDEX IF NOT EXISTS ix_external_alert_pending_source ON config.external_alert_pending (source_id, submitted_at);

CREATE TABLE IF NOT EXISTS config.external_alert_state
(
    source_id integer NOT NULL,
    alert_key text NOT NULL,
    server_label text NOT NULL DEFAULT '',
    server_id integer NOT NULL DEFAULT 0,
    server_key text NOT NULL DEFAULT '',
    server_name text NOT NULL DEFAULT '',
    firing boolean NOT NULL DEFAULT false,
    fired_severity text,
    last_fired_at timestamptz,
    CONSTRAINT pk_external_alert_state PRIMARY KEY (source_id, alert_key, server_label),
    CONSTRAINT fk_external_alert_state_source FOREIGN KEY (source_id)
        REFERENCES config.external_alert_source (source_id) ON DELETE CASCADE
);

ALTER TABLE config.config_mute_rules ADD COLUMN IF NOT EXISTS external_source text;
";
}
