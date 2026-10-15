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
/// The store's tables for Level 1 investigations that an alert hands to the operator's own agent (V184, #5632 Phase A).
/// Only the text lives here; the V184 rung embeds it.
///
/// <para><b>What each table holds.</b> <c>config.investigation_requests</c> is one request per server, alert family and
/// 30-minute grouping window, with its state (open, claimed, reported, closed, expired), the current claim and its lease,
/// the claim count, the expiry, and the brief composed at claim. <c>config.investigation_request_alerts</c> holds a row
/// per alert that opened or joined a request, and per incident dedup key of that alert, so a later alert carrying a known
/// key joins the same request. <c>config.investigation_reports</c> holds the one report an agent returned for a request.
/// <c>config.investigation_journal</c> is append-only, like <c>collect.plan_force_actions</c>: every open, join, limit,
/// claim, lease expiry, report and refusal, with a <c>once_key</c> that is unique where set, so an event that must be
/// recorded once (a limit per route per hour, a refusal per request and reason, a lease expiry per claim) is inserted
/// with <c>ON CONFLICT DO NOTHING</c>. <c>config.investigation_notes</c> are notes about a server or a finding that a
/// person approves before a brief carries them, and <c>config.agent_environment_profiles</c> the free-text profile per
/// server, tag or fleet.</para>
///
/// <para><b>The route columns.</b> <c>config.config_notification_routes</c> gains the agent level (NULL inherits, 0 is
/// off and stops the lookup, 1 investigates), the hourly and daily request limits (20 and 100 by default, ruling R6),
/// the lease and the expiry in minutes. Each is a constant default or nullable, so the <c>ADD COLUMN</c> is catalog-only
/// and every stored route keeps its effect (NULL level: no agent).</para>
///
/// <para><b>Write privileges are taken away where the tables are made</b>, as V180 does: the last statement revokes every
/// write privilege on the six tables from PUBLIC and from the <c>admin</c>, <c>viewer</c> and <c>mcp</c> roles when they
/// exist. The narrow writes the <c>mcp</c> role holds (column UPDATE on the claim and report columns, INSERT on reports,
/// the journal and notes) and the viewer's are provisioning's, re-asserted on every start.</para>
/// </summary>
public static class InvestigationTables
{
    /// <summary>The store schema version of the rung that creates the tables (V184). A store below it has none of them.</summary>
    public const int RungVersion = 184;

    /// <summary>The investigation families (ruling Q9): six alert families plus <c>other</c> for custom and external alerts.</summary>
    public static readonly IReadOnlyList<string> Families =
        new[] { "blocking", "query_performance", "resources", "availability", "jobs", "self_monitor", "other" };

    /// <summary>
    /// The route columns, the six tables and their indexes, as the V184 rung runs them. Every statement is guarded, so a
    /// re-run changes nothing. The rung embeds this text, so once V184 has shipped, editing it edits a shipped rung: add a
    /// new rung instead.
    /// </summary>
    public const string CreateSql = @"
ALTER TABLE config.config_notification_routes
    ADD COLUMN IF NOT EXISTS agent_level smallint,
    ADD COLUMN IF NOT EXISTS agent_max_per_hour integer NOT NULL DEFAULT 20,
    ADD COLUMN IF NOT EXISTS agent_max_per_day integer NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS agent_lease_minutes integer NOT NULL DEFAULT 15,
    ADD COLUMN IF NOT EXISTS agent_expiry_minutes integer NOT NULL DEFAULT 240;

DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_config_notification_routes_agent'
                     AND conrelid = 'config.config_notification_routes'::regclass) THEN
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_agent CHECK (
                (agent_level IS NULL OR agent_level IN (0, 1))
                AND agent_max_per_hour BETWEEN 1 AND 60
                AND agent_max_per_day BETWEEN 1 AND 500
                AND agent_lease_minutes BETWEEN 5 AND 120
                AND agent_expiry_minutes BETWEEN 15 AND 1440);
    END IF;
END $do$;

CREATE TABLE IF NOT EXISTS config.investigation_requests
(
    request_id uuid NOT NULL,
    route_id integer NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    family text NOT NULL,
    first_metric text NOT NULL,
    first_dedup_key text,
    story_path_hash text,
    first_alert_at timestamptz NOT NULL,
    last_alert_at timestamptz NOT NULL,
    window_until timestamptz NOT NULL,
    alert_count integer NOT NULL DEFAULT 1,
    state text NOT NULL DEFAULT 'open',
    autonomy_level smallint NOT NULL DEFAULT 1,
    opened_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    lease_minutes integer NOT NULL DEFAULT 15,
    claimed_by text,
    claim_id uuid,
    claimed_at timestamptz,
    lease_until timestamptz,
    claim_count integer NOT NULL DEFAULT 0,
    brief_format smallint,
    brief jsonb,
    reported_at timestamptz,
    closed_at timestamptz,
    closed_by text,
    closed_reason text,
    CONSTRAINT pk_investigation_requests PRIMARY KEY (request_id),
    CONSTRAINT ck_investigation_requests_family CHECK (family IN
        ('blocking', 'query_performance', 'resources', 'availability', 'jobs', 'self_monitor', 'other')),
    CONSTRAINT ck_investigation_requests_state CHECK (state IN ('open', 'claimed', 'reported', 'closed', 'expired')),
    CONSTRAINT ck_investigation_requests_autonomy CHECK (autonomy_level = 1),
    CONSTRAINT ck_investigation_requests_claim_count CHECK (claim_count BETWEEN 0 AND 3),
    CONSTRAINT ck_investigation_requests_claimed_by CHECK (claimed_by IS NULL OR claimed_by ~ '^[A-Za-z0-9._:-]{1,64}$')
);

CREATE INDEX IF NOT EXISTS ix_investigation_requests_server_family ON config.investigation_requests (server_id, family, state);

CREATE INDEX IF NOT EXISTS ix_investigation_requests_route_opened ON config.investigation_requests (route_id, opened_at);

CREATE INDEX IF NOT EXISTS ix_investigation_requests_state_expires ON config.investigation_requests (state, expires_at);

CREATE TABLE IF NOT EXISTS config.investigation_request_alerts
(
    request_alert_id bigint GENERATED ALWAYS AS IDENTITY,
    request_id uuid NOT NULL,
    alert_time timestamptz NOT NULL,
    server_id integer NOT NULL,
    metric_name text NOT NULL,
    dedup_key text,
    CONSTRAINT pk_investigation_request_alerts PRIMARY KEY (request_alert_id),
    CONSTRAINT fk_investigation_request_alerts_request FOREIGN KEY (request_id)
        REFERENCES config.investigation_requests (request_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_investigation_request_alerts_request ON config.investigation_request_alerts (request_id, alert_time);

CREATE INDEX IF NOT EXISTS ix_investigation_request_alerts_dedup ON config.investigation_request_alerts (dedup_key) WHERE dedup_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS config.investigation_reports
(
    report_id bigint GENERATED ALWAYS AS IDENTITY,
    request_id uuid NOT NULL,
    report_format smallint NOT NULL,
    agent_id text NOT NULL,
    claim_id uuid NOT NULL,
    submitted_at timestamptz NOT NULL DEFAULT now(),
    verdict text NOT NULL,
    root_cause text NOT NULL,
    root_cause_found boolean NOT NULL DEFAULT false,
    summary text,
    evidence jsonb NOT NULL,
    server_id integer NOT NULL,
    metric_name text,
    dedup_key text,
    story_path_hash text,
    CONSTRAINT pk_investigation_reports PRIMARY KEY (report_id),
    CONSTRAINT uq_investigation_reports_request UNIQUE (request_id),
    CONSTRAINT fk_investigation_reports_request FOREIGN KEY (request_id)
        REFERENCES config.investigation_requests (request_id) ON DELETE CASCADE,
    CONSTRAINT ck_investigation_reports_verdict CHECK (verdict IN ('real', 'false_positive', 'needs_person')),
    CONSTRAINT ck_investigation_reports_summary CHECK (summary IS NULL OR length(summary) <= 4000),
    CONSTRAINT ck_investigation_reports_evidence CHECK (jsonb_typeof(evidence) = 'array' AND jsonb_array_length(evidence) >= 1),
    CONSTRAINT ck_investigation_reports_agent CHECK (agent_id ~ '^[A-Za-z0-9._:-]{1,64}$'),
    CONSTRAINT ck_investigation_reports_root_cause CHECK (length(root_cause) <= 8000),
    CONSTRAINT ck_investigation_reports_evidence_size CHECK (
        jsonb_array_length(evidence) <= 50
        AND octet_length(evidence::text) <= 32768
        AND NOT jsonb_path_exists(evidence, 'strict $.** ? (@.type() == ""string"" && @ like_regex ""^(?:.{200}){5}."" flag ""s"")'))
);

CREATE INDEX IF NOT EXISTS ix_investigation_reports_story ON config.investigation_reports (story_path_hash, submitted_at);

CREATE INDEX IF NOT EXISTS ix_investigation_reports_server ON config.investigation_reports (server_id, submitted_at);

CREATE TABLE IF NOT EXISTS config.investigation_journal
(
    journal_id bigint GENERATED ALWAYS AS IDENTITY,
    event_time timestamptz NOT NULL DEFAULT now(),
    request_id uuid,
    route_id integer,
    server_id integer,
    event text NOT NULL,
    actor text NOT NULL,
    reason text,
    detail jsonb,
    once_key text,
    CONSTRAINT pk_investigation_journal PRIMARY KEY (journal_id),
    CONSTRAINT ck_investigation_journal_event CHECK (event ~ '^[a-z][a-z_]{1,40}$')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_investigation_journal_once ON config.investigation_journal (once_key) WHERE once_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_investigation_journal_time ON config.investigation_journal (event_time);

CREATE INDEX IF NOT EXISTS ix_investigation_journal_request ON config.investigation_journal (request_id, event_time) WHERE request_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS config.investigation_notes
(
    note_id bigint GENERATED ALWAYS AS IDENTITY,
    server_id integer,
    story_path_hash text,
    dedup_key text,
    metric_name text,
    note_text text NOT NULL,
    author text NOT NULL,
    source_request_id uuid,
    state text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT (now() + interval '90 days'),
    decided_at timestamptz,
    decided_by text,
    CONSTRAINT pk_investigation_notes PRIMARY KEY (note_id),
    CONSTRAINT ck_investigation_notes_text CHECK (btrim(note_text) <> '' AND length(note_text) <= 2000),
    CONSTRAINT ck_investigation_notes_state CHECK (state IN ('pending', 'approved', 'rejected'))
);

CREATE INDEX IF NOT EXISTS ix_investigation_notes_server ON config.investigation_notes (server_id, state);

CREATE TABLE IF NOT EXISTS config.agent_environment_profiles
(
    profile_id integer GENERATED ALWAYS AS IDENTITY,
    scope_server_id integer,
    scope_tag_id integer,
    profile_text text NOT NULL,
    modified_at timestamptz NOT NULL DEFAULT now(),
    modified_by text,
    CONSTRAINT pk_agent_environment_profiles PRIMARY KEY (profile_id),
    CONSTRAINT ck_agent_environment_profiles_one_scope CHECK (scope_server_id IS NULL OR scope_tag_id IS NULL),
    CONSTRAINT ck_agent_environment_profiles_text CHECK (length(profile_text) <= 4000)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_environment_profiles_scope
    ON config.agent_environment_profiles ((coalesce(scope_server_id, 0)), (coalesce(scope_tag_id, 0)));

DO $do$
BEGIN
   REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports, config.investigation_journal, config.investigation_notes, config.agent_environment_profiles FROM PUBLIC;
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'admin') THEN
      REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports, config.investigation_journal, config.investigation_notes, config.agent_environment_profiles FROM admin;
   END IF;
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'viewer') THEN
      REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports, config.investigation_journal, config.investigation_notes, config.agent_environment_profiles FROM viewer;
   END IF;
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'mcp') THEN
      REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports, config.investigation_journal, config.investigation_notes, config.agent_environment_profiles FROM mcp;
   END IF;
END $do$;
";
}
