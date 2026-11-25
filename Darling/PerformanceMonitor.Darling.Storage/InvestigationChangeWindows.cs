/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9485: planned change windows. A person or an agent declares that a planned change touches one server, a list or every server
/// between two times. An investigation request that opens inside the window, or inside the tail after it, is created HELD: it stays
/// listable and claimable, but it is not handed to the request runner (it does not wake <c>wait_for_investigation_requests</c> and
/// it is not posted to the route's webhook). Pages and the alert history do not change.
///
/// <para><b>Shape (V318).</b> <c>config.investigation_change_windows</c> holds one row per declared window. <c>server_ids</c> NULL means
/// every server. <c>cleared_at</c> ends the window early: a cleared window holds nothing opened after that time. <c>summarized_at</c> is
/// written when the close summary is journaled. <c>config.investigation_requests</c> gains <c>held_window_id</c> (the window that held
/// the request, NULL when none did) and <c>hold_released_at</c> (set when a late join releases the hold). A request is held while
/// <c>held_window_id IS NOT NULL AND hold_released_at IS NULL</c>.</para>
///
/// <para><b>Membership</b> is decided once, when the request opens, against the request's own recorded <c>opened_at</c> (the store's
/// clock), by <see cref="HeldWindowIdSql"/>. The edges follow the instant-row convention (#9140): held when
/// <c>starts_at &lt; opened_at AND opened_at &lt;= ends_at + tail</c>, so a request opened exactly at the start is not held and one
/// opened exactly at the end of the tail is.</para>
///
/// <para><b>Who is held.</b> Alert-triggered requests and change-watch requests, which both open through
/// <c>InvestigationRequestStore.OpenOrJoinAsync</c>. A chat request, a force check, a practice request and a follow-up are asked for
/// by a person or an agent on purpose, and are never held.</para>
/// </summary>
public static class InvestigationChangeWindows
{
    /// <summary>The store schema version that adds the windows table and the two request columns. One constant so a renumber, when another rung lands first, is one edit.</summary>
    public const int Version = 318;

    /// <summary>
    /// How long after a window's end a request still opens held (#9485): a re-capture or a settle period runs past the end of the work.
    /// Fixed at 30 minutes on purpose. A change can cause a real problem well after the work ends (a write-latency step a day and a half
    /// after a planned resize was real), and a longer tail would hide that. Every SQL statement that needs the tail takes it from
    /// <see cref="TailIntervalSql"/>, so a change is made here only.
    /// </summary>
    public const int TailMinutes = 30;

    /// <summary>The tail as an interval literal, built from <see cref="TailMinutes"/> so the SQL never holds a second copy of the number.</summary>
    public static readonly string TailIntervalSql = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"interval '{TailMinutes} minutes'");

    /// <summary>The <c>held</c> filter word that lists only the requests a window is holding (<c>list_investigation_requests</c>).</summary>
    public const string HeldOnly = "only";

    /// <summary>The <c>held</c> filter word that lists everything a window is not holding, a request whose hold a late join released included.</summary>
    public const string HeldExclude = "exclude";

    /// <summary>
    /// The window that holds a request opening at <c>{openedAt}</c> on server <c>{serverId}</c>: the newest-ending one not cleared
    /// before the open, whose server list is NULL (every server) or names the server, with <c>starts_at &lt; opened_at &lt;= ends_at + tail</c>.
    /// A window cleared at time c holds nothing opened after c. A scalar subquery, NULL when no window holds the request. The
    /// <c>ends_at</c> bound is written against the column, so the index on <c>ends_at</c> serves it.
    /// </summary>
    public static string HeldWindowIdSql(string openedAt, string serverId) => $@"(SELECT w.window_id
        FROM config.investigation_change_windows AS w
        WHERE w.ends_at >= {openedAt} - {TailIntervalSql}
          AND w.starts_at < {openedAt}
          AND (w.cleared_at IS NULL OR w.cleared_at >= {openedAt})
          AND (w.server_ids IS NULL OR {serverId} = ANY (w.server_ids))
        ORDER BY w.ends_at DESC, w.window_id DESC
        LIMIT 1)";

    /// <summary>The window starts later than now.</summary>
    public const string StateUpcoming = "upcoming";

    /// <summary>Now is inside the window: it holds requests that open now.</summary>
    public const string StateOpen = "open";

    /// <summary>The window ended, and its tail still holds requests that open now.</summary>
    public const string StateInTail = "in_tail";

    /// <summary>The tail ended, or the window was cleared: it holds nothing that opens now.</summary>
    public const string StateClosed = "closed";

    /// <summary>When a window's tail ends: <c>ends_at</c> plus <see cref="TailMinutes"/>.</summary>
    public static DateTime TailEnd(DateTime endsAt) => endsAt.AddMinutes(TailMinutes);

    /// <summary>
    /// A window's state at <paramref name="now"/>, on the same edges as <see cref="HeldWindowIdSql"/>: it holds an open time t when
    /// <c>starts &lt; t &lt;= tail end</c> and t is not after <c>cleared_at</c>. So it is upcoming up to and including its start, open up to and
    /// including its end, in its tail up to and including the tail end, and closed after that, or after <c>cleared_at</c> when that is earlier.
    /// </summary>
    public static string StateOf(DateTime startsAt, DateTime endsAt, DateTime? clearedAt, DateTime now)
    {
        var heldUntil = TailEnd(endsAt);
        if (clearedAt is DateTime cleared && cleared < heldUntil)
        {
            heldUntil = cleared;
        }

        if (now > heldUntil)
        {
            return StateClosed;
        }

        if (now <= startsAt)
        {
            return StateUpcoming;
        }

        return now <= endsAt ? StateOpen : StateInTail;
    }

    /// <summary>The rung (V318): one table and its index, two nullable columns on the requests and a partial index on the new link. All catalog-only, no data moves.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS config.investigation_change_windows
(
    window_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_ids integer[] NULL,
    starts_at timestamptz NOT NULL,
    ends_at timestamptz NOT NULL,
    reason text NOT NULL,
    declared_by text NOT NULL,
    declared_at timestamptz NOT NULL DEFAULT now(),
    cleared_at timestamptz NULL,
    summarized_at timestamptz NULL,
    CONSTRAINT ck_investigation_change_windows_span CHECK (ends_at > starts_at)
);

CREATE INDEX IF NOT EXISTS ix_investigation_change_windows_ends
    ON config.investigation_change_windows (ends_at DESC);

ALTER TABLE config.investigation_requests
    ADD COLUMN IF NOT EXISTS held_window_id bigint NULL,
    ADD COLUMN IF NOT EXISTS hold_released_at timestamptz NULL;

CREATE INDEX IF NOT EXISTS ix_investigation_requests_held_window
    ON config.investigation_requests (held_window_id) WHERE held_window_id IS NOT NULL;

REVOKE ALL ON config.investigation_change_windows FROM PUBLIC;
DO $do$
DECLARE
   r text;
BEGIN
   /* Every role provision-roles.sql makes (admin, viewer, web, mcp) may read the table and none may write it: the service writes it as the store
      owner. The SELECT grant matters for mcp on a store the operator provisioned himself: that script gives mcp no default read on config, and
      list_investigation_requests now joins this table, so without the grant the tool fails until the script is run again. */
   FOREACH r IN ARRAY ARRAY['admin', 'viewer', 'web', 'mcp']
   LOOP
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = r) THEN
         EXECUTE pg_catalog.format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config.investigation_change_windows FROM %I', r);
         EXECUTE pg_catalog.format('GRANT SELECT ON config.investigation_change_windows TO %I', r);
      END IF;
   END LOOP;
END $do$;";
}
