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

/// <summary>One alert row's identity: the (alert_time, server_id, metric_name) triple the dismiss writes key on.</summary>
public readonly record struct AlertDismissKey(DateTime AlertTime, int ServerId, string MetricName);

/// <summary>What a keyed dismiss did. <see cref="Requested"/> counts distinct keys; <see cref="Known"/> is how
/// many of them name an alert row that exists (dismissed or not); <see cref="Dismissed"/> is the rows the
/// UPDATE changed.</summary>
public readonly record struct AlertDismissOutcome(int Requested, int Known, int Dismissed);

/// <summary>
/// The one copy of the alert-dismiss write: the Viewer's Dismiss Selected / Dismiss All and the web
/// dashboard's dismiss route both run these statements.
///
/// <para>Each UPDATE sets exactly one column, <c>config_alert_log.dismissed</c>, and no other. That is the
/// whole write surface the <c>viewer</c> role's column-level grant covers
/// (<c>GRANT UPDATE (dismissed) ON config.config_alert_log TO viewer</c>), so a change that makes one of these
/// statements set a second column fails 42501 for the web host until the grant in
/// <c>DarlingManagedRoles</c> and <c>tools/provision-roles.sql</c> grows with it.</para>
///
/// <para>The commands carry the caller's deadline and are returned unexecuted so each surface keeps its own
/// error translation (the Viewer turns a 42501 into its read-only message).</para>
/// </summary>
public static class AlertDismissStore
{
    /// <summary>
    /// Dismiss the given rows by (alert_time, server_id, metric_name). One set-based UPDATE keyed off
    /// unnested arrays (so the whole batch dismisses in a single round-trip). Already-dismissed rows are
    /// left alone by the <c>dismissed = FALSE</c> guard.
    /// </summary>
    public const string DismissAlertsSql = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE
AND    (alert_time, server_id, metric_name) IN (
    SELECT t.alert_time, t.server_id, t.metric_name
    FROM   unnest($1::timestamp[], $2::integer[], $3::text[]) AS t(alert_time, server_id, metric_name)
)";

    /// <summary>Dismiss all visible rows in the window across every server. $1 window start.</summary>
    public const string DismissAllAlertsSql = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time >= $1
AND    dismissed = FALSE";

    /// <summary>Dismiss all visible rows in the window for one server. $1 window start, $2 server_id.</summary>
    public const string DismissAllAlertsForServerSql = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time >= $1
AND    server_id = $2
AND    dismissed = FALSE";

    /// <summary>How many of the distinct keys name an alert row that exists, dismissed or not. Same
    /// parameters as <see cref="DismissAlertsSql"/>.</summary>
    public const string CountKnownKeysSql = @"
SELECT count(*)::integer
FROM   (SELECT DISTINCT t.alert_time, t.server_id, t.metric_name
        FROM   unnest($1::timestamp[], $2::integer[], $3::text[]) AS t(alert_time, server_id, metric_name)) k
WHERE  EXISTS (SELECT 1 FROM config_alert_log a
               WHERE  a.alert_time = k.alert_time AND a.server_id = k.server_id AND a.metric_name = k.metric_name)";

    /// <summary>The keyed dismiss command (<see cref="DismissAlertsSql"/>) over parallel key arrays.</summary>
    public static NpgsqlCommand CreateDismissCommand(
        NpgsqlDataSource dataSource, IReadOnlyList<AlertDismissKey> keys, int timeoutSeconds)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        var command = dataSource.CreateCommand(DismissAlertsSql);
        command.CommandTimeout = timeoutSeconds;
        BindKeys(command, keys);
        return command;
    }

    /// <summary>The window dismiss command: every visible row since <paramref name="sinceUtc"/>, scoped to one
    /// server when <paramref name="serverId"/> is set.</summary>
    public static NpgsqlCommand CreateDismissAllCommand(
        NpgsqlDataSource dataSource, DateTime sinceUtc, int? serverId, int timeoutSeconds)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        var command = dataSource.CreateCommand(serverId.HasValue ? DismissAllAlertsForServerSql : DismissAllAlertsSql);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        return command;
    }

    /// <summary>
    /// Dismisses the keyed rows and reports how many keys were unknown. The known-key count and the UPDATE are
    /// two statements without a transaction on purpose: the count only labels the response, and a row that
    /// appears or vanishes between them changes a count, never a write.
    /// </summary>
    public static async Task<AlertDismissOutcome> DismissKeysAsync(
        NpgsqlDataSource dataSource, IReadOnlyList<AlertDismissKey> keys, int timeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0)
        {
            return new AlertDismissOutcome(0, 0, 0);
        }

        var distinct = new HashSet<AlertDismissKey>(keys).Count;

        int known;
        await using (var count = dataSource.CreateCommand(CountKnownKeysSql))
        {
            count.CommandTimeout = timeoutSeconds;
            BindKeys(count, keys);
            known = Convert.ToInt32(await count.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
        }

        int dismissed;
        await using (var update = CreateDismissCommand(dataSource, keys, timeoutSeconds))
        {
            dismissed = await update.ExecuteNonQueryAsync(cancellationToken);
        }

        return new AlertDismissOutcome(distinct, known, dismissed);
    }

    private static void BindKeys(NpgsqlCommand command, IReadOnlyList<AlertDismissKey> keys)
    {
        var times = new DateTime[keys.Count];
        var ids = new int[keys.Count];
        var metrics = new string[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            times[i] = DateTime.SpecifyKind(keys[i].AlertTime, DateTimeKind.Unspecified);
            ids[i] = keys[i].ServerId;
            metrics[i] = keys[i].MetricName;
        }

        command.Parameters.Add(new NpgsqlParameter { Value = times });
        command.Parameters.Add(new NpgsqlParameter { Value = ids });
        command.Parameters.Add(new NpgsqlParameter { Value = metrics });
    }
}
