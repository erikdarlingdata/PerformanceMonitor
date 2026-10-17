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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane writes to <c>config.config_notification_routes</c> (#3598, V131) — the sparse
/// routes the Settings window's Notifications section authors beside the parent channel fields. Same
/// discipline as <see cref="ViewerDataService"/>'s other write partials: public-const SQL (Darling.Tests pin
/// the column parity with the service's <c>StoreConfigProvider.NotificationRoutesSelectSql</c>), bound
/// <c>$N</c> parameters, routed through <see cref="ExecuteWriteAsync"/> so a read-only seat degrades to
/// <see cref="ViewerReadOnlyException"/>. The V131 trigger bumps <c>config_version</c> on every statement
/// here, so the running service re-reads the routes on its next sweep — no IPC.
///
/// <para><b>The secret columns, and the read-only seat.</b> Four of the five destination columns are the
/// same bearer secrets the parent row's are and are column-REVOKEd from the <c>viewer</c> role
/// (<c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>), so a read-only seat reads the secret-free
/// projection (<see cref="NotificationRoutesSelectNoSecretSql"/>) — <c>configured_channels</c>, the
/// GENERATED presence column, says which channels each route sets without disclosing a value, which is
/// what the read-only grid renders. An <c>admin</c> seat reads the full row and edits it.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /* The five destination columns in the SAME order the service reads them
       (StoreConfigProvider.NotificationRoutesSelectSql), so the parity test pins one list against both ends. */
    private const string NotificationRouteDestinationColumns =
        "teams_url, slack_url, generic_url, pagerduty_routing_key, smtp_recipients";

    /* The agent channel's five columns (V184, #5632), in the SAME order the service reads them. Not secrets: both
       projections carry them, so a read-only seat sees which routes send alerts to agents. */
    private const string NotificationRouteAgentColumns =
        "agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes";

    /// <summary>The full row, for an <c>admin</c> seat, with the two scope columns (V181, #5750). Ordered by
    /// <c>route_id</c>; the window re-sorts the grid newest first. The resolver's order is the scope tier, then the
    /// metric level, then the route id, so the id order is no longer the order routes are consulted in.</summary>
    public const string NotificationRoutesSelectSql =
        "SELECT route_id, metric_match, " + NotificationRouteDestinationColumns + ", enabled, modified_at, configured_channels, scope_server_id, scope_tag_id, "
        + NotificationRouteAgentColumns + ", send_resolutions "
        + "FROM config_notification_routes ORDER BY route_id";

    /// <summary>The secret-free projection a read-only <c>viewer</c> seat reads: the four webhook-class
    /// destinations are omitted (they 42501 for that role), <c>smtp_recipients</c> stays (an address list is
    /// not a secret, on the parent either), and <c>configured_channels</c> carries presence.</summary>
    public const string NotificationRoutesSelectNoSecretSql =
        "SELECT route_id, metric_match, smtp_recipients, enabled, modified_at, configured_channels, scope_server_id, scope_tag_id, "
        + NotificationRouteAgentColumns + ", send_resolutions "
        + "FROM config_notification_routes ORDER BY route_id";

    /// <summary>Inserts one route. <c>route_id</c> is <c>GENERATED ALWAYS AS IDENTITY</c>, so it is never in
    /// the column list; RETURNING hands it back for the grid.</summary>
    public const string NotificationRouteInsertSql = @"
INSERT INTO config_notification_routes
    (metric_match, " + NotificationRouteDestinationColumns + @", enabled, scope_server_id, scope_tag_id, " + NotificationRouteAgentColumns + @", send_resolutions, modified_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, (now() AT TIME ZONE 'UTC'))
RETURNING route_id";

    public const string NotificationRouteUpdateSql = @"
UPDATE config_notification_routes SET
    metric_match = $2,
    teams_url = $3,
    slack_url = $4,
    generic_url = $5,
    pagerduty_routing_key = $6,
    smtp_recipients = $7,
    enabled = $8,
    scope_server_id = $9,
    scope_tag_id = $10,
    agent_level = $11,
    agent_max_per_hour = $12,
    agent_max_per_day = $13,
    agent_lease_minutes = $14,
    agent_expiry_minutes = $15,
    send_resolutions = $16,
    modified_at = (now() AT TIME ZONE 'UTC')
WHERE route_id = $1";

    public const string NotificationRouteSetEnabledSql =
        "UPDATE config_notification_routes SET enabled = $2, modified_at = (now() AT TIME ZONE 'UTC') WHERE route_id = $1";

    public const string NotificationRouteDeleteSql = "DELETE FROM config_notification_routes WHERE route_id = $1";

    /// <summary>Every route, in resolution order. A read-only seat gets the secret-free projection.</summary>
    public async Task<IReadOnlyList<NotificationRouteRow>> GetNotificationRoutesAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<NotificationRouteRow>();
        await using var command = _dataSource.CreateCommand(IsReadOnly ? NotificationRoutesSelectNoSecretSql : NotificationRoutesSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(IsReadOnly ? ReadRouteRowNoSecret(reader) : ReadRouteRow(reader));
        }

        return rows;
    }

    /// <summary>Inserts the route and returns its generated <c>route_id</c>.</summary>
    public async Task<int> InsertNotificationRouteAsync(NotificationRouteRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        ValidateRoute(row);

        await using var command = _dataSource.CreateCommand(NotificationRouteInsertSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.MetricMatch.Trim() });      // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.TeamsUrl.Trim() });          // $2
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SlackUrl.Trim() });          // $3
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.GenericUrl.Trim() });        // $4
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.PagerDutyRoutingKey.Trim() }); // $5
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SmtpRecipients.Trim() });    // $6
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.Enabled });                    // $7
        AddNullableInt(command, row.ScopeServerId);                                                        // $8
        AddNullableInt(command, row.ScopeTagId);                                                           // $9
        AddAgentColumns(command, row);                                                                     // $10-$15
        var id = await ExecuteWriteScalarAsync(command, cancellationToken);
        return Convert.ToInt32(id, System.Globalization.CultureInfo.InvariantCulture);
    }

    public async Task UpdateNotificationRouteAsync(NotificationRouteRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        ValidateRoute(row);

        await using var command = _dataSource.CreateCommand(NotificationRouteUpdateSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.RouteId });                     // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.MetricMatch.Trim() });      // $2
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.TeamsUrl.Trim() });          // $3
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SlackUrl.Trim() });          // $4
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.GenericUrl.Trim() });        // $5
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.PagerDutyRoutingKey.Trim() }); // $6
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SmtpRecipients.Trim() });    // $7
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.Enabled });                    // $8
        AddNullableInt(command, row.ScopeServerId);                                                        // $9
        AddNullableInt(command, row.ScopeTagId);                                                           // $10
        AddAgentColumns(command, row);                                                                     // $11-$16
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Saves a route with its webhook destinations sealed (#5366) and returns its <c>route_id</c>. A new route is inserted with
    /// every destination empty to get its id, because a sealed value is bound to the row it is stored in, then updated with the
    /// sealed values, in one transaction: a refusal rolls the insert back. <paramref name="stored"/> is the row as read, for the
    /// values a blank box keeps; <paramref name="parent"/> is the settings row, which holds each channel's proxy.
    /// </summary>
    public async Task<int> SaveNotificationRouteSealedAsync(
        NotificationRouteRow typed, NotificationRouteRow? stored, NotificationRow parent, ViewerPasswordSealer? sealer,
        string? refusal, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(typed);
        ArgumentNullException.ThrowIfNull(parent);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            var routeId = typed.RouteId;
            if (routeId == 0)
            {
                await using var insert = new NpgsqlCommand(NotificationRouteInsertSql, connection, transaction);
                insert.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = typed.MetricMatch.Trim() });
                for (var i = 0; i < 5; i++)
                {
                    insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "" });
                }

                insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = typed.Enabled });
                /* The scope goes in with the insert: a scoped "*" route would fail the V181 CHECK with no scope. */
                AddNullableInt(insert, typed.ScopeServerId);
                AddNullableInt(insert, typed.ScopeTagId);
                /* The agent columns too: an agent-only route has no destination for the update to add. */
                AddAgentColumns(insert, typed);
                routeId = Convert.ToInt32(await insert.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
            }

            var resolved = typed.Clone();
            resolved.RouteId = routeId;
            ViewerWebhookSealing.ResolveRoute(resolved, stored, parent, routeId, sealer, refusal);
            ValidateRoute(resolved);

            /* #5998: a signing key is sealed for the route's stored URL text, so a save that changes that text would leave the key
               unreadable to the service. A new route has no key. */
            if (typed.RouteId != 0 && await RouteKeyWouldBreakAsync(connection, transaction, routeId, resolved.GenericUrl, cancellationToken))
            {
                throw new ViewerPasswordRefusedException(ViewerWebhookSigningKeys.RemoveKeyFirstText);
            }

            await using var update = new NpgsqlCommand(NotificationRouteUpdateSql, connection, transaction);
            update.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            update.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });                          // $1
            update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.MetricMatch.Trim() });    // $2
            update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.TeamsUrl });              // $3
            update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.SlackUrl });              // $4
            update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.GenericUrl });            // $5
            update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.PagerDutyRoutingKey });   // $6
            update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.SmtpRecipients.Trim() }); // $7
            update.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = resolved.Enabled });                 // $8
            AddNullableInt(update, resolved.ScopeServerId);                                                      // $9
            AddNullableInt(update, resolved.ScopeTagId);                                                         // $10
            AddAgentColumns(update, resolved);                                                                   // $11-$15
            await update.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return routeId;
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>Whether the route holds a signing key sealed for a stored URL text other than <paramref name="newUrlText"/> (#5998).
    /// The read takes the row's lock (<c>FOR UPDATE</c>) and the caller's transaction keeps it to the commit, so a Generate in another
    /// viewer waits, then finds the URL changed and is refused: no key is left tied to the old URL.</summary>
    private static async Task<bool> RouteKeyWouldBreakAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, string newUrlText, CancellationToken cancellationToken)
    {
        await using var keyRead = new NpgsqlCommand(
            "SELECT signing_key <> '', generic_url FROM config_notification_routes WHERE route_id = $1 FOR UPDATE", connection, transaction);
        keyRead.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        keyRead.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        await using var keyReader = await keyRead.ExecuteReaderAsync(cancellationToken);
        return await keyReader.ReadAsync(cancellationToken) && keyReader.GetBoolean(0)
            && !string.Equals(newUrlText, keyReader.GetString(1), StringComparison.Ordinal);
    }

    public async Task SetNotificationRouteEnabledAsync(int routeId, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(NotificationRouteSetEnabledSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    public async Task DeleteNotificationRouteAsync(int routeId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(NotificationRouteDeleteSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// The write-side validation the edit dialog also runs, so a row the dialog accepts is a row the store
    /// accepts: a non-blank match (the V131 CHECK's twin), and at least one destination — a route with every
    /// column empty inherits everything and would read in the grid as a configured route that does nothing. Since
    /// V181 (#5750) it also refuses a route scoped to both a server and a tag, and the every-metric match
    /// (<see cref="NotificationRoute.WildcardMatch"/>) on a route with no scope: the store's two CHECKs, said in words
    /// before the statement runs.
    /// </summary>
    public static string? ValidateNotificationRoute(NotificationRouteRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        if (string.IsNullOrWhiteSpace(row.MetricMatch))
        {
            return "Choose a family or enter an exact metric name.";
        }

        if (row.ScopeServerId.HasValue && row.ScopeTagId.HasValue)
        {
            return "A route is scoped to one server or one tag, not both.";
        }

        if (row.MetricMatch.Trim() == NotificationRoute.WildcardMatch && !row.ScopeServerId.HasValue && !row.ScopeTagId.HasValue)
        {
            return "Every metric (*) needs a server or a tag scope. Pick one, or choose a family or an exact metric.";
        }

        if (!row.HasAnyDestination && !row.AgentLevel.HasValue)
        {
            return "Set at least one destination or the agent channel — a route with every channel empty inherits everything and routes nothing.";
        }

        if (row.AgentLevel is { } level && level is not (NotificationRoute.AgentLevelOff or NotificationRoute.AgentLevelInvestigate or NotificationRoute.AgentLevelAct))
        {
            return "The agent channel is Inherit, Off, Investigate or Act.";
        }

        return ValidateAgentLimit("Requests per hour", row.AgentMaxPerHour, NotificationRoute.MinAgentMaxPerHour, NotificationRoute.MaxAgentMaxPerHour)
            ?? ValidateAgentLimit("Requests per day", row.AgentMaxPerDay, NotificationRoute.MinAgentMaxPerDay, NotificationRoute.MaxAgentMaxPerDay)
            ?? ValidateAgentLimit("Lease minutes", row.AgentLeaseMinutes, NotificationRoute.MinAgentLeaseMinutes, NotificationRoute.MaxAgentLeaseMinutes)
            ?? ValidateAgentLimit("Expiry minutes", row.AgentExpiryMinutes, NotificationRoute.MinAgentExpiryMinutes, NotificationRoute.MaxAgentExpiryMinutes);
    }

    /// <summary>One agent limit against the V184 CHECK's bounds (#5632), said in words before the statement runs.</summary>
    private static string? ValidateAgentLimit(string name, int value, int min, int max) =>
        value < min || value > max
            ? string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{name} must be from {min} to {max}.")
            : null;

    /// <summary>Binds the five agent columns in <see cref="NotificationRouteAgentColumns"/> order (#5632).</summary>
    private static void AddAgentColumns(NpgsqlCommand command, NotificationRouteRow row)
    {
        AddNullableShort(command, row.AgentLevel);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentMaxPerHour });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentMaxPerDay });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentLeaseMinutes });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentExpiryMinutes });
        /* #5825 (V187): the resolution override, bound right after the agent columns in every insert and update. NULL is
           "use the channel's setting". */
        AddNullableBool(command, row.SendResolutions);
    }

    /// <summary>The <c>smallint</c> <c>agent_level</c>: a value, or SQL NULL for "inherit".</summary>
    private static void AddNullableShort(NpgsqlCommand command, int? value) =>
        command.Parameters.Add(value.HasValue
            ? new NpgsqlParameter<short> { TypedValue = (short)value.Value }
            : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Smallint });

    private static void ValidateRoute(NotificationRouteRow row)
    {
        var error = ValidateNotificationRoute(row);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(row));
        }
    }

    private static NotificationRouteRow ReadRouteRow(NpgsqlDataReader reader) => new()
    {
        RouteId = reader.GetInt32(0),
        MetricMatch = reader.GetString(1),
        TeamsUrl = reader.GetString(2),
        SlackUrl = reader.GetString(3),
        GenericUrl = reader.GetString(4),
        PagerDutyRoutingKey = reader.GetString(5),
        SmtpRecipients = reader.GetString(6),
        Enabled = reader.GetBoolean(7),
        ModifiedAtUtc = DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Utc),
        ConfiguredChannels = reader.IsDBNull(9) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(9),
        ScopeServerId = reader.IsDBNull(10) ? null : reader.GetInt32(10),
        ScopeTagId = reader.IsDBNull(11) ? null : reader.GetInt32(11),
        AgentLevel = reader.IsDBNull(12) ? null : reader.GetInt16(12),
        AgentMaxPerHour = reader.GetInt32(13),
        AgentMaxPerDay = reader.GetInt32(14),
        AgentLeaseMinutes = reader.GetInt32(15),
        AgentExpiryMinutes = reader.GetInt32(16),
        SendResolutions = reader.IsDBNull(17) ? null : reader.GetBoolean(17),
    };

    /// <summary>The secret-free reader: the four carved destinations stay empty (the seat cannot author them
    /// anyway) and every later column shifts ordinal, the <c>ReadNotificationRowNoSecret</c> idiom.</summary>
    private static NotificationRouteRow ReadRouteRowNoSecret(NpgsqlDataReader reader) => new()
    {
        RouteId = reader.GetInt32(0),
        MetricMatch = reader.GetString(1),
        TeamsUrl = "",               /* carved — not selected for a read-only viewer (#1262 / V131) */
        SlackUrl = "",               /* carved */
        GenericUrl = "",             /* carved */
        PagerDutyRoutingKey = "",    /* carved */
        SmtpRecipients = reader.GetString(2),
        Enabled = reader.GetBoolean(3),
        ModifiedAtUtc = DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc),
        ConfiguredChannels = reader.IsDBNull(5) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(5),
        ScopeServerId = reader.IsDBNull(6) ? null : reader.GetInt32(6),
        ScopeTagId = reader.IsDBNull(7) ? null : reader.GetInt32(7),
        AgentLevel = reader.IsDBNull(8) ? null : reader.GetInt16(8),
        AgentMaxPerHour = reader.GetInt32(9),
        AgentMaxPerDay = reader.GetInt32(10),
        AgentLeaseMinutes = reader.GetInt32(11),
        AgentExpiryMinutes = reader.GetInt32(12),
        SendResolutions = reader.IsDBNull(13) ? null : reader.GetBoolean(13),
    };
}

/// <summary>
/// A <c>config.config_notification_routes</c> row as the viewer authors and reads it (#3598) — the desired-state
/// twin of the service's <see cref="NotificationRoute"/>. Empty destination = inherit the parent channel's;
/// <see cref="ConfiguredChannels"/> is the store's GENERATED presence list, read back rather than derived so a
/// read-only seat that cannot see the URLs still shows which channels a route sets.
/// </summary>
public sealed class NotificationRouteRow
{
    public int RouteId { get; set; }
    public string MetricMatch { get; set; } = "";
    public string TeamsUrl { get; set; } = "";
    public string SlackUrl { get; set; } = "";
    public string GenericUrl { get; set; } = "";
    public string PagerDutyRoutingKey { get; set; } = "";
    public string SmtpRecipients { get; set; } = "";
    public bool Enabled { get; set; } = true;
    public DateTime ModifiedAtUtc { get; set; }
    public IReadOnlyList<string> ConfiguredChannels { get; set; } = Array.Empty<string>();

    /// <summary>The registry <c>server_id</c> this route is scoped to, or null (V181, #5750). At most one of the two scope
    /// ids is set; both null means fleet-wide.</summary>
    public int? ScopeServerId { get; set; }

    /// <summary>The server tag this route is scoped to, with all its sub-tags, or null (V181, #5750).</summary>
    public int? ScopeTagId { get; set; }

    /// <summary>The agent channel (V184, #5632): null inherits, 0 is off and stops the lookup, 1 opens Level 1 investigation
    /// requests. See <see cref="NotificationRoute.AgentLevel"/>.</summary>
    public int? AgentLevel { get; set; }

    /// <summary>The most requests this route opens in an hour (#5632).</summary>
    public int AgentMaxPerHour { get; set; } = NotificationRoute.DefaultAgentMaxPerHour;

    /// <summary>The most requests this route opens in a day (#5632).</summary>
    public int AgentMaxPerDay { get; set; } = NotificationRoute.DefaultAgentMaxPerDay;

    /// <summary>An agent's claim lasts this long before the request opens again (#5632).</summary>
    public int AgentLeaseMinutes { get; set; } = NotificationRoute.DefaultAgentLeaseMinutes;

    /// <summary>A request nobody claims expires after this long (#5632).</summary>
    public int AgentExpiryMinutes { get; set; } = NotificationRoute.DefaultAgentExpiryMinutes;

    /// <summary>Whether this route's destinations get an alert's resolution (#5825): null uses each channel's setting,
    /// true always sends it, false never does.</summary>
    public bool? SendResolutions { get; set; }

    /// <summary>The Scope column: "All servers", the server's display name or the tag path. Filled by the window from the
    /// registry and the tag tree (<see cref="ViewerNotificationRouteScope.ScopeText"/>); not stored.</summary>
    public string ScopeDisplay { get; set; } = ViewerNotificationRouteScope.AllServersLabel;

    /// <summary>The Status column (<see cref="ViewerNotificationRouteScope.StatusText"/>). Filled by the window; not stored.</summary>
    public string StatusDisplay { get; set; } = "";

    /// <summary>The family this route matches, or null for an exact metric name.</summary>
    public string? Family => AlertFamily.NormalizeFamily(MetricMatch);

    /// <summary>Whether any destination column is set on THIS row's values (the edit dialog's test; the
    /// store's <see cref="ConfiguredChannels"/> is the same fact for a row read back).</summary>
    public bool HasAnyDestination =>
        !string.IsNullOrWhiteSpace(TeamsUrl) || !string.IsNullOrWhiteSpace(SlackUrl)
        || !string.IsNullOrWhiteSpace(GenericUrl) || !string.IsNullOrWhiteSpace(PagerDutyRoutingKey)
        || !string.IsNullOrWhiteSpace(SmtpRecipients);

    /* Grid display members. */
    public string MatchKindDisplay =>
        MetricMatch.Trim() == NotificationRoute.WildcardMatch ? "Every metric"
        : Family is not null ? "Family"
        : NotificationRoute.AnalysisCategoryOf(MetricMatch) is not null ? "Analysis category"
        : "Exact metric";

    /// <summary>The Channels column: the store's presence list, then the agent channel when this route sets it (#5632). The
    /// GENERATED <c>configured_channels</c> covers the destination columns only, so an agent-only route reads "agent" here
    /// and not "—".</summary>
    public string ChannelsDisplay
    {
        get
        {
            var channels = new List<string>(ConfiguredChannels);
            if (AgentLevel is { } level)
            {
                channels.Add(level is NotificationRoute.AgentLevelInvestigate or NotificationRoute.AgentLevelAct ? "agent" : "agent off");
            }

            return channels.Count == 0 ? "—" : string.Join(", ", channels);
        }
    }

    /// <summary>The Agent column (#5632): Inherit, Off, Investigate, or Act (#5912, Level 2) with its hourly and daily limits.</summary>
    public string AgentDisplay => AgentLevel switch
    {
        null => "Inherit",
        NotificationRoute.AgentLevelInvestigate => string.Create(System.Globalization.CultureInfo.InvariantCulture, $"Investigate ({AgentMaxPerHour}/h, {AgentMaxPerDay}/day)"),
        NotificationRoute.AgentLevelAct => string.Create(System.Globalization.CultureInfo.InvariantCulture, $"Act ({AgentMaxPerHour}/h, {AgentMaxPerDay}/day)"),
        _ => "Off",
    };

    /// <summary>
    /// When the route was last saved, in the viewer's display mode (#4766). <see cref="ModifiedAtUtc"/> is the store's
    /// <c>modified_at</c>, written as <c>now() AT TIME ZONE 'UTC'</c>, so it is one UTC instant, and it reads as the
    /// other grids' times do: Server, Local or UTC per the mode, with the UTC offset added in the repeated autumn
    /// hour (<see cref="ViewerTimeHelper.FormatForDisplay(DateTime, string)"/>). This used to be the machine's local
    /// time in every mode. Empty for a route that has not been stored yet.
    /// </summary>
    public string ModifiedDisplay => ModifiedAtUtc == default ? "" : ViewerTimeHelper.FormatForDisplay(ModifiedAtUtc, "yyyy-MM-dd HH:mm:ss");

    public NotificationRouteRow Clone() => new()
    {
        RouteId = RouteId,
        MetricMatch = MetricMatch,
        TeamsUrl = TeamsUrl,
        SlackUrl = SlackUrl,
        GenericUrl = GenericUrl,
        PagerDutyRoutingKey = PagerDutyRoutingKey,
        SmtpRecipients = SmtpRecipients,
        Enabled = Enabled,
        ModifiedAtUtc = ModifiedAtUtc,
        ConfiguredChannels = ConfiguredChannels,
        ScopeServerId = ScopeServerId,
        ScopeTagId = ScopeTagId,
        AgentLevel = AgentLevel,
        AgentMaxPerHour = AgentMaxPerHour,
        AgentMaxPerDay = AgentMaxPerDay,
        AgentLeaseMinutes = AgentLeaseMinutes,
        AgentExpiryMinutes = AgentExpiryMinutes,
        SendResolutions = SendResolutions,
        ScopeDisplay = ScopeDisplay,
        StatusDisplay = StatusDisplay,
    };
}
