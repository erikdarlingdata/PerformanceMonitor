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
using PerformanceMonitor.Darling.Storage;
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
    private const string NotificationRouteDestinationColumns = NotificationRouteWriter.DestinationColumns;

    /* The agent channel's five columns (V184, #5632), in the SAME order the service reads them. Not secrets: both
       projections carry them, so a read-only seat sees which routes send alerts to agents. */
    private const string NotificationRouteAgentColumns = NotificationRouteWriter.AgentColumns;

    /* The columns both projections share, so the V229 variants below differ only by the one appended column. */
    private const string NotificationRoutesAdminColumns =
        "route_id, metric_match, " + NotificationRouteDestinationColumns + ", enabled, modified_at, configured_channels, scope_server_id, scope_tag_id, "
        + NotificationRouteAgentColumns + ", send_resolutions, fallback_route_id";

    private const string NotificationRoutesNoSecretColumns =
        "route_id, metric_match, smtp_recipients, enabled, modified_at, configured_channels, scope_server_id, scope_tag_id, "
        + NotificationRouteAgentColumns + ", send_resolutions, fallback_route_id";

    /// <summary>The store version that added <c>config_notification_routes.severities</c> (V229, #6928). A store below it has no column, so
    /// the list reads the statements without it and every route shows as Both.</summary>
    internal const int RouteSeveritiesStoreSchemaVersion = 229;

    /// <summary>The store version that added <c>config_notification_routes.report_notice</c> (V233, #6979). A store below it has no column,
    /// so the list reads the statements without it and every route shows as Off.</summary>
    internal const int RouteReportNoticeStoreSchemaVersion = 233;

    /// <summary>The full row, for an <c>admin</c> seat, with the two scope columns (V181, #5750), for a store below V229 (no
    /// <c>severities</c> column). Ordered by <c>route_id</c>; the window re-sorts the grid newest first. The resolver's order is the
    /// scope tier, then the metric level, then the route id, so the id order is no longer the order routes are consulted in.</summary>
    public const string NotificationRoutesSelectSql =
        "SELECT " + NotificationRoutesAdminColumns + " FROM config_notification_routes ORDER BY route_id";

    /// <summary>The secret-free projection a read-only <c>viewer</c> seat reads: the four webhook-class
    /// destinations are omitted (they 42501 for that role), <c>smtp_recipients</c> stays (an address list is
    /// not a secret, on the parent either), and <c>configured_channels</c> carries presence.</summary>
    public const string NotificationRoutesSelectNoSecretSql =
        "SELECT " + NotificationRoutesNoSecretColumns + " FROM config_notification_routes ORDER BY route_id";

    /// <summary>The full row with the severity filter as the last column (V229, #6928), for a store that has it.</summary>
    public const string NotificationRoutesSelectWithSeveritiesSql =
        "SELECT " + NotificationRoutesAdminColumns + ", severities FROM config_notification_routes ORDER BY route_id";

    /// <summary>The secret-free projection with the severity filter as the last column (V229, #6928). The column is not a secret.</summary>
    public const string NotificationRoutesSelectNoSecretWithSeveritiesSql =
        "SELECT " + NotificationRoutesNoSecretColumns + ", severities FROM config_notification_routes ORDER BY route_id";

    /// <summary>The full row with the severity filter and the report notice setting as the last two columns (V233, #6979).</summary>
    public const string NotificationRoutesSelectWithReportNoticeSql =
        "SELECT " + NotificationRoutesAdminColumns + ", severities, report_notice FROM config_notification_routes ORDER BY route_id";

    /// <summary>The secret-free projection with the severity filter and the report notice setting as the last two columns (V233, #6979). Neither is a secret.</summary>
    public const string NotificationRoutesSelectNoSecretWithReportNoticeSql =
        "SELECT " + NotificationRoutesNoSecretColumns + ", severities, report_notice FROM config_notification_routes ORDER BY route_id";

    /// <summary>Inserts one route (#6231: the one copy is <see cref="NotificationRouteWriter.InsertSql"/>, which the MCP tools share).</summary>
    public const string NotificationRouteInsertSql = NotificationRouteWriter.InsertSql;

    public const string NotificationRouteUpdateSql = NotificationRouteWriter.UpdateSql;

    public const string NotificationRouteSetEnabledSql = NotificationRouteWriter.SetEnabledSql;

    /// <summary>The column has no foreign key (V193), so a delete clears the references to the deleted route first (#6036).</summary>
    public const string NotificationRouteClearFallbackSql = NotificationRouteWriter.ClearFallbackSql;

    public const string NotificationRouteDeleteSql = NotificationRouteWriter.DeleteSql;

    /// <summary>Every route, in resolution order. A read-only seat gets the secret-free projection.</summary>
    public async Task<IReadOnlyList<NotificationRouteRow>> GetNotificationRoutesAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<NotificationRouteRow>();
        /* V229 (#6928): a store below it has no severities column, so the list reads the older statements and every route shows as Both. */
        var version = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        var hasSeverities = version is int known && known >= RouteSeveritiesStoreSchemaVersion;
        /* V233 (#6979): a store below it has no report_notice column, so every route shows as Off. */
        var hasReportNotice = version is int reportKnown && reportKnown >= RouteReportNoticeStoreSchemaVersion;
        var sql = IsReadOnly
            ? (hasReportNotice ? NotificationRoutesSelectNoSecretWithReportNoticeSql : hasSeverities ? NotificationRoutesSelectNoSecretWithSeveritiesSql : NotificationRoutesSelectNoSecretSql)
            : (hasReportNotice ? NotificationRoutesSelectWithReportNoticeSql : hasSeverities ? NotificationRoutesSelectWithSeveritiesSql : NotificationRoutesSelectSql);
        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(IsReadOnly ? ReadRouteRowNoSecret(reader, hasSeverities, hasReportNotice) : ReadRouteRow(reader, hasSeverities, hasReportNotice));
        }

        foreach (var row in rows.Where(r => r.FallbackRouteId.HasValue))
        {
            row.FallbackRouteName = rows.FirstOrDefault(o => o.RouteId == row.FallbackRouteId)?.MetricMatch;
        }

        return rows;
    }

    /// <summary>Inserts the route and returns its generated <c>route_id</c>.</summary>
    public async Task<int> InsertNotificationRouteAsync(NotificationRouteRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        ValidateRoute(row);
        await EnsureFallbackRouteExistsAsync(row, null, null, cancellationToken);

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
        AddAgentColumns(command, row);                                                                     // $10-$16
        var id = await ExecuteWriteScalarAsync(command, cancellationToken);
        return Convert.ToInt32(id, System.Globalization.CultureInfo.InvariantCulture);
    }

    public async Task UpdateNotificationRouteAsync(NotificationRouteRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        ValidateRoute(row);
        await EnsureFallbackRouteExistsAsync(row, null, null, cancellationToken);

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
        AddAgentColumns(command, row);                                                                     // $11-$17
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Saves a route with its webhook destinations sealed (#5366) and returns its <c>route_id</c>. The save itself is
    /// <see cref="NotificationRouteWriter.SaveAsync"/>, the one copy the MCP route tools share (#6231). <paramref name="stored"/> is the
    /// row as read, for the values a blank box keeps; <paramref name="parent"/> is the settings row, which holds each channel's proxy.
    /// #6274: the stored row's <c>ModifiedAtUtc</c> is the version this save expects (a route saved elsewhere since is refused, not
    /// overwritten), and <paramref name="login"/> is the person, whose change to the route's agent level, limits, scope or match is journaled.
    /// </summary>
    public Task<int> SaveNotificationRouteSealedAsync(
        NotificationRouteRow typed, NotificationRouteRow? stored, NotificationRow parent, ViewerPasswordSealer? sealer,
        string? refusal, CancellationToken cancellationToken = default) =>
        SaveNotificationRouteSealedAsync(typed, stored, parent, sealer, refusal, null, cancellationToken);

    /// <summary>The save with the person who makes it (#6274): see the overload above.</summary>
    public async Task<int> SaveNotificationRouteSealedAsync(
        NotificationRouteRow typed, NotificationRouteRow? stored, NotificationRow parent, ViewerPasswordSealer? sealer,
        string? refusal, string? login, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(typed);
        ArgumentNullException.ThrowIfNull(parent);

        Func<string, PasswordBinding, string>? seal = sealer is null ? null : (text, binding) =>
        {
            try
            {
                return sealer.SealWebhook(text, binding);
            }
            catch (ViewerPasswordRefusedException ex)
            {
                throw new NotificationRouteRefusedException(ex.Message, NotificationRouteRefusalKind.Value);
            }
        };

        try
        {
            return await NotificationRouteWriter.SaveAsync(
                _dataSource, typed.ToDraft(), stored?.ToDraft(),
                new WebhookProxies(parent.TeamsProxy, parent.SlackProxy, parent.GenericProxy, parent.PagerDutyProxy),
                seal, refusal, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken, login);
        }
        catch (NotificationRouteRefusedException ex) when (ex.Kind == NotificationRouteRefusalKind.Value)
        {
            throw new ViewerPasswordRefusedException(ex.Message);
        }
        catch (NotificationRouteRefusedException ex)
        {
            throw new ArgumentException(ex.Message, nameof(typed));
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

    public Task SetNotificationRouteEnabledAsync(int routeId, bool enabled, CancellationToken cancellationToken = default) =>
        SetNotificationRouteEnabledAsync(routeId, enabled, null, cancellationToken);

    /// <summary>Turns a route on or off. #6274: <paramref name="login"/> is the person; turning a route that has an agent level on or
    /// off writes a <c>route_agent_changed</c> journal row in the same transaction (a disabled Off route no longer stops the lookup).</summary>
    public async Task SetNotificationRouteEnabledAsync(int routeId, bool enabled, string? login, CancellationToken cancellationToken = default)
    {
        try
        {
            await NotificationRouteWriter.SetEnabledAsync(
                _dataSource, routeId, enabled, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken, login);
        }
        catch (NotificationRouteRefusedException ex)
        {
            throw new ArgumentException(ex.Message, nameof(login));
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

    public Task DeleteNotificationRouteAsync(int routeId, CancellationToken cancellationToken = default) =>
        DeleteNotificationRouteAsync(routeId, null, cancellationToken);

    /// <summary>Clears every other route's fallback reference to the route (the column has no foreign key), then deletes it,
    /// in one transaction (#6036). #6274: <paramref name="login"/> is the person; deleting a route that had an agent level writes a
    /// <c>route_agent_changed</c> journal row in the same transaction.</summary>
    public async Task DeleteNotificationRouteAsync(int routeId, string? login, CancellationToken cancellationToken = default)
    {
        try
        {
            await NotificationRouteWriter.DeleteAsync(
                _dataSource, routeId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken, login);
        }
        catch (NotificationRouteRefusedException ex)
        {
            throw new ArgumentException(ex.Message, nameof(login));
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

    /// <summary>Refuses a fallback route id that names no route (#6036): the column has no foreign key, so the check is
    /// <see cref="NotificationRouteWriter.FallbackRefusalAsync"/>. Runs on the caller's transaction when it has one.</summary>
    private async Task EnsureFallbackRouteExistsAsync(
        NotificationRouteRow row, NpgsqlConnection? connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        if (row.FallbackRouteId is null)
        {
            return;
        }

        var ownConnection = connection is null ? await _dataSource.OpenConnectionAsync(cancellationToken) : null;
        try
        {
            if (await NotificationRouteWriter.FallbackRefusalAsync(
                    row.ToDraft(), (connection ?? ownConnection)!, transaction, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken) is { } refusal)
            {
                throw new ArgumentException(refusal, nameof(row));
            }
        }
        finally
        {
            if (ownConnection is not null)
            {
                await ownConnection.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// The write-side validation the edit dialog also runs (#6231: the rules are <see cref="NotificationRouteWriter.Validate"/>, shared with
    /// the MCP route tools), so a row the dialog accepts is a row the store accepts: a non-blank match (the V131 CHECK's twin), and at least one destination — a route with every
    /// column empty inherits everything and would read in the grid as a configured route that does nothing. Since
    /// V181 (#5750) it also refuses a route scoped to both a server and a tag, and the every-metric match
    /// (<see cref="NotificationRoute.WildcardMatch"/>) on a route with no scope: the store's two CHECKs, said in words
    /// before the statement runs.
    /// </summary>
    public static string? ValidateNotificationRoute(NotificationRouteRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        return NotificationRouteWriter.Validate(row.ToDraft());
    }

    /// <summary>Binds the agent columns in <see cref="NotificationRouteAgentColumns"/> order (#5632, #5825, #6036).</summary>
    private static void AddAgentColumns(NpgsqlCommand command, NotificationRouteRow row) =>
        NotificationRouteWriter.AddAgentColumns(command, row.ToDraft());

    private static void ValidateRoute(NotificationRouteRow row)
    {
        var error = ValidateNotificationRoute(row);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(row));
        }
    }

    private static NotificationRouteRow ReadRouteRow(NpgsqlDataReader reader, bool hasSeverities, bool hasReportNotice) => new()
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
        FallbackRouteId = reader.IsDBNull(18) ? null : reader.GetInt32(18),
        Severities = hasSeverities ? NotificationRouteKinds.NormalizeSeverities(reader.GetString(19)) : NotificationRouteKinds.SeveritiesBoth,
        ReportNotice = hasReportNotice ? NotificationRouteKinds.NormalizeReportNotice(reader.GetString(20)) : NotificationRouteKinds.ReportNoticeOff,
    };

    /// <summary>The secret-free reader: the four carved destinations stay empty (the seat cannot author them
    /// anyway) and every later column shifts ordinal, the <c>ReadNotificationRowNoSecret</c> idiom.</summary>
    private static NotificationRouteRow ReadRouteRowNoSecret(NpgsqlDataReader reader, bool hasSeverities, bool hasReportNotice) => new()
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
        FallbackRouteId = reader.IsDBNull(14) ? null : reader.GetInt32(14),
        Severities = hasSeverities ? NotificationRouteKinds.NormalizeSeverities(reader.GetString(15)) : NotificationRouteKinds.SeveritiesBoth,
        ReportNotice = hasReportNotice ? NotificationRouteKinds.NormalizeReportNotice(reader.GetString(16)) : NotificationRouteKinds.ReportNoticeOff,
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

    /// <summary>The severity filter (V229, #6928): <see cref="NotificationRouteKinds.SeveritiesBoth"/>, <see cref="NotificationRouteKinds.SeveritiesCritical"/>
    /// or <see cref="NotificationRouteKinds.SeveritiesWarning"/>. A route the filter excludes an alert from is skipped and matching goes on.</summary>
    public string Severities { get; set; } = NotificationRouteKinds.SeveritiesBoth;

    /// <summary>The Severity column: Both, Critical only or Warning only (#6928).</summary>
    public string SeveritiesDisplay => NotificationRouteKinds.NormalizeSeverities(Severities) switch
    {
        NotificationRouteKinds.SeveritiesCritical => "Critical only",
        NotificationRouteKinds.SeveritiesWarning => "Warning only",
        _ => "Both",
    };

    /// <summary>The report notice setting (V233, #6979): <see cref="NotificationRouteKinds.ReportNoticeOff"/>, <see cref="NotificationRouteKinds.ReportNoticeNeedsPersonOrChange"/>
    /// or <see cref="NotificationRouteKinds.ReportNoticeAll"/>.</summary>
    public string ReportNotice { get; set; } = NotificationRouteKinds.ReportNoticeOff;

    /// <summary>The Report notice column: Off, Needs a person or made a change, or All reports (#6979).</summary>
    public string ReportNoticeDisplay => NotificationRouteKinds.NormalizeReportNotice(ReportNotice) switch
    {
        NotificationRouteKinds.ReportNoticeNeedsPersonOrChange => "Needs a person or made a change",
        NotificationRouteKinds.ReportNoticeAll => "All reports",
        _ => "Off",
    };

    /// <summary>The route an expired investigation request goes to first (#6036, D8 of #5632 Phase C), or null for none.
    /// Never the route itself; the delete clears it when that route is deleted.</summary>
    public int? FallbackRouteId { get; set; }

    /// <summary>The fallback route's name (its match), filled by the read; display only.</summary>
    public string? FallbackRouteName { get; set; }

    /// <summary>The grid's Fallback cell: the fallback route's number and match, or blank for none.</summary>
    public string FallbackDisplay => FallbackRouteId is not { } id ? "" : $"#{id} {FallbackRouteName ?? "(missing)"}";

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
                channels.Add(level is NotificationRoute.AgentLevelInvestigate or NotificationRoute.AgentLevelAct or NotificationRoute.AgentLevelServerAct ? "agent" : "agent off");
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
        NotificationRoute.AgentLevelServerAct => string.Create(System.Globalization.CultureInfo.InvariantCulture, $"Act on servers ({AgentMaxPerHour}/h, {AgentMaxPerDay}/day)"),
        _ => "Off",
    };

    /// <summary>
    /// When the route was last saved (#4766, #6457). <see cref="ModifiedAtUtc"/> is the store's <c>modified_at</c>,
    /// written as <c>now() AT TIME ZONE 'UTC'</c>, so it is one UTC instant. A route belongs to the whole fleet and no
    /// one server, so "Server" display mode has no clock to apply: it used to apply the active server's offset and
    /// showed a route saved at 10:07 UTC as 03:07. It reads as every fleet-wide window does
    /// (<see cref="ViewerTimeHelper.FormatFleetWide(DateTime, string)"/>): this machine's time in Local mode, UTC in
    /// the others, always followed by the zone's name. Empty for a route that has not been stored yet.
    /// </summary>
    public string ModifiedDisplay => ModifiedAtUtc == default ? "" : ViewerTimeHelper.FormatFleetWide(ModifiedAtUtc, "yyyy-MM-dd HH:mm:ss");

    /// <summary>The row as the shared writer states it (#6231).</summary>
    public NotificationRouteDraft ToDraft() => new()
    {
        RouteId = RouteId,
        MetricMatch = MetricMatch,
        TeamsUrl = TeamsUrl,
        SlackUrl = SlackUrl,
        GenericUrl = GenericUrl,
        PagerDutyRoutingKey = PagerDutyRoutingKey,
        SmtpRecipients = SmtpRecipients,
        Enabled = Enabled,
        ScopeServerId = ScopeServerId,
        ScopeTagId = ScopeTagId,
        AgentLevel = AgentLevel,
        AgentMaxPerHour = AgentMaxPerHour,
        AgentMaxPerDay = AgentMaxPerDay,
        AgentLeaseMinutes = AgentLeaseMinutes,
        AgentExpiryMinutes = AgentExpiryMinutes,
        SendResolutions = SendResolutions,
        FallbackRouteId = FallbackRouteId,
        Severities = Severities,
        ReportNotice = ReportNotice,
        ModifiedAtUtc = ModifiedAtUtc == default ? null : ModifiedAtUtc,
    };

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
        FallbackRouteId = FallbackRouteId,
        FallbackRouteName = FallbackRouteName,
        Severities = Severities,
        ReportNotice = ReportNotice,
        ScopeDisplay = ScopeDisplay,
        StatusDisplay = StatusDisplay,
    };
}
