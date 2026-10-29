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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One SQL Server Agent alert in the newest capture inside the window (#6874). <see cref="FiredInWindow"/> is the change in
/// <c>occurrence_count</c> between the window's oldest and newest capture: null when the window holds only one capture, because one
/// count is not a change.
/// </summary>
public sealed record AgentAlertFact(
    int AlertId,
    string? Name,
    bool? Enabled,
    string? EventSource,
    int? MessageId,
    int? Severity,
    string? DatabaseName,
    string? EventDescriptionKeyword,
    string? PerformanceCondition,
    int? DelayBetweenResponses,
    DateTime? LastOccurrence,
    DateTime? LastResponse,
    int? OccurrenceCount,
    DateTime? CountReset,
    int? HasNotification,
    int? OperatorCount,
    string? ResponseJobId,
    bool? FiredInWindow)
{
    /// <summary>
    /// The alert tells no operator: <c>sysnotifications</c> names none, or the agent's own <c>has_notification</c> bitmask is zero.
    /// Unknown (a null count) is not "no operator".
    /// </summary>
    public bool NotifiesNoOne => OperatorCount is 0 || HasNotification is 0;
}

/// <summary>What <see cref="DarlingAgentAlertReader.ReadAsync"/> found.</summary>
/// <param name="Alerts">One entry per alert in the newest capture, by alert id. Empty for the all-NULL marker row (collected, none defined).</param>
/// <param name="NewestCapture">The newest capture in the window, or null when the window holds none.</param>
/// <param name="OldestCapture">The oldest capture in the window.</param>
/// <param name="Captures">How many captures the window holds.</param>
public sealed record AgentAlertRead(
    IReadOnlyList<AgentAlertFact> Alerts,
    DateTime? NewestCapture,
    DateTime? OldestCapture,
    int Captures)
{
    /// <summary>The window holds at least one capture (a capture of an instance with no alerts still counts).</summary>
    public bool HasCapture => NewestCapture is not null;
}

/// <summary>One thing the instance's alerts lack. <see cref="Code"/> is a stable word, <see cref="Message"/> the sentence for a person.</summary>
/// <param name="Code">One of <see cref="AgentAlertStatus"/>'s code constants.</param>
/// <param name="Message">What is missing, in plain words.</param>
/// <param name="Alerts">The alert names the gap is about (empty for a missing alert).</param>
/// <param name="Missing">The severities or error numbers that have no alert (empty for the alert-level gaps).</param>
public sealed record AgentAlertGap(string Code, string Message, IReadOnlyList<string> Alerts, IReadOnlyList<int> Missing);

/// <summary>The instance-level answer: <see cref="Word"/> is <c>ok</c> with no gap, <c>gaps</c> otherwise.</summary>
public sealed record AgentAlertInstanceStatus(string Word, IReadOnlyList<AgentAlertGap> Gaps);

/// <summary>
/// #6874: the pure instance status over one capture's alerts. No I/O, so the web page and the Viewer can call it with the same facts
/// and get the same words. Coverage is by alert existence: a severity or error with a DISABLED alert is reported as disabled, not as
/// missing, so one alert is never counted as two gaps.
/// </summary>
public static class AgentAlertStatus
{
    /// <summary>Status word when every check passes.</summary>
    public const string Ok = "ok";

    /// <summary>Status word when at least one gap is found.</summary>
    public const string Gaps = "gaps";

    /// <summary>The instance has no alert at all (the collector's all-NULL marker row).</summary>
    public const string NoAlertsDefined = "no_alerts_defined";

    /// <summary>Some of severities 19 to 25 have no alert.</summary>
    public const string MissingSeverityAlerts = "missing_severity_alerts";

    /// <summary>Some of errors 823, 824 and 825 have no alert.</summary>
    public const string MissingErrorAlerts = "missing_error_alerts";

    /// <summary>An enabled alert that tells no operator.</summary>
    public const string AlertWithoutOperator = "alert_without_operator";

    /// <summary>An alert that is switched off.</summary>
    public const string AlertDisabled = "alert_disabled";

    /// <summary>An alert whose count rose in the window and that tells no operator.</summary>
    public const string FiredUnnotified = "fired_unnotified";

    /// <summary>The severities every instance should alert on (the fatal and hardware-error range).</summary>
    public static readonly IReadOnlyList<int> RequiredSeverities = [19, 20, 21, 22, 23, 24, 25];

    /// <summary>The errors every instance should alert on (823 and 824 are read and checksum failures, 825 is a read-retry).</summary>
    public static readonly IReadOnlyList<int> RequiredErrors = [823, 824, 825];

    /// <summary>The gaps in one capture's alerts. <paramref name="alerts"/> is empty for the marker row: no alert defined is itself a gap.</summary>
    public static AgentAlertInstanceStatus Evaluate(IReadOnlyList<AgentAlertFact> alerts)
    {
        ArgumentNullException.ThrowIfNull(alerts);
        var gaps = new List<AgentAlertGap>();

        if (alerts.Count == 0)
            gaps.Add(new AgentAlertGap(NoAlertsDefined, "No SQL Server Agent alert is defined on this instance.", [], []));

        var missingSeverities = RequiredSeverities.Where(s => !alerts.Any(a => a.Severity == s)).ToList();
        if (missingSeverities.Count > 0)
            gaps.Add(new AgentAlertGap(MissingSeverityAlerts,
                $"No alert for severity {Join(missingSeverities)}.", [], missingSeverities));

        var missingErrors = RequiredErrors.Where(e => !alerts.Any(a => a.MessageId == e)).ToList();
        if (missingErrors.Count > 0)
            gaps.Add(new AgentAlertGap(MissingErrorAlerts,
                $"No alert for error {Join(missingErrors)}.", [], missingErrors));

        var noOperator = alerts.Where(a => a.Enabled != false && a.NotifiesNoOne).Select(Label).ToList();
        if (noOperator.Count > 0)
            gaps.Add(new AgentAlertGap(AlertWithoutOperator,
                "An enabled alert tells no operator, so nobody hears when it fires.", noOperator, []));

        var disabled = alerts.Where(a => a.Enabled == false).Select(Label).ToList();
        if (disabled.Count > 0)
            gaps.Add(new AgentAlertGap(AlertDisabled, "An alert is disabled, so it cannot fire.", disabled, []));

        var unnotified = alerts.Where(a => a.FiredInWindow == true && a.NotifiesNoOne).Select(Label).ToList();
        if (unnotified.Count > 0)
            gaps.Add(new AgentAlertGap(FiredUnnotified,
                "An alert fired in the window and told no operator.", unnotified, []));

        return new AgentAlertInstanceStatus(gaps.Count == 0 ? Ok : Gaps, gaps);
    }

    /// <summary>
    /// Whether an alert fired between a capture's count and a later one. A count that fell is a reset (<c>count_reset</c>), and the
    /// alert fired in the window if it has counted anything since: a count above zero after the reset. A new alert starts from zero.
    /// </summary>
    public static bool? Fired(int? oldestCount, int? newestCount, bool hasTwoCaptures)
    {
        if (!hasTwoCaptures || newestCount is not int newest)
            return null;
        var oldest = oldestCount ?? 0;
        return newest >= oldest ? newest > oldest : newest > 0;
    }

    private static string Label(AgentAlertFact alert) => alert.Name ?? $"alert {alert.AlertId}";

    private static string Join(IReadOnlyList<int> values) => string.Join(", ", values);
}

/// <summary>
/// #6874: the SQL Server Agent alerts in place, from <c>collect.agent_alerts</c> (V222): one row per alert per pass, or one all-NULL
/// marker row when the instance has none. The newest capture in the window gives the alerts; the oldest gives the starting
/// <c>occurrence_count</c> for "fired in the window". Both reads are bounded by <c>server_id</c> and the window on
/// <c>collection_time</c> (<c>idx_agent_alerts_time</c>).
/// </summary>
public static class DarlingAgentAlertReader
{
    /// <summary>How old the newest capture may be and still count for the alert and the fleet card chip (#6874): eight passes of the 15-minute collector.</summary>
    public static readonly TimeSpan FreshCaptureWindow = TimeSpan.FromHours(2);

    /// <summary>
    /// The newest capture's rows, each beside the same alert's count in the oldest capture, and the capture bounds. One pass stamps
    /// one <c>collection_time</c>, so the joins are on equality. $1 server, $2 start, $3 end.
    /// </summary>
    public const string AlertsSql = @"
WITH win AS (
    SELECT collection_time, alert_id, alert_name, enabled, event_source, message_id, severity, database_name,
           event_description_keyword, performance_condition, delay_between_responses, last_occurrence, last_response,
           occurrence_count, count_reset, has_notification, operator_count, response_job_id
    FROM collect.agent_alerts
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3),
bounds AS (
    SELECT MIN(collection_time) AS oldest, MAX(collection_time) AS newest, COUNT(DISTINCT collection_time) AS captures FROM win)
SELECT n.collection_time, b.oldest, b.captures, n.alert_id, n.alert_name, n.enabled, n.event_source, n.message_id, n.severity,
       n.database_name, n.event_description_keyword, n.performance_condition, n.delay_between_responses, n.last_occurrence,
       n.last_response, n.occurrence_count, n.count_reset, n.has_notification, n.operator_count, n.response_job_id,
       o.occurrence_count
FROM bounds AS b
JOIN win AS n ON n.collection_time = b.newest
LEFT JOIN win AS o ON o.collection_time = b.oldest AND o.alert_id = n.alert_id
ORDER BY n.alert_id NULLS LAST";

    /// <summary>Reads the alerts of one server in the window <paramref name="start"/> to <paramref name="end"/> (UTC).</summary>
    public static async Task<AgentAlertRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        await using var command = postgres.CreateCommand(AlertsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));

        DateTime? newest = null;
        DateTime? oldest = null;
        var captures = 0;
        var alerts = new List<AgentAlertFact>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            newest = reader.GetDateTime(0);
            oldest = reader.GetDateTime(1);
            captures = checked((int)reader.GetInt64(2));
            if (reader.IsDBNull(3))
                continue; // the marker row: a capture with no alert
            var hasTwo = captures >= 2;
            int? OptInt(int i) => reader.IsDBNull(i) ? null : reader.GetInt32(i);
            string? OptText(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
            DateTime? OptTime(int i) => reader.IsDBNull(i) ? null : reader.GetDateTime(i);
            var count = OptInt(15);
            alerts.Add(new AgentAlertFact(
                reader.GetInt32(3), OptText(4), reader.IsDBNull(5) ? null : reader.GetBoolean(5), OptText(6), OptInt(7), OptInt(8),
                OptText(9), OptText(10), OptText(11), OptInt(12), OptTime(13), OptTime(14), count, OptTime(16),
                OptInt(17), OptInt(18), OptText(19), AgentAlertStatus.Fired(OptInt(20), count, hasTwo)));
        }

        return new AgentAlertRead(alerts, newest, oldest, captures);
    }
}
