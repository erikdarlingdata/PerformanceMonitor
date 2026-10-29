/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// SQL Server Agent alert definitions and their counters (#6874, part of #6872): one row per alert in
/// <c>msdb.dbo.sysalerts</c>, with the number of operators that alert notifies (<c>msdb.dbo.sysnotifications</c>),
/// every 15 minutes. Each pass stores the whole picture, so two passes show how <c>occurrence_count</c> moved
/// between them: an alert that fired in a window is one whose count rose, and the alert-setup status reads it that way.
///
/// <para><b>An empty <c>sysalerts</c> still writes one row.</b> The reader's main finding is an instance that has no
/// alerts for severities 19 to 25 or errors 823 to 825, and an instance with no alerts at all is the clearest case of
/// it. A pass that wrote nothing would look the same as a pass that never ran, so when no alert exists the query returns
/// one marker row whose <c>alert_id</c> is NULL (a LEFT JOIN from a one-row source). Every other row has an
/// <c>alert_id</c>.</para>
///
/// <para><b>Where it runs.</b> Agent alerts exist only on SQL Server. <see cref="AppliesTo"/> starts from the gate
/// <see cref="AgentStatusCollector"/> uses (no Azure SQL Database, no Amazon RDS) and also leaves out Azure SQL Managed
/// Instance: <c>sysalerts</c> is there but empty, and <c>sp_add_alert</c> fails with "SQL Server Agent feature alerting is
/// not supported in SQL Database Managed Instance" (measured on a test instance, 10-28). All three answer
/// <c>not_collected</c> through the engine-capability path, which sweeps this gate over every target shape
/// (<c>IsAzureManagedInstance</c> is already one of the swept facts).</para>
///
/// <para><b>Cost on the target.</b> Tens of rows from msdb, read uncommitted with a one-second lock timeout (a 1222
/// is a yield, not a failure) and a short command timeout. Nothing is written. Darling only (<see cref="IsDarlingOnly"/>):
/// Lite has no twin.</para>
/// </summary>
public sealed class AgentAlertsCollector : CollectorDefinitionBase<AgentAlertsCollector.Row>
{
    public static AgentAlertsCollector Instance { get; } = new();

    private AgentAlertsCollector()
    {
    }

    /// <summary>One alert, or the marker row (<see cref="AlertId"/> NULL) when the instance defines none.</summary>
    public readonly record struct Row(
        int? AlertId,
        string? AlertName,
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
        string? ResponseJobId);

    /// <summary>The per-command deadline: tens of rows of msdb metadata, so this is a backstop.</summary>
    public const int CommandTimeoutSeconds = 30;

    /* The agent_datetime decode is AgentStatusCollector's: msdb stores a date as yyyymmdd and a time as hhmmss in two
       ints, and 0 means "never". The CASE keeps a 0 date from reaching the function. The LEFT JOIN onto a one-row source
       returns one all-NULL row when sysalerts is empty (see the class remarks). The operator count is a correlated
       subquery so an alert with no sysnotifications rows reads 0, not a lost row. The response job is NULL for the
       all-zero GUID msdb stores when no job runs. */
    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000;

SELECT /* PerformanceMonitorLite */
    alert_id = a.id,
    alert_name = a.name,
    enabled = a.enabled,
    event_source = a.event_source,
    message_id = a.message_id,
    severity = a.severity,
    database_name = a.database_name,
    event_description_keyword = a.event_description_keyword,
    performance_condition = a.performance_condition,
    delay_between_responses = a.delay_between_responses,
    last_occurrence =
        CASE
            WHEN a.last_occurrence_date > 0
            THEN msdb.dbo.agent_datetime(a.last_occurrence_date, a.last_occurrence_time)
        END,
    last_response =
        CASE
            WHEN a.last_response_date > 0
            THEN msdb.dbo.agent_datetime(a.last_response_date, a.last_response_time)
        END,
    occurrence_count = a.occurrence_count,
    count_reset =
        CASE
            WHEN a.count_reset_date > 0
            THEN msdb.dbo.agent_datetime(a.count_reset_date, a.count_reset_time)
        END,
    has_notification = a.has_notification,
    operator_count =
        CASE
            WHEN a.id IS NULL
            THEN NULL
            ELSE
            (
                SELECT COUNT_BIG(DISTINCT sn.operator_id)
                FROM msdb.dbo.sysnotifications AS sn
                WHERE sn.alert_id = a.id
            )
        END,
    response_job_id =
        CASE
            WHEN a.job_id = CONVERT(uniqueidentifier, '00000000-0000-0000-0000-000000000000')
            THEN NULL
            ELSE CONVERT(nvarchar(36), a.job_id)
        END
FROM (SELECT one = 1) AS anchor
LEFT JOIN msdb.dbo.sysalerts AS a
  ON 1 = 1
ORDER BY
    a.id
OPTION(RECOMPILE);";

    public override string Name => "agent_alerts";

    public override string TargetTable => "agent_alerts";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>The query sets a one-second lock timeout, so a 1222 is a yield, not a failure.</summary>
    public override bool YieldsOnLockTimeout => true;

    /// <summary>A plain table with a time-sliced DELETE, like the forced-plans and hints tables: tens of rows per pass.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>
    /// SQL Server only: not Azure SQL Database (no Agent), not Amazon RDS (no Agent alerts), and not Azure SQL Managed
    /// Instance (<c>sp_add_alert</c> is refused, so <c>sysalerts</c> is always empty). Starts from
    /// <see cref="AgentStatusCollector.AppliesTo"/> so the two Agent collectors keep one definition of the first two.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        AgentStatusCollector.Instance.AppliesTo(target) && !target.IsAzureManagedInstance;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("alert_id", CollectorColumnType.Integer),
        new CollectorColumn("alert_name", CollectorColumnType.Varchar),
        new CollectorColumn("enabled", CollectorColumnType.Boolean),
        new CollectorColumn("event_source", CollectorColumnType.Varchar),
        new CollectorColumn("message_id", CollectorColumnType.Integer),
        new CollectorColumn("severity", CollectorColumnType.Integer),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("event_description_keyword", CollectorColumnType.Varchar),
        new CollectorColumn("performance_condition", CollectorColumnType.Varchar),
        new CollectorColumn("delay_between_responses", CollectorColumnType.Integer),
        new CollectorColumn("last_occurrence", CollectorColumnType.Timestamp),
        new CollectorColumn("last_response", CollectorColumnType.Timestamp),
        new CollectorColumn("occurrence_count", CollectorColumnType.Integer),
        new CollectorColumn("count_reset", CollectorColumnType.Timestamp),
        new CollectorColumn("has_notification", CollectorColumnType.Integer),
        new CollectorColumn("operator_count", CollectorColumnType.Integer),
        new CollectorColumn("response_job_id", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                NullableInt(reader, 0),
                NullableString(reader, 1),
                reader.IsDBNull(2) ? null : Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture),
                NullableString(reader, 3),
                NullableInt(reader, 4),
                NullableInt(reader, 5),
                NullableString(reader, 6),
                NullableString(reader, 7),
                NullableString(reader, 8),
                NullableInt(reader, 9),
                reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                NullableInt(reader, 12),
                reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                NullableInt(reader, 14),
                NullableInt(reader, 15),
                NullableString(reader, 16)));
        }

        return rows;
    }

    private static string? NullableString(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal).ToString();

    private static int? NullableInt(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.AlertId)                  /* alert_id INTEGER, NULL on the marker row */
            .Value(row.AlertName)                /* alert_name VARCHAR */
            .Value(row.Enabled)                  /* enabled BOOLEAN */
            .Value(row.EventSource)              /* event_source VARCHAR */
            .Value(row.MessageId)                /* message_id INTEGER */
            .Value(row.Severity)                 /* severity INTEGER */
            .Value(row.DatabaseName)             /* database_name VARCHAR */
            .Value(row.EventDescriptionKeyword)  /* event_description_keyword VARCHAR */
            .Value(row.PerformanceCondition)     /* performance_condition VARCHAR */
            .Value(row.DelayBetweenResponses)    /* delay_between_responses INTEGER, seconds */
            .Value(row.LastOccurrence)           /* last_occurrence TIMESTAMP, NULL = never */
            .Value(row.LastResponse)             /* last_response TIMESTAMP, NULL = never */
            .Value(row.OccurrenceCount)          /* occurrence_count INTEGER */
            .Value(row.CountReset)               /* count_reset TIMESTAMP, NULL = never */
            .Value(row.HasNotification)          /* has_notification INTEGER, the msdb bitmask (1 email, 2 pager, 4 net send) */
            .Value(row.OperatorCount)            /* operator_count INTEGER, operators in sysnotifications */
            .Value(row.ResponseJobId);           /* response_job_id VARCHAR, NULL = no job */
    }
}
