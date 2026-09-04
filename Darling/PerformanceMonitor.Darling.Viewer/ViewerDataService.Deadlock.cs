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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using static PerformanceMonitor.Common.DeadlockGraphProcessParser;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's deadlock grid row (W1e). The alert-consumed members (victim process/SQL, graph XML)
/// live on the shared <see cref="DeadlockAlertRow"/> base — the same shape Lite's <c>DeadlockRow</c>
/// derives from — so the store reads flow into the shared surfaces without a mapping copy; only the
/// grid-fetch extras stay here. Mirror of Lite's <c>DeadlockRow</c>.
/// </summary>
public sealed class ViewerDeadlockRow : DeadlockAlertRow
{
    public DateTime CollectionTime { get; set; }
    public DateTime? DeadlockTime { get; set; }

    /// <summary>
    /// The deadlock's BEST-EFFORT victim plan (deadlocks.victim_query_plan_xml, #1368 / V7 — resolved at
    /// collection time from the victim's sql_handle, only when the host sets CapturePlanXml; often NULL,
    /// always NULL under Lite). Threaded onto every <see cref="DeadlockProcessDetail"/> parsed from this row.
    /// </summary>
    public string? VictimQueryPlanXml { get; set; }
}

/// <summary>
/// One parsed process inside a deadlock graph — the Deadlocks grid binds a
/// <c>List&lt;DeadlockProcessDetail&gt;</c> (one row per process, parsed from each
/// <see cref="ViewerDeadlockRow"/>'s graph XML). The raw parsed fields and the sp_BlitzLock-style graph
/// walk now live once on the shared <see cref="DeadlockProcessInfo"/> / <see cref="DeadlockGraphProcessParser"/>
/// (Common), de-duplicated with Lite; the viewer adds only its machine-local display getters
/// (<see cref="ViewerTimeHelper.ForDisplay"/>, where Lite uses its per-server <c>ServerTimeHelper</c>) and the
/// per-row "View Victim Plan" gate. The walk is CPU-bound XML work, so callers run
/// <see cref="ParseFromRows"/> off the UI thread.
/// </summary>
public sealed class DeadlockProcessDetail : DeadlockProcessInfo
{
    public bool HasVictimQueryPlan => !string.IsNullOrEmpty(VictimQueryPlanXml);

    /// <summary>
    /// Whether "View Victim Plan" should be active for THIS row. The victim plan is a single per-deadlock plan
    /// copied onto every process row, so gating only on <see cref="HasVictimQueryPlan"/> lit it up on the
    /// deadlocker rows too (where it reads as "this process's plan", which it isn't). Requires the row to be the
    /// victim AND to carry a plan, so the item is active only on the victim row.
    /// </summary>
    public bool CanViewVictimPlan => IsVictim && HasVictimQueryPlan;

    public string DeadlockTimeLocal
        => DeadlockTime is { } t ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss") : "";
    public string VictimDisplay => IsVictim ? "Victim" : "";
    public string WaitTimeFormatted => WaitTime > 0 ? $"{WaitTime:N0} ms" : "";
    public string LastTranStartedLocal
        => LastTranStarted is { } t ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss") : "";

    /// <summary>
    /// Parses a list of <see cref="ViewerDeadlockRow"/> into per-process detail rows via the shared
    /// <see cref="DeadlockGraphProcessParser"/> (the sp_BlitzLock-style graph walk, de-duplicated with Lite).
    /// Each row's best-effort victim plan (#1368 / V7) is threaded onto every process it produces.
    /// </summary>
    public static List<DeadlockProcessDetail> ParseFromRows(List<ViewerDeadlockRow> rows)
        => DeadlockGraphProcessParser.Parse<DeadlockProcessDetail>(
            rows.Select(r => new DeadlockGraphInput(r.DeadlockGraphXml, r.DeadlockTime, r.VictimSqlText, r.VictimQueryPlanXml))).ToList();
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Recent deadlock events for one server — Lite's <c>GetRecentDeadlocksAsync</c> ported to Postgres.
    /// Windows on the collection prefix, orders newest deadlock first, caps at 50 (Lite's cap). Carries the
    /// graph XML so <see cref="DeadlockProcessDetail.ParseFromRows"/> and the deadlock-graph viewer work
    /// without a second fetch, and the BEST-EFFORT victim plan (#1368 / V7) the grid's "View Victim Plan"
    /// item opens.
    ///
    /// <para>Reads the BASE <c>deadlocks</c> table, not <c>v_deadlocks</c> (deliberate divergence from the
    /// other deadlock reads' v_-view twinning): the V7 <c>victim_query_plan_xml</c> column is projected
    /// reliably only by the base table. Postgres pins <c>SELECT *</c> in a view to the columns present at
    /// view-creation (V4, before V7), and V8 only <c>ALTER VIEW … SET SCHEMA</c>s it (never re-creates it),
    /// so an upgraded store's view would not expose the plan column and this read would fail. The column is
    /// Darling-only (Lite's DuckDB view has none), so this read was never twinnable with Lite once it
    /// carries the plan.</para>
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string RecentDeadlocksSql = """
        SELECT
            collection_time,
            deadlock_time,
            victim_process_id,
            victim_sql_text,
            deadlock_graph_xml,
            victim_query_plan_xml
        FROM deadlocks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY deadlock_time DESC
        LIMIT 50
        """;

    /// <summary>The raw deadlock rows for the window; the grid parses them into per-process detail rows.</summary>
    public async Task<List<ViewerDeadlockRow>> GetRecentDeadlocksAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerDeadlockRow>();

        await using var command = _dataSource.CreateCommand(RecentDeadlocksSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddBlockingParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerDeadlockRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeadlockTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                VictimProcessId = reader.IsDBNull(2) ? "" : reader.GetString(2),
                VictimSqlText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                DeadlockGraphXml = reader.IsDBNull(4) ? "" : reader.GetString(4),
                VictimQueryPlanXml = reader.IsDBNull(5) ? null : reader.GetString(5),
            });
        }

        return rows;
    }
}
