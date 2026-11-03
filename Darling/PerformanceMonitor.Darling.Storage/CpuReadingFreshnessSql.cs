/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The lower bound on <c>collection_time</c> for a SQL Server's displayed CPU reading (#7196), shared by the fleet
/// card, the Overview card and <c>get_server_summary</c> so the three agree on what "current" means.
///
/// <para>A reading is current when it is newer than the larger of <see cref="DarlingPgCpuUtilizationReader.Freshness"/>
/// and twice the server's own <c>cpu_utilization</c> interval, so a collector an operator scheduled every 30 minutes
/// is not read as stopped between samples. The interval is the one the scheduler runs by: the server's own row in
/// <c>config_collector_schedules</c>, else the fleet-wide row, else the shipped default. The table is sparse, so the
/// two lookups are keyed scalar subqueries against a handful of rows rather than a join that could multiply rows.</para>
/// </summary>
public static class CpuReadingFreshnessSql
{
    /// <summary>The collector the SQL Server CPU reading comes from.</summary>
    private const string CollectorName = "cpu_utilization";

    /// <summary>The freshness floor in whole minutes, bound by the caller as an integer.</summary>
    public static int FloorMinutes => (int)DarlingPgCpuUtilizationReader.Freshness.TotalMinutes;

    /// <summary>
    /// A SQL expression for the cutoff instant (naive UTC, a <c>timestamp</c>).
    /// </summary>
    /// <param name="serverIdExpression">The server's id as SQL: <c>s.server_id</c> or a parameter.</param>
    /// <param name="nowParameter">The parameter holding now as naive UTC, for example <c>$2</c>.</param>
    /// <param name="floorMinutesParameter">The parameter holding <see cref="FloorMinutes"/>, for example <c>$3</c>.</param>
    public static string Cutoff(string serverIdExpression, string nowParameter, string floorMinutesParameter) =>
        $@"({nowParameter} - GREATEST({floorMinutesParameter}, 2 * COALESCE(
        (SELECT frequency_minutes FROM config.config_collector_schedules
         WHERE collector_name = '{CollectorName}' AND server_id = {serverIdExpression}),
        (SELECT frequency_minutes FROM config.config_collector_schedules
         WHERE collector_name = '{CollectorName}' AND server_id IS NULL),
        {CollectorScheduleDefaults.ResolveFrequencyMinutes(CollectorName, null, null)})) * INTERVAL '1 minute')";
}
