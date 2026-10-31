/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL form of a mute rule's recurring schedule (#7040), for every reader of <c>config.config_mute_rules</c> that decides
/// "is this rule in force now" in SQL instead of through <c>MuteRule.IsInEffectAt</c>. It is the same test as
/// <c>MuteRecurrence.IsInWindowAt</c>: convert now to the rule's zone (PostgreSQL's <c>AT TIME ZONE</c> takes the IANA id the store keeps),
/// then a same-day window needs a listed day and start &lt;= time &lt; end, and a window that crosses midnight belongs to the day it starts
/// (listed today and time &gt;= start, or listed yesterday and time &lt; end). Days are the <c>recur_days</c> bit mask, bit n = day of week n
/// with Sunday = 0, which is what <c>EXTRACT(DOW ...)</c> returns; a NULL mask is every day.
/// </summary>
public static class MuteRuleRecurrenceSql
{
    /// <summary>
    /// A boolean SQL expression, true when the rule aliased <paramref name="alias"/> has no schedule or is inside a window at
    /// <paramref name="nowExpression"/> (a <c>timestamptz</c> expression). AND it with the rule's start and expiry tests.
    /// </summary>
    public static string InWindow(string alias = "m", string nowExpression = "CURRENT_TIMESTAMP")
    {
        var local = $"({nowExpression} AT TIME ZONE {alias}.recur_time_zone)";
        var dow = $"EXTRACT(DOW FROM {local})::int";
        var yesterday = $"((EXTRACT(DOW FROM {local})::int + 6) % 7)";
        var t = $"{local}::time";
        string Listed(string day) => $"({alias}.recur_days IS NULL OR ({alias}.recur_days::int & (1 << {day})) <> 0)";
        return $@"({alias}.recur_start_time IS NULL OR {alias}.recur_end_time IS NULL OR
    CASE WHEN {alias}.recur_start_time < {alias}.recur_end_time
         THEN {Listed(dow)} AND {t} >= {alias}.recur_start_time AND {t} < {alias}.recur_end_time
         ELSE ({Listed(dow)} AND {t} >= {alias}.recur_start_time) OR ({Listed(yesterday)} AND {t} < {alias}.recur_end_time)
    END)";
    }
}
