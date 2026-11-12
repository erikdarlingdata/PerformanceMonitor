/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL form of a mute rule's recurring schedule (#7040), for every reader of <c>config.config_mute_rules</c> that decides
/// "is this rule in force now" in SQL instead of through <c>MuteRule.IsInEffectAt</c>. It is the same test as
/// <c>MuteRecurrence.IsInWindowAt</c>, including a zone the reader cannot resolve, which is outside the window and never an error
/// (see <see cref="ZoneKnown"/>): convert now to the rule's zone (PostgreSQL's <c>AT TIME ZONE</c> takes the IANA id the store keeps),
/// then a same-day window needs a listed day and start &lt;= time &lt; end, and a window that crosses midnight belongs to the day it starts
/// (listed today and time &gt;= start, or listed yesterday and time &lt; end). Days are the <c>recur_days</c> bit mask, bit n = day of week n
/// with Sunday = 0, which is what <c>EXTRACT(DOW ...)</c> returns; a NULL mask is every day.
/// </summary>
public static class MuteRuleRecurrenceSql
{
    /// <summary>
    /// A boolean SQL expression, true when <paramref name="zoneExpression"/> (a text expression) names a time zone this PostgreSQL can
    /// convert to, so <c>AT TIME ZONE</c> on it cannot raise <c>time zone not recognized</c>. Case-insensitive like <c>AT TIME ZONE</c>.
    /// NULL for a NULL zone, which a caller reads as "not known".
    ///
    /// <para>It parses the name as the zone of a <c>timestamptz</c> literal with <c>pg_input_is_valid</c> (PostgreSQL 16 and later, the
    /// store is 17 or 18), which resolves it through the same tzdata <c>AT TIME ZONE</c> uses, in microseconds. The shape test keeps a
    /// blank or a name with a space from reading as a zone. The set <c>pg_timezone_names</c> was measured and rejected: it reads every
    /// zone file each time it is evaluated (about half a second on the Windows store), and a hashed or CTE copy of it is rebuilt for
    /// every server row of the fleet statement.</para>
    /// </summary>
    public static string ZoneKnown(string zoneExpression) =>
        $"(({zoneExpression}) <> '' AND ({zoneExpression}) NOT LIKE '% %' AND pg_input_is_valid('2000-01-01 00:00 ' || ({zoneExpression}), 'timestamptz'))";

    /// <summary>A one-row query, <c>$1</c> a zone name, answering whether the store's PostgreSQL knows it (<see cref="ZoneKnown"/>).
    /// What every store write runs on a rule's zone, so a write refuses what the fleet statement would read as outside the window.</summary>
    public static readonly string ZoneKnownSql = $"SELECT COALESCE({ZoneKnown("$1::text")}, false)";

    /// <summary>The refusal text for a zone the store's PostgreSQL does not know.</summary>
    public static string UnknownZoneMessage(string zone) =>
        $"'recur_time_zone' is not a time zone the store's PostgreSQL knows (got '{zone}'). Use an IANA id such as America/New_York or UTC. Nothing was written.";

    /// <summary>
    /// Runs <see cref="ZoneKnownSql"/> on <paramref name="command"/> (a command on the connection, and transaction, the write will use)
    /// and returns the refusal text when the store does not know <paramref name="zone"/>, or null when it does or there is no zone.
    /// </summary>
    public static async Task<string?> RefusalAsync(NpgsqlCommand command, string? zone, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(zone)) return null;
        command.CommandText = ZoneKnownSql;
        command.Parameters.Clear();
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = zone });
        var known = await command.ExecuteScalarAsync(cancellationToken);
        return known is true ? null : UnknownZoneMessage(zone);
    }

    /// <summary>
    /// A boolean SQL expression, true when the rule aliased <paramref name="alias"/> has no schedule or is inside a window at
    /// <paramref name="nowExpression"/> (a <c>timestamptz</c> expression). AND it with the rule's start and expiry tests.
    /// A zone the store does not know (a newer IANA zone than the store's tzdata, a Windows id typed straight into the table) is
    /// outside the window, as <c>MuteRecurrence.IsInWindowAt</c> reads a zone its host cannot resolve; without the guard one such row
    /// would raise and fail the whole statement it sits in. CASE evaluates its branches in order, so the conversion is never reached
    /// for such a row.
    /// </summary>
    public static string InWindow(string alias = "m", string nowExpression = "CURRENT_TIMESTAMP")
    {
        var local = $"({nowExpression} AT TIME ZONE {alias}.recur_time_zone)";
        var dow = $"EXTRACT(DOW FROM {local})::int";
        var yesterday = $"((EXTRACT(DOW FROM {local})::int + 6) % 7)";
        var t = $"{local}::time";
        string Listed(string day) => $"({alias}.recur_days IS NULL OR ({alias}.recur_days::int & (1 << {day})) <> 0)";
        return $@"({alias}.recur_start_time IS NULL OR {alias}.recur_end_time IS NULL OR
    CASE WHEN NOT COALESCE({ZoneKnown($"{alias}.recur_time_zone")}, false)
         THEN false
         WHEN {alias}.recur_start_time < {alias}.recur_end_time
         THEN {Listed(dow)} AND {t} >= {alias}.recur_start_time AND {t} < {alias}.recur_end_time
         ELSE ({Listed(dow)} AND {t} >= {alias}.recur_start_time) OR ({Listed(yesterday)} AND {t} < {alias}.recur_end_time)
    END)";
    }
}
