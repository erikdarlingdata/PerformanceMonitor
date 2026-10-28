/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>One distinct <c>(local timestamp text, zone abbreviation)</c> a log line carries, as written (#6866).</summary>
public readonly record struct PgLogZonePair(string Stamp, string Zone);

/// <summary>
/// Runs the server check for a read: the setting the target's <c>log_timezone</c> holds, the distinct stamps the read
/// found, and a connection to the target to ask on. Returns null when the setting is not a named zone (the refusal
/// stays), otherwise the map of the stamps the server proved (#6866).
/// </summary>
public delegate Task<PgLogZoneMap?> PgLogZoneResolver(
    string logTimezoneSetting, IReadOnlyCollection<PgLogZonePair> pairs, CancellationToken cancellationToken);

/// <summary>The target's <c>log_timezone</c> as written and a way to ask it, for a route that parses log text away from
/// the connection (the RDS ingestors, #6866).</summary>
public sealed record PgLogZoneCheck(string Setting, PgLogZoneResolver Resolver);

/// <summary>
/// What the target itself proved about the stamps of one read, when <c>log_timezone</c> is a named local zone (#6866).
/// A pair is in the map only when exactly ONE UTC instant renders back, on the server and in the session zone
/// <c>log_timezone</c> names, as the identical text the line carries, abbreviation included. A pair with no such
/// instant (a line written under an earlier setting, a forged line, a time in the hour a clock skipped) or with two
/// (a zone that keeps one abbreviation across a transition) is absent, and the reader skips and counts the line.
/// Never a guess.
/// </summary>
public sealed class PgLogZoneMap
{
    private readonly Dictionary<PgLogZonePair, DateTime> _resolved;

    private PgLogZoneMap(Dictionary<PgLogZonePair, DateTime> resolved) => _resolved = resolved;

    /// <summary>A map that proves nothing: every line is skipped and counted.</summary>
    public static PgLogZoneMap Empty { get; } = new(new Dictionary<PgLogZonePair, DateTime>());

    /// <summary>Number of pairs the server proved.</summary>
    public int Count => _resolved.Count;

    /// <summary>
    /// Builds the map from what the server answered for each pair: how many candidate instants rendered back as the
    /// line's text, and the earliest of them. Only <c>matches == 1</c> is kept.
    /// </summary>
    public static PgLogZoneMap FromMatches(IEnumerable<(string Stamp, string Zone, int Matches, DateTime Utc)> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var resolved = new Dictionary<PgLogZonePair, DateTime>();

        foreach (var row in rows)
        {
            if (row.Matches == 1)
            {
                resolved[new PgLogZonePair(row.Stamp, row.Zone)] = DateTime.SpecifyKind(row.Utc, DateTimeKind.Utc);
            }
        }

        return new PgLogZoneMap(resolved);
    }

    /// <summary>The proven UTC instant (Kind Utc) for a line's stamp and zone text, or false.</summary>
    public bool TryGet(string stamp, string zone, out DateTime utc) =>
        _resolved.TryGetValue(new PgLogZonePair(stamp, zone), out utc);
}

/// <summary>
/// Converts the time of a PostgreSQL log line stamped in a NAMED local zone, only where the server proves it (#6866).
///
/// <para><b>Why this is not the #2993 guess.</b> The zone in a log prefix is an abbreviation and one abbreviation
/// can name several zones, so the abbreviation alone converts nothing. But the setting <c>log_timezone</c> names ONE
/// zone, and the server that wrote the line can render any instant in it. So the check runs the other way round: the
/// candidate instants for a stamp are the local time read with each UTC offset the zone has near that time (one
/// normally, two in the repeated hour after a clock goes back, none in the hour a clock skips), each is rendered
/// BACK on the target with <c>log_timezone</c> as the session zone in the format the prefix wrote (<c>%m</c> with
/// milliseconds, <c>%t</c> without), and a candidate is accepted only when the rendered text equals the line's text
/// exactly. Exactly one match is the instant. The time database used is the one that wrote the line.</para>
///
/// <para><b>A setting read later than the line (#2993's other reason)</b> only selects the zone. Every line is
/// still checked against its OWN text, so a line written under an earlier setting renders back differently, has no
/// match, and is skipped and counted, never converted.</para>
///
/// <para><b>One round trip per read.</b> The distinct stamps go in as two arrays and the matches come out of one
/// command, never one trip per line. The session zone is set with <c>set_config(.., is_local := true)</c> inside a
/// transaction of its own, which ends with the read and leaves the pooled connection as it found it.</para>
/// </summary>
public static class PgLogNamedZone
{
    /* A stamp the prefix escapes can write: %t is `YYYY-MM-DD HH:MI:SS`, %m adds exactly three millisecond digits.
       Anything else (a forged `.1`, a month 13) is not sent: the server would cast it and fail the whole batch with
       22008, which one forged line could then repeat on every read. It is simply unresolved, so it is skipped. */
    private static readonly Regex s_sendableStamp = new(
        @"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{3})?$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The batched check. Statement one pins the session zone to the setting and returns a row only when the setting
    /// is a name in <c>pg_timezone_names</c> (a POSIX offset string such as <c>-03</c> and an unknown name return
    /// none, and the reader refuses as before). Statement two returns, per distinct pair, how many candidate instants
    /// rendered back as the line's text and the earliest of them. The candidate offsets are the zone's offsets at
    /// every whole hour from 14 hours before to 12 hours after the stamp read as UTC, which covers every instant a
    /// local time can come from, since no offset is outside -12 to +14. A candidate that is not the instant of a real
    /// transition simply fails the render check.
    /// </summary>
    public const string ResolveSql = @"
SELECT pg_catalog.set_config('TimeZone', n.name, true)
FROM pg_catalog.pg_timezone_names AS n
WHERE pg_catalog.lower(n.name) = pg_catalog.lower(@zone)
LIMIT 1;

SELECT m.stamp, m.zone, pg_catalog.count(*)::integer AS matches, pg_catalog.min(m.utc) AS utc
FROM (
    SELECT p.stamp,
           p.zone,
           (c.candidate AT TIME ZONE 'UTC') AS utc,
           pg_catalog.to_char(
               c.candidate,
               CASE WHEN p.stamp LIKE '%.%' THEN 'YYYY-MM-DD HH24:MI:SS.MS' ELSE 'YYYY-MM-DD HH24:MI:SS' END)
               || ' ' || pg_catalog.to_char(c.candidate, 'TZ') AS rendered
    FROM ROWS FROM (pg_catalog.unnest(@stamps::text[]), pg_catalog.unnest(@zones::text[])) AS p(stamp, zone)
    CROSS JOIN LATERAL (
        SELECT DISTINCT (p.stamp::timestamp AT TIME ZONE 'UTC')
               - pg_catalog.date_part('timezone', (p.stamp::timestamp AT TIME ZONE 'UTC') + h.h * interval '1 hour')::integer
                 * interval '1 second' AS candidate
        FROM pg_catalog.generate_series(-14, 12) AS h(h)
    ) AS c
) AS m
WHERE m.rendered = m.stamp || ' ' || m.zone
GROUP BY m.stamp, m.zone;";

    /// <summary>Whether a stamp is worth asking the server about: shaped as a prefix escape writes it, and far enough
    /// from <see cref="DateTime"/>'s ends that no zone offset can carry the instant out of range.</summary>
    public static bool IsAskable(string stamp)
    {
        if (!s_sendableStamp.IsMatch(stamp))
        {
            return false;
        }

        if (!DateTime.TryParseExact(
                stamp.Length > 19 ? stamp : stamp + ".000", "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var local))
        {
            return false;
        }

        return local > DateTime.MinValue.AddDays(2) && local < DateTime.MaxValue.AddDays(-2);
    }

    /// <summary>The distinct pairs of <paramref name="entries"/> (csvlog, jsonlog and RDS records), whose stamp the
    /// server can be asked about. csvlog and jsonlog carry the zone inside <see cref="PgLogEntry.TimestampText"/>.</summary>
    public static List<PgLogZonePair> PairsOf(IEnumerable<PgLogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var seen = new HashSet<PgLogZonePair>();
        var pairs = new List<PgLogZonePair>();

        foreach (var entry in entries)
        {
            var pair = PairOf(entry);

            if (seen.Add(pair))
            {
                pairs.Add(pair);
            }
        }

        return pairs;
    }

    private static PgLogZonePair PairOf(PgLogEntry entry)
    {
        var suffix = " " + entry.ZoneText;
        var stamp = entry.TimestampText.EndsWith(suffix, StringComparison.Ordinal)
            ? entry.TimestampText[..^suffix.Length]
            : entry.TimestampText;

        return new PgLogZonePair(stamp, entry.ZoneText);
    }

    /// <summary>
    /// <paramref name="entries"/> with every record the server proved given its true UTC instant, and every other
    /// record dropped and counted in <paramref name="foreignZoneLines"/> (#6866), the csvlog, jsonlog and RDS shape of
    /// what <see cref="PgLogEntryAssembler.Assemble(string?, bool, string?, PgLogZoneMap?, out int)"/> does per line.
    /// </summary>
    public static List<PgLogEntry> Apply(IReadOnlyList<PgLogEntry> entries, PgLogZoneMap zones, out int foreignZoneLines)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(zones);

        foreignZoneLines = 0;
        var kept = new List<PgLogEntry>(entries.Count);

        foreach (var entry in entries)
        {
            var pair = PairOf(entry);

            if (zones.TryGet(pair.Stamp, pair.Zone, out var utc))
            {
                kept.Add(entry with { OccurredAtUtc = utc });
            }
            else
            {
                foreignZoneLines++;
            }
        }

        return kept;
    }

    /// <summary>
    /// Asks the target on <paramref name="connection"/> (open) to prove the pairs. Null when the setting is not a
    /// named zone, which keeps the refusal. Pairs whose stamp is not askable are left out and so come back
    /// unresolved.
    /// </summary>
    public static async Task<PgLogZoneMap?> ResolveAsync(
        DbConnection connection,
        string logTimezoneSetting,
        IReadOnlyCollection<PgLogZonePair> pairs,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(pairs);

        var askable = pairs.Where(p => IsAskable(p.Stamp)).Distinct().ToList();

        /* An explicit transaction, so the local session zone set by statement one holds for statement two whatever the
           driver does about implicit transactions across a multi-statement command. Rolled back on dispose: it reads only. */
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = commandTimeoutSeconds;
        command.CommandText = ResolveSql;
        AddParameter(command, "zone", logTimezoneSetting.Trim());
        AddParameter(command, "stamps", askable.Select(p => p.Stamp).ToArray());
        AddParameter(command, "zones", askable.Select(p => p.Zone).ToArray());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        await reader.NextResultAsync(cancellationToken);

        var rows = new List<(string, string, int, DateTime)>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetDateTime(3)));
        }

        return PgLogZoneMap.FromMatches(rows);
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    /// <summary>
    /// Whether a read should ask the server (#6866): the setting is not a UTC one (that has its own rule, #4046), it
    /// says something, and the host gave this read a way to ask.
    /// </summary>
    public static bool NeedsServerCheck(bool logTimezoneIsUtc, string? logTimezoneSetting, PgLogZoneResolver? resolver) =>
        !logTimezoneIsUtc && resolver is not null && !string.IsNullOrWhiteSpace(logTimezoneSetting);

    /// <summary>
    /// The server's proof for the stderr slab <paramref name="body"/>, or null when the read should keep today's
    /// behaviour: no check needed (<see cref="NeedsServerCheck"/>), no line to check, or a setting that is not a
    /// named zone (the refusal stays).
    /// </summary>
    public static async Task<PgLogZoneMap?> ResolveBodyAsync(
        string? body,
        bool logTimezoneIsUtc,
        string? logTimezoneSetting,
        PgLogZoneResolver? resolver,
        CancellationToken cancellationToken)
    {
        if (!NeedsServerCheck(logTimezoneIsUtc, logTimezoneSetting, resolver))
        {
            return null;
        }

        var pairs = PgLogEntryAssembler.ZonePairs(body);
        return pairs.Count == 0 ? null : await resolver!(logTimezoneSetting!, pairs, cancellationToken);
    }

    /// <summary>
    /// <see cref="PgLogEventsCollector.FilterForeignZoneEntries"/> for csvlog, jsonlog and RDS records, with the server
    /// check of a named local zone in front of it (#6866): when the setting is a named zone every record is judged by
    /// the server's proof, otherwise the UTC rule and the refusal apply as before.
    /// </summary>
    public static async Task<(List<PgLogEntry> Kept, int ForeignZoneLines)> FilterEntriesAsync(
        List<PgLogEntry> entries,
        bool logTimezoneIsUtc,
        string? logTimezoneSetting,
        PgLogZoneResolver? resolver,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);

        if (entries.Count > 0 && NeedsServerCheck(logTimezoneIsUtc, logTimezoneSetting, resolver))
        {
            /* The same pairs ResolveBodyAsync asks about: a stamp the server cannot be asked is not a pair. With none
               left there is nothing to prove, so the refusal below stands, exactly as the stderr route keeps it (an
               empty map would instead drop every record and report the read as clean). */
            var pairs = PairsOf(entries).Where(p => IsAskable(p.Stamp)).ToList();
            var zones = pairs.Count == 0 ? null : await resolver!(logTimezoneSetting!, pairs, cancellationToken);

            if (zones is not null)
            {
                var proven = Apply(entries, zones, out var foreign);
                return (proven, foreign);
            }
        }

        var kept = PgLogEventsCollector.FilterForeignZoneEntries(entries, logTimezoneIsUtc, out var foreignZoneLines);
        return (kept, foreignZoneLines);
    }
}
