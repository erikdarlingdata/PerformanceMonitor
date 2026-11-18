/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The pure state logic behind judging CHECKDB from a replica's own SQL Server error log (#8865): which check
/// is remembered per database, which error log archives to read this cycle, how many times an archive has
/// failed, and whether the remembered state still belongs to this instance. No I/O: the collector loads
/// <c>collect.collector_state</c> into a dictionary, calls this, and stages the changes itself.
///
/// <para>Every date held here is naive UTC (<see cref="DateTimeKind.Unspecified"/>, like the rest of the
/// collector) and is written in one invariant, round-trippable format, so a value read back compares equal to
/// the one stored.</para>
/// </summary>
public static class DatabaseProtectionErrorLogState
{
    /// <summary>
    /// Prefix of the per-database key holding the newest CHECKDB seen for that database
    /// (<c>checkdb:&lt;database&gt;</c>). Keyed by database name, which is what lets the shared per-database
    /// prune retire it when the database goes away. The name ends in <c>KeyPrefix</c> because the contract census
    /// looks for that suffix.
    /// </summary>
    public const string CheckKeyPrefix = "checkdb:";

    /// <summary>Server-scoped: the <c>@@SERVERNAME</c> the remembered state belongs to.</summary>
    public const string InstanceKey = "errorlog_instance";

    /// <summary>
    /// Server-scoped: log date of the newest archive already read (the steady-state marker), with how many listed archives carried that
    /// date when it was read, as <c>date=count</c>. #8865: the list dates an archive to the minute only, so several archives can share
    /// the marker's date, and a new one closed in that same minute is told apart by the count going up.
    /// </summary>
    public const string NewestArchiveKey = "errorlog_newest_archive";

    /// <summary>Server-scoped: log date below which the backfill of older archives has already been read.</summary>
    public const string BackfillBeforeKey = "errorlog_backfill_before";

    /// <summary>Server-scoped: <c>True</c> once the backfill has reached the oldest retained archive.</summary>
    public const string BackfillDoneKey = "errorlog_backfill_done";

    /// <summary>Server-scoped: failed read attempts per archive date (all archives sharing that date together), as <c>date=count</c> pairs.</summary>
    public const string AttemptsKey = "errorlog_attempts";

    /// <summary>Server-scoped: log dates of archives given up on after <see cref="MaxArchiveAttempts"/> failures.</summary>
    public const string SkippedKey = "errorlog_skipped";

    /// <summary>
    /// Server-scoped: how many CHECKDB lines the newest read that found any could not parse, kept until a read parses a line and finds
    /// none it cannot read. #8865: on an instance whose error log is not English no line ever parses, and a cycle that happens to read
    /// no CHECKDB line (the current log just rolled over) must not make every Availability Group database read as never checked.
    /// </summary>
    public const string UnparsedKey = "errorlog_unparsed";

    /// <summary>
    /// Archives (besides the current log) read per cycle, so a long backlog never makes one cycle slow. Measured (#8865, SQL Server 2022 on
    /// Linux): <c>sp_readerrorlog</c> with both search strings reads a 228 MB log in under 1 s, so three archives plus the current log are
    /// a few seconds, far under half of the 60 s supplemental timeout; the number stays at three.
    /// </summary>
    public const int ArchivesPerCycle = 3;

    /// <summary>
    /// The highest archive number read. #8865: Windows keeps at most 99 archives; SQL Server on Linux can keep more, and reading archive
    /// 100 or higher fails the whole supplemental batch with a severe error, so those are treated as not retained.
    /// </summary>
    public const int MaxReadableArchive = 99;

    /// <summary>An archive that failed this many times is skipped for good rather than blocking the markers.</summary>
    public const int MaxArchiveAttempts = 2;

    /// <summary>
    /// The longest <see cref="ArchiveList"/> the supplemental batch takes: its <c>@archives</c> is bound as <c>nvarchar(128)</c> and the
    /// batch appends one comma. #8865: a longer list is cut short by the server and the batch fails (error 537) every cycle.
    /// </summary>
    public const int MaxArchiveListLength = 127;

    private const string DateFormat = "yyyy-MM-ddTHH:mm:ss.fff";

    /// <summary>The newest CHECKDB seen for one database: when it ran, with which options, and its error count.</summary>
    public readonly record struct StoredCheck(DateTime TimeUtc, string Options, int Errors)
    {
        /// <summary>Serialises as <c>date|options|errors</c>, invariant.</summary>
        public string Format()
            => string.Create(CultureInfo.InvariantCulture, $"{FormatDate(TimeUtc)}|{Options}|{Errors}");

        /// <summary>Reads what <see cref="Format"/> wrote; false for anything else, including null.</summary>
        public static bool TryParse(string? value, out StoredCheck check)
        {
            check = default;
            if (string.IsNullOrEmpty(value))
            {
                return false;
            }

            var parts = value.Split('|');
            if (parts.Length != 3 || parts[1].Length == 0
                || !TryParseDate(parts[0], out var time)
                || !int.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var errors))
            {
                return false;
            }

            check = new StoredCheck(time, parts[1], errors);
            return true;
        }
    }

    /// <summary>One error log: <see cref="Archive"/> 0 is the current log, higher numbers are older archives.</summary>
    public readonly record struct ArchiveInfo(int Archive, DateTime LogDate);

    /// <summary>
    /// The archive-reading markers; see the key constants for what each one means. <see cref="NewestArchiveCount"/> is how many listed
    /// archives carried <see cref="NewestArchive"/>'s date when the marker was last set.
    /// </summary>
    public sealed record Markers(DateTime? NewestArchive, DateTime? BackfillBefore, bool BackfillDone, int NewestArchiveCount = 0);

    /// <summary>Everything loaded from state; <see cref="Reset"/> means the instance changed and it is all empty.</summary>
    public sealed record LoadedState(
        Markers Markers,
        IReadOnlyDictionary<DateTime, int> Attempts,
        IReadOnlyList<DateTime> Skipped,
        bool Reset);

    /// <summary>
    /// What to read this cycle, what was newly given up on, and the state the plan was made from.
    /// <see cref="Enumerated"/> is every archive the instance listed (not only those read), kept so the markers can be
    /// recomputed after the read: <see cref="MarkersAfter"/> needs the whole list to know whether the backfill finished.
    /// <see cref="EnumerateError"/> is the error number the list raised when it could not be read (null when it was read, or when
    /// it came back with a date that could not be parsed).
    /// </summary>
    public sealed record ErrorLogPlan(
        IReadOnlyList<ArchiveInfo> Read,
        IReadOnlyList<DateTime> NewlySkipped,
        LoadedState Before,
        bool EnumerateOk,
        IReadOnlyList<ArchiveInfo>? Enumerated = null,
        int? EnumerateError = null);

    /// <summary>
    /// Loads the markers, attempts and skips. A stored <see cref="InstanceKey"/> that differs from
    /// <paramref name="instance"/> means the state was written by another instance (a restored or re-pointed
    /// server): everything loads empty and <see cref="LoadedState.Reset"/> is true. An absent key is a first
    /// run, not a reset.
    /// </summary>
    public static LoadedState Load(IReadOnlyDictionary<string, string> state, string instance)
    {
        if (state.TryGetValue(InstanceKey, out var stored)
            && !string.Equals(stored, instance, StringComparison.Ordinal))
        {
            return new LoadedState(new Markers(null, null, false), new Dictionary<DateTime, int>(), Array.Empty<DateTime>(), true);
        }

        var (newest, newestCount) = ReadNewestArchive(state);
        var markers = new Markers(
            newest,
            ReadDate(state, BackfillBeforeKey),
            state.TryGetValue(BackfillDoneKey, out var done) && done == bool.TrueString,
            newestCount);
        var attempts = state.TryGetValue(AttemptsKey, out var a) ? ParseAttempts(a) : new Dictionary<DateTime, int>();
        var skipped = state.TryGetValue(SkippedKey, out var s) ? ParseDates(s) : new List<DateTime>();
        return new LoadedState(markers, attempts, skipped, false);
    }

    /// <summary>The remembered check for a database; null when none, unreadable, or the state was reset.</summary>
    public static StoredCheck? StoredFor(IReadOnlyDictionary<string, string> state, string database, bool reset)
    {
        if (reset || !state.TryGetValue(CheckKeyPrefix + database, out var value)
            || !StoredCheck.TryParse(value, out var check))
        {
            return null;
        }

        return check;
    }

    /// <summary>#8865: the remembered count of CHECKDB lines that could not be parsed (<see cref="UnparsedKey"/>); 0 when none, unreadable, or reset.</summary>
    public static int RememberedUnparsed(IReadOnlyDictionary<string, string> state, bool reset) =>
        !reset && state.TryGetValue(UnparsedKey, out var value)
        && int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var count) ? count : 0;

    /// <summary>
    /// The newer of the remembered check and the one seen this cycle. A check before
    /// <paramref name="createDateUtc"/> belongs to an earlier database of the same name and never counts.
    /// </summary>
    public static StoredCheck? Effective(StoredCheck? stored, StoredCheck? seenThisCycle, DateTime? createDateUtc)
    {
        StoredCheck? best = null;
        foreach (var candidate in new[] { stored, seenThisCycle })
        {
            if (candidate is not { } c || (createDateUtc is { } created && c.TimeUtc < created))
            {
                continue;
            }

            if (best is not { } b || c.TimeUtc > b.TimeUtc)
            {
                best = c;
            }
        }

        return best;
    }

    /// <summary>
    /// Picks the archives to read. The current log is always read. Then, up to <paramref name="perCycle"/>
    /// archives: new ones past the steady marker oldest first, then the backfill below its marker newest first.
    /// An archive at <see cref="MaxArchiveAttempts"/> is skipped instead.
    ///
    /// <para>#8865: archives are taken a date at a time. The list dates an archive to the minute, so logs cycled within one minute share
    /// a date, and a date is planned, read, counted and skipped whole: reading part of a date and marking the date read would mark the
    /// rest of it read too. A date holding more archives than <paramref name="perCycle"/> is read whole in a cycle of its own, because
    /// splitting it is the same fault; that costs one longer read on a server that cycled its log more than three times in one minute. A date
    /// whose archive numbers do not fit in <see cref="MaxArchiveListLength"/> characters is read in part and skipped in that
    /// same cycle, so the markers move past it.</para>
    ///
    /// <para>#8865: a list that was refused or failed (<paramref name="enumerateOk"/> false) reads the current log only and
    /// leaves the loaded state exactly as it was. It says nothing about which archives exist, so it must not move a marker:
    /// marking the backfill done here would make the first list that works set the steady marker to its newest archive with the
    /// backfill already finished, and no retained archive would ever be read. Whether the rows wait on the backfill meanwhile is
    /// the collector's call (they do not: nothing older can be read in that cycle).</para>
    /// </summary>
    public static ErrorLogPlan PlanArchives(
        LoadedState loaded, IReadOnlyList<ArchiveInfo> enumerated, bool enumerateOk, int perCycle, int? enumerateError = null)
    {
        var current = enumerated.Where(a => a.Archive == 0).Select(a => (ArchiveInfo?)a).FirstOrDefault()
            ?? new ArchiveInfo(0, default);

        if (!enumerateOk)
        {
            return new ErrorLogPlan(new[] { current }, Array.Empty<DateTime>(), loaded, false, null, enumerateError);
        }

        var archives = enumerated.Where(a => a.Archive > 0).ToList();
        /* #8865: an attempt count or a skip for a date the list no longer holds is for an archive that aged out; kept, the skipped list
           would show a note nobody can clear. Trimmed only when the list was read, since a list that was not read says nothing. */
        var listed = new HashSet<DateTime>(archives.Select(a => a.LogDate));
        var before = loaded with
        {
            Attempts = loaded.Attempts.Where(p => listed.Contains(p.Key)).ToDictionary(p => p.Key, p => p.Value),
            Skipped = loaded.Skipped.Where(listed.Contains).ToList(),
        };
        if (before.Markers.NewestArchive is null)
        {
            /* #8865: with no archives yet the marker is the minimum date, not null: a null marker would make the
               first archive that later appears look like a first run and be taken as already read, losing every
               CHECKDB line written between the last current-log read and that roll.
               #8865: the backfill is not done on a first run. A store an earlier build wrote marked it done on a refused list with no
               steady marker; kept, that store would never read a retained archive. */
            var newest = archives.Count > 0 ? archives.Max(a => a.LogDate) : DateTime.MinValue;
            before = before with
            {
                Markers = before.Markers with
                {
                    NewestArchive = newest, NewestArchiveCount = archives.Count(a => a.LogDate == newest), BackfillDone = false,
                },
            };
        }

        var read = new List<ArchiveInfo> { current };
        var listLength = ArchiveList(read).Length;
        var newlySkipped = new List<DateTime>();
        var archivesRead = 0;
        foreach (var group in WalkOrder(before.Markers, archives))
        {
            var date = group[0].LogDate;
            if (before.Skipped.Contains(date))
            {
                continue;
            }

            if (before.Attempts.TryGetValue(date, out var tries) && tries >= MaxArchiveAttempts)
            {
                newlySkipped.Add(date);
                continue;
            }

            if (archivesRead > 0 && archivesRead + group.Count > perCycle)
            {
                break;
            }

            /* #8865: a date read alone is still capped at what the archive list can carry, rather than the whole batch failing every
               cycle on a cut list. A date read in part can never be read whole, and every read of it goes fine, so it would gain no
               attempt and the markers would stall on it: it is skipped in the same cycle that reads its part, whose lines still count. */
            var taken = new List<ArchiveInfo>();
            foreach (var archive in group)
            {
                var length = listLength + 1 + archive.Archive.ToString(CultureInfo.InvariantCulture).Length;
                if (length > MaxArchiveListLength)
                {
                    break;
                }

                taken.Add(archive);
                listLength = length;
            }

            read.AddRange(taken);
            archivesRead += taken.Count;
            if (taken.Count < group.Count)
            {
                newlySkipped.Add(date);
                break;
            }

            if (archivesRead >= perCycle)
            {
                break;
            }
        }

        return new ErrorLogPlan(read, newlySkipped, before, true, enumerated);
    }

    /// <summary>The planned logs as the supplemental batch's <c>@archives</c>: their numbers, comma-separated.</summary>
    public static string ArchiveList(IEnumerable<ArchiveInfo> read) =>
        string.Join(',', read.Select(a => a.Archive.ToString(CultureInfo.InvariantCulture)));

    /// <summary>
    /// The dates read whole this cycle: every archive the list gave that date was in the plan and <paramref name="archiveOk"/> says it
    /// read fine. #8865: one archive of a date reading fine says nothing about another archive with the same date, so a date with any
    /// archive unread or failed is not read.
    /// </summary>
    public static List<DateTime> DatesReadWhole(ErrorLogPlan plan, Func<int, bool> archiveOk)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(archiveOk);
        var planned = new HashSet<int>(plan.Read.Where(a => a.Archive > 0).Select(a => a.Archive));
        return (plan.Enumerated ?? plan.Read)
            .Where(a => a.Archive > 0)
            .GroupBy(a => a.LogDate)
            .Where(g => g.All(a => planned.Contains(a.Archive) && archiveOk(a.Archive)))
            .Select(g => g.Key)
            .ToList();
    }

    /// <summary>The distinct dates of the archives planned this cycle: one attempt each, however many archives share it.</summary>
    public static List<DateTime> PlannedDates(ErrorLogPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        return plan.Read.Where(a => a.Archive > 0).Select(a => a.LogDate).Distinct().ToList();
    }

    /// <summary>
    /// The markers after a cycle. Each marker moves over the contiguous run, in walk order, of dates that
    /// are done (read whole, or skipped) and stops at the first that is neither, so an archive that failed in the
    /// middle is retried next cycle and never jumped over. A list that was not read moves nothing (#8865).
    /// </summary>
    public static Markers MarkersAfter(
        ErrorLogPlan plan, IReadOnlyCollection<DateTime> readOk, IReadOnlyList<ArchiveInfo> enumerated)
    {
        var before = plan.Before.Markers;
        if (!plan.EnumerateOk)
        {
            return before;
        }

        var counts = enumerated.Where(a => a.Archive > 0).GroupBy(a => a.LogDate).ToDictionary(g => g.Key, g => g.Count());
        var skipped = new HashSet<DateTime>(plan.Before.Skipped);
        skipped.UnionWith(plan.NewlySkipped);
        bool IsDone(DateTime date) => readOk.Contains(date) || skipped.Contains(date);

        var newest = before.NewestArchive;
        var newestCount = before.NewestArchiveCount;
        if (newest is { } steadyMarker)
        {
            /* #8865: more archives on the marker's date than when it was set means one was closed in the marker's minute after it was
               read, and the marker holds until that date is read again whole. Fewer means older ones of that date aged out, and the count
               follows the list down so the next one closed in that minute still shows as new. The one case this cannot see is a roll that
               adds an archive in the marker's minute while an archive of that same minute ages out, which needs every retained archive
               to share one minute. */
            var markerCount = counts.GetValueOrDefault(steadyMarker);
            if (markerCount <= before.NewestArchiveCount || IsDone(steadyMarker))
            {
                newestCount = markerCount;
                foreach (var date in counts.Keys.Where(d => d > steadyMarker).OrderBy(d => d))
                {
                    if (!IsDone(date))
                    {
                        break;
                    }

                    newest = date;
                    newestCount = counts[date];
                }
            }
        }

        var backfillBefore = before.BackfillBefore;
        var backfillDone = before.BackfillDone;
        if (!backfillDone)
        {
            var candidates = counts.Keys.Where(d => d < (before.BackfillBefore ?? DateTime.MaxValue))
                .OrderByDescending(d => d).ToList();
            var advanced = 0;
            foreach (var date in candidates)
            {
                if (!IsDone(date))
                {
                    break;
                }

                backfillBefore = date;
                advanced++;
            }

            backfillDone = advanced == candidates.Count;
        }

        return new Markers(newest, backfillBefore, backfillDone, newestCount);
    }

    /// <summary>Serialises the steady marker as <c>date=count</c>; empty when there is none.</summary>
    public static string FormatNewestArchive(Markers markers)
    {
        ArgumentNullException.ThrowIfNull(markers);
        return markers.NewestArchive is { } newest
            ? string.Create(CultureInfo.InvariantCulture, $"{FormatDate(newest)}={markers.NewestArchiveCount}")
            : string.Empty;
    }

    /// <summary>Stages <paramref name="value"/> for <paramref name="key"/> unless state already holds exactly it.</summary>
    public static void StageIfChanged(
        IDictionary<string, string> pending, IReadOnlyDictionary<string, string> state, string key, string value)
    {
        if (state.TryGetValue(key, out var current) && string.Equals(current, value, StringComparison.Ordinal))
        {
            return;
        }

        pending[key] = value;
    }

    /// <summary>Serialises attempts as <c>date=count</c> pairs, oldest first.</summary>
    public static string FormatAttempts(IReadOnlyDictionary<DateTime, int> attempts)
        => string.Join(',', attempts.OrderBy(p => p.Key)
            .Select(p => string.Create(CultureInfo.InvariantCulture, $"{FormatDate(p.Key)}={p.Value}")));

    /// <summary>Serialises dates, oldest first.</summary>
    public static string FormatDates(IEnumerable<DateTime> dates)
        => string.Join(',', dates.OrderBy(d => d).Select(FormatDate));

    /// <summary>Reads what <see cref="FormatAttempts"/> wrote, dropping any pair it cannot read.</summary>
    public static Dictionary<DateTime, int> ParseAttempts(string? value)
    {
        var result = new Dictionary<DateTime, int>();
        foreach (var pair in (value ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=');
            if (parts.Length == 2 && TryParseDate(parts[0], out var date)
                && int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var count))
            {
                result[date] = count;
            }
        }

        return result;
    }

    /// <summary>Reads what <see cref="FormatDates"/> wrote, dropping any entry it cannot read.</summary>
    public static List<DateTime> ParseDates(string? value)
    {
        var result = new List<DateTime>();
        foreach (var part in (value ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            if (TryParseDate(part, out var date))
            {
                result.Add(date);
            }
        }

        return result;
    }

    /* The dates to read, each as the whole group of archives the list gave it, in archive order: first the marker's own date when it holds
       more archives than when it was read, then the newer dates oldest first, then the backfill newest first. A date is given once. */
    private static IEnumerable<List<ArchiveInfo>> WalkOrder(Markers markers, List<ArchiveInfo> archives)
    {
        var groups = archives.GroupBy(a => a.LogDate).ToDictionary(g => g.Key, g => g.OrderBy(a => a.Archive).ToList());
        var given = new HashSet<DateTime>();
        if (markers.NewestArchive is { } newest)
        {
            if (groups.TryGetValue(newest, out var same) && same.Count > markers.NewestArchiveCount && given.Add(newest))
            {
                yield return same;
            }

            foreach (var date in groups.Keys.Where(d => d > newest).OrderBy(d => d))
            {
                if (given.Add(date))
                {
                    yield return groups[date];
                }
            }
        }

        if (!markers.BackfillDone)
        {
            foreach (var date in groups.Keys.Where(d => d < (markers.BackfillBefore ?? DateTime.MaxValue)).OrderByDescending(d => d))
            {
                if (given.Add(date))
                {
                    yield return groups[date];
                }
            }
        }
    }

    /* The steady marker as FormatNewestArchive writes it. A bare date with no count reads as count 0, so a marker written before the count
       existed re-reads its own date once. */
    private static (DateTime? Date, int Count) ReadNewestArchive(IReadOnlyDictionary<string, string> state)
    {
        if (!state.TryGetValue(NewestArchiveKey, out var value) || string.IsNullOrEmpty(value))
        {
            return (null, 0);
        }

        var parts = value.Split('=');
        if (parts.Length > 2 || !TryParseDate(parts[0], out var date))
        {
            return (null, 0);
        }

        var count = parts.Length == 2 && int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? n : 0;
        return (date, count);
    }

    private static DateTime? ReadDate(IReadOnlyDictionary<string, string> state, string key)
        => state.TryGetValue(key, out var value) && TryParseDate(value, out var date) ? date : null;

    private static string FormatDate(DateTime value) => value.ToString(DateFormat, CultureInfo.InvariantCulture);

    private static bool TryParseDate(string value, out DateTime date)
        => DateTime.TryParseExact(value, DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
}
