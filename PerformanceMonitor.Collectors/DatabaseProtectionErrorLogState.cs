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

    /// <summary>Server-scoped: log date of the newest archive already read (the steady-state marker).</summary>
    public const string NewestArchiveKey = "errorlog_newest_archive";

    /// <summary>Server-scoped: log date below which the backfill of older archives has already been read.</summary>
    public const string BackfillBeforeKey = "errorlog_backfill_before";

    /// <summary>Server-scoped: <c>True</c> once the backfill has reached the oldest retained archive.</summary>
    public const string BackfillDoneKey = "errorlog_backfill_done";

    /// <summary>Server-scoped: failed read attempts per archive, as <c>date=count</c> pairs.</summary>
    public const string AttemptsKey = "errorlog_attempts";

    /// <summary>Server-scoped: log dates of archives given up on after <see cref="MaxArchiveAttempts"/> failures.</summary>
    public const string SkippedKey = "errorlog_skipped";

    /// <summary>
    /// Archives (besides the current log) read per cycle, so a long backlog never makes one cycle slow. Measured (#8865, SQL Server 2022 on
    /// Linux): <c>sp_readerrorlog</c> with both search strings reads a 228 MB log in under 1 s, so three archives plus the current log are
    /// a few seconds, far under half of the 60 s supplemental timeout; the number stays at three.
    /// </summary>
    public const int ArchivesPerCycle = 3;

    /// <summary>An archive that failed this many times is skipped for good rather than blocking the markers.</summary>
    public const int MaxArchiveAttempts = 2;

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

    /// <summary>The archive-reading markers; see the key constants for what each one means.</summary>
    public sealed record Markers(DateTime? NewestArchive, DateTime? BackfillBefore, bool BackfillDone);

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

        var markers = new Markers(
            ReadDate(state, NewestArchiveKey),
            ReadDate(state, BackfillBeforeKey),
            state.TryGetValue(BackfillDoneKey, out var done) && done == bool.TrueString);
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

        var before = loaded;
        var archives = enumerated.Where(a => a.Archive > 0).ToList();
        if (before.Markers.NewestArchive is null)
        {
            /* #8865: with no archives yet the marker is the minimum date, not null: a null marker would make the
               first archive that later appears look like a first run and be taken as already read, losing every
               CHECKDB line written between the last current-log read and that roll. */
            before = before with
            {
                Markers = before.Markers with { NewestArchive = archives.Count > 0 ? archives.Max(a => a.LogDate) : DateTime.MinValue },
            };
        }

        var read = new List<ArchiveInfo> { current };
        var newlySkipped = new List<DateTime>();
        foreach (var archive in WalkOrder(before.Markers, archives))
        {
            if (read.Count - 1 >= perCycle)
            {
                break;
            }

            if (before.Skipped.Contains(archive.LogDate))
            {
                continue;
            }

            if (before.Attempts.TryGetValue(archive.LogDate, out var tries) && tries >= MaxArchiveAttempts)
            {
                newlySkipped.Add(archive.LogDate);
                continue;
            }

            read.Add(archive);
        }

        return new ErrorLogPlan(read, newlySkipped, before, true, enumerated);
    }

    /// <summary>
    /// The markers after a cycle. Each marker moves over the contiguous run, in walk order, of archives that
    /// are done (read, or skipped) and stops at the first that is neither, so an archive that failed in the
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

        var archives = enumerated.Where(a => a.Archive > 0).ToList();
        var skipped = new HashSet<DateTime>(plan.Before.Skipped);
        skipped.UnionWith(plan.NewlySkipped);
        bool IsDone(ArchiveInfo a) => readOk.Contains(a.LogDate) || skipped.Contains(a.LogDate);

        var newest = before.NewestArchive;
        if (newest is { } steadyMarker)
        {
            foreach (var a in archives.Where(x => x.LogDate > steadyMarker).OrderBy(x => x.LogDate))
            {
                if (!IsDone(a))
                {
                    break;
                }

                newest = a.LogDate;
            }
        }

        var backfillBefore = before.BackfillBefore;
        var backfillDone = before.BackfillDone;
        if (!backfillDone)
        {
            var candidates = archives.Where(x => x.LogDate < (before.BackfillBefore ?? DateTime.MaxValue))
                .OrderByDescending(x => x.LogDate).ToList();
            var advanced = 0;
            foreach (var a in candidates)
            {
                if (!IsDone(a))
                {
                    break;
                }

                backfillBefore = a.LogDate;
                advanced++;
            }

            backfillDone = advanced == candidates.Count;
        }

        return new Markers(newest, backfillBefore, backfillDone);
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

    private static IEnumerable<ArchiveInfo> WalkOrder(Markers markers, List<ArchiveInfo> archives)
    {
        if (markers.NewestArchive is { } newest)
        {
            foreach (var a in archives.Where(x => x.LogDate > newest).OrderBy(x => x.LogDate))
            {
                yield return a;
            }
        }

        if (!markers.BackfillDone)
        {
            foreach (var a in archives.Where(x => x.LogDate < (markers.BackfillBefore ?? DateTime.MaxValue))
                         .OrderByDescending(x => x.LogDate))
            {
                yield return a;
            }
        }
    }

    private static DateTime? ReadDate(IReadOnlyDictionary<string, string> state, string key)
        => state.TryGetValue(key, out var value) && TryParseDate(value, out var date) ? date : null;

    private static string FormatDate(DateTime value) => value.ToString(DateFormat, CultureInfo.InvariantCulture);

    private static bool TryParseDate(string value, out DateTime date)
        => DateTime.TryParseExact(value, DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
}
