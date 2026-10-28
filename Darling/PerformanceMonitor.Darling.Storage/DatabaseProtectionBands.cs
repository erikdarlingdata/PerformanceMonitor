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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>How old a fact may get before it is overdue (#6873). The defaults are the issue's: full backup 7 days, log backup 60 minutes, CHECKDB 14 days.</summary>
public sealed record DatabaseProtectionThresholds(TimeSpan FullBackup, TimeSpan LogBackup, TimeSpan CheckDb)
{
    public static DatabaseProtectionThresholds Default { get; } =
        new(TimeSpan.FromDays(7), TimeSpan.FromMinutes(60), TimeSpan.FromDays(14));
}

/// <summary>
/// One fact (CHECKDB, full backup or log backup) after banding. <see cref="State"/> is one of <see cref="DatabaseProtectionBands"/>'s
/// state words. <see cref="Age"/> is measured at the capture, null when there is no time to measure.
/// </summary>
public sealed record DatabaseProtectionFactBand(string State, bool Overdue, TimeSpan? Age, TimeSpan Threshold);

/// <summary>One database after banding. <see cref="ExcludedReason"/> is set when the database is not judged (every fact then reads <c>excluded</c>).</summary>
public sealed record DatabaseProtectionBand(
    string? ExcludedReason,
    DatabaseProtectionFactBand CheckDb,
    DatabaseProtectionFactBand FullBackup,
    DatabaseProtectionFactBand LogBackup)
{
    /// <summary>Any of the three facts is overdue.</summary>
    public bool AnyOverdue => CheckDb.Overdue || FullBackup.Overdue || LogBackup.Overdue;
}

/// <summary>What <see cref="DatabaseProtectionBands.Summarize"/> found: which databases are overdue on what.</summary>
public sealed record DatabaseProtectionSummary(
    int Databases,
    int Assessed,
    int Excluded,
    int OverdueDatabases,
    IReadOnlyList<string> OverdueCheckDb,
    IReadOnlyList<string> OverdueFullBackup,
    IReadOnlyList<string> OverdueLogBackup,
    string Text);

/// <summary>
/// #6873: the pure banding over one capture's stored facts. No I/O, so the MCP tool and the Viewer call it with the same facts and say the
/// same words. Order of the rules, first match wins: a database that is not online (or is a secondary) is excluded and judged on nothing;
/// a SIMPLE-recovery log reads <c>not_expected</c>; the collector's <c>unknown</c> and <c>unavailable</c> pass through as their own words;
/// a platform-managed or native-only target shows the age under the platform's word and is never overdue; a CHECKDB or backup that never
/// happened is overdue unless the database is newer than the threshold (<c>new_database</c>); a recorded time is overdue when its age at
/// the capture is above the threshold.
/// </summary>
public static class DatabaseProtectionBands
{
    public const string Ok = "ok";
    public const string Overdue = "overdue";
    public const string Never = DatabaseProtectionCollector.CheckDbNever;
    public const string NoneInWindow = DatabaseProtectionCollector.BackupNoneInWindow;
    public const string NewDatabase = "new_database";
    public const string NotExpected = DatabaseProtectionCollector.BackupNotExpected;
    public const string Excluded = "excluded";
    public const string Unknown = DatabaseProtectionCollector.CheckDbUnknown;
    public const string Unavailable = DatabaseProtectionCollector.BackupUnavailable;
    public const string PlatformManaged = DatabaseProtectionCollector.PlatformManaged;
    public const string NativeOnly = DatabaseProtectionCollector.PlatformNativeOnly;

    /// <summary>The database is judged on nothing: offline, restoring, recovering, not yet recovered, suspect or in emergency, or a secondary replica.</summary>
    public static string? ExclusionReason(string? stateDesc, string? replicaRole)
    {
        if (!string.IsNullOrEmpty(stateDesc) && !string.Equals(stateDesc, "ONLINE", StringComparison.OrdinalIgnoreCase))
            return stateDesc.ToLowerInvariant();
        return string.Equals(replicaRole, "SECONDARY", StringComparison.OrdinalIgnoreCase) ? "secondary" : null;
    }

    /// <summary>Bands one database's three facts as of <paramref name="capturedAt"/> (UTC).</summary>
    public static DatabaseProtectionBand Evaluate(DatabaseProtectionFact fact, DateTime capturedAt, DatabaseProtectionThresholds thresholds)
    {
        ArgumentNullException.ThrowIfNull(fact);
        ArgumentNullException.ThrowIfNull(thresholds);

        var excluded = ExclusionReason(fact.StateDesc, fact.ReplicaRole);
        if (excluded is not null)
        {
            return new DatabaseProtectionBand(excluded,
                new(Excluded, false, AgeOf(fact.LastGoodCheckDbTime, capturedAt), thresholds.CheckDb),
                new(Excluded, false, AgeOf(fact.LastFullBackupTime, capturedAt), thresholds.FullBackup),
                new(Excluded, false, AgeOf(fact.LastLogBackupTime, capturedAt), thresholds.LogBackup));
        }

        return new DatabaseProtectionBand(null,
            Band(fact, fact.CheckDbState, fact.LastGoodCheckDbTime, thresholds.CheckDb, capturedAt),
            Band(fact, fact.FullBackupState, fact.LastFullBackupTime, thresholds.FullBackup, capturedAt),
            Band(fact, fact.LogBackupState, fact.LastLogBackupTime, thresholds.LogBackup, capturedAt));
    }

    private static TimeSpan? AgeOf(DateTime? time, DateTime capturedAt) =>
        time is { } t ? (capturedAt > t ? capturedAt - t : TimeSpan.Zero) : null;

    private static DatabaseProtectionFactBand Band(
        DatabaseProtectionFact fact, string? stored, DateTime? time, TimeSpan threshold, DateTime capturedAt)
    {
        var age = AgeOf(time, capturedAt);
        if (stored == DatabaseProtectionCollector.BackupNotExpected)
            return new(NotExpected, false, age, threshold);
        if (stored == DatabaseProtectionCollector.CheckDbUnknown)
            return new(Unknown, false, age, threshold);
        if (stored == DatabaseProtectionCollector.BackupUnavailable)
            return new(Unavailable, false, age, threshold);

        if (fact.Platform == DatabaseProtectionCollector.PlatformManaged)
            return new(PlatformManaged, false, age, threshold);
        if (fact.Platform == DatabaseProtectionCollector.PlatformNativeOnly)
            return new(NativeOnly, false, age, threshold);

        if (stored is DatabaseProtectionCollector.CheckDbNever or DatabaseProtectionCollector.BackupNoneInWindow)
        {
            /* A database created less than the threshold ago cannot be late yet. */
            if (fact.CreateDate is { } created && capturedAt - created < threshold)
                return new(NewDatabase, false, null, threshold);
            return new(stored, true, null, threshold);
        }

        if (age is { } a)
            return a > threshold ? new(Overdue, true, a, threshold) : new(Ok, false, a, threshold);

        /* A word this build does not know, with no time: say it as it is stored. */
        return new(stored ?? Unknown, false, null, threshold);
    }

    /// <summary>"3 days 4 hours", "5 hours 10 minutes" or "42 minutes": the two largest units. The tool and the Viewer say an age this way.</summary>
    public static string Describe(TimeSpan age)
    {
        string Unit(long n, string unit) => string.Create(CultureInfo.InvariantCulture, $"{n} {unit}{(n == 1 ? "" : "s")}");
        if (age.TotalDays >= 1) return Unit(age.Days, "day") + (age.Hours > 0 ? " " + Unit(age.Hours, "hour") : "");
        if (age.TotalHours >= 1) return Unit(age.Hours, "hour") + (age.Minutes > 0 ? " " + Unit(age.Minutes, "minute") : "");
        return Unit((long)age.TotalMinutes, "minute");
    }

    /// <summary>The server answer: how many databases were judged and which are overdue on what.</summary>
    public static DatabaseProtectionSummary Summarize(IReadOnlyList<(DatabaseProtectionFact Fact, DatabaseProtectionBand Band)> bands)
    {
        ArgumentNullException.ThrowIfNull(bands);
        var judged = bands.Where(b => b.Band.ExcludedReason is null).ToList();
        List<string> Names(Func<DatabaseProtectionBand, DatabaseProtectionFactBand> pick) =>
            judged.Where(b => pick(b.Band).Overdue).Select(b => b.Fact.DatabaseName).ToList();

        var checkDb = Names(b => b.CheckDb);
        var full = Names(b => b.FullBackup);
        var log = Names(b => b.LogBackup);
        var overdueDatabases = judged.Count(b => b.Band.AnyOverdue);

        string text;
        if (bands.Count == 0)
            text = "No databases in the capture.";
        else if (judged.Count == 0)
            text = "No database is judged: every one is offline, restoring or a secondary.";
        else if (overdueDatabases == 0)
            text = string.Create(CultureInfo.InvariantCulture, $"No database is overdue ({judged.Count} judged).");
        else
        {
            var parts = new List<string>();
            if (full.Count > 0) parts.Add("full backup: " + string.Join(", ", full));
            if (log.Count > 0) parts.Add("log backup: " + string.Join(", ", log));
            if (checkDb.Count > 0) parts.Add("CHECKDB: " + string.Join(", ", checkDb));
            text = string.Create(CultureInfo.InvariantCulture, $"{overdueDatabases} of {judged.Count} judged databases overdue. ") +
                   string.Join(". ", parts) + ".";
        }

        return new DatabaseProtectionSummary(bands.Count, judged.Count, bands.Count - judged.Count, overdueDatabases, checkDb, full, log, text);
    }
}
