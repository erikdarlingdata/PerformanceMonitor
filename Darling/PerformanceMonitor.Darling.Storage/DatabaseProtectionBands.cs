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
/// state words. <see cref="Age"/> is measured at the capture, null when there is no time to measure. #8865: <see cref="Source"/> says where a CHECKDB time
/// came from (<c>error_log</c> or <c>boot_page</c>, null when the row predates the error-log read), <see cref="Options"/> and <see cref="Errors"/> are what the
/// error-log line said, <see cref="Detail"/> explains a state in words (a secondary's backups, a replica that is not readable, a backfill still running), and
/// <see cref="ReferenceTime"/> is the boot-page time of an AG database, shown for reference and not tied to this replica.
/// </summary>
public sealed record DatabaseProtectionFactBand(
    string State, bool Overdue, TimeSpan? Age, TimeSpan Threshold,
    string? Source = null, string? Options = null, int? Errors = null, string? Detail = null, DateTime? ReferenceTime = null);

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

/// <summary>One overdue fact of one database (#6873): <see cref="Kind"/> is one of <see cref="DatabaseProtectionBands"/>'s kind words, <see cref="State"/> the banded
/// state word (<c>overdue</c>, <c>never</c>, <c>none_in_window</c> or, #8865, <c>errors_found</c>) and <see cref="Age"/> the age at the capture, null when there is no time to measure and <see cref="Threshold"/> the age this database was judged against (#7611). <see cref="Errors"/> is the error count of an <c>errors_found</c> CHECKDB (#8865).</summary>
public sealed record DatabaseProtectionOverdue(string Kind, string Database, string State, TimeSpan? Age, TimeSpan? Threshold = null, int? Errors = null)
{
    /// <summary>The words the alert detail and the fleet chip use for this fact, for example <c>Sales (9 days 4 hours old)</c> or <c>Sales (never checked)</c>.</summary>
    public string Text => DatabaseProtectionBands.DescribeOverdue(this);

    /// <summary>The fact in words for a chip: <c>Full backup</c>, <c>Log backup</c> or <c>CHECKDB</c>.</summary>
    public string Label => Kind switch
    {
        DatabaseProtectionBands.KindFullBackup => "Full backup",
        DatabaseProtectionBands.KindLogBackup => "Log backup",
        _ => "CHECKDB",
    };
}

/// <summary>
/// #6873: the pure banding over one capture's stored facts. No I/O, so the MCP tool and the Viewer call it with the same facts and say the
/// same words. Order of the rules, first match wins: a database that is not online is excluded and judged on nothing (a secondary whose own error log was not read is too; #8865 a secondary whose log was read keeps only its two backup facts excluded and its CHECKDB is judged);
/// a SIMPLE-recovery log reads <c>not_expected</c>, and so does a late or missing log backup of master or model (#9376); the collector's <c>unknown</c> and <c>unavailable</c> pass through as their own words;
/// a platform-managed target shows the age under the platform's word and is never overdue, and a native-only target does the same for its two backup facts while its CHECKDB is judged as on SQL Server (#6902); a CHECKDB or backup that never
/// happened is overdue unless the database is newer than the threshold (<c>new_database</c>); a recorded time is overdue when its age at
/// the capture is above the threshold. #8865: CHECKDB comes from the replica's own error log when it was read (an AG database on the log only, <c>pending</c> while the
/// backfill still runs, <c>unknown</c> when no check was found but the log held CHECKDB lines that could not be read, <c>errors_found</c> when the check reported
/// errors), else from the boot page as before.
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
    /// <summary>#8865: the newest CHECKDB in the replica's own error log reported errors. Overdue whatever its age.</summary>
    public const string ErrorsFound = "errors_found";
    /// <summary>#8865: an AG database with no CHECKDB found yet while the error-log backfill is still reading older logs. Never overdue.</summary>
    public const string Pending = "pending";
    public const string SourceErrorLog = "error_log";
    public const string SourceBootPage = "boot_page";
    private const string DetailSecondary = "secondary";
    /// <summary>#8865: the sentence a replica that refuses connections gets, said by every surface (the band's detail, the web cell's tooltip, the Viewer's).</summary>
    public const string DetailNotReadable = "replica is not readable, so CHECKDB cannot run on it";
    /// <summary>#8865: the sentence an Availability Group database with no check found gets when the error log holds CHECKDB lines that could
    /// not be read, which is what an instance whose error log is not written in English shows.</summary>
    public const string DetailUnreadableLog = "the error log's CHECKDB lines could not be read (the log is likely not in English), so this replica's last check is unknown";

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
            /* #8865: a secondary whose error log was read is excluded on the two backup facts only. Its CHECKDB is judged below. */
            var secondaryOnly = excluded == "secondary" && UsesErrorLog(fact);
            if (!secondaryOnly)
            {
                return new DatabaseProtectionBand(excluded,
                    new(Excluded, false, AgeOf(fact.LastGoodCheckDbTime, capturedAt), thresholds.CheckDb),
                    new(Excluded, false, AgeOf(fact.LastFullBackupTime, capturedAt), thresholds.FullBackup),
                    new(Excluded, false, AgeOf(fact.LastLogBackupTime, capturedAt), thresholds.LogBackup));
            }

            return new DatabaseProtectionBand(null,
                BandCheckDb(fact, capturedAt, thresholds.CheckDb),
                new(Excluded, false, AgeOf(fact.LastFullBackupTime, capturedAt), thresholds.FullBackup, Detail: DetailSecondary),
                new(Excluded, false, AgeOf(fact.LastLogBackupTime, capturedAt), thresholds.LogBackup, Detail: DetailSecondary));
        }

        return new DatabaseProtectionBand(null,
            UsesErrorLog(fact)
                ? BandCheckDb(fact, capturedAt, thresholds.CheckDb)
                : Band(fact, fact.CheckDbState, fact.LastGoodCheckDbTime, thresholds.CheckDb, capturedAt, isCheckDb: true),
            Band(fact, fact.FullBackupState, fact.LastFullBackupTime, thresholds.FullBackup, capturedAt, isCheckDb: false),
            BandLog(fact, capturedAt, thresholds.LogBackup));
    }

    /// <summary>#9376: the sentence a system database's log fact gets when it is not judged, said by every surface.</summary>
    public const string DetailSystemDatabase = "system database: log backups are not needed";

    /// <summary>#9376: <c>master</c> cannot take a log backup (SQL Server allows only full backups of it) and nothing writes to <c>model</c>, which is only the
    /// template for new databases. Their names cannot be changed and sys.databases always spells them in lowercase, so the match is exact: on a case-sensitive server a user database named
    /// <c>Model</c> or <c>MASTER</c> is still judged. msdb and every other user database are judged.</summary>
    internal static bool IsLogBackupExempt(string? databaseName) =>
        string.Equals(databaseName, "master", StringComparison.Ordinal)
        || string.Equals(databaseName, "model", StringComparison.Ordinal);

    /// <summary>The log fact: banded as any backup, except that a late or missing log backup of <c>master</c> or <c>model</c> reads <c>not_expected</c> with a
    /// detail, whatever the recovery model (#9376). The full backup and CHECKDB of both are judged as before.</summary>
    private static DatabaseProtectionFactBand BandLog(DatabaseProtectionFact fact, DateTime capturedAt, TimeSpan threshold)
    {
        var band = Band(fact, fact.LogBackupState, fact.LastLogBackupTime, threshold, capturedAt, isCheckDb: false);
        return band.Overdue && IsLogBackupExempt(fact.DatabaseName)
            ? new(NotExpected, false, band.Age, threshold, Detail: DetailSystemDatabase)
            : band;
    }

    /// <summary>#8865 rules 1 and 2: the error log is the CHECKDB source only on a platform that is not managed and when the read finished
    /// (<c>read</c>) or only that cycle's fresh lines are missing (<c>failed</c>, the row carries the remembered check). NULL (an old row),
    /// <c>refused</c> and <c>not_applicable</c> band as before the error-log read existed.</summary>
    private static bool UsesErrorLog(DatabaseProtectionFact fact) =>
        fact.Platform != DatabaseProtectionCollector.PlatformManaged
        && fact.ErrorLogState is DatabaseProtectionCollector.ErrorLogRead or DatabaseProtectionCollector.ErrorLogFailed;

    /// <summary>#8865 rules 3 and 4: CHECKDB judged from the replica's own error log. An AG database (<c>replica_role</c> set) is judged on the log only,
    /// because the boot page is shared by every replica and says nothing about this one; any other database takes the newer of the boot page and the log.</summary>
    private static DatabaseProtectionFactBand BandCheckDb(DatabaseProtectionFact fact, DateTime capturedAt, TimeSpan threshold)
    {
        var logTime = fact.ErrorLogCheckDbTime;
        var inAg = !string.IsNullOrEmpty(fact.ReplicaRole);
        var notReadable = inAg && fact.ReplicaReadable == false ? DetailNotReadable : null;

        if (!inAg)
        {
            /* Nothing newer in the log: the boot page alone, as before. A newer log check wins, because the boot page moves only on a good check. */
            var bootTime = fact.LastGoodCheckDbTime;
            if (logTime is null || (bootTime is { } b && b >= logTime.Value))
            {
                var band = Band(fact, fact.CheckDbState, bootTime, threshold, capturedAt, isCheckDb: true);
                return bootTime is null ? band : band with { Source = SourceBootPage };
            }
        }
        else if (logTime is null)
        {
            var reference = fact.LastGoodCheckDbTime;
            if (fact.ErrorLogBackfillComplete == false)
            {
                return new(Pending, false, null, threshold, SourceErrorLog,
                    Detail: "the older error logs are still being read", ReferenceTime: reference);
            }

            /* #8865: lines the parser could not read mean the checks may be there in another language, so "none found" is not "never". */
            if (fact.ErrorLogUnparsedLines is > 0)
            {
                return new(Unknown, false, null, threshold, SourceErrorLog, Detail: DetailUnreadableLog, ReferenceTime: reference);
            }

            if (fact.CreateDate is { } created && capturedAt - created < threshold)
                return new(NewDatabase, false, null, threshold, SourceErrorLog, Detail: notReadable, ReferenceTime: reference);
            return new(Never, true, null, threshold, SourceErrorLog, Detail: notReadable, ReferenceTime: reference);
        }

        var age = AgeOf(logTime, capturedAt);
        var referenceTime = inAg ? fact.LastGoodCheckDbTime : null;
        if (fact.ErrorLogCheckDbErrors is > 0)
        {
            return new(ErrorsFound, true, age, threshold, SourceErrorLog, fact.ErrorLogCheckDbOptions, fact.ErrorLogCheckDbErrors, notReadable, referenceTime);
        }

        var late = age is { } a && a > threshold;
        return new(late ? Overdue : Ok, late, age, threshold, SourceErrorLog, fact.ErrorLogCheckDbOptions, fact.ErrorLogCheckDbErrors, notReadable, referenceTime);
    }

    private static TimeSpan? AgeOf(DateTime? time, DateTime capturedAt) =>
        time is { } t ? (capturedAt > t ? capturedAt - t : TimeSpan.Zero) : null;

    private static DatabaseProtectionFactBand Band(
        DatabaseProtectionFact fact, string? stored, DateTime? time, TimeSpan threshold, DateTime capturedAt, bool isCheckDb)
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
        /* #6902: native_only (Amazon RDS) is about the two backup facts only, because backupset holds native backups only and the
           automated snapshots are not per database. RDS runs no CHECKDB for the owner and the last good CHECKDB time is read as on
           SQL Server, so the CHECKDB fact falls through to be judged. */
        if (fact.Platform == DatabaseProtectionCollector.PlatformNativeOnly && !isCheckDb)
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

    /// <summary>"3 days 4 hours", "5 hours 10 minutes" or "42 minutes": the two largest units. The tool, the web panels, the Viewer and the alert detail say an age this way.</summary>
    public static string Describe(TimeSpan age)
    {
        /* #8572: a count of days reads "2,196 days", grouped like every other count in a reply. */
        string Unit(long n, string unit) => string.Create(CultureInfo.InvariantCulture, $"{n:N0} {unit}{(n == 1 ? "" : "s")}");
        if (age.TotalDays >= 1) return Unit(age.Days, "day") + (age.Hours > 0 ? " " + Unit(age.Hours, "hour") : "");
        if (age.TotalHours >= 1) return Unit(age.Hours, "hour") + (age.Minutes > 0 ? " " + Unit(age.Minutes, "minute") : "");
        return Unit((long)age.TotalMinutes, "minute");
    }

    public const string KindFullBackup = "full_backup";
    public const string KindLogBackup = "log_backup";
    public const string KindCheckDb = "checkdb";

    /// <summary>How old the newest capture may be and still be judged (#6873): three passes of the 60-minute collector. An older one is the
    /// collection-stopped alert's to report, so it neither fires nor clears an overdue alert and shows no chip.</summary>
    public static readonly TimeSpan CaptureFreshness = TimeSpan.FromHours(3);

    /// <summary>Every overdue fact of the capture, one entry per database and fact, in the order of the databases then full backup, log backup, CHECKDB.
    /// The only place that turns bands into "overdue", so the alerts, the fleet chip and the Viewer agree with the tool.</summary>
    public static IReadOnlyList<DatabaseProtectionOverdue> OverdueFacts(
        IReadOnlyList<DatabaseProtectionFact> facts, DateTime capturedAt, DatabaseProtectionThresholds? thresholds = null)
    {
        var t = thresholds ?? DatabaseProtectionThresholds.Default;
        return OverdueFacts(facts, capturedAt, _ => t);
    }

    /// <summary>The same judgment with the ages chosen per database (#7611): <paramref name="thresholdsFor"/> takes the database name and answers the ages
    /// in force for it (<see cref="DatabaseProtectionThresholdResolver.ForDatabase"/>). Each fact carries the age it was judged against.</summary>
    public static IReadOnlyList<DatabaseProtectionOverdue> OverdueFacts(
        IReadOnlyList<DatabaseProtectionFact> facts, DateTime capturedAt, Func<string, DatabaseProtectionThresholds> thresholdsFor)
    {
        ArgumentNullException.ThrowIfNull(facts);
        ArgumentNullException.ThrowIfNull(thresholdsFor);
        var list = new List<DatabaseProtectionOverdue>();
        foreach (var fact in facts)
        {
            var t = thresholdsFor(fact.DatabaseName);
            var band = Evaluate(fact, capturedAt, t);
            if (band.FullBackup.Overdue) list.Add(new(KindFullBackup, fact.DatabaseName, band.FullBackup.State, band.FullBackup.Age, t.FullBackup));
            if (band.LogBackup.Overdue) list.Add(new(KindLogBackup, fact.DatabaseName, band.LogBackup.State, band.LogBackup.Age, t.LogBackup));
            if (band.CheckDb.Overdue) list.Add(new(KindCheckDb, fact.DatabaseName, band.CheckDb.State, band.CheckDb.Age, t.CheckDb, band.CheckDb.Errors));
        }

        return list;
    }

    /// <summary>The chip's value: how many facts are overdue and on how many databases, for example <c>25 items on 11 databases</c> (#7416). Null when none,
    /// so a chip shows only when something is late. The Health Checks sentence counts databases, so this one says what it counts too.</summary>
    public static string? ChipValue(IReadOnlyList<DatabaseProtectionOverdue>? overdue) =>
        overdue is { Count: > 0 } ? CountText(overdue.Count, overdue.Select(o => o.Database).Distinct(StringComparer.OrdinalIgnoreCase).Count()) : null;

    /// <summary>"25 items on 11 databases", "1 item on 1 database".</summary>
    public static string CountText(int items, int databases) =>
        string.Create(CultureInfo.InvariantCulture, $"{items} {(items == 1 ? "item" : "items")} on {databases} {(databases == 1 ? "database" : "databases")}");

    /// <summary>The chip's detail: each overdue fact with its label, for example <c>Full backup: Sales (9 days old), CHECKDB: Sales (never checked)</c>.</summary>
    public static string ChipDetail(IReadOnlyList<DatabaseProtectionOverdue> overdue)
    {
        ArgumentNullException.ThrowIfNull(overdue);
        return string.Join(", ", overdue.Select(o => o.Label + ": " + o.Text));
    }

    /// <summary>The words for one overdue fact: the database and how late it is, the age spelled by <see cref="Describe"/> like everywhere else.</summary>
    public static string DescribeOverdue(DatabaseProtectionOverdue item)
    {
        ArgumentNullException.ThrowIfNull(item);
        string what;
        if (item.State == ErrorsFound && item.Age is { } errorAge)
        {
            var n = item.Errors ?? 0;
            what = string.Create(CultureInfo.InvariantCulture, $"CHECKDB found {n:N0} {(n == 1 ? "error" : "errors")}, {Describe(errorAge)} old");
        }
        else if (item.Age is { } age)
            what = Describe(age) + " old";
        else if (item.State == Never)
            what = "never checked";
        else
            what = item.Kind == KindCheckDb ? "no CHECKDB on record" : "no backup on record";
        return item.Database + " (" + what + ")";
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
        /* #8865 M3: a secondary is still excluded whole when its error log was not read (refused, an old row), so name it when one is. */
        else if (judged.Count == 0)
            text = bands.Any(b => b.Band.ExcludedReason == DetailSecondary)
                ? "No database is judged: every one is offline, restoring or a secondary whose error log was not read."
                : "No database is judged: every one is offline or restoring.";
        else if (overdueDatabases == 0)
            text = string.Create(CultureInfo.InvariantCulture, $"No database is overdue ({judged.Count} judged).");
        else
        {
            var parts = new List<string>();
            if (full.Count > 0) parts.Add("Full backup: " + string.Join(", ", full));
            if (log.Count > 0) parts.Add("Log backup: " + string.Join(", ", log));
            if (checkDb.Count > 0) parts.Add("CHECKDB: " + string.Join(", ", checkDb));
            text = string.Create(CultureInfo.InvariantCulture, $"{overdueDatabases} of {judged.Count} judged databases overdue. ") +
                   string.Join(". ", parts) + ".";
        }

        return new DatabaseProtectionSummary(bands.Count, judged.Count, bands.Count - judged.Count, overdueDatabases, checkDb, full, log, text);
    }
}
