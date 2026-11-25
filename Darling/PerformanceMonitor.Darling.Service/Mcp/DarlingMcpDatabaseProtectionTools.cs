/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6873: <c>get_database_protection</c>, per database the last good CHECKDB, the last full backup and the last log backup, each with its
/// age and a state word, and a server summary of which databases are overdue on what. A STORED read of the <c>database_protection</c>
/// collector through <see cref="DarlingDatabaseProtectionReader"/>; the judgment is the pure <see cref="DatabaseProtectionBands"/>, so the
/// Viewer can say the same words.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpDatabaseProtectionTools
{
    private static string? Format(DateTime? t) =>
        t is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture) : null;

    private static object Fact(DateTime? lastTime, DatabaseProtectionFactBand band, string storedState) => new
    {
        state = band.State,
        overdue = band.Overdue,
        stored_state = storedState,
        last_time = Format(lastTime),
        age_minutes = band.Age is { } a ? (long)a.TotalMinutes : (long?)null,
        age = band.Age is { } t ? Describe(t) : null,
        threshold_minutes = (long)band.Threshold.TotalMinutes,
        detail = band.Detail,
    };

    /* #8865: the CHECKDB fact also says where its time came from, the options and error count of the error-log line, why it reads as it does (a
       backfill still running, a replica that is not readable) and, on an AG database, the boot-page time kept for reference. R11: a band with no
       source is the boot page, because that is the time the old path shows. last_time is the time the band judged (I7): the error-log check
       when the source is error_log, else the boot page, so it always agrees with age and state. */
    private static object CheckDbFact(DatabaseProtectionFact fact, DatabaseProtectionFactBand band) => new
    {
        state = band.State,
        overdue = band.Overdue,
        stored_state = fact.CheckDbState ?? "",
        last_time = Format(band.Source == DatabaseProtectionBands.SourceErrorLog ? fact.ErrorLogCheckDbTime : fact.LastGoodCheckDbTime),
        age_minutes = band.Age is { } a ? (long)a.TotalMinutes : (long?)null,
        age = band.Age is { } t ? Describe(t) : null,
        threshold_minutes = (long)band.Threshold.TotalMinutes,
        source = band.Source ?? DatabaseProtectionBands.SourceBootPage,
        options = band.Options,
        errors = band.Errors,
        detail = band.Detail,
        boot_page_time = Format(band.ReferenceTime),
    };

    /// <summary>#8865: what the collector said about the SQL Server error log read, taken from the first fact (it repeats on every row of a capture).
    /// <c>refused</c> reads <c>precondition</c>, as the other tools word a refused grant; an old capture with no state reads <c>not_applicable</c>.</summary>
    internal static object ErrorLogBlock(DatabaseProtectionFact? first)
    {
        var status = first?.ErrorLogState switch
        {
            DatabaseProtectionCollector.ErrorLogRead => "read",
            DatabaseProtectionCollector.ErrorLogFailed => "failed",
            DatabaseProtectionCollector.ErrorLogRefused => "precondition",
            _ => "not_applicable",
        };
        return new
        {
            status,
            message = first?.ErrorLogMessage,
            grant = first?.ErrorLogGrant,
            backfill = first?.ErrorLogBackfillComplete switch { true => "complete", false => "in_progress", _ => null },
            skipped_archives = first?.ErrorLogSkippedArchives,
            unparsed_lines = first?.ErrorLogUnparsedLines,
        };
    }

    /// <summary>One database as <c>get_database_protection</c> lists it: the tool and its tests build the reply through this one projection.</summary>
    internal static object DatabaseEntry(DatabaseProtectionFact fact, DatabaseProtectionBand band) => new
    {
        database = fact.DatabaseName,
        state = fact.StateDesc,
        recovery_model = fact.RecoveryModelDesc,
        replica_role = fact.ReplicaRole,
        platform = fact.Platform,
        created = Format(fact.CreateDate),
        excluded_reason = band.ExcludedReason,
        replica_readable = fact.ReplicaReadable,
        checkdb = CheckDbFact(fact, band.CheckDb),
        full_backup = Fact(fact.LastFullBackupTime, band.FullBackup, fact.FullBackupState ?? ""),
        log_backup = Fact(fact.LastLogBackupTime, band.LogBackup, fact.LogBackupState ?? ""),
    };

    /// <summary>"3 days 4 hours", "5 hours 10 minutes" or "42 minutes": the two largest units.</summary>
    internal static string Describe(TimeSpan age) => DatabaseProtectionBands.Describe(age);

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_database_protection"), Description(
        "Per database, the last good CHECKDB, the last full backup and the last log backup, each with its age and a state word, and a server summary naming which databases are overdue on what. <<GUIDE>> " +
        "Thresholds: full backup 7 days, log backup 60 minutes, CHECKDB 14 days (shown in the answer). " +
        "Each fact's state: ok, overdue (older than the threshold), never (CHECKDB never passed) or none_in_window (no backup in the last 60 days), both of which count as overdue, " +
        "new_database (created less than the threshold ago, so not late yet), not_expected (a SIMPLE-recovery database expects no log backup, and neither do master and model whatever their recovery model, with detail saying so), " +
        "platform_managed (Azure SQL Database and Managed Instance, all three facts) or native_only (Amazon RDS, the full and log backup facts only): the age shows and is never overdue, " +
        "unknown (SQL Server 2017 before CU9 cannot answer CHECKDB) or unavailable (the backup read was refused, for example no VIEW DATABASE PERFORMANCE STATE on Azure SQL Database, which VIEW DATABASE STATE grants): both pass through and are never overdue, " +
        "excluded (the database is offline, restoring or otherwise not online; excluded_reason says which; a secondary's full and log backup facts also read excluded, with detail secondary; when the error log was not read (refused, or an old capture) a secondary is still excluded whole, with excluded_reason secondary), " +
        "pending (an Availability Group database whose CHECKDB is not found yet while the older error logs are still being read: never overdue), " +
        "unknown with source error_log (an Availability Group database with no CHECKDB found while the error log holds CHECKDB lines that could not be read, which usually means the instance writes its error log in another language: never overdue, detail says so), " +
        "errors_found (the newest CHECKDB in the replica's own error log reported errors, so it is overdue whatever its age; errors counts them). An Amazon RDS database's CHECKDB is judged like any other. " +
        "CHECKDB is judged per replica: a database in an Availability Group is judged on its own SQL Server error log, so a secondary has its own CHECKDB state, and the boot-page time (shared by every replica) shows only as checkdb.boot_page_time for reference. " +
        "checkdb.source says where the time came from: error_log (the replica's own error log) or boot_page (the database boot page, which is what every database reads when the error log was not read); options and errors are what the error-log line said, and detail explains a state in words (a replica that is not readable, a backfill still running). replica_readable is false when a secondary refuses connections, so CHECKDB cannot run on it. " +
        "error_log at server level says how the error log read went: status read, failed, precondition (the login was refused; grant names what to grant) or not_applicable (an old capture, or a platform that is not read), backfill in_progress or complete, skipped_archives and unparsed_lines when the read could not use some of what it found (unparsed_lines is the newest read that found any, kept until a line parses: only English CHECKDB lines are parsed). " +
        "summary.text, overdue_full_backup, overdue_log_backup and overdue_checkdb name the overdue databases. overdue_only lists just the databases overdue on something. " +
        "Reads the database_protection collector (every 60 minutes), never a monitored server. Ages are measured at captured_at, the newest capture in the window. " +
        "Status words: unavailable (no capture in the window), precondition (the collector was refused, for example no SELECT on msdb.dbo.backupset), not_collected (the server's engine is not collected).")]
    public static async Task<string> GetDatabaseProtection(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of captures ending at as_of to look back for the newest one (default 24).")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("List only the databases overdue on at least one fact. Default false.")] bool overdue_only = false,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var read = await DarlingDatabaseProtectionReader.ReadAsync(
                postgres, resolved.ServerId, start, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceRaw);

            if (!read.HasCapture)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "database_protection", cancellationToken)
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "database_protection", cancellationToken)
                    ?? McpHelpers.Status("unavailable",
                        $"No database protection capture in the {hours_back}-hour window ending {Format(windowEnd)}. The database_protection collector has not run in it.");
            }

            /* #7611: the global ages and this server's overrides. A database is judged against its own (database override, server override, global
               value); `thresholds` below is the server-wide figure and each fact's threshold_minutes is the one its database was judged against. */
            var resolver = await DatabaseProtectionThresholdSettings.ReadAsync(postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, cancellationToken);
            var thresholds = resolver.For(null);
            var capturedAt = DateTime.SpecifyKind(read.NewestCapture!.Value, DateTimeKind.Utc);
            var banded = read.Databases
                .Select(d => (Fact: d, Band: DatabaseProtectionBands.Evaluate(d, capturedAt, resolver.For(d.DatabaseName))))
                .ToList();
            var summary = DatabaseProtectionBands.Summarize(banded);
            var listed = overdue_only ? banded.Where(b => b.Band.AnyOverdue).ToList() : banded;

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                as_of = Format(windowEnd),
                hours_back,
                captured_at = Format(capturedAt),
                thresholds = new
                {
                    full_backup_days = thresholds.FullBackup.TotalDays,
                    log_backup_minutes = (long)thresholds.LogBackup.TotalMinutes,
                    checkdb_days = thresholds.CheckDb.TotalDays,
                    full_backup_hours = (long)thresholds.FullBackup.TotalHours,
                    checkdb_hours = (long)thresholds.CheckDb.TotalHours,
                    global_full_backup_hours = resolver.Global.BackupHours,
                    global_log_backup_minutes = resolver.Global.LogBackupMinutes,
                    global_checkdb_hours = resolver.Global.CheckDbHours,
                    databases_with_own_thresholds = read.Databases.Where(d => resolver.For(d.DatabaseName) != thresholds).Select(d => d.DatabaseName).ToList(),
                },
                summary = new
                {
                    databases = summary.Databases,
                    assessed = summary.Assessed,
                    excluded = summary.Excluded,
                    overdue_databases = summary.OverdueDatabases,
                    overdue_full_backup = summary.OverdueFullBackup,
                    overdue_log_backup = summary.OverdueLogBackup,
                    overdue_checkdb = summary.OverdueCheckDb,
                    text = summary.Text,
                },
                error_log = ErrorLogBlock(read.Databases.Count > 0 ? read.Databases[0] : null),
                databases_listed = listed.Count,
                databases = listed.Select(b => DatabaseEntry(b.Fact, b.Band)),
                note = "All times are UTC. A threshold is the global setting (get_alert_settings) unless a server or database override (set_alert_threshold_override) replaces it; each fact's threshold_minutes is the one its database was judged against. last_time is null when there is no time to show (never, none_in_window, unknown, unavailable). " +
                    "Backups are read from the last 60 days of msdb, so none_in_window means none in 60 days, not never. " +
                    "An excluded database and a platform-managed target are never overdue. A native-only (Amazon RDS) target's backups are not judged, but its CHECKDB is. A secondary's backups are not judged (they run on the primary), but its CHECKDB is, from its own error log.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_database_protection", ex);
        }
    }
}
