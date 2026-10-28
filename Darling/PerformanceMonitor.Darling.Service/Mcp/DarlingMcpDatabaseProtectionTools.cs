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
    };

    /// <summary>"3 days 4 hours", "5 hours 10 minutes" or "42 minutes": the two largest units.</summary>
    internal static string Describe(TimeSpan age) => DatabaseProtectionBands.Describe(age);

    [McpServerTool(Name = "get_database_protection"), Description(
        "Per database, the last good CHECKDB, the last full backup and the last log backup, each with its age and a state word, and a server summary naming which databases are overdue on what. <<GUIDE>> " +
        "Thresholds: full backup 7 days, log backup 60 minutes, CHECKDB 14 days (shown in the answer). " +
        "Each fact's state: ok, overdue (older than the threshold), never (CHECKDB never passed) or none_in_window (no backup in the last 60 days), both of which count as overdue, " +
        "new_database (created less than the threshold ago, so not late yet), not_expected (a SIMPLE-recovery database expects no log backup), " +
        "platform_managed (Azure SQL Database and Managed Instance) or native_only (Amazon RDS): the age shows and is never overdue, " +
        "unknown (SQL Server 2017 before CU9 cannot answer CHECKDB) or unavailable (the backup read was refused, for example no VIEW DATABASE PERFORMANCE STATE on Azure SQL Database, which VIEW DATABASE STATE grants): both pass through and are never overdue, " +
        "excluded (the database is offline, restoring, a secondary or otherwise not online; excluded_reason says which). " +
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

            var thresholds = DatabaseProtectionThresholds.Default;
            var capturedAt = DateTime.SpecifyKind(read.NewestCapture!.Value, DateTimeKind.Utc);
            var banded = read.Databases
                .Select(d => (Fact: d, Band: DatabaseProtectionBands.Evaluate(d, capturedAt, thresholds)))
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
                databases_listed = listed.Count,
                databases = listed.Select(b => new
                {
                    database = b.Fact.DatabaseName,
                    state = b.Fact.StateDesc,
                    recovery_model = b.Fact.RecoveryModelDesc,
                    replica_role = b.Fact.ReplicaRole,
                    platform = b.Fact.Platform,
                    created = Format(b.Fact.CreateDate),
                    excluded_reason = b.Band.ExcludedReason,
                    checkdb = Fact(b.Fact.LastGoodCheckDbTime, b.Band.CheckDb, b.Fact.CheckDbState ?? ""),
                    full_backup = Fact(b.Fact.LastFullBackupTime, b.Band.FullBackup, b.Fact.FullBackupState ?? ""),
                    log_backup = Fact(b.Fact.LastLogBackupTime, b.Band.LogBackup, b.Fact.LogBackupState ?? ""),
                }),
                note = "All times are UTC. last_time is null when there is no time to show (never, none_in_window, unknown, unavailable). " +
                    "Backups are read from the last 60 days of msdb, so none_in_window means none in 60 days, not never. " +
                    "An excluded database and a platform-managed or native-only target are never overdue.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_database_protection", ex);
        }
    }
}
