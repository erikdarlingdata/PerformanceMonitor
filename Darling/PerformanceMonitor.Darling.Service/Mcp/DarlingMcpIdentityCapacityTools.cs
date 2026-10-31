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
/// #6875: <c>get_identity_capacity</c>, how full each identity column is and how long until it runs out. A STORED read of the
/// <c>identity_capacity</c> collector through <see cref="DarlingIdentityCapacityReader"/>. The rate and the days left are the pure
/// <see cref="IdentityCapacityMath.EstimateExhaustion"/> over the column's newest and oldest stored values in the window, so the Viewer
/// can say the same figures.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpIdentityCapacityTools
{
    /// <summary>The most columns one call lists (the totals above count every column).</summary>
    internal const int MaxListed = 200;

    /// <summary>The window the rate is judged over by default: seven days, so a quiet weekend and a busy weekday both count.</summary>
    internal const int DefaultHoursBack = 168;

    /// <summary>The longest window: the collector's 90-day retention.</summary>
    internal const int MaxHoursBack = 2160;

    private static string? Format(DateTime? t) =>
        t is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture) : null;

    [McpServerTool(Name = "get_identity_capacity"), Description(
        "How full each identity column is, and how long until it runs out. <<GUIDE>> " +
        "An identity column that reaches the end of its type stops every insert into its table. The identity_capacity collector (every 6 hours, per user database) keeps the 20 fullest columns above 10 percent used per database, plus one marker per database it scanned. " +
        "columns[] lists them fullest first: percent_used, band (ok under 70, warning from 70, critical from 90), values_remaining (inserts left, as text because bigint and decimal values exceed what a JSON number holds exactly), last_value, seed_value, increment_value, data_type. " +
        "Percent is of the values from the seed to the end the increment moves to (the type's maximum for a positive increment, its minimum for a negative one), not of the whole type range. " +
        "rate_per_day is identity values issued per day and days_to_exhaustion the days until values_remaining is issued at that rate. Both come from the column's newest stored value and its oldest stored value in the window (hours_back, default 168), and are null when the window holds one sample of the column, when the value did not move, or when it moved backwards (the column was reseeded). " +
        "columns[] holds at most limit columns (default and most 200), fullest first: truncated is true when more exist, and summary counts them all. databases[] lists each database scanned in the window and when. A scanned database with no column in columns[] has nothing above 10 percent: that is clean, not missing. An online database of the server (from its newest database config capture) with no scan in the window is listed in databases[] with scanned false and last_scanned_at (its last scan before the window, null when none): it is NOT clean, it is not yet scanned (the pass budget has not reached it yet) when skip_reason is null, and skipped on purpose when skip_reason is secondary, snapshot or no_access (scan_note says why). A database that is offline, restoring, system, vendor, left out by the server's database filter or left out of the identity_capacity collector's own database scope is in neither list. " +
        "Reads the identity_capacity collector, never a monitored server. Status words: empty (databases were scanned, none has a column above 10 percent and none is waiting to be scanned), unavailable (no database was scanned in the window: the collector has not run in it, or has not reached the database yet), precondition (the collector's latest run was refused or denied on the monitored server, with its own sentence), not_collected (the engine has no such data).")]
    public static Task<string> GetIdentityCapacity(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of scans ending at as_of (default 168). The rate compares each column's newest value with its oldest in this window.")] int hours_back = DefaultHoursBack,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum columns to list, fullest first. Default and most 200.")] int limit = MaxListed,
        CancellationToken cancellationToken = default) =>
        GetIdentityCapacity(postgres, server_name, hours_back, as_of, DatabaseFilter.One(database_name), limit, cancellationToken);

    /// <summary>
    /// <c>get_identity_capacity</c> over a SET of databases (<see cref="DatabaseFilter.All"/> = every database): the web server page's
    /// database filter (#5244) sends the chosen databases, and the MCP tool sends the one it was given.
    /// </summary>
    public static async Task<string> GetIdentityCapacity(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, DatabaseFilter databases, int limit,
        CancellationToken cancellationToken = default)
    {
        if (limit < 1 || limit > MaxListed)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be 1-{MaxListed}.");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, MaxHoursBack, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var read = await DarlingIdentityCapacityReader.ReadAsync(
                postgres, resolved.ServerId, databases, start, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceRaw);

            /* #7010: the window is a data window here (each column's rate runs from its oldest stored value in it), so a window that reaches past
               the store's identity_capacity data says so. The refused, denied and unmet-engine answers below say their own cause and stay bare;
               a failed probe costs the notice, never the answer. */
            Task<McpWindowNotice> NoticeAsync(bool emptyAnswer) => DarlingMcpWindowNotice.ReadAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "identity_capacity", resolved.ServerName, start, windowEnd, cancellationToken),
                start, windowEnd, "identity_capacity", emptyAnswer: emptyAnswer, cancellationToken: cancellationToken);

            var scope = databases.Describe();
            var where = scope is null ? "" : $" for database '{scope}'";
            var unscanned = read.Unscanned;
            var skipped = read.Skipped;
            if (!read.HasScan)
            {
                var unmet = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "identity_capacity", cancellationToken)
                    /* A refused or denied run says so in the monitored server's own words, as the Viewer's tab does. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "identity_capacity", cancellationToken);
                if (unmet is not null) return unmet;

                var noScan = await NoticeAsync(emptyAnswer: true);
                return DarlingMcpWindowNotice.FinishEmpty(McpHelpers.Status("unavailable",
                        $"No identity capacity scan{where} in the {hours_back}-hour window ending {Format(windowEnd)}" +
                        (unscanned.Count == 0 ? "" : $", and {unscanned.Count} database(s) not yet scanned ({string.Join(", ", unscanned.Take(10).Select(u => u.DatabaseName))}{(unscanned.Count > 10 ? ", ..." : "")})") +
                        (skipped.Count == 0 ? "" : $", and {skipped.Count} database(s) skipped on purpose ({string.Join("; ", skipped.Take(10).Select(u => $"{u.DatabaseName} - {u.Text}"))}{(skipped.Count > 10 ? "; ..." : "")})") +
                        ". The identity_capacity collector has not run in it, " +
                        "or has not reached the database yet (it runs every 6 hours, stops a pass after 5 minutes and starts the next one where the last stopped). " +
                        "A snapshot, a readable secondary or a database the monitoring login cannot enter is skipped on purpose and listed with its reason. " +
                        "A database that is offline, restoring, a system or vendor database, or left out by the server's database filter or the collector's database scope is never scanned.",
                        noScan.AsHints()), noScan);
            }

            var notice = await NoticeAsync(emptyAnswer: false);

            var worst = read.Columns.Count == 0 ? null : read.Columns[0];
            double? worstPercent = worst?.PercentUsed is double wp ? Math.Round(wp, 4) : null;
            double? soonestDays = read.Columns.Select(c => c.Exhaustion?.DaysLeft).Min() is double sd ? Math.Round(sd, 1) : null;
            var summary = new
            {
                databases_scanned = read.Databases.Count,
                databases_not_yet_scanned = unscanned.Count,
                databases_skipped = skipped.Count,
                oldest_scan = Format(read.Databases.Min(d => d.ScannedAt)),
                newest_scan = Format(read.Databases.Max(d => d.ScannedAt)),
                columns_above_10_percent = read.Columns.Count,
                critical = read.Columns.Count(c => c.Band == IdentityCapacityMath.BandCritical),
                warning = read.Columns.Count(c => c.Band == IdentityCapacityMath.BandWarning),
                worst_band = worst?.Band ?? IdentityCapacityMath.BandOk,
                worst_percent_used = worstPercent,
                soonest_exhaustion_days = soonestDays,
            };
            var databaseNodes = read.Databases.Select(d => new
            {
                database = d.DatabaseName,
                scanned = true,
                scanned_at = Format(d.ScannedAt),
                last_scanned_at = (string?)null,
                skip_reason = (string?)null,
                scan_note = (string?)null,
                columns_above_10_percent = read.Columns.Count(c => string.Equals(c.DatabaseName, d.DatabaseName, StringComparison.Ordinal)),
            }).Concat(unscanned.Select(u => new
            {
                database = u.DatabaseName,
                scanned = false,
                scanned_at = (string?)null,
                last_scanned_at = Format(u.LastScannedAt),
                skip_reason = (string?)null,
                scan_note = (string?)null,
                columns_above_10_percent = 0,
            })).Concat(skipped.Select(u => new
            {
                database = u.DatabaseName,
                scanned = false,
                scanned_at = (string?)null,
                last_scanned_at = Format(u.LastScannedAt),
                skip_reason = (string?)u.Reason,
                scan_note = (string?)u.Text,
                columns_above_10_percent = 0,
            })).ToList();

            if (read.Columns.Count == 0 && unscanned.Count == 0 && skipped.Count == 0)
            {
                return DarlingMcpWindowNotice.FinishEmpty(JsonSerializer.Serialize(new
                {
                    server = resolved.DisplayName,
                    status = "empty",
                    message = $"{read.Databases.Count} database(s){where} scanned in the window and none has an identity column above 10 percent used.",
                    as_of = Format(windowEnd),
                    hours_back,
                    hints = notice.AsHints(),
                    summary,
                    databases = databaseNodes,
                }, McpHelpers.JsonOptions), notice);
            }

            var listed = read.Columns.Take(limit).ToList();
            return DarlingMcpWindowNotice.Finish(JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                as_of = Format(windowEnd),
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                summary,
                databases = databaseNodes,
                columns_returned = listed.Count,
                truncated = listed.Count < read.Columns.Count,
                columns = listed.Select(c => new
                {
                    database = c.DatabaseName,
                    schema = c.SchemaName,
                    table = c.TableName,
                    column = c.ColumnName,
                    data_type = c.DataType,
                    type_precision = c.TypePrecision,
                    percent_used = c.PercentUsed is double cp ? Math.Round(cp, 4) : (double?)null,
                    band = c.Band,
                    values_remaining = c.ValuesRemaining?.ToString(CultureInfo.InvariantCulture),
                    last_value = c.LastValue?.ToString(CultureInfo.InvariantCulture),
                    seed_value = c.SeedValue?.ToString(CultureInfo.InvariantCulture),
                    increment_value = c.IncrementValue?.ToString(CultureInfo.InvariantCulture),
                    rate_per_day = c.Exhaustion?.StepsPerDay is double cr ? Math.Round(cr, 1) : (double?)null,
                    days_to_exhaustion = c.Exhaustion?.DaysLeft is double cd ? Math.Round(cd, 1) : (double?)null,
                    sampled_from = Format(c.Exhaustion is null ? null : c.FirstAt),
                    sampled_to = Format(c.Exhaustion is null ? null : c.CapturedAt),
                    scanned_at = Format(c.CapturedAt),
                }),
                note = "Times are UTC. values_remaining, last_value, seed_value and increment_value are text. rate_per_day is identity values issued per day (one step is one increment). " +
                       "days_to_exhaustion assumes the rate between the two samples holds, and is null with one sample, a value that did not move, or one that moved backwards (reseeded). " +
                       "A database in databases[] with columns_above_10_percent 0 was scanned and is clean. The band uses 70 and 90 percent. truncated is true when more columns exist than limit allowed: the summary counts them all, and database_name narrows the read.",
            }, McpHelpers.JsonOptions), notice);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_identity_capacity", ex);
        }
    }
}
