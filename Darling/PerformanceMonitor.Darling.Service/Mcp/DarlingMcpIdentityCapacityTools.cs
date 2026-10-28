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
        "columns[] holds at most limit columns (default and most 200), fullest first: truncated is true when more exist, and summary counts them all. databases[] lists each database scanned in the window and when. A scanned database with no column in columns[] has nothing above 10 percent: that is clean, not missing. A database absent from databases[] was not scanned in the window (not yet reached, or skipped on purpose: offline, restoring, snapshot, readable secondary, system, vendor, or left out by the server's database filter). " +
        "Reads the identity_capacity collector, never a monitored server. Status words: empty (databases were scanned and none has a column above 10 percent), unavailable (no database was scanned in the window: the collector has not run in it, or has not reached the database yet).")]
    public static async Task<string> GetIdentityCapacity(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of scans ending at as_of (default 168). The rate compares each column's newest value with its oldest in this window.")] int hours_back = DefaultHoursBack,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum columns to list, fullest first. Default and most 200.")] int limit = MaxListed,
        CancellationToken cancellationToken = default)
    {
        if (limit < 1 || limit > MaxListed)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be 1-{MaxListed}.");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, MaxHoursBack, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);
        var databases = DatabaseFilter.One(database_name);

        try
        {
            var read = await DarlingIdentityCapacityReader.ReadAsync(
                postgres, resolved.ServerId, databases, start, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceRaw);

            var scope = databases.Describe();
            var where = scope is null ? "" : $" for database '{scope}'";
            if (!read.HasScan)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "identity_capacity", cancellationToken)
                    ?? McpHelpers.Status("unavailable",
                        $"No identity capacity scan{where} in the {hours_back}-hour window ending {Format(windowEnd)}. The identity_capacity collector has not run in it, " +
                        "or has not reached the database yet (it runs every 6 hours, stops a pass after 5 minutes and starts the next one where the last stopped). " +
                        "A database that is offline, restoring, a snapshot, a readable secondary, a system or vendor database, or left out by the server's database filter is never scanned.");
            }

            var worst = read.Columns.Count == 0 ? null : read.Columns[0];
            double? worstPercent = worst?.PercentUsed is double wp ? Math.Round(wp, 4) : null;
            double? soonestDays = read.Columns.Select(c => c.Exhaustion?.DaysLeft).Min() is double sd ? Math.Round(sd, 1) : null;
            var summary = new
            {
                databases_scanned = read.Databases.Count,
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
                columns_above_10_percent = read.Columns.Count(c => string.Equals(c.DatabaseName, d.DatabaseName, StringComparison.Ordinal)),
            }).ToList();

            if (read.Columns.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.DisplayName,
                    status = "empty",
                    message = $"{read.Databases.Count} database(s){where} scanned in the window and none has an identity column above 10 percent used.",
                    as_of = Format(windowEnd),
                    hours_back,
                    summary,
                    databases = databaseNodes,
                }, McpHelpers.JsonOptions);
            }

            var listed = read.Columns.Take(limit).ToList();
            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                as_of = Format(windowEnd),
                hours_back,
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
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_identity_capacity", ex);
        }
    }
}
