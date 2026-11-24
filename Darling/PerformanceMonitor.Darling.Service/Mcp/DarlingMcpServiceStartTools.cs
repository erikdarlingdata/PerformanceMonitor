/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service's start history as an MCP read (#9415): the newest starts, each with the build it ran, the schema change it made, the migration
/// rungs it tried and its error and warning counts. The rows are <c>collect.service_starts</c> and <c>collect.service_start_rungs</c>, read through
/// <see cref="ServiceStartHistory.ReadAsync"/>. The same table gives the store trend reads their build markers (<see cref="ReadBuildChangesAsync"/>).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServiceStartTools
{
    /// <summary>The default page size.</summary>
    public const int DefaultLimit = 10;

    /// <summary>The most starts one answer lists.</summary>
    public const int MaxLimit = 100;

    /// <summary>Whether both start tables exist: a store below the rung that creates them has neither.</summary>
    public const string ShapeSql = @"
SELECT to_regclass('collect.service_starts') IS NOT NULL
   AND to_regclass('collect.service_start_rungs') IS NOT NULL";

    /// <summary>What a trend read says in place of its build markers when the store has no start history.</summary>
    public const string BuildChangesUnavailableNote =
        "The store has no service start history yet (it predates the start tables), so builds are not marked. The service migrates the store at its next start.";

    [McpServerTool(Name = "get_service_starts"), Description(
        "The monitoring SERVICE's own starts, newest first: for each, the build it ran, the schema version before and after, how long each " +
        "migration rung took, and the ERROR and WARN counts since the start. Use it to tell which build ran when and what a start did. " +
        "No server_name. <<GUIDE>> Reads collect.service_starts and collect.service_start_rungs, one row per start of a service host (a restart " +
        "with no build change is a row too). Each start: build (the full version string), commit, current (true for the newest start of its host), " +
        "process_started_at, store_ready_at (null when the start never reached the store), previous_stop_at (the host's previous start's last " +
        "flush: the stop after a clean stop, within a minute of it after a crash), last_flush_at (the running start flushes about every minute), " +
        "schema_version_before and schema_version_after (equal when no rung ran), error_count and warn_count, first_errors and first_warns (the " +
        "first 10 lines each, secrets masked, cut at 500 characters), at_10_minutes and at_30_minutes (the counts at start + 10 and + 30 minutes, " +
        "null until then), and rungs. A rung: version, name, attempt (a rung that failed and then applied has two rows), started_at, elapsed_ms, " +
        "outcome (applied or failed) and error_text for a failure. Counts cover the service's own log lines from the start of the process. " +
        "limit is the number of starts (default 10, max 100); service_host keeps one host's starts. status precondition means the store predates " +
        "the start tables; empty means no start is recorded for the filter.")]
    public static async Task<string> GetServiceStarts(
        NpgsqlDataSource postgres,
        [Description("How many starts, newest first. Default 10, max 100.")] int limit = DefaultLimit,
        [Description("Only this service host's starts. Omit for every host.")] string? service_host = null,
        CancellationToken cancellationToken = default)
    {
        var limitError = McpHelpers.ValidateTop(limit, MaxLimit);
        if (limitError != null)
        {
            return limitError;
        }

        var host = string.IsNullOrWhiteSpace(service_host) ? null : service_host.Trim();

        try
        {
            await using (var shape = postgres.CreateCommand(ShapeSql))
            {
                shape.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                if (!(bool)(await shape.ExecuteScalarAsync(cancellationToken))!)
                {
                    return McpHelpers.Status("precondition",
                        "This store has no service start history yet: it predates the start tables. The service migrates the store at its next start and records the start once the store is ready.");
                }
            }

            /* One more than asked, so the extra row proves the page was cut (the page contract). */
            var fetched = await ServiceStartHistory.ReadAsync(postgres, limit + 1, host, cancellationToken);
            if (fetched.Count == 0)
            {
                return McpHelpers.Status("empty", host is null
                    ? "No service start is recorded yet. The service records each start once its store is ready."
                    : $"No service start is recorded for service_host '{host}'. Omit service_host to list every host's starts.");
            }

            var (page, truncated) = McpHelpers.BoundPage(fetched, limit);
            return JsonSerializer.Serialize(new
            {
                limit,
                service_host = host,
                starts_returned = page.Count,
                truncated,
                starts = page.Select(Shape),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_service_starts", ex);
        }
    }

    /// <summary>
    /// The points since <paramref name="sinceUtc"/> where a service host's build changed, for the store trend reads (#9415): <c>List</c> holds one
    /// entry per change, oldest first, and is null with <c>Note</c> set when the store has no start history or the read failed. A failed marker
    /// read never fails the trend it rides on.
    /// </summary>
    internal static async Task<(IReadOnlyList<object>? List, string? Note)> ReadBuildChangesAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            var changes = await ServiceStartHistory.ReadBuildChangesAsync(postgres, sinceUtc, cancellationToken);
            return (changes.Select(c => (object)new
            {
                service_host = c.ServiceHost,
                at = Iso(c.StartedUtc),
                from_build = c.FromBuild,
                to_build = c.ToBuild,
            }).ToList(), null);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return (null, BuildChangesUnavailableNote);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "Build markers could not be read from the service start history; the trend is answered without them.");
            return (null, "The build markers could not be read just now; the trend itself is complete.");
        }
    }

    private static object Shape(ServiceStartRecord record)
    {
        var s = record.Start;
        return new
        {
            start_id = s.StartId,
            service_host = s.ServiceHost,
            build = s.ProductVersion,
            commit = s.CommitId,
            current = record.Current,
            process_started_at = Iso(s.ProcessStartedUtc),
            store_ready_at = Iso(s.StoreReadyUtc),
            previous_stop_at = Iso(s.PreviousStopUtc),
            last_flush_at = Iso(s.LastFlushUtc),
            schema_version_before = s.SchemaVersionBefore,
            schema_version_after = s.SchemaVersionAfter,
            error_count = s.ErrorCount,
            warn_count = s.WarnCount,
            at_10_minutes = new { error_count = s.Errors10Minutes, warn_count = s.Warns10Minutes },
            at_30_minutes = new { error_count = s.Errors30Minutes, warn_count = s.Warns30Minutes },
            first_errors = s.FirstErrors,
            first_warns = s.FirstWarns,
            rungs = record.Rungs.Select(r => new
            {
                version = r.Version,
                name = r.Name,
                attempt = r.Attempt,
                started_at = Iso(r.StartedUtc),
                elapsed_ms = r.ElapsedMilliseconds,
                outcome = r.Outcome,
                error_text = r.ErrorText,
            }),
        };
    }

    private static string? Iso(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture);
}
