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
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service's start history as an MCP read (#9415): the newest starts, each with the build it ran, the schema change it made, the migration
/// rungs it tried and its error and warning counts. The rows are <c>collect.service_starts</c> and <c>collect.service_start_rungs</c>, read through
/// <see cref="ServiceStartHistory.ReadAsync(NpgsqlDataSource, int, string?, CancellationToken)"/>. With <c>rung</c> it lists the objects that rung created, changed or dropped
/// (#9438), from <c>collect.service_start_rung_objects</c> through <see cref="ServiceStartRungObjects.ReadRungAsync"/>. The same table gives the store trend reads their build markers (<see cref="ReadBuildChangesAsync"/>).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServiceStartTools
{
    /// <summary>The default page size.</summary>
    public const int DefaultLimit = 10;

    /// <summary>The most starts one answer lists.</summary>
    public const int MaxLimit = 100;

    /// <summary>Whether the three start tables exist: a store below the rung that creates them lacks them (V307 made two, V311 the object table).</summary>
    public const string ShapeSql = @"
SELECT to_regclass('collect.service_starts') IS NOT NULL
   AND to_regclass('collect.service_start_rungs') IS NOT NULL
   AND to_regclass('collect.service_start_rung_objects') IS NOT NULL";

    /// <summary>What the read says in place of each start's upgrade run when the store has no <c>collect.upgrade_runs</c> (#9902).</summary>
    public const string UpgradeRunsUnavailableNote =
        "The store has no upgrade run history (it predates collect.upgrade_runs), so no start names the upgrade run before it. The service migrates the store at its next start.";

    /// <summary>What the read says when the upgrade runs could not be read just now (#9902). The starts are complete.</summary>
    public const string UpgradeRunsUnreadableNote =
        "The upgrade runs could not be read just now, so no start names the upgrade run before it. The starts themselves are complete.";

    /// <summary>What a trend read says in place of its build markers when the store has no start history.</summary>
    public const string BuildChangesUnavailableNote =
        "The store has no service start history yet (it predates the start tables), so builds are not marked. The service migrates the store at its next start.";

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_service_starts"), Description(
        "The monitoring SERVICE's own starts, newest first: for each, the build it ran, the schema version before and after, how long each " +
        "migration rung took, and the ERROR and WARN counts since the start. Use it to tell which build ran when and what a start did. " +
        "No server_name. <<GUIDE>> Reads collect.service_starts and collect.service_start_rungs, one row per start of a service host (a restart " +
        "with no build change is a row too). Each start: build (the full version string), commit, current (true for the newest start of its host), " +
        "process_started_at, store_ready_at (null when the start never reached the store), previous_stop_at (the host's previous start's last " +
        "flush: the stop after a clean stop, within a minute of it after a crash), last_flush_at (the running start flushes about every minute), " +
        "schema_version_before and schema_version_after (equal when no rung ran), error_count and warn_count, first_errors and first_warns (the " +
        "first 5 lines each, secrets masked, cut at 160 characters, log_lines_truncated true when a line was dropped or cut), at_10_minutes and at_30_minutes (the counts at start + 10 and + 30 minutes, " +
        "null until then), upgrade_run and rungs. upgrade_run (#9902) names the upgrade run before the start, from collect.upgrade_runs: the newest run that began at or before the start's process_started_at, so a start reads next to the upgrade that came before it (null when no run began that early; runs carry no service host, so on a store with several hosts a start can name a run another host made). It has run_id, state, recorded_state, target_build, started_at, ended_at (null until the run's file said it ended) and start_on_target_build (true when this start runs the run's target build, false when it runs another build, null when the run recorded no target build). recorded_state is what the store holds, which is what the upgrade script's file last said, and state is what the read makes of it: a run stored as in_progress reads as ended_without_result once the service has started on its target build after the run began (the script's last write never landed, so the run ended with no recorded result) and as superseded once a newer run exists, and ended_without_result wins when both hold; a run with no later start on its build and no newer run reads in_progress, and succeeded, failed and verified read as stored. A run mid-upgrade reads ended_without_result from the new build's start until the script's last write is recorded, which takes at most one five-minute pass. When the store has no collect.upgrade_runs, upgrade_run is null on every start and upgrade_runs_note says why. A rung: version, name, attempt (a rung that failed and then applied has two rows), started_at, elapsed_ms, " +
        "outcome (applied or failed), objects (how many tables, columns, indexes, constraints, views, functions, hypertables and policies the rung created, changed or dropped, or the word fresh_install when the start's ladder began on an empty store, which records no objects) and error_text for a failure (cut at 160 characters). A start lists at most 10 rungs, the failed ones first and then the slowest, with rungs_total and rungs_omitted saying how many it tried and how many are not listed, and rungs_truncated true when any rung was left out or its text cut. Counts cover the service's own log lines from the start of the process. " +
        "limit is the number of starts (default 10, max 100), and a reply too big for the response budget leaves the oldest starts off whole (truncated and budget_truncated true); service_host keeps one host's starts, and hours_back keeps the starts that began in the last that many hours (1 to 9600). " +
        "Pass rung (a schema version, 1 to the highest this build knows) to list what that rung changed instead: each start that applied it, newest first, with objects (schema, object, column for a column row, kind of table, column, index, constraint, view, function, hypertable or policy, and action of create, alter or drop; alter is a column type, nullability or default, an index or constraint definition, a view or function body or a policy setting that changed; a constraint row names the table and the constraint) and objects_total, objects_omitted and objects_truncated (a start lists at most 40). " +
        "A column or index of a table the same rung created or dropped is not listed, the table row says it. Only a start that applied the rung to a store that already had a version records objects. status precondition means the store predates " +
        "the start tables; empty means no start (or, with rung, no object row) is recorded for the filter.")]
    public static async Task<string> GetServiceStarts(
        NpgsqlDataSource postgres,
        [Description("How many starts, newest first. Default 10, max 100.")] int limit = DefaultLimit,
        [Description("Only this service host's starts. Omit for every host.")] string? service_host = null,
        [Description("List the objects this migration rung (a schema version) created, changed or dropped, for each start that applied it. Omit for the starts themselves.")] int? rung = null,
        [Description("Only the starts that began in the last this many hours (1 to 9600). Omit for every start.")] int? hours_back = null,
        CancellationToken cancellationToken = default)
    {
        var limitError = McpHelpers.ValidateTop(limit, MaxLimit);
        if (limitError != null)
        {
            return limitError;
        }

        if (rung is { } rungValue && (rungValue < 1 || rungValue > StorageVersion.SchemaVersion))
        {
            return McpHelpers.Refusal("rung", $"Invalid rung value '{rungValue}'. Must be a schema version from 1 to {StorageVersion.SchemaVersion}.");
        }

        if (hours_back is { } hoursValue)
        {
            var hoursError = McpHelpers.ValidateHoursBack(hoursValue, MaxHoursBack);
            if (hoursError != null)
            {
                return hoursError;
            }
        }

        DateTime? sinceUtc = hours_back is { } hours ? DateTime.UtcNow.AddHours(-hours) : null;

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

            if (rung is { } rungVersion)
            {
                return await ReadRungObjectsAsync(postgres, rungVersion, limit, host, hours_back, sinceUtc, cancellationToken);
            }

            /* One more than asked, so the extra row proves the page was cut (the page contract). */
            var fetched = await ServiceStartHistory.ReadAsync(postgres, limit + 1, host, sinceUtc, cancellationToken);
            if (fetched.Count == 0)
            {
                return McpHelpers.Status("empty", host is null && hours_back is null
                    ? "No service start is recorded yet. The service records each start once its store is ready."
                    : $"No service start is recorded for this filter{FilterText(host, hours_back)}. Omit service_host and hours_back to list every start.");
            }

            var (page, truncated) = McpHelpers.BoundPage(fetched, limit);
            var (upgradeRuns, upgradeRunsNote) = await ReadUpgradeRunsAsync(postgres, cancellationToken);

            /* The newest starts that fit the response budget: starts are left off the end whole, so `truncated` is true for the limit or the
               budget, and budget_truncated is the budget alone. Each start is also cut inside (rungs and log lines), and says so on its own row. */
            string Reply(int kept) => JsonSerializer.Serialize(new
            {
                limit,
                service_host = host,
                hours_back,
                starts_returned = kept,
                truncated = truncated || kept < page.Count,
                budget_truncated = kept < page.Count,
                budget_note = kept < page.Count
                    ? McpResponseBudget.PageCutNote(page.Count, kept, "newest first", "service starts",
                        "Pass a smaller limit, or service_host to read one host's starts.")
                    : null,
                upgrade_runs_note = upgradeRunsNote,
                starts = page.Take(kept).Select(r => Shape(r, upgradeRuns)),
            }, McpHelpers.JsonOptions);

            return McpResponseBudget.FitPrefix(page.Count, Reply).Json;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_service_starts", ex);
        }
    }

    /// <summary>The most hours back <c>hours_back</c> may reach: the days the start history is kept, as hours.</summary>
    internal const int MaxHoursBack = ServiceStartHistory.RetentionDays * 24;

    /// <summary>The most objects one start lists for a rung. A rung that touches more says so (objects_truncated), and objects_total gives the count.</summary>
    internal const int MaxObjectsPerRung = 40;

    private static string FilterText(string? host, int? hoursBack) =>
        (host is null ? string.Empty : $" (service_host '{host}')") + (hoursBack is null ? string.Empty : $" (started in the last {hoursBack} hours)");

    /// <summary>The objects one rung created, changed or dropped, for each start that applied it (#9438).</summary>
    private static async Task<string> ReadRungObjectsAsync(
        NpgsqlDataSource postgres, int rung, int limit, string? host, int? hoursBack, DateTime? sinceUtc, CancellationToken cancellationToken)
    {
        /* One more than asked, so the extra row proves the page was cut (the page contract). */
        var fetched = await ServiceStartRungObjects.ReadRungAsync(postgres, rung, limit + 1, host, sinceUtc, cancellationToken);
        if (fetched.Count == 0)
        {
            return McpHelpers.Status("empty",
                $"No object row is recorded for rung {rung}{FilterText(host, hoursBack)}. A rung that changed no table, column, index, constraint, view, function, hypertable or policy has none, " +
                "and a start whose ladder began on an empty store records none: its rung rows say objects is fresh_install. Read the starts without rung to see which starts applied the rung.");
        }

        var (page, truncated) = McpHelpers.BoundPage(fetched, limit);

        string Reply(int kept) => JsonSerializer.Serialize(new
        {
            rung,
            limit,
            service_host = host,
            hours_back = hoursBack,
            starts_returned = kept,
            truncated = truncated || kept < page.Count,
            budget_truncated = kept < page.Count,
            budget_note = kept < page.Count
                ? McpResponseBudget.PageCutNote(page.Count, kept, "newest first", "service starts",
                    "Pass a smaller limit, or service_host to read one host's starts.")
                : null,
            starts = page.Take(kept).Select(l => new
            {
                start_id = l.StartId,
                service_host = l.ServiceHost,
                build = l.ProductVersion,
                process_started_at = Iso(l.StartedUtc),
                rung_name = l.RungName,
                attempt = l.Attempt,
                objects_total = l.Objects.Count,
                objects_omitted = Math.Max(l.Objects.Count - MaxObjectsPerRung, 0),
                objects_truncated = l.Objects.Count > MaxObjectsPerRung,
                objects = l.Objects.Take(MaxObjectsPerRung).Select(o => new
                {
                    schema = o.Schema,
                    @object = o.ObjectName,
                    column = o.Column,
                    kind = o.Kind,
                    action = o.Action,
                }),
            }),
        }, McpHelpers.JsonOptions);

        return McpResponseBudget.FitPrefix(page.Count, Reply).Json;
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

    /// <summary>
    /// The upgrade runs the starts are paired with (#9902), newest first, or null with the note that says why there are none to pair: the store predates
    /// <c>collect.upgrade_runs</c>, or the read failed. A failed run read never fails the starts it rides on.
    /// </summary>
    private static async Task<(IReadOnlyList<UpgradeRunRead>? Runs, string? Note)> ReadUpgradeRunsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        try
        {
            await using (var shape = postgres.CreateCommand(UpgradeRuns.ExistsSql))
            {
                shape.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                if (!(bool)(await shape.ExecuteScalarAsync(cancellationToken))!)
                {
                    return (null, UpgradeRunsUnavailableNote);
                }
            }

            return (await UpgradeRuns.ReadAsync(postgres, cancellationToken), null);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return (null, UpgradeRunsUnavailableNote);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, UpgradeRunsUnreadableNote);
        }
    }

    /// <summary>The upgrade run before a start, as the start's <c>upgrade_run</c> object, or null when no run began at or before the start or the runs were not read.</summary>
    private static object? ShapeUpgradeRun(ServiceStartRow start, IReadOnlyList<UpgradeRunRead>? runs)
    {
        var run = runs is null ? null : UpgradeRuns.RunBefore(runs, start.ProcessStartedUtc);
        return run is null
            ? null
            : new
            {
                run_id = run.RunId,
                state = run.State,
                recorded_state = run.RecordedState,
                target_build = run.TargetBuild,
                started_at = Iso(run.StartedUtc),
                ended_at = Iso(run.EndedUtc),
                start_on_target_build = UpgradeRuns.StartIsOnTarget(run.TargetBuild, start.ProductVersion),
            };
    }

    private static object Shape(ServiceStartRecord record, IReadOnlyList<UpgradeRunRead>? upgradeRuns)
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
            first_errors = ShownLines(s.FirstErrors),
            first_warns = ShownLines(s.FirstWarns),
            log_lines_truncated = LinesCut(s.FirstErrors) || LinesCut(s.FirstWarns),
            upgrade_run = ShapeUpgradeRun(s, upgradeRuns),
            rungs_total = record.Rungs.Count,
            rungs_omitted = record.Rungs.Count - ShownRungs(record.Rungs).Count,
            rungs_truncated = record.Rungs.Count > MaxRungsPerStart || ShownRungs(record.Rungs).Any(r => r.ErrorText is { Length: > MaxLineChars }),
            rungs = ShownRungs(record.Rungs).Select(r => new
            {
                version = r.Version,
                name = r.Name,
                attempt = r.Attempt,
                started_at = Iso(r.StartedUtc),
                elapsed_ms = r.ElapsedMilliseconds,
                outcome = r.Outcome,
                objects = r.FreshInstall ? (object)MigrationObjectAction.FreshInstall : r.Objects,
                error_text = r.ErrorText is { Length: > MaxLineChars } ? r.ErrorText[..MaxLineChars] : r.ErrorText,
            }),
        };
    }

    /// <summary>The most rungs one start lists. A start on a fresh store tries every rung of the ladder, which alone would put one answer over the response budget.</summary>
    internal const int MaxRungsPerStart = 10;

    /// <summary>The rungs a start lists, in their stored order: the failed rungs first in line for a place, then the slowest of the rest, up to <see cref="MaxRungsPerStart"/>.</summary>
    private static IReadOnlyList<ServiceStartRung> ShownRungs(IReadOnlyList<ServiceStartRung> rungs)
    {
        if (rungs.Count <= MaxRungsPerStart)
        {
            return rungs;
        }

        var keep = rungs
            .Select((r, i) => (Rung: r, Index: i))
            .OrderBy(x => x.Rung.Outcome == MigrationRungOutcome.Failed ? 0 : 1)
            .ThenByDescending(x => x.Rung.ElapsedMilliseconds)
            .Take(MaxRungsPerStart)
            .Select(x => x.Index)
            .ToHashSet();
        return rungs.Where((_, i) => keep.Contains(i)).ToList();
    }

    /// <summary>The most kept log lines one start lists for each of errors and warnings, and the longest each is shown. The store keeps 10 lines of 500 characters, which for ten starts alone passes the response budget.</summary>
    internal const int MaxLinesShown = 5;

    internal const int MaxLineChars = 160;

    /// <summary>Whether listing these lines drops a line or shortens one.</summary>
    private static bool LinesCut(IReadOnlyList<string> lines) => lines.Count > MaxLinesShown || lines.Any(l => l.Length > MaxLineChars);

    private static List<string> ShownLines(IReadOnlyList<string> lines) =>
        lines.Take(MaxLinesShown).Select(l => l.Length <= MaxLineChars ? l : l[..MaxLineChars]).ToList();

    private static string? Iso(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture);
}
