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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The database-state expectation tools (#6397): <c>get_database_state_expectations</c> reads, and
/// <c>set_database_state_expected</c> sets or clears, the per-(server, database) expected state the Database State alert compares
/// against, the same thing the Viewer's override editor edits. Both run <see cref="DatabaseStateExpectedStore"/>, the one copy of the SQL
/// the Viewer also runs. The write is served only with <c>mcp.allowWrites</c> and is on
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>: it can opt a database out of the alert (<c>IGNORE</c>), so an agent working an
/// alert never calls it, the same ruling as the other config writes that silence alerting. The read writes nothing (the Viewer's editor
/// also seeds baselines on open; the service seeds them in its own sweeps).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpDatabaseStateTools
{
    private const string ClearToken = "CLEAR";
    private const string IgnoreToken = "IGNORE";
    private const int MaxRows = 500;

    /* The engine half of the collector's gate (Azure SQL Database) is answered before this is used, so the one candidate left is the
       operator having switched the collector off in this server's schedule. */
    private const string GateCandidates =
        "Possible cause: the database_states collector is switched off in this server's collector schedule; get_collector_schedules shows it.";

    [ReadsLatestSnapshot(LatestSnapshotShape.Stamped)]
    [McpServerTool(Name = "get_database_state_expectations"), Description(
        "Lists one server's databases with the state each is in now and the state the Database State alert expects it to be in: expected_state, whether a person set it (is_user_override), and status (as_expected, deviating, ignored, or pending when no baseline is learned yet). The Viewer's database state override editor shows the same. " +
        "<<GUIDE>>" +
        "Reads the latest collected snapshot for the server and the expectations stored beside it; it writes nothing. LATEST IS A TIME: current_state and status describe the newest snapshot, not now - captured_at is the instant it was collected, so a deviating or as_expected verdict is as of that stamp. database_name narrows the list to one database. " +
        "At most 500 databases are returned (truncated says when more exist). allowed_states lists the values set_database_state_expected accepts. " +
        "An expected_state of (ignore) means the alert never fires for that database. A database with no expectation is pending: the service learns a baseline from its next observation, " +
        "unless the state is an integrity or transient one (SUSPECT, RECOVERY_PENDING, EMERGENCY, RESTORING, RECOVERING), which stays pending. " +
        "Change one with set_database_state_expected.")]
    public static async Task<string> GetDatabaseStateExpectations(
        NpgsqlDataSource postgres,
        [Description("Server name (the name list_servers shows), or a unique part of it.")] string server_name,
        [Description("Optional: only this database (exact name).")] string? database_name = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error is not null)
            {
                return error;
            }

            var rows = await DatabaseStateExpectedStore.ReadAsync(postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, cancellationToken);
            if (rows.Count == 0)
            {
                /* No snapshot exists for this server at all, so the database_name filter has nothing to say yet. A PostgreSQL target or an
                   Azure SQL Database never writes one (not_collected, permanent). A SQL Server target whose collector ran and recorded a
                   fault gets that fault; one whose collector is switched off (never invoked while the server collects normally) gets the
                   gated-off answer, called AFTER StatusAsync as the other reads do; any other SQL Server target has not been collected
                   yet (unavailable). */
                var noSnapshot = await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.DisplayName, "database_states", cancellationToken)
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.DisplayName, "database_states", cancellationToken)
                    ?? await DarlingRuntimePrecondition.GatedOffStatusAsync(
                        postgres, resolved.ServerId, resolved.DisplayName, "database_states", GateCandidates, cancellationToken);
                return noSnapshot ?? DarlingMcpAlertTools.Outcome("unavailable",
                    $"No database state has been collected for server '{resolved.DisplayName}' yet. The database_states collector writes it every minute by default on a SQL Server target, " +
                    "so if this server has been monitored for more than a few minutes, check collection health (get_collection_health).");
            }

            if (!string.IsNullOrWhiteSpace(database_name))
            {
                rows = rows.Where(r => string.Equals(r.DatabaseName, database_name, StringComparison.Ordinal)).ToList();
                if (rows.Count == 0)
                {
                    return DarlingMcpAlertTools.Outcome("empty",
                        $"The newest database state snapshot for server '{resolved.DisplayName}' does not list a database named '{database_name}'. " +
                        "A database created, renamed or dropped since that snapshot is not in it.");
                }
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                server_id = resolved.ServerId,
                /* #6545: the instant of the snapshot current_state and status were read from (the census's one stamp spelling). Every
                   row comes from the same snapshot, so the first row's stamp is the snapshot's. */
                captured_at = DateTime.SpecifyKind(rows[0].CapturedAt, DateTimeKind.Utc).ToString("o"),
                allowed_states = AllowedStates(),
                truncated = rows.Count > MaxRows,
                databases = rows.Take(MaxRows).Select(r => new
                {
                    database_name = r.DatabaseName,
                    current_state = r.CurrentState,
                    expected_state = r.ExpectedState,
                    is_user_override = r.ExpectedState is not null && r.IsUserOverride,
                    status = StatusOf(r),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_database_state_expectations", ex);
        }
    }

    [McpServerTool(Name = "set_database_state_expected"), Description(
        "Sets or clears the state the Database State alert expects one database to be in, the same override the Viewer's editor sets: expected_state is a state (ONLINE, OFFLINE, RESTORING, RECOVERING, RECOVERY_PENDING, SUSPECT, EMERGENCY, STANDBY), IGNORE (never alert for this database) or CLEAR (remove the expectation). Changes shared alert configuration. Returns previous and the undo call." +
        "<<GUIDE>>" +
        "Writes only config.database_state_expected, one row, through the store code the Viewer uses; the service sees it on its next alert pass. " +
        "The database must be in the server's latest collected snapshot (get_database_state_expectations lists them). A state outside the list is refused with the allowed values. " +
        "A set is an override: the service never replaces it with a learned baseline. CLEAR deletes the row, so the database is pending again and the service learns a baseline from its next observation " +
        "(an integrity or transient state is not learned and stays pending). " +
        "Answer: {status:\"set\"|\"cleared\"|\"unchanged\", previous: {expected_state, is_user_override} or null, undo: {tool, arguments}}. " +
        "Undo of a set over no expectation is CLEAR; over an expectation, the earlier state (an earlier learned baseline comes back as an override, undo_note says so). Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static async Task<string> SetDatabaseStateExpected(
        NpgsqlDataSource postgres,
        [Description("Server name (the name list_servers shows), or a unique part of it.")] string server_name,
        [Description("The database's exact name, as get_database_state_expectations shows it.")] string database_name,
        [Description("ONLINE, OFFLINE, RESTORING, RECOVERING, RECOVERY_PENDING, SUSPECT, EMERGENCY, STANDBY, IGNORE, or CLEAR.")] string expected_state,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(database_name))
            {
                return DarlingMcpAlertTools.Outcome("invalid", "database_name is required. Nothing was changed.");
            }

            if (string.IsNullOrWhiteSpace(expected_state))
            {
                return DarlingMcpAlertTools.Outcome("invalid", "expected_state is required: one of " + string.Join(", ", AllowedStates()) + ". Nothing was changed.");
            }

            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error is not null)
            {
                return error;
            }

            var rows = await DatabaseStateExpectedStore.ReadAsync(postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, cancellationToken);
            var current = rows.Find(r => string.Equals(r.DatabaseName, database_name, StringComparison.Ordinal));
            if (current is null)
            {
                return DarlingMcpAlertTools.Outcome("invalid",
                    $"Database '{database_name}' is not in the latest collected snapshot for server '{resolved.DisplayName}', so no expectation is set. " +
                    "get_database_state_expectations lists the databases. Nothing was changed.");
            }

            var wanted = expected_state.Trim();
            var upper = wanted.ToUpperInvariant();
            var clear = upper == ClearToken;
            string? state = null;
            if (!clear)
            {
                state = upper == IgnoreToken || wanted == DatabaseStateTokens.Ignore
                    ? DatabaseStateTokens.Ignore
                    : DatabaseStateExpectedStore.StandardStates.FirstOrDefault(s => s == upper)
                        ?? (string.Equals(current.CurrentState, wanted, StringComparison.Ordinal) ? current.CurrentState : null);
                if (state is null)
                {
                    return DarlingMcpAlertTools.Outcome("invalid",
                        $"expected_state '{wanted}' is not accepted. Allowed: {string.Join(", ", AllowedStates())}. Nothing was changed.");
                }
            }

            var previous = current.ExpectedState is null ? null : new Previous(current.ExpectedState, current.IsUserOverride);
            string status;
            if (clear)
            {
                var removed = await DatabaseStateExpectedStore.ClearAsync(postgres, resolved.ServerId, database_name, McpCommandDeadlines.ReadSeconds, cancellationToken);
                previous = removed is { } r ? new Previous(r.ExpectedState, r.IsUserOverride) : null;
                status = previous is null ? "unchanged" : "cleared";
            }
            else if (previous is { IsUserOverride: true } && previous.ExpectedState == state)
            {
                status = "unchanged";
            }
            else
            {
                await DatabaseStateExpectedStore.SetAsync(postgres, resolved.ServerId, database_name, state!, McpCommandDeadlines.ReadSeconds, cancellationToken);
                status = "set";
            }

            if (status != "unchanged")
            {
                logger?.LogInformation("MCP write set_database_state_expected by {Identity}: {Status} {Server}/{Database}",
                    ReadCaller.WriteIdentity(), status, resolved.DisplayName, database_name);
            }

            object? undo;
            string? undoNote = null;
            if (status == "unchanged")
            {
                undo = null;
            }
            else if (previous is null)
            {
                undo = UndoCall(resolved.DisplayName, database_name, ClearToken);
            }
            else
            {
                undo = UndoCall(resolved.DisplayName, database_name, previous.ExpectedState == DatabaseStateTokens.Ignore ? IgnoreToken : previous.ExpectedState);
                if (!previous.IsUserOverride)
                {
                    undoNote = "The earlier value was a learned baseline; the undo sets it as an override. CLEAR instead lets the service learn a baseline again.";
                }
            }

            return JsonSerializer.Serialize(new
            {
                status,
                server = resolved.DisplayName,
                database_name,
                expected_state = clear ? null : state,
                previous = previous is null ? null : new { expected_state = previous.ExpectedState, is_user_override = previous.IsUserOverride },
                message = status switch
                {
                    "set" => "The expectation is set; the service uses it on its next alert pass.",
                    "cleared" => "The expectation is removed; the service learns a baseline from the database's next observation.",
                    _ => clear ? "The database had no expectation; nothing changed." : "That override is already in place; nothing changed.",
                },
                undo,
                undo_note = undoNote,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("set_database_state_expected", ex);
        }
    }

    private sealed record Previous(string ExpectedState, bool IsUserOverride);

    private static object UndoCall(string server, string database, string expectedState) => new
    {
        tool = "set_database_state_expected",
        arguments = new { server_name = server, database_name = database, expected_state = expectedState },
    };

    /// <summary>The values the write accepts, in the spelling a caller sends: the state tokens, IGNORE and CLEAR.</summary>
    private static List<string> AllowedStates() =>
        DatabaseStateExpectedStore.StandardStates
            .Select(s => s == DatabaseStateTokens.Ignore ? IgnoreToken : s)
            .Append(ClearToken)
            .ToList();

    private static string StatusOf(DatabaseStateExpectedStore.Row r) =>
        r.ExpectedState is null ? "pending"
        : r.ExpectedState == DatabaseStateTokens.Ignore ? "ignored"
        : string.Equals(r.CurrentState, r.ExpectedState, StringComparison.Ordinal) ? "as_expected"
        : "deviating";
}
