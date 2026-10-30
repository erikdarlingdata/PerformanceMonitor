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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The <c>args_json</c> of a <c>snapshot_now</c> command (#5941): <c>{"collectors":["pg_buffer_usage", ...]}</c>, or none.
/// No list (or an empty one) is the run-all form: every enabled collector. A list is the run-one form: only those, each
/// even when disabled. Every name is checked against <see cref="CollectorScheduleDefaults.All"/>.
/// </summary>
public static class SnapshotNowArgs
{
    private static readonly Dictionary<string, string> s_canonicalNames =
        CollectorScheduleDefaults.All.Keys.ToDictionary(key => key, key => key, StringComparer.OrdinalIgnoreCase);

    /// <summary>The most names one request carries (the store function enforces the same cap).</summary>
    public const int MaxCollectors = 200;

    /// <summary>Parses the args. False with a plain error naming the bad collector when a name is not in the catalog.</summary>
    public static bool TryParse(string? argsJson, out IReadOnlyList<string> collectors, out string? error)
    {
        collectors = [];
        error = null;
        if (string.IsNullOrWhiteSpace(argsJson))
        {
            return true;
        }

        var names = new List<string>();
        try
        {
            using var doc = JsonDocument.Parse(argsJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                error = "snapshot_now args_json must be an object with an optional \"collectors\" list";
                return false;
            }

            foreach (var property in doc.RootElement.EnumerateObject())
            {
                if (!string.Equals(property.Name, "collectors", StringComparison.Ordinal))
                {
                    error = $"snapshot_now takes only a \"collectors\" list, not '{property.Name}'";
                    return false;
                }

                if (property.Value.ValueKind == JsonValueKind.Null)
                {
                    continue;
                }

                if (property.Value.ValueKind != JsonValueKind.Array)
                {
                    error = "snapshot_now \"collectors\" must be a list of collector names";
                    return false;
                }

                foreach (var item in property.Value.EnumerateArray())
                {
                    if (item.ValueKind != JsonValueKind.String)
                    {
                        error = "snapshot_now \"collectors\" must be a list of collector names";
                        return false;
                    }

                    names.Add(item.GetString() ?? string.Empty);
                }
            }
        }
        catch (JsonException)
        {
            error = "snapshot_now args_json is not valid JSON";
            return false;
        }

        if (names.Count > MaxCollectors)
        {
            error = $"snapshot_now takes at most {MaxCollectors} collectors";
            return false;
        }

        var seen = new List<string>();
        foreach (var raw in names)
        {
            /* The catalog's own spelling is what leaves here: the schedule table matches names without regard to case, but the
               collector catalog's engine gate matches exactly and passes a name it does not know, so a differently cased name
               would otherwise reach the worker as a collector the catalog cannot place and run on the wrong engine. */
            if (!s_canonicalNames.TryGetValue(raw.Trim(), out var name))
            {
                error = $"unknown collector '{raw.Trim()}'. Use a name from the collector list.";
                return false;
            }

            if (!seen.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                seen.Add(name);
            }
        }

        collectors = seen;
        return true;
    }

    /// <summary>The <c>args_json</c> for a list, or null for the run-all form.</summary>
    public static string? Build(IReadOnlyList<string>? collectors) =>
        collectors is { Count: > 0 }
            ? new JsonObject { ["collectors"] = new JsonArray(collectors.Select(c => (JsonNode?)JsonValue.Create(c)).ToArray()) }.ToJsonString()
            : null;
}

/// <summary>One <c>snapshot_now</c> row as the guard reads it.</summary>
public sealed record SnapshotCommandRow(long CommandId, DateTime CreatedUtc, string Status);

/// <summary>A command's current state and result, for the poll.</summary>
public sealed record SnapshotCommandState(string Status, string? ResultStatus, string? ResultJson);

/// <summary>The store seam of the run-collectors-now request: the newest request for a server, the enqueue, the poll.</summary>
public interface ISnapshotNowStore
{
    Task<SnapshotCommandRow?> ReadLatestAsync(int serverId, CancellationToken cancellationToken);

    Task<long> EnqueueAsync(int serverId, string? argsJson, string requestedBy, CancellationToken cancellationToken);

    Task<SnapshotCommandState?> ReadStateAsync(long commandId, CancellationToken cancellationToken);
}

/// <summary>What a request came to: accepted with a command id, or refused with a code and a plain message.</summary>
public sealed record SnapshotRequestOutcome(bool Accepted, string Code, string Text, long? CommandId, DateTime? NextAllowedUtc)
{
    public const string InFlightCode = "already_running";
    public const string RateLimitedCode = "too_soon";
    public const string UnknownCollectorCode = "unknown_collector";
    public const string UnknownServerCode = "unknown_server";

    /// <summary>The store function refused the request itself (DL400): for example a transaction that is not READ COMMITTED.</summary>
    public const string BadRequestCode = "bad_request";
}

/// <summary>
/// The guarded request for a run-all or run-one collector pass (#5941), shared by the web route and the MCP tool. It checks the
/// collector names, refuses a second request for a server while one is queued or running, refuses a request within
/// <see cref="MinimumInterval"/> of the last one for that server, and then queues the command through the store function. The
/// clock is a <see cref="TimeProvider"/> so a test moves it instead of sleeping. The service also takes a permit from the sweep gate when it
/// runs the command, which is what caps the fleet.
/// </summary>
public sealed class SnapshotNowRequests
{
    /// <summary>The shortest gap between two requests for one server.</summary>
    public static readonly TimeSpan MinimumInterval = TimeSpan.FromSeconds(60);

    /* One request at a time through the check and the enqueue, so two clicks that arrive together cannot both pass. The web
       route and the MCP tool run in the same process and share this. */
    private static readonly SemaphoreSlim s_requestLock = new(1, 1);

    private readonly ISnapshotNowStore _store;
    private readonly TimeProvider _clock;

    public SnapshotNowRequests(ISnapshotNowStore store, TimeProvider? clock = null)
    {
        _store = store;
        _clock = clock ?? TimeProvider.System;
    }

    public async Task<SnapshotRequestOutcome> RequestAsync(
        int serverId, IReadOnlyList<string>? collectors, string requestedBy, CancellationToken cancellationToken)
    {
        /* The names stored are the catalog's spelling (TryParse canonicalises), whatever case the caller used. */
        if (!SnapshotNowArgs.TryParse(SnapshotNowArgs.Build(collectors), out var canonical, out var error))
        {
            return new SnapshotRequestOutcome(false, SnapshotRequestOutcome.UnknownCollectorCode, error!, null, null);
        }

        var argsJson = SnapshotNowArgs.Build(canonical);

        await s_requestLock.WaitAsync(cancellationToken);
        try
        {
            var now = _clock.GetUtcNow().UtcDateTime;
            var latest = await _store.ReadLatestAsync(serverId, cancellationToken);
            if (latest is not null)
            {
                if (latest.Status is "pending" or "in_progress")
                {
                    return new SnapshotRequestOutcome(
                        false, SnapshotRequestOutcome.InFlightCode,
                        "A collector run for this server is already queued or running. Wait for it to finish.", latest.CommandId, null);
                }

                var nextAllowed = latest.CreatedUtc + MinimumInterval;
                if (now < nextAllowed)
                {
                    return new SnapshotRequestOutcome(
                        false, SnapshotRequestOutcome.RateLimitedCode,
                        $"A collector run was requested for this server less than {(int)MinimumInterval.TotalSeconds} seconds ago. "
                        + $"The next one is allowed at {nextAllowed.ToString("HH:mm:ss", CultureInfo.InvariantCulture)} UTC.",
                        latest.CommandId, nextAllowed);
                }
            }

            try
            {
                var id = await _store.EnqueueAsync(serverId, argsJson, requestedBy, cancellationToken);
                return new SnapshotRequestOutcome(true, "queued", "Queued.", id, null);
            }
            catch (PostgresException ex) when (ex.SqlState == "DL404")
            {
                return new SnapshotRequestOutcome(false, SnapshotRequestOutcome.UnknownServerCode, "No monitored server has that id.", null, null);
            }
            catch (PostgresException ex) when (ex.SqlState == "DL400")
            {
                return new SnapshotRequestOutcome(false, SnapshotRequestOutcome.BadRequestCode, ex.MessageText, null, null);
            }
            catch (PostgresException ex) when (ex.SqlState == "DL409")
            {
                /* The store function's own guard (a second service instance, or a direct caller, got there first). */
                return new SnapshotRequestOutcome(
                    false, SnapshotRequestOutcome.InFlightCode,
                    "A collector run for this server is already queued or running. Wait for it to finish.", null, null);
            }
            catch (PostgresException ex) when (ex.SqlState == "DL429")
            {
                DateTime? nextAllowed = DateTime.TryParse(
                    ex.Hint, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed)
                    ? parsed
                    : null;
                return new SnapshotRequestOutcome(
                    false, SnapshotRequestOutcome.RateLimitedCode,
                    $"A collector run was requested for this server less than {(int)MinimumInterval.TotalSeconds} seconds ago."
                    + (nextAllowed is { } at ? $" The next one is allowed at {at.ToString("HH:mm:ss", CultureInfo.InvariantCulture)} UTC." : string.Empty),
                    null, nextAllowed);
            }
        }
        finally
        {
            s_requestLock.Release();
        }
    }

    /// <summary>
    /// Waits for a queued command to finish, up to <paramref name="timeout"/>, and returns its state. A command still going at
    /// the end comes back with its non-terminal status so the caller can say so and let the user look again.
    /// </summary>
    public async Task<SnapshotCommandState?> WaitForResultAsync(long commandId, TimeSpan timeout, TimeSpan pollInterval, CancellationToken cancellationToken)
    {
        var deadline = _clock.GetUtcNow() + timeout;
        SnapshotCommandState? state = null;
        while (true)
        {
            state = await _store.ReadStateAsync(commandId, cancellationToken);
            if (state is not null && state.Status is "succeeded" or "failed")
            {
                return state;
            }

            if (_clock.GetUtcNow() >= deadline)
            {
                return state;
            }

            await Task.Delay(pollInterval, _clock, cancellationToken);
        }
    }
}

/// <summary>The PostgreSQL store behind <see cref="SnapshotNowRequests"/>: the store function for the write, plain reads of the non-secret command columns.</summary>
public sealed class PostgresSnapshotNowStore : ISnapshotNowStore
{
    private readonly NpgsqlDataSource _postgres;
    private readonly bool _resultThroughMcpFunction;

    /// <param name="postgres">The store connection: the web host's (viewer role) or the MCP host's (mcp role).</param>
    /// <param name="resultThroughMcpFunction">True for the <c>mcp</c> role, which holds no SELECT on <c>result_json</c> and
    /// reads a command's result through <c>config.read_mcp_command_result</c> (#6230).</param>
    public PostgresSnapshotNowStore(NpgsqlDataSource postgres, bool resultThroughMcpFunction = false)
    {
        _postgres = postgres;
        _resultThroughMcpFunction = resultThroughMcpFunction;
    }

    public async Task<SnapshotCommandRow?> ReadLatestAsync(int serverId, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand(
            "SELECT command_id, created_at, status FROM config.config_command "
            + "WHERE command_type = 'snapshot_now' AND target_server_id = $1 ORDER BY command_id DESC LIMIT 1");
        command.CommandTimeout = 15;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new SnapshotCommandRow(reader.GetInt64(0), DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc), reader.GetString(2))
            : null;
    }

    public async Task<long> EnqueueAsync(int serverId, string? argsJson, string requestedBy, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand("SELECT config.request_snapshot_now($1, $2, $3)");
        command.CommandTimeout = 15;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)argsJson ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = requestedBy });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    public async Task<SnapshotCommandState?> ReadStateAsync(long commandId, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand(_resultThroughMcpFunction
            ? "SELECT status, result_status, result_json FROM config.read_mcp_command_result($1) WHERE command_type = 'snapshot_now'"
            : "SELECT status, result_status, result_json FROM config.config_command "
              + "WHERE command_id = $1 AND command_type = 'snapshot_now'");
        command.CommandTimeout = 15;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = commandId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new SnapshotCommandState(
            reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<string>(2));
    }
}
