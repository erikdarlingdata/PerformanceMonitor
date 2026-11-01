/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The store seam of the web's Generate now button (#6389): queue one <c>analyze_now</c> command (the same command
/// the Viewer queues; it takes no arguments) and read its state. Enqueue throws a <see cref="PostgresException"/> carrying the store
/// function's code: <c>DL404</c> no such server, <c>DL409</c> an analysis run for the server is queued or running, <c>DL429</c> too soon
/// (the hint is the next allowed time).
/// </summary>
internal interface IAnalyzeNowStore
{
    /// <summary>The guard lives in the store function; the web only passes the server and who asked.</summary>
    Task<long> EnqueueAsync(int serverId, string requestedBy, CancellationToken cancellationToken);

    Task<SnapshotCommandState?> ReadStateAsync(long commandId, CancellationToken cancellationToken);

    /// <summary>Whether the web's store login can queue the command: the function exists and the login may execute it.</summary>
    Task<bool> CanQueueAsync(CancellationToken cancellationToken);
}

/// <summary>The PostgreSQL store behind the web's Generate now button: the store function for the write, a plain read of the non-secret command columns.</summary>
internal sealed class PostgresAnalyzeNowStore : IAnalyzeNowStore
{
    private readonly NpgsqlDataSource _postgres;

    public PostgresAnalyzeNowStore(NpgsqlDataSource postgres) => _postgres = postgres;

    public async Task<long> EnqueueAsync(int serverId, string requestedBy, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand("SELECT config.request_analyze_now($1, $2)");
        command.CommandTimeout = 15;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = requestedBy });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    public async Task<SnapshotCommandState?> ReadStateAsync(long commandId, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand(
            "SELECT status, result_status, result_json FROM config.config_command WHERE command_id = $1 AND command_type = 'analyze_now'");
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

    public async Task<bool> CanQueueAsync(CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand(
            "SELECT COALESCE(has_function_privilege(to_regprocedure('config.request_analyze_now(integer, text)'), 'EXECUTE'), false)");
        command.CommandTimeout = 15;
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }
}

/// <summary>
/// The guarded request for an analysis run (#6389): queues through the store function and turns its refusal codes into the
/// outcome shape <see cref="SnapshotNowRequests"/> uses, so the web route answers like the run-collectors-now route. The function is the
/// guard (one run per server at a time, not within 15 seconds of the last); the web adds nothing of its own.
/// </summary>
internal static class AnalyzeNowRequests
{
    /// <summary>The store function is missing, or the login may not execute it.</summary>
    public const string StoreRefusedCode = "store_cannot_queue";

    internal static async Task<SnapshotRequestOutcome> RequestAsync(
        IAnalyzeNowStore store, int serverId, string requestedBy, CancellationToken cancellationToken)
    {
        try
        {
            var id = await store.EnqueueAsync(serverId, requestedBy, cancellationToken);
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
            return new SnapshotRequestOutcome(
                false, SnapshotRequestOutcome.InFlightCode,
                "An analysis run for this server is already queued or running. Wait for it to finish.", null, null);
        }
        catch (PostgresException ex) when (ex.SqlState == "DL429")
        {
            DateTime? nextAllowed = DateTime.TryParse(
                ex.Hint, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed)
                ? parsed
                : null;
            return new SnapshotRequestOutcome(
                false, SnapshotRequestOutcome.RateLimitedCode,
                "An analysis run was requested for this server less than 15 seconds ago. Try again in a moment.", null, nextAllowed);
        }
        catch (PostgresException ex) when (ex.SqlState is "42883" or "42501")
        {
            /* The function is missing (a store provisioned before this release) or the login may not execute it. */
            return new SnapshotRequestOutcome(
                false, StoreRefusedCode,
                "This dashboard's store login cannot queue an analysis run. Re-run " + DarlingManagedRoles.ProvisionRolesScriptPath + " on the store "
                + "(it adds config.request_analyze_now), or use the Viewer's Generate now.",
                null, null);
        }
    }
}
