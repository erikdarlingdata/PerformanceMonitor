/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer half of the store&lt;-&gt;service COMMAND plane (Stage 2's <c>DarlingCommandExecutor</c> is the
/// service half): the viewer INSERTs a <c>pending</c> row into <c>config.config_command</c> and POLLS that
/// row until the service claims, executes, and writes back a terminal <c>status</c>
/// (<c>succeeded</c>/<c>failed</c>) plus <c>result_status</c>/<c>result_json</c>. This is how the Add-server
/// dialog drives <c>test_connect</c> now (the SERVICE holds the network path + credentials and does the
/// actual connect) and how Stage 3b will drive <c>snapshot_now</c>/<c>analyze_now</c>.
///
/// <para>Same discipline as the config writes: public-const SQL, bound <c>$N</c> parameters, the enqueue
/// routed through <see cref="ExecuteWriteScalarAsync"/> so a read-only <c>viewer</c> seat degrades to
/// <see cref="ViewerReadOnlyException"/> (correct — it can't command the service). The bare table name
/// resolves through the search_path to <c>config.config_command</c>. <c>args_json</c> is bound as
/// <c>jsonb</c>.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Enqueues a pending command and returns its generated id. $1 requested_by, $2 command_type,
    /// $3 target_server_id (nullable), $4 args_json (jsonb, nullable).</summary>
    public const string CommandEnqueueSql = @"
INSERT INTO config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
VALUES ($1, $2, $3, $4, 'pending', (now() AT TIME ZONE 'UTC'))
RETURNING command_id";

    /// <summary>Reads a command's current status + terminal result for the poll loop. $1 command_id.</summary>
    public const string CommandPollSql =
        "SELECT status, result_status, result_json FROM config_command WHERE command_id = $1";

    /// <summary>Deletes a command row by id. $1 command_id.</summary>
    public const string CommandDeleteSql = "DELETE FROM config_command WHERE command_id = $1";

    /// <summary>The two terminal command states (the poll stops when it sees either).</summary>
    public const string StatusSucceeded = "succeeded";
    public const string StatusFailed = "failed";

    /// <summary>Default overall wait for a command's terminal result: the service polls the queue on a ~5s
    /// tick and a <c>test_connect</c> carries a 15s SqlClient connect budget, so 45s comfortably covers a
    /// slow claim + a timing-out connection without hanging the dialog forever.</summary>
    public static readonly TimeSpan DefaultCommandTimeout = TimeSpan.FromSeconds(45);

    private static readonly TimeSpan s_commandPollInterval = TimeSpan.FromMilliseconds(400);

    private static readonly JsonSerializerOptions s_argsJsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Enqueues a command and returns its <c>command_id</c>. A read-only seat throws
    /// <see cref="ViewerReadOnlyException"/> (it cannot command the service — correct).
    /// </summary>
    public async Task<long> EnqueueCommandAsync(
        string commandType,
        int? targetServerId,
        string? argsJson,
        string? requestedBy,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(commandType))
        {
            throw new ArgumentException("commandType is required.", nameof(commandType));
        }

        await using var command = _dataSource.CreateCommand(CommandEnqueueSql);
        command.CommandTimeout = ViewerCommandDeadlines.CommandPlaneSeconds;
        AddNullableText(command, requestedBy);                                              // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = commandType });  // $2
        command.Parameters.Add(new NpgsqlParameter                                          // $3
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            Value = targetServerId.HasValue ? targetServerId.Value : DBNull.Value,
        });
        command.Parameters.Add(new NpgsqlParameter                                          // $4
        {
            NpgsqlDbType = NpgsqlDbType.Jsonb,
            Value = (object?)argsJson ?? DBNull.Value,
        });

        var result = await ExecuteWriteScalarAsync(command, cancellationToken);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Reads a command row's current status + result (one poll). Returns null when the row is gone (never,
    /// under normal use — a command is never deleted before it terminates).
    /// </summary>
    public async Task<CommandResult?> ReadCommandResultAsync(long commandId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CommandPollSql);
        command.CommandTimeout = ViewerCommandDeadlines.CommandPlaneSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = commandId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new CommandResult(
            Status: reader.GetString(0),
            ResultStatus: reader.IsDBNull(1) ? null : reader.GetString(1),
            ResultJson: reader.IsDBNull(2) ? null : reader.GetFieldValue<string>(2));
    }

    /// <summary>
    /// Polls a command row until it reaches a terminal state (<c>succeeded</c>/<c>failed</c>) or the timeout
    /// elapses, returning the terminal <see cref="CommandResult"/> or null on timeout (the caller shows a
    /// "still running / try again" message). Polls every <see cref="s_commandPollInterval"/>.
    /// </summary>
    public async Task<CommandResult?> PollCommandResultAsync(
        long commandId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var deadline = Stopwatch.StartNew();
        var budget = timeout ?? DefaultCommandTimeout;

        while (deadline.Elapsed < budget)
        {
            var result = await ReadCommandResultAsync(commandId, cancellationToken);
            if (result is not null && IsTerminal(result.Status))
            {
                return result;
            }

            await Task.Delay(s_commandPollInterval, cancellationToken);
        }

        return null;
    }

    /// <summary>Enqueue + poll in one call — the common "run a command and wait for its result" path.</summary>
    public async Task<CommandResult?> RunCommandAsync(
        string commandType,
        int? targetServerId,
        string? argsJson,
        string? requestedBy,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var commandId = await EnqueueCommandAsync(commandType, targetServerId, argsJson, requestedBy, cancellationToken);
        return await PollCommandResultAsync(commandId, timeout, cancellationToken);
    }

    /// <summary>
    /// Runs a <c>test_connect</c>: enqueue the inline server definition, poll for the probe result, then
    /// DELETE the command row so its <c>args_json</c> DPAPI credential blob does not linger in the store. On a
    /// poll timeout the row is LEFT in place (the service may still be connecting) — a service-side retention
    /// sweep is the backstop for that case. Best-effort cleanup: a delete failure never masks the result.
    /// </summary>
    public async Task<CommandResult?> RunTestConnectAsync(
        string argsJson,
        string? requestedBy,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var commandId = await EnqueueCommandAsync("test_connect", targetServerId: null, argsJson, requestedBy, cancellationToken);
        var result = await PollCommandResultAsync(commandId, timeout, cancellationToken);

        if (result is not null)
        {
            try
            {
                await DeleteCommandAsync(commandId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The credential-bearing row couldn't be cleaned up now; the service-side purge is the
                   backstop. Never let cleanup hide the probe result the user is waiting on. */
            }
        }

        return result;
    }

    /// <summary>Deletes a command row (the viewer cleans up its own terminal test_connect command).</summary>
    public async Task DeleteCommandAsync(long commandId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CommandDeleteSql);
        command.CommandTimeout = ViewerCommandDeadlines.CommandPlaneSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = commandId });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Whether a status string is one of the two terminal states.</summary>
    public static bool IsTerminal(string? status) =>
        status is StatusSucceeded or StatusFailed;

    /// <summary>
    /// Serializes a <c>test_connect</c> server definition into the <c>args_json</c> the service deserializes
    /// as its <c>MonitoredServer</c> (property names match its <c>[JsonPropertyName]</c> contract, camelCase).
    /// The password rides as the DPAPI <c>encryptedPassword</c> blob, NOT plaintext — the service decrypts it
    /// on its own host (same-machine managed deploy) exactly as it does a stored server's password, so no
    /// plaintext credential ever lands in Postgres. Pure — pinned by Darling.Tests.
    /// </summary>
    public static string BuildTestConnectArgs(TestConnectServer server)
    {
        ArgumentNullException.ThrowIfNull(server);
        return JsonSerializer.Serialize(server, s_argsJsonOptions);
    }
}

/// <summary>A command row's polled status + terminal result (<c>result_json</c> is the service's probe facts
/// or error, as raw JSON text).</summary>
public sealed record CommandResult(string Status, string? ResultStatus, string? ResultJson);

/// <summary>
/// The inline server definition a <c>test_connect</c> command carries in <c>args_json</c>. The property
/// names/casing mirror the service's <c>MonitoredServer</c> <c>[JsonPropertyName]</c> contract so its
/// case-insensitive deserializer reconstructs the server and probes it. Carries only the connect-relevant
/// fields; the password is the DPAPI <see cref="EncryptedPassword"/> blob (never plaintext).
/// </summary>
public sealed class TestConnectServer
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("host")]
    public string Host { get; set; } = "";

    [JsonPropertyName("database")]
    public string? Database { get; set; }

    [JsonPropertyName("auth")]
    public string Auth { get; set; } = ServerStoreCredential.Integrated;

    [JsonPropertyName("username")]
    public string? Username { get; set; }

    /// <summary>DPAPI-LocalMachine blob (<see cref="ViewerServerSecret.Protect"/>) — the service decrypts it on its host.</summary>
    [JsonPropertyName("encryptedPassword")]
    public string? EncryptedPassword { get; set; }

    [JsonPropertyName("readOnlyIntent")]
    public bool ReadOnlyIntent { get; set; }

    [JsonPropertyName("trustServerCertificate")]
    public bool TrustServerCertificate { get; set; }

    [JsonPropertyName("encryptMode")]
    public string EncryptMode { get; set; } = "Mandatory";

    [JsonPropertyName("multiSubnetFailover")]
    public bool MultiSubnetFailover { get; set; }
}
