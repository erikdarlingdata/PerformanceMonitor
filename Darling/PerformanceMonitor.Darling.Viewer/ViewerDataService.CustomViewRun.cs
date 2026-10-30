/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>What one composed-panel run gave the viewer: the payload on success (<c>{sql, rows, annotations, notice?}</c>, the same
/// object the web's <c>/api/compose/run</c> answers), else the runner's own sentence.</summary>
public sealed record ViewerPanelRun(JsonObject? Payload, string? Error)
{
    public bool Ok => Payload is not null;
}

/// <summary>
/// #6390 S6: running a Custom View's composed panels from the viewer. The run is <see cref="ComposePanelRunner.RunComposedPanelCoreAsync"/>
/// (Storage), the one call the web's <c>/api/compose/run</c> makes; only the <see cref="ComposeRunHost"/> is the viewer's own. It
/// resolves server names through <see cref="ServerNameMatcher"/> (exact matches only, as the service's resolver does), reads the
/// compose statement timeout from the store as the service does, and adds no data-start instants (those are web-only; the viewer
/// shows its own data-start line on the tabs). The run reads no ambient scope, so <see cref="ComposeReadNotes"/> keeps its defaults.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The bounds the store clamps the compose statement timeout to; they mirror the service's
    /// <c>StoreConfigProvider.ClampComposeStatementTimeoutSeconds</c> (a test holds the two equal).</summary>
    internal static int ClampComposeSeconds(int value) => Math.Clamp(value <= 0 ? 60 : value, 5, 600);

    private const string ComposeDeadlineSql = "SELECT compose_statement_timeout_seconds FROM config.config_service WHERE id = 1";

    private const string ComposeRegistrySql = "SELECT server_id, server_name, display_name FROM servers ORDER BY server_name";

    private const string ComposeUnregisteredSql =
        "SELECT n FROM unnest($1::text[]) AS n WHERE NOT EXISTS (SELECT 1 FROM collect.servers AS s WHERE s.server_name = n)";

    internal static readonly ComposeRunHost ViewerComposeHost = new(
        static (postgres, scope, ct, _) => ResolveComposeServerNamesAsync(postgres, scope, ct),
        static (postgres, servers, ct, _, onLookupFailed) => FindUnregisteredComposeServersAsync(postgres, servers, onLookupFailed, ct),
        ResolveComposeQuerySecondsAsync,
        static (_, _, _, _, _) => { });

    /// <summary>Runs one composed panel body (<c>{panel, variables?, values?, server?, hours?, windowStart?, windowEnd?}</c>).
    /// A refusal or error comes back as the runner's text; nothing here throws except a cancellation.</summary>
    public async Task<ViewerPanelRun> RunComposedPanelAsync(JsonObject body, CancellationToken cancellationToken = default)
    {
        try
        {
            var outcome = await ComposePanelRunner.RunComposedPanelCoreAsync(
                ViewerComposeHost, _dataSource, body, ComposePanelRunner.ComposeClientDeadlineHeadroomSeconds,
                remapClientTimeout: true, includeDataStartFields: false, cancellationToken);
            return outcome.Payload is { } payload
                ? new ViewerPanelRun(payload, null)
                : new ViewerPanelRun(null, outcome.Error ?? "The panel could not be run.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new ViewerPanelRun(null, $"Error running query: {ex.Message}");
        }
    }

    internal static (List<string> Scope, string? Refusal) ResolveComposeNames(IReadOnlyList<string> scope, IReadOnlyList<RegisteredServer> servers)
    {
        var resolved = new List<string>(scope.Count);
        foreach (var rawName in scope)
        {
            if (servers.Any(s => string.Equals(s.ServerName, rawName, StringComparison.Ordinal)))
            {
                resolved.Add(rawName);
                continue;
            }

            var name = rawName.Trim();
            var match = ServerNameMatcher.Match(servers, name, storageNameIgnoresCase: true, NameClash.StoredNameWins);
            if (match.MatchedBy != "exact")
            {
                /* No exact match: a removed server whose history is still stored, or a name nobody registered. It stays as written. */
                resolved.Add(rawName);
            }
            else if (match.Candidates.Count == 1)
            {
                resolved.Add(match.Candidates[0].ServerName);
            }
            else
            {
                return (resolved, ServerNameMatcher.AmbiguitySentence(name, match));
            }
        }

        return (resolved.Distinct(StringComparer.Ordinal).ToList(), null);
    }

    private static async Task<(List<string>? Scope, string? Refusal)> ResolveComposeServerNamesAsync(
        NpgsqlDataSource postgres, List<string>? scope, CancellationToken cancellationToken)
    {
        if (scope is not { Count: > 0 })
        {
            return (scope, null);
        }

        try
        {
            var servers = new List<RegisteredServer>();
            await using var command = postgres.CreateCommand(ComposeRegistrySql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add(new RegisteredServer(reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }

            return ResolveComposeNames(scope, servers);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The registry could not be read: the scope runs as written, the service's own fail-open. */
            return (scope, null);
        }
    }

    private static async Task<IReadOnlyList<string>?> FindUnregisteredComposeServersAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? servers, Action? onLookupFailed, CancellationToken cancellationToken)
    {
        if (servers is not { Count: > 0 })
        {
            return null;
        }

        var names = servers.Distinct(StringComparer.Ordinal).ToArray();
        try
        {
            await using var command = postgres.CreateCommand(ComposeUnregisteredSql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = names });
            var missing = new List<string>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                missing.Add(reader.GetString(0));
            }

            return missing.Count == 0 ? null : missing;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            onLookupFailed?.Invoke();
            return names;
        }
    }

    private static async Task<int> ResolveComposeQuerySecondsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand(ComposeDeadlineSql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return ClampComposeSeconds(value is int seconds ? seconds : 0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A seat that cannot read the setting (or a store without it) takes the service's fallback. */
            return 60;
        }
    }
}
