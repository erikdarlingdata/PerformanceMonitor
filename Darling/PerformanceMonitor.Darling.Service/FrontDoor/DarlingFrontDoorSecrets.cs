/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// A resolved access token. It is a class with its own <see cref="ToString"/> on purpose: a record prints every member, so a
/// token held in one would reach a log line or an exception message the first time somebody interpolated the object.
/// </summary>
internal sealed class FrontDoorSecretValue
{
    internal FrontDoorSecretValue(string value) => Value = value;

    /// <summary>The token. Read it only to put it on the wire.</summary>
    internal string Value { get; }

    /// <summary>Never the token.</summary>
    public override string ToString() => "[front door token]";
}

/// <summary>
/// The front door's tokens for its stores (#6840), resolved the way <see cref="McpTokenEntry.ResolveToken"/> resolves a named
/// token: the DPAPI-encrypted value first, then the plain value or its <c>env:</c>/<c>file:</c> reference. Each is resolved on
/// first use and kept for the life of the process, like the rest of the file's settings (an edit takes effect on a restart). A
/// token that cannot be resolved is logged once, by store and setting and never by value, and answers null, so one store's bad
/// reference costs that store its calls and not the front door its start.
/// </summary>
internal sealed class DarlingFrontDoorSecrets : IFrontDoorSecrets
{
    private readonly FrontDoorConfig _config;
    private readonly ILogger? _logger;
    private readonly ConcurrentDictionary<string, Lazy<FrontDoorSecretValue?>> _resolved = new();

    public DarlingFrontDoorSecrets(FrontDoorConfig config, ILogger? logger = null)
    {
        _config = config;
        _logger = logger;
    }

    public string? McpToken(string store, bool agentRole)
        => Resolve(store, agentRole ? "mcp-agent" : "mcp", e => e.ResolveMcpToken(agentRole))?.Value;

    public string? WebToken(string store)
        => Resolve(store, "web", e => e.ResolveWebToken())?.Value;

    private FrontDoorSecretValue? Resolve(string store, string kind, Func<FrontDoorStoreConfig, string?> read)
    {
        var entry = (_config.Stores ?? new()).FirstOrDefault(
            s => s is not null && string.Equals(s.Name?.Trim(), store, StringComparison.OrdinalIgnoreCase));
        if (entry is null)
        {
            return null;
        }

        return _resolved.GetOrAdd(
            entry.Name!.Trim().ToLowerInvariant() + "|" + kind,
            _ => new Lazy<FrontDoorSecretValue?>(() =>
            {
                try
                {
                    return read(entry) is { Length: > 0 } token ? new FrontDoorSecretValue(token) : null;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger?.LogWarning(
                        "Front door: the {Kind} token for store {Store} cannot be read ({Message}). Calls to that store go without it until the service restarts.",
                        kind, entry.Name!.Trim(), ex.Message);
                    return null;
                }
            })).Value;
    }
}
