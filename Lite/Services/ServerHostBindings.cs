/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The host (and Query Store replica) each server was last confirmed for by the Windows user running Lite, kept in that
/// user's own config folder. servers.json is one list for every user of the computer, while a saved password and the user's
/// own sign-in belong to the user, so Lite connects to a server only when the host servers.json names now is the one this
/// user saved or confirmed for that server. Host names are compared as written (port and instance included), trimmed and
/// case-insensitively.
/// </summary>
public sealed class ServerHostBindings
{
    /// <summary>The file in the user's own config folder (never the shared one that holds servers.json).</summary>
    public const string FileName = "server-hosts.json";

    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    private readonly string? _filePath;
    private readonly object _lock = new();
    private readonly Dictionary<string, Binding> _bindings = new(StringComparer.OrdinalIgnoreCase);
    private bool _hasRecord;

    /// <summary>One server's confirmed host and Query Store replica (empty when it has none).</summary>
    public sealed record Binding(string Host, string Replica);

    /// <param name="filePath">The per-user file; null keeps the record in memory only (a <see cref="ServerManager"/> built
    /// without a per-user config folder, as tests do).</param>
    public ServerHostBindings(string? filePath)
    {
        _filePath = filePath;
        if (filePath == null || !File.Exists(filePath))
            return;

        /* A file that exists but cannot be read leaves every server unconfirmed rather than trusting servers.json as it is
           now: the first-run binding is for a user who has no record yet, not for one whose record is damaged. */
        _hasRecord = true;
        try
        {
            var saved = JsonSerializer.Deserialize<BindingsFile>(File.ReadAllText(filePath));
            if (saved?.Servers != null)
            {
                foreach (var (id, binding) in saved.Servers)
                {
                    if (!string.IsNullOrEmpty(id) && binding != null)
                        _bindings[id] = new Binding(binding.Host ?? "", binding.Replica ?? "");
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
        {
            AppLogger.Warn("ServerHostBindings", $"Could not read {filePath}: {ex.Message}. Every server asks for its host to be confirmed again.");
        }
    }

    /// <summary>Whether this user already had a record when Lite started (false on the first run after the upgrade).</summary>
    public bool HasRecord
    {
        get { lock (_lock) return _hasRecord; }
    }

    /// <summary>
    /// The upgrade step. A user with no record yet (the first run of a version that keeps one) gets every server bound to
    /// the host servers.json names at this moment; from then on a changed host has to be confirmed. Does nothing once a
    /// record exists.
    /// </summary>
    public void BindAllOnFirstRun(IEnumerable<ServerConnection> servers)
    {
        lock (_lock)
        {
            if (_hasRecord)
                return;

            foreach (var server in servers)
                _bindings[server.Id] = BindingFor(server);

            _hasRecord = true;
            SaveLocked();
        }
    }

    /// <summary>Null when the server's host and replica are the ones this user confirmed; otherwise what changed.</summary>
    public ServerHostChange? Check(ServerConnection server)
    {
        lock (_lock)
        {
            if (!_bindings.TryGetValue(server.Id, out var bound))
                return new ServerHostChange(Normalize(server.ServerName), null, Normalize(server.QueryStoreReplica), ReplicaChanged: false);

            if (!SameHost(bound.Host, server.ServerName))
                return new ServerHostChange(Normalize(server.ServerName), bound.Host, Normalize(server.QueryStoreReplica), ReplicaChanged: false);

            /* A removed replica sends nothing anywhere, so only a replica that is set and differs needs confirming. */
            if (Normalize(server.QueryStoreReplica).Length > 0 && !SameHost(bound.Replica, server.QueryStoreReplica))
                return new ServerHostChange(Normalize(server.ServerName), bound.Host, Normalize(server.QueryStoreReplica), ReplicaChanged: true);

            return null;
        }
    }

    /// <summary>Binds the server to the host and replica it names now (the user saved it, added it, or confirmed it).</summary>
    public void Confirm(ServerConnection server)
    {
        lock (_lock)
        {
            _bindings[server.Id] = BindingFor(server);
            _hasRecord = true;
            SaveLocked();
        }
    }

    /// <summary>Drops the server's binding (the server was removed).</summary>
    public void Forget(string serverId)
    {
        lock (_lock)
        {
            if (_bindings.Remove(serverId))
                SaveLocked();
        }
    }

    /// <summary>The host as written, trimmed. Comparison is case-insensitive (<see cref="SameHost"/>).</summary>
    internal static string Normalize(string? host) => (host ?? "").Trim();

    internal static bool SameHost(string? a, string? b) =>
        string.Equals(Normalize(a), Normalize(b), StringComparison.OrdinalIgnoreCase);

    private static Binding BindingFor(ServerConnection server) =>
        new(Normalize(server.ServerName), Normalize(server.QueryStoreReplica));

    private void SaveLocked()
    {
        if (_filePath == null)
            return;

        try
        {
            var file = new BindingsFile();
            foreach (var (id, binding) in _bindings)
                file.Servers[id] = new BindingEntry { Host = binding.Host, Replica = binding.Replica };

            Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);
            var temp = _filePath + ".tmp";
            File.WriteAllText(temp, JsonSerializer.Serialize(file, s_jsonOptions));
            File.Move(temp, _filePath, overwrite: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* The in-memory record still holds, so this run behaves; the next run asks again for what it could not keep. */
            AppLogger.Warn("ServerHostBindings", $"Could not save {_filePath}: {ex.Message}");
        }
    }

    private sealed class BindingsFile
    {
        public int Version { get; set; } = 1;
        public Dictionary<string, BindingEntry?> Servers { get; set; } = new();
    }

    private sealed class BindingEntry
    {
        public string? Host { get; set; }
        public string? Replica { get; set; }
    }
}

/// <summary>
/// A server whose host (or Query Store replica) is not the one this user confirmed for it.
/// </summary>
/// <param name="Host">The host servers.json names now.</param>
/// <param name="ConfirmedHost">The host this user last confirmed; null when this user has never confirmed this server.</param>
/// <param name="Replica">The Query Store replica servers.json names now (empty when none).</param>
/// <param name="ReplicaChanged">True when the host matches and only the replica changed.</param>
public sealed record ServerHostChange(string Host, string? ConfirmedHost, string Replica, bool ReplicaChanged)
{
    /// <summary>
    /// True when the server signs in with a password or client secret saved for it alone: the change is then answered by
    /// entering that secret again. Every other mode (Windows, the Entra modes, managed identity, a credential profile) is
    /// answered by confirming the new host.
    /// </summary>
    public static bool AsksForSecret(ServerConnection server) =>
        string.IsNullOrEmpty(server.CredentialProfileId) &&
        (server.AuthenticationType == AuthenticationTypes.SqlServer ||
         server.AuthenticationType == AuthenticationTypes.ServicePrincipal);

    /// <summary>The plain sentence the server's status, the connect prompt and the Edit dialog show.</summary>
    public string Describe(ServerConnection server)
    {
        var what = ReplicaChanged
            ? $"The Query Store replica for this server changed to {Replica}."
            : ConfirmedHost == null
                ? $"You have not connected to this server's host, {Host}, from this Windows account yet."
                : $"The host name for this server changed to {Host}.";

        if (!AsksForSecret(server))
            return what + " Open Edit Server and save to confirm it before Lite connects.";

        return server.AuthenticationType == AuthenticationTypes.ServicePrincipal
            ? what + " Enter the client secret again to connect to it."
            : what + " Enter the password again to connect to it.";
    }
}

/// <summary>
/// Thrown in place of a connection string for a server whose host this user has not confirmed (see
/// <see cref="ServerHostBindings"/>). An <see cref="InvalidOperationException"/>, so every caller that already reports a
/// connection string it could not resolve reports this one the same way.
/// </summary>
public sealed class ServerHostNotConfirmedException : InvalidOperationException
{
    public ServerHostNotConfirmedException(string message) : base(message)
    {
    }
}
