/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// What the password key tools (#9644) need from the config the service started with: the folder that holds its active password
/// key file, and whether <c>passwordKey</c> is set. The worker publishes them once, right after it loads darling.json at start.
/// The tools read them here and never load the file again, because a load publishes the process-wide owned-secrets set, and
/// that set follows the file the service started with until the next start.
/// </summary>
public sealed class PasswordKeyStartupFacts
{
    /// <summary>The published facts. <paramref name="CredentialsDirectory"/> is null when the worker could not work the folder out.</summary>
    public sealed record Snapshot(string? CredentialsDirectory, bool KeyFromSetting);

    private volatile Snapshot? _current;

    /// <summary>Publishes the facts of the config the service started with (worker only, once).</summary>
    public void Publish(string? credentialsDirectory, bool keyFromSetting) =>
        _current = new Snapshot(credentialsDirectory, keyFromSetting);

    /// <summary>The published facts, or null when the worker has not loaded its config yet.</summary>
    public Snapshot? Read() => _current;
}
