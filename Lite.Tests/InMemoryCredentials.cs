/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit.Sdk;
using Xunit.v3;

/// <summary>
/// #8754: a credential store that keeps its entries in memory, for a test that builds a window or a manager and does not test
/// credential storage itself. It follows <see cref="CredentialStore"/>'s contract: the same argument checks, the same
/// <see cref="CredentialStore.MaxSecretLength"/> limit and the same refusal text, and a key that is not there reads as not found.
/// </summary>
internal sealed class InMemoryCredentialStore : ICredentialStore
{
    private readonly ConcurrentDictionary<string, (string Username, string Password)> _entries = new(StringComparer.Ordinal);

    public bool SaveCredential(string serverId, string username, string password)
        => TrySaveCredential(serverId, username, password).Succeeded;

    public CredentialSaveResult TrySaveCredential(string serverId, string username, string password)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        if (string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException("Username cannot be null or empty", nameof(username));
        }

        ArgumentNullException.ThrowIfNull(password);
        if (password.Length > CredentialStore.MaxSecretLength)
        {
            return CredentialSaveResult.Failed(
                $"it is {password.Length:N0} characters long, and Windows Credential Manager stores at most {CredentialStore.MaxSecretLength:N0}");
        }

        _entries[serverId] = (username, password);
        return CredentialSaveResult.Ok;
    }

    public CredentialSaveResult TryDeleteCredential(string serverId)
    {
        RequireKey(serverId);
        _entries.TryRemove(serverId, out _);
        return CredentialSaveResult.Ok;
    }

    public (string Username, string Password)? GetCredential(string serverId)
    {
        RequireKey(serverId);
        return _entries.TryGetValue(serverId, out var entry) ? entry : null;
    }

    public CredentialReadOutcome TryReadCredential(string serverId, out (string Username, string Password)? credential, out string? failure)
    {
        RequireKey(serverId);
        failure = null;
        if (_entries.TryGetValue(serverId, out var entry))
        {
            credential = entry;
            return CredentialReadOutcome.Found;
        }

        credential = null;
        return CredentialReadOutcome.NotFound;
    }

    public bool DeleteCredential(string serverId)
    {
        RequireKey(serverId);
        _entries.TryRemove(serverId, out _);
        return true;
    }

    public bool CredentialExists(string serverId) => !string.IsNullOrWhiteSpace(serverId) && _entries.ContainsKey(serverId);

    public bool UpdateCredential(string serverId, string username, string password)
    {
        DeleteCredential(serverId);
        return SaveCredential(serverId, username, password);
    }

    public IReadOnlyList<string> ListKeys(string keyPrefix)
    {
        if (string.IsNullOrWhiteSpace(keyPrefix))
        {
            throw new ArgumentException("Key prefix cannot be null or empty", nameof(keyPrefix));
        }

        return _entries.Keys.Where(k => k.StartsWith(keyPrefix, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    private static void RequireKey(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }
    }
}

/// <summary>
/// #8754: gives every <c>new CredentialService()</c> a test makes (a window's, a ServerManager's, <c>App.SaveWebhookUrl</c>'s) an
/// in-memory store of the test's own instead of the real Windows Credential Manager, so the class needs no
/// <c>credential-manager</c> collection. The store is made in <c>Before</c> and dropped in <c>After</c>; it lives in the test's async
/// flow, so classes that run in parallel never share one, and the shared STA test thread runs each body under it
/// (<c>StaHost</c> flows the calling test's context). A class that tests reading or writing the real store does not take this
/// attribute: it stays in <c>CredentialManagerCollection</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class InMemoryCredentialsAttribute : BeforeAfterTestAttribute
{
    private static readonly System.Threading.AsyncLocal<IDisposable?> s_scope = new();

    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
        => s_scope.Value = CredentialService.UseStoreForCurrentTest(new InMemoryCredentialStore());

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        s_scope.Value?.Dispose();
        s_scope.Value = null;
    }
}
