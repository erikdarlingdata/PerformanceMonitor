/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// An in-memory stand-in for Windows Credential Manager behind <see cref="RouteSecretAccess"/> (#5775), with fault
/// injection, so a route-store test never touches the real one. <see cref="WriteGate"/> and <see cref="DeleteGate"/> refuse a
/// call like the real wrapper does; <see cref="Keeps"/> changes what a stored value reads back as; <see cref="Dead"/> makes
/// every call throw, which is how a test stands in for "the process died here": the store's own cleanup then cannot run, as it
/// could not after a real crash. Thread-safe, since the store's loader runs on a background thread.
/// </summary>
internal sealed class FakeRouteSecrets
{
    private readonly object _lock = new();
    private readonly Dictionary<string, string> _values = new(StringComparer.Ordinal);

    /// <summary>Return false to refuse a write of this key and value.</summary>
    public Func<string, string, bool> WriteGate { get; set; } = (_, _) => true;

    /// <summary>Return false to refuse (and keep) a delete of this key.</summary>
    public Func<string, bool> DeleteGate { get; set; } = _ => true;

    /// <summary>What Credential Manager actually keeps for a value, for the store's read-back check.</summary>
    public Func<string, string> Keeps { get; set; } = v => v;

    /// <summary>When true every call throws.</summary>
    public bool Dead { get; set; }

    /// <summary>Counts the calls to write and to delete, for tests that assert nothing was touched.</summary>
    public int WriteCalls { get; private set; }

    public int DeleteCalls { get; private set; }

    public RouteSecretAccess Access => new(Read, Write, Delete, ListKeys);

    public string Read(string key)
    {
        lock (_lock)
        {
            ThrowIfDead();
            return _values.TryGetValue(key, out var v) ? v : "";
        }
    }

    public bool Write(string key, string value)
    {
        lock (_lock)
        {
            ThrowIfDead();
            WriteCalls++;
            if (!WriteGate(key, value))
            {
                return false;
            }

            _values[key] = Keeps(value);
            return true;
        }
    }

    public bool Delete(string key)
    {
        lock (_lock)
        {
            ThrowIfDead();
            DeleteCalls++;
            if (!DeleteGate(key))
            {
                return false;
            }

            _values.Remove(key);
            return true;
        }
    }

    public IReadOnlyList<string> ListKeys(string prefix)
    {
        lock (_lock)
        {
            ThrowIfDead();
            return _values.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList();
        }
    }

    /// <summary>Every key now stored, in no particular order.</summary>
    public IReadOnlyList<string> Keys
    {
        get { lock (_lock) { return _values.Keys.ToList(); } }
    }

    public int Count
    {
        get { lock (_lock) { return _values.Count; } }
    }

    /// <summary>The keys stored for one route and channel, at any generation.</summary>
    public IReadOnlyList<string> KeysOf(int routeId, string channel)
    {
        var prefix = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"NotificationRoute_{routeId}_{channel}_");
        lock (_lock) { return _values.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList(); }
    }

    public bool Has(int routeId, string channel) => KeysOf(routeId, channel).Count > 0;

    /// <summary>The value under the one key a route and channel has; fails when there are none or several.</summary>
    public string ValueOf(int routeId, string channel)
    {
        var keys = KeysOf(routeId, channel);
        lock (_lock) { return _values[keys.Single()]; }
    }

    /// <summary>Takes every key of a route and channel away, like a credential Windows lost.</summary>
    public void Lose(int routeId, string channel)
    {
        foreach (var key in KeysOf(routeId, channel))
        {
            lock (_lock) { _values.Remove(key); }
        }
    }

    /// <summary>Puts a key in directly, like one a crash or an old build left behind.</summary>
    public void Plant(string key, string value)
    {
        lock (_lock) { _values[key] = value; }
    }

    private void ThrowIfDead()
    {
        if (Dead)
        {
            throw new InvalidOperationException("The secret store is unavailable.");
        }
    }
}
