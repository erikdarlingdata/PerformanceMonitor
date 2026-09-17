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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Manages alert mute rules, persisting through <see cref="IMuteRuleStore"/>.
/// Rules are cached in memory for fast matching and synced to the store on changes.
/// Thread-safe: all in-memory operations are protected by _lock.
/// <para>
/// Ordering is persist-then-cache: the store is written FIRST, and on a persist
/// failure the cache mutation is skipped (an add that fails to persist does not
/// linger in the in-memory cache). Shared between Lite and Dashboard since E3b.
/// </para>
/// </summary>
public class MuteRuleService
{
    private readonly IMuteRuleStore _store;
    private readonly ILogger<MuteRuleService> _logger;
    private readonly object _lock = new object();
    private List<MuteRule> _rules = new();

    public MuteRuleService(IMuteRuleStore store, ILogger<MuteRuleService> logger)
    {
        _store = store;
        _logger = logger;
    }

    public bool IsAlertMuted(AlertMuteContext context)
    {
        lock (_lock)
        {
            return _rules.Any(r => r.Matches(context));
        }
    }

    public List<MuteRule> GetRules()
    {
        lock (_lock)
        {
            return _rules.ToList();
        }
    }

    public List<MuteRule> GetActiveRules()
    {
        lock (_lock)
        {
            return _rules.Where(r => r.Enabled && !r.IsExpired).ToList();
        }
    }

    /// <summary>
    /// Replaces the cache with the store's rules and purges any that have expired. Reached at service start
    /// AND on every control-plane reload, so it is a RELOAD as often as it is a startup path.
    ///
    /// <para><b>A failed load must not reduce the set of rules in force.</b> The store read is awaited
    /// BEFORE the cache assignment and this method catches nothing, so a read that throws leaves the
    /// previously-loaded rules exactly where they were: the service keeps suppressing what the operator
    /// asked it to suppress until a read succeeds. That ordering is the whole mechanism — a catch here, or
    /// a cache write hoisted above the await, would let a store blip un-mute every rule at once, and every
    /// surface that could report it reads this same cache.</para>
    ///
    /// <para>The first load needs no special case and deliberately has none. A fresh or unmigrated store
    /// genuinely has no rules and must not stop the service from starting — and at that point the cache is
    /// already empty, so retaining it IS starting clean. One rule covers both.</para>
    ///
    /// <para>An empty list from a SUCCESSFUL read still empties the cache: that is an operator deleting
    /// their last rule, and refusing to honour it would be the opposite error. Distinguishing the two is
    /// the store's job, and <see cref="IMuteRuleStore"/> requires it.</para>
    ///
    /// <para>Callers must handle the throw — not to preserve the cache, which is guaranteed here whatever
    /// they do, but because an unhandled one takes down a host whose collection is otherwise healthy. Each
    /// does, and reports it on the surfaces its own SKU has.</para>
    /// </summary>
    public async Task LoadAsync()
    {
        var rules = await _store.LoadAllAsync();

        lock (_lock)
        {
            _rules = rules.ToList();
        }

        /* Purge expired rules on startup */
        await PurgeExpiredRulesAsync();
    }

    public async Task AddRuleAsync(MuteRule rule)
    {
        try
        {
            await _store.InsertAsync(rule);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist new mute rule — rule will not be saved");
            return;
        }

        lock (_lock)
        {
            _rules.Add(rule);
        }
    }

    public async Task RemoveRuleAsync(string ruleId)
    {
        try
        {
            await _store.DeleteAsync(ruleId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete mute rule");
        }

        lock (_lock)
        {
            _rules.RemoveAll(r => r.Id == ruleId);
        }
    }

    public async Task UpdateRuleAsync(MuteRule updated)
    {
        try
        {
            await _store.UpdateAsync(updated);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update mute rule");
        }

        lock (_lock)
        {
            var index = _rules.FindIndex(r => r.Id == updated.Id);
            if (index >= 0)
                _rules[index] = updated;
        }
    }

    public async Task SetRuleEnabledAsync(string ruleId, bool enabled)
    {
        try
        {
            await _store.SetEnabledAsync(ruleId, enabled);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update mute rule enabled state");
        }

        lock (_lock)
        {
            var rule = _rules.FirstOrDefault(r => r.Id == ruleId);
            if (rule != null) rule.Enabled = enabled;
        }
    }

    public async Task<int> PurgeExpiredRulesAsync()
    {
        List<string> expiredIds;
        lock (_lock)
        {
            expiredIds = _rules.Where(r => r.IsExpired).Select(r => r.Id).ToList();
            if (expiredIds.Count == 0) return 0;
        }

        try
        {
            await _store.DeleteExpiredAsync(expiredIds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to purge expired mute rules");
            return 0;
        }

        lock (_lock)
        {
            _rules.RemoveAll(r => expiredIds.Contains(r.Id));
        }

        return expiredIds.Count;
    }
}
