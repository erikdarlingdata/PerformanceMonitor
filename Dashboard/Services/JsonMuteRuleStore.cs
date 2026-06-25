/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// JSON-backed <see cref="IMuteRuleStore"/> over <c>alert_mute_rules.json</c>.
    /// Wraps the persistence (Load / Save + whole-file rewrite) that previously
    /// lived inside <see cref="MuteRuleService"/> — verbatim logic. The file is
    /// loaded synchronously in the constructor (so <c>MainWindow</c> needs no async
    /// init); <see cref="LoadAllAsync"/> returns the already-loaded set. All I/O is
    /// synchronous internally and wrapped in completed tasks so the async interface
    /// is satisfied without changing Dashboard's startup ordering (E2 keeps the
    /// cache-then-save ordering; convergence is deferred to E3b).
    /// </summary>
    public sealed class JsonMuteRuleStore : IMuteRuleStore
    {
        private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

        private readonly string _filePath;
        private readonly object _lock = new object();
        private List<MuteRule> _rules = new();

        public JsonMuteRuleStore()
        {
            var appDataDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "PerformanceMonitorDashboard");
            Directory.CreateDirectory(appDataDir);
            _filePath = Path.Combine(appDataDir, "alert_mute_rules.json");
            Load();
        }

        public Task<IReadOnlyList<MuteRule>> LoadAllAsync()
        {
            lock (_lock)
            {
                IReadOnlyList<MuteRule> snapshot = _rules.ToList();
                return Task.FromResult(snapshot);
            }
        }

        public Task InsertAsync(MuteRule rule)
        {
            lock (_lock)
            {
                _rules.Add(rule);
                Save();
            }
            return Task.CompletedTask;
        }

        public Task UpdateAsync(MuteRule rule)
        {
            lock (_lock)
            {
                var index = _rules.FindIndex(r => r.Id == rule.Id);
                if (index >= 0)
                {
                    _rules[index] = rule;
                    Save();
                }
            }
            return Task.CompletedTask;
        }

        public Task SetEnabledAsync(string ruleId, bool enabled)
        {
            lock (_lock)
            {
                var rule = _rules.FirstOrDefault(r => r.Id == ruleId);
                if (rule != null)
                {
                    rule.Enabled = enabled;
                    Save();
                }
            }
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string ruleId)
        {
            lock (_lock)
            {
                _rules.RemoveAll(r => r.Id == ruleId);
                Save();
            }
            return Task.CompletedTask;
        }

        public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds)
        {
            lock (_lock)
            {
                int removed = _rules.RemoveAll(r => expiredIds.Contains(r.Id));
                if (removed > 0) Save();
            }
            return Task.CompletedTask;
        }

        private void Load()
        {
            lock (_lock)
            {
                try
                {
                    if (File.Exists(_filePath))
                    {
                        var json = File.ReadAllText(_filePath);
                        _rules = JsonSerializer.Deserialize<List<MuteRule>>(json) ?? new();
                    }
                }
                catch
                {
                    _rules = new();
                }
            }
        }

        private void Save()
        {
            try
            {
                var json = JsonSerializer.Serialize(_rules, s_jsonOptions);
                File.WriteAllText(_filePath, json);
            }
            catch (Exception ex)
            {
                Helpers.Logger.Error("MuteRuleService: Failed to save mute rules", ex);
            }
        }
    }
}
