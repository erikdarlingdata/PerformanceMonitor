using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Manages alert mute rules with JSON persistence.
    /// Thread-safe: all operations are protected by _lock.
    /// </summary>
    public class MuteRuleService
    {
        private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

        private readonly string _filePath;
        private readonly object _lock = new object();
        private List<MuteRule> _rules = new();

        public MuteRuleService()
        {
            var appDataDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "PerformanceMonitorDashboard");
            Directory.CreateDirectory(appDataDir);
            _filePath = Path.Combine(appDataDir, "alert_mute_rules.json");
            Load();
            PurgeExpiredRules();
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

        public void AddRule(MuteRule rule)
        {
            lock (_lock)
            {
                _rules.Add(rule);
                Save();
            }
        }

        public void RemoveRule(string ruleId)
        {
            lock (_lock)
            {
                _rules.RemoveAll(r => r.Id == ruleId);
                Save();
            }
        }

        public void UpdateRule(MuteRule updated)
        {
            lock (_lock)
            {
                var index = _rules.FindIndex(r => r.Id == updated.Id);
                if (index >= 0)
                {
                    _rules[index] = updated;
                    Save();
                }
            }
        }

        public void SetRuleEnabled(string ruleId, bool enabled)
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
        }

        /// <summary>
        /// Removes all expired rules from the list.
        /// </summary>
        public int PurgeExpiredRules()
        {
            lock (_lock)
            {
                int removed = _rules.RemoveAll(r => r.IsExpired);
                if (removed > 0) Save();
                return removed;
            }
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
