/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.IO;
using System.Text.Json;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public class UserPreferencesService : IUserPreferencesService
    {
        private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };
        private readonly object _lock = new();
        private readonly string _preferencesFilePath;
        private UserPreferences _preferences;

        public UserPreferencesService()
        {
            string appDataPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "PerformanceMonitorDashboard"
            );

            Directory.CreateDirectory(appDataPath);
            _preferencesFilePath = Path.Combine(appDataPath, "preferences.json");
            _preferences = LoadPreferences();
        }

        public UserPreferences GetPreferences()
        {
            lock (_lock)
            {
                return _preferences;
            }
        }

        public void SavePreferences(UserPreferences preferences)
        {
            lock (_lock)
            {
                _preferences = preferences;

                string json = JsonSerializer.Serialize(_preferences, s_jsonOptions);
                File.WriteAllText(_preferencesFilePath, json);
            }
        }

        private UserPreferences LoadPreferences()
        {
            if (!File.Exists(_preferencesFilePath))
            {
                return new UserPreferences();
            }

            try
            {
                string json = File.ReadAllText(_preferencesFilePath);
                var preferences = JsonSerializer.Deserialize<UserPreferences>(json);
                return preferences ?? new UserPreferences();
            }
            catch (Exception ex)
            {
                // If preferences file is corrupted, return defaults
                Logger.Warning($"Failed to load preferences file, using defaults: {ex.Message}");
                return new UserPreferences();
            }
        }

        // Helper methods for specific preference updates
        public void UpdateWaitStatsRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.WaitStatsHoursBack = hoursBack;
                _preferences.WaitStatsUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.WaitStatsFromDate = fromDate?.ToString("o");
                _preferences.WaitStatsToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        public void UpdateCpuRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.CpuHoursBack = hoursBack;
                _preferences.CpuUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.CpuFromDate = fromDate?.ToString("o");
                _preferences.CpuToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        public void UpdateMemoryRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.MemoryHoursBack = hoursBack;
                _preferences.MemoryUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.MemoryFromDate = fromDate?.ToString("o");
                _preferences.MemoryToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        public void UpdateFileIoRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.FileIoHoursBack = hoursBack;
                _preferences.FileIoUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.FileIoFromDate = fromDate?.ToString("o");
                _preferences.FileIoToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        public void UpdateExpensiveQueriesRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.ExpensiveQueriesHoursBack = hoursBack;
                _preferences.ExpensiveQueriesUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.ExpensiveQueriesFromDate = fromDate?.ToString("o");
                _preferences.ExpensiveQueriesToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        public void UpdateBlockingRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.BlockingHoursBack = hoursBack;
                _preferences.BlockingUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.BlockingFromDate = fromDate?.ToString("o");
                _preferences.BlockingToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        public void UpdateCollectionHealthRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            lock (_lock)
            {
                _preferences.CollectionHealthHoursBack = hoursBack;
                _preferences.CollectionHealthUseCustomDates = fromDate.HasValue && toDate.HasValue;
                _preferences.CollectionHealthFromDate = fromDate?.ToString("o");
                _preferences.CollectionHealthToDate = toDate?.ToString("o");
                SavePreferencesInternal();
            }
        }

        // Internal method that doesn't acquire lock (caller must hold lock)
        private void SavePreferencesInternal()
        {
            string json = JsonSerializer.Serialize(_preferences, s_jsonOptions);
            File.WriteAllText(_preferencesFilePath, json);
        }
    }
}
