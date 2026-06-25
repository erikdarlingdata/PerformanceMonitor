/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace PerformanceMonitorDashboard.Models
{
    public class CollectorScheduleItem : INotifyPropertyChanged
    {
        private bool _enabled;
        private int _frequencyMinutes;
        private int _retentionDays;

        public int ScheduleId { get; set; }
        public string CollectorName { get; set; } = string.Empty;

        public bool Enabled
        {
            get => _enabled;
            set { if (_enabled != value) { _enabled = value; OnPropertyChanged(); } }
        }

        public int FrequencyMinutes
        {
            get => _frequencyMinutes;
            set { if (_frequencyMinutes != value) { _frequencyMinutes = value; OnPropertyChanged(); } }
        }

        public int RetentionDays
        {
            get => _retentionDays;
            set { if (_retentionDays != value) { _retentionDays = value; OnPropertyChanged(); } }
        }

        public DateTime? LastRunTime { get; set; }
        public DateTime? NextRunTime { get; set; }
        public string? Description { get; set; }

        /// <summary>
        /// Display-friendly collector name (removes _collector suffix and formats)
        /// </summary>
        public string DisplayName
        {
            get
            {
                var name = CollectorName
                    .Replace("_collector", "", StringComparison.OrdinalIgnoreCase)
                    .Replace("_", " ");

                // Title case
                if (string.IsNullOrEmpty(name)) return name;
                var words = name.Split(' ');
                for (int i = 0; i < words.Length; i++)
                {
                    if (words[i].Length > 0)
                    {
                        words[i] = char.ToUpper(words[i][0], CultureInfo.InvariantCulture) + words[i][1..].ToLowerInvariant();
                    }
                }
                return string.Join(" ", words);
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        protected void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
