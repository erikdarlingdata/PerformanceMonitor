/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// Severity levels for health indicators.
    /// </summary>
    public enum HealthSeverity
    {
        Unknown,
        Healthy,
        Warning,
        Critical
    }

    /// <summary>
    /// Represents the real-time health status of a server for the NOC landing page.
    /// Includes connection status plus all live health metrics.
    /// </summary>
    public class ServerHealthStatus : INotifyPropertyChanged
    {
        private readonly ServerConnection _server;
        private bool _isLoading;
        private bool? _isOnline;
        private string? _errorMessage;
        private DateTime? _lastUpdated;

        // CPU metrics
        private int? _cpuPercent;
        private int? _otherCpuPercent;

        // Memory metrics
        private decimal? _bufferPoolGb;
        private decimal? _grantedMemoryGb;
        private decimal? _usedMemoryGb;
        private int _requestsWaitingForMemory;

        // Blocking metrics
        private long _totalBlocked;
        private decimal _longestBlockedSeconds;
        private int? _lastBlockingMinutesAgo;

        // Thread metrics
        private int _totalThreads;
        private int _availableThreads;
        private int _threadsWaitingForCpu;
        private int _requestsWaitingForThreads;

        // Deadlocks (cumulative counter - delta tracked separately)
        private long _deadlockCount;
        private long _previousDeadlockCount;
        private bool _isFirstDeadlockRead = true;
        private int? _lastDeadlockMinutesAgo;

        // Collector health
        private int _healthyCollectorCount;
        private int _failedCollectorCount;

        // Top waits
        private string? _topWaitType;
        private decimal _topWaitDurationSeconds;

        public ServerHealthStatus(ServerConnection server)
        {
            _server = server;
        }

        public ServerConnection Server => _server;
        public string ServerId => _server.Id;
        public string DisplayName => _server.DisplayName;
        public string ServerName => _server.ServerName;

        public bool IsLoading
        {
            get => _isLoading;
            set { _isLoading = value; OnPropertyChanged(); }
        }

        public bool? IsOnline
        {
            get => _isOnline;
            set
            {
                _isOnline = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ConnectionSeverity));
                OnPropertyChanged(nameof(ConnectionStatusText));
            }
        }

        public string? ErrorMessage
        {
            get => _errorMessage;
            set { _errorMessage = value; OnPropertyChanged(); }
        }

        public DateTime? LastUpdated
        {
            get => _lastUpdated;
            set
            {
                _lastUpdated = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(LastUpdatedDisplay));
            }
        }

        public string LastUpdatedDisplay
        {
            get
            {
                if (!_lastUpdated.HasValue)
                    return "Never";

                var elapsed = DateTime.Now - _lastUpdated.Value;

                if (elapsed.TotalSeconds < 60)
                    return "Just now";

                if (elapsed.TotalMinutes < 60)
                    return $"{(int)elapsed.TotalMinutes}m ago";

                return $"{(int)elapsed.TotalHours}h ago";
            }
        }

        // CPU (SQL Server process)
        public int? CpuPercent
        {
            get => _cpuPercent;
            set
            {
                _cpuPercent = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(TotalCpuPercent));
                OnPropertyChanged(nameof(CpuSeverity));
                OnPropertyChanged(nameof(CpuDisplayText));
                OnPropertyChanged(nameof(CpuDetailText));
            }
        }

        // Other CPU (non-SQL processes)
        public int? OtherCpuPercent
        {
            get => _otherCpuPercent;
            set
            {
                _otherCpuPercent = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(TotalCpuPercent));
                OnPropertyChanged(nameof(CpuSeverity));
                OnPropertyChanged(nameof(CpuDisplayText));
                OnPropertyChanged(nameof(CpuDetailText));
            }
        }

        // Total CPU = SQL + Other
        public int? TotalCpuPercent
        {
            get
            {
                if (!_cpuPercent.HasValue && !_otherCpuPercent.HasValue) return null;
                return (_cpuPercent ?? 0) + (_otherCpuPercent ?? 0);
            }
        }

        public HealthSeverity CpuSeverity
        {
            get
            {
                var total = TotalCpuPercent;
                if (!total.HasValue) return HealthSeverity.Unknown;
                if (total >= 95) return HealthSeverity.Critical;
                if (total >= 80) return HealthSeverity.Warning;
                return HealthSeverity.Healthy;
            }
        }

        public string CpuDisplayText => TotalCpuPercent.HasValue ? $"{TotalCpuPercent}%" : "--";

        public string CpuDetailText
        {
            get
            {
                if (!_cpuPercent.HasValue && !_otherCpuPercent.HasValue) return "";
                return $"SQL: {_cpuPercent ?? 0}% Other: {_otherCpuPercent ?? 0}%";
            }
        }

        // Memory
        public decimal? BufferPoolGb
        {
            get => _bufferPoolGb;
            set { _bufferPoolGb = value; OnPropertyChanged(); OnPropertyChanged(nameof(MemoryDetailText)); }
        }

        public decimal? GrantedMemoryGb
        {
            get => _grantedMemoryGb;
            set { _grantedMemoryGb = value; OnPropertyChanged(); OnPropertyChanged(nameof(MemoryDetailText)); }
        }

        public decimal? UsedMemoryGb
        {
            get => _usedMemoryGb;
            set { _usedMemoryGb = value; OnPropertyChanged(); OnPropertyChanged(nameof(MemoryDetailText)); }
        }

        public int RequestsWaitingForMemory
        {
            get => _requestsWaitingForMemory;
            set
            {
                _requestsWaitingForMemory = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(MemorySeverity));
                OnPropertyChanged(nameof(MemoryDisplayText));
            }
        }

        public HealthSeverity MemorySeverity
        {
            get
            {
                if (_requestsWaitingForMemory > 0) return HealthSeverity.Critical;
                return HealthSeverity.Healthy;
            }
        }

        public string MemoryDisplayText => _requestsWaitingForMemory > 0 ? $"{_requestsWaitingForMemory} waiting" : "OK";

        public string MemoryDetailText
        {
            get
            {
                if (!_bufferPoolGb.HasValue) return "";
                var bp = $"BP: {_bufferPoolGb:F1}GB";
                var qmg = _grantedMemoryGb.HasValue ? $"QMG: {_grantedMemoryGb:F1}GB" : "";
                return string.IsNullOrEmpty(qmg) ? bp : $"{bp}, {qmg}";
            }
        }

        // Blocking
        public long TotalBlocked
        {
            get => _totalBlocked;
            set
            {
                _totalBlocked = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(BlockingSeverity));
                OnPropertyChanged(nameof(BlockingDisplayText));
                OnPropertyChanged(nameof(BlockingDetailText));
            }
        }

        public decimal LongestBlockedSeconds
        {
            get => _longestBlockedSeconds;
            set
            {
                _longestBlockedSeconds = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(BlockingSeverity));
                OnPropertyChanged(nameof(BlockingDisplayText));
                OnPropertyChanged(nameof(BlockingDetailText));
            }
        }

        public HealthSeverity BlockingSeverity
        {
            get
            {
                // Critical: long blocking OR many sessions blocked
                if (_longestBlockedSeconds >= 60) return HealthSeverity.Critical;
                if (_totalBlocked >= 5) return HealthSeverity.Critical;
                
                // Warning: moderate blocking
                if (_longestBlockedSeconds >= 10) return HealthSeverity.Warning;
                if (_totalBlocked >= 2) return HealthSeverity.Warning;
                
                // Any blocking at all is at least a warning
                if (_totalBlocked > 0) return HealthSeverity.Warning;
                
                return HealthSeverity.Healthy;
            }
        }

        public string BlockingDisplayText
        {
            get
            {
                if (_totalBlocked == 0) return "0";
                return $"{_totalBlocked}";
            }
        }

        public int? LastBlockingMinutesAgo
        {
            get => _lastBlockingMinutesAgo;
            set
            {
                _lastBlockingMinutesAgo = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(BlockingDetailText));
            }
        }

        public string BlockingDetailText
        {
            get
            {
                if (_totalBlocked > 0)
                    return $"max: {_longestBlockedSeconds:F0}s";
                if (!_lastBlockingMinutesAgo.HasValue)
                    return "Last: Unknown";
                return $"Last: {FormatMinutesAgo(_lastBlockingMinutesAgo.Value)}";
            }
        }

        // Threads
        public int TotalThreads
        {
            get => _totalThreads;
            set
            {
                _totalThreads = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ThreadsSeverity));
                OnPropertyChanged(nameof(ThreadsDetailText));
            }
        }

        public int AvailableThreads
        {
            get => _availableThreads;
            set
            {
                _availableThreads = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ThreadsSeverity));
                OnPropertyChanged(nameof(ThreadsDetailText));
            }
        }

        public int ThreadsWaitingForCpu
        {
            get => _threadsWaitingForCpu;
            set { _threadsWaitingForCpu = value; OnPropertyChanged(); }
        }

        public int RequestsWaitingForThreads
        {
            get => _requestsWaitingForThreads;
            set
            {
                _requestsWaitingForThreads = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ThreadsSeverity));
                OnPropertyChanged(nameof(ThreadsDisplayText));
            }
        }

        public HealthSeverity ThreadsSeverity
        {
            get
            {
                // Critical: thread starvation (work queue backing up)
                if (_requestsWaitingForThreads > 0) return HealthSeverity.Critical;

                // Warning: high CPU pressure (many runnable tasks waiting)
                if (_threadsWaitingForCpu >= 20) return HealthSeverity.Warning;

                // Warning: less than 10% of threads available
                if (_totalThreads > 0 && _availableThreads < _totalThreads * 0.10) return HealthSeverity.Warning;

                return HealthSeverity.Healthy;
            }
        }

        public string ThreadsDisplayText
        {
            get
            {
                if (_requestsWaitingForThreads > 0) return $"{_requestsWaitingForThreads} starved";
                if (_threadsWaitingForCpu >= 20) return $"{_threadsWaitingForCpu} runnable";
                if (_totalThreads > 0 && _availableThreads < _totalThreads * 0.10) return "Low";
                return "OK";
            }
        }

        public string ThreadsDetailText => _totalThreads > 0 ? $"Available: {_availableThreads}/{_totalThreads}" : "";

        // Deadlocks
        public long DeadlockCount
        {
            get => _deadlockCount;
            set
            {
                if (_isFirstDeadlockRead)
                {
                    // On first load, set previous = current so delta starts at 0
                    _previousDeadlockCount = value;
                    _isFirstDeadlockRead = false;
                }
                else
                {
                    _previousDeadlockCount = _deadlockCount;
                }
                _deadlockCount = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(DeadlocksSinceLastCheck));
                OnPropertyChanged(nameof(DeadlockSeverity));
                OnPropertyChanged(nameof(DeadlockDisplayText));
            }
        }

        public long DeadlocksSinceLastCheck => _deadlockCount - _previousDeadlockCount;

        public HealthSeverity DeadlockSeverity
        {
            get
            {
                if (DeadlocksSinceLastCheck > 0) return HealthSeverity.Critical;
                if (_lastDeadlockMinutesAgo.HasValue && _lastDeadlockMinutesAgo.Value <= 10) return HealthSeverity.Critical;
                if (_lastDeadlockMinutesAgo.HasValue && _lastDeadlockMinutesAgo.Value <= 60) return HealthSeverity.Warning;
                return HealthSeverity.Healthy;
            }
        }

        public string DeadlockDisplayText => DeadlocksSinceLastCheck > 0 ? $"+{DeadlocksSinceLastCheck}" : "0";

        public int? LastDeadlockMinutesAgo
        {
            get => _lastDeadlockMinutesAgo;
            set
            {
                _lastDeadlockMinutesAgo = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(DeadlockDetailText));
                OnPropertyChanged(nameof(DeadlockSeverity));
            }
        }

        public string DeadlockDetailText
        {
            get
            {
                if (!_lastDeadlockMinutesAgo.HasValue)
                    return "Last: Unknown";
                return $"Last: {FormatMinutesAgo(_lastDeadlockMinutesAgo.Value)}";
            }
        }

        // Collector health
        public int HealthyCollectorCount
        {
            get => _healthyCollectorCount;
            set
            {
                _healthyCollectorCount = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(CollectorDetailText));
            }
        }

        public int FailedCollectorCount
        {
            get => _failedCollectorCount;
            set
            {
                _failedCollectorCount = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(CollectorSeverity));
                OnPropertyChanged(nameof(CollectorDisplayText));
                OnPropertyChanged(nameof(CollectorDetailText));
            }
        }

        public HealthSeverity CollectorSeverity
        {
            get
            {
                if (_failedCollectorCount > 0) return HealthSeverity.Warning;
                return HealthSeverity.Healthy;
            }
        }

        public string CollectorDisplayText => _failedCollectorCount > 0 ? $"{_failedCollectorCount} failed" : "OK";

        public string CollectorDetailText => $"Healthy: {_healthyCollectorCount}, Failing: {_failedCollectorCount}";

        // Top waits
        public string? TopWaitType
        {
            get => _topWaitType;
            set { _topWaitType = value; OnPropertyChanged(); OnPropertyChanged(nameof(TopWaitDisplayText)); }
        }

        public decimal TopWaitDurationSeconds
        {
            get => _topWaitDurationSeconds;
            set { _topWaitDurationSeconds = value; OnPropertyChanged(); OnPropertyChanged(nameof(TopWaitDisplayText)); }
        }

        public string TopWaitDisplayText
        {
            get
            {
                if (string.IsNullOrEmpty(_topWaitType)) return "--";
                return $"{_topWaitType} ({_topWaitDurationSeconds:F0}s)";
            }
        }

        // Connection status
        public HealthSeverity ConnectionSeverity
        {
            get
            {
                if (!_isOnline.HasValue) return HealthSeverity.Unknown;
                return _isOnline.Value ? HealthSeverity.Healthy : HealthSeverity.Critical;
            }
        }

        public string ConnectionStatusText
        {
            get
            {
                if (!_isOnline.HasValue) return "Unknown";
                return _isOnline.Value ? "Online" : "Offline";
            }
        }

        // Overall health - worst severity across all metrics
        public HealthSeverity OverallSeverity
        {
            get
            {
                if (_isOnline != true) return HealthSeverity.Critical;

                var severities = new[]
                {
                    CpuSeverity,
                    MemorySeverity,
                    BlockingSeverity,
                    ThreadsSeverity,
                    DeadlockSeverity,
                    CollectorSeverity
                };

                var worst = HealthSeverity.Healthy;
                foreach (var s in severities)
                {
                    if (s == HealthSeverity.Critical) return HealthSeverity.Critical;
                    if (s == HealthSeverity.Warning) worst = HealthSeverity.Warning;
                }
                return worst;
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private static string FormatMinutesAgo(int minutes)
        {
            if (minutes < 1)
                return "just now";
            if (minutes < 60)
                return $"{minutes}m ago";
            if (minutes < 1440) // 24 hours
                return $"{minutes / 60}h ago";
            if (minutes < 10080) // 7 days
                return $"{minutes / 1440}d ago";
            return $"{minutes / 10080}w ago";
        }

        /// <summary>
        /// Refreshes the timestamp display without re-querying.
        /// </summary>
        public void RefreshTimestampDisplay()
        {
            OnPropertyChanged(nameof(LastUpdatedDisplay));
        }

        /// <summary>
        /// Notifies UI that overall severity may have changed.
        /// </summary>
        public void NotifyOverallSeverityChanged()
        {
            OnPropertyChanged(nameof(OverallSeverity));
        }
    }
}
