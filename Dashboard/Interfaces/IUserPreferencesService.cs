/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Interfaces
{
    /// <summary>
    /// Interface for user preferences management.
    /// </summary>
    public interface IUserPreferencesService
    {
        /// <summary>
        /// Gets the current user preferences.
        /// </summary>
        UserPreferences GetPreferences();

        /// <summary>
        /// Saves the user preferences.
        /// </summary>
        void SavePreferences(UserPreferences preferences);

        /// <summary>
        /// Updates wait stats time range preferences.
        /// </summary>
        void UpdateWaitStatsRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);

        /// <summary>
        /// Updates CPU time range preferences.
        /// </summary>
        void UpdateCpuRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);

        /// <summary>
        /// Updates memory time range preferences.
        /// </summary>
        void UpdateMemoryRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);

        /// <summary>
        /// Updates file I/O time range preferences.
        /// </summary>
        void UpdateFileIoRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);

        /// <summary>
        /// Updates expensive queries time range preferences.
        /// </summary>
        void UpdateExpensiveQueriesRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);

        /// <summary>
        /// Updates blocking time range preferences.
        /// </summary>
        void UpdateBlockingRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);

        /// <summary>
        /// Updates collection health time range preferences.
        /// </summary>
        void UpdateCollectionHealthRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null);
    }
}
