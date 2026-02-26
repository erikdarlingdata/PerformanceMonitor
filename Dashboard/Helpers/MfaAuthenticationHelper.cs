/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorDashboard.Helpers
{
    /// <summary>
    /// Helper utilities for Microsoft Entra MFA authentication.
    /// </summary>
    public static class MfaAuthenticationHelper
    {
        /// <summary>
        /// Checks if an exception indicates that the user cancelled MFA authentication.
        /// </summary>
        /// <param name="ex">The exception to check.</param>
        /// <returns>True if the exception represents user cancellation, false otherwise.</returns>
        public static bool IsMfaCancelledException(Exception ex)
        {
            var message = ex.Message?.ToLowerInvariant() ?? string.Empty;

            // Only treat explicit user cancellation messages as cancellation
            // Do NOT treat authentication errors (wrong password, account selection, etc.) as cancellation
            return message.Contains("user canceled") ||
                   message.Contains("user cancelled") ||
                   message.Contains("authentication was cancelled") ||
                   message.Contains("authentication was canceled");
        }
    }
}
