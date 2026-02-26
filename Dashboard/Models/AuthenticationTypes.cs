/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// Constants for server authentication types.
    /// </summary>
    public static class AuthenticationTypes
    {
        /// <summary>
        /// Windows integrated authentication.
        /// </summary>
        public const string Windows = "Windows";

        /// <summary>
        /// SQL Server username/password authentication.
        /// </summary>
        public const string SqlServer = "SqlServer";

        /// <summary>
        /// Microsoft Entra MFA (Azure AD) interactive authentication.
        /// </summary>
        public const string EntraMFA = "EntraMFA";
    }
}
