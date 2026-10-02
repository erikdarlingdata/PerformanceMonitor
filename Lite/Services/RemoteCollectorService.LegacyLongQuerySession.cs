/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Replaces the look for a legacy long-query session that an older install created again: called with the server and the
    /// database (empty on server scope), true when the session exists there. Null in production, where the ensure's own batch
    /// looks.
    /// </summary>
    internal Func<ServerConnection, string, bool>? LegacyLongQuerySessionExistsForTests { get; set; }
}
