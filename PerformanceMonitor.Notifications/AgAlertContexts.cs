/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The discrete facts for a database-scoped AG alert (#2109): Database, Availability Group, and
/// Replica as fields a webhook consumer can read by name — the same names the prose detail already
/// speaks, now structured. Shared by both SKUs' AG evaluators for the same reason
/// <c>AgAlertPolicy</c> is: the fact NAMES are a wire contract downstream automation keys on, and
/// two hand-rolled copies would drift. Takes plain strings rather than <c>AgDatabaseReading</c>
/// because this project cannot see Common — the caller passes the reading's members.
/// </summary>
public static class AgAlertContexts
{
    /// <summary>One detail item headed by the database (matching the other per-database builders),
    /// carrying the identity triple plus any alert-specific extras (e.g. Suspend Reason).</summary>
    public static AlertContext ForDatabase(
        string database, string agName, string replica, params (string Label, string Value)[] extras)
    {
        var fields = new List<(string, string)>
        {
            ("Database", database),
            ("Availability Group", agName),
            ("Replica", replica)
        };
        fields.AddRange(extras);

        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = database, Fields = fields });
        return context;
    }
}
