/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    /// <summary>#7068: true when a "AG Sync Fell Behind" fire row's context carries this availability group, database and replica
    /// as the exact field values <see cref="ForDatabase"/> wrote. Equality on each field, so database <c>Sales</c> does not
    /// own a row written for <c>Sales2</c>.</summary>
    public static bool OwnsSyncFire(string? contextJson, string agName, string database, string replica) =>
        AlertContextSerializer.ContextJsonHasDetailFields(
            contextJson, ("Database", database), ("Availability Group", agName), ("Replica", replica));

    /// <summary>#7068: true when an "AG Sync Recovered" row's message is this grain's. Both apps word it "database D in AG G on
    /// replica R has caught up with the primary" (Darling in lower case, Lite with a capital), so the match ignores case. The
    /// closing words end the replica name, so replica <c>R</c> does not own a clear written for <c>R2</c>.</summary>
    public static bool OwnsSyncClear(string? detailText, string agName, string database, string replica) =>
        detailText is { } text
        && text.Contains($"database {database} in AG {agName} on replica {replica} has caught up with the primary", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The incident identity for the AG disconnect/reconnect pair: one replica within one group. A single
    /// "AG:replica" member (not two), so it can't be set half-way. Only this pair carries it — every other
    /// grain's incidents answer through fingerprints already.
    /// </summary>
    public static AlertContext ForReplica(string agName, string replicaServerName) =>
        new() { AgReplicaIdentity = $"{agName}:{replicaServerName}" };
}
