/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Binds the (server_id, window start, window end) triple the moved shared reads take (#6388), the way the
/// Service project's <c>DarlingMcpReadParameters.AddWindow</c> does: both timestamps bind <c>Kind=Unspecified</c> so they
/// map to the store's <c>timestamp without time zone</c> columns.</summary>
internal static class StoreReadParameters
{
    public static void AddWindow(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
    }
}
