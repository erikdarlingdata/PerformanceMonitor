/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7590, #8464: the registry's <c>connect_down_since</c> as every reader takes it, healed in the statement that reads it. The flag is
/// the first failure of the current outage and is cleared by one write per connect that is allowed to fail, so a reader never trusts it
/// alone: a collector row newer than the flag proves a later connect worked, because a collector runs only over an open connection, and a
/// flag older than the server's newest collector row reads as NULL. Any collector row proves the connect, a failed one included: a server
/// whose login connects but lacks VIEW SERVER STATE writes a PERMISSIONS row every cycle, and it is connected. This is the test
/// <c>MarkConnectFailedSql</c> applies before it keeps a stored flag and the one <c>get_collection_health</c>'s <c>server_connection</c>
/// read applies, so the Overview card, the sidebar dot, <c>list_servers</c>, the Admin grid, the fleet card, every offline note and
/// <c>get_collection_health</c> agree on whether the service can connect. The newest SUCCESS run, which those readers band freshness and
/// print "last collected" from (<c>CollectionRunSql.SuccessfulCollectorRun</c>), answers a different question: a server can connect and
/// still collect nothing.
/// </summary>
public static class ConnectDownSinceSql
{
    /// <summary>The healed <c>connect_down_since</c> of a <c>servers</c> row aliased <c>s</c>, as a select-list expression with no alias of
    /// its own. The EXISTS is one index probe on (server_id, collection_time) above the flag's instant.</summary>
    public const string Healed =
        "CASE WHEN s.connect_down_since IS NOT NULL AND EXISTS (SELECT 1 FROM v_collection_log cl WHERE cl.server_id = s.server_id"
        + " AND cl.collection_time > s.connect_down_since AND cl." + CollectionCycleRow.NotCycleRowSql + ") THEN NULL ELSE s.connect_down_since END";
}
