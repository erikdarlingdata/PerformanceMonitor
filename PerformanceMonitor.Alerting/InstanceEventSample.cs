/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One collected <c>server_properties</c> row, reduced to what the Instance Restarted and Memory Dumps alerts compare (#8884).
///
/// <para>The alerts are EVENTS, not standing conditions: each fires when a value moved between two collections, never because a
/// value is high. So the read hands the engine the recent samples and the engine compares neighbours. The three values come from
/// the same collection, so they describe one moment.</para>
///
/// <para><see cref="SqlServerStartTime"/> and <see cref="NewestMemoryDumpTime"/> are the SERVER's local wall clock, stored as the
/// DMVs report them. They are only ever compared with each other (a later start time, a newer dump), and are shown in the alert
/// text as the server reports them, so no offset is applied.</para>
/// </summary>
public sealed class InstanceEventSample
{
    /// <summary>When the row was collected, UTC. Orders the samples and bounds the 10-minute dump window.</summary>
    public DateTime CollectionTimeUtc { get; set; }

    /// <summary><c>sys.dm_os_sys_info.sqlserver_start_time</c>; null when the row could not read it.</summary>
    public DateTime? SqlServerStartTime { get; set; }

    /// <summary><c>COUNT(*)</c> of <c>sys.dm_server_memory_dumps</c>; null on an engine without the view (Azure SQL Database).</summary>
    public int? MemoryDumpCount { get; set; }

    /// <summary><c>MAX(creation_time)</c> of <c>sys.dm_server_memory_dumps</c>; null with no dump, without the view, or on a row
    /// stored before the column existed.</summary>
    public DateTime? NewestMemoryDumpTime { get; set; }

    /// <summary>How many collector runs for this server FAILED between the previous sample and this one, from the collection log.
    /// Zero for the oldest sample, which has no previous. A restart that took the server away from the monitor leaves failed runs here,
    /// which is how the Instance Restarted alert says the server was unreachable in between.</summary>
    public int FailedCollectionsSincePrevious { get; set; }
}
