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
/// What the store holds about an instance restart and a rise in memory dumps, for the Instance Restarted and Memory Dumps alerts (#8884).
/// Each member is null when the store holds no such event. The read adapters build it from the rows the collectors persisted in
/// <c>collector_state</c> (identity pair, memory dump stamp), through the shared judges in <c>PerformanceMonitor.Collectors.InstanceEvents</c>.
/// The adapter only says what the store holds. The engine decides whether it is recent enough and whether it fired already.
/// </summary>
public sealed record InstanceEventInfo(InstanceRestartInfo? Restart, MemoryDumpInfo? MemoryDumps);

/// <summary>
/// A restart: the instance start time moved later while the server name did not. The two start times are the SERVER's local clock, as
/// <c>sqlserver_start_time</c> reports it, and are only compared with each other and shown as the server reports them.
/// <see cref="ObservedAtUtc"/> is when a collector persisted the change (UTC).
/// </summary>
public sealed record InstanceRestartInfo(DateTime PreviousStartTime, DateTime NewStartTime, DateTime ObservedAtUtc);

/// <summary>
/// A rise in the newest memory dump: <see cref="NewestDumpUtc"/> is later than the previous stamp's. <see cref="TotalCount"/> is the dump
/// count, <see cref="RecentCount"/> the dumps created in the last ten minutes as the collector read them, and <see cref="NewCount"/> how many
/// dumps arrived since the previous stamp (at least one). <see cref="ObservedAtUtc"/> is when a collector persisted the stamp (UTC).
/// </summary>
public sealed record MemoryDumpInfo(DateTime NewestDumpUtc, long TotalCount, long RecentCount, long NewCount, DateTime ObservedAtUtc);
