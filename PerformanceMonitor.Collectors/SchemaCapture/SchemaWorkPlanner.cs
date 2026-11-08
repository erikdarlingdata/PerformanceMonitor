/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: one row of <c>collect.schema_capture_state</c>, the capture's cursor. The server row (database name empty) holds the event
/// session's position; a database row holds that database's recovery fork, baseline and last reads.
/// </summary>
public sealed record SchemaCaptureStateRow
{
    public int ServerId { get; init; }
    public string DatabaseName { get; init; } = string.Empty;
    public DateTime? SessionCreateTime { get; init; }
    public long? LastEventSequence { get; init; }
    public DateTime? LastEventTime { get; init; }
    public long? TotalEventsProcessed { get; init; }
    public long? DroppedEventCount { get; init; }
    public Guid? RecoveryForkGuid { get; init; }
    public int? HistoryServerId { get; init; }
    public Guid? HistoryAgGroupId { get; init; }
    public bool? WasPrimary { get; init; }
    public DateTime? BaselineAt { get; init; }
    public DateTime? LastCompleteReadAt { get; init; }
    public DateTime? LastSeenInScopeAt { get; init; }
    public DateTime? LastReconcileAt { get; init; }
    public string? LastReconcileReason { get; init; }
}

/// <summary>What the pass does for one database.</summary>
public enum SchemaDatabaseWork
{
    /// <summary>Nothing to read.</summary>
    Skip,

    /// <summary>First read: every object is recorded as the baseline.</summary>
    Baseline,

    /// <summary>A full read compared with the stored versions.</summary>
    Reconcile,

    /// <summary>A read of the named objects only.</summary>
    Targeted,
}

/// <summary>Where a database's history is kept: the server, or the availability group for a database that belongs to one.</summary>
public readonly record struct SchemaHistoryKey(int? ServerId, Guid? AgGroupId, string? AgName);

/// <summary>
/// #7643: the plan for one database. <see cref="Reason"/> is why a reconcile or a skip happens (null for a plain skip with no events).
/// <see cref="ObjectIds"/> are the objects of a targeted read. <see cref="Attribution"/> are the events that led to the plan, for the
/// login, application and host of the versions it writes.
/// </summary>
public sealed record SchemaDatabasePlan(
    string DatabaseName, SchemaDatabaseWork Work, string? Reason, IReadOnlySet<int> ObjectIds, SchemaHistoryKey Key,
    IReadOnlyList<SchemaEvent> Attribution);

/// <summary>
/// #7643: decides, per database and per pass, whether to skip, baseline, reconcile or read named objects. A pure function of the
/// stored cursor, the server's current state and the events the session recorded, so every rule is testable without a server.
/// </summary>
public static class SchemaWorkPlanner
{
    /* The object_type values the DDL events carry. */
    private const int TriggerType = 21076;
    private const int UserTableType = 8277;
    private const int ViewType = 8278;
    private const int IndexType = 22601;
    private const int SchemaType = 17235;

    /// <summary>The start of the reason a database is skipped while the session does not run.</summary>
    public const string NoChangeTrackingPrefix = "No change tracking: ";

    private const string ObjectAltered = "object_altered";
    private const string ModuleEnd = "module_end";

    private static readonly IReadOnlySet<int> NoIds = new HashSet<int>();

    /// <summary>
    /// The plan for each in-scope database the server reports, in probe order. A database that is not in scope gets no plan: its
    /// events never target or reconcile anything, but they still count for the session's sequence, which is server-wide.
    /// <paramref name="serverState"/> is the server row, or null on the first pass. <paramref name="batch"/> is null when the events
    /// were not read; the cursor is then not advanced, so what the session recorded is read on the next pass.
    ///
    /// <para><b>Rule order</b> (first match wins): not writable (skip), no state (baseline), history key changed (baseline), run now,
    /// session started, session restarted, events lost, restored, became primary, returned to scope, session not running (skip),
    /// schema transfer, renamed, module refreshed, trigger changed, then a targeted read of the named objects. A full read needs no
    /// session, so every rule that reads a database in full for its own state comes before the session-not-running skip.</para>
    /// </summary>
    public static IReadOnlyList<SchemaDatabasePlan> Plan(
        int serverId,
        SchemaCaptureStateRow? serverState,
        IReadOnlyDictionary<string, SchemaCaptureStateRow> dbStates,
        SchemaSessionState session,
        SchemaEventBatch? batch,
        IReadOnlyList<SchemaDatabaseProbe> probes,
        IReadOnlyCollection<string> inScope,
        bool runNow)
    {
        ArgumentNullException.ThrowIfNull(dbStates);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(probes);
        ArgumentNullException.ThrowIfNull(inScope);

        var scope = new HashSet<string>(inScope, StringComparer.OrdinalIgnoreCase);
        var sameSession = IsSameSession(serverState, session.CreateTimeUtc, batch);
        var fresh = Fresh(batch, serverState, sameSession);
        var restarted = serverState?.SessionCreateTime is not null
            && session.CreateTimeUtc is not null
            && !sameSession;
        var eventsLost = sameSession && batch is not null && LostEvents(batch, serverState!, fresh);

        var plans = new List<SchemaDatabasePlan>();
        foreach (var probe in probes)
        {
            if (!scope.Contains(probe.Name))
            {
                continue;
            }

            var key = KeyFor(serverId, probe);
            var notWritable = NotWritableReason(probe);
            if (notWritable is not null)
            {
                plans.Add(Make(probe, SchemaDatabaseWork.Skip, notWritable, key));
                continue;
            }

            if (!dbStates.TryGetValue(probe.Name, out var state))
            {
                plans.Add(Make(probe, SchemaDatabaseWork.Baseline, null, key));
                continue;
            }

            /* The database joined or left an availability group: its history moves to the other key, where it starts again with a
               baseline. The old key's versions age out with retention (DarlingRetention). */
            if ((state.HistoryServerId is not null || state.HistoryAgGroupId is not null)
                && (state.HistoryServerId != key.ServerId || state.HistoryAgGroupId != key.AgGroupId))
            {
                plans.Add(Make(probe, SchemaDatabaseWork.Baseline, "history_key_changed", key));
                continue;
            }

            var events = fresh.Where(e => string.Equals(e.DatabaseName, probe.Name, StringComparison.Ordinal)).ToList();
            plans.Add(PlanDatabase(probe, state, serverState, session, restarted, eventsLost, batch, events, key, runNow));
        }

        return plans;
    }

    private static SchemaDatabasePlan PlanDatabase(
        SchemaDatabaseProbe probe, SchemaCaptureStateRow state, SchemaCaptureStateRow? serverState, SchemaSessionState session,
        bool restarted, bool eventsLost, SchemaEventBatch? batch, List<SchemaEvent> events, SchemaHistoryKey key, bool runNow)
    {
        if (runNow)
        {
            return Reconcile(probe, "run_now", key, events);
        }

        /* A session that runs now where an earlier pass left no create time (the server row has a pass stamp) never ran for this server
           (the grant was missing at the baseline), or was dropped and its position cleared. With no server row, or only the mark a pass
           leaves when it finds the session running, a first pass wrote database rows and failed before it stored its position. Either
           way what changed before it started was recorded by nothing, so every database is read in full, once. */
        if (serverState?.SessionCreateTime is null && session.CreateTimeUtc is not null)
        {
            return Reconcile(probe, "session_started", key, events);
        }

        if (restarted)
        {
            return Reconcile(probe, "session_restarted", key, events);
        }

        if (eventsLost || (batch?.XmlTruncated ?? false))
        {
            return Reconcile(probe, "events_lost", key, events);
        }

        if (probe.RecoveryForkGuid is { } fork && state.RecoveryForkGuid is { } storedFork && fork != storedFork)
        {
            return Reconcile(probe, "restored", key, events);
        }

        if (state.WasPrimary != true && probe.AgGroupId is not null && probe.IsPrimaryReplica == true)
        {
            return Reconcile(probe, "became_primary", key, events);
        }

        if (state.LastSeenInScopeAt is { } seen && serverState?.LastSeenInScopeAt is { } previousPass && seen < previousPass)
        {
            return Reconcile(probe, "returned_to_scope", key, events);
        }

        if (!session.Running)
        {
            return Make(probe, SchemaDatabaseWork.Skip, NoChangeTrackingPrefix + (session.FaultMessage ?? "the schema-changes session is not running."), key);
        }

        if (events.Any(e => e.EventName == ObjectAltered && e.ObjectType == SchemaType))
        {
            return Reconcile(probe, "schema_transfer", key, events);
        }

        if (events.Any(e => e.EventName == ModuleEnd && !IsRefresh(e)))
        {
            return Reconcile(probe, "renamed", key, events);
        }

        /* sp_refreshview and sp_refreshsqlmodule change a view's columns and name no object: the database is read in full. */
        if (events.Any(e => e.EventName == ModuleEnd))
        {
            return Reconcile(probe, "module_refreshed", key, events);
        }

        /* A trigger event names the trigger, and its related_object_id is not the parent table (live-checked), so the table to re-read
           is unknown: the database is read in full. */
        if (events.Any(e => e.ObjectType == TriggerType && e.RelatedObjectId is null))
        {
            return Reconcile(probe, "trigger_changed", key, events);
        }

        var targeted = events.Where(e => e.EventName != ModuleEnd && TargetOf(e) is not null).ToList();
        if (targeted.Count == 0)
        {
            return Make(probe, SchemaDatabaseWork.Skip, null, key);
        }

        var ids = targeted.Select(e => TargetOf(e)!.Value).ToHashSet();
        return new SchemaDatabasePlan(probe.Name, SchemaDatabaseWork.Targeted, null, ids, key, targeted);
    }

    /// <summary>True for the <c>module_end</c> of a view refresh, as opposed to <c>sp_rename</c>.</summary>
    public static bool IsRefresh(SchemaEvent e) =>
        e.EventName == ModuleEnd
        && (string.Equals(e.ObjectName, "sp_refreshview", StringComparison.OrdinalIgnoreCase)
            || string.Equals(e.ObjectName, "sp_refreshsqlmodule", StringComparison.OrdinalIgnoreCase));

    /* Table and view events name their object, and an index event the table it belongs to. A trigger event counts only when it
       carries its parent table. Any other object type is not tracked. */
    private static int? TargetOf(SchemaEvent e) => e.ObjectType switch
    {
        TriggerType => e.RelatedObjectId,
        UserTableType or ViewType or IndexType => e.ObjectId,
        _ => null,
    };

    /// <summary>
    /// #7643: whether the session that runs now is the one <paramref name="stored"/> describes (the continuity rule, used by the plan and
    /// by the cursor's advance alike). It is a different session, started since, when any of these holds:
    /// <list type="bullet">
    /// <item><description>its create time is later than the stored one by a second or more;</description></item>
    /// <item><description>its buffer's total, or its count of dropped events, is lower than the stored one: neither goes back within one
    /// session;</description></item>
    /// <item><description>an event numbered at or below the stored sequence is newer than the stored event. Within one session every event
    /// up to the stored sequence was read with a time at or before the stored one (events are dispatched in order, one partition).</description></item>
    /// </list>
    /// The create time alone is not enough. The server reports one running session's create time a few milliseconds apart on different
    /// reads, and on a server whose tick clock runs faster than the wall clock (seen on a SQL Server 2019 container: about 0.6 s earlier per
    /// second) it moves earlier with every read, as the DMV derives it from the tick count. So an earlier create time, or one later by under
    /// a second, is the same session unless the counters or a reused sequence say otherwise. Only a restart in which neither session
    /// recorded an event is told by the create time alone. A tick clock that runs slow would still read as a restart: that costs a
    /// reconcile, never a missed change.
    /// </summary>
    public static bool IsSameSession(SchemaCaptureStateRow? stored, DateTime? createTimeUtc, SchemaEventBatch? batch)
    {
        if (stored?.SessionCreateTime is not { } storedCreate || createTimeUtc is not { } now || now - storedCreate >= TimeSpan.FromSeconds(1))
        {
            return false;
        }

        if (batch is null)
        {
            return true;
        }

        if (stored.TotalEventsProcessed is { } total && batch.TotalEventsProcessed < total)
        {
            return false;
        }

        if (stored.DroppedEventCount is { } dropped && batch.DroppedEventCount is { } nowDropped && nowDropped < dropped)
        {
            return false;
        }

        return !(stored.LastEventSequence is { } sequence
            && stored.LastEventTime is { } time
            && batch.Events.Any(e => e.Sequence <= sequence && Microseconds(e.TimeUtc) > Microseconds(time)));
    }

    /* The store keeps a timestamp to the microsecond, so both sides are compared at that precision. */
    private static long Microseconds(DateTime value) => value.Ticks / 10;

    /* The events the stored cursor has not seen. With the same session, those after the stored sequence: the read starts a second
       before the stored event's time so a tie at one timestamp is not missed, and the sequence tells the read ones apart. */
    private static IReadOnlyList<SchemaEvent> Fresh(SchemaEventBatch? batch, SchemaCaptureStateRow? serverState, bool sameSession)
    {
        if (batch is null)
        {
            return [];
        }

        return sameSession && serverState?.LastEventSequence is { } stored
            ? batch.Events.Where(e => e.Sequence > stored).ToList()
            : batch.Events;
    }

    /* A gap in what the session saw: the first new event does not follow the stored one, the buffer took more than was read, the
       session dropped events before they reached the buffer, or two new events are not consecutive. */
    private static bool LostEvents(SchemaEventBatch batch, SchemaCaptureStateRow serverState, IReadOnlyList<SchemaEvent> fresh)
    {
        if (serverState.LastEventSequence is { } stored && fresh.Count > 0 && fresh.Min(e => e.Sequence) > stored + 1)
        {
            return true;
        }

        if (serverState.DroppedEventCount is { } dropped && batch.DroppedEventCount is { } nowDropped && nowDropped > dropped)
        {
            return true;
        }

        var sequences = fresh.Select(e => e.Sequence).Order().ToList();
        for (var i = 1; i < sequences.Count; i++)
        {
            if (sequences[i] > sequences[i - 1] + 1)
            {
                return true;
            }
        }

        return serverState.TotalEventsProcessed is { } storedTotal && batch.TotalEventsProcessed - storedTotal > fresh.Count;
    }

    private static string? NotWritableReason(SchemaDatabaseProbe p)
    {
        if (p.SourceDatabaseId is not null)
        {
            return "The database is a snapshot.";
        }

        if (!string.Equals(p.StateDesc, "ONLINE", StringComparison.OrdinalIgnoreCase))
        {
            return $"The database is {p.StateDesc.ToLowerInvariant()}, not online.";
        }

        if (p.IsInStandby)
        {
            return "The database is in standby.";
        }

        if (p.IsReadOnly)
        {
            return "The database is read-only.";
        }

        if (!p.HasAccess)
        {
            return "The login has no access to the database.";
        }

        if (p.AgGroupId is not null && p.IsPrimaryReplica != true)
        {
            return "The database is a secondary replica.";
        }

        return null;
    }

    private static SchemaHistoryKey KeyFor(int serverId, SchemaDatabaseProbe p) =>
        p.AgGroupId is { } group ? new SchemaHistoryKey(null, group, p.AgName) : new SchemaHistoryKey(serverId, null, null);

    private static SchemaDatabasePlan Make(SchemaDatabaseProbe p, SchemaDatabaseWork work, string? reason, SchemaHistoryKey key) =>
        new(p.Name, work, reason, NoIds, key, []);

    private static SchemaDatabasePlan Reconcile(SchemaDatabaseProbe p, string reason, SchemaHistoryKey key, IReadOnlyList<SchemaEvent> events) =>
        new(p.Name, SchemaDatabaseWork.Reconcile, reason, NoIds, key, events);
}
