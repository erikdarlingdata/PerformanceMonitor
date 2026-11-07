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
        var sameSession = IsSameSession(serverState, session);
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

            var events = fresh.Where(e => string.Equals(e.DatabaseName, probe.Name, StringComparison.OrdinalIgnoreCase)).ToList();
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

        if (!session.Running)
        {
            return Make(probe, SchemaDatabaseWork.Skip, NoChangeTrackingPrefix + (session.FaultMessage ?? "the schema-changes session is not running."), key);
        }

        /* A session that runs now where an earlier pass left no create time (the server row has a pass stamp) never ran for this server (the grant was missing at the baseline), or was dropped
           and its position cleared. What changed before it started was recorded by nothing, so every database is read in full, once. */
        if (serverState is { SessionCreateTime: null, LastSeenInScopeAt: not null } && session.CreateTimeUtc is not null)
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

        if (events.Any(e => e.EventName == ObjectAltered && e.ObjectType == SchemaType))
        {
            return Reconcile(probe, "schema_transfer", key, events);
        }

        if (events.Any(e => e.EventName == ModuleEnd))
        {
            return Reconcile(probe, "renamed", key, events);
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

    /* Table and view events name their object, and an index event the table it belongs to. A trigger event counts only when it
       carries its parent table. Any other object type is not tracked. */
    private static int? TargetOf(SchemaEvent e) => e.ObjectType switch
    {
        TriggerType => e.RelatedObjectId,
        UserTableType or ViewType or IndexType => e.ObjectId,
        _ => null,
    };

    /* The create time the server reports for one running session is not stable to the millisecond: two reads of the same session were
       seen a few milliseconds apart. A real restart is a stop and a start, seconds apart at the least, so a second is the tolerance. */
    public static bool IsSameSession(SchemaCaptureStateRow? serverState, SchemaSessionState session) =>
        serverState?.SessionCreateTime is { } stored
        && session.CreateTimeUtc is { } now
        && (stored - now).Duration() < TimeSpan.FromSeconds(1);

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

    private static bool LostEvents(SchemaEventBatch batch, SchemaCaptureStateRow serverState, IReadOnlyList<SchemaEvent> fresh)
    {
        if (serverState.LastEventSequence is { } stored && fresh.Count > 0 && fresh.Min(e => e.Sequence) > stored + 1)
        {
            return true;
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
