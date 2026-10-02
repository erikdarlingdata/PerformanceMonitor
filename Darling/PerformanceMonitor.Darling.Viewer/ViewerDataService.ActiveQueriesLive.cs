/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's typed wrapper over the Stage-2 <c>fetch_active_queries</c> command — the LIVE "what is running
/// right now" snapshot the store-only viewer cannot read itself. The viewer holds no SqlClient connection to a
/// monitored server, but the SERVICE keeps a live runtime connection to every one, so a "read this server's
/// active queries now" request rides the SAME command channel <c>test_connect</c> / <c>snapshot_now</c> /
/// <c>fetch_plan</c> use: the viewer enqueues a <c>fetch_active_queries</c> row, the service runs the SHARED
/// Active Queries collector's DMV query on the target, and the viewer polls for the rows in <c>result_json</c>
/// and renders them in the same grid shape as the stored Active Queries tab. This is the distinct LIVE,
/// on-demand counterpart of the collector's stored hourly snapshot.
///
/// <para>Like every other command it is a WRITE (enqueue), so a read-only <c>viewer</c> seat throws
/// <see cref="ViewerReadOnlyException"/> from the enqueue — correct, and consistent with Pause / Snapshot /
/// fetch_plan: enqueuing a command is a config write a read-only seat cannot make. The command row is DELETED
/// after its terminal result is read (like <c>test_connect</c> / <c>fetch_plan</c>): a live snapshot's rows —
/// with inline plan XML — can be large and there is no reason to retain them in <c>config_command</c> once the
/// viewer has them.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The command type the viewer enqueues for a live active-queries fetch (must match the service
    /// executor's <c>fetch_active_queries</c> case — Darling.Tests pin the two ends against each other).</summary>
    public const string CommandFetchActiveQueries = "fetch_active_queries";

    /// <summary>The overall poll budget for a live active-queries fetch. Wider than a config write's
    /// <see cref="DefaultCommandTimeout"/> because the service reads a live DMV snapshot (potentially many
    /// running requests with inline plans), but the service caps the SQL read itself at
    /// <c>DarlingWorker.ActiveQueriesFetchTimeoutSeconds</c> (~30s) — comfortably under this — so the viewer
    /// sees the service's real "timed out" outcome rather than a poll miss.</summary>
    public static readonly TimeSpan ActiveQueriesLiveTimeout = TimeSpan.FromSeconds(60);

    private static readonly JsonSerializerOptions s_activeQueriesResultOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Enqueues a <c>fetch_active_queries</c> for one server, polls for its terminal result, deletes the
    /// (possibly large) command row, and maps the outcome to an <see cref="ActiveQueriesLiveResult"/>. A
    /// read-only seat throws <see cref="ViewerReadOnlyException"/> from the enqueue (a read-only seat cannot
    /// command the service). Returns <see cref="ActiveQueriesLiveStatus.TimedOut"/> when the service does not
    /// report within the budget (the caller shows "still reading / try again").
    /// </summary>
    public async Task<ActiveQueriesLiveResult> RequestActiveQueriesLiveAsync(
        int serverId, CancellationToken cancellationToken = default)
    {
        var commandId = await EnqueueCommandAsync(CommandFetchActiveQueries, serverId, argsJson: null, RequestedBy(), cancellationToken);
        var result = await PollCommandResultAsync(commandId, ActiveQueriesLiveTimeout, cancellationToken);

        if (result is not null)
        {
            try
            {
                await DeleteCommandAsync(commandId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The rows-bearing row could not be cleaned up now; the service-side reaper is the backstop.
                   Never let cleanup hide the snapshot the user is waiting on. */
            }
        }

        return ParseActiveQueriesLiveResult(result);
    }

    /// <summary>
    /// Maps a polled command result to an <see cref="ActiveQueriesLiveResult"/> (pure — pinned by Darling.Tests):
    /// a null poll → <see cref="ActiveQueriesLiveStatus.TimedOut"/>; a <c>succeeded</c> row → the running-request
    /// rows parsed from <c>result_json</c> (an empty list when nothing is running); a <c>failed</c> row →
    /// <see cref="ActiveQueriesLiveStatus.Failed"/> with the service's error / status message.
    /// </summary>
    public static ActiveQueriesLiveResult ParseActiveQueriesLiveResult(CommandResult? result)
    {
        if (result is null)
        {
            return new ActiveQueriesLiveResult(ActiveQueriesLiveStatus.TimedOut, new List<ViewerQuerySnapshotRow>(),
                "The service did not return the live active queries in time. It may still be reading — try again.");
        }

        if (result.Status == StatusSucceeded)
        {
            return new ActiveQueriesLiveResult(
                ActiveQueriesLiveStatus.Fetched, DeserializeActiveQueriesRows(result.ResultJson), null);
        }

        /* ReadStringField lives in the LivePlan partial of this same class. */
        var error = ReadStringField(result.ResultJson, "error");
        return new ActiveQueriesLiveResult(ActiveQueriesLiveStatus.Failed, new List<ViewerQuerySnapshotRow>(),
            !string.IsNullOrEmpty(error) ? error
            : !string.IsNullOrEmpty(result.ResultStatus) ? result.ResultStatus
            : "The service could not read the live active queries.");
    }

    /// <summary>
    /// Parses the <c>fetch_active_queries</c> <c>result_json</c> (built by the service's
    /// <c>ActiveQueriesLivePayload.Serialize</c>) into <see cref="ViewerQuerySnapshotRow"/>s — the SAME row model
    /// the stored Active Queries grid binds, so the live and stored views render identically. Each row's
    /// <see cref="ViewerQuerySnapshotRow.CollectionTime"/> is stamped from the payload's <c>fetchedAtUtc</c> (the
    /// DMV row has no capture timestamp — the collector stamps it at store time; the live fetch stamps the fetch
    /// time). Malformed / empty JSON degrades to an empty list, never throws.
    /// </summary>
    private static List<ViewerQuerySnapshotRow> DeserializeActiveQueriesRows(string? json)
    {
        var rows = new List<ViewerQuerySnapshotRow>();
        if (string.IsNullOrWhiteSpace(json))
        {
            return rows;
        }

        ActiveQueriesResultDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<ActiveQueriesResultDto>(json, s_activeQueriesResultOptions);
        }
        catch (JsonException)
        {
            return rows;
        }

        if (dto?.Rows is null)
        {
            return rows;
        }

        var fetchedAt = dto.FetchedAtUtc ?? DateTime.MinValue;
        foreach (var r in dto.Rows)
        {
            rows.Add(new ViewerQuerySnapshotRow
            {
                SessionId = r.SessionId,
                DatabaseName = r.DatabaseName ?? "",
                ElapsedTimeFormatted = r.ElapsedTimeFormatted ?? "",
                QueryText = r.QueryText ?? "",
                Status = r.Status ?? "",
                BlockingSessionId = r.BlockingSessionId,
                WaitType = r.WaitType ?? "",
                WaitTimeMs = r.WaitTimeMs,
                WaitResource = r.WaitResource ?? "",
                CpuTimeMs = r.CpuTimeMs,
                TotalElapsedTimeMs = r.TotalElapsedTimeMs,
                Reads = r.Reads,
                Writes = r.Writes,
                LogicalReads = r.LogicalReads,
                GrantedQueryMemoryGb = r.GrantedQueryMemoryGb,
                TransactionIsolationLevel = r.TransactionIsolationLevel ?? "",
                Dop = r.Dop,
                ParallelWorkerCount = r.ParallelWorkerCount,
                QueryPlan = r.QueryPlan,
                LiveQueryPlan = r.LiveQueryPlan,
                /* #4239: HasQueryPlan/HasLiveQueryPlan are now independent flags (no longer computed from
                   QueryPlan/LiveQueryPlan), so the live path — the only place a snapshot row carries plan XML
                   in-row — must set them explicitly here from that same in-row plan. */
                HasQueryPlan = !string.IsNullOrEmpty(r.QueryPlan),
                HasLiveQueryPlan = !string.IsNullOrEmpty(r.LiveQueryPlan),
                CollectionTime = fetchedAt,
                LoginName = r.LoginName ?? "",
                HostName = r.HostName ?? "",
                ProgramName = r.ProgramName ?? "",
                OpenTransactionCount = r.OpenTransactionCount,
                PercentComplete = r.PercentComplete,
                QueryHash = r.QueryHash ?? "",
                RequestedMemoryMb = r.RequestedMemoryMb ?? 0,
                UsedMemoryMb = r.UsedMemoryMb ?? 0,
                MaxUsedMemoryMb = r.MaxUsedMemoryMb ?? 0,
                TempdbCurrentMb = r.TempdbCurrentMb ?? 0,
                TempdbAllocationsMb = r.TempdbAllocationsMb ?? 0,
                TranLogUsedMb = r.TranLogUsedMb ?? 0,
                TranStartTime = r.TranStartTime,
                RequestId = r.RequestId,
            });
        }

        return rows;
    }

    /// <summary>The wire shape of a <c>fetch_active_queries</c> <c>result_json</c> — the service serializes
    /// <c>QuerySnapshotsCollector.Row</c>s whole; the property names below mirror that row (matched
    /// case-insensitively), so the round-trip test guards the two ends against drift.</summary>
    private sealed class ActiveQueriesResultDto
    {
        public bool Success { get; set; }
        public DateTime? FetchedAtUtc { get; set; }
        public List<ActiveQueryRowDto>? Rows { get; set; }
    }

    private sealed class ActiveQueryRowDto
    {
        public int SessionId { get; set; }
        public string? DatabaseName { get; set; }
        public string? ElapsedTimeFormatted { get; set; }
        public string? QueryText { get; set; }
        public string? QueryPlan { get; set; }
        public string? LiveQueryPlan { get; set; }
        public string? Status { get; set; }
        public int BlockingSessionId { get; set; }
        public string? WaitType { get; set; }
        public long WaitTimeMs { get; set; }
        public string? WaitResource { get; set; }
        public long CpuTimeMs { get; set; }
        public long TotalElapsedTimeMs { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public long LogicalReads { get; set; }
        public double GrantedQueryMemoryGb { get; set; }
        public string? TransactionIsolationLevel { get; set; }
        public int Dop { get; set; }
        public int ParallelWorkerCount { get; set; }
        public string? LoginName { get; set; }
        public string? HostName { get; set; }
        public string? ProgramName { get; set; }
        public int OpenTransactionCount { get; set; }
        public decimal PercentComplete { get; set; }
        public string? QueryHash { get; set; }
        public double? RequestedMemoryMb { get; set; }
        public double? UsedMemoryMb { get; set; }
        public double? MaxUsedMemoryMb { get; set; }
        public double? TempdbCurrentMb { get; set; }
        public double? TempdbAllocationsMb { get; set; }
        public double? TranLogUsedMb { get; set; }
        public DateTime? TranStartTime { get; set; }
        public int RequestId { get; set; }
    }
}

/// <summary>The terminal state of a live active-queries fetch — the viewer's Current Active Queries handler switches on it.</summary>
public enum ActiveQueriesLiveStatus
{
    /// <summary>The live snapshot was read (Rows may be empty when nothing is running).</summary>
    Fetched,

    /// <summary>The service reported a failure (server not connected, connect / SQL / permission error).</summary>
    Failed,

    /// <summary>The service did not report a terminal result within the poll budget.</summary>
    TimedOut,
}

/// <summary>A live active-queries fetch's outcome: the running-request rows on success, else a message to surface.</summary>
public sealed record ActiveQueriesLiveResult(ActiveQueriesLiveStatus Status, List<ViewerQuerySnapshotRow> Rows, string? Message);
