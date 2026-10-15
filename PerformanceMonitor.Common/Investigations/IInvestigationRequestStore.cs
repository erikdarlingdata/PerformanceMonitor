/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>
/// The store operations of Level 1 investigations (#5632 Phase A), as one contract each product's store implements (#5820):
/// Darling's PostgreSQL <c>InvestigationRequestStore</c> and, later, Lite's DuckDB store. Every time is UTC. Each store
/// documents how it keeps the open-or-join, claim and submit rules atomic.
/// </summary>
public interface IInvestigationRequestStore
{
    /// <summary>Opens a request for <paramref name="alert"/>, joins an open or claimed one of its server and family inside the
    /// grouping window, or opens nothing at the route's hourly or daily limit.</summary>
    Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken = default);

    /// <summary>Claims a request for <paramref name="agentId"/> under a new claim id and lease.</summary>
    Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, CancellationToken cancellationToken = default);

    /// <summary>Stores the brief composed for a claim; false when <paramref name="claimId"/> no longer holds the request.</summary>
    Task<bool> SetBriefAsync(Guid requestId, Guid claimId, short briefFormat, string briefJson, CancellationToken cancellationToken = default);

    /// <summary>Returns a request whose claim could not be served (the brief failed to compose) to <c>open</c>; the claim
    /// stays counted. False when <paramref name="claimId"/> no longer holds the request.</summary>
    Task<bool> ReleaseAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default);

    /// <summary>Stores a report under its claim and marks the request reported.</summary>
    Task<InvestigationSubmitOutcome> SubmitAsync(InvestigationReportSubmission report, CancellationToken cancellationToken = default);

    /// <summary>Moves a live claim's lease on, capped at the request's expiry.</summary>
    Task<InvestigationRenewResult> RenewAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default);

    /// <summary>The agent that holds <paramref name="claimId"/> on the request, or null when that claim does not hold it.
    /// Writes nothing.</summary>
    Task<string?> ClaimHolderAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default);

    /// <summary>The requests each route opened in the last hour and the last day, keyed by route id.</summary>
    Task<IReadOnlyDictionary<int, (int LastHour, int LastDay)>> RouteUseAsync(CancellationToken cancellationToken = default);

    /// <summary>Journals a refusal the tool layer decided, once per request, operation and reason.</summary>
    Task RecordRefusalAsync(Guid requestId, string agentId, string operation, string reason, string? detailJson,
        CancellationToken cancellationToken = default);

    /// <summary>One request by id with its stored brief; null when no request has that id. Writes nothing.</summary>
    Task<(InvestigationRequestRow Row, string? Brief)?> ReadRequestAsync(Guid requestId,
        CancellationToken cancellationToken = default);

    /// <summary>One request, or the requests opened since <paramref name="sinceUtc"/>, with their reports and journal.
    /// Writes nothing.</summary>
    Task<InvestigationReportsRead> ReadReportsAsync(Guid? requestId, int? serverId, DateTime sinceUtc, int limit,
        int journalLimit, CancellationToken cancellationToken = default);

    /// <summary>Lists requests newest first, with the state computed from the lease and the expiry. Writes nothing.</summary>
    Task<IReadOnlyList<InvestigationRequestRow>> ListAsync(
        string? state, int? serverId, string? family, int limit, CancellationToken cancellationToken = default);

    /// <summary>The requests opened after <paramref name="sinceUtc"/> on <paramref name="routeIds"/> (every route when null
    /// or empty), oldest first. Writes nothing.</summary>
    Task<IReadOnlyList<InvestigationOpenedRow>> OpenedSinceAsync(
        DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken = default);

    /// <summary>The expiry sweep: journals lapsed leases and expires requests past their expiry or out of claims. It never
    /// reopens a lease.</summary>
    Task<InvestigationSweepResult> SweepAsync(CancellationToken cancellationToken = default);
}
