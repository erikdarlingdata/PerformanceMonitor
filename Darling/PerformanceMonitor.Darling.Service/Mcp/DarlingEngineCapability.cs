/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store half of the #2511 engine-capability answer: reads this server's probed engine edition and, when
/// the collector serving a read cannot run on that engine, returns the <c>not_collected</c> envelope both
/// SKUs emit. The DECISION and the WORDS both come from
/// <see cref="CollectorEngineCapability"/> — nothing about which collectors are gated, and nothing about how
/// the gap is described, lives in this file or in its Lite twin (<c>McpEngineCapability</c>).
///
/// <para><b>Why <c>not_collected</c> rather than <c>unavailable</c>.</b> The miss vocabulary already has the
/// right word: <c>not_collected</c> is "the input names something this server does not collect", which is
/// exactly true and final here. <c>unavailable</c> means "supported here, just not retrievable now", and it
/// sends an operator hunting for a collector to restart — which is the defect #2511 was filed about, not the
/// fix for it.</para>
///
/// <para><b>Called only on the miss path.</b> Every call site checks capability after its read came back
/// empty, never before it. That costs nothing in the common case, and — more importantly — a server whose
/// registry row says one engine while its collected rows say another (a re-registration, a restored
/// database) still gets its DATA rather than a confident explanation of why it cannot have any.</para>
/// </summary>
internal static class DarlingEngineCapability
{
    /// <summary>
    /// The two engine facts the registration upsert stamps on every connect
    /// (<see cref="DarlingObservability"/>): the probed <c>SERVERPROPERTY('EngineEdition')</c> and, since
    /// V82 (#2530), the target's engine KIND. Exposed as a const so Darling.Tests can pin the dialect without
    /// a live store. $1 server_id.
    ///
    /// <para>ONE round trip for both, deliberately: they are read together on every miss, and two reads would
    /// also make it possible to answer the kind axis from one server's row and the edition axis from a stale
    /// copy of another's.</para>
    /// </summary>
    public const string ServerEngineSql = @"
SELECT sql_engine_edition, engine_kind
FROM servers
WHERE server_id = $1";

    /// <summary>
    /// The <c>not_collected</c> envelope when <paramref name="collectorName"/> cannot run on this server's
    /// engine, or <c>null</c> when it can — in which case the caller falls through to its own
    /// <c>empty</c>/<c>unavailable</c> miss, unchanged.
    ///
    /// <para>A registry read that FAILS answers null, deliberately. This runs on a path that has already
    /// found no data; turning a capability probe into a read error would replace one honest miss with a
    /// worse one.</para>
    /// </summary>
    public static async Task<string?> NotCollectedStatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        string collectorName,
        CancellationToken cancellationToken = default)
    {
        int engineEdition;
        string? engineKind;
        try
        {
            (engineEdition, engineKind) = await ReadServerEngineAsync(postgres, serverId, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, collectorName);
        return message is null ? null : McpHelpers.Status("not_collected", message);
    }

    /// <summary>
    /// The server's probed engine edition and engine kind, defaulting to
    /// <see cref="CollectorEngineCapability.UnknownEngineEdition"/> and <c>null</c> when the registry has no
    /// row or a NULL.
    ///
    /// <para>The two NULLs mean different things and are both correct. A PostgreSQL target always has edition
    /// 0 — <c>SERVERPROPERTY</c> does not exist there — so the edition is unknown for it permanently, and it
    /// is the KIND that carries the fact. A NULL kind is a row no connect has stamped since V82 landed, which
    /// makes no claim on that axis and leaves the edition axis answering exactly as it did before (#2530).</para>
    /// </summary>
    private static async Task<(int EngineEdition, string? EngineKind)> ReadServerEngineAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(ServerEngineSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return (CollectorEngineCapability.UnknownEngineEdition, null);
        }

        var edition = reader.IsDBNull(0) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(0);
        var kind = reader.IsDBNull(1) ? null : reader.GetString(1);
        return (edition, kind);
    }

    /// <summary>The registry's PostgreSQL major for one server (V100, #2653). $1 server_id.</summary>
    public const string PostgresMajorVersionSql = @"
SELECT postgres_major_version
FROM servers
WHERE server_id = $1";

    /// <summary>
    /// The target's probed PostgreSQL major, or <c>null</c> when the registry makes no claim — no row, a
    /// server no connect has stamped since V100 landed, or a SQL Server target, where it is not a fact about
    /// that server at all.
    ///
    /// <para><b>Null is not an error and must not be rendered as a version.</b> Callers use this to decide
    /// whether they may state that a column is absent on this server's version; with no claim they say
    /// nothing about the version instead of guessing, because a wrong version in an explanation is worse
    /// than an unexplained NULL.</para>
    ///
    /// <para>A registry read that FAILS answers null for the same reason
    /// <see cref="NotCollectedStatusAsync"/> does: this runs on a path that already has its data, and a
    /// capability probe must never turn a good answer into a read error.</para>
    /// </summary>
    public static async Task<int?> PostgresMajorVersionAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = postgres.CreateCommand(PostgresMajorVersionSql);
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0))
            {
                return null;
            }

            return reader.GetInt32(0);
        }
        catch (Exception)
        {
            return null;
        }
    }
}
