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

        var engineEnvelope = await EnvelopeAsync(postgres, serverId, serverName, engineEdition, engineKind, collectorName, cancellationToken);
        if (engineEnvelope is not null || !IsReadableSecondaryGated(collectorName))
        {
            return engineEnvelope;
        }

        /* #6554: the collectors skip every readable-secondary database, so on a secondary these reads are empty by design. */
        return await DarlingRuntimePrecondition.QueryStoreReadableSecondaryStatusAsync(postgres, serverId, serverName, cancellationToken);
    }

    /// <summary>The collectors that skip a readable secondary's databases (bit 8 of readonly_reason), so a read of what they store can be empty by design there (#6554).</summary>
    internal static bool IsReadableSecondaryGated(string collectorName) =>
        collectorName is "query_store" or "query_store_forced_plans" or "query_store_hints" or "plan_correction";

    /// <summary>
    /// #6439: the <c>not_collected</c> envelope for the tempdb file I/O read on an Azure SQL Database, or <c>null</c> on any other
    /// engine. <c>file_io_stats</c> runs there, but the database sees only its own files, so a tempdb row never exists and no
    /// collector setting can make one. The sentence carries the same engine-gate mark and engine name as
    /// <see cref="NotCollectedStatusAsync"/>'s, so the web page shows the same plain "Azure SQL Database does not have this
    /// data." line as its other Azure panels instead of sending the reader to check collection.
    /// </summary>
    public static async Task<string?> TempDbFileIoNotCollectedStatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        CancellationToken cancellationToken = default) =>
        TempDbFileIoNotCollectedStatus(serverName, await EngineEditionAsync(postgres, serverId, cancellationToken));

    /// <summary>The pure half of <see cref="TempDbFileIoNotCollectedStatusAsync"/>: the envelope for this engine edition, or <c>null</c>.</summary>
    internal static string? TempDbFileIoNotCollectedStatus(string serverName, int engineEdition) =>
        engineEdition == CollectorEngineCapability.AzureSqlDatabaseEngineEdition
            ? McpHelpers.Status(
                "not_collected",
                $"{serverName} runs on {CollectorEngineCapability.DescribeEngineEdition(engineEdition)} (EngineEdition {engineEdition}). " +
                "The file I/O collector runs there, but an Azure SQL Database sees only its own database's files, so this server " +
                "has no tempdb file I/O rows, and never will. " +
                "This is a permanent engine capability gap, not a collection outage: checking collection health, " +
                "enabling a collector or starting a capture cannot change it.")
            : null;

    /// <summary>
    /// #6894: <see cref="Envelope"/> plus the Amazon RDS axis. The registry's RDS fact is read only when the engine axes found no gap AND the
    /// collector's own gate is closed on RDS, so a read of any other collector costs nothing extra. A NULL (never stamped) or failed read is
    /// false, which makes no RDS claim and leaves the read on its own precondition answer.
    /// </summary>
    private static async Task<string?> EnvelopeAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int engineEdition, string? engineKind, string collectorName,
        CancellationToken cancellationToken)
    {
        var envelope = Envelope(serverName, engineEdition, engineKind, collectorName);
        if (envelope is not null || CollectorEngineCapability.IsCollectedOnAwsRds(collectorName, engineKind))
        {
            return envelope;
        }

        return await IsAwsRdsAsync(postgres, serverId, cancellationToken)
            ? Envelope(serverName, engineEdition, engineKind, collectorName, isAwsRds: true)
            : null;
    }

    /// <summary>#6894: the registry's Amazon RDS fact for one server, stamped by every connect (V225). $1 server_id.</summary>
    public const string AwsRdsSql = @"
SELECT is_aws_rds
FROM servers
WHERE server_id = $1";

    /// <summary>
    /// #6894: whether the registry says this server is an Amazon RDS for SQL Server instance. False when there is no row, the column is NULL
    /// (no connect has stamped it, or a PostgreSQL target) or the read fails: no claim is the answer that keeps a read on its own precondition
    /// text. A cancellation still propagates.
    /// </summary>
    internal static async Task<bool> IsAwsRdsAsync(NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand(AwsRdsSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            return await command.ExecuteScalarAsync(cancellationToken) is true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>The <c>not_collected</c> envelope for this engine and collector, or <c>null</c> when the collector can run there.</summary>
    private static string? Envelope(string serverName, int engineEdition, string? engineKind, string collectorName, bool isAwsRds = false)
    {
        var message = CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, collectorName, isAwsRds);
        return message is null ? null : McpHelpers.Status("not_collected", message);
    }

    /// <summary>#6178: every registered server, enabled or not: the fleet a read with no server named would have covered.</summary>
    public const string RegisteredServersSql = @"
SELECT server_id, server_name
FROM servers
ORDER BY server_name";

    /// <summary>
    /// #6178: the <c>not_collected</c> envelope for a read that covers every registered server and found nothing, when
    /// <paramref name="collectorName"/> cannot run on ANY of them (a fleet of only Azure SQL Databases). It asks
    /// <see cref="NotCollectedStatusAsync"/> about each server and returns the first envelope, which names the engine and the
    /// collector (for a fleet of several, the first server). It returns <c>null</c> when at least one server could have had the
    /// data, or when the fleet cannot be read or is empty, so the caller keeps its own <c>empty</c> answer.
    /// </summary>
    public static async Task<string?> FleetNotCollectedStatusAsync(
        NpgsqlDataSource postgres,
        string collectorName,
        CancellationToken cancellationToken = default)
    {
        var servers = new System.Collections.Generic.List<(int ServerId, string ServerName)>();
        try
        {
            await using var command = postgres.CreateCommand(RegisteredServersSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add((reader.GetInt32(0), reader.GetString(1)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }

        string? first = null;
        foreach (var (serverId, serverName) in servers)
        {
            /* The same answer NotCollectedStatusAsync gives one server; a registry read that fails makes no claim. */
            string? envelope;
            try
            {
                var (engineEdition, engineKind) = await ReadServerEngineAsync(postgres, serverId, cancellationToken);
                envelope = await EnvelopeAsync(postgres, serverId, serverName, engineEdition, engineKind, collectorName, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return null;
            }

            if (envelope is null) return null;
            first ??= envelope;
        }

        return first;
    }

    /// <summary>
    /// #6178: whether the newest stored server properties say this server's database is on Azure SQL Database Hyperscale (the
    /// stored edition or an <c>HS_</c> service objective). A missing row or a failed read is false, so the caller falls back to
    /// reading Hyperscale from its own rows. A cancellation still propagates.
    /// </summary>
    public static async Task<bool> IsHyperscaleAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var row = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, serverId, cancellationToken);
            return row is not null && HyperscaleLogSize.IsHyperscaleTier(row.Edition, row.ServiceObjective);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// The server's probed engine edition from the registry, or <see cref="CollectorEngineCapability.UnknownEngineEdition"/>
    /// when the registry has none or the read fails. The unknown edition makes no claim, so a failed read leaves every
    /// answer as it was. A read that HAS data asks this too when one of its fields has no source on some engine
    /// (<c>get_memory_stats</c>' memory state on an Azure SQL Database). A cancellation still propagates.
    /// </summary>
    public static async Task<int> EngineEditionAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return (await ReadServerEngineAsync(postgres, serverId, cancellationToken)).EngineEdition;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return CollectorEngineCapability.UnknownEngineEdition;
        }
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
    internal static async Task<(int EngineEdition, string? EngineKind)> ReadServerEngineAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(ServerEngineSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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

    /// <summary>
    /// The registry's two PostgreSQL facts for one server (the major from V100 #2653, the kind from V82
    /// #2530). $1 server_id.
    ///
    /// <para>ONE round trip for both, for the reason <see cref="ServerEngineSql"/> gives one axis over: they
    /// are read together by every caller that has a NULL column to explain, and two reads would make it
    /// possible to answer the VERSION axis from this server's row and the FLAVOUR axis from a stale copy —
    /// which is how an explanation ends up naming a major belonging to one server and an engine belonging to
    /// another. Exposed as a const so the tests can pin the shape without a live store.</para>
    /// </summary>
    public const string PostgresTargetFactsSql = @"
SELECT postgres_major_version, engine_kind
FROM servers
WHERE server_id = $1";

    /// <summary>
    /// The target's probed PostgreSQL major and its engine KIND token, each <c>null</c> when the registry
    /// makes no claim about that fact — no row, a server no connect has stamped since the rung adding the
    /// column landed, or a SQL Server target, where the major is not a fact about that server at all.
    /// Decode the token through <see cref="MonitoredEngineKind"/> rather than comparing strings.
    ///
    /// <para><b>Neither null is an error.</b> A null major must not be rendered as a version, and a null
    /// kind must not be read as "not Aurora". Callers use these to decide whether they may state that a
    /// column is structurally absent on this server's version or on its flavour; with no claim they say
    /// nothing rather than guessing, because a wrong version or a wrong engine inside an explanation is
    /// worse than an unexplained NULL. <see cref="MonitoredEngineKind.IsAurora"/> already answers false for
    /// null, which is the direction that stays honest.</para>
    ///
    /// <para>A registry read that FAILS answers both nulls, for the reason
    /// <see cref="NotCollectedStatusAsync"/> does: this runs on a path that already has its data, and a
    /// capability probe must never turn a good answer into a read error.</para>
    /// </summary>
    public static async Task<(int? PostgresMajorVersion, string? EngineKind)> PostgresTargetFactsAsync(
        NpgsqlDataSource postgres,
        int serverId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = postgres.CreateCommand(PostgresTargetFactsSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken))
            {
                return (null, null);
            }

            return (
                reader.IsDBNull(0) ? null : reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetString(1));
        }
        catch (Exception)
        {
            return (null, null);
        }
    }
}
