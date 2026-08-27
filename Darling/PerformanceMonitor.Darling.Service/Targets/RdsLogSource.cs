/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Fetches PostgreSQL server-log text from the RDS API, for targets with no filesystem to read (#2538).
///
/// <para><b>Why this exists at all.</b> <c>auto_explain</c> writes plans to the server log and nowhere else.
/// On a self-hosted server the collector reads that log with <c>pg_read_file</c>; on Aurora and RDS there is
/// no filesystem, <c>pg_read_server_files</c> is not grantable, and the log is only reachable through
/// <c>DownloadDBLogFilePortion</c>. Same text, different transport — which is exactly why the parsing and
/// redaction were moved into <c>PgPlanLogParser</c> first.</para>
///
/// <para><b>This is the only code in the product that reaches a monitored target other than through a
/// database connection.</b> It holds no credentials of its own: the SDK's default chain finds the EC2
/// instance profile the service already runs under, which is how the monitoring hosts reach every other AWS
/// API today. Nothing is stored, so nothing can leak from config — and a host with no role simply fails the
/// call and the collector degrades, rather than the product asking anyone to paste keys into a file.</para>
///
/// <para><b>The marker is deliberately in memory rather than in the store.</b> RDS returns a position to
/// resume from, and keeping it per-process means a restart re-reads a bounded tail instead of nothing.
/// Re-reading is HARMLESS here and that is not luck: plan rows dedup on (queryid, plan_hash), so an
/// overlapping window produces the same shapes rather than duplicates — the same property the
/// <c>pg_read_file</c> route already relies on. Persisting the marker would buy nothing and add a schema
/// rung that could disagree with reality after a log rotation.</para>
/// </summary>
public sealed class RdsLogSource
{
    /// <summary>
    /// How much of a log file to take on the FIRST read of a target, before any marker exists. Bounded for
    /// the same reason the file route reads a tail: #2565 measured 772 MB of log in twenty seconds at
    /// capture-everything, and an unbounded first read would pull all of it across the network.
    /// </summary>
    private const int FirstReadLines = 10_000;

    private readonly Dictionary<string, string> _markers = new(StringComparer.Ordinal);

    private readonly Func<string, IAmazonRDS> _clientFactory;

    public RdsLogSource(Func<string, IAmazonRDS>? clientFactory = null)
        => _clientFactory = clientFactory
            ?? (region => new AmazonRDSClient(RegionEndpoint.GetBySystemName(region)));

    /// <param name="Text">Raw log text, to be handed to <c>PgPlanLogParser.Extract</c> unchanged.</param>
    /// <param name="MoreAvailable">RDS had more than one call's worth. The caller decides whether to keep
    /// pulling; this type does not loop, so one cycle cannot spend unbounded time on one target.</param>
    public readonly record struct LogChunk(string Text, bool MoreAvailable);

    /// <summary>
    /// The newest PostgreSQL log file's unread portion, or null when this target is not RDS at all.
    ///
    /// <para>A cluster endpoint is resolved to its WRITER, because <c>DownloadDBLogFilePortion</c> takes an
    /// instance identifier and because the writer is where the workload worth capturing runs. A READER
    /// endpoint is refused rather than guessed at: it round-robins across replicas, so the instance behind
    /// it is not stable between calls and plans captured through one would be attributed to whichever
    /// replica answered.</para>
    /// </summary>
    public async Task<LogChunk?> ReadNewestAsync(string host, CancellationToken cancellationToken = default)
    {
        var endpoint = RdsEndpoint.TryParse(host);

        if (endpoint is null)
        {
            return null;
        }

        var parsed = endpoint.Value;

        if (parsed.Kind is RdsEndpointKind.ClusterReader or RdsEndpointKind.ClusterCustom)
        {
            throw new InvalidOperationException(
                $"'{host}' is an Aurora {(parsed.Kind == RdsEndpointKind.ClusterReader ? "reader" : "custom")} "
                + "endpoint, which does not resolve to a stable instance — it moves between replicas "
                + "call to call. Point the target at the cluster writer endpoint or at a specific instance "
                + "so captured plans belong to a server that can be named.");
        }

        using var client = _clientFactory(parsed.Region);

        var instanceId = parsed.Kind == RdsEndpointKind.ClusterWriter
            ? await ResolveWriterAsync(client, parsed.Identifier, cancellationToken)
            : parsed.Identifier;

        var newest = await NewestLogFileAsync(client, instanceId, cancellationToken);

        if (newest is null)
        {
            return new LogChunk(string.Empty, false);
        }

        var key = instanceId + "|" + newest;
        _markers.TryGetValue(key, out var marker);

        var response = await client.DownloadDBLogFilePortionAsync(
            new DownloadDBLogFilePortionRequest
            {
                DBInstanceIdentifier = instanceId,
                LogFileName = newest,
                /* "0" means from the start, which on a rotated multi-GB log is not what anyone wants on a
                   first read. NumberOfLines with no marker asks RDS for the TAIL, matching the file
                   route's bounded-tail behaviour. */
                Marker = marker,
                NumberOfLines = marker is null ? FirstReadLines : 0,
            },
            cancellationToken);

        if (!string.IsNullOrEmpty(response.Marker))
        {
            /* Keyed by FILE as well as instance, so a log rotation starts a fresh marker instead of
               resuming a new file at an old file's offset. */
            _markers[key] = response.Marker;
        }

        /* AdditionalDataPending is bool? in the SDK. Treated as false when null: claiming more is
               pending when the API did not say so would make a caller loop for data that is not there. */
            return new LogChunk(response.LogFileData ?? string.Empty, response.AdditionalDataPending == true);
    }

    private static async Task<string> ResolveWriterAsync(
        IAmazonRDS client, string clusterId, CancellationToken cancellationToken)
    {
        var clusters = await client.DescribeDBClustersAsync(
            new DescribeDBClustersRequest { DBClusterIdentifier = clusterId }, cancellationToken);

        var cluster = clusters.DBClusters.FirstOrDefault()
            ?? throw new InvalidOperationException($"Aurora cluster '{clusterId}' was not found.");

        var writer = cluster.DBClusterMembers.FirstOrDefault(m => m.IsClusterWriter == true)
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{clusterId}' reports no writer. That is a real state during a failover, "
                + "so this is worth retrying rather than treating as a configuration error.");

        return writer.DBInstanceIdentifier;
    }

    /// <summary>
    /// The newest PostgreSQL log file. Filtered by name because an instance's log list also carries
    /// upgrade and other logs, and sorted by last-written rather than by name — the filename embeds a
    /// timestamp, but sorting text would order 2026-08-9 after 2026-08-10.
    /// </summary>
    private static async Task<string?> NewestLogFileAsync(
        IAmazonRDS client, string instanceId, CancellationToken cancellationToken)
    {
        var files = await client.DescribeDBLogFilesAsync(
            new DescribeDBLogFilesRequest
            {
                DBInstanceIdentifier = instanceId,
                FilenameContains = "postgresql",
            },
            cancellationToken);

        return files.DescribeDBLogFiles
            .OrderByDescending(f => f.LastWritten)
            .Select(f => f.LogFileName)
            .FirstOrDefault();
    }
}
