/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The host a target connects to is not the endpoint AWS reports for the RDS instance or cluster that host's name
/// spells. Raised before any RDS or Performance Insights read for that target; the runner records it as a
/// PERMISSIONS outcome and nothing else is read.
/// </summary>
public sealed class RdsEndpointMismatchException : Exception
{
    public RdsEndpointMismatchException(string message) : base(message)
    {
    }
}

/// <summary>
/// Matches an RDS target to its AWS instance. <see cref="RdsEndpoint.TryParse"/> takes the instance or cluster id from the
/// first label of the host name, and the host name is whatever the server's connection string says; nothing in the name
/// proves AWS agrees. This asks AWS for the addresses it reports for that id and compares them with the host Darling
/// connects to (case-insensitive, trailing dot trimmed) before any RDS or Performance Insights read runs.
///
/// <para>An instance is checked against <c>DescribeDBInstances</c> <c>Endpoint.Address</c>; a cluster, reader or custom
/// endpoint against <c>DescribeDBClusters</c> <c>Endpoint</c>, <c>ReaderEndpoint</c> and <c>CustomEndpoints</c>.</para>
///
/// <para>The verdict is cached per (server id, host, id), so an edit of the host asks again. A match is kept for an hour
/// and a mismatch for five minutes, so a fixed AWS side heals within one cycle of the shorter window and a stale match
/// cannot outlive an instance that was recreated under the same id. A Describe call that fails (a denied
/// <c>rds:DescribeDBInstances</c>, a throttle) is not cached: it propagates, and the runner reads it exactly as it reads a
/// denied log call.</para>
/// </summary>
public sealed class RdsEndpointVerifier
{
    private static readonly TimeSpan MatchLifetime = TimeSpan.FromHours(1);
    private static readonly TimeSpan MismatchLifetime = TimeSpan.FromMinutes(5);

    /* The test assembly's older fixtures predate the check and answer neither Describe call. This switch lets their
       default verifier skip it; the new tests construct a verifier with enforce: true. A pin test keeps this name out
       of every other file in the service. */
    internal static bool TestOnlySkipCheck { get; set; }

    private readonly Func<DateTime> _clock;
    private readonly bool _enforce;
    private readonly ConcurrentDictionary<(int ServerId, string Host, string Id), (bool Matches, DateTime AtUtc)> _verdicts = new();

    public RdsEndpointVerifier(Func<DateTime>? clock = null, bool? enforce = null)
    {
        _clock = clock ?? (() => DateTime.UtcNow);
        _enforce = enforce ?? !TestOnlySkipCheck;
    }

    /// <summary>Host names compare equal ignoring case and any trailing dots (the absolute form of a DNS name).</summary>
    public static string Normalize(string? host)
        => (host ?? string.Empty).Trim().TrimEnd('.').ToLowerInvariant();

    /// <summary>Whether <paramref name="host"/> is one of the addresses AWS reported.</summary>
    public static bool Matches(string host, IEnumerable<string?> reported)
    {
        var wanted = Normalize(host);

        return wanted.Length > 0
            && reported.Any(address => Normalize(address) is { Length: > 0 } normalized && normalized == wanted);
    }

    internal static string MismatchMessage(string host, RdsEndpoint.Parsed parsed)
        => $"The host '{host}' does not match the endpoint AWS reports for RDS "
            + $"{(parsed.Kind == RdsEndpointKind.Instance ? "instance" : "cluster")} '{parsed.Identifier}', "
            + "so Darling reads no RDS logs or Performance Insights data for this server.";

    /// <exception cref="RdsEndpointMismatchException">AWS reports no endpoint equal to <paramref name="host"/>.</exception>
    public async Task EnsureAsync(
        IAmazonRDS client, RdsEndpoint.Parsed parsed, string host, int serverId, CancellationToken cancellationToken)
    {
        if (!_enforce)
        {
            return;
        }

        var key = (serverId, Normalize(host), parsed.Identifier.ToLowerInvariant());
        var now = _clock();

        if (_verdicts.TryGetValue(key, out var held)
            && now - held.AtUtc < (held.Matches ? MatchLifetime : MismatchLifetime))
        {
            if (held.Matches)
            {
                return;
            }

            throw new RdsEndpointMismatchException(MismatchMessage(host, parsed));
        }

        var reported = await ReportedAddressesAsync(client, parsed, cancellationToken);
        var matches = Matches(host, reported);

        _verdicts[key] = (matches, now);

        if (!matches)
        {
            throw new RdsEndpointMismatchException(MismatchMessage(host, parsed));
        }
    }

    private static async Task<List<string?>> ReportedAddressesAsync(
        IAmazonRDS client, RdsEndpoint.Parsed parsed, CancellationToken cancellationToken)
    {
        if (parsed.Kind == RdsEndpointKind.Instance)
        {
            var instances = await client.DescribeDBInstancesAsync(
                new DescribeDBInstancesRequest { DBInstanceIdentifier = parsed.Identifier }, cancellationToken);

            var instance = instances.DBInstances?.FirstOrDefault()
                ?? throw new InvalidOperationException(
                    $"RDS/Aurora instance '{parsed.Identifier}' was not found: DescribeDBInstances returned no "
                    + "instance for it.");

            return [instance.Endpoint?.Address];
        }

        var clusters = await client.DescribeDBClustersAsync(
            new DescribeDBClustersRequest { DBClusterIdentifier = parsed.Identifier }, cancellationToken);

        var cluster = clusters.DBClusters?.FirstOrDefault()
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{parsed.Identifier}' was not found: DescribeDBClusters returned no cluster for it.");

        var addresses = new List<string?> { cluster.Endpoint, cluster.ReaderEndpoint };
        addresses.AddRange(cluster.CustomEndpoints ?? []);

        return addresses;
    }
}
