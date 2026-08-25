/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// What an RDS or Aurora endpoint hostname tells us, for the log-download route (#2538).
///
/// <para><b>Why this has to be parsed at all.</b> A target is a connection string. The RDS API needs a
/// <c>DBInstanceIdentifier</c> and a region, and neither appears in a connection string — but both are
/// encoded in the endpoint hostname AWS hands out. Asking an operator to configure them separately would
/// mean two sources of truth for the same server, and the one they typed would eventually disagree with the
/// one they connect to.</para>
///
/// <para><b>The cluster/instance distinction is the part that matters.</b>
/// <c>DownloadDBLogFilePortion</c> takes an INSTANCE identifier. An Aurora CLUSTER endpoint
/// (<c>…​.cluster-xxxx.…</c>) names a cluster, whose writer is whatever instance currently holds that role —
/// so a cluster endpoint cannot be used directly and has to be resolved through the API first. Reading a
/// cluster endpoint as an instance id yields <c>DBInstanceNotFound</c> against a perfectly healthy cluster,
/// which is a confusing way to fail.</para>
///
/// <para><b>Reader endpoints are called out separately</b> (<c>.cluster-ro-</c>). They round-robin across
/// replicas, so the instance behind one is not stable between calls — and plans captured from a reader are
/// a different workload from the writer's, not a substitute for it.</para>
/// </summary>
public static class RdsEndpoint
{
    /// <param name="Identifier">The instance or cluster id, depending on <paramref name="Kind"/>.</param>
    /// <param name="Region">From the hostname, so it always matches the endpoint actually connected to.</param>
    public readonly record struct Parsed(string Identifier, string Region, RdsEndpointKind Kind);

    /* Instance:    name.hash.region.rds.amazonaws.com
       Cluster:     name.cluster-hash.region.rds.amazonaws.com
       Reader:      name.cluster-ro-hash.region.rds.amazonaws.com
       Custom:      name.cluster-custom-hash.region.rds.amazonaws.com

       The suffix is matched loosely on purpose: commercial AWS, China (.com.cn) and GovCloud all differ
       there, and pinning the exact tail would silently refuse to parse a perfectly valid GovCloud endpoint.
       What must be exact is the SECOND label, because that is what separates a cluster from an instance. */
    private static readonly Regex s_endpoint = new(
        @"^(?<name>[a-z0-9][a-z0-9-]*)\.(?<second>cluster-ro-[a-z0-9]+|cluster-custom-[a-z0-9]+|cluster-[a-z0-9]+|[a-z0-9]+)\.(?<region>[a-z]{2}(?:-[a-z]+)+-\d)\.rds\.",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Null when the host is not an RDS endpoint at all — a self-hosted server, a proxy, or an IP. That is
    /// an ordinary answer rather than an error: the caller falls back to the <c>pg_read_file</c> route.
    /// </summary>
    public static Parsed? TryParse(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return null;
        }

        var match = s_endpoint.Match(host.Trim());

        if (!match.Success)
        {
            return null;
        }

        var second = match.Groups["second"].Value;

        var kind = second.StartsWith("cluster-ro-", StringComparison.OrdinalIgnoreCase)
            ? RdsEndpointKind.ClusterReader
            : second.StartsWith("cluster-custom-", StringComparison.OrdinalIgnoreCase)
                ? RdsEndpointKind.ClusterCustom
                : second.StartsWith("cluster-", StringComparison.OrdinalIgnoreCase)
                    ? RdsEndpointKind.ClusterWriter
                    : RdsEndpointKind.Instance;

        return new Parsed(
            Identifier: match.Groups["name"].Value,
            Region: match.Groups["region"].Value.ToLowerInvariant(),
            Kind: kind);
    }
}

/// <summary>
/// Which shape of endpoint was found. The distinction decides whether the identifier can be used with
/// <c>DownloadDBLogFilePortion</c> directly or has to be resolved to a writer instance first.
/// </summary>
public enum RdsEndpointKind
{
    /// <summary>A DB instance. Its identifier IS the log-download identifier.</summary>
    Instance,

    /// <summary>An Aurora cluster writer endpoint. Names a cluster; the writer must be resolved.</summary>
    ClusterWriter,

    /// <summary>An Aurora reader endpoint. Round-robins across replicas, so no stable instance behind it.</summary>
    ClusterReader,

    /// <summary>A custom endpoint over a chosen subset of instances. Same resolution problem as a reader.</summary>
    ClusterCustom,
}
