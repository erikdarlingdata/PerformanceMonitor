/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The assumed-role credentials the process holds, one <see cref="AwsAssumedRoleCredentials"/> per
/// <see cref="AwsRoleKey"/> (#5452). Two servers that name the same role and external ID share one entry and one STS
/// call per refresh.
///
/// <para><b>What <see cref="For"/> checks, in this order, on EVERY call.</b> First the allow list: a role the list does
/// not allow is never assumed, even when a row already stores it and even when an entry for it is cached
/// (<see cref="AwsRoleAssumeKind.NotAllowed"/>). Then the partition: the role ARN's partition must be the partition of
/// the target's region, and this is checked on every call because the same key can be asked for from regions in two
/// partitions (<see cref="AwsRoleAssumeKind.PartitionMismatch"/>). Only then does it find or make the entry, and STS
/// is called later, when an SDK client first signs a request with the handle it returns. Both refusals are thrown from
/// <see cref="For"/> before any client is built or any AWS call is made.</para>
///
/// <para>One cache per process, built beside the collector runner's ingestors. It holds no long-lived disposable, so
/// nothing needs disposing; <see cref="Retain"/> drops the keys that no enabled server names any more.</para>
/// </summary>
public sealed class AwsRoleCredentialCache
{
    private readonly ConcurrentDictionary<AwsRoleKey, AwsAssumedRoleCredentials> _entries = new();
    private readonly AwsRoleAllowlist? _allowlist;
    private readonly Func<string?>? _installId;
    private readonly Func<RegionEndpoint, AWSCredentials, IAmazonSecurityTokenService>? _stsFactory;
    private readonly Func<RegionEndpoint, Task<AWSCredentials>>? _source;
    private readonly TimeProvider? _clock;
    private readonly TimeSpan? _refreshDeadline;
    private readonly ILogger? _logger;

    /// <param name="allowlist">The roles that may be assumed. Default: <see cref="AwsRoleAllowlist.Current"/>, read at
    /// each call, so the list the worker sets at start is the one in force.</param>
    /// <param name="installId">The store's install id, for the session name.</param>
    /// <param name="stsFactory">Test seam: builds the STS client.</param>
    /// <param name="source">Test seam: the credentials that call STS.</param>
    /// <param name="clock">Test seam: the clock for the expiry arithmetic.</param>
    /// <param name="refreshDeadline">Test seam: the longest one refresh may take.</param>
    /// <param name="logger">Gets a warning line for each failed refresh.</param>
    public AwsRoleCredentialCache(
        AwsRoleAllowlist? allowlist = null,
        Func<string?>? installId = null,
        Func<RegionEndpoint, AWSCredentials, IAmazonSecurityTokenService>? stsFactory = null,
        Func<RegionEndpoint, Task<AWSCredentials>>? source = null,
        TimeProvider? clock = null,
        TimeSpan? refreshDeadline = null,
        ILogger? logger = null)
    {
        _allowlist = allowlist;
        _installId = installId;
        _stsFactory = stsFactory;
        _source = source;
        _clock = clock;
        _refreshDeadline = refreshDeadline;
        _logger = logger;
    }

    /// <summary>How many roles have an entry.</summary>
    public int Count => _entries.Count;

    /// <summary>Whether <paramref name="key"/> has an entry.</summary>
    public bool Contains(AwsRoleKey key) => _entries.ContainsKey(key);

    /// <summary>
    /// The credentials for <paramref name="key"/> to hand an SDK client built for <paramref name="region"/>.
    /// Throws <see cref="AwsRoleAssumeException"/> (<see cref="AwsRoleAssumeKind.NotAllowed"/> or
    /// <see cref="AwsRoleAssumeKind.PartitionMismatch"/>) before any AWS call when the role may not be used here.
    /// </summary>
    /// <param name="key">The server's role and external ID.</param>
    /// <param name="region">The target's AWS region, such as <c>us-east-1</c>.</param>
    /// <param name="allowlist">The list to check against, instead of the cache's own.</param>
    public AWSCredentials For(AwsRoleKey key, string region, AwsRoleAllowlist? allowlist = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key.RoleArn);
        ArgumentException.ThrowIfNullOrWhiteSpace(region);

        var list = allowlist ?? _allowlist ?? AwsRoleAllowlist.Current;
        if (!list.IsAllowed(key.RoleArn))
        {
            throw AwsRoleAssumeException.ForNotAllowed(key, region);
        }

        var regionEndpoint = RegionEndpoint.GetBySystemName(region);
        var rolePartition = PartitionOf(key.RoleArn);
        var regionPartition = regionEndpoint.PartitionName;
        if (!string.Equals(rolePartition, regionPartition, StringComparison.Ordinal))
        {
            throw AwsRoleAssumeException.ForPartitionMismatch(key, region, rolePartition, regionPartition);
        }

        var entry = _entries.GetOrAdd(key, k => new AwsAssumedRoleCredentials(
            k, regionEndpoint, _stsFactory, _source, _installId, _clock, _refreshDeadline, _logger));
        return entry.ForRegion(regionEndpoint);
    }

    /// <summary>Drops every entry whose key is not in <paramref name="liveKeys"/>: the roles an enabled server still names.</summary>
    public void Retain(IEnumerable<AwsRoleKey> liveKeys)
    {
        ArgumentNullException.ThrowIfNull(liveKeys);
        var keep = new HashSet<AwsRoleKey>(liveKeys);
        foreach (var key in _entries.Keys.Where(k => !keep.Contains(k)).ToList())
        {
            _entries.TryRemove(key, out _);
        }
    }

    /// <summary>The partition segment of a role ARN (<c>arn:aws-cn:iam::...</c> gives <c>aws-cn</c>), or "" when it has none.</summary>
    internal static string PartitionOf(string roleArn)
    {
        var parts = roleArn.Split(':', 3);
        return parts.Length == 3 && parts[0] == "arn" ? parts[1] : string.Empty;
    }
}
