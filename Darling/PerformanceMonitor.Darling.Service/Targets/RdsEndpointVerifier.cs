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
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The host a target connects to is not the endpoint AWS reports for the RDS instance or cluster that host's name
/// spells, or AWS does not know that id. Raised before any RDS or Performance Insights read for that target; the runner
/// records it as a PERMISSIONS outcome and nothing else is read.
/// </summary>
public class RdsEndpointMismatchException : Exception
{
    public RdsEndpointMismatchException(string message) : base(message)
    {
    }
}

/// <summary>
/// The target did not accept a fresh, unpooled login, so no RDS or Performance Insights read runs for it. Raised in
/// the place <see cref="RdsEndpointMismatchException"/> is, and travels the same way (the ingestors let it through
/// unwrapped and the runner records it as a PERMISSIONS outcome); it names the login, never the AWS side.
/// </summary>
public sealed class RdsTargetLoginException : RdsEndpointMismatchException
{
    public RdsTargetLoginException(string message) : base(message)
    {
    }
}

/// <summary>
/// Before an RDS or Performance Insights read, confirm with AWS that the connected host is the endpoint of the instance
/// or cluster the host names. <see cref="RdsEndpoint.TryParse"/> takes the id from the first label of the host name;
/// this asks AWS for the addresses it reports for that id and compares them with the host Darling connects to
/// (ASCII only, case-insensitive, trailing dot trimmed).
///
/// <para>An instance is checked against <c>DescribeDBInstances</c> <c>Endpoint.Address</c>; a cluster, reader or custom
/// endpoint against <c>DescribeDBClusters</c> <c>Endpoint</c>, <c>ReaderEndpoint</c> and <c>CustomEndpoints</c>. An id AWS
/// does not know reads as a mismatch, with the same text as any other mismatch.</para>
///
/// <para>The verdict is cached per (server id, host, id, credential scope), so an edit of the host asks again. A match is
/// kept for an hour and a mismatch for five minutes, so a fixed AWS side heals within one cycle of the shorter window
/// and a stale match cannot outlive an instance that was recreated under the same id. A Describe call that fails (a
/// denied <c>rds:DescribeDBInstances</c>, a throttle) is not cached: it propagates, and the runner reads it exactly as it
/// reads a denied log call. Each cache holds at most <see cref="MaxEntries"/> entries and drops an expired entry when it
/// is read.</para>
///
/// <para>A match is honoured only while the target has accepted an unpooled login within the match lifetime: a login the
/// target refuses (a revoked role, a changed password) ends the reads even when a pooled session is still open. With no
/// fresh login there is no RDS or Performance Insights read, and the row names the login.</para>
///
/// <para>The credential scope is "" for the process credentials every call uses today. It is the place for the role
/// identity when a server reads under its own role (#5452), so a role edit never reuses a verdict made under another.</para>
/// </summary>
public sealed class RdsEndpointVerifier
{
    private static readonly TimeSpan MatchLifetime = TimeSpan.FromHours(1);
    private static readonly TimeSpan MismatchLifetime = TimeSpan.FromMinutes(5);

    /// <summary>The login probe waits this long for the target to accept a fresh connection.</summary>
    internal const int LoginProbeSeconds = 5;

    /// <summary>The most entries either cache keeps; past it the oldest entry goes.</summary>
    internal const int MaxEntries = 4096;

    /* The test assembly's older fixtures predate the check and answer neither Describe call. This switch lets their
       default verifier skip it; the new tests construct a verifier with enforce: true. A pin test keeps this name out
       of every other file in the service. */
    internal static bool TestOnlySkipCheck { get; set; }

    private readonly Func<DateTime> _clock;
    private readonly bool _enforce;
    private readonly Func<string?, CancellationToken, Task<string?>> _loginProbe;
    private readonly ConcurrentDictionary<(int ServerId, string Host, string Id, string Scope), (bool Matches, DateTime AtUtc)> _verdicts = new();
    private readonly ConcurrentDictionary<(int ServerId, string Host), DateTime> _logins = new();

    /// <summary>The verifier every product caller uses: it enforces, with the real login probe.</summary>
    public RdsEndpointVerifier() : this(null, null)
    {
    }

    /// <summary>
    /// Internal so no product caller can switch the check off: <paramref name="enforce"/> is for the tests.
    /// <paramref name="loginProbe"/> answers null when the target accepted a fresh login, otherwise what failed.
    /// </summary>
    internal RdsEndpointVerifier(
        Func<DateTime>? clock, bool? enforce, Func<string?, CancellationToken, Task<string?>>? loginProbe = null)
    {
        _clock = clock ?? (() => DateTime.UtcNow);
        _enforce = enforce ?? !TestOnlySkipCheck;
        _loginProbe = loginProbe ?? ProbeLoginAsync;
    }

    /// <summary>
    /// Host names compare equal ignoring case and any trailing dots (the absolute form of a DNS name). A host with a
    /// non-ASCII character normalizes to the empty string, which matches nothing.
    /// </summary>
    public static string Normalize(string? host)
    {
        var trimmed = (host ?? string.Empty).Trim().TrimEnd('.');

        return trimmed.Any(character => character > 0x7F) ? string.Empty : trimmed.ToLowerInvariant();
    }

    /// <summary>Whether <paramref name="host"/> is one of the addresses AWS reported.</summary>
    public static bool Matches(string host, IEnumerable<string?> reported)
    {
        var wanted = Normalize(host);

        return wanted.Length > 0
            && reported.Any(address => Normalize(address) is { Length: > 0 } normalized
                && string.Equals(normalized, wanted, StringComparison.OrdinalIgnoreCase));
    }

    internal static string MismatchMessage(string host, RdsEndpoint.Parsed parsed)
        => $"The host '{host}' does not match the endpoint AWS reports for RDS "
            + $"{(parsed.Kind == RdsEndpointKind.Instance ? "instance" : "cluster")} '{parsed.Identifier}', "
            + "so Darling reads no RDS logs or Performance Insights data for this server.";

    internal static string LoginMessage(string host, string detail)
        => $"The login to '{host}' could not be confirmed with a fresh connection ({detail}), "
            + "so Darling reads no RDS logs or Performance Insights data for this server.";

    internal int CachedVerdictCount => _verdicts.Count;

    internal int CachedLoginCount => _logins.Count;

    /// <summary>Forgets every verdict and login held for <paramref name="serverId"/> (its definition changed).</summary>
    public void ClearServer(int serverId)
    {
        foreach (var key in _verdicts.Keys.Where(key => key.ServerId == serverId).ToList())
        {
            _verdicts.TryRemove(key, out _);
        }

        foreach (var key in _logins.Keys.Where(key => key.ServerId == serverId).ToList())
        {
            _logins.TryRemove(key, out _);
        }
    }

    /// <param name="loginConnectionString">The target's connection string, for the fresh login a cached or new match
    /// requires. Null or empty is a failed login: with no way to log in there is nothing to confirm.</param>
    /// <param name="credentialScope">"" for the process credentials; the role identity under #5452.</param>
    /// <exception cref="RdsEndpointMismatchException">AWS reports no endpoint equal to <paramref name="host"/>.</exception>
    /// <exception cref="RdsTargetLoginException">The target did not accept a fresh login.</exception>
    public async Task EnsureAsync(
        IAmazonRDS client, RdsEndpoint.Parsed parsed, string host, int serverId, CancellationToken cancellationToken,
        string? loginConnectionString = null, string credentialScope = "")
    {
        if (!_enforce)
        {
            return;
        }

        var key = (serverId, Normalize(host), parsed.Identifier.ToLowerInvariant(), credentialScope);
        var now = _clock();

        if (_verdicts.TryGetValue(key, out var held))
        {
            if (now - held.AtUtc < (held.Matches ? MatchLifetime : MismatchLifetime))
            {
                if (!held.Matches)
                {
                    throw new RdsEndpointMismatchException(MismatchMessage(host, parsed));
                }

                await EnsureLoginAsync(serverId, host, loginConnectionString, now, cancellationToken);
                return;
            }

            _verdicts.TryRemove(key, out _);
        }

        var reported = await ReportedAddressesAsync(client, parsed, cancellationToken);
        var matches = Matches(host, reported);

        Store(_verdicts, key, (matches, now), entry => entry.Item2);

        if (!matches)
        {
            throw new RdsEndpointMismatchException(MismatchMessage(host, parsed));
        }

        await EnsureLoginAsync(serverId, host, loginConnectionString, now, cancellationToken);
    }

    /// <summary>A match is honoured only when an unpooled login to the target succeeded within the match lifetime.</summary>
    private async Task EnsureLoginAsync(
        int serverId, string host, string? connectionString, DateTime now, CancellationToken cancellationToken)
    {
        var key = (serverId, Normalize(host));

        if (_logins.TryGetValue(key, out var at))
        {
            if (now - at < MatchLifetime)
            {
                return;
            }

            _logins.TryRemove(key, out _);
        }

        var failure = await _loginProbe(connectionString, cancellationToken);

        if (failure is not null)
        {
            throw new RdsTargetLoginException(LoginMessage(host, failure));
        }

        Store(_logins, key, now, loggedInAt => loggedInAt);
    }

    /// <summary>Adds the entry; at <see cref="MaxEntries"/> the oldest one goes first.</summary>
    private static void Store<TKey, TValue>(
        ConcurrentDictionary<TKey, TValue> cache, TKey key, TValue value, Func<TValue, DateTime> atUtc)
        where TKey : notnull
    {
        if (cache.Count >= MaxEntries && !cache.ContainsKey(key))
        {
            var oldest = cache.OrderBy(pair => atUtc(pair.Value)).First().Key;
            cache.TryRemove(oldest, out _);
        }

        cache[key] = value;
    }

    /// <summary>
    /// An unpooled login and SELECT 1 inside <see cref="LoginProbeSeconds"/> seconds. Null when it worked, otherwise the
    /// SQLSTATE or the exception type: never the exception's text, which can carry the address.
    /// </summary>
    internal static async Task<string?> ProbeLoginAsync(string? connectionString, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return "no connection string";
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Pooling = false,
                Timeout = LoginProbeSeconds,
                CommandTimeout = LoginProbeSeconds,
            };

            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand("SELECT 1;", connection) { CommandTimeout = LoginProbeSeconds };
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            return null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PostgresException pg)
        {
            return $"SQLSTATE {pg.SqlState}";
        }
        catch (Exception ex)
        {
            return ex.GetType().Name;
        }
    }

    private static async Task<List<string?>> ReportedAddressesAsync(
        IAmazonRDS client, RdsEndpoint.Parsed parsed, CancellationToken cancellationToken)
    {
        /* An id AWS does not know reports no address, so it reads as the same mismatch (and the same text) as an id
           that exists with another endpoint. */
        if (parsed.Kind == RdsEndpointKind.Instance)
        {
            DescribeDBInstancesResponse instances;

            try
            {
                instances = await client.DescribeDBInstancesAsync(
                    new DescribeDBInstancesRequest { DBInstanceIdentifier = parsed.Identifier }, cancellationToken);
            }
            catch (DBInstanceNotFoundException)
            {
                return [];
            }

            var instance = instances.DBInstances?.FirstOrDefault();

            return instance is null ? [] : [instance.Endpoint?.Address];
        }

        DescribeDBClustersResponse clusters;

        try
        {
            clusters = await client.DescribeDBClustersAsync(
                new DescribeDBClustersRequest { DBClusterIdentifier = parsed.Identifier }, cancellationToken);
        }
        catch (DBClusterNotFoundException)
        {
            return [];
        }

        var cluster = clusters.DBClusters?.FirstOrDefault();

        if (cluster is null)
        {
            return [];
        }

        var addresses = new List<string?> { cluster.Endpoint, cluster.ReaderEndpoint };
        addresses.AddRange(cluster.CustomEndpoints ?? []);

        return addresses;
    }
}
