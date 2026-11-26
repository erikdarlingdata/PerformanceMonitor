/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>What <see cref="BlockedProcessThresholdBootstrap.RunAsync"/> did for one ensure of the blocked-process session (#9743).</summary>
public enum BlockedProcessThresholdOutcome
{
    /// <summary>This server was already tried under these connection settings in this process. Nothing was sent to it.</summary>
    AlreadyTried,

    /// <summary>The server read 0 and the batch set the threshold to 5 seconds.</summary>
    Configured,

    /// <summary>The server already had a threshold. The batch changed nothing.</summary>
    AlreadySet,

    /// <summary>The server refused the batch (the login lacks ALTER SETTINGS, or the platform has no sp_configure for it). It is not asked again.</summary>
    Refused,

    /// <summary>The batch failed on a timeout or a dropped connection, which says nothing about the server. It is asked again on the next ensure.</summary>
    Transient,
}

/// <summary>The outcome of one <see cref="BlockedProcessThresholdBootstrap.RunAsync"/>, with the server's error when it was refused.</summary>
public readonly record struct BlockedProcessThresholdResult(BlockedProcessThresholdOutcome Outcome, DbException? Refusal = null);

/// <summary>
/// The once-per-server decision behind the blocked process threshold bootstrap, shared by Lite and Darling (#9743). Both products
/// ensure the blocked-process session on every run of its collector (Lite) or on every connect and hourly sweep (Darling), and the
/// ensure sends <c>sp_configure 'blocked process threshold (s)'</c> to a server that reads 0. A server that refuses it (AWS RDS, or a
/// login without ALTER SETTINGS) refused it again on every run and was logged each time. A server is now tried once per process and
/// once more after its connection settings change, and the one failure carries the remedy for its platform.
/// </summary>
public sealed class BlockedProcessThresholdBootstrap
{
    private readonly ConcurrentDictionary<int, string> _triedSettings = new();

    /// <summary>
    /// Sends the bootstrap batch to the server unless it was already tried with these connection settings. <paramref name="sendBatch"/>
    /// returns the threshold the server read before the batch ran: 0 means the batch set it to 5 seconds. A SQL Server error that is
    /// not a timeout or a dropped connection (<paramref name="isTransient"/> says which each host's own classifier reads that way) counts
    /// as the server's answer: it is returned as <see cref="BlockedProcessThresholdOutcome.Refused"/> with the error, and the server is
    /// not asked again. Anything else (a cancellation, a non-SQL fault) propagates and leaves the server untried.
    /// </summary>
    public async Task<BlockedProcessThresholdResult> RunAsync(
        int serverId, string connectionSettings, Func<Task<int>> sendBatch, Func<DbException, bool> isTransient)
    {
        var settingsKey = KeyOf(connectionSettings);
        if (_triedSettings.TryGetValue(serverId, out var tried) && string.Equals(tried, settingsKey, StringComparison.Ordinal))
        {
            return new(BlockedProcessThresholdOutcome.AlreadyTried);
        }

        try
        {
            var threshold = await sendBatch();
            _triedSettings[serverId] = settingsKey;
            return new(threshold == 0 ? BlockedProcessThresholdOutcome.Configured : BlockedProcessThresholdOutcome.AlreadySet);
        }
        catch (DbException ex) when (!isTransient(ex))
        {
            _triedSettings[serverId] = settingsKey;
            return new(BlockedProcessThresholdOutcome.Refused, ex);
        }
        catch (DbException)
        {
            return new(BlockedProcessThresholdOutcome.Transient);
        }
    }

    /// <summary>Forgets what was tried for a server, so its next ensure sends the batch again.</summary>
    public void Forget(int serverId) => _triedSettings.TryRemove(serverId, out _);

    /// <summary>
    /// The one line logged when a server refuses the bootstrap: what failed, then the remedy for the platform. AWS RDS has no
    /// sp_configure for this setting, so its remedy is the instance's parameter group. Anywhere else the refusal is the login's
    /// missing ALTER SETTINGS permission. Azure SQL Database never reaches the bootstrap: its blocked-process session is
    /// database-scoped and has no server threshold to set.
    /// </summary>
    public static string RefusalMessage(CollectorTargetInfo target, string serverDetail)
    {
        var remedy = target.IsAwsRds
            ? "AWS RDS does not allow sp_configure for this setting: set 'blocked process threshold (s)' to 5 in the DB parameter group of the instance."
            : "Grant the monitoring login ALTER SETTINGS, or run sp_configure 'blocked process threshold (s)', 5 and RECONFIGURE on the server as a sysadmin.";

        return "Could not set 'blocked process threshold (s)' to 5 seconds, so blocked process reports are not raised and the blocked-process XE session stays empty. "
            + remedy
            + " Blocking is still captured by the DMV blocking snapshot. This is logged once per server until the service restarts or the server's connection settings change."
            + $" Detail: {serverDetail}";
    }

    /* A connection string can carry a password, so the settings are kept as a hash. */
    private static string KeyOf(string connectionSettings) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(connectionSettings)));
}
