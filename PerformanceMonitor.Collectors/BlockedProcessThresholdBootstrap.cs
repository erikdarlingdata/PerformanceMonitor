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
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>What <see cref="BlockedProcessThresholdBootstrap.RunAsync"/> did for one ensure of the blocked-process session (#9743).</summary>
public enum BlockedProcessThresholdOutcome
{
    /// <summary>This server already refused the batch under these connection settings in this process. Nothing was sent to it.</summary>
    AlreadyRefused,

    /// <summary>The server read 0 and the batch set the threshold to 5 seconds.</summary>
    Configured,

    /// <summary>The server already had a threshold. The batch changed nothing.</summary>
    AlreadySet,

    /// <summary>The server answered the batch with a refusal (the login lacks ALTER SETTINGS, or the platform has no sp_configure for it, see <see cref="BlockedProcessThresholdBootstrap.IsRefusal"/>). It is not asked again under these connection settings.</summary>
    Refused,

    /// <summary>The batch failed with anything that is not the server's refusal: a timeout, a dropped connection, a deadlock victim, a lock timeout. That says nothing about the server's answer, so it is asked again on the next ensure.</summary>
    Transient,
}

/// <summary>The outcome of one <see cref="BlockedProcessThresholdBootstrap.RunAsync"/>, with the server's error when it was refused.</summary>
public readonly record struct BlockedProcessThresholdResult(BlockedProcessThresholdOutcome Outcome, DbException? Refusal = null);

/// <summary>
/// The refusal memory behind the blocked process threshold bootstrap, shared by Lite and Darling (#9743). Both products
/// ensure the blocked-process session on every run of its collector (Lite) or on every connect and hourly sweep (Darling), and the
/// ensure sends <c>sp_configure 'blocked process threshold (s)'</c> to a server that reads 0. A server that refuses it (AWS RDS, or a
/// login without ALTER SETTINGS) is asked and logged once per process, and once more after its connection settings change, and the
/// one failure carries the remedy for its platform. A server that did not refuse is read on every run: the read is one row of
/// sys.configurations, and the server behind an unchanged connection string can change under it (an availability group listener
/// that fails over to a replica whose threshold is 0), so the threshold is checked each time.
///
/// <para>Only the server's own answer is remembered. <see cref="IsRefusal"/> lists the SQL Server error numbers that are that answer:
/// the permission denials <see cref="SqlServerPermissionErrors.IsPermissionDenied"/> reads (15247 from sp_configure without
/// ALTER SETTINGS, 229, 262, 297 and 300 for an object, database or server permission denied, 916 and 8189), 5812 (RECONFIGURE
/// refused), 15123 (the option does not exist or is not advanced) and 2812 (sp_configure is not found). AWS RDS answers the batch
/// with 15247 and 5812. Any other error is a fault of the moment (a deadlock victim 1205, a lock timeout 1222, a command timeout,
/// a dropped connection such as SNI 64 or 121) and is retried on the next run, because remembering it would switch blocked process
/// reports off for the server until the process restarts.</para>
/// </summary>
public sealed class BlockedProcessThresholdBootstrap
{
    private readonly ConcurrentDictionary<int, string> _refusedSettings = new();

    /// <summary>
    /// True when the SQL Server error number is the server's answer that it will not take the batch, rather than a fault of the moment.
    /// The permission denials come from <see cref="SqlServerPermissionErrors.IsPermissionDenied"/>, so this list cannot drift from the
    /// rest of the product; 5812, 15123 and 2812 are the refusals that are specific to sp_configure and RECONFIGURE.
    /// </summary>
    public static bool IsRefusal(int sqlErrorNumber)
        => SqlServerPermissionErrors.IsPermissionDenied(sqlErrorNumber) || sqlErrorNumber is 5812 or 15123 or 2812;

    /// <summary>
    /// Sends the bootstrap batch to the server unless it already refused it with these connection settings. <paramref name="sendBatch"/>
    /// returns the threshold the server read before the batch ran: 0 means the batch set it to 5 seconds. A server that answers is
    /// not recorded, so the next run reads its threshold again. A SQL Server error that <see cref="IsRefusal"/> lists is the server's
    /// refusal: it is returned as <see cref="BlockedProcessThresholdOutcome.Refused"/> with the error, and the server is not asked
    /// again. Any other database error is <see cref="BlockedProcessThresholdOutcome.Transient"/> and records nothing. This project has no
    /// SqlClient, so <paramref name="errorNumbersOf"/> is each host's reader of the SQL error numbers a failure carries. Anything that is
    /// not a database error (a cancellation, a non-SQL fault) propagates and records nothing.
    /// </summary>
    public async Task<BlockedProcessThresholdResult> RunAsync(
        int serverId, string connectionSettings, Func<Task<int>> sendBatch, Func<DbException, IEnumerable<int>> errorNumbersOf)
    {
        var settingsKey = KeyOf(connectionSettings);
        if (_refusedSettings.TryGetValue(serverId, out var refused) && string.Equals(refused, settingsKey, StringComparison.Ordinal))
        {
            return new(BlockedProcessThresholdOutcome.AlreadyRefused);
        }

        try
        {
            var threshold = await sendBatch();
            return new(threshold == 0 ? BlockedProcessThresholdOutcome.Configured : BlockedProcessThresholdOutcome.AlreadySet);
        }
        catch (DbException ex) when (errorNumbersOf(ex).Any(IsRefusal))
        {
            _refusedSettings[serverId] = settingsKey;
            return new(BlockedProcessThresholdOutcome.Refused, ex);
        }
        catch (DbException)
        {
            return new(BlockedProcessThresholdOutcome.Transient);
        }
    }

    /// <summary>Forgets a server's refusal, so its next ensure sends the batch again. Called when the server is removed, so a re-added server is tried again.</summary>
    public void Forget(int serverId) => _refusedSettings.TryRemove(serverId, out _);

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
