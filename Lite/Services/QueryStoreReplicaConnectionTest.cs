/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #6254: what the Add and Edit server dialog's Test button does for a server that names a Query Store replica. Collection reads
/// Query Store from the replica and, when the replica cannot be reached, from the primary without saying so, so a Test that only
/// opened the primary would report "works" for a replica that is down or refuses the login. This opens the replica connection the
/// way collection builds it (<see cref="ServerConnection.WithQueryStoreReplicaHost"/> over the same connection string the primary
/// test used: the replica host and <c>ApplicationIntent=ReadOnly</c>, nothing else changed) and reports the result apart from the
/// primary's. A replica that does not open is a warning, never a failed test.
/// </summary>
internal static class QueryStoreReplicaConnectionTest
{
    /// <summary>
    /// Opens the replica connection built from <paramref name="primaryConnectionString"/> and <paramref name="replicaHost"/>. Returns
    /// null when the replica text is empty (nothing to test), otherwise the host as it will be used and the error, null when the
    /// replica opened. Never throws except for cancellation. <paramref name="createConnection"/> is a test seam; null opens a
    /// <see cref="SqlConnection"/>, which is what collection opens.
    /// </summary>
    internal static async Task<(string Host, string? Error)?> RunAsync(
        string primaryConnectionString,
        string? replicaHost,
        Func<string, DbConnection>? createConnection = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(replicaHost))
        {
            return null;
        }

        var host = replicaHost.Trim();
        try
        {
            var connectionString = ServerConnection.WithQueryStoreReplicaHost(primaryConnectionString, host);
            await using var connection = createConnection is null ? new SqlConnection(connectionString) : createConnection(connectionString);
            /* A device-code sign-in needs its rendezvous (the window that shows the code) for this open too; null for every other mode. */
            using var deviceCode = EntraDeviceCodeAuth.Begin(new SqlConnectionStringBuilder(connectionString));
            await connection.OpenAsync(deviceCode?.Token ?? cancellationToken);
            return (host, null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (host, SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory) ?? ex.Message);
        }
    }

    /// <summary>
    /// The two results, apart: "Primary: connected." and the replica's. A replica that did not open says that collection reads the
    /// primary until it can, so the user knows what the warning costs.
    /// </summary>
    internal static string Compose((string Host, string? Error) replica) =>
        string.IsNullOrEmpty(replica.Error)
            ? $"Primary: connected.\nQuery Store replica {replica.Host}: connected."
            : $"Primary: connected.\nQuery Store replica {replica.Host}: could not connect ({replica.Error}).\n"
                + "Collection reads Query Store from the primary until the replica can be reached.";

    /// <summary>
    /// #6459: the sentence for a Query Store replica typed against an Azure SQL Database (EngineEdition 5), or null for any other
    /// engine or an empty box. Azure SQL Database has no readable-replica Query Store read: collection never opens the replica
    /// (<c>replicaConfigured</c> is false for it), so the setting is saved and has no effect, and the dialog says so.
    /// </summary>
    internal static string? IgnoredOnAzureSqlDatabaseNote(int engineEdition, string? replicaHost) =>
        engineEdition == PerformanceMonitor.Collectors.CollectorEngineCapability.AzureSqlDatabaseEngineEdition
        && !string.IsNullOrWhiteSpace(replicaHost)
            ? $"Query Store replica {replicaHost.Trim()}: not used. Azure SQL Database has no readable-replica Query Store read, "
                + "so this setting has no effect and collection reads Query Store from the database itself. Clear the replica box."
            : null;
}
