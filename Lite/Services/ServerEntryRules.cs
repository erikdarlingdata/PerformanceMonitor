/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9237: the rules the Add Server window applies before it saves, with no window in them, so the window and
/// <c>--add-server</c> run the same ones. The host rule is <see cref="ServerManager.HostRefusal"/> and the id rule is
/// <see cref="ServerManager.FindServerIdHolder"/>. This class holds the credential rule and the connection test.
/// </summary>
internal static class ServerEntryRules
{
    /// <summary>
    /// The sentence to refuse with when the mode needs a user name or a secret and has none, otherwise null. SQL Server
    /// authentication needs a user name, and service principal needs a client id and a secret. The other modes need nothing here.
    /// </summary>
    internal static string? CredentialRefusal(string authenticationType, string? username, string? password)
    {
        if (authenticationType == AuthenticationTypes.ServicePrincipal)
        {
            if (string.IsNullOrEmpty(username))
                return "Client (Application) ID is required for service principal authentication.";
            if (string.IsNullOrEmpty(password))
                return "Client secret is required for service principal authentication.";
        }
        else if (authenticationType == AuthenticationTypes.SqlServer && string.IsNullOrEmpty(username))
        {
            return "Username is required for SQL Server authentication.";
        }

        return null;
    }

    /// <summary>
    /// The Test Connection builder before authentication is applied. The Data Source goes through
    /// <see cref="ServerHostSetting.TcpDataSource"/>, as the collectors' own builder does, so the test does not fall back
    /// to a named pipe over SMB with this process's Windows credentials (#6352).
    /// </summary>
    internal static SqlConnectionStringBuilder BuildBaseConnectionBuilder(
        string serverName,
        string databaseName,
        bool trustServerCertificate,
        SqlConnectionEncryptOption encrypt,
        bool readOnlyIntent,
        bool multiSubnetFailover)
    {
        var dbName = databaseName.Trim();
        return new SqlConnectionStringBuilder
        {
            DataSource = ServerHostSetting.TcpDataSource(serverName.Trim()),
            InitialCatalog = string.IsNullOrEmpty(dbName) ? "master" : dbName,
            ApplicationName = "PerformanceMonitorLite",
            ConnectTimeout = 10,
            TrustServerCertificate = trustServerCertificate,
            Encrypt = encrypt,
            ApplicationIntent = readOnlyIntent ? ApplicationIntent.ReadOnly : ApplicationIntent.ReadWrite,
            MultiSubnetFailover = multiSubnetFailover
        };
    }

    /// <summary>
    /// The connection test <c>--add-server</c> runs: opens the server with the entry's own credentials and reads the detection
    /// query, as the window's Test Connection does. Returns null when it connected, otherwise the failure text. The text is the
    /// driver's message, which never carries the password.
    /// </summary>
    internal static async Task<string?> TestConnectionAsync(
        ServerConnection server, string? username, string? password, CancellationToken cancellationToken)
    {
        try
        {
            var encrypt = server.EncryptMode switch
            {
                "Mandatory" => SqlConnectionEncryptOption.Mandatory,
                "Strict" => SqlConnectionEncryptOption.Strict,
                _ => SqlConnectionEncryptOption.Optional
            };
            var builder = BuildBaseConnectionBuilder(
                server.ServerName, server.DatabaseName ?? string.Empty, server.TrustServerCertificate, encrypt,
                server.ReadOnlyIntent, server.MultiSubnetFailover);
            ServerConnection.ApplyAuthentication(
                builder, server.AuthenticationType, username, password, server.AzureClientId, server.ManagedIdentityClientId);

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            using var command = new SqlCommand(ServerManager.DetectionQueryText, connection);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory) ?? ex.Message;
        }
    }
}
