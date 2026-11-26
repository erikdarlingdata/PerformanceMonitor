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
    /// The Encrypt box position (0 Optional, 1 Mandatory, 2 Strict) the Edit window shows for a stored mode. It follows
    /// <see cref="EncryptOptionFor"/>: a blank or unknown mode is Mandatory, so the window shows, and tests, what Lite connects with.
    /// </summary>
    internal static int EncryptBoxIndexFor(string? encryptMode) => EncryptOptionFor(encryptMode) switch
    {
        var option when option == SqlConnectionEncryptOption.Optional => 0,
        var option when option == SqlConnectionEncryptOption.Strict => 2,
        _ => 1
    };

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
    /// The encryption option a stored mode builds. It is <see cref="ServerConnection.EncryptOptionFor"/>, the one mapping the connection
    /// builder uses: a blank or unknown mode is Mandatory, never a silent drop to an unencrypted connection (#9237).
    /// </summary>
    internal static SqlConnectionEncryptOption EncryptOptionFor(string? encryptMode) => ServerConnection.EncryptOptionFor(encryptMode);

    /// <summary>
    /// True when a stored mode builds <c>encrypt_mode</c> Strict (<see cref="EncryptOptionFor"/>), which ignores trust_server_certificate.
    /// The certificate sentence and the Strict line both read it, so a Strict entry gets one piece of advice (#9692).
    /// </summary>
    internal static bool UsesStrict(string? encryptMode) => EncryptOptionFor(encryptMode) == SqlConnectionEncryptOption.Strict;

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
            var encrypt = EncryptOptionFor(server.EncryptMode);
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
            return FailureText(ex, server.EncryptMode);
        }
    }

    /// <summary>
    /// #9674: the failure text a connection test shows. The driver's message, with the Strict line after it when the entry used
    /// Strict and the failure is a transport failure (<see cref="IsTransportFailure"/>, <see cref="StrictEncryptionHint"/>). The Add Server window uses it too.
    /// </summary>
    internal static string FailureText(Exception ex, string? encryptMode)
    {
        /* Lite connects with Strict only for the exact stored word, so a "strict" it builds as Mandatory gets no Strict line, and
           its certificate sentence still offers trustServerCertificate. */
        var usedStrict = UsesStrict(encryptMode);
        /* #9692: a server certificate this machine rejects gets the shared sentence that says so after the driver's text. */
        var text = SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory) ?? ServerCertificateTrustFailure.Append(ex.Message, ex, usedStrict);
        /* A native-library load failure is not a Strict problem: no SQL Server connection can work from this install. */
        if (ex is OperationCanceledException || SqlClientNativeLoadFailure.IsMatch(ex))
        {
            return text;
        }

        return StrictEncryptionHint.Append(text, usedStrict ? "Strict" : null, IsTransportFailure(ex));
    }

    /// <summary>
    /// #9674: true when <paramref name="ex"/> is a <see cref="SqlException"/> (or wraps one) none of whose errors the server sent. A
    /// server-sent error has a class below 20 and a positive number: a login failure, 4064, 40613. A reset connection (10054), a
    /// certificate chain failure, a refused port, a timeout and other client-side errors are transport failures. Any exception that is
    /// not a SqlException, such as a failure to build the connection before it is made, is not.
    /// </summary>
    internal static bool IsTransportFailure(Exception ex)
    {
        var sql = ex as SqlException ?? ex.InnerException as SqlException;
        if (sql is null)
        {
            return false;
        }

        foreach (SqlError error in sql.Errors)
        {
            if (error.Class < 20 && error.Number > 0)
            {
                return false;
            }
        }

        return true;
    }
}
