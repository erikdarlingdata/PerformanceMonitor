/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Common;

/// <summary>
/// The text of the error thrown when master cannot be read on Azure SQL Database and the connection has no
/// target database to fall back to. One copy for both products (#5647): Lite and Darling each carried a
/// near-identical string, and the #5500 change for #5498 left the set-a-database advice on the end of the
/// 40615 message in both.
///
/// 40615 is a firewall rejection ("Client with IP address ... is not allowed to access the server"). A
/// missing database setting never causes it, so the 40615 message names the firewall and the client IP the
/// server reported and says nothing about setting a database. The firewall advice already covers a client that
/// is admitted only by a database-level rule on a user database: a database-level rule can be created in master,
/// which makes master reachable, so a set-a-database sentence is not needed there. Any other known error number
/// (4060, 18456) gets the set-a-database advice alone, since the firewall is ruled out; only a caller with no
/// number keeps the combined message.
///
/// The decision is made on the SQL error number the caller read off the SqlException, never on the message
/// text, so a translated server message cannot change which advice a user is given. Only the client IP is
/// read out of the server's text, and its absence just leaves the IP out of the message.
/// </summary>
public static class AzureMasterFallbackMessage
{
    /// <summary>Azure SQL: the client IP address is not allowed through the logical server's firewall.</summary>
    public const int FirewallErrorNumber = 40615;

    /* The IP is the only thing taken from the server's text, and only when it is made of address characters,
       so an odd or hostile message cannot put arbitrary text into ours. */
    private static readonly Regex ClientIpPattern = new(
        @"IP address '(?<ip>[0-9A-Fa-f:.%]{2,64})'",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
        TimeSpan.FromSeconds(1));

    /// <summary>
    /// The client IP address a 40615 message reports (<c>Client with IP address '192.0.2.1' is not allowed ...</c>),
    /// or null when the message does not carry one.
    /// </summary>
    public static string? ExtractClientIp(string? serverMessage)
    {
        if (string.IsNullOrEmpty(serverMessage))
        {
            return null;
        }

        try
        {
            var match = ClientIpPattern.Match(serverMessage);
            return match.Success ? match.Groups["ip"].Value : null;
        }
        catch (RegexMatchTimeoutException)
        {
            return null;
        }
    }

    /// <param name="reason">The caller's description, for example <c>master DB inaccessible (SQL error 40615)</c>.</param>
    /// <param name="errorNumber">The SQL error number from the SqlException, or null when the caller has none.</param>
    /// <param name="serverMessage">The SqlException message, used only to read the client IP out of a 40615.</param>
    /// <param name="displayName">The server's display name, quoted in the set-a-database advice.</param>
    /// <param name="databaseSetting">What the product calls the setting (Lite: "Database", Darling: "database").</param>
    public static string Build(
        string reason, int? errorNumber, string? serverMessage, string displayName, string databaseSetting)
    {
        var head = $"{reason}, and this connection has no target database to fall back to (it resolves to master). ";

        if (errorNumber == FirewallErrorNumber)
        {
            var ip = ExtractClientIp(serverMessage);
            var client = ip is null ? "this client's IP address" : $"this client's IP address ({ip})";

            return head +
                $"The Azure firewall did not allow {client}. Add a firewall rule for it, " +
                "either a server-level rule or a database-level rule in master.";
        }

        var setDatabase =
            $"If this login can open a user database but not master, set a {databaseSetting} for " +
            $"'{displayName}' so database-scoped collectors have something to read.";

        if (errorNumber is not null)
        {
            return head + setDatabase;
        }

        return head +
            "If the error is 40615, the Azure firewall did not allow this client's IP address: " +
            "add a firewall rule for it, either a server-level rule or a database-level rule in master. " +
            setDatabase;
    }
}
