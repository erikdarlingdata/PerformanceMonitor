/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one rule that refuses Windows authentication over a connection whose server identity is not verified, held
/// apart from the MCP caller policy so the web dashboard's add and edit routes can run it without the policy's other
/// checks (the agent-route holds). It reads one setting's value and carries that setting's NAME, so a refusal names
/// the key that applies: <c>mcp.allowWindowsAuthOverUnverifiedTls</c> over MCP, <c>web.allowWindowsAuthOverUnverifiedTls</c>
/// on the web.
///
/// <para>"Windows authentication" here is the connector's own definition: the arm of
/// <c>MonitoredServerConnection.BuildConnectionString</c> that sets integrated security, which is every mode that is
/// not SQL, service principal or managed identity. "Not verified" is <c>TrustServerCertificate</c>, or an
/// <c>EncryptMode</c> of Optional, the only mode the connector does not map to Mandatory or Strict.</para>
/// </summary>
internal sealed class DarlingUnverifiedWindowsAuthScreen
{
    /// <summary>The darling.json key that lets the web dashboard's add and edit accept Windows authentication over a
    /// connection whose server identity is not verified.</summary>
    internal const string WebSetting = "web.allowWindowsAuthOverUnverifiedTls";

    private readonly bool _allow;
    private readonly string _settingName;

    internal DarlingUnverifiedWindowsAuthScreen(bool allow, string settingName)
    {
        _allow = allow;
        _settingName = settingName;
    }

    /// <summary>The screen the web routes run, from <c>web.allowWindowsAuthOverUnverifiedTls</c>.</summary>
    internal static DarlingUnverifiedWindowsAuthScreen ForWeb(bool allow) => new(allow, WebSetting);

    /// <summary>Why <paramref name="server"/> must not be saved, or null when it may be. PURE: it reads only the
    /// parsed entry and the setting.</summary>
    internal string? Refusal(MonitoredServer server)
    {
        if (_allow || server.UsesSqlAuth || server.UsesEntra)
        {
            return null;
        }

        var unverified = new List<string>(2);
        if (server.TrustServerCertificate)
        {
            unverified.Add("trust_server_certificate is true");
        }

        if (string.Equals(server.EncryptMode?.Trim(), "Optional", StringComparison.OrdinalIgnoreCase))
        {
            unverified.Add("encrypt_mode is Optional");
        }

        if (unverified.Count == 0)
        {
            return null;
        }

        return "Not added: Windows authentication presents this service's own computer account to the host you " +
               $"named, and this entry ({string.Join(" and ", unverified)}) would not confirm that the host is the " +
               "server you meant. Use auth \"SQL\" with a login instead, or leave trust_server_certificate false and " +
               "set encrypt_mode to \"Mandatory\" or \"Strict\". An operator can allow this combination by setting " +
               $"{_settingName} to true in darling.json and restarting the service.";
    }

    /// <summary>
    /// Moves every entry <paramref name="screen"/> refuses out of <paramref name="entries"/> and into
    /// <paramref name="results"/> as an <c>invalid</c> row, and returns the entries that may go on. Runs before the
    /// store is opened or a probe is made. A null screen refuses nothing.
    /// </summary>
    internal static List<DarlingMcpServerAdminTools.ParsedServerEntry> Screen(
        DarlingUnverifiedWindowsAuthScreen? screen,
        List<DarlingMcpServerAdminTools.ParsedServerEntry> entries,
        List<DarlingMcpServerAdminTools.ServerResult> results)
    {
        if (screen is null)
        {
            return entries;
        }

        var allowed = new List<DarlingMcpServerAdminTools.ParsedServerEntry>(entries.Count);
        foreach (var entry in entries)
        {
            var refusal = screen.Refusal(entry.ProbeConfig);
            if (refusal is null)
            {
                allowed.Add(entry);
            }
            else
            {
                results.Add(new DarlingMcpServerAdminTools.ServerResult(
                    entry.Order, entry.DisplayName, DarlingMcpServerAdminTools.AddStatus.Invalid, refusal));
            }
        }

        return allowed;
    }
}
