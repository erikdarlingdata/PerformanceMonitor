using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The one rule that refuses Windows authentication over a connection whose server identity is not verified (#9640), the twin of Darling's
/// <c>DarlingUnverifiedWindowsAuthScreen</c>. Windows authentication presents the Windows account Lite runs as to whatever host the caller names. A tool
/// call that names a host and says <c>trust_server_certificate</c> true, or <c>encrypt_mode</c> Optional, gives the connection nothing that proves the
/// host is the server meant, so <c>add_servers</c> and <c>edit_server</c> refuse the combination BEFORE any connection test, unless the operator has set
/// <see cref="Setting"/> in settings.json (<see cref="McpSettings.AllowWindowsAuthOverUnverifiedTls"/>, default off).
///
/// <para>"Windows authentication" is the connector's own definition (<see cref="ServerConnection.ApplyAuthentication"/>): the mode that sets integrated
/// security, which in Lite is <see cref="AuthenticationTypes.Windows"/> only. "Not verified" is <c>TrustServerCertificate</c>, or an <c>EncryptMode</c> of
/// Optional, the only mode the connector does not map to Mandatory or Strict.</para>
/// </summary>
internal static class McpUnverifiedWindowsAuthScreen
{
    /// <summary>The settings.json key that lets the two tools accept the combination. Read at start, so a change needs a restart.</summary>
    internal const string Setting = "mcp_allow_windows_auth_over_unverified_tls";

    /// <summary>
    /// Why a server with these settings must not be saved, or null when it may be. PURE: it reads only its arguments.
    /// <paramref name="refusedAs"/> is the first words of the answer, "Not added" for add_servers and "Not saved" for edit_server.
    /// </summary>
    internal static string? Refusal(bool allow, string authenticationType, string? encryptMode, bool trustServerCertificate, string refusedAs)
    {
        if (allow || authenticationType != AuthenticationTypes.Windows)
        {
            return null;
        }

        var unverified = new List<string>(2);
        if (trustServerCertificate)
        {
            unverified.Add("trust_server_certificate is true");
        }

        if (string.Equals(encryptMode?.Trim(), "Optional", StringComparison.OrdinalIgnoreCase))
        {
            unverified.Add("encrypt_mode is Optional");
        }

        if (unverified.Count == 0)
        {
            return null;
        }

        return $"{refusedAs}: Windows authentication presents the Windows account Lite runs as to the host you named, and this entry " +
               $"({string.Join(" and ", unverified)}) would not confirm that the host is the server you meant. Use auth \"SQL\" with a login instead, " +
               "or leave trust_server_certificate false and set encrypt_mode to \"Mandatory\" or \"Strict\". An operator can allow this combination by " +
               $"setting {Setting} to true in settings.json and restarting Lite.";
    }
}
