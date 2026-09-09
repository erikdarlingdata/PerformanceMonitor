/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// Constants for server authentication types.
/// </summary>
public static class AuthenticationTypes
{
    /// <summary>
    /// Windows integrated authentication.
    /// </summary>
    public const string Windows = "Windows";

    /// <summary>
    /// SQL Server username/password authentication.
    /// </summary>
    public const string SqlServer = "SqlServer";

    /// <summary>
    /// Microsoft Entra MFA (Azure AD) interactive authentication.
    /// </summary>
    public const string EntraMFA = "EntraMFA";

    /// <summary>
    /// Microsoft Entra service principal (client id + secret) — non-interactive.
    /// Maps to SqlAuthenticationMethod.ActiveDirectoryServicePrincipal.
    /// </summary>
    public const string ServicePrincipal = "ServicePrincipal";

    /// <summary>
    /// Azure managed identity (system- or user-assigned) — non-interactive.
    /// Maps to SqlAuthenticationMethod.ActiveDirectoryManagedIdentity.
    /// Only works when the app runs on an Azure resource with a managed identity.
    /// </summary>
    public const string ManagedIdentity = "ManagedIdentity";

    /// <summary>
    /// Microsoft Entra, signing in as whichever Azure identity is <b>already established on this
    /// machine</b> — an <c>az login</c> session, an Azure PowerShell session, a Visual Studio /
    /// VS Code sign-in, the <c>AZURE_CLIENT_ID</c>/<c>AZURE_CLIENT_SECRET</c> environment
    /// variables, a workload identity, or a managed identity. Maps to
    /// <c>SqlAuthenticationMethod.ActiveDirectoryDefault</c>.
    ///
    /// <para><b>It prompts for nothing, ever.</b> That is not a design choice here; the driver
    /// forces it. <c>ActiveDirectoryAuthenticationProvider.CreateTokenCredentialInstance</c> builds
    /// the <c>DefaultAzureCredential</c> itself with <c>ExcludeInteractiveBrowserCredential = true</c>
    /// hard-coded (<c>Microsoft.Data.SqlClient.Extensions.Azure</c> 7.0.2, :869), so there is no
    /// browser and no account picker. A machine with no Azure credential on it does not get a
    /// sign-in window — it gets a failure, and <see cref="EntraAmbientCredentialFailure"/> exists to
    /// make that failure say so.</para>
    ///
    /// <para><b>Why it exists: it is the one Entra mode that never reaches the Windows account
    /// broker</b>, which <see cref="EntraMFA"/> cannot avoid (see
    /// <see cref="EntraBrokerFailure"/>). Two independent facts put the broker out of reach, and
    /// both are load-bearing:</para>
    ///
    /// <para>1. <i>The driver returns before it builds anything brokered.</i> In
    /// <c>AcquireTokenAsync</c> at 7.0.2, the <c>ActiveDirectoryDefault</c> arm resolves a
    /// <c>DefaultAzureCredential</c> and <b>returns at :264</b>. The MSAL public-client application
    /// — the only thing <c>WithBroker(new BrokerOptions(...Windows))</c> is ever attached to, at
    /// :826 — is not constructed until <c>GetPublicClientAppInstanceAsync</c> at <b>:325</b>. 264 is
    /// before 325, so on this path no public-client app exists to configure a broker on. This is the
    /// order of two statements in one method, not an inference about MSAL.</para>
    ///
    /// <para>2. <i>The credential chain's own broker link is inert in this build.</i>
    /// <c>Azure.Identity</c> 1.18.0 puts a <c>BrokerCredential</c> at the tail of
    /// <c>DefaultAzureCredential</c>'s chain and <c>ExcludeBrokerCredential</c> defaults to
    /// <b>false</b>, so leg 1 alone would not settle it. That link throws
    /// <c>CredentialUnavailableException</c> on first use unless the <c>Azure.Identity.Broker</c>
    /// package is present, which it is not anywhere in this repository's dependency graph.
    /// <c>EntraDefaultCredentialTests</c> pins that absence two ways, because adding that package —
    /// directly or transitively — would silently put the Windows broker back at the end of this
    /// mode's chain and reintroduce exactly the failure it exists to route around.</para>
    ///
    /// <para><b>Unverified against a live Entra tenant.</b> Everything above is read off the pinned
    /// package sources. That a real tenant then issues a token to a real <c>az login</c> session
    /// through this path has not been run by anyone here — it needs a tenant and a Windows host, the
    /// same limit #3196 and #2184 had. Treat it as offered-for-testing, not as confirmed working.</para>
    /// </summary>
    public const string EntraDefaultCredential = "EntraDefaultCredential";

    /// <summary>
    /// True when signing in with this mode can put a window in front of the user.
    ///
    /// <para>This exists so "which modes are interactive" is a stated decision in one place rather
    /// than an equality test repeated at each gate that cares. The background-collection and
    /// dialog-open gates in Lite's <c>ServerManager</c> both suppress connection attempts for
    /// interactive modes — the point being to not raise a sign-in prompt at a user who is not
    /// looking at the app. A new non-interactive mode inherits "not suppressed" by default, and a
    /// default is not a decision: <see cref="EntraDefaultCredential"/> is deliberately absent from
    /// this list because the driver excludes the interactive browser from its credential chain
    /// outright, so it can run unattended on a collection cycle with nothing to pop up.</para>
    /// </summary>
    /// <param name="authenticationType">One of the constants on this class, or any other value.</param>
    /// <returns>True only for modes that can raise a sign-in UI.</returns>
    public static bool RequiresInteractiveSignIn(string? authenticationType) =>
        authenticationType == EntraMFA;
}
