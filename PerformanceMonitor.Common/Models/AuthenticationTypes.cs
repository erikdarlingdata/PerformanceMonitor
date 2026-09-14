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
    /// Microsoft Entra, signing in by <b>typing a short code into a browser on any device</b> — the
    /// OAuth 2.0 device authorization grant. Maps to
    /// <c>SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow</c>.
    ///
    /// <para><b>Why it exists: it is the one INTERACTIVE Entra mode that needs neither the Windows
    /// account broker nor a user profile</b>, which is what makes it reachable from a process the
    /// interactive user did not launch normally. <see cref="EntraMFA"/> hands sign-in to the broker
    /// and cannot avoid it (see <see cref="EntraBrokerFailure"/>); <see cref="EntraDefaultCredential"/>
    /// reads a credential established under <c>%USERPROFILE%</c>, which is the launching account's
    /// profile and not necessarily the person's. Neither dependency exists here: the whole exchange
    /// is HTTP from this process to the tenant, and the human half happens in a browser this app
    /// never touches.</para>
    ///
    /// <para><b>The broker-free claim is about MSAL's request type, not about the application
    /// object.</b> On Windows the driver attaches <c>WithBroker(new BrokerOptions(...Windows))</c> to
    /// the MSAL public-client application it builds for <i>every</i> public-client mode
    /// (<c>Microsoft.Data.SqlClient.Extensions.Azure</c> 7.0.2,
    /// <c>ActiveDirectoryAuthenticationProvider.cs:806</c>), and caches that application process-wide
    /// — so this mode does not get a broker-free one, and does not need one.
    /// <c>DeviceCodeRequest.ExecuteAsync</c> (<c>Microsoft.Identity.Client</c> 4.84.2) POSTs to the
    /// authority's device-code endpoint through <c>OAuth2Client</c>, invokes the callback, then polls
    /// the token endpoint until the user finishes. That type names no broker, reads no broker option,
    /// and has no path that could reach one; <c>WithBroker</c> governs
    /// <c>AcquireTokenInteractive</c> and <c>AcquireTokenSilent</c> and has nothing to say about a
    /// device-code request.</para>
    ///
    /// <para><b>Three minutes, set by the driver rather than by the connection string.</b> The
    /// device-code arm builds its own <c>CancellationTokenSource</c> with
    /// <c>CancelAfter(TimeSpan.FromMinutes(3))</c> and passes THAT to <c>ExecuteAsync</c> rather than
    /// the <c>Connect Timeout</c>-derived token beside it (<c>:687-689</c>), so changing
    /// <c>Connect Timeout</c> does not move the deadline the user is racing, and the tenant's own
    /// code lifetime — typically fifteen minutes, and what <c>DeviceCodeResult.ExpiresOn</c> reports
    /// — is not the binding limit either. An overrun of <c>Connect Timeout</c> while the user is
    /// still typing is absorbed by the driver:
    /// <c>SqlInternalConnectionTds.AttemptRetryADAuthWithTimeoutError</c> resets the timeout timer
    /// and retries the connection once with the token already in hand, raising no second prompt.
    /// That is the accommodation <see cref="EntraMFA"/> has always depended on — both modes land in
    /// the same arm of <c>GetFedAuthToken</c> — so this mode inherits its timeout behaviour rather
    /// than introducing one.</para>
    ///
    /// <para><b>Interactive, so it is named in <see cref="RequiresInteractiveSignIn"/>.</b> Unlike
    /// <see cref="EntraDefaultCredential"/> this mode does raise something at the user, and a code
    /// nobody is looking at is worse than no code at all — see that method for why.</para>
    ///
    /// <para><b>Unverified against a live Entra tenant.</b> Everything above is read off the pinned
    /// package sources. That a real tenant issues a code, accepts it in a browser and hands SqlClient
    /// a token has not been run by anyone here — it needs a tenant and a Windows host, the same limit
    /// #3196, #3214 and #2184 had. Treat it as offered-for-testing, not as confirmed working.</para>
    /// </summary>
    public const string EntraDeviceCode = "EntraDeviceCode";

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
    ///
    /// <para><see cref="EntraDeviceCode"/> is present for a reason worth separating from
    /// <see cref="EntraMFA"/>'s. Entra MFA is suppressed because an account picker in front of
    /// someone who is not looking at the app is rude. Device code is suppressed because it would not
    /// work: the code it issues lives about three minutes, so one raised on a sweep expires unseen
    /// and turns a reachable server into a recurring authentication failure.</para>
    ///
    /// <para><b>This governs the connectivity sweep and the bulk-add belt, NOT data collection.</b>
    /// Lite's collector prompts for an interactive mode and serializes the prompt rather than
    /// skipping the server, which is how <see cref="EntraMFA"/> has always worked and is what makes
    /// either mode usable for collection at all — it asks once per run of the app and the driver's
    /// token cache serves the rest. What this list buys there is the SERIALIZATION and the
    /// honour-a-decline behaviour, not suppression. Reading it as "interactive modes are never
    /// prompted in the background" is wrong, and the connection dialog said so until #3196's review
    /// caught it.</para>
    /// </summary>
    /// <param name="authenticationType">One of the constants on this class, or any other value.</param>
    /// <returns>True only for modes that can raise a sign-in UI.</returns>
    public static bool RequiresInteractiveSignIn(string? authenticationType) =>
        authenticationType is EntraMFA or EntraDeviceCode;
}
