/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// Why an <see cref="AuthenticationTypes.EntraDefaultCredential"/> sign-in failed, or
/// <see cref="None"/> when the failure was not the credential chain's.
/// </summary>
public enum EntraAmbientCredentialFailureKind
{
    /// <summary>
    /// Not an ambient-credential failure. Ordinary login, network and timeout failures land here,
    /// and so does a broker failure — those belong to <see cref="EntraBrokerFailure"/>.
    /// </summary>
    None,

    /// <summary>
    /// Every credential source was asked and every one declined: there is no Azure sign-in on this
    /// machine for this mode to use. This is the first-run failure, and it is the one with something
    /// the user can actually do about it.
    /// </summary>
    NoCredentialFound,

    /// <summary>
    /// A credential source was available and threw while using it — a broken token cache, a helper
    /// process that failed, a network refusal reaching the identity endpoint. Distinct from
    /// <see cref="NoCredentialFound"/> because nothing is missing: something is wrong.
    /// </summary>
    CredentialSourceFaulted,
}

/// <summary>
/// Classifies a failed <see cref="AuthenticationTypes.EntraDefaultCredential"/> connection into
/// "there is no Azure sign-in here" versus "one broke", and says in plain language what to do.
///
/// <para><b>Why this is not optional.</b> The mode's whole value is that it avoids the Windows
/// account broker, and its whole risk is that it fails the same way the broker does: opaquely.
/// <c>DefaultAzureCredential</c> reports a chain of nine sources each saying it was unavailable, and
/// "none of several credential providers were available" is precisely the unactionable text #3201
/// existed to stop showing people. A mode that replaces one incomprehensible failure with another
/// is not a fix, so the failure gets named here and the dialog says which of the two it was.</para>
///
/// <para><b>Matched on message text, for the same reason <see cref="EntraBrokerFailure"/> is.</b>
/// The exception arrives wrapped: <c>Microsoft.Data.SqlClient</c> wraps the
/// <c>AuthenticationFailedException</c>, which for the chain case is a
/// <c>CredentialUnavailableException</c> carrying an inner <c>AggregateException</c>. Reading a type
/// off the middle layer would mean referencing <c>Azure.Identity</c> from here and pinning the app
/// to whichever version the driver happens to carry; the text travels, and each source's own reason
/// (including "run <c>az login</c>") is appended to it by
/// <c>CredentialUnavailableException.CreateAggregateException</c>, which is why logging the chain is
/// worth as much as classifying it.</para>
///
/// <para><b>A known blind spot, stated rather than papered over.</b>
/// <c>CreateAggregateException</c> returns the single exception unchanged when the chain produced
/// exactly one, so the marker below is absent in that case and this classifies as
/// <see cref="EntraAmbientCredentialFailureKind.None"/>. With the options the driver hard-codes the
/// chain always holds nine sources, so the collapse is only reachable when the user has narrowed it
/// with the <c>AZURE_TOKEN_CREDENTIALS</c> environment variable. The consequence of being wrong
/// there is that the dialog falls back to the driver's error plus the log pointer — #3201's
/// behaviour — rather than asserting a cause it cannot support.</para>
///
/// <para><b>Unverified against a live Entra tenant</b>, like everything else on this path. The
/// markers are the literal message constants in <c>Azure.Identity</c> 1.18.0's
/// <c>DefaultAzureCredential</c> (:63 and :64); that a real failing tenant renders them is read off
/// that source, not off a run.</para>
/// </summary>
public static class EntraAmbientCredentialFailure
{
    /* Azure.Identity 1.18.0, DefaultAzureCredential.cs:63 - the message every all-sources-declined
       failure is built from. Truncated before "See the troubleshooting guide", which is a shared
       suffix several unrelated Azure.Identity messages also carry and would match far too much. */
    private const string NoCredentialMarker = "DefaultAzureCredential failed to retrieve a token";

    /* Same file, :64. The trailing colon and the exception text after it are dropped: the constant
       ends with ": " and the source exception's rendering follows, so matching that far would depend
       on formatting rather than on the message. */
    private const string FaultedMarker = "DefaultAzureCredential authentication failed due to an unhandled exception";

    /// <summary>
    /// Classifies <paramref name="ex"/> and every exception nested inside it.
    /// </summary>
    /// <param name="ex">The failure from the connection attempt. Null classifies as
    /// <see cref="EntraAmbientCredentialFailureKind.None"/>.</param>
    /// <returns>Why the credential chain failed, or
    /// <see cref="EntraAmbientCredentialFailureKind.None"/> when it was not the chain that failed.</returns>
    public static EntraAmbientCredentialFailureKind Classify(Exception? ex)
    {
        /* Walked to the bottom, because the driver's own message is the outermost layer and the
           chain's is underneath it - the same shape that made a Message-only read useless in #3196.

           OUTERMOST WINS, and unlike EntraBrokerFailure that is the whole of the priority rule here.
           The two markers are prefixes of different sentences, so no single message can match both,
           and the order the two ifs are written in is therefore not observable by any caller - only
           the DEPTH is. Azure.Identity produces the two at different depths: GetTokenFromCredential-
           Async's unhandled-exception wrapper (DefaultAzureCredential.cs:199) sits above whatever the
           source threw, so the shallower marker describes this attempt and the deeper one is context
           it wrapped. Anything that flattens the chain before matching loses that distinction, which
           is what Classify_TakesTheShallowestMarkerWhenTheChainHoldsBoth asserts in both directions. */
        for (var current = ex; current is not null; current = current.InnerException)
        {
            var message = current.Message;
            if (string.IsNullOrEmpty(message))
                continue;

            if (Contains(message, FaultedMarker))
                return EntraAmbientCredentialFailureKind.CredentialSourceFaulted;

            if (Contains(message, NoCredentialMarker))
                return EntraAmbientCredentialFailureKind.NoCredentialFound;
        }

        return EntraAmbientCredentialFailureKind.None;
    }

    /// <summary>
    /// The user-facing explanation for an ambient-credential failure, or null for
    /// <see cref="EntraAmbientCredentialFailureKind.None"/>.
    /// </summary>
    /// <param name="kind">The classification from <see cref="Classify"/>.</param>
    /// <returns>Text to show beneath the driver's own error, or null when there is nothing to add.</returns>
    public static string? Explain(EntraAmbientCredentialFailureKind kind) => kind switch
    {
        EntraAmbientCredentialFailureKind.NoCredentialFound =>
            "This authentication mode signs in as an Azure identity that is already established on "
            + "this machine. It never prompts, and it found nothing to use: no Azure CLI session, no "
            + "Azure PowerShell session, no Visual Studio or VS Code sign-in, no AZURE_CLIENT_ID / "
            + "AZURE_CLIENT_SECRET environment variables, and no managed identity. The usual fix is "
            + "to open a terminal, run \"az login\", sign in there, and then test the connection "
            + "again. If you cannot install the Azure CLI, use Azure — Service Principal "
            + "instead. The log names each source and why it declined.",

        EntraAmbientCredentialFailureKind.CredentialSourceFaulted =>
            "An Azure sign-in was found on this machine but failed while being used, which is a "
            + "different problem from having none: something is broken rather than missing. The "
            + "server name, the database name and the target server are not implicated. The log "
            + "holds the underlying error; signing in again with \"az login\" clears the common case "
            + "of a stale cached token.",

        _ => null,
    };

    private static bool Contains(string haystack, string needle) =>
        haystack.Contains(needle, StringComparison.OrdinalIgnoreCase);
}
