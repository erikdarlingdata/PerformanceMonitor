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
/// The kind of Windows Web Account Manager (WAM) broker failure behind a failed
/// <c>Authentication=ActiveDirectoryInteractive</c> connection, or <see cref="None"/> when the
/// failure is not a broker failure at all.
///
/// <para>The three broker arms are kept apart because they point at different things and are fixed
/// by different people. Collapsing them into one "Entra auth failed" bucket is what makes a report
/// unactionable: the reporter of #3196 sent an error whose text alone settles which arm it is, and
/// the answer changed what there was to do about it.</para>
/// </summary>
public enum EntraBrokerFailureKind
{
    /// <summary>Not a broker failure. Ordinary login/network/timeout failures land here.</summary>
    None,

    /// <summary>
    /// MSAL was handed no parent window handle (<c>window_handle_required</c>). The application's
    /// own wiring is at fault: <see cref="System.IntPtr.Zero"/> reached
    /// <c>WithParentActivityOrWindow</c>, and MSAL rejects the request before the broker is asked
    /// anything.
    /// </summary>
    WindowHandleMissing,

    /// <summary>
    /// The broker runtime itself could not be loaded (<c>wam_runtime_init_failed</c>). Nothing about
    /// the account or the tenant is implicated; the native <c>msalruntime</c> library did not start.
    /// </summary>
    BrokerUnavailable,

    /// <summary>
    /// The broker loaded, was given a window, ran, and refused the sign-in
    /// (<c>unknown_broker_error</c>, <c>WAM_provider_error_*</c>). The failure is on the Windows
    /// account side, past everything the application controls.
    /// </summary>
    BrokerRejected,
}

/// <summary>
/// Classifies a failed interactive-Entra connection by which stage of the WAM broker handshake it
/// died at, and states in plain language what that stage means.
///
/// <para><b>Why a classifier rather than a fix.</b> <c>Microsoft.Data.SqlClient</c> routes
/// <c>Authentication=ActiveDirectoryInteractive</c> through the Windows WAM broker and offers no way
/// out of it for an application that uses the driver's own Entra application id.
/// <c>ActiveDirectoryAuthenticationProviderOptions.UseWamBroker</c> looks like an opt-out and is not
/// one: the provider computes <c>useWamBroker = (applicationClientId == driverOwnId) ||
/// options.UseWamBroker</c>, so leaving the id unset pins the broker on and the flag is inert. MSAL
/// falls back to a browser only when the broker is <i>unavailable</i> — an invoked broker that
/// returns an error is propagated, not retried another way. So a broker refusal has no in-process
/// remedy, and naming the stage is the whole of what this code can honestly do.</para>
///
/// <para><b>Why the stage is worth naming.</b> The broker's own explanation of <i>why</i> it refused
/// is unreachable from here. MSAL only un-redacts it when its logger has PII enabled, and SqlClient
/// never configures an MSAL logger, so the reason arrives as the literal text <c>Context: (pii)</c>.
/// What is left is the stage, and the stage alone separates "the application forgot the window
/// handle" from "Windows refused" — which is the difference between a bug here and a condition on
/// the user's machine.</para>
///
/// <para><b>Verification status.</b> The classification above is read off the driver and MSAL
/// sources and off the error text a reporter sent. None of it is verified against a live Entra-MFA
/// tenant, because reproducing an interactive Entra failure needs a real tenant and a Windows host.
/// The string matching and the stage boundaries are pinned by tests; that the strings are the ones a
/// live tenant produces rests on the reported error and on the driver source, not on a run.</para>
/// </summary>
public static class EntraBrokerFailure
{
    /* Matched against the whole exception chain's message text rather than against a typed
       MsalException, deliberately. SqlClient wraps the MsalException in its own internal
       AuthenticationException and then in a SqlException, and reading the error code off the middle
       layer would mean referencing Microsoft.Identity.Client directly and pinning Lite to whichever
       MSAL version the driver happens to carry. The codes travel in the text: SqlClient renders the
       MSAL error code into its message as "Error code 0x<code>", which is how the #3196 report
       arrived legible. */

    private const string WindowHandleMarker = "window_handle_required";

    private const string RuntimeInitMarker = "wam_runtime_init_failed";

    /* Two markers for the refusal arm because MSAL splits it: a status it has a mapping for becomes
       WAM_provider_error_<code>, and one it does not becomes unknown_broker_error. Both mean the
       broker ran and said no. */
    private const string UnknownBrokerMarker = "unknown_broker_error";

    private const string ProviderErrorMarker = "wam_provider_error";

    /// <summary>
    /// Classifies <paramref name="ex"/> and every exception nested inside it.
    /// </summary>
    /// <param name="ex">The failure from the connection attempt. Null classifies as
    /// <see cref="EntraBrokerFailureKind.None"/>.</param>
    /// <returns>The stage the broker handshake died at, or
    /// <see cref="EntraBrokerFailureKind.None"/> when this is not a broker failure.</returns>
    public static EntraBrokerFailureKind Classify(Exception? ex)
    {
        /* Ordered by specificity, not by likelihood. The handle and runtime-init arms name a
           definite cause; the refusal arm is the residue, so it is tested last. Testing it first
           would swallow the two arms that say something more precise. */
        for (var current = ex; current is not null; current = current.InnerException)
        {
            var message = current.Message;
            if (string.IsNullOrEmpty(message))
                continue;

            if (Contains(message, WindowHandleMarker))
                return EntraBrokerFailureKind.WindowHandleMissing;

            if (Contains(message, RuntimeInitMarker))
                return EntraBrokerFailureKind.BrokerUnavailable;

            if (Contains(message, UnknownBrokerMarker) || Contains(message, ProviderErrorMarker))
                return EntraBrokerFailureKind.BrokerRejected;
        }

        return EntraBrokerFailureKind.None;
    }

    /// <summary>
    /// The user-facing explanation for a broker failure stage, or null for
    /// <see cref="EntraBrokerFailureKind.None"/>.
    ///
    /// <para>Each arm says which side the failure is on and stops there. None of them offers a retry
    /// that would work, because for a broker refusal there is not one: the driver has no browser
    /// fallback to reach for.</para>
    /// </summary>
    /// <param name="kind">The stage from <see cref="Classify"/>.</param>
    /// <returns>Text to show beneath the driver's own error, or null when there is nothing to add.</returns>
    public static string? Explain(EntraBrokerFailureKind kind) => kind switch
    {
        EntraBrokerFailureKind.WindowHandleMissing =>
            "Microsoft Entra MFA sign-in was rejected before Windows was asked: the sign-in prompt "
            + "was given no parent window to open in. This is a fault in Performance Monitor itself "
            + "rather than anything about the server or the account, and it should be reported.",

        EntraBrokerFailureKind.BrokerUnavailable =>
            "Microsoft Entra MFA sign-in could not start because the Windows account broker did not "
            + "load on this machine. The server name, database and account are not implicated.",

        EntraBrokerFailureKind.BrokerRejected =>
            "The Windows account broker (Web Account Manager) handled this Microsoft Entra MFA "
            + "sign-in and refused it. The server name, the database name and the account password "
            + "are not what failed. Windows does not report the reason to applications, and the SQL "
            + "client offers no way to sign in through a browser instead, so retrying as-is will "
            + "fail the same way. Signing the Windows work-or-school account out and back in, or "
            + "clearing the broker's cached state, is what usually clears it.",

        _ => null,
    };

    private static bool Contains(string haystack, string needle) =>
        haystack.Contains(needle, StringComparison.OrdinalIgnoreCase);
}
