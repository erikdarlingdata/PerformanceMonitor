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

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What the tenant issued for one device-code sign-in, reduced to the four values a user needs.
///
/// <para>A copy rather than the driver's own <c>DeviceCodeResult</c> on purpose. The rest of this
/// app has no reference to <c>Microsoft.Identity.Client</c> and should not grow one for a window,
/// and <c>DeviceCodeResult</c> also carries <c>DeviceCode</c> — the <b>secret</b> half of the
/// exchange, which whoever holds it can redeem the token with. That value is not on this type, so
/// no window, log line or crash dump downstream of here can carry it.</para>
/// </summary>
/// <param name="UserCode">The short code the user types into the browser.</param>
/// <param name="VerificationUrl">The page to type it into.</param>
/// <param name="Message">The tenant's own instruction text, already naming both of the above.</param>
/// <param name="ExpiresOn">
/// When the tenant stops accepting the code. Note this is NOT the deadline the user is racing: the
/// driver cancels the acquisition after three minutes regardless (see
/// <see cref="PerformanceMonitor.Common.AuthenticationTypes.EntraDeviceCode"/>), and tenants
/// typically issue fifteen.
/// </param>
public sealed record EntraDeviceCodeChallenge(
    string UserCode,
    string VerificationUrl,
    string Message,
    DateTimeOffset ExpiresOn);

/// <summary>
/// One in-flight device-code sign-in: the rendezvous between the driver's callback, the window that
/// shows the code, and the connection attempt that can be cancelled.
///
/// <para>One object rather than three because all three parties need the same identity, and there is
/// nothing to correlate on if they are separate — see <see cref="EntraDeviceCodeAuth.Begin"/>.</para>
/// </summary>
public sealed class EntraDeviceCodeAttempt : IDisposable
{
    private readonly CancellationTokenSource _cancellation = new();
    private int _finished;

    /// <summary>
    /// Pass this to <c>SqlConnection.OpenAsync</c>. Cancelling it returns the UI thread immediately
    /// rather than leaving the user watching a window they cannot dismiss.
    ///
    /// <para><b>What cancellation does and does not reach.</b> It completes the caller's
    /// <c>OpenAsync</c> as cancelled. It does <b>not</b> stop the driver's poll loop: MSAL's
    /// <c>WaitForTokenResponseAsync</c> honours a token, but SqlClient hands it a
    /// <c>CancellationTokenSource</c> of its own making rather than anything the caller supplied
    /// (<c>Extensions.Azure</c> 7.0.2, <c>:687-689</c>), so the abandoned acquisition keeps polling
    /// until the user completes it or its three minutes run out. There is no seam to close that gap
    /// from here. The consequence is worth stating because it is visible: a user who cancels and
    /// then finishes the sign-in in the browser anyway has minted a token nothing collects, and the
    /// next attempt in the same process may complete silently from MSAL's cache.</para>
    /// </summary>
    public CancellationToken Token => _cancellation.Token;

    /// <summary>
    /// The code and URL, once the tenant has issued them, or <c>null</c> before that — which is the
    /// normal state for the first second or so of an attempt, and the permanent state of an attempt
    /// that failed before reaching the tenant.
    /// </summary>
    public EntraDeviceCodeChallenge? Challenge { get; internal set; }

    /// <summary>
    /// Raised exactly once, when the connection attempt ends for any reason — success, failure or
    /// cancellation. The window showing the code closes on this; nothing else subscribes.
    /// </summary>
    public event Action? Finished;

    /// <summary>Cancels the connection attempt. Safe to call after it has already ended.</summary>
    public void Cancel()
    {
        try
        {
            _cancellation.Cancel();
        }
        catch (ObjectDisposedException)
        {
            /* The attempt already ended and disposed its source. Cancelling a finished attempt is
               what the window's Cancel button does when the connection completed a moment earlier,
               so it is an ordinary race and not a fault. */
        }
    }

    /// <summary>
    /// Ends the attempt: raises <see cref="Finished"/> once so the window closes, then releases the
    /// token source.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _finished, 1) == 0)
        {
            Finished?.Invoke();
        }

        _cancellation.Dispose();
    }
}

/// <summary>
/// Makes <c>Authentication=ActiveDirectoryDeviceCodeFlow</c> usable from a GUI by giving the driver
/// somewhere to put the code.
///
/// <para><b>Why this has to exist.</b> Every other authentication mode is a connection-string
/// setting and nothing more. This one has a human step in the middle: the driver calls back with a
/// code and a URL and then polls, so a mode with no callback installed is a mode that waits three
/// minutes for a code nobody was shown. The driver's default callback is
/// <c>Console.WriteLine(result.Message)</c> (<c>Extensions.Azure</c> 7.0.2,
/// <c>ActiveDirectoryAuthenticationProvider.cs:698-703</c>), which in a WPF process with no console
/// writes to nowhere.</para>
///
/// <para>Registration is process-wide, the same shape as
/// <see cref="EntraInteractiveAuth"/>: <see cref="SqlAuthenticationProvider.SetProvider"/> installs
/// against the authentication METHOD, so one call at startup covers the Add/Edit dialog's Test
/// Connection and the Manage Servers connectivity check without threading a presenter through call
/// sites that could each forget it.</para>
/// </summary>
public static class EntraDeviceCodeAuth
{
    /// <summary>The <c>AppLogger</c> source column for this type's lines.</summary>
    internal const string LogSource = "EntraDeviceCode";

    /// <summary>
    /// How long the driver waits for the user, plus slack — <c>CancelAfter(TimeSpan.FromMinutes(3))</c>
    /// in <c>AcquireTokenInteractiveDeviceFlowAsync</c> (<c>Microsoft.Data.SqlClient.Extensions.Azure</c>
    /// 7.0.2, <c>:688</c>). A copy of a value in someone else's binary, and used for one thing only:
    /// bounding how long an unowned prompt window can stay on screen. Nothing about the sign-in itself
    /// depends on this number being right, so a driver that changed it would cost a window that closes
    /// slightly early or late rather than a failed connection.
    /// </summary>
    internal static readonly TimeSpan UnownedPromptLifetime =
        TimeSpan.FromMinutes(3) + TimeSpan.FromSeconds(10);

    private static readonly object Gate = new();
    private static bool _registered;
    private static Action<EntraDeviceCodeAttempt>? _present;

    /* The attempt the driver's callback will publish into. A single slot, because the callback
       cannot be correlated to a connection: MSAL hands the callback a DeviceCodeResult and nothing
       else, and SqlClient's wrapper drops the SqlAuthenticationParameters (including its
       ConnectionId) before invoking it (Extensions.Azure 7.0.2, :689). So there is no key to route
       on even in principle, and a per-connection map would be a map with no lookup.

       One at a time is what the UI produces anyway: the Add/Edit dialog is modal and disables Test
       and Save while a test runs, and ServerManager's connectivity check refuses to open an
       interactive-mode connection while that dialog is open. A second concurrent Begin therefore
       does not happen today; if it ever does, the newer attempt owns the slot, the older one gets no
       window and expires on the driver's own three-minute deadline, and the displacement is logged
       rather than silent. */
    private static EntraDeviceCodeAttempt? s_current;

    /// <summary>
    /// Registers the device-code provider, routing the driver's callback to
    /// <paramref name="present"/>.
    /// </summary>
    /// <param name="present">
    /// Shows the code to the user. Invoked on whichever thread the driver's token acquisition runs
    /// on, which is never the UI thread, so an implementation that touches WPF must marshal. It must
    /// also return promptly: the driver awaits the callback before it starts polling for the token,
    /// so a blocking presenter delays the sign-in it is announcing.
    /// </param>
    /// <returns>True if this call registered the provider; false if already registered.</returns>
    public static bool Register(Action<EntraDeviceCodeAttempt> present)
    {
        ArgumentNullException.ThrowIfNull(present);

        lock (Gate)
        {
            if (_registered)
            {
                return false;
            }

            _present = present;

            var provider = new ActiveDirectoryAuthenticationProvider();
            provider.SetDeviceCodeFlowCallback(OnDeviceCodeIssued);

            SqlAuthenticationProvider.SetProvider(
                SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow, provider);

            _registered = true;
            return true;
        }
    }

    /// <summary>
    /// An attempt for a connection about to be opened with
    /// <see cref="SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow"/> — which is what
    /// <see cref="PerformanceMonitor.Common.AuthenticationTypes.EntraDeviceCode"/> maps to — or
    /// <c>null</c> for every other mode, which is a <c>using</c> that disposes nothing and a token
    /// that is never cancelled.
    ///
    /// <para>Keyed off the BUILDER rather than off the app's own mode string, for the reason
    /// <c>EntraCredentialSelectionLog.Begin</c> gives: this is the keyword the driver will actually
    /// act on, so the gate cannot disagree with the connection string, and it would follow a
    /// credential profile into this mode for free if one were ever offered there.</para>
    /// </summary>
    public static EntraDeviceCodeAttempt? Begin(SqlConnectionStringBuilder? builder)
    {
        if (builder?.Authentication != SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow)
        {
            return null;
        }

        var attempt = new EntraDeviceCodeAttempt();
        var displaced = Interlocked.Exchange(ref s_current, attempt);

        if (displaced is not null)
        {
            AppLogger.Warn(
                LogSource,
                "A second device-code sign-in started while one was still in flight. The earlier "
                    + "attempt will not be shown a code and will expire on the driver's deadline; "
                    + "the driver's callback carries nothing to tell the two apart.");
        }

        return attempt;
    }

    /// <summary>
    /// Forgets the registration flag and any in-flight attempt so a test can observe registration
    /// order. Registration itself is irreversible in production —
    /// <see cref="SqlAuthenticationProvider"/> offers no unregister — and tests open no connections,
    /// so leaving the provider installed with SqlClient is inert.
    /// </summary>
    internal static void ResetForTests()
    {
        lock (Gate)
        {
            _registered = false;
            _present = null;
            Interlocked.Exchange(ref s_current, null);
        }
    }

    /// <summary>
    /// The driver's callback. Publishes the challenge onto the current attempt and hands it to the
    /// presenter.
    ///
    /// <para><b>Returns a completed task rather than the presenter's own work.</b> The driver awaits
    /// this before it begins polling, so anything awaited here is time subtracted from the user's
    /// three minutes.</para>
    ///
    /// <para><b>A throwing presenter cancels the attempt instead of propagating.</b>
    /// <c>DeviceCodeRequest.ExecuteAsync</c> awaits this callback with no <c>catch</c> around it, so
    /// an exception here surfaces as an authentication failure whose message is about a window
    /// rather than about a sign-in. Worse, the alternative to cancelling is a three-minute wait for
    /// a code that was never displayed — so failing to show the code has to end the attempt, not
    /// merely be reported.</para>
    /// </summary>
    private static Task OnDeviceCodeIssued(Microsoft.Identity.Client.DeviceCodeResult result)
    {
        var attempt = Volatile.Read(ref s_current);
        var unowned = attempt is null;

        if (attempt is null)
        {
            /* A device-code connection opened somewhere that does not wrap it in Begin. The code is
               still shown, because a code nobody can read is a guaranteed failure and this is the
               fallback that removes that outcome from every present and future call site at once -
               Lite resolves a server connection string at a dozen places and only the three a user
               drives are wrapped.

               What is lost is only the EARLY RETURN, and only where nobody is waiting for one. This
               attempt's token is awaited by nothing, so its Cancel closes the window and leaves the
               background open to finish on the driver's deadline. That costs nothing a caller can
               observe: cancelling a wrapped attempt does not stop the driver's polling either (see
               EntraDeviceCodeAttempt.Token), it only hands a waiting UI thread back to the user, and
               there is no waiting UI thread here.

               Its lifetime is bounded so the window cannot outlive the sign-in it describes: nothing
               else will dispose an attempt nobody owns, and a prompt left on screen after the driver
               gave up is a code that no longer works. */
            attempt = new EntraDeviceCodeAttempt();

            var orphan = attempt;
            _ = Task.Delay(UnownedPromptLifetime).ContinueWith(
                _ => orphan.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        /* DeviceCode is deliberately not copied across - see EntraDeviceCodeChallenge. */
        attempt.Challenge = new EntraDeviceCodeChallenge(
            result.UserCode ?? string.Empty,
            result.VerificationUrl ?? string.Empty,
            result.Message ?? string.Empty,
            result.ExpiresOn);

        /* The user code is short, single-use, and useless without the device code the app never
           holds; the URL is public. Logged because "did the tenant issue a code at all" is the first
           question a failure report has to answer, and a screenshot cannot be searched. */
        AppLogger.Info(
            LogSource,
            $"Device code issued: enter {attempt.Challenge.UserCode} at "
                + $"{attempt.Challenge.VerificationUrl}. The driver stops waiting after three "
                + "minutes."
                + (unowned
                    ? " This connection was not opened through EntraDeviceCodeAuth.Begin, so closing "
                        + "the prompt hides the code without ending the attempt."
                    : string.Empty));

        try
        {
            _present?.Invoke(attempt);
        }
        catch (Exception ex)
        {
            AppLogger.Error(
                LogSource,
                "Could not show the device code, so the sign-in was cancelled rather than left to "
                    + "expire unseen.",
                ex);
            attempt.Cancel();
        }

        return Task.CompletedTask;
    }
}
