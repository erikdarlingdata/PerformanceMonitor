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
/// What the tenant issued for one device-code sign-in, reduced to the two values a user acts on.
///
/// <para>A copy rather than the driver's own <c>DeviceCodeResult</c> on purpose, and a deliberately
/// short one. The rest of this app has no reference to <c>Microsoft.Identity.Client</c> and should
/// not grow one for a window; <c>DeviceCodeResult</c> also carries <c>DeviceCode</c> — the
/// <b>secret</b> half of the exchange, which whoever holds it can redeem the token with — so that
/// value is not on this type and no window, log line or crash dump downstream of here can carry it.
/// The remaining members it does not copy are omitted because nothing uses them, not because they
/// are sensitive: <c>Message</c> is the tenant's own instruction text, which the prompt window
/// states in its own words, and <c>ExpiresOn</c> is when the TENANT stops accepting the code, which
/// is not the deadline the user is racing — the driver gives up after three minutes regardless (see
/// <see cref="PerformanceMonitor.Common.AuthenticationTypes.EntraDeviceCode"/>) and tenants
/// typically issue fifteen, so showing it would be showing the wrong number.</para>
/// </summary>
/// <param name="UserCode">The short code the user types into the browser.</param>
/// <param name="VerificationUrl">The page to type it into.</param>
public sealed record EntraDeviceCodeChallenge(string UserCode, string VerificationUrl);

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
    private EntraDeviceCodeChallenge? _challenge;
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
    public EntraDeviceCodeChallenge? Challenge => Volatile.Read(ref _challenge);

    /// <summary>
    /// The server this attempt is signing in to, for the prompt to name, or <c>null</c> when the
    /// connection was not opened through <see cref="EntraDeviceCodeAuth.Begin"/> and there is
    /// therefore nothing to name it with.
    ///
    /// <para>It exists because two prompts can legitimately be on screen at once — a sign-in a user
    /// started and an unattributable one from a background read — and two anonymous windows holding
    /// different codes is a trap. See <see cref="TryPublish"/>.</para>
    /// </summary>
    public string? Target { get; internal set; }

    /// <summary>
    /// Publishes the tenant's challenge onto this attempt, once. Returns false if a challenge is
    /// already published, in which case the caller must NOT assume the new one belongs here.
    ///
    /// <para><b>This is the only discriminator available, and it is exact.</b> The driver's callback
    /// carries no connection id, so "which attempt is this code for" cannot be answered directly —
    /// but the driver invokes the callback exactly once per acquisition, so a second challenge
    /// arriving while one is already published cannot belong to the attempt that published it. That
    /// turns an unanswerable question into an answerable one: not "whose code is this" but "can this
    /// be the code of the attempt holding the slot", which a published challenge settles as no.</para>
    ///
    /// <para>Atomic, because two connections' callbacks can arrive on different threads at the same
    /// instant and a read-then-write pair would let both conclude they were first.</para>
    /// </summary>
    internal bool TryPublish(EntraDeviceCodeChallenge challenge) =>
        Interlocked.CompareExchange(ref _challenge, challenge, null) is null;

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
    /// Ends the attempt: gives up the slot the driver's callback publishes into, raises
    /// <see cref="Finished"/> once so the window closes, then releases the token source.
    ///
    /// <para><b>The slot has to be released, not merely finished.</b> A disposed attempt left in that
    /// slot is worse than an empty one: the next device-code connection would publish its challenge
    /// onto an object whose <see cref="Finished"/> has already fired and whose token source is gone,
    /// so the window it opens never closes and its Cancel button does nothing. Released
    /// conditionally, so an attempt that was already displaced by a newer one does not take the
    /// newer one's slot with it on the way out.</para>
    /// </summary>
    public void Dispose()
    {
        EntraDeviceCodeAuth.Release(this);

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

       The slot is CLAIMED, not overwritten, and that is a correctness requirement rather than
       tidiness. Three sites call Begin - the Add/Edit dialog's connection test, ServerManager's
       connectivity check, and RemoteCollectorService's collector connections - and the third runs on
       a timer, independent of dialog state. RemoteCollectorService's own interactive-auth semaphore
       serializes collectors against each other and nothing else, so a collection cycle and a user
       pressing Test genuinely can overlap.

       Overwriting the slot in that overlap is the worst available outcome: the EARLIER attempt's
       code is published onto the LATER attempt's window, so the user reads a code for one server and
       types it into a browser, completing a sign-in for a different one, while the connection they
       were watching fails three minutes later for no visible reason. Refusing the second claim
       replaces that with an immediate, named failure. */
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
    ///
    /// <para><b>ONE attempt may be in flight at a time, and this method ENFORCES that rather than
    /// documenting it.</b> A second overlapping call throws
    /// <see cref="InvalidOperationException"/> with <see cref="ConcurrentSignInMessage"/>.</para>
    ///
    /// <para>The constraint is unfixable, so it has to be enforced. MSAL hands the callback a
    /// <c>DeviceCodeResult</c> and SqlClient's wrapper drops the
    /// <c>SqlAuthenticationParameters</c>, including its <c>ConnectionId</c>, before invoking it — so
    /// nothing reaches this type that could tell two attempts apart, and there is no key a
    /// per-connection map could use. What a second claim would otherwise do is the worst outcome
    /// available: publish the EARLIER attempt's code onto the LATER attempt's window, so the user
    /// reads a code labelled as one server's and completes a sign-in for another's, while the
    /// connection they were watching fails three minutes later for no visible reason.</para>
    ///
    /// <para><b>And the overlap is reachable, which is why enforcing it is not belt-and-braces.</b>
    /// Three sites call this — the Add/Edit dialog's connection test,
    /// <c>ServerManager.CheckConnectionAsync</c>, and
    /// <c>RemoteCollectorService.CreateConnectionAsync</c> — and the third runs on a collection
    /// timer, independent of dialog state. The collector's own interactive-auth semaphore serializes
    /// collectors against each other and holds nothing the UI paths take, so a collection cycle
    /// prompting for a code while the user presses Test is an ordinary Tuesday, not a thought
    /// experiment. Dialog modality does not cover it; an earlier draft of this comment claimed it
    /// did.</para>
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// A device-code sign-in is already in flight. Callers do not catch this specially: the dialog
    /// shows the message, and a collector logs it and tries again next cycle.
    /// </exception>
    public static EntraDeviceCodeAttempt? Begin(SqlConnectionStringBuilder? builder)
    {
        if (builder?.Authentication != SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow)
        {
            return null;
        }

        /* The target, so a prompt can name the server it belongs to. Read off the builder rather
           than passed in, for the same reason the mode gate is: the builder is what the connection
           will actually use, so the label cannot disagree with the connection. */
        var attempt = new EntraDeviceCodeAttempt { Target = DescribeTarget(builder) };

        /* CompareExchange, so the claim is atomic and the loser knows it lost. Exchange would hand
           the slot over and leave the previous holder's callback publishing onto this attempt. */
        var holder = Interlocked.CompareExchange(ref s_current, attempt, null);

        if (holder is not null)
        {
            attempt.Dispose();

            AppLogger.Warn(
                LogSource,
                "A device-code sign-in was requested while one was already in flight, and was "
                    + "refused. Two at once cannot be told apart: the driver's callback carries no "
                    + "connection id, so the first sign-in's code would be shown on the second "
                    + "one's window.");

            throw new InvalidOperationException(ConcurrentSignInMessage);
        }

        return attempt;
    }

    /// <summary>
    /// What the user is told when a second device-code sign-in is requested while one is in flight.
    /// Names the action, because the remedy is entirely in their hands: finish the sign-in on screen,
    /// or close its window.
    /// </summary>
    public const string ConcurrentSignInMessage =
        "A device code sign-in is already in progress. Finish it in your browser, or close the code "
            + "window to cancel it, and then try again. Only one device code sign-in can run at a "
            + "time because the SQL driver gives no way to tell two of them apart.";

    /// <summary>
    /// Whether an attempt currently owns the slot the driver's callback publishes into.
    ///
    /// <para>Internal, and it exists to be asserted on. "A disposed attempt no longer owns the slot"
    /// has no other observable consequence until the next device-code connection arrives, which is
    /// exactly the shape of defect that ships.</para>
    /// </summary>
    internal static bool SignInInFlight => Volatile.Read(ref s_current) is not null;

    /// <summary>Gives up the slot, but only if <paramref name="attempt"/> still holds it.</summary>
    internal static void Release(EntraDeviceCodeAttempt attempt) =>
        Interlocked.CompareExchange(ref s_current, null, attempt);

    /// <summary>
    /// How a prompt names the server it is signing in to: the data source, plus the database when
    /// one is named. Never credentials — the builder holds them for other modes and this reads two
    /// non-secret keywords by name rather than rendering the string.
    /// </summary>
    internal static string? DescribeTarget(SqlConnectionStringBuilder builder)
    {
        var server = builder.DataSource;
        if (string.IsNullOrWhiteSpace(server))
        {
            return null;
        }

        var database = builder.InitialCatalog;
        return string.IsNullOrWhiteSpace(database) ? server : $"{server} ({database})";
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
        /* DeviceCode is deliberately not copied across - see EntraDeviceCodeChallenge. */
        var challenge = new EntraDeviceCodeChallenge(
            result.UserCode ?? string.Empty,
            result.VerificationUrl ?? string.Empty);

        var owner = Volatile.Read(ref s_current);

        /* Occupied is NOT the same as "this challenge belongs to the occupant", and conflating the
           two was a real misattribution bug. Four sites in Lite open a server connection without
           going through Begin (a plan fetch, an MCP read, a Query Store backfill, the
           excluded-databases picker), so a device-code callback can arrive while an unrelated,
           Begin-owned sign-in holds the slot. Reusing the occupant then overwrote ITS code with the
           unrelated server's and re-invoked the presenter - a second window on the same attempt,
           showing a code for a server the user was not looking at, with no exception anywhere
           because the unwrapped caller never called Begin.

           TryPublish is the discriminator: the driver invokes its callback once per acquisition, so
           a challenge arriving at an attempt that already has one cannot be that attempt's. It takes
           a fresh unowned attempt instead, with its own window and its own bounded lifetime. */
        var attempt = owner;
        var unowned = attempt is null || !attempt.TryPublish(challenge);

        if (unowned)
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
            attempt.TryPublish(challenge);

            var orphan = attempt;
            _ = Task.Delay(UnownedPromptLifetime).ContinueWith(
                _ => orphan.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        /* The user code is short, single-use, and useless without the device code the app never
           holds; the URL is public. Logged because "did the tenant issue a code at all" is the first
           question a failure report has to answer, and a screenshot cannot be searched. */
        AppLogger.Info(
            LogSource,
            $"Device code issued for {attempt.Target ?? "a background connection"}: enter "
                + $"{challenge.UserCode} at {challenge.VerificationUrl}. The driver stops waiting "
                + "after three minutes."
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
