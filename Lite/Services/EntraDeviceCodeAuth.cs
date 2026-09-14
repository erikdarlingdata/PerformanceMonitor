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
/// Which of the two ways attribution can degrade happened on one attribution, or
/// <see cref="None"/> when neither did.
///
/// <para><b>A return value because the prompt cannot carry it.</b> Both degradations leave a prompt
/// that looks like a prompt — one merely unnamed, one merely slow to cancel — so the warning
/// <c>Claim</c> writes is the only thing an operator sees, and a warning is not something a test can
/// assert on: <see cref="AppLogger"/> is static and has no sink to capture. Reported instead of
/// merely logged so the DECISION is observable, which is what makes an inverted condition a red
/// test rather than a silence nobody notices.</para>
///
/// <para>Three values rather than four: this answers "did attribution degrade, and which way",
/// which is a different question from <c>Claim</c>'s <c>unowned</c> — that one answers whether the
/// prompt's Cancel completes a waiting <c>OpenAsync</c>. The two are orthogonal, and an unowned
/// prompt is usually <see cref="None"/>: a background read with no sign-in waiting is the ordinary
/// case, and so is a stray code arriving for a sign-in that already holds its own.</para>
/// </summary>
internal enum EntraDeviceCodeDegradation
{
    /// <summary>
    /// Attribution did not degrade. Either the challenge took the waiting sign-in's slot, or it had
    /// nothing to take: no sign-in was waiting, or the one waiting already held its own code.
    /// </summary>
    None,

    /// <summary>
    /// No acquisition identity reached the callback at all, so the challenge was shown unnamed.
    /// Every acquisition through Lite's own provider records one, so this means the driver refused
    /// that provider at startup or the execution context no longer reaches the callback.
    /// </summary>
    NoAcquisitionIdentity,

    /// <summary>
    /// A challenge naming one server arrived while a sign-in to a different one was still waiting
    /// for its own code. The challenge is shown as its own prompt, correctly named; what the waiting
    /// caller loses is its early return, because its prompt's Cancel is not the one on screen.
    /// </summary>
    SignInStillAwaitingItsOwnCode,
}

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
    /// The server this attempt is signing in to, for the prompt to name, or <c>null</c> when
    /// nothing names it.
    ///
    /// <para>It exists because two prompts can legitimately be on screen at once — a sign-in a user
    /// started and one from a background read — and two anonymous windows holding different codes
    /// is a trap.</para>
    ///
    /// <para><b>Every value this holds comes from a token acquisition, directly or by agreement
    /// with one.</b> On an attempt <see cref="EntraDeviceCodeAuth.Begin"/> created it is read off
    /// that caller's own builder, and a challenge reaches such an attempt only when the acquisition
    /// that produced the challenge names the same target — see <c>Claim</c>. On an attempt the
    /// driver's callback created for itself it is that acquisition's own server and database. So
    /// this never names a server other than the one the code beside it is signing in to, which is
    /// the one failure a labelled prompt has that an anonymous one does not.</para>
    /// </summary>
    public string? Target { get; internal set; }

    /// <summary>
    /// Publishes the tenant's challenge onto this attempt, once. Returns false if a challenge is
    /// already published, in which case the caller must NOT assume the new one belongs here.
    ///
    /// <para><b>The second of the two tests a challenge passes to take this attempt's slot, and
    /// the weaker one.</b> It answers "can this be the code of the attempt holding the slot": the
    /// driver invokes its callback exactly once per acquisition, so a challenge arriving while one
    /// is already published cannot belong to the attempt that published it. What it cannot answer
    /// is whether an arriving challenge came from THIS attempt's connection, because an empty slot
    /// looks identical whether the code reaching it is the owner's own or an unrelated
    /// connection's. That question is settled first, by <c>Claim</c>, off the identity of the
    /// acquisition that produced the challenge.</para>
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
/// The device-code provider Lite installs with the driver: the driver's own
/// <see cref="ActiveDirectoryAuthenticationProvider"/>, with the identity of each token acquisition
/// recorded around it so the callback that acquisition triggers knows which connection it is for.
///
/// <para><b>This exists because the callback cannot be told, but the frame above it can.</b>
/// <c>Microsoft.Identity.Client.DeviceCodeResult</c> carries a user code, a device code, a URL, an
/// expiry, a client id and a scope list and nothing that names a SQL connection, and SqlClient
/// invokes the callback with that result alone (<c>Extensions.Azure</c> 7.0.2 and 7.0.3,
/// <c>AcquireTokenInteractiveDeviceFlowAsync</c>). A <see cref="SqlAuthenticationProvider"/>, by
/// contrast, is handed <see cref="SqlAuthenticationParameters"/> on every acquisition, which names
/// the server, the database and the connection id. So the provenance the callback lacks is one
/// frame up, and this class is that frame.</para>
///
/// <para><b>Carried on an async-local, which is what makes it reach the callback.</b> MSAL awaits
/// the device-code callback directly inside the request the provider started — one <c>await</c> in
/// <c>DeviceCodeRequest.ExecuteAsync</c> (<c>Microsoft.Identity.Client</c> 4.84.2) — and neither
/// that assembly nor <c>Extensions.Azure</c> suppresses execution-context flow, so a value written
/// here is in scope when the callback runs. <c>ConfigureAwait(false)</c> along that chain does not
/// change it: it governs the synchronization context, not the execution context an async-local
/// lives in.</para>
///
/// <para><b>A wrapper rather than a subclass because the driver's provider is sealed</b>, and a
/// wrapper anyway: this delegates every member, adding nothing to the acquisition but a record of
/// whose it is. Installing a provider the driver refuses would leave the mode silently dead, so
/// <see cref="IsSupported"/> answers from the inner provider rather than from a literal —
/// <c>SqlAuthenticationProviderManager.SetProvider</c> asks it and rejects a provider that says no
/// (<c>Microsoft.Data.SqlClient</c> 7.0.2 and 7.0.3).</para>
/// </summary>
internal sealed class EntraDeviceCodeProvider : SqlAuthenticationProvider
{
    private readonly ActiveDirectoryAuthenticationProvider _inner;

    /// <summary>Wraps the driver's provider, which must already carry the device-code callback.</summary>
    internal EntraDeviceCodeProvider(ActiveDirectoryAuthenticationProvider inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    /// <summary>The inner provider's answer, never a literal — see the remarks on this type.</summary>
    public override bool IsSupported(SqlAuthenticationMethod authenticationMethod) =>
        _inner.IsSupported(authenticationMethod);

    /// <inheritdoc/>
    public override void BeforeLoad(SqlAuthenticationMethod authenticationMethod) =>
        _inner.BeforeLoad(authenticationMethod);

    /// <inheritdoc/>
    public override void BeforeUnload(SqlAuthenticationMethod authenticationMethod) =>
        _inner.BeforeUnload(authenticationMethod);

    /// <summary>
    /// Records who this acquisition is for, then runs it.
    ///
    /// <para>Awaited inside the scope rather than returning the inner task, because the scope has
    /// to still be open when the callback fires — and the callback fires partway through the task,
    /// not before it is returned.</para>
    /// </summary>
    public override async Task<SqlAuthenticationToken> AcquireTokenAsync(
        SqlAuthenticationParameters parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        using var acquisition = EntraDeviceCodeAuth.EnterAcquisition(
            EntraDeviceCodeAuth.DescribeTarget(parameters.ServerName, parameters.DatabaseName));

        return await _inner.AcquireTokenAsync(parameters).ConfigureAwait(false);
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

    /* The attempt a waiting caller's challenge publishes into, and whose token that caller's
       prompt cancels. ONE slot, because what it holds is a rendezvous with a caller that is
       awaiting an OpenAsync, and widening it to a map keyed on the acquisition identity
       EntraDeviceCodeProvider records would still collide on the one pair a single slot already
       refuses: two sign-ins to the same server produce the same identity.

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

            var driverProvider = new ActiveDirectoryAuthenticationProvider();
            driverProvider.SetDeviceCodeFlowCallback(OnDeviceCodeIssued);

            /* Wrapped, so every acquisition the driver runs for this method records whose it is
               before the callback it triggers asks - see EntraDeviceCodeProvider. */
            var provider = new EntraDeviceCodeProvider(driverProvider);

            /* Checked, not called and forgotten. SetProvider REFUSES a provider whose IsSupported
               says no for the method (SqlAuthenticationProviderManager.SetProvider,
               Microsoft.Data.SqlClient 7.0.2 and 7.0.3), and a refusal leaves the driver's own
               callback installed - which writes the code to a console a WPF process does not have.
               That is the whole feature dead with nothing on screen and nothing in the log. */
            if (!SqlAuthenticationProvider.SetProvider(
                    SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow, provider))
            {
                AppLogger.Error(
                    LogSource,
                    "The driver refused Lite's device-code provider, so no device-code sign-in can "
                        + "display a code. Servers using Azure device-code authentication will fail "
                        + "to connect after the driver's three-minute wait.");
            }

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
    /// <para>The constraint outlives the provenance <c>EntraDeviceCodeProvider</c> recovers, so it
    /// is enforced rather than documented. That provenance is the acquisition's own server and
    /// database, which separates two sign-ins to DIFFERENT servers — but two to the same server
    /// describe identically, and there is one slot regardless, so a second claim has nowhere to go
    /// that is certainly its own. What it would otherwise do is the worst outcome available:
    /// publish the EARLIER attempt's code onto the LATER attempt's window, so the user reads a code
    /// labelled as one server's and completes a sign-in for another's, while the connection they
    /// were watching fails three minutes later for no visible reason.</para>
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
                    + "refused. There is one slot for a sign-in a caller is waiting on, and two "
                    + "sign-ins to the same server describe identically, so the first one's code "
                    + "could be shown on the second one's window.");

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
            + "time because two sign-ins to the same server cannot be told apart.";

    /// <summary>
    /// Whether an attempt currently owns the slot the driver's callback publishes into.
    ///
    /// <para>Internal, and it exists to be asserted on. "A disposed attempt no longer owns the slot"
    /// has no other observable consequence until the next device-code connection arrives, which is
    /// exactly the shape of defect that ships.</para>
    /// </summary>
    internal static bool SignInInFlight => Volatile.Read(ref s_current) is not null;

    /* The identity of the token acquisition running on this async flow, or null outside one. Set
       by EntraDeviceCodeProvider, read by the driver's callback; async-local because that is the
       only thing the two share - the callback is handed a DeviceCodeResult and nothing else, and
       it runs inside the acquisition the provider started. */
    private static readonly AsyncLocal<string?> s_acquisitionTarget = new();

    /// <summary>
    /// What the acquisition on this async flow is signing in to, or <c>null</c> outside one — which
    /// is every caller that is not the driver asking for a token, and also the state a broken
    /// execution-context flow would leave. Both are handled the same way, and it is the safe way:
    /// an unidentified challenge is presented unnamed rather than borrowing a name. See <c>Claim</c>.
    /// </summary>
    internal static string? CurrentAcquisitionTarget => s_acquisitionTarget.Value;

    /// <summary>
    /// Marks this async flow as running a token acquisition for <paramref name="target"/> until the
    /// returned scope is disposed.
    /// </summary>
    internal static AcquisitionScope EnterAcquisition(string? target) => new(target);

    /// <summary>
    /// The lifetime of one acquisition's identity. Restores the previous value rather than clearing
    /// it, so a nested acquisition cannot end by declaring its parent's flow to be outside one.
    /// </summary>
    internal readonly struct AcquisitionScope : IDisposable
    {
        private readonly string? _previous;

        internal AcquisitionScope(string? target)
        {
            _previous = s_acquisitionTarget.Value;
            s_acquisitionTarget.Value = target;
        }

        /// <inheritdoc/>
        public void Dispose() => s_acquisitionTarget.Value = _previous;
    }

    /// <summary>
    /// Decides which attempt a challenge belongs to, and returns the attempt whose window will show
    /// it. <paramref name="unowned"/> is true when the challenge did not take a waiting caller's
    /// slot, in which case the returned attempt is a fresh one with a bounded lifetime and nothing
    /// awaiting its token.
    ///
    /// <para><b>Two tests, in this order, and the first is the one that matters.</b> A challenge may
    /// take the waiting attempt's slot only when the acquisition that produced it names that
    /// attempt's own target, and only then when the slot is still empty. The second test alone —
    /// "is the slot empty" — is not attribution: it is true of an attempt whose own code has just
    /// arrived AND of an attempt still waiting while some other connection's code arrives first,
    /// and those two orderings are the difference between a correct prompt and a prompt that names
    /// a user's server beside a different connection's code.</para>
    ///
    /// <para><b>Every failure of the first test costs a prompt its early return, never its
    /// label.</b> An unvouched challenge is shown on its own attempt, named from its own
    /// acquisition; what is lost is that the prompt's Cancel no longer completes a waiting
    /// <c>OpenAsync</c>, so a caller who gives up waits out the driver's three minutes instead of
    /// getting its thread back. That is the direction this has to fail in: a vague or slow prompt is
    /// recoverable and a confidently mislabelled one is not.</para>
    ///
    /// <para>Both ways attribution can degrade are CLASSIFIED here, off the same read the decision
    /// used, and reported through <paramref name="degradation"/>: a challenge with no acquisition
    /// identity at all, and one whose acquisition names a different server than the sign-in
    /// currently waiting. The warning each one writes is selected by that classification rather than
    /// by a second reading of the state, so the condition a log line rests on is the condition the
    /// returned value rests on and there is no second copy to drift.</para>
    ///
    /// <para><b>What the identity does NOT separate, stated because the guarantee is narrower than
    /// it reads.</b> Two acquisitions for the SAME server describe identically, so an unwrapped
    /// read of the server a user is signing in to can still take that sign-in's slot. Both prompts
    /// then name that server and both codes are that server's, so nobody is pointed at another
    /// server — what can happen is that the Cancel belonging to the user's own attempt sits on the
    /// other prompt. Separating those two would need the connection id, which the driver does give
    /// a provider but which <see cref="Begin"/> cannot know: it runs before the connection
    /// exists.</para>
    /// </summary>
    /// <param name="challenge">The code and URL the tenant issued.</param>
    /// <param name="acquiredTarget">
    /// What the acquisition that produced <paramref name="challenge"/> is signing in to, or
    /// <c>null</c> when no identity reached the callback.
    /// </param>
    /// <param name="unowned">
    /// True when the challenge did not take a waiting caller's slot, so cancelling its prompt
    /// completes no <c>OpenAsync</c>.
    /// </param>
    /// <param name="degradation">
    /// Which way attribution degraded, or <see cref="EntraDeviceCodeDegradation.None"/>. The same
    /// classification that selects the warning below, so the two cannot disagree.
    /// </param>
    internal static EntraDeviceCodeAttempt Claim(
        EntraDeviceCodeChallenge challenge,
        string? acquiredTarget,
        out bool unowned,
        out EntraDeviceCodeDegradation degradation)
    {
        ArgumentNullException.ThrowIfNull(challenge);

        var owner = Volatile.Read(ref s_current);

        var vouched =
            owner is not null
            && acquiredTarget is not null
            && string.Equals(owner.Target, acquiredTarget, StringComparison.OrdinalIgnoreCase);

        if (vouched && owner!.TryPublish(challenge))
        {
            unowned = false;
            degradation = EntraDeviceCodeDegradation.None;
            return owner;
        }

        unowned = true;

        /* The two ways attribution degrades, classified here rather than by the caller because they
           are facts about the state THIS decision was made on. A caller re-reading the slot
           afterwards would be describing a later instant, and a diagnostic that names the wrong
           instant is worse than none. Neither degradation is visible in the prompt: a prompt that is
           merely unnamed, or merely slow to cancel, looks like a prompt - so the classification is
           RETURNED as well as logged, because a log line is the one diagnostic nothing here can
           assert on. */
        degradation =
            acquiredTarget is null
                ? EntraDeviceCodeDegradation.NoAcquisitionIdentity
                : owner is { Challenge: null }
                    ? EntraDeviceCodeDegradation.SignInStillAwaitingItsOwnCode
                    : EntraDeviceCodeDegradation.None;

        /* The operator's half, selected BY the classification above rather than by asking the state
           again. The prose is deliberately not asserted on anywhere - a message pinned word for word
           rots into a test that fails on an improvement - but the condition each line rests on is
           now the condition a caller can read, so an inverted one is a red test and not a silently
           missing warning.

           The second line suppresses a null on the owner it names. SignInStillAwaitingItsOwnCode is
           reachable only through a pattern that matched a slot holder, which the compiler cannot see
           through an enum. */
        if (degradation is EntraDeviceCodeDegradation.NoAcquisitionIdentity)
        {
            AppLogger.Warn(
                LogSource,
                "A device code arrived with no acquisition identity, so it was shown unnamed. Every "
                    + "acquisition that runs through Lite's own provider records one, so this means "
                    + "either the driver refused that provider at startup or the execution context "
                    + "no longer reaches the callback.");
        }
        else if (degradation is EntraDeviceCodeDegradation.SignInStillAwaitingItsOwnCode)
        {
            AppLogger.Warn(
                LogSource,
                $"A device code for {acquiredTarget} arrived while a sign-in to {owner!.Target} was "
                    + "still waiting for its own code, so it was shown as its own prompt. A code "
                    + "takes a waiting sign-in's slot only when the acquisition that produced it "
                    + "names that sign-in's own server.");
        }

        /* Named from the acquisition rather than left anonymous: this is the only name anything has
           for this code, and it is the right one. */
        var orphan = new EntraDeviceCodeAttempt { Target = acquiredTarget };
        orphan.TryPublish(challenge);

        /* Bounded, so the window cannot outlive the sign-in it describes: nothing else will dispose
           an attempt nobody owns, and a prompt left on screen after the driver gave up is a code
           that no longer works. */
        _ = Task.Delay(UnownedPromptLifetime).ContinueWith(
            _ => orphan.Dispose(),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return orphan;
    }

    /// <summary>Gives up the slot, but only if <paramref name="attempt"/> still holds it.</summary>
    internal static void Release(EntraDeviceCodeAttempt attempt) =>
        Interlocked.CompareExchange(ref s_current, null, attempt);

    /// <summary>
    /// How a prompt names the server it is signing in to: the data source, plus the database when
    /// one is named. Never credentials — the builder holds them for other modes and this reads two
    /// non-secret keywords by name rather than rendering the string.
    /// </summary>
    internal static string? DescribeTarget(SqlConnectionStringBuilder builder) =>
        DescribeTarget(builder.DataSource, builder.InitialCatalog);

    /// <summary>
    /// The same name, built from the two values the driver hands an authentication provider rather
    /// than from a builder.
    ///
    /// <para><b>One implementation, because the two callers' results are COMPARED.</b>
    /// <see cref="Begin"/> describes the builder a caller is about to open,
    /// <c>EntraDeviceCodeProvider</c> describes the acquisition the driver is running, and a
    /// challenge takes a waiting attempt's slot only when those two strings agree. Two spellings of
    /// the same server would make that comparison fail on correct code.</para>
    ///
    /// <para>The comparison holds because the driver passes both values through: SqlClient builds
    /// its <see cref="SqlAuthenticationParameters"/> from <c>ConnectionOptions.DataSource</c> and
    /// <c>ConnectionOptions.InitialCatalog</c>, which are the connection string's own
    /// <c>Data Source</c> and <c>Initial Catalog</c> with nothing but a length check applied
    /// (<c>Microsoft.Data.SqlClient</c> 7.0.2 and 7.0.3). A driver that started normalising them, or a
    /// failover that re-resolved the data source mid-login, would make the two disagree — which
    /// costs a prompt its early return and not its label, as <c>Claim</c> describes.</para>
    /// </summary>
    internal static string? DescribeTarget(string? server, string? database)
    {
        if (string.IsNullOrWhiteSpace(server))
        {
            return null;
        }

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
            s_acquisitionTarget.Value = null;
        }
    }

    /// <summary>
    /// The driver's callback. Attributes the challenge, then hands the attempt that will show it to
    /// the presenter.
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

        /* Whose acquisition this is, recorded by EntraDeviceCodeProvider one frame above this
           callback. It is the only provenance this process has, and Claim is where it decides
           something: "the slot is empty" is not attribution, because it is equally true of an
           attempt whose own code has just arrived and of an attempt still waiting while some other
           connection's code arrives first.

           Unowned is the normal outcome for the connections Lite opens without wrapping them in
           Begin - a plan fetch, an MCP read, a Query Store backfill, the excluded-databases picker
           - and their codes are still shown, named from their own acquisitions. A code nobody can
           read is a guaranteed failure, and showing it removes that outcome from every present and
           future call site at once rather than one wrapped site at a time. What an unowned prompt
           does not have is the early return: cancelling it closes the window without completing a
           waiting OpenAsync, and there is no waiting OpenAsync behind an unwrapped read. */
        var acquired = CurrentAcquisitionTarget;
        var attempt = Claim(challenge, acquired, out var unowned, out _);

        /* The user code is short, single-use, and useless without the device code the app never
           holds; the URL is public. Logged because "did the tenant issue a code at all" is the first
           question a failure report has to answer, and a screenshot cannot be searched. */
        AppLogger.Info(
            LogSource,
            $"Device code issued for {attempt.Target ?? "a background connection"}: enter "
                + $"{challenge.UserCode} at {challenge.VerificationUrl}. The driver stops waiting "
                + "after three minutes."
                + (unowned
                    ? " This code did not take a waiting sign-in's slot, so closing the prompt hides "
                        + "the code without ending the attempt."
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
