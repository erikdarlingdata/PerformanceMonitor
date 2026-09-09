/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics.Tracing;
using System.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Captures which credential <c>DefaultAzureCredential</c> actually selected, by listening to the
/// one <c>Azure-Identity</c> event that says so.
///
/// <para><b>Observability only.</b> Nothing here participates in acquiring a token, building a
/// connection string, or ordering the credential chain. Remove the type and the app connects
/// identically; the only difference is that nobody can tell which identity it connected as.</para>
///
/// <para><b>The event.</b> <c>Azure.Identity</c> 1.18.0,
/// <c>Credentials/DefaultAzureCredential.cs:178</c>, on the success path immediately after
/// <c>GetTokenFromSourcesAsync</c> picks a source:
/// <c>AzureIdentityEventSource.Singleton.DefaultAzureCredentialCredentialSelected(credential.GetType().FullName)</c>.
/// Its declaration is <c>AzureIdentityEventSource.cs:307</c> — event id <b>13</b>
/// (<c>AzureIdentityEventSource.cs:34</c>), <c>Level = EventLevel.Informational</c>, one
/// <c>string credentialType</c> parameter — on the source named <b><c>Azure-Identity</c></b>
/// (<c>AzureIdentityEventSource.cs:18</c>).</para>
///
/// <para><b>An ALLOWLIST on the event id, because the level cannot narrow this and keywords do not
/// exist here.</b> Event 13 is itself <c>Informational</c>, so <c>Informational</c> is the LOWEST
/// level that delivers it — and <see cref="EventListener.EnableEvents(EventSource, EventLevel)"/>
/// admits every event at or above the requested severity — which at 1.18.0 is <b>28 of this
/// source's 29 events</b>: 20 at <c>Informational</c> (19 besides this one), plus 4
/// <c>Warning</c>, 2 <c>Error</c>, 1 <c>Critical</c> and 1 <c>LogAlways</c>. Only the single
/// <c>Verbose</c> event is excluded. Counted twice, off two things that fail differently: the
/// <c>[Event]</c> attributes in the source at tag <c>Azure.Identity_1.18.0</c>, and reflection over
/// the shipped <c>Azure.Identity.dll</c>. Several of the 28 carry exactly what an application log must never hold:
/// <c>TenantIdDiscoveredAndUsed</c> / <c>TenantIdDiscoveredAndNotUsed</c> carry tenant ids,
/// <c>AuthenticatedAccountDetails</c> carries account details, <c>GetTokenFailed</c> carries a
/// formatted <c>Exception</c>, the six <c>MsalLog*</c> events carry MSAL's own log lines, and
/// <c>GetToken</c> / <c>GetTokenSucceeded</c> carry scopes and parent request ids.</para>
///
/// <para><b>And keyword filtering cannot help</b>: not one of the 29 events in that file declares
/// <c>Keywords</c>, so <c>EnableEvents</c> has no dimension to exclude the noisy siblings on. That
/// makes <see cref="OnEventWritten"/>'s filter the only barrier between this app's log and a tenant
/// id — which is why it is an allowlist on <see cref="CredentialSelectedEventId"/> rather than a
/// denylist of the events known to be sensitive today. A denylist is the form that fails when a
/// future <c>Azure.Identity</c> adds an event; an allowlist is the form that survives it, because
/// the new event is not event 13.</para>
///
/// <para><b>What leaves this type is <see cref="EventWrittenEventArgs.Payload"/>[0] and nothing
/// else</b>, and only when it has the shape of a CLR type name (see
/// <see cref="EntraCredentialSelectionLog.IsCredentialTypeName"/>). Never
/// <c>eventData.ToString()</c>, never the payload collection, never
/// <see cref="EventWrittenEventArgs.Message"/> formatted with its arguments.</para>
/// </summary>
internal sealed class EntraCredentialSelectionListener : EventListener
{
    /// <summary>
    /// <c>Azure.Identity</c> 1.18.0, <c>AzureIdentityEventSource.cs:18</c>. Matched
    /// <see cref="StringComparison.Ordinal"/>: this is a wire identifier, not display text.
    /// </summary>
    internal const string AzureIdentitySourceName = "Azure-Identity";

    /// <summary>
    /// <c>Azure.Identity</c> 1.18.0, <c>AzureIdentityEventSource.cs:34</c>
    /// (<c>DefaultAzureCredentialCredentialSelectedEvent</c>). The whole allowlist.
    /// </summary>
    internal const int CredentialSelectedEventId = 13;

    /* Both values the OnEventSourceCreated callback needs are const, and that is the mitigation for
       the classic EventListener bug rather than an accident of style. EventListener's BASE
       CONSTRUCTOR calls OnEventSourceCreated for every EventSource that already exists in the
       process - and a C# derived-class constructor BODY runs after the base constructor, so a source
       name or event id assigned there is still null/0 when that callback fires. The symptom is not an
       exception: the listener simply never enables the source and silently observes nothing forever.
       Azure-Identity is a lazily-created singleton, so whether it pre-exists depends on whether
       anything has touched Azure.Identity yet - which makes this a bug that appears and disappears
       with unrelated ordering. Nothing on this type is assigned in a constructor, there is no
       constructor, and EntraCredentialSelectionListenerTests constructs the listener AFTER forcing
       the source to exist, which is the path that goes through the base constructor. */

    private volatile string? _selected;
    private volatile bool _rejectedPayload;

    /// <summary>
    /// The credential type name event 13 reported, or null if it has not arrived (or arrived in a
    /// shape this type declines to forward — see <see cref="RejectedPayload"/>).
    /// </summary>
    internal string? SelectedCredentialType => _selected;

    /// <summary>
    /// True when event 13 arrived but its first payload slot was not something this type is willing
    /// to log. Kept separate from "nothing arrived" so a future payload change is a legible signal
    /// rather than silence — an absence and a refusal need different answers, and the refused value
    /// itself is never retained.
    /// </summary>
    internal bool RejectedPayload => _rejectedPayload;

    /// <inheritdoc/>
    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource is null ||
            !string.Equals(eventSource.Name, AzureIdentitySourceName, StringComparison.Ordinal))
        {
            return;
        }

        /* Informational is not a choice - it is the lowest level that delivers event 13, because
           event 13 is itself Informational. See this type's remarks for what else that admits and
           why OnEventWritten's allowlist, not this level, is the barrier. */
        EnableEvents(eventSource, EventLevel.Informational);
    }

    /// <inheritdoc/>
    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        /* A conjunction, so neither half alone can admit an event. The id is the allowlist; the
           source name means an id-13 collision from some other source this listener never enabled
           still cannot reach the log. */
        if (eventData is null ||
            eventData.EventId != CredentialSelectedEventId ||
            !string.Equals(eventData.EventSource?.Name, AzureIdentitySourceName, StringComparison.Ordinal))
        {
            return;
        }

        var payload = eventData.Payload;
        var credentialType = payload is { Count: > 0 } ? payload[0] as string : null;

        /* Payload[0] only, and only if it looks like a type name. Both halves are load-bearing: the
           index is what keeps a future second parameter out of the log, and the shape check is what
           keeps a REORDERED first parameter out of it. A tenant id, a UPN, a scope URL and an
           exception message each fail the shape check on a character a type name cannot contain. */
        if (EntraCredentialSelectionLog.IsCredentialTypeName(credentialType))
        {
            _selected = credentialType;
        }
        else
        {
            _rejectedPayload = true;
        }
    }
}

/// <summary>
/// Decides whether a captured credential selection is worth a log line, and writes it.
///
/// <para><b>Two lifetimes, both deliberate.</b></para>
///
/// <para><i>The listener's window is one connection open, not the process.</i> While the source is
/// enabled, <c>AzureIdentityEventSource.IsEnabled(EventLevel.Informational, …)</c> answers true
/// inside <c>Azure.Identity</c> and it performs the formatting work that the
/// <c>IsEnabled(…)</c> guard on almost every one of its event methods otherwise skips — formatting
/// scope arrays, rendering exceptions. At 1.18.0 that is 21 such guards plus 10
/// <c>when IsEnabled(…)</c> switch arms.
/// A monitoring tool that left it on would pay that on every token operation for the life of the
/// process, forever, to learn a fact that can only be reported once. So <see cref="Begin"/> is
/// called immediately before the open and the listener is disposed immediately after it; an
/// undisposed <see cref="EventListener"/> keeps receiving events, so the disposal is the window.
/// The cost of the narrow window is that an event raised outside it is missed — so the two
/// instrumented sites had to be the ones that get there FIRST, and they are, by construction rather
/// than by luck: <c>CollectionBackgroundService.ExecuteAsync</c> runs
/// <c>ServerManager.CheckAllConnectionsAsync</c> BEFORE
/// <c>RemoteCollectorService.RunDueCollectorsAsync</c> on every cycle including the first, and that
/// loop is the only thing that drives collection — so the connectivity check is the first code in
/// the process to open a connection to a monitored server. The interactive first-touch paths are
/// the connection dialog's Test button and <c>MainWindow</c>'s explicit retry, and both go through
/// an instrumented open. <c>EntraCredentialSelectionOrderingTests</c> pins that ordering, because it
/// lives in another file and a reorder there would silently blind this.</para>
///
/// <para><b>What that still does not cover, stated rather than implied.</b> Lite has other
/// <c>SqlConnection</c> sites that reach a monitored server — the bulk-add dialog, the excluded-
/// databases dialog, a server tab's ad-hoc reads, the plan fetcher. Each needs an already-configured
/// server, so in practice the sweep has run first; but a user who drives one of them inside the
/// background service's five-second startup delay could acquire the first token there. The
/// consequence is a <see cref="LogLevel.Debug"/> "not observed" line rather than a wrong one, which
/// is the direction to fail in — and it is why that line names the caching as the expected cause
/// instead of asserting it.</para>
///
/// <para><i>Only the last REPORTED name outlives the attempt.</i> Nothing else is retained: the
/// listener is gone and the captured value is read once. <see cref="s_lastReported"/> exists so an
/// unchanged repeat is a <see cref="LogLevel.Debug"/> line instead of an
/// <see cref="LogLevel.Information"/> one, because the selection is a process fact and a monitoring
/// tool restating it once per server per sweep would be noise. A CHANGED name still reports at
/// <see cref="LogLevel.Information"/> — the driver clears its credential cache
/// (<c>ActiveDirectoryAuthenticationProvider.cs:136-138</c>), so a different source genuinely can
/// win later, and that is the one thing here worth interrupting someone with.</para>
///
/// <para><b>Why this reports at most once in practice, stated rather than discovered.</b> Two
/// caches sit above the event and both are process-wide. <c>DefaultAzureCredential</c> raises event
/// 13 only on the branch that has no cached credential yet
/// (<c>DefaultAzureCredential.cs:168-179</c>): once <c>_credentialLock</c> holds a value, later
/// token requests take the <c>HasValue</c> branch and raise nothing. And SqlClient caches the
/// <c>DefaultAzureCredential</c> INSTANCE in a <c>static</c> map keyed by authority, scope, audience
/// and client id (<c>Microsoft.Data.SqlClient.Extensions.Azure</c> 7.0.2,
/// <c>ActiveDirectoryAuthenticationProvider.cs:29</c> and <c>:258-262</c>). So event 13 fires at most
/// once per process per key, and a second connection to the same tenant reports nothing at all. That
/// absence is a <see cref="LogLevel.Debug"/> line saying so, rather than silence: an empty result
/// must not be mistakable for "nothing happened".</para>
///
/// <para><b>No server name on the line, and that is not squeamishness.</b> The selection is scoped
/// to the process and to the driver's cache key, not to a server — the credential chain resolves
/// from environment variables, an <c>az login</c> session and machine identity, none of which are
/// per-server. Naming the server that happened to observe it would assert a per-server fact that is
/// not true, and the next connection to a different server in the same tenant reports nothing while
/// using the very same credential.</para>
/// </summary>
internal static class EntraCredentialSelectionLog
{
    /// <summary>The <c>AppLogger</c> source column for these lines.</summary>
    internal const string LogSource = "EntraCredential";

    /// <summary>
    /// The longest value forwarded as a credential type name. <c>Azure.Identity</c>'s longest
    /// credential type name is well under 60 characters; this is a bound on what an unexpected
    /// payload could put in the log, not a measurement of anything.
    /// </summary>
    internal const int MaxCredentialTypeNameLength = 256;

    /* Not volatile: every read goes through Volatile.Read and every write through
       Interlocked.Exchange, and a volatile field cannot be passed by ref to either. */
    private static string? s_lastReported;

    /// <summary>
    /// A listener for a connection about to be opened with
    /// <see cref="SqlAuthenticationMethod.ActiveDirectoryDefault"/> — which is what
    /// <see cref="PerformanceMonitor.Common.AuthenticationTypes.EntraDefaultCredential"/> maps to,
    /// and the only mode whose token comes from a <c>DefaultAzureCredential</c> — or <c>null</c> for
    /// every other mode. A <c>null</c> is a <c>using</c> that disposes nothing and a
    /// <see cref="Report"/> that does nothing, so no other mode enables the source or pays for it.
    ///
    /// <para>Keyed off the BUILDER rather than off the app's own mode string on purpose: this is the
    /// keyword the driver will actually act on, so the gate cannot disagree with the connection
    /// string, and it follows a credential profile into this mode for free if one is ever offered
    /// there (today they offer SQL, service principal and managed identity only).</para>
    /// </summary>
    internal static EntraCredentialSelectionListener? Begin(SqlConnectionStringBuilder? builder) =>
        builder?.Authentication == SqlAuthenticationMethod.ActiveDirectoryDefault
            ? new EntraCredentialSelectionListener()
            : null;

    /// <summary>
    /// Whether a captured payload has the shape of a CLR type name, and may therefore be logged.
    ///
    /// <para><b>The second barrier, and the one that survives a payload REORDER.</b> The allowlist
    /// in <see cref="EntraCredentialSelectionListener.OnEventWritten"/> guarantees the event is
    /// event 13; this guarantees the value taken from it is not something else. Every sensitive
    /// sibling payload in that source fails on a character a namespace-qualified type name cannot
    /// contain: a tenant id has <c>-</c>, an account UPN has <c>@</c>, a scope has <c>:</c> and
    /// <c>/</c>, an exception message and an MSAL log line have spaces. A dot is required, so a bare
    /// word is not a type name either.</para>
    ///
    /// <para>Generic type names (<c>`1[[…]]</c>) are rejected by the same rule. No credential type in
    /// <c>Azure.Identity</c> is generic, and rejecting one costs a <see cref="LogLevel.Debug"/> line
    /// saying the payload was declined — which is the direction to fail in.</para>
    /// </summary>
    internal static bool IsCredentialTypeName(string? value)
    {
        if (string.IsNullOrEmpty(value) || value.Length > MaxCredentialTypeNameLength)
        {
            return false;
        }

        foreach (var c in value)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '.' && c != '_' && c != '+')
            {
                return false;
            }
        }

        return value.Contains('.', StringComparison.Ordinal) && value[0] != '.' && value[^1] != '.';
    }

    /// <summary>
    /// What a captured (or absent, or declined) selection should produce, given what was last
    /// reported. Pure and static: no clock, no I/O, the caller passes the previous state and
    /// persists the returned one, which is what makes the whole decision testable without a log
    /// file. One return value carries the level, the text and the new state together so a caller
    /// cannot write the line and drop the state.
    /// </summary>
    /// <param name="credentialTypeName">
    /// <see cref="EntraCredentialSelectionListener.SelectedCredentialType"/>.
    /// </param>
    /// <param name="rejectedPayload">
    /// <see cref="EntraCredentialSelectionListener.RejectedPayload"/>.
    /// </param>
    /// <param name="lastReported">The last name reported at <see cref="LogLevel.Information"/>.</param>
    internal static EntraCredentialSelectionLine Decide(
        string? credentialTypeName,
        bool rejectedPayload,
        string? lastReported)
    {
        if (!IsCredentialTypeName(credentialTypeName))
        {
            /* Re-checked here rather than trusted from the listener, so this function's answer does
               not depend on which of two types applied the rule. The declined value is NOT in the
               message - naming it would be the leak the check exists to prevent. */
            return rejectedPayload
                ? new EntraCredentialSelectionLine(
                    LogLevel.Debug,
                    "DefaultAzureCredential reported a credential selection whose payload was not a "
                        + "type name, so it was not logged. Azure.Identity's event 13 payload may have "
                        + "changed shape.",
                    Reported: null)
                : new EntraCredentialSelectionLine(
                    LogLevel.Debug,
                    "DefaultAzureCredential did not report which credential it selected on this "
                        + "connection. Expected on any connection but the first: Azure.Identity "
                        + "raises that event once per credential instance, and SqlClient caches the "
                        + "instance for the process.",
                    Reported: null);
        }

        return string.Equals(credentialTypeName, lastReported, StringComparison.Ordinal)
            ? new EntraCredentialSelectionLine(
                LogLevel.Debug,
                $"DefaultAzureCredential selected {credentialTypeName} (unchanged).",
                Reported: null)
            : new EntraCredentialSelectionLine(
                LogLevel.Information,
                $"DefaultAzureCredential selected {credentialTypeName}.",
                Reported: credentialTypeName);
    }

    /// <summary>
    /// Reads the listener and writes the line. A no-op for a <c>null</c> listener, which is every
    /// mode but this one.
    ///
    /// <para><b>Called from a <c>finally</c> at both call sites, so it must not throw.</b> Nothing
    /// here does I/O: <see cref="Decide"/> is pure string work and <see cref="AppLogger"/> enqueues
    /// into a buffer. There is deliberately no blanket <c>catch</c> — a <c>finally</c> that swallows
    /// everything would make this feature silently dead at exactly the moment it broke, and would
    /// hide the defect rather than the symptom.</para>
    /// </summary>
    internal static void Report(EntraCredentialSelectionListener? listener)
    {
        if (listener is null)
        {
            return;
        }

        var captured = listener.SelectedCredentialType;
        var line = Decide(captured, listener.RejectedPayload, Volatile.Read(ref s_lastReported));

        if (line.Reported is not null)
        {
            /* Claimed atomically, because a read-then-write pair does not dedupe here.
               CheckAllConnectionsAsync checks servers concurrently, and one raised event is delivered
               to EVERY attached listener - so two concurrent first connections capture the SAME name
               and, reading before either writes, both decide they are the first and both log at
               Information. Interlocked.Exchange hands exactly one caller a previous value different
               from what it is claiming; anyone else gets its own name back and re-decides against it.
               The correctness of that rests on the exchange being atomic, not on a test: the race is
               not reproducible on demand, and the sequential form of the same path is pinned. */
            var previous = Interlocked.Exchange(ref s_lastReported, line.Reported);

            if (string.Equals(previous, line.Reported, StringComparison.Ordinal))
            {
                line = Decide(captured, listener.RejectedPayload, previous);
            }
        }

        if (line.Level == LogLevel.Information)
        {
            AppLogger.Info(LogSource, line.Message);
        }
        else
        {
            AppLogger.Debug(LogSource, line.Message);
        }
    }

    /// <summary>
    /// Forgets the last reported name, so a test can exercise the first-observation arm. Never
    /// called by the app.
    /// </summary>
    internal static void ResetForTests() => s_lastReported = null;
}

/// <summary>
/// One log line: the level it is written at, its text, and the name to remember as reported (null
/// when nothing should be remembered). A record struct so the three cannot be returned separately
/// and recombined wrongly.
/// </summary>
internal readonly record struct EntraCredentialSelectionLine(
    LogLevel Level,
    string Message,
    string? Reported);
