/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Darling.Tests;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3218 shipped <c>EntraDefaultCredential</c> and disclosed, in dialog text, that the credential
/// search order is the driver's and that a machine with several Azure identities connects as
/// whichever comes first. Nothing recorded WHICH. This is the recording.
///
/// <para><b>Every pin here raises the real event on the real event source.</b>
/// <c>Azure.Identity</c>'s <c>AzureIdentityEventSource</c> is reached by reflection — it is
/// <c>internal</c>, so this is the only way in from outside the assembly — and its actual
/// <c>DefaultAzureCredentialCredentialSelected</c> method is invoked. So the id, the level, the
/// payload shape and the source name are the ones that ship rather than a stand-in's, and an
/// upstream move fails these tests instead of passing them.</para>
///
/// <para><b>The failure mode this file exists to avoid.</b> A listener test that constructs a
/// listener and asserts nothing was logged passes trivially in a harness where no
/// <c>DefaultAzureCredential</c> ever runs — the shape that put two vacuous pins into #3218. So
/// every negative here is paired with a positive on the SAME listener: the sibling event is raised
/// first and must not be forwarded, then event 13 is raised and must be. A listener that received
/// nothing fails the second half, which is what makes the first half evidence.</para>
///
/// <para><b>What is NOT claimed.</b> That a real Entra tenant issues a token to a real
/// <c>az login</c> session through this path — still unverified, still needs a tenant and a Windows
/// host. This file answers only for what the listener does with the event once it is raised.</para>
/// </summary>
public sealed class EntraCredentialSelectionTests : IDisposable
{
    /* Azure.Identity 1.18.0, AzureIdentityEventSource.cs — every id and signature reflected below is
       read from that file at tag Azure.Identity_1.18.0, which is what this repository resolves
       transitively through Microsoft.Data.SqlClient.Extensions.Azure 7.0.2 (see #3219: it is not
       pinned, which is exactly why the upstream contract is pinned HERE). */
    private const string EventSourceTypeName = "Azure.Identity.AzureIdentityEventSource, Azure.Identity";

    /// <summary>:307 — the event this listener exists for. One <c>string credentialType</c>.</summary>
    private const string SelectedMethod = "DefaultAzureCredentialCredentialSelected";

    /// <summary>
    /// :371 — event 18, <c>Informational</c>, and it carries TENANT IDS. The sibling that says why
    /// the callback filter has to be the barrier.
    /// </summary>
    private const string TenantIdMethod = "TenantIdDiscoveredAndUsed";

    /// <summary>
    /// :427 — event 26, <c>Informational</c>, and its FIRST payload slot is also called
    /// <c>credentialType</c> and holds a real credential type name. The sharpest negative control
    /// available: it passes the type-name shape check, it passes the level, it passes the source
    /// name, and the ONLY thing that keeps it out of the log is the event-id allowlist.
    /// </summary>
    private const string ManagedIdentitySelectedMethod = "ManagedIdentityCredentialSelected";

    /// <summary>:399 — event 21, <c>Warning</c>. Proves the level admits MORE than Informational.</summary>
    private const string WarningMethod = "UserAssignedManagedIdentityNotSupported";

    /// <summary>A plausible winner, and the value every positive assertion looks for.</summary>
    private const string CliCredential = "Azure.Identity.AzureCliCredential";

    /// <summary>
    /// The process-wide minimum is restored, and the last-reported name forgotten, because both are
    /// static and a leak from here would change what a neighbouring test observes.
    /// </summary>
    public void Dispose()
    {
        AppLogger.SetMinimumLevel(AppLogger.DefaultMinimumLevel);
        EntraCredentialSelectionLog.ResetForTests();
    }

    // ---- The upstream contract this listener is built on ---------------------------------

    /// <summary>
    /// The four facts the listener hard-codes, read off the shipped assembly rather than off a
    /// version note. <c>Azure.Identity</c> arrives transitively and unpinned (#3219), so a SqlClient
    /// bump can move it with no code change anywhere — this is the test that makes such a move loud.
    /// </summary>
    [Fact]
    public void TheUpstreamEventContract_IsWhatTheListenerAssumes()
    {
        var type = EventSourceType();
        var source = Singleton();

        Assert.Equal(
            EntraCredentialSelectionListener.AzureIdentitySourceName,
            source.Name);

        var method = type.GetMethod(SelectedMethod, BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(method);

        var parameters = method!.GetParameters();
        Assert.Single(parameters);
        Assert.Equal(typeof(string), parameters[0].ParameterType);

        var attribute = method.GetCustomAttribute<EventAttribute>();
        Assert.NotNull(attribute);
        Assert.Equal(EntraCredentialSelectionListener.CredentialSelectedEventId, attribute!.EventId);
        Assert.Equal(EventLevel.Informational, attribute.Level);
    }

    /// <summary>
    /// <para><b>Why the filter is an allowlist and not a level or a keyword.</b> Not one event in
    /// that source declares <c>Keywords</c>, so <c>EnableEvents</c> has no dimension to exclude the
    /// sensitive siblings on; and event 13 is itself <c>Informational</c>, so there is no level that
    /// admits it and not them.</para>
    ///
    /// <para>The positive control is the event COUNT: "no event declares keywords" is also true of a
    /// type with no events, which is not the claim. And the second assertion is a FLOOR on how many
    /// events share event 13's level, not the exact number — the exact number (20 of 29 at
    /// <c>Informational</c> in 1.18.0) is a measurement that goes stale on any upstream bump, while
    /// "event 13 is not alone at its level" is the property the design rests on and does not.</para>
    /// </summary>
    [Fact]
    public void NoEventInThatSourceDeclaresKeywords_AndEventThirteenIsNotAloneAtItsLevel()
    {
        var events = EventSourceType()
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Select(m => m.GetCustomAttribute<EventAttribute>())
            .Where(a => a is not null)
            .Select(a => a!)
            .ToList();

        Assert.True(
            events.Count > 1,
            $"reflected {events.Count} [Event]-attributed methods on the Azure-Identity source; the "
                + "keyword assertion below is vacuous unless there are events to assert about");

        var withKeywords = events
            .Where(a => a.Keywords != EventKeywords.None)
            .Select(a => a.EventId)
            .ToList();

        Assert.True(
            withKeywords.Count == 0,
            "an event in Azure-Identity now declares Keywords (ids: "
                + string.Join(", ", withKeywords)
                + "). EnableEvents could then exclude the sensitive siblings on that dimension, which "
                + "is a better barrier than a callback filter — reconsider the design rather than "
                + "just updating this number.");

        var atInformational = events
            .Count(a => a.Level == EventLevel.Informational);

        Assert.True(
            atInformational > 1,
            "event 13 is now the only Informational event in Azure-Identity, so the level alone would "
                + $"isolate it (counted {atInformational}). The allowlist is still correct but its "
                + "stated reason has changed.");
    }

    // ---- The mechanism working, and the sibling that must not ----------------------------

    /// <summary>
    /// <para>The positive half, and the hazard-1 falsifier at the same time. The event source is
    /// forced to EXIST before the listener is constructed, so
    /// <c>EventListener.OnEventSourceCreated</c> is reached from the BASE CONSTRUCTOR — the path on
    /// which a derived field assigned in a constructor body is still null. If the listener ever grows
    /// a constructor that the callback depends on, it stops enabling the source and this test goes
    /// red rather than the app going quietly blind.</para>
    /// </summary>
    [Fact]
    public void EventThirteen_ArrivesAsTheCredentialTypeName_WithTheSourceAlreadyCreated()
    {
        var source = Singleton();
        Assert.Equal(EntraCredentialSelectionListener.AzureIdentitySourceName, source.Name);

        using var listener = new EntraCredentialSelectionListener();

        Raise(SelectedMethod, CliCredential);

        Assert.Equal(CliCredential, listener.SelectedCredentialType);
        Assert.False(listener.RejectedPayload);
    }

    /// <summary>
    /// <para><b>Both directions on one listener, which is the whole point.</b> Three siblings are
    /// raised first and none may be forwarded; then event 13 is raised and must be. A listener that
    /// simply received nothing — the vacuous shape — fails the final assertion, so the three
    /// preceding ones are evidence of filtering rather than of silence.</para>
    ///
    /// <para>The siblings are chosen to defeat each barrier in turn:
    /// <c>ManagedIdentityCredentialSelected</c>'s first payload slot IS a valid credential type name,
    /// so only the id keeps it out; <c>TenantIdDiscoveredAndUsed</c> carries tenant ids, which is the
    /// value that must never reach the log; and <c>UserAssignedManagedIdentityNotSupported</c> is at
    /// <c>Warning</c>, proving the enabled level admits events more severe than the one requested and
    /// not merely the one asked for.</para>
    /// </summary>
    [Fact]
    public void SensitiveSiblings_AreNotForwarded_AndTheSameListenerStillCapturesEventThirteen()
    {
        Singleton();
        using var listener = new EntraCredentialSelectionListener();

        Raise(ManagedIdentitySelectedMethod, "Azure.Identity.ManagedIdentityCredential", "some-id");
        Assert.Null(listener.SelectedCredentialType);
        Assert.False(listener.RejectedPayload);

        Raise(TenantIdMethod, "00000000-0000-0000-0000-000000000000", "11111111-1111-1111-1111-111111111111");
        Assert.Null(listener.SelectedCredentialType);

        Raise(WarningMethod, "SomeEnvironment");
        Assert.Null(listener.SelectedCredentialType);

        /* The discriminator. Everything above is consistent with a listener that never enabled the
           source at all until this line succeeds. */
        Raise(SelectedMethod, CliCredential);
        Assert.Equal(CliCredential, listener.SelectedCredentialType);
    }

    /// <summary>
    /// <para>An INDEPENDENT instrument for the claim the allowlist rests on: that the siblings really
    /// do reach a callback at this level, rather than being filtered out before it. A recording
    /// listener enabling the same source at the same level records every id it sees; the production
    /// listener over the identical three events keeps one.</para>
    ///
    /// <para>Without this, "the sibling was not forwarded" could equally mean the level never
    /// delivered it — in which case the callback filter would be doing nothing and could be deleted
    /// without any test noticing.</para>
    /// </summary>
    [Fact]
    public void TheEnabledLevelDeliversTheSiblingsToTheCallback_AndOnlyEventThirteenIsKept()
    {
        Singleton();

        using var recorder = new RecordingListener();
        using var listener = new EntraCredentialSelectionListener();

        Raise(SelectedMethod, CliCredential);
        Raise(TenantIdMethod, "00000000-0000-0000-0000-000000000000", "11111111-1111-1111-1111-111111111111");
        Raise(WarningMethod, "SomeEnvironment");

        var seen = recorder.SeenEventIds;

        Assert.Contains(EntraCredentialSelectionListener.CredentialSelectedEventId, seen);
        Assert.True(
            seen.Count >= 3,
            "the recording listener saw " + string.Join(", ", seen.Order())
                + " — fewer than the three events raised, so the level did not deliver the siblings and "
                + "the allowlist below is not what excluded them");

        Assert.Equal(CliCredential, listener.SelectedCredentialType);
    }

    /// <summary>
    /// <para><b>The listener enables Azure-Identity and nothing else.</b> Enabling an
    /// <see cref="EventSource"/> is process-global in effect — once enabled, <c>IsEnabled()</c>
    /// answers true inside the owning library and it performs formatting work it otherwise skips. A
    /// name check dropped from <c>OnEventSourceCreated</c> would turn this into a listener that
    /// enables EVERY event source in the process at <c>Informational</c>, and every other pin in this
    /// file would stay green: the callback allowlist would still keep the log clean, so the only
    /// symptom would be cost paid forever by every instrumented library in the app.</para>
    ///
    /// <para>The control is the first assertion: a source that is not enabled while the listener is
    /// alive is only evidence if some source demonstrably IS.</para>
    /// </summary>
    [Fact]
    public void TheListenerEnablesAzureIdentity_AndNoOtherEventSource()
    {
        var azureIdentity = Singleton();
        using var bystander = new BystanderEventSource();

        using var listener = new EntraCredentialSelectionListener();

        Assert.True(
            azureIdentity.IsEnabled(EventLevel.Informational, EventKeywords.None),
            "the listener did not enable Azure-Identity, so the assertion below proves nothing");

        Assert.False(
            bystander.IsEnabled(),
            $"the listener enabled {bystander.Name} as well as Azure-Identity. Enabling a source is "
                + "process-global in effect, so this is formatting work paid by an unrelated library "
                + "on every event for the life of every connection attempt.");
    }

    // ---- The shape check, which is the barrier against a payload REORDER -----------------

    [Theory]
    [InlineData("Azure.Identity.AzureCliCredential")]
    [InlineData("Azure.Identity.EnvironmentCredential")]
    [InlineData("Azure.Identity.Outer+Inner")]
    [InlineData("A.B")]
    [InlineData("Azure.Identity.Some_Credential2")]
    public void IsCredentialTypeName_TakesATypeName(string value) =>
        Assert.True(EntraCredentialSelectionLog.IsCredentialTypeName(value));

    /// <summary>
    /// Every rejected case is the shape of a real payload carried by a sibling event in that source
    /// at a level this listener enables, so each one is a value that a reordered or reshaped payload
    /// could actually put in front of the read.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("00000000-0000-0000-0000-000000000000")]   // a tenant id: hyphens
    [InlineData("someone@example.com")]                     // an account upn: @
    [InlineData("https://database.windows.net/.default")]   // a scope: : and /
    [InlineData("DefaultAzureCredential failed to retrieve a token")] // a message: spaces
    [InlineData("AzureCliCredential")]                      // no namespace at all
    [InlineData(".Leading")]
    [InlineData("Trailing.")]
    [InlineData("Azure.Identity.Thing`1[[System.String, System.Private.CoreLib]]")]
    public void IsCredentialTypeName_RejectsEverySensitiveSiblingShape(string? value) =>
        Assert.False(EntraCredentialSelectionLog.IsCredentialTypeName(value));

    [Fact]
    public void IsCredentialTypeName_RejectsSomethingLongerThanAnyTypeName() =>
        Assert.False(EntraCredentialSelectionLog.IsCredentialTypeName(
            "Azure.Identity." + new string('x', EntraCredentialSelectionLog.MaxCredentialTypeNameLength)));

    /// <summary>
    /// <para>A real event 13 whose payload is a tenant id rather than a type name — which is what a
    /// future upstream parameter reorder looks like from here. It must be declined, the value must
    /// not be retained, and the resulting log line must not contain it.</para>
    ///
    /// <para>The positive control is the second half: the same listener then takes a well-shaped
    /// value, so "declined" is a decision about the payload and not a listener that stopped
    /// working.</para>
    /// </summary>
    [Fact]
    public void AReorderedPayload_IsDeclinedAndNeverReachesTheLine()
    {
        const string TenantIdShaped = "00000000-0000-0000-0000-000000000000";

        Singleton();
        using var listener = new EntraCredentialSelectionListener();

        Raise(SelectedMethod, TenantIdShaped);

        Assert.Null(listener.SelectedCredentialType);
        Assert.True(listener.RejectedPayload);

        var line = EntraCredentialSelectionLog.Decide(
            listener.SelectedCredentialType, listener.RejectedPayload, lastReported: null);

        Assert.Equal(LogLevel.Debug, line.Level);
        Assert.DoesNotContain(TenantIdShaped, line.Message, StringComparison.Ordinal);
        Assert.Null(line.Reported);

        Raise(SelectedMethod, CliCredential);
        Assert.Equal(CliCredential, listener.SelectedCredentialType);
    }

    // ---- The gate: this mode and no other ------------------------------------------------

    [Fact]
    public void Begin_ReturnsAListener_ForActiveDirectoryDefault()
    {
        using var listener = EntraCredentialSelectionLog.Begin(
            new SqlConnectionStringBuilder { Authentication = SqlAuthenticationMethod.ActiveDirectoryDefault });

        Assert.NotNull(listener);
    }

    /// <summary>
    /// Every other <see cref="SqlAuthenticationMethod"/> the driver defines, swept rather than
    /// sampled, plus a builder that sets no keyword at all and a null builder. A mode added to the
    /// enum upstream lands here as a new theory row automatically, and lands on the null arm — which
    /// is the safe direction: it enables no event source.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryOtherAuthenticationMethod))]
    public void Begin_ReturnsNull_ForEveryOtherAuthenticationMethod(SqlAuthenticationMethod method)
    {
        var builder = new SqlConnectionStringBuilder { Authentication = method };

        Assert.Null(EntraCredentialSelectionLog.Begin(builder));
    }

    public static TheoryData<SqlAuthenticationMethod> EveryOtherAuthenticationMethod()
    {
        var data = new TheoryData<SqlAuthenticationMethod>();
        foreach (var method in Enum.GetValues<SqlAuthenticationMethod>())
        {
            if (method != SqlAuthenticationMethod.ActiveDirectoryDefault)
            {
                data.Add(method);
            }
        }

        return data;
    }

    [Fact]
    public void Begin_ReturnsNull_ForABuilderWithNoAuthenticationKeywordAndForNull()
    {
        Assert.Null(EntraCredentialSelectionLog.Begin(new SqlConnectionStringBuilder()));
        Assert.Null(EntraCredentialSelectionLog.Begin(null));
    }

    [Fact]
    public void Report_DoesNothingForANullListener() =>
        EntraCredentialSelectionLog.Report(null);

    // ---- What gets written, and at which level -------------------------------------------

    [Fact]
    public void Decide_ReportsAFirstObservationAtInformation()
    {
        var line = EntraCredentialSelectionLog.Decide(CliCredential, rejectedPayload: false, lastReported: null);

        Assert.Equal(LogLevel.Information, line.Level);
        Assert.Contains(CliCredential, line.Message, StringComparison.Ordinal);
        Assert.Equal(CliCredential, line.Reported);
    }

    [Fact]
    public void Decide_ReportsAnUnchangedRepeatAtDebug_AndRemembersNothingNew()
    {
        var line = EntraCredentialSelectionLog.Decide(
            CliCredential, rejectedPayload: false, lastReported: CliCredential);

        Assert.Equal(LogLevel.Debug, line.Level);
        Assert.Null(line.Reported);
    }

    /// <summary>
    /// A CHANGED selection is the one thing here worth an <see cref="LogLevel.Information"/> line
    /// after the first: the driver clears its static credential cache, so a different source
    /// genuinely can start winning mid-process, and that is a different identity connecting.
    /// </summary>
    [Fact]
    public void Decide_ReportsAChangeAtInformation()
    {
        var line = EntraCredentialSelectionLog.Decide(
            "Azure.Identity.EnvironmentCredential",
            rejectedPayload: false,
            lastReported: CliCredential);

        Assert.Equal(LogLevel.Information, line.Level);
        Assert.Contains("Azure.Identity.EnvironmentCredential", line.Message, StringComparison.Ordinal);
        Assert.Equal("Azure.Identity.EnvironmentCredential", line.Reported);
    }

    /// <summary>
    /// Nothing observed is a line saying so, not silence — and it names the reason, because "no line"
    /// and "the event fires once per process" are indistinguishable to whoever reads the log.
    /// </summary>
    [Fact]
    public void Decide_SaysNothingWasObserved_RatherThanSayingNothing()
    {
        var line = EntraCredentialSelectionLog.Decide(null, rejectedPayload: false, lastReported: null);

        Assert.Equal(LogLevel.Debug, line.Level);
        Assert.False(string.IsNullOrWhiteSpace(line.Message));
        Assert.Null(line.Reported);
    }

    /// <summary>
    /// <para><b>End to end through the real <see cref="AppLogger"/>, at the level a default install
    /// runs at.</b> The first observation must actually reach the log; the unchanged repeat must not.
    /// Nothing else in this file would catch a <see cref="Report"/> that wrote every line at
    /// <see cref="LogLevel.Debug"/> — the messages would still be correct and the whole feature would
    /// be invisible on every shipped install.</para>
    /// </summary>
    [Fact]
    public void Report_WritesTheFirstObservationAtTheDefaultLevel_AndNotTheRepeat()
    {
        Singleton();
        EntraCredentialSelectionLog.ResetForTests();
        Assert.Equal(LogLevel.Information, AppLogger.MinimumLevel);

        AppLogger.DrainBufferedLines();

        using (var first = new EntraCredentialSelectionListener())
        {
            Raise(SelectedMethod, CliCredential);
            EntraCredentialSelectionLog.Report(first);
        }

        var written = Ours();
        Assert.Single(written);
        Assert.Contains(CliCredential, written[0], StringComparison.Ordinal);
        Assert.Contains("INFO", written[0], StringComparison.Ordinal);

        using (var again = new EntraCredentialSelectionListener())
        {
            Raise(SelectedMethod, CliCredential);
            EntraCredentialSelectionLog.Report(again);
        }

        Assert.Empty(Ours());
    }

    /// <summary>
    /// The same repeat, with the minimum lowered — so "not written at the default" above is a level
    /// decision rather than a <see cref="Report"/> that produced no line at all.
    /// </summary>
    [Fact]
    public void Report_WritesTheRepeatAndTheNotObservedLine_OnceDebugIsAdmitted()
    {
        Singleton();
        EntraCredentialSelectionLog.ResetForTests();
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        AppLogger.DrainBufferedLines();

        using (var first = new EntraCredentialSelectionListener())
        {
            Raise(SelectedMethod, CliCredential);
            EntraCredentialSelectionLog.Report(first);
        }

        Assert.Single(Ours());

        using (var again = new EntraCredentialSelectionListener())
        {
            Raise(SelectedMethod, CliCredential);
            EntraCredentialSelectionLog.Report(again);
        }

        var repeat = Ours();
        Assert.Single(repeat);
        Assert.Contains("DEBUG", repeat[0], StringComparison.Ordinal);

        /* And the third arm: a listener that observed nothing at all, which is what every connection
           after the first actually looks like. */
        using (var silent = new EntraCredentialSelectionListener())
        {
            EntraCredentialSelectionLog.Report(silent);
        }

        var absent = Ours();
        Assert.Single(absent);
        Assert.Contains("DEBUG", absent[0], StringComparison.Ordinal);
        Assert.DoesNotContain(CliCredential, absent[0], StringComparison.Ordinal);
    }

    // ---- Both connection-open sites are instrumented, in the right order -----------------

    /// <summary>
    /// <para>Behavioural coverage cannot reach either site: one is a WPF <c>Window</c> needing a
    /// dispatcher and a live form, the other needs a reachable SQL Server. So the call sites are
    /// asserted in source, on <c>StripCommentsAndStrings</c>' output — which is what makes this a
    /// check on CODE rather than on my own comments. Both sites carry a comment naming
    /// <c>EntraCredentialSelectionLog</c> and explaining it, so a raw substring search over either
    /// file would be satisfied by the comment alone and would pass with every call deleted. #3201's
    /// equivalent pin did exactly that until the strip was added.</para>
    ///
    /// <para><b>The ORDER is the discriminating assertion.</b> Both names being present is satisfied
    /// by a <c>Report</c> placed before the open, which would capture nothing on every connection
    /// forever while reading as fully instrumented.</para>
    /// </summary>
    [Theory]
    [InlineData("Lite/Services/ServerManager.cs", "CheckConnectionAsync(string serverId")]
    [InlineData("Lite/Windows/AddServerDialog.xaml.cs", "RunConnectionTestAsync()")]
    public void EveryConnectionOpenSite_BeginsBeforeTheOpenAndReportsAfterIt(
        string relativePath, string methodAnchor)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(relativePath));

        var signature = source.IndexOf(methodAnchor, StringComparison.Ordinal);
        Assert.True(signature >= 0, $"{methodAnchor} is the connection-open method in {relativePath} and must exist");

        var brace = source.IndexOf('{', signature);
        Assert.True(brace > signature, $"{methodAnchor} must have a body");

        var body = CSharpSourceWalker.BraceBalanced(source, brace);

        var begin = body.IndexOf("EntraCredentialSelectionLog.Begin", StringComparison.Ordinal);
        var open = body.IndexOf("connection.OpenAsync", StringComparison.Ordinal);
        var report = body.IndexOf("EntraCredentialSelectionLog.Report", StringComparison.Ordinal);

        Assert.True(open >= 0, $"{relativePath} must still open the connection in {methodAnchor}");
        Assert.True(begin >= 0, $"{relativePath} must attach the credential-selection listener in {methodAnchor}");
        Assert.True(report >= 0, $"{relativePath} must report the credential selection in {methodAnchor}");

        Assert.True(
            begin < open,
            "the listener must be attached BEFORE the open, or Azure.Identity raises event 13 with "
                + "nothing subscribed and the selection is never observed");
        Assert.True(
            open < report,
            "the selection must be reported AFTER the open, or it is read before the event that "
                + "produces it and every connection reports nothing");
    }

    // ---- Helpers -------------------------------------------------------------------------

    /// <summary>
    /// <c>Azure.Identity</c>'s event source type. Asserted non-null rather than skipped: this whole
    /// file is about a specific upstream event, and a harness that cannot see the assembly must fail
    /// loudly rather than report an absence it has no instrument for (the #3218 lesson).
    /// </summary>
    private static Type EventSourceType()
    {
        var type = Type.GetType(EventSourceTypeName, throwOnError: false);

        Assert.NotNull(type);
        return type!;
    }

    /// <summary>
    /// The real singleton, which also FORCES the event source to exist — so a listener constructed
    /// after this call reaches <c>OnEventSourceCreated</c> from the base constructor.
    /// </summary>
    private static EventSource Singleton()
    {
        var property = EventSourceType().GetProperty("Singleton", BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(property);

        var value = property!.GetValue(null) as EventSource;
        Assert.NotNull(value);
        return value!;
    }

    /// <summary>Invokes one of the source's real event methods, so a real event is written.</summary>
    private static void Raise(string methodName, params object[] arguments)
    {
        var method = EventSourceType().GetMethod(
            methodName,
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            arguments.Select(a => a.GetType()).ToArray());

        Assert.NotNull(method);
        method!.Invoke(Singleton(), arguments);
    }

    /// <summary>
    /// This feature's lines out of the process-wide buffer, so a concurrently-running test's lines
    /// are not mistaken for these and these are not mistaken for theirs.
    /// </summary>
    private static List<string> Ours() =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains($"[{EntraCredentialSelectionLog.LogSource}]", StringComparison.Ordinal))
            .ToList();

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }

    /// <summary>
    /// An independent instrument: records every event id the enabled level actually delivers, so the
    /// production listener's filtering can be told apart from a level that delivered nothing.
    ///
    /// <para>The bag is a FIELD INITIALISER, which in C# runs BEFORE the base constructor — the same
    /// hazard the production listener avoids with consts. Assigned in a constructor body it would be
    /// null when <see cref="OnEventSourceCreated"/> fires for an already-existing source, which is
    /// exactly the case every test here sets up.</para>
    /// </summary>
    /// <summary>
    /// An unrelated event source, existing only so "the listener enabled nothing else" has something
    /// to be false about. Named outside the <c>Azure-</c> family on purpose.
    /// </summary>
    [EventSource(Name = "PerformanceMonitorLite-Tests-Bystander")]
    private sealed class BystanderEventSource : EventSource
    {
    }

    private sealed class RecordingListener : EventListener
    {
        private readonly ConcurrentBag<int> _seen = new();

        internal List<int> SeenEventIds => _seen.ToList();

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            if (string.Equals(
                    eventSource?.Name,
                    EntraCredentialSelectionListener.AzureIdentitySourceName,
                    StringComparison.Ordinal))
            {
                EnableEvents(eventSource!, EventLevel.Informational);
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData) => _seen.Add(eventData.EventId);
    }
}
