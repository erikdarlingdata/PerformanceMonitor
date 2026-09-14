/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Lite.Tests;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3196: a device-code sign-in, added because BOTH Entra paths Lite already offered are out of
/// reach from a process the interactive user did not launch — Entra MFA because the Windows account
/// broker cannot see their logon session, Existing Sign-In because <c>%USERPROFILE%</c> is the
/// launching account's.
///
/// <para><b>What is and is not claimed here.</b> Every pin below runs without an Entra tenant: the
/// connection string's keywords, which modes demand a stored secret, which modes can raise a window
/// and what each gate does about it, that the rendezvous between the driver's callback and the
/// prompt window is wired in both directions, and that the secret half of the exchange is never
/// copied out of the driver's result. That a real tenant issues a code, accepts it in a browser and
/// hands SqlClient a token is NOT pinned by anything here and has not been run by anyone — it needs
/// a tenant and a Windows host, the limit #3196, #3214 and #2184 all had.</para>
///
/// <para><b>Why so much of this is asserted against SOURCE.</b> The parts that decide whether a user
/// ever sees the code are wiring: a provider registered against the right authentication method, a
/// callback that publishes onto the attempt a call site created, a window that closes when the
/// attempt ends and cancels it when the user closes it. None of that is reachable behaviourally
/// without a WPF dispatcher and a live tenant, and the shape of the defect is always an omission
/// rather than a wrong value — which is exactly the case the <c>EntraBrokerFailureTests</c> idiom
/// exists for. Comments and string literals are stripped FIRST at every such site, because these
/// files carry comments naming the very calls being searched for.</para>
/// </summary>
public class EntraDeviceCodeTests
{
    // ---- The connection string ------------------------------------------------------------

    [Fact]
    public void DeviceCode_SetsActiveDirectoryDeviceCodeFlow()
    {
        var server = new ServerConnection
        {
            ServerName = "example.database.windows.net",
            DatabaseName = "mydb",
            AuthenticationType = AuthenticationTypes.EntraDeviceCode,
            EncryptMode = "Mandatory",
        };

        var conn = new SqlConnectionStringBuilder(server.BuildConnectionString(null, null));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow, conn.Authentication);
        Assert.False(conn.IntegratedSecurity);
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, conn.Encrypt);
    }

    [Fact]
    public void DeviceCode_SurvivesTheRoundTripThroughTheConnectionStringText()
    {
        /* The enum assignment above is compile-time. This is the bytes that travel, and the mode is
           one keyword - so a driver that changed how it renders or parses that keyword would leave
           every other pin in this file green while no connection worked.

           Measured rather than assumed: the builder renders the ENUM NAME
           (Authentication=ActiveDirectoryDeviceCodeFlow), not the spaced "Active Directory Device
           Code Flow" that Microsoft's documentation uses and that ConvertToAuthenticationType also
           accepts. Both parse; only one is emitted. Asserting the documented spelling here would
           have been a pin on a fact the code does not have. */
        var builder = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        var text = builder.ConnectionString;

        Assert.Contains("Authentication=ActiveDirectoryDeviceCodeFlow", text, StringComparison.Ordinal);

        /* The round trip is what actually matters: SqlConnection is handed TEXT, and re-parsing it
           has to land back on the same method. A rename on either side breaks this. */
        Assert.Equal(
            SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow,
            new SqlConnectionStringBuilder(text).Authentication);

        /* Positive control: the same probe on the neighbouring mode must render something DIFFERENT.
           Without it, a builder that emitted one fixed Authentication value for everything - or a
           ConnectionString property that listed the whole vocabulary - satisfies the assertions above
           for no reason. */
        var sibling = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(
            sibling, AuthenticationTypes.EntraDefaultCredential, null, null, null, null);

        Assert.DoesNotContain(
            "Authentication=ActiveDirectoryDeviceCodeFlow", sibling.ConnectionString, StringComparison.Ordinal);
    }

    [Fact]
    public void DeviceCode_CarriesNoSecretAndNoUserId_EvenWhenHandedBoth()
    {
        /* Handed a username AND a password AND both client ids, because that is the mistake this pin
           exists to catch: the arm sits between ServicePrincipal's, which assigns UserID and Password
           from exactly these parameters, and EntraMFA's, which assigns UserID from the first.

           UserID is the one that matters, and for a reason specific to this mode rather than borrowed
           from EntraDefaultCredential's. SqlClient DOES forward UserId on this path -
           GetFedAuthToken's arm for methods 2 and 4-8 calls WithUserId - but the device-code request
           never reads it: AcquireTokenInteractiveDeviceFlowAsync consumes userId only in its
           authenticationMethod == 4 branch, as WithLoginHint for the interactive flow
           (Extensions.Azure 7.0.2, :668-689). A copy of either neighbouring arm would pass every
           other test in this file. */
        var builder = new SqlConnectionStringBuilder();

        ServerConnection.ApplyAuthentication(
            builder,
            AuthenticationTypes.EntraDeviceCode,
            username: "someone@contoso.com",
            password: "a-secret-that-must-not-travel",
            azureClientId: "00000000-0000-0000-0000-000000000000",
            managedIdentityClientId: "11111111-1111-1111-1111-111111111111");

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow, builder.Authentication);
        Assert.Equal(string.Empty, builder.UserID);
        Assert.Equal(string.Empty, builder.Password);
        Assert.False(builder.IntegratedSecurity);

        /* And on the rendered string too, because that is what a connection travels as and what a
           log line or a bug report could carry. */
        var rendered = builder.ConnectionString;
        Assert.DoesNotContain("Password", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("User ID", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("someone@contoso.com", rendered, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DeviceCode_ClearsIntegratedSecurityOnAPrePopulatedBuilder()
    {
        /* Both build sites hand in a builder they have already touched. IntegratedSecurity left true
           beside an Authentication keyword is a connection string the driver rejects, and the arm
           sets it false explicitly rather than relying on the default. */
        var builder = new SqlConnectionStringBuilder { IntegratedSecurity = true };

        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        Assert.False(builder.IntegratedSecurity);
    }

    [Fact]
    public void DeviceCode_LeavesTheConnectTimeoutAlone()
    {
        /* A deliberate NON-change, pinned because it looks like an omission. The three minutes the
           user is racing come from a CancellationTokenSource the driver builds inside its device-code
           arm and does NOT link to the Connect Timeout-derived one beside it (Extensions.Azure 7.0.2,
           :687-689), so raising this buys nothing - while it would make an unreachable server hang
           for minutes in this mode alone. A Connect Timeout overrun mid-sign-in is absorbed by
           SqlInternalConnectionTds.AttemptRetryADAuthWithTimeoutError, which both interactive modes
           reach through the same arm of GetFedAuthToken.

           Asserted against the SIBLING's value rather than against the number 15, so a change to
           ServerConnection's own default cannot fail this for the wrong reason. */
        var deviceCode = new ServerConnection
        {
            ServerName = "example.database.windows.net",
            AuthenticationType = AuthenticationTypes.EntraDeviceCode,
        };
        var entraMfa = new ServerConnection
        {
            ServerName = "example.database.windows.net",
            AuthenticationType = AuthenticationTypes.EntraMFA,
        };

        Assert.Equal(
            new SqlConnectionStringBuilder(entraMfa.BuildConnectionString(null, null)).ConnectTimeout,
            new SqlConnectionStringBuilder(deviceCode.BuildConnectionString(null, null)).ConnectTimeout);
    }

    // ---- Which modes can raise a window ---------------------------------------------------

    [Fact]
    public void TheDeviceCodeMode_RequiresAnInteractiveSignIn()
    {
        /* Stated positively and on its own, so a sweep that silently stopped covering this mode
           cannot leave it untested — the reason EntraCredentialSelectionModeGateTests carries a
           standalone twin beside its sweep. EntraDefaultCredentialTests holds the exhaustive count. */
        Assert.True(AuthenticationTypes.RequiresInteractiveSignIn(AuthenticationTypes.EntraDeviceCode));
    }

    [Fact]
    public void DeviceCode_NeedsNoStoredCredential()
    {
        /* The credential service is null deliberately: a zero-touch mode returns before it is
           dereferenced, so null proves the short-circuit rather than merely observing a "true" a
           lookup could also have produced. The negative control for this arm lives in
           EntraDefaultCredentialTests.CredentialRequiringModes_StillReachTheCredentialStore. */
        var server = new ServerConnection { AuthenticationType = AuthenticationTypes.EntraDeviceCode };

        Assert.True(ServerConnection.HasStoredCredentials(server, null!, profileLookup: null));
    }

    [Fact]
    public void DeviceCode_DoesNotAttachTheAmbientCredentialListener()
    {
        /* The EntraCredentialSelectionLog listener reads an Azure-Identity event, and this mode never
           touches Azure.Identity at all: the driver serves it from an MSAL public-client application,
           not from a TokenCredential. Attaching the listener here would enable an event source with
           28 other events on it for a mode that raises none of them. The exhaustive sweep is in
           EntraCredentialSelectionModeGateTests; this is the one row that is new. */
        var builder = new SqlConnectionStringBuilder { DataSource = "example-server" };
        ServerConnection.ApplyAuthentication(
            builder,
            AuthenticationTypes.EntraDeviceCode,
            username: "someone",
            password: "a-secret",
            azureClientId: "an-azure-client-id",
            managedIdentityClientId: "a-managed-identity-client-id");

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow, builder.Authentication);
        Assert.Null(PerformanceMonitorLite.Helpers.EntraCredentialSelectionLog.Begin(builder));
    }

    // ---- The rendezvous: Begin ------------------------------------------------------------

    [Fact]
    public void Begin_ReturnsAnAttempt_ForTheDeviceCodeMethodOnly()
    {
        /* Enumerated over the driver's whole SqlAuthenticationMethod enum rather than over this app's
           modes, because Begin is keyed off the BUILDER - the keyword the driver will act on - and a
           future driver mode is exactly the case a list of this app's modes would miss. */
        var devicePin = new SqlConnectionStringBuilder
        {
            Authentication = SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow,
        };

        using (var attempt = EntraDeviceCodeAuth.Begin(devicePin))
        {
            Assert.NotNull(attempt);
        }

        foreach (var method in Enum.GetValues<SqlAuthenticationMethod>()
                                   .Where(m => m != SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow))
        {
            var builder = new SqlConnectionStringBuilder { Authentication = method };
            using var other = EntraDeviceCodeAuth.Begin(builder);
            Assert.Null(other);
        }

        using var fromNull = EntraDeviceCodeAuth.Begin(null);
        Assert.Null(fromNull);
    }

    [Fact]
    public void Begin_IsReachedByTheModeConstant_NotOnlyByTheRawKeyword()
    {
        /* The pin above proves Begin keys off the keyword. This proves the app's own mode arrives at
           that keyword, so the two cannot be right separately and wrong together - which is what a
           mode whose ApplyAuthentication arm was deleted would look like. */
        var builder = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        using var attempt = EntraDeviceCodeAuth.Begin(builder);
        Assert.NotNull(attempt);
    }

    // ---- The rendezvous: the attempt ------------------------------------------------------

    [Fact]
    public void Attempt_CancelCancelsTheTokenTheOpenIsWaitingOn()
    {
        using var attempt = new EntraDeviceCodeAttempt();

        Assert.False(attempt.Token.IsCancellationRequested);

        attempt.Cancel();

        Assert.True(attempt.Token.IsCancellationRequested);
    }

    [Fact]
    public void Attempt_RaisesFinishedExactlyOnce_SoTheWindowClosesOnceAndOnly()
    {
        var attempt = new EntraDeviceCodeAttempt();
        var raised = 0;
        attempt.Finished += () => raised++;

        attempt.Dispose();
        attempt.Dispose();

        /* Once, not twice, and not never. The window unsubscribes in Closing, so a second Finished
           would arrive at nobody - but Close() on an already-closing window is the kind of reentrancy
           that produces an InvalidOperationException in WPF rather than a no-op, and a using inside a
           finally is disposed on paths a reader does not enumerate. */
        Assert.Equal(1, raised);
    }

    [Fact]
    public void Attempt_CancelAfterDisposeDoesNotThrow()
    {
        /* The ordinary race, not an edge case: the connection completes, the caller's using disposes
           the attempt, and the user's click on Cancel lands a moment later. An ObjectDisposedException
           out of a WPF Closing handler would take the window down with it. */
        var attempt = new EntraDeviceCodeAttempt();
        attempt.Dispose();

        attempt.Cancel();
    }

    [Fact]
    public void Attempt_StartsWithNoChallenge()
    {
        /* The window reads Challenge in its constructor, and there is a real window between Begin and
           the driver's callback in which it is null - the first second or so of every attempt, and
           the permanent state of one that failed before reaching the tenant. Pinned so the window's
           null-tolerant reads are not mistaken for defensive noise. */
        using var attempt = new EntraDeviceCodeAttempt();

        Assert.Null(attempt.Challenge);
    }

    [Fact]
    public void Attempt_GivesUpTheCallbackSlotWhenItEnds()
    {
        /* A disposed attempt left owning the slot is worse than an empty slot: the NEXT device-code
           connection publishes its code onto an object whose Finished has already fired and whose
           token source is gone, so the window it opens never closes and its Cancel does nothing.
           Nothing about a single attempt's own lifetime reveals that, which is why it is pinned
           rather than left to the next connection to discover. */
        var builder = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            Assert.False(EntraDeviceCodeAuth.SignInInFlight);

            var attempt = EntraDeviceCodeAuth.Begin(builder);
            Assert.NotNull(attempt);
            Assert.True(EntraDeviceCodeAuth.SignInInFlight);

            attempt!.Dispose();
            Assert.False(EntraDeviceCodeAuth.SignInInFlight);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void Begin_RefusesASecondConcurrentSignIn_RatherThanDisplacingTheFirst()
    {
        /* The finding #3408's review caught, and the reason this is a refusal rather than a warning.
           Three sites call Begin and the collector's runs on a timer, so a collection cycle and a
           user pressing Test genuinely overlap - the collector's own semaphore serializes collectors
           against each other and holds nothing the UI paths take.

           Overwriting the slot in that overlap publishes the EARLIER attempt's code onto the LATER
           attempt's window: the user reads a code, types it into a browser, completes a sign-in for
           a server they did not choose, and watches the one they did choose fail three minutes later
           with nothing on screen to explain it. There is no correlation available to fix it with -
           the driver's callback carries no connection id - so the only honest behaviour is to refuse.

           Asserted in BOTH directions, because a Begin that threw unconditionally would satisfy the
           throw alone: the first claim must succeed, the second must throw, and after the first is
           disposed a third must succeed again. */
        var builder = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            var first = EntraDeviceCodeAuth.Begin(builder);
            Assert.NotNull(first);
            Assert.True(EntraDeviceCodeAuth.SignInInFlight);

            var refused = Assert.Throws<InvalidOperationException>(() => EntraDeviceCodeAuth.Begin(builder));

            /* The message names the remedy, because the remedy is entirely the user's: finish the
               sign-in on screen or close its window. */
            Assert.Equal(EntraDeviceCodeAuth.ConcurrentSignInMessage, refused.Message);
            Assert.Contains("already in progress", refused.Message, StringComparison.OrdinalIgnoreCase);

            /* The refusal must not have taken the slot, or the first attempt's own code would land
               on an object nobody is watching. */
            Assert.True(
                EntraDeviceCodeAuth.SignInInFlight,
                "a refused claim must leave the in-flight attempt holding the slot");

            first!.Dispose();
            Assert.False(EntraDeviceCodeAuth.SignInInFlight);

            /* And the slot is reusable afterwards - a refusal that wedged the mode for the rest of
               the run would be worse than the race it replaced. */
            using var third = EntraDeviceCodeAuth.Begin(builder);
            Assert.NotNull(third);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void TheDialogDoesNotClaimBackgroundCollectionSkipsInteractiveModes()
    {
        /* The second finding from the same review, and it was a user-facing falsehood: the panel said
           "background collection skips these servers", which is true of the CONNECTIVITY SWEEP and
           false of collection. RemoteCollectorService skips an interactive-mode server only once
           UserCancelledMfa is set, so the first cycle after a start does prompt - which is exactly
           how Entra MFA has always worked and is what makes either mode usable for collection.

           Pinned on the dialog AND the README together, because the two disagreed and the README was
           the correct one. Anchored on the panel so a sentence elsewhere in the file cannot stand in
           for it. */
        var xaml = ParitySource.ReadFile("Lite/Windows/AddServerDialog.xaml");

        var panel = xaml.IndexOf("x:Name=\"EntraDeviceCodePanel\"", StringComparison.Ordinal);
        Assert.True(panel >= 0, "the device-code notes panel must exist");

        var end = xaml.IndexOf("</StackPanel>", panel, StringComparison.Ordinal);
        Assert.True(end > panel, "the panel must be closed");

        var text = xaml[panel..end];

        /* The claim that was wrong, in the shape it was written. */
        Assert.DoesNotContain("background collection skips", text, StringComparison.OrdinalIgnoreCase);

        /* And the accurate replacement, which has to say that collection is NOT skipped. Positive
           control: without this, deleting the whole paragraph passes the assertion above. */
        Assert.Contains("data collection does not", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("per run of the app", text, StringComparison.OrdinalIgnoreCase);

        var readme = ParitySource.ReadFile("README.md");
        Assert.Contains("data collection does not skip them", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "Background connectivity sweeps skip these servers rather than raising a code at nobody, collection prompts once",
            readme,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AnAttemptTakesOneChallengeOnly_SoAnUnrelatedCallbackCannotOverwriteIt()
    {
        /* One half of the misattribution class: four sites in Lite open a server connection without
           going through Begin at all, so a device-code callback can arrive while an unrelated
           Begin-owned sign-in holds the slot, and reusing the occupant whenever the slot was
           non-null overwrote ITS code with the unrelated server's - silently, because the unwrapped
           caller never calls Begin and nothing throws.

           This is the narrow guarantee that closes it, and the narrowness is the point: the driver
           invokes its callback once per acquisition, so a challenge arriving at an attempt that
           already has one cannot be that attempt's. What it does NOT settle is whose an arriving
           challenge is when the slot is EMPTY, which is #3409 and is decided before this by Claim.
           Asserted in both directions - the first publish must take, the second must be refused AND
           must leave the original in place. */
        using var attempt = new EntraDeviceCodeAttempt();

        var mine = new EntraDeviceCodeChallenge("MINE-1234", "https://example.invalid/devicelogin");
        var theirs = new EntraDeviceCodeChallenge("THEIRS-9999", "https://example.invalid/other");

        Assert.True(attempt.TryPublish(mine), "the first challenge must publish");
        Assert.Same(mine, attempt.Challenge);

        Assert.False(attempt.TryPublish(theirs), "a second challenge cannot belong to this attempt");

        /* The half that discriminates: a TryPublish that returned false but still assigned would
           satisfy the assertion above and leave the user reading the wrong code. */
        Assert.Same(mine, attempt.Challenge);
        Assert.Equal("MINE-1234", attempt.Challenge!.UserCode);
    }

    [Fact]
    public void ThePromptNamesTheServerItIsSigningInTo()
    {
        /* Two prompts can now legitimately be on screen at once - a sign-in the user started and an
           unattributable one from a background read - so an anonymous window is a trap rather than
           merely terse. Begin records the target off the BUILDER, so the label cannot disagree with
           the connection it belongs to. */
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = "example.database.windows.net",
            InitialCatalog = "mydb",
        };

        Assert.Equal("example.database.windows.net (mydb)", EntraDeviceCodeAuth.DescribeTarget(builder));

        /* No database named, so no parenthetical - and not the word "master", which the connection
           string builder does not put there and the prompt must not invent. */
        var serverOnly = new SqlConnectionStringBuilder { DataSource = "example.database.windows.net" };
        Assert.Equal("example.database.windows.net", EntraDeviceCodeAuth.DescribeTarget(serverOnly));

        /* Nothing to name rather than an empty label. */
        Assert.Null(EntraDeviceCodeAuth.DescribeTarget(new SqlConnectionStringBuilder()));

        /* Credentials must not travel into a window caption. The builder holds them for other modes,
           and this reads two named non-secret keywords rather than rendering the string - so a
           password present on the builder cannot reach the label. */
        var withSecret = new SqlConnectionStringBuilder
        {
            DataSource = "example.database.windows.net",
            UserID = "someone@contoso.com",
            Password = "a-secret-that-must-not-travel",
        };
        var described = EntraDeviceCodeAuth.DescribeTarget(withSecret)!;
        Assert.DoesNotContain("secret", described, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("contoso", described, StringComparison.OrdinalIgnoreCase);

        /* And Begin actually puts it on the attempt, or the window has nothing to read. */
        var deviceCode = new SqlConnectionStringBuilder { DataSource = "example.database.windows.net" };
        ServerConnection.ApplyAuthentication(
            deviceCode, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            using var attempt = EntraDeviceCodeAuth.Begin(deviceCode);
            Assert.Equal("example.database.windows.net", attempt!.Target);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    // ---- #3409: attribution comes from the acquisition, not from the slot ----------------

    /// <summary>
    /// A device-code connection string for <paramref name="server"/>, shaped the way the three
    /// wrapped call sites shape theirs: a builder whose authentication keyword is the one
    /// <c>Begin</c> gates on.
    /// </summary>
    private static SqlConnectionStringBuilder DeviceCodeBuilder(string server, string? database)
    {
        var builder = new SqlConnectionStringBuilder { DataSource = server };
        if (!string.IsNullOrEmpty(database))
        {
            builder.InitialCatalog = database;
        }

        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        return builder;
    }

    /// <summary>
    /// Reads the acquisition identity back after the kinds of await the driver puts between the
    /// provider and the callback — a yield, a thread-pool hop, a timer.
    /// </summary>
    private static async Task<string?> AcquisitionTargetAfterHopsAsync()
    {
        await Task.Yield();
        await Task.Run(() => { }).ConfigureAwait(false);
        await Task.Delay(1).ConfigureAwait(false);
        return EntraDeviceCodeAuth.CurrentAcquisitionTarget;
    }

    /// <summary>
    /// Reads the acquisition identity back on a DEDICATED thread, and reports which thread that
    /// was so the caller can require it was not the calling one.
    ///
    /// <para><b>The awaits above cannot do this job, which was measured rather than assumed.</b>
    /// A thread-pool hop may resume on the thread it started from, so a value that in fact lived on
    /// the THREAD rather than on the async flow can survive those awaits by luck — crippling the
    /// helper above to a single completed await left a <c>[ThreadStatic]</c> implementation of the
    /// identity passing. <c>LongRunning</c> asks the default scheduler for a thread of its own, so
    /// this read happens somewhere a thread-local value cannot be, while an async-local still
    /// arrives because <c>StartNew</c> captures the execution context.</para>
    /// </summary>
    private static async Task<(string? Value, int ThreadId)> AcquisitionTargetOnItsOwnThreadAsync() =>
        await Task.Factory.StartNew(
            () => (EntraDeviceCodeAuth.CurrentAcquisitionTarget, Environment.CurrentManagedThreadId),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).ConfigureAwait(false);

    [Fact]
    public void AnUnrelatedCodeCannotTakeTheSlotOfASignInStillWaitingForItsOwn()
    {
        /* #3409, and the ordering #3408's fix did not close. That fix discriminated on whether the
           slot holder ALREADY HELD a challenge, which answers "can this be the code of the attempt
           holding the slot" - but an attempt that has not yet received its own code holds an empty
           slot too, and an empty slot is equally consistent with "the owner's code has arrived" and
           with "an unrelated connection's code arrived first".

           In that second ordering the old decision published the stray onto the owner and the
           prompt named the owner's server beside the stray's code: authoritative and wrong, which
           is worse than anonymous, because it tells a user confidently to authenticate something
           they did not choose.

           The discriminator now is the identity of the acquisition that produced the challenge,
           which EntraDeviceCodeProvider records one frame above the callback. Asserted in BOTH
           directions in one test, because a Claim that refused everything would satisfy the
           refusal half alone. */
        var mine = DeviceCodeBuilder("mine.example.invalid", "mydb");

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            using var owner = EntraDeviceCodeAuth.Begin(mine);

            Assert.NotNull(owner);
            Assert.Equal("mine.example.invalid (mydb)", owner!.Target);

            /* The ordering's premise, and without it this test is the one #3408 already passes. */
            Assert.Null(owner.Challenge);

            var theirs = new EntraDeviceCodeChallenge("THEIRS-9999", "https://example.invalid/other");
            var shown = EntraDeviceCodeAuth.Claim(theirs, "other.example.invalid", out var unowned);

            Assert.True(unowned, "a code from another connection must not take a waiting sign-in's slot");
            Assert.NotSame(owner, shown);

            /* The slot is still the owner's, so the owner's own code can still land in it. Without
               this, a Claim that consumed the slot and merely relabelled would pass the assertions
               above and strand the user's own sign-in. */
            Assert.Null(owner.Challenge);

            Assert.Same(theirs, shown.Challenge);

            /* And the prompt names the stray's OWN server. Not the owner's, which is the defect,
               and not nothing, which would be the cheap fix. */
            Assert.Equal("other.example.invalid", shown.Target);
            Assert.DoesNotContain("mine", shown.Target!, StringComparison.OrdinalIgnoreCase);

            /* The legitimate direction, which is what makes the refusal above evidence rather than
               a blanket: the owner's own code must take the slot it is waiting on. */
            var own = new EntraDeviceCodeChallenge("MINE-1234", "https://example.invalid/devicelogin");
            var ownShown = EntraDeviceCodeAuth.Claim(
                own, EntraDeviceCodeAuth.DescribeTarget(mine), out var ownUnowned);

            Assert.False(ownUnowned, "a sign-in's own code must take the slot it is waiting on");
            Assert.Same(owner, ownShown);
            Assert.Same(own, owner.Challenge);
            Assert.Equal("mine.example.invalid (mydb)", owner.Target);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void ACodeWithNoAcquisitionIdentityIsShownUnnamedRatherThanBorrowingAName()
    {
        /* The fail-safe, and the direction this whole mechanism has to fail in. The acquisition
           identity travels on an async-local set by Lite's own provider; if the driver ever refuses
           that provider, or an execution context stops reaching the callback, the identity is
           absent rather than wrong. Absent must mean "show it unnamed", never "assume it is the
           waiting sign-in's" - which is the defect with an extra step.

           Paired with its positive control: the same slot, the same waiting owner, and an identity
           that DOES name the owner is claimed. Without that, a Claim hard-wired to refuse passes. */
        var mine = DeviceCodeBuilder("mine.example.invalid", "mydb");

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            using var owner = EntraDeviceCodeAuth.Begin(mine);
            Assert.NotNull(owner);

            var unidentified = new EntraDeviceCodeChallenge("NOID-4321", "https://example.invalid/devicelogin");
            var shown = EntraDeviceCodeAuth.Claim(unidentified, null, out var unowned);

            Assert.True(unowned);
            Assert.NotSame(owner, shown);
            Assert.Null(owner!.Challenge);

            /* Unnamed, so the window says it is a background connection rather than naming a server
               nothing established. */
            Assert.Null(shown.Target);
            Assert.Same(unidentified, shown.Challenge);

            var identified = new EntraDeviceCodeChallenge("MINE-1234", "https://example.invalid/devicelogin");
            Assert.Same(
                owner,
                EntraDeviceCodeAuth.Claim(
                    identified, EntraDeviceCodeAuth.DescribeTarget(mine), out _));
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void ACodeThatNamesTheSlotHolderStillCannotOverwriteAChallengeAlreadyThere()
    {
        /* #3408's ordering, re-asserted through the new decision path so the older guarantee cannot
           be lost while closing the newer one. The driver invokes its callback once per
           acquisition, so a second challenge naming the same target is a second acquisition to the
           same server - the one pair no identity separates - and overwriting would replace a code
           the user may already be typing.

           Asserted on the OCCUPANT as well as on the return value: a Claim that returned a fresh
           attempt while still assigning over the occupant would satisfy the first half and leave
           the user reading a dead code. */
        var mine = DeviceCodeBuilder("mine.example.invalid", "mydb");
        var target = EntraDeviceCodeAuth.DescribeTarget(mine);

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            using var owner = EntraDeviceCodeAuth.Begin(mine);
            Assert.NotNull(owner);

            var first = new EntraDeviceCodeChallenge("FIRST-1111", "https://example.invalid/devicelogin");
            Assert.Same(owner, EntraDeviceCodeAuth.Claim(first, target, out var firstUnowned));
            Assert.False(firstUnowned);

            var second = new EntraDeviceCodeChallenge("SECOND-2222", "https://example.invalid/devicelogin");
            var secondShown = EntraDeviceCodeAuth.Claim(second, target, out var secondUnowned);

            Assert.True(secondUnowned);
            Assert.NotSame(owner, secondShown);
            Assert.Same(first, owner!.Challenge);
            Assert.Equal("FIRST-1111", owner.Challenge!.UserCode);

            /* Still named, because the identity is still known - a refused claim does not make a
               code anonymous. */
            Assert.Same(second, secondShown.Challenge);
            Assert.Equal(target, secondShown.Target);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void ACodeWithNoSignInWaitingIsNamedFromItsOwnAcquisition()
    {
        /* The route the four unwrapped sites take - a plan fetch, an MCP read, a Query Store
           backfill, the excluded-databases picker. Nothing called Begin, so there is no slot to
           take and never was; what changed is that the prompt is no longer anonymous, because the
           provider knows which server the acquisition is for even when no call site announced it.

           The anonymous case is kept and asserted beside it: no slot AND no identity is the only
           state left that the window labels as a background connection. */
        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            Assert.False(EntraDeviceCodeAuth.SignInInFlight);

            var challenge = new EntraDeviceCodeChallenge("BKGD-7777", "https://example.invalid/devicelogin");
            var named = EntraDeviceCodeAuth.Claim(challenge, "reader.example.invalid (plans)", out var unowned);

            Assert.True(unowned);
            Assert.Equal("reader.example.invalid (plans)", named.Target);
            Assert.Same(challenge, named.Challenge);

            /* A claim that never held the slot must not take it on the way out, or the next
               wrapped sign-in publishes into a disposed object. */
            Assert.False(EntraDeviceCodeAuth.SignInInFlight);

            var anonymous = EntraDeviceCodeAuth.Claim(
                new EntraDeviceCodeChallenge("BKGD-8888", "https://example.invalid/devicelogin"),
                null,
                out var anonymousUnowned);

            Assert.True(anonymousUnowned);
            Assert.Null(anonymous.Target);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void EveryRouteToAPrompt_NamesTheServerOfTheCodesOwnAcquisition()
    {
        /* The enumeration, asserted as ONE invariant rather than route by route, because the routes
           differ only in what happens to the SLOT and the label is what the user acts on. Every way
           a challenge can reach a window is a combination of two things: what state the rendezvous
           slot is in, and what the acquisition that produced the challenge is for. This walks all of
           them and requires the same property of each - the prompt names the server of the
           acquisition that produced its code, or names nothing when no acquisition identity
           reached the callback.

           It covers a case the individual pins do not, and one the identity cannot separate: an
           unwrapped read of the server a user is already signing in to. That challenge DOES take
           the waiting attempt's slot, because two acquisitions for one server describe identically.
           The invariant still holds - both prompts name that server, both codes are that server's -
           and pinning the invariant rather than the slot outcome is deliberate, so this does not
           freeze a limitation in place. */
        var mine = DeviceCodeBuilder("mine.example.invalid", "mydb");
        var mineTarget = EntraDeviceCodeAuth.DescribeTarget(mine);

        var routes = new (string Name, bool Owner, bool OwnerHoldsAChallenge, string? Acquired)[]
        {
            ("no sign-in waiting, a named background read", false, false, "reader.example.invalid"),
            ("no sign-in waiting, no identity at all", false, false, null),
            ("a sign-in waiting, its own code", true, false, mineTarget),
            ("a sign-in waiting, another server's code", true, false, "other.example.invalid"),
            ("a sign-in waiting, no identity at all", true, false, null),
            ("a sign-in holding its code, its own server again", true, true, mineTarget),
            ("a sign-in holding its code, another server's code", true, true, "other.example.invalid"),
            ("a sign-in holding its code, no identity at all", true, true, null),
        };

        /* Population floor: the walk below is vacuous if the table ever empties, and a table that
           lost its two unidentified rows would stop covering the fail-safe entirely. */
        Assert.Equal(8, routes.Length);
        Assert.Equal(3, routes.Count(route => route.Acquired is null));

        foreach (var route in routes)
        {
            EntraDeviceCodeAuth.ResetForTests();
            try
            {
                EntraDeviceCodeAttempt? owner = null;
                if (route.Owner)
                {
                    owner = EntraDeviceCodeAuth.Begin(mine);
                    Assert.NotNull(owner);

                    if (route.OwnerHoldsAChallenge)
                    {
                        EntraDeviceCodeAuth.Claim(
                            new EntraDeviceCodeChallenge("HELD-0000", "https://example.invalid/devicelogin"),
                            mineTarget,
                            out var heldUnowned);
                        Assert.False(heldUnowned, route.Name);
                        Assert.NotNull(owner!.Challenge);
                    }
                }

                var challenge = new EntraDeviceCodeChallenge("CODE-1234", "https://example.invalid/devicelogin");
                var shown = EntraDeviceCodeAuth.Claim(challenge, route.Acquired, out _);

                /* The invariant. Not "the label is non-null" and not "the label is the owner's" -
                   the label is the CODE'S, on every route. */
                Assert.Equal(route.Acquired, shown.Target);
                Assert.Same(challenge, shown.Challenge);

                owner?.Dispose();
            }
            finally
            {
                EntraDeviceCodeAuth.ResetForTests();
            }
        }
    }

    [Fact]
    public async Task TheAcquisitionIdentityReachesWhatTheAcquisitionAwaits()
    {
        /* The mechanism the attribution rests on, exercised rather than assumed. The provider
           records the identity and then AWAITS the driver's acquisition; MSAL awaits the device-code
           callback inside that acquisition (one await in DeviceCodeRequest.ExecuteAsync,
           Microsoft.Identity.Client 4.84.2), so the value has to survive the awaits in between.
           This asserts that property of the runtime through the same shapes - a yield, a thread-pool
           hop, a timer - rather than through MSAL, which needs a tenant.

           ConfigureAwait(false) is used deliberately in the helper: it governs the synchronization
           context and not the execution context an async-local lives in, and a reader who believed
           otherwise would conclude this cannot work. */
        EntraDeviceCodeAuth.ResetForTests();

        Assert.Null(EntraDeviceCodeAuth.CurrentAcquisitionTarget);

        string? inside;
        using (EntraDeviceCodeAuth.EnterAcquisition("scoped.example.invalid (db)"))
        {
            Assert.Equal("scoped.example.invalid (db)", EntraDeviceCodeAuth.CurrentAcquisitionTarget);
            inside = await AcquisitionTargetAfterHopsAsync();

            /* The discriminating read. The hops above are the SHAPE the driver uses; this is the
               one that can tell an async-local apart from a thread-local, because a dedicated
               thread has never held either. The thread id is asserted to differ as well, or a
               scheduler that inlined the delegate would make the check vacuous. */
            var (elsewhere, elsewhereThread) = await AcquisitionTargetOnItsOwnThreadAsync();

            Assert.NotEqual(Environment.CurrentManagedThreadId, elsewhereThread);
            Assert.Equal("scoped.example.invalid (db)", elsewhere);
        }

        Assert.Equal("scoped.example.invalid (db)", inside);

        /* And it does not outlive its acquisition. A value left behind would attribute the NEXT
           connection's code to the previous connection - the same defect, one cycle later - so the
           closing half is asserted through the same hops as the opening one. */
        Assert.Null(EntraDeviceCodeAuth.CurrentAcquisitionTarget);
        Assert.Null(await AcquisitionTargetAfterHopsAsync());
        Assert.Null((await AcquisitionTargetOnItsOwnThreadAsync()).Value);

        /* Nested scopes restore rather than clear, so an inner acquisition cannot leave its parent
           looking like no acquisition at all. */
        using (EntraDeviceCodeAuth.EnterAcquisition("outer.example.invalid"))
        {
            using (EntraDeviceCodeAuth.EnterAcquisition("inner.example.invalid"))
            {
                Assert.Equal("inner.example.invalid", EntraDeviceCodeAuth.CurrentAcquisitionTarget);
            }

            Assert.Equal("outer.example.invalid", EntraDeviceCodeAuth.CurrentAcquisitionTarget);
        }

        Assert.Null(EntraDeviceCodeAuth.CurrentAcquisitionTarget);
    }

    [Fact]
    public void Register_InstallsLitesOwnProviderAndTheDriverAcceptsIt()
    {
        /* Verified by STATE, not by the call. SqlAuthenticationProviderManager.SetProvider asks
           IsSupported and REFUSES a provider that says no (Microsoft.Data.SqlClient 7.0.2 and
           7.0.3), leaving the driver's own callback installed - which writes the code to a console
           a WPF process does not have. So "Register called SetProvider" is not evidence the driver
           took it, and the whole feature can be dead with nothing on screen. */
        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            Assert.True(EntraDeviceCodeAuth.Register(_ => { }));

            var installed = SqlAuthenticationProvider.GetProvider(
                SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow);

            Assert.NotNull(installed);
            Assert.IsType<EntraDeviceCodeProvider>(installed);

            Assert.True(
                installed!.IsSupported(SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow),
                "the provider must support the method it is installed against or the driver refuses it");

            /* Negative control: IsSupported must ANSWER rather than agree. A forwarding override
               replaced by `=> true` passes the assertion above and would claim methods the inner
               provider cannot serve. */
            Assert.False(installed.IsSupported(SqlAuthenticationMethod.NotSpecified));
            Assert.False(installed.IsSupported(SqlAuthenticationMethod.SqlPassword));

            /* And the methods the inner provider does serve are still served, so the forwarding is
               not a device-code special case. */
            Assert.True(installed.IsSupported(SqlAuthenticationMethod.ActiveDirectoryInteractive));
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void TheProviderRecordsTheAcquisitionBeforeRunningIt()
    {
        /* The wiring, in source, because reaching it behaviourally needs a tenant: an acquisition
           that gets as far as MSAL needs an authority to talk to. Three things have to hold
           together, and each is its own silent failure - an identity read off the wrong source
           labels every prompt with the same string, a scope disposed before the inner task is
           awaited labels every prompt with nothing, and an IsSupported that answers from a literal
           gets the whole provider refused. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Services/EntraDeviceCodeAuth.cs"));

        var at = code.IndexOf("class EntraDeviceCodeProvider", StringComparison.Ordinal);
        Assert.True(at >= 0, "the provider that records each acquisition must exist");

        var provider = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', at));

        var acquire = provider.IndexOf(
            "override async Task<SqlAuthenticationToken>", StringComparison.Ordinal);
        Assert.True(acquire >= 0, "the provider must override the acquisition it wraps");

        var body = CSharpSourceWalker.BraceBalanced(provider, provider.IndexOf('{', acquire));

        /* Off the driver's own parameters for THIS acquisition, which is the only per-connection
           fact anywhere in the call path. */
        Assert.Contains(
            "DescribeTarget" + "(parameters.ServerName, parameters.DatabaseName)",
            body,
            StringComparison.Ordinal);

        var enter = body.IndexOf("EnterAcquisition" + "(", StringComparison.Ordinal);
        Assert.True(enter >= 0, "the acquisition's identity must be recorded for the callback to read");

        /* AWAITED inside the scope rather than returned from it. The callback fires partway through
           the inner task, so `return _inner.AcquireTokenAsync(parameters);` would dispose the scope
           first and leave every prompt unnamed - a change that compiles, passes a source pin
           looking only for the two calls, and silently degrades the feature to the old behaviour. */
        var delegated = body.IndexOf("await _inner." + "AcquireTokenAsync(", StringComparison.Ordinal);
        Assert.True(delegated >= 0, "the wrapped acquisition must be awaited inside the scope");
        Assert.True(enter < delegated, "the acquisition must be recorded before it is run");

        var supported = provider.IndexOf("IsSupported(SqlAuthenticationMethod", StringComparison.Ordinal);
        Assert.True(supported >= 0, "the provider must answer IsSupported or the driver refuses it");
        Assert.Contains(
            "_inner.IsSupported" + "(",
            provider[supported..Math.Min(provider.Length, supported + 200)],
            StringComparison.Ordinal);
    }

    [Fact]
    public void TheCallbackAttributesThroughClaimRatherThanTakingTheSlotItself()
    {
        /* OnDeviceCodeIssued is private and its parameter type has no public constructor, so the
           callback cannot be driven from a test - the behaviour above is pinned on Claim, and this
           pins that the callback is wired to it. Two halves: the callback must read the acquisition
           identity, and it must not publish onto anything itself. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Services/EntraDeviceCodeAuth.cs"));

        var at = code.IndexOf(
            "OnDeviceCodeIssued(Microsoft.Identity.Client.DeviceCodeResult", StringComparison.Ordinal);
        Assert.True(at >= 0, "the driver callback must exist");

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', at));

        Assert.Contains("CurrentAcquisition" + "Target", body, StringComparison.Ordinal);
        Assert.Contains("Claim" + "(challenge, acquired, out var unowned)", body, StringComparison.Ordinal);

        /* And the decision is NOT taken here. A callback that published for itself could publish
           onto the slot holder without asking whose the code is, which is the whole defect. */
        Assert.DoesNotContain("TryPublish", body, StringComparison.Ordinal);

        var claim = code.IndexOf("EntraDeviceCodeAttempt Claim(", StringComparison.Ordinal);
        Assert.True(claim >= 0, "the attribution decision must live in Claim");

        var claimBody = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', claim));

        var vouch = claimBody.IndexOf("var vouched", StringComparison.Ordinal);
        Assert.True(vouch >= 0, "Claim must decide whether the acquisition names the slot holder");

        var publish = claimBody.IndexOf("TryPublish", StringComparison.Ordinal);
        Assert.True(publish >= 0, "Claim must still refuse a slot that already holds a challenge");

        /* The ORDER is the fix. Asking TryPublish first and the identity second - or asking the
           identity and ignoring the answer - is #3408's decision with extra code. */
        Assert.True(vouch < publish, "the acquisition must be checked before the slot is");
        Assert.Contains(
            "acquiredTarget",
            claimBody[vouch..Math.Min(claimBody.Length, vouch + 260)],
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("srv.example.invalid", "mydb", "srv.example.invalid (mydb)")]
    [InlineData("srv.example.invalid", "", "srv.example.invalid")]
    [InlineData("tcp:srv.example.invalid,1433", "mydb", "tcp:srv.example.invalid,1433 (mydb)")]
    [InlineData("srv.example.invalid\\SQL2022", "", "srv.example.invalid\\SQL2022")]
    [InlineData("", "mydb", null)]
    public void BothSourcesOfATargetNameDescribeItIdentically(
        string server, string database, string? expected)
    {
        /* Begin describes the builder a caller is about to open; the provider describes the
           SqlAuthenticationParameters the driver hands it; and a challenge takes a waiting
           attempt's slot only when those two strings AGREE. Two spellings of the same server would
           make that comparison fail on correct code - costing every prompt its Cancel - so there is
           one formatter and this asserts the builder path routes through it rather than carrying a
           second copy.

           The shapes are the ones that would expose a divergence if one existed: a port suffix, a
           named instance, a backslash. */
        Assert.Equal(expected, EntraDeviceCodeAuth.DescribeTarget(server, database));

        var builder = new SqlConnectionStringBuilder();
        if (server.Length > 0)
        {
            builder.DataSource = server;
        }

        if (database.Length > 0)
        {
            builder.InitialCatalog = database;
        }

        Assert.Equal(expected, EntraDeviceCodeAuth.DescribeTarget(builder));

        /* The round trip the comparison actually depends on, measured rather than reasoned about:
           Begin reads the builder, the DRIVER re-parses the connection string text, and SqlClient
           hands the provider what it parsed - ConnectionOptions.DataSource and
           ConnectionOptions.InitialCatalog, which are those keywords with nothing but a length
           check applied (Microsoft.Data.SqlClient 7.0.2 and 7.0.3). A driver that began normalising
           them would fail here rather than in the field. */
        Assert.Equal(
            expected,
            EntraDeviceCodeAuth.DescribeTarget(new SqlConnectionStringBuilder(builder.ConnectionString)));
    }

    [Fact]
    public void TargetIsOnlyEverAssignedFromAnAcquisition()
    {
        /* The invariant the label rests on: every value Target holds is acquisition-derived, either
           read off the acquisition directly or verified to agree with it. There are exactly two
           assignments in the service and none anywhere else in Lite; a third is how a slot-derived
           label comes back, and it would be invisible in behaviour until the racing ordering
           happened to a user. */
        var service = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Services/EntraDeviceCodeAuth.cs"));

        var assignment = new Regex(@"\bTarget\s*=(?!=)", RegexOptions.Compiled);

        Assert.Equal(2, assignment.Matches(service).Count);

        /* Both of them inside the two methods entitled to set one, so two assignments in the wrong
           places cannot satisfy the count. */
        var begin = service.IndexOf("Begin(SqlConnectionStringBuilder", StringComparison.Ordinal);
        Assert.True(begin >= 0, "Begin must exist to record a caller's own target");
        Assert.True(
            assignment.IsMatch(CSharpSourceWalker.BraceBalanced(service, service.IndexOf('{', begin))),
            "Begin must record the target of the builder its caller is about to open");

        var claim = service.IndexOf("EntraDeviceCodeAttempt Claim(", StringComparison.Ordinal);
        Assert.True(claim >= 0, "Claim must exist to name an attempt it creates");
        Assert.True(
            assignment.IsMatch(CSharpSourceWalker.BraceBalanced(service, service.IndexOf('{', claim))),
            "Claim must name the attempt it creates from the acquisition");

        /* And nowhere else under Lite, because the setter is internal rather than private. */
        foreach (var file in Directory.GetFiles(
                     Path.Combine(ParitySource.RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(ParitySource.RepoRoot(), file).Replace('\\', '/');
            if (relative.Contains("/obj/", StringComparison.Ordinal) ||
                relative.Contains("/bin/", StringComparison.Ordinal) ||
                relative.Equals("Lite/Services/EntraDeviceCodeAuth.cs", StringComparison.Ordinal))
            {
                continue;
            }

            var other = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            if (!other.Contains("EntraDeviceCodeAttempt", StringComparison.Ordinal))
            {
                continue;
            }

            Assert.False(
                assignment.IsMatch(other),
                $"{relative} touches an attempt and assigns a Target; only the service may");
        }
    }

    [Fact]
    public void NoDocStillExplainsTheMisattributionByCallingItUnfixable()
    {
        /* #3408's review caught prose that had gone stale inside the change that made it stale, and
           this is the same hazard one level on. Both the service and the README explained
           misattribution by saying no correlation exists anywhere. One does - the driver hands every
           authentication provider the server, database and connection id of the acquisition it is
           running - so an unqualified "cannot be told apart" is now false, and false in a
           user-facing doc.

           Each absence is paired with the statement that replaced it, because deleting a paragraph
           satisfies a bare DoesNotContain. */
        var readme = ParitySource.ReadFile("README.md");

        Assert.DoesNotContain(
            "The driver's callback carries no connection id, so two at once cannot be told apart",
            readme,
            StringComparison.Ordinal);

        Assert.Contains(
            "Every code window names the server its code is for", readme, StringComparison.Ordinal);
        Assert.Contains(
            "Two sign-ins to the same server describe identically", readme, StringComparison.Ordinal);

        var service = ParitySource.ReadFile("Lite/Services/EntraDeviceCodeAuth.cs");

        Assert.DoesNotContain("no key to route on even in principle", service, StringComparison.Ordinal);
        Assert.DoesNotContain("The constraint is unfixable", service, StringComparison.Ordinal);

        /* And it names where the provenance does live, so a reader is not left with the old
           conclusion and no replacement. */
        Assert.Contains("the provenance the callback lacks is one", service, StringComparison.Ordinal);
    }

    // ---- The challenge must not carry the secret half ------------------------------------

    [Fact]
    public void TheChallengeDoesNotCarryTheDeviceCode()
    {
        /* DeviceCodeResult.DeviceCode is the value that redeems the token: whoever holds it can
           complete the sign-in, and it is not the code shown to the user. It must not cross into a
           type this app's windows, logs and crash dumps can reach.

           The POSITIVE CONTROL comes first and is what makes the absence evidence of anything: the
           same probe is run against the driver's own result type, which DOES declare that property.
           Without it, "no member named DeviceCode" is equally true of a type with no members, of a
           renamed property, and of a probe that was looking at the wrong type. */
        var driverResult = Type.GetType(
            "Microsoft.Identity.Client.DeviceCodeResult, Microsoft.Identity.Client", throwOnError: false);

        Assert.NotNull(driverResult);
        Assert.NotNull(driverResult!.GetProperty("DeviceCode"));
        Assert.NotNull(driverResult.GetProperty("UserCode"));

        var challenge = typeof(EntraDeviceCodeChallenge);

        Assert.Null(challenge.GetProperty("DeviceCode"));

        /* And the members it DOES have are exactly the two the window uses, so a later addition is a
           decision rather than a copy of the driver's type. Every other member of DeviceCodeResult -
           Message, ExpiresOn, Interval, ClientId, Scopes - is absent because nothing reads it; see
           EntraDeviceCodeChallenge for why showing ExpiresOn would be showing the wrong number. */
        Assert.Equal(
            new[] { "UserCode", "VerificationUrl" },
            challenge.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                     .Select(property => property.Name)
                     .OrderBy(name => name, StringComparer.Ordinal)
                     .ToArray());
    }

    [Fact]
    public void NeitherTheServiceNorTheWindowReadsTheDeviceCode()
    {
        /* The type-level pin above stops the secret being STORED. This stops it being read straight
           off the driver's result and used without storing it - a log line, a window caption, a
           clipboard write. Comments and literals are stripped, so the comments in those files that
           discuss DeviceCode by name cannot satisfy or break this.

           The searched token is built by concatenation, so this assertion's own text is not a
           candidate match for itself - the failure mode where a source pin spelled as one literal
           passes with the call site deleted. */
        /* A word BOUNDARY, not a prefix. ".DeviceCode" alone also matches ".DeviceCodeResult" -
           which is the parameter type of the callback and must stay - so the naive spelling of this
           pin fails on correct code, which is how it was caught. The token is assembled rather than
           written whole so this assertion's own text is not a candidate match for itself. */
        var needle = new Regex(Regex.Escape("." + "DeviceCode") + "(?![A-Za-z0-9_])", RegexOptions.Compiled);
        var reads = new Regex(Regex.Escape("." + "UserCode") + "(?![A-Za-z0-9_])", RegexOptions.Compiled);

        foreach (var path in new[]
                 {
                     "Lite/Services/EntraDeviceCodeAuth.cs",
                     "Lite/Windows/EntraDeviceCodeWindow.xaml.cs",
                 })
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(path));

            /* Positive control per file: the property that IS read must be found by an identically
               shaped probe, so "not found" cannot mean "the strip ate everything", "the path is
               wrong", or "the boundary assertion never matches anything". */
            Assert.True(reads.IsMatch(code), $"{path} must read the user code through the same shape");
            Assert.False(needle.IsMatch(code), $"{path} must not read the device code");
        }
    }

    // ---- The provider registration --------------------------------------------------------

    [Fact]
    public void Register_InstallsTheCallbackAgainstTheDeviceCodeMethod()
    {
        /* Three calls have to be present together, and each is a separate way for this feature to be
           silently dead: a provider with no callback writes the code to a console this process does
           not have, a callback installed against the wrong method never fires, and a registration
           that never calls SetProvider leaves the driver's default in place. Searched as
           concatenations so no token here is its own match. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Services/EntraDeviceCodeAuth.cs"));

        var register = code.IndexOf("Register" + "(Action<", StringComparison.Ordinal);
        Assert.True(register >= 0, "EntraDeviceCodeAuth.Register is the registration entry point and must exist");

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', register));

        Assert.Contains("SetDeviceCodeFlow" + "Callback(", body, StringComparison.Ordinal);
        Assert.Contains("SqlAuthenticationProvider." + "SetProvider(", body, StringComparison.Ordinal);
        Assert.Contains("SqlAuthenticationMethod." + "ActiveDirectoryDeviceCodeFlow", body, StringComparison.Ordinal);
    }

    [Fact]
    public void Register_IsIdempotentAndRejectsANullPresenter()
    {
        Assert.Throws<ArgumentNullException>(() => EntraDeviceCodeAuth.Register(null!));

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            Assert.True(EntraDeviceCodeAuth.Register(_ => { }));
            Assert.False(EntraDeviceCodeAuth.Register(_ => { }));
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
    }

    [Fact]
    public void TheAppRegistersTheDeviceCodePresenterAtStartup()
    {
        /* A provider nothing registers is a mode that waits three minutes and fails. The startup call
           cannot be reached behaviourally - App.OnStartup needs a WPF application - so it is asserted
           in source, beside the Entra MFA registration it sits next to.

           The presenter is asserted to MARSHAL rather than merely to exist: the driver awaits this
           callback before it starts polling, so a blocking Invoke would spend the user's own deadline
           waiting for the dispatcher, and BeginInvoke is the difference. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile("Lite/App.xaml.cs"));

        Assert.Contains(
            "EntraDeviceCodeAuth." + "Register(ShowDeviceCodePrompt)", code, StringComparison.Ordinal);

        var presenter = code.IndexOf("ShowDeviceCodePrompt(Services.", StringComparison.Ordinal);
        Assert.True(presenter >= 0, "the presenter method must exist to be registered");

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', presenter));

        Assert.Contains("Begin" + "Invoke(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("dispatcher." + "Invoke(", body, StringComparison.Ordinal);
        Assert.Contains("EntraDeviceCodeWindow(attempt)", body, StringComparison.Ordinal);
    }

    // ---- The window's two directions ------------------------------------------------------

    [Fact]
    public void TheWindowClosesWhenTheAttemptEnds_AndCancelsItWhenTheUserCloses()
    {
        /* Both directions, because either one missing leaves a specific stuck state: without the
           first, a window survives a connection that already succeeded and shows a dead code; without
           the second, closing the window returns nothing to the user and the dialog stays disabled
           until the driver gives up. Neither is reachable without a dispatcher. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Windows/EntraDeviceCodeWindow.xaml.cs"));

        Assert.Contains("attempt." + "Finished += OnAttemptFinished", code, StringComparison.Ordinal);

        var finished = code.IndexOf("OnAttemptFinished()", StringComparison.Ordinal);
        Assert.True(finished >= 0, "the attempt-ended handler must exist");
        var finishedBody = CSharpSourceWalker.BraceBalanced(
            code, code.IndexOf('{', code.IndexOf("private void OnAttemptFinished()", StringComparison.Ordinal)));
        Assert.Contains("Close" + "()", finishedBody, StringComparison.Ordinal);

        var closing = code.IndexOf("Window_Closing(object", StringComparison.Ordinal);
        Assert.True(closing >= 0, "the Closing handler is where the cancellation lives and must exist");
        var closingBody = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', closing));
        Assert.Contains("_attempt." + "Cancel()", closingBody, StringComparison.Ordinal);

        /* And the close from the attempt's side is GUARDED, so a queued BeginInvoke that lands after
           the user has already closed the window does not issue a second Close(). Measured, that
           second call is currently discarded by Window.InternalClose's `if (_disposed) return;` -
           but that is a fact about undocumented internals in a framework this app does not version,
           and no test can reach the race to notice a change. The guard is asserted to be read INSIDE
           the handler, not merely present in the file, because a flag assigned and never tested is
           the shape this pin exists to catch. */
        var guardAssigned = code.IndexOf("_closed = true", StringComparison.Ordinal);
        Assert.True(guardAssigned >= 0, "Window_Closing must record that the window has closed");

        Assert.Contains("if (_closed)", finishedBody, StringComparison.Ordinal);
        Assert.True(
            finishedBody.IndexOf("if (_closed)", StringComparison.Ordinal)
                < finishedBody.IndexOf("Close" + "()", StringComparison.Ordinal),
            "the guard must be read BEFORE the close it guards");

        /* The cancellation lives in Closing rather than in the button handler, so the button, Escape
           and the title-bar X all reach it. A Cancel() in the click handler INSTEAD would leave two
           of those three routes closing the window without ending the attempt. */
        var click = code.IndexOf("Cancel_Click(object", StringComparison.Ordinal);
        Assert.True(click >= 0, "the Cancel button handler must exist");
        Assert.DoesNotContain(
            "Cancel" + "()", code[click..(click + 120)], StringComparison.Ordinal);
    }

    [Fact]
    public void TheWindowDoesNotUseIsCancel_WhichWouldThrowOnAModelessWindow()
    {
        /* IsCancel on a button routes through Window.OnDialogCancel, whose dialog branch assigns
           DialogResult - and assigning DialogResult on a window that was never shown with ShowDialog
           throws InvalidOperationException. This window is modeless by design, so the property is a
           crash waiting for the first user who presses Escape.

           Read off the XAML, because that is where the property would be set, and paired with a
           positive control: the file must be the one holding this window's buttons. */
        var xaml = ParitySource.ReadFile("Lite/Windows/EntraDeviceCodeWindow.xaml");

        Assert.Contains("Click=\"Cancel_Click\"", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("IsCancel=\"True\"", xaml, StringComparison.Ordinal);

        /* Shown with Show(), not ShowDialog(), which is the premise the line above rests on. */
        var presenter = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/App.xaml.cs"));
        Assert.Contains("prompt." + "Show()", presenter, StringComparison.Ordinal);
        Assert.DoesNotContain("prompt." + "ShowDialog()", presenter, StringComparison.Ordinal);
    }

    // ---- Every gate that asks whether a mode is interactive -------------------------------

    [Theory]
    [InlineData("Lite/Services/ServerManager.cs", "!allowInteractiveAuth")]
    [InlineData("Lite/Services/RemoteCollectorService.cs", "bool isInteractiveServer")]
    [InlineData("Lite/Windows/AddMultipleServersDialog.xaml.cs", "BuildServerConnection(BulkServerParseLine")]
    [InlineData("Lite/MainWindow.xaml.cs", "var currentStatus =")]
    public void EveryInteractiveModeGate_AsksTheSharedQuestion(string path, string anchor)
    {
        /* Four gates decided "is this mode interactive" by comparing against EntraMFA, so a new
           interactive mode inherited its behaviour from an omission rather than from a decision. Each
           is pinned here with its anchor asserted to EXIST first: without that this degrades into
           "the file does not contain a string", which a deleted gate also satisfies.

           The window searched is the anchor plus the statements that follow it rather than the whole
           file, so a RequiresInteractiveSignIn call somewhere else in the same file cannot stand in
           for this one. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(path));

        var at = code.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{path} must still contain the gate anchored at '{anchor}'");

        var window = code[at..Math.Min(code.Length, at + 600)];

        Assert.Contains("RequiresInteractiveSignIn", window, StringComparison.Ordinal);
    }

    [Fact]
    public void NoInteractiveModeGateStillComparesAgainstEntraMfaAlone()
    {
        /* The complement of the theory above, and the half that would have caught the original
           defect. That theory asks whether the shared question is present; this asks whether the
           equality test it replaced is gone from the four windows, because BOTH can be true at once
           and the equality test is what goes stale.

           The EntraMFA comparisons that remain in these files are legitimately mode-specific - the
           MFA username Credential Manager arms, the radio-to-mode mappings, the display switch - so
           this is scoped to the same windows the theory pins rather than to whole files. */
        var gates = new (string Path, string Anchor)[]
        {
            ("Lite/Services/ServerManager.cs", "!allowInteractiveAuth"),
            ("Lite/Services/RemoteCollectorService.cs", "bool isInteractiveServer"),
            ("Lite/Windows/AddMultipleServersDialog.xaml.cs", "BuildServerConnection(BulkServerParseLine"),
            ("Lite/MainWindow.xaml.cs", "var currentStatus ="),
        };

        foreach (var (path, anchor) in gates)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(path));
            var at = code.IndexOf(anchor, StringComparison.Ordinal);
            Assert.True(at >= 0, $"{path} must still contain the gate anchored at '{anchor}'");

            var window = code[at..Math.Min(code.Length, at + 600)];

            Assert.DoesNotContain("AuthenticationTypes." + "EntraMFA", window, StringComparison.Ordinal);
        }
    }

    // ---- Which sites hand the driver somewhere to put the code ---------------------------

    [Fact]
    public void EveryServerConnectionStringSite_IsEitherWrappedOrAccountedFor()
    {
        /* Lite resolves a monitored server's connection string at many places, and any of them can be
           the FIRST device-code connection of a process - MSAL's token cache is in-memory and
           process-scoped, so "an earlier connection already signed in" is not a property of the
           installation, only of the run.

           So this enumerates the resolution sites out of source rather than listing them, and every
           one must be in exactly one of two buckets. A new site lands in neither and fails here,
           which is the point: the alternative is a site that opens a device-code connection, gets no
           window, and fails three minutes later with nothing on screen.

           The fallback in EntraDeviceCodeAuth.OnDeviceCodeIssued means an unwrapped site still SHOWS
           the code; what wrapping adds is the early return that hands a waiting UI thread back. That
           is why the second bucket is legitimate rather than a backlog. */
        var sites = new Regex(
            @"(CredentialResolver\.GetConnectionString|ServerConnection\.ResolveConnectionString|\.BuildConnectionString\()",
            RegexOptions.Compiled);

        var wrapped = new[]
        {
            /* The three a user drives, where an early return is the difference between getting the
               dialog back and watching a disabled button for three minutes. */
            "Lite/Windows/AddServerDialog.xaml.cs",
            "Lite/Services/ServerManager.cs",
            "Lite/Services/RemoteCollectorService.cs",
        };

        var accountedFor = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Lite/Services/CredentialResolver.cs"] =
                "the resolver itself - it returns a string and opens nothing",
            ["Lite/Models/ServerConnection.cs"] =
                "the builder itself - it composes the string the resolver returns",
            ["Lite/Windows/AddMultipleServersDialog.xaml.cs"] =
                "bulk add refuses every interactive mode at BuildServerConnection's belt, so no "
                    + "connection built here can be a device-code one",
            ["Lite/Analysis/SqlPlanFetcher.cs"] =
                "background plan fetch - nobody is awaiting a UI thread, so an early return buys "
                    + "nothing the fallback does not already give",
            ["Lite/Mcp/McpPlanTools.cs"] =
                "an MCP read - same reason, and its open is inside LocalDataService",
            ["Lite/Services/RemoteCollectorService.DefinitionRunner.cs"] =
                "reads InitialCatalog off the string and opens no connection",
            ["Lite/Services/RemoteCollectorService.QueryStoreBackfill.cs"] =
                "background backfill - no UI thread waiting",
            ["Lite/Windows/ExcludedDatabasesDialog.xaml.cs"] =
                "a foreground dialog, but one reached only from a saved server, so the process has "
                    + "already signed in through one of the wrapped sites in every path that gets "
                    + "here; the fallback covers the race",
        };

        var found = new List<string>();
        foreach (var file in Directory.GetFiles(
                     Path.Combine(ParitySource.RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(ParitySource.RepoRoot(), file).Replace('\\', '/');
            if (relative.Contains("/obj/", StringComparison.Ordinal) ||
                relative.Contains("/bin/", StringComparison.Ordinal))
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            if (sites.IsMatch(code))
            {
                found.Add(relative);
            }
        }

        /* Population floor, and the positive control for the whole sweep: a walk that found nothing,
           or that stripped its way to empty files, satisfies every assertion below vacuously. */
        Assert.True(
            found.Count >= 8,
            $"found {found.Count} connection-string resolution sites under Lite/; the sweep below is "
                + "vacuous unless it is enumerating the real set");

        var unaccounted = found
            .Where(f => !wrapped.Contains(f, StringComparer.Ordinal) && !accountedFor.ContainsKey(f))
            .ToArray();

        Assert.Empty(unaccounted);

        /* And the wrapped three really do wrap: without this, moving a file from `wrapped` into
           `accountedFor` with any sentence at all would keep this green. */
        foreach (var file in wrapped)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(file));
            Assert.Contains("EntraDeviceCodeAuth." + "Begin(", code, StringComparison.Ordinal);
            Assert.Contains("OpenAsync(", code, StringComparison.Ordinal);
        }
    }

    // ---- Cancelling is recognised as a decision, not a fault -----------------------------

    [Fact]
    public void ACancelledDeviceCodeSignIn_IsRecordedAsADeclineNotAnError()
    {
        /* A cancelled device-code sign-in arrives as an OperationCanceledException, which
           MfaAuthenticationHelper.IsMfaCancelledException does NOT recognise - it matches on message
           text the broker produces, and "A task was canceled." is not in it. Left unhandled, closing
           the code window produces a "Connection Failed" dialog and an ERROR log line for something
           the user chose.

           Pinned in source at both sites that can raise the prompt for a user, each anchored on the
           method that must exist. */
        var dialog = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Windows/AddServerDialog.xaml.cs"));

        var run = dialog.IndexOf("RunConnectionTestAsync()", StringComparison.Ordinal);
        Assert.True(run >= 0, "RunConnectionTestAsync is the connection-test entry point and must exist");
        var runBody = CSharpSourceWalker.BraceBalanced(dialog, dialog.IndexOf('{', run));

        Assert.Contains("is Operation" + "CanceledException", runBody, StringComparison.Ordinal);
        Assert.Contains("signInCancelled = true", runBody, StringComparison.Ordinal);

        var manager = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Services/ServerManager.cs"));
        Assert.Contains("is Operation" + "CanceledException", manager, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCollectorSeparatesAUserDeclineFromAShutdown()
    {
        /* The collector links its own cancellation token with the prompt's, so the token alone cannot
           say which side fired. Flagging a shutdown as a decline would leave the server skipped for
           the rest of the session over an app restart nobody chose, and the flag is only cleared by
           opening the server by hand.

           The discriminating half is the second condition: without !cancellationToken, every
           shutdown becomes a decline, and no test of the decline path alone would notice. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Services/RemoteCollectorService.cs"));

        var at = code.IndexOf("userDeclined", StringComparison.Ordinal);
        Assert.True(at >= 0, "the collector must classify a failed interactive sign-in");

        var window = code[at..Math.Min(code.Length, at + 500)];

        Assert.Contains("deviceCode.Token.IsCancellationRequested", window, StringComparison.Ordinal);
        Assert.Contains("!cancellationToken.IsCancellationRequested", window, StringComparison.Ordinal);
    }

    // ---- The label ------------------------------------------------------------------------

    [Fact]
    public void AuthenticationDisplay_NamesWhatTheUserHasToDo()
    {
        var display = ServerConnection.AuthenticationDisplayFor(AuthenticationTypes.EntraDeviceCode);

        /* "Device Code Flow" is the driver's phrase for a grant type. What a user choosing between
           seven modes needs to know is that a browser is involved and that it does not have to be
           this machine - which is the whole reason this mode survives the elevation #3196 reported. */
        Assert.Contains("Device Code", display, StringComparison.Ordinal);
        Assert.Contains("browser", display, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("any device", display, StringComparison.OrdinalIgnoreCase);

        /* The static helper and the instance property must agree, because the bulk-add belt names the
           mode through the static one and the grid shows the instance one. */
        Assert.Equal(
            display,
            new ServerConnection { AuthenticationType = AuthenticationTypes.EntraDeviceCode }
                .AuthenticationDisplay);
    }

    [Fact]
    public void TheBulkAddRefusalNamesTheModeItIsRefusing()
    {
        /* BulkServerOnboardingTests asserts the refusal TEXT behaviourally, but it needs a loadable
           WPF Window to call the belt and so cannot run on a non-Windows host at all. This pins the
           half that can: the belt composes its message from AuthenticationDisplayFor, so the
           substrings that theory expects are derived here from the same helper rather than typed in
           two places and hoped to agree. */
        Assert.Contains(
            "Device Code",
            ServerConnection.AuthenticationDisplayFor(AuthenticationTypes.EntraDeviceCode),
            StringComparison.OrdinalIgnoreCase);

        Assert.Contains(
            "Entra MFA",
            ServerConnection.AuthenticationDisplayFor(AuthenticationTypes.EntraMFA),
            StringComparison.OrdinalIgnoreCase);

        /* And the belt really does compose from that helper rather than from a literal of its own,
           which is what makes the two assertions above about the shipped message. */
        var belt = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Lite/Windows/AddMultipleServersDialog.xaml.cs"));

        var at = belt.IndexOf("BuildServerConnection(BulkServerParseLine", StringComparison.Ordinal);
        Assert.True(at >= 0, "the belt's choke point must exist");

        Assert.Contains(
            "ServerConnection." + "AuthenticationDisplayFor(",
            belt[at..Math.Min(belt.Length, at + 600)],
            StringComparison.Ordinal);
    }

    // ---- Lite/Darling parity --------------------------------------------------------------

    [Fact]
    public void TheDarlingServiceStoreRejectsTheMode_ByTheWhitelistItAlreadyHad()
    {
        /* The parity finding, pinned rather than asserted in prose. ServerStoreCredential.MapAuth
           names the two modes the Darling service can honor and answers null for everything else, so
           this mode was rejected on the day it was added with no edit to that file. Darling.Tests
           holds the behavioural rows; this is the Lite-side statement that the mode is Lite-only.

           Read out of source because Lite.Tests cannot reference the Darling viewer project, and
           anchored on MapAuth's own body so a whitelist quietly turned into a blacklist fails here. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Viewer/ServerStoreCredential.cs"));

        var at = code.IndexOf("MapAuth(string? authenticationType)", StringComparison.Ordinal);
        Assert.True(at >= 0, "MapAuth is the whitelist and must exist");

        var arms = code[at..Math.Min(code.Length, at + 400)];

        Assert.Contains("AuthenticationTypes.Windows", arms, StringComparison.Ordinal);
        Assert.Contains("AuthenticationTypes.SqlServer", arms, StringComparison.Ordinal);
        Assert.Contains("_ => null", arms, StringComparison.Ordinal);

        /* No Entra mode is named in it at all - the discriminating assertion, because a blacklist
           that happened to list the six Azure modes would satisfy everything above. */
        Assert.DoesNotContain("Entra", arms, StringComparison.Ordinal);
        Assert.DoesNotContain("ManagedIdentity", arms, StringComparison.Ordinal);
        Assert.DoesNotContain("ServicePrincipal", arms, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDarlingViewerDoesNotOfferTheMode()
    {
        /* The store rejecting it is one half. This is the other: the viewer's own dialogs must not
           put it on screen, or a user is offered a mode whose save is refused. Swept over the whole
           viewer project rather than the two dialogs, so a third surface added later is covered.

           Positive control first: the sweep must find the mode's SIBLING in Lite's dialogs, which is
           what proves the probe can see a radio at all. */
        var liteDialog = ParitySource.ReadFile("Lite/Windows/AddServerDialog.xaml");
        Assert.Contains("EntraDeviceCodeAuthRadio", liteDialog, StringComparison.Ordinal);

        var viewerDir = Path.Combine(ParitySource.RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");
        var offenders = new List<string>();
        var scanned = 0;

        foreach (var file in Directory.GetFiles(viewerDir, "*.*", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(ParitySource.RepoRoot(), file).Replace('\\', '/');
            if (relative.Contains("/obj/", StringComparison.Ordinal) ||
                relative.Contains("/bin/", StringComparison.Ordinal) ||
                (!file.EndsWith(".cs", StringComparison.Ordinal) && !file.EndsWith(".xaml", StringComparison.Ordinal)))
            {
                continue;
            }

            scanned++;

            var text = File.ReadAllText(file);

            /* COMMENTS do not count, and that distinction is the whole difficulty. The viewer's own
               ServerStoreCredential doc comment names this mode deliberately - it explains why the
               whitelist rejects it - and a raw substring sweep reads that as the viewer offering the
               mode. So a .cs file is scanned as CODE, plus its string literals separately, because
               the strip removes both and a mode assigned as a bare literal is a real way to offer
               one. A .xaml file has its XML comments removed and is then scanned whole: a radio
               button's x:Name and Content are attribute text, not something the strip would keep. */
            var offers = file.EndsWith(".cs", StringComparison.Ordinal)
                ? CSharpSourceWalker.StripCommentsAndStrings(text).Contains("EntraDeviceCode", StringComparison.Ordinal)
                    || CSharpSourceWalker.StringLiteralBodies(text)
                        .Any(literal => literal.Text.Contains("EntraDeviceCode", StringComparison.Ordinal))
                : Regex.Replace(text, "<!--.*?-->", string.Empty, RegexOptions.Singleline)
                    .Contains("EntraDeviceCode", StringComparison.Ordinal);

            if (offers)
            {
                offenders.Add(relative);
            }
        }

        Assert.True(scanned >= 10, $"scanned {scanned} viewer sources; the sweep is vacuous below that");
        Assert.Empty(offenders);
    }

    // ---- The doc's own claims -------------------------------------------------------------

    [Fact]
    public void TheReadmeDescribesTheModeAndSaysItIsUnverified()
    {
        /* The README's mode COUNT is pinned in EntraDefaultCredentialTests, derived from reflection.
           This pins the two claims a count cannot carry: that the new mode has a row in the table
           naming the edition that offers it, and that the "not confirmed against a live tenant"
           caveat travels with it. The caveat is the one a reader acts on, and the one most likely to
           be quietly dropped when the mode is eventually confirmed - at which point dropping it is
           correct, and this test is where that decision is recorded. */
        var readme = ParitySource.ReadFile("README.md");

        Assert.Contains("| Device Code | Lite |", readme, StringComparison.Ordinal);
        Assert.Contains("### Device Code (Lite only, `ActiveDirectoryDeviceCodeFlow`)", readme, StringComparison.Ordinal);

        var section = readme[readme.IndexOf(
            "### Device Code (Lite only, `ActiveDirectoryDeviceCodeFlow`)", StringComparison.Ordinal)..];
        section = section[..section.IndexOf("### Credential Profiles", StringComparison.Ordinal)];

        Assert.Contains("Not yet confirmed against a live Entra tenant", section, StringComparison.Ordinal);
        Assert.Contains("#3196", section, StringComparison.Ordinal);

        /* And the section says the two things that stop a support round trip: the three-minute limit
           is the driver's, and Darling does not offer the mode. */
        Assert.Contains("three minutes", section, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Darling does not offer this mode", section, StringComparison.Ordinal);
    }
}
