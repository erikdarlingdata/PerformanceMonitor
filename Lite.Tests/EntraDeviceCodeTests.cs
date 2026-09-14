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
    public void Attempt_DisposingADisplacedAttemptDoesNotEvictTheNewerOne()
    {
        /* The conditional half of the release, and the reason it is a CompareExchange rather than an
           assignment. Two attempts can overlap only in a shape the UI does not produce today, but the
           failure if it ever does is the quiet kind: the older attempt finishes, clears the slot, and
           the newer one's code is then published onto nothing - a sign-in with no window at all,
           replacing one with a window showing the wrong code. */
        var builder = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDeviceCode, null, null, null, null);

        EntraDeviceCodeAuth.ResetForTests();
        try
        {
            var older = EntraDeviceCodeAuth.Begin(builder);
            var newer = EntraDeviceCodeAuth.Begin(builder);

            Assert.NotNull(older);
            Assert.NotNull(newer);
            Assert.NotSame(older, newer);

            older!.Dispose();

            Assert.True(
                EntraDeviceCodeAuth.SignInInFlight,
                "the displaced attempt must not take the newer attempt's slot with it");

            newer!.Dispose();
            Assert.False(EntraDeviceCodeAuth.SignInInFlight);
        }
        finally
        {
            EntraDeviceCodeAuth.ResetForTests();
        }
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
