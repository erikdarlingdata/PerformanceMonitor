/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Darling.Tests;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3214: a broker-free Entra sign-in path, added because #3196's Entra MFA failure cannot be
/// repaired in place.
///
/// <para><b>What is and is not claimed here.</b> Every pin below runs without an Entra tenant: the
/// connection string's shape, which modes demand a stored secret, which modes can raise a window,
/// how a failed credential chain is classified, and that the one package that would put the Windows
/// broker back into this mode's credential chain is absent from the build. That a real tenant then
/// issues a token to a real <c>az login</c> session is NOT pinned by anything here and has not been
/// run by anyone — it needs a tenant and a Windows host, the limit #3196 and #2184 both had.</para>
/// </summary>
public class EntraDefaultCredentialTests
{
    /* Reconstructed from the message constants in the pinned packages rather than from a paste, so
       the fixtures fail if the packages move rather than agreeing with a stale copy:

       Azure.Identity 1.18.0, DefaultAzureCredential.cs:63 builds the all-sources-declined message,
       and CredentialUnavailableException.CreateAggregateException appends "- <reason>" per source.
       Microsoft.Data.SqlClient.Extensions.Azure 7.0.2, ActiveDirectoryAuthenticationProvider.cs:515
       then wraps a CredentialUnavailableException as "Azure.Identity error: {ex.Message}". */
    private const string ChainDeclinedMessage =
        "DefaultAzureCredential failed to retrieve a token from the included credentials. "
        + "See the troubleshooting guide for more information. "
        + "https://aka.ms/azsdk/net/identity/defaultazurecredential/troubleshoot\n"
        + "- EnvironmentCredential authentication unavailable. Environment variables are not fully configured.\n"
        + "- ManagedIdentityCredential authentication unavailable. No response received from the managed identity endpoint.\n"
        + "- Azure CLI not installed. Please run 'az login' to set up account.\n"
        + "- BrokerCredential requires the Azure.Identity.Broker package to be referenced.";

    /* Same file, :64 — a source that was available and threw, rather than declining. */
    private const string ChainFaultedMessage =
        "DefaultAzureCredential authentication failed due to an unhandled exception: "
        + "The token cache could not be read.";

    /* The #3196 report's own text, which must classify as a BROKER failure and NOT as an ambient
       one. The two classifiers run on every failure, so each has to leave the other's cases alone. */
    private const string ReportedBrokerRefusal =
        "Failed to authenticate the user in Active Directory (Authentication=ActiveDirectoryInteractive). "
        + "Error code 0xunknown_broker_error\n"
        + "Failed to acquire access token for ActiveDirectoryInteractive: Unknown Status: Unexpected\n"
        + "Error: 0xffffffff80070520\n"
        + "Context: (pii)";

    // ---- The connection string ------------------------------------------------------------

    [Fact]
    public void ExistingSignIn_SetsActiveDirectoryDefault()
    {
        var server = new ServerConnection
        {
            ServerName = "example.database.windows.net",
            DatabaseName = "mydb",
            AuthenticationType = AuthenticationTypes.EntraDefaultCredential,
            EncryptMode = "Mandatory",
        };

        var conn = new SqlConnectionStringBuilder(server.BuildConnectionString(null, null));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDefault, conn.Authentication);
        Assert.False(conn.IntegratedSecurity);
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, conn.Encrypt);
    }

    [Fact]
    public void ExistingSignIn_CarriesNoSecretAndNoUserId_EvenWhenHandedBoth()
    {
        /* Handed a username AND a password AND a client id, all of which a neighbouring arm would
           use, because that is the mistake this pin exists to catch: the arm was written next to
           ServicePrincipal's, which assigns UserID and Password from exactly these parameters.

           UserID is the one that matters. It reads like the Entra MFA username hint and is not:
           SqlClient forwards UserId on this path into DefaultAzureCredentialOptions as
           ManagedIdentityClientId, SharedTokenCacheUsername and WorkloadIdentityClientId at once
           (Extensions.Azure 7.0.2, :873-878), so a UPN there misconfigures two credential sources.
           A copy of the ServicePrincipal arm would pass every other test in this file. */
        var builder = new SqlConnectionStringBuilder();

        ServerConnection.ApplyAuthentication(
            builder,
            AuthenticationTypes.EntraDefaultCredential,
            username: "someone@contoso.com",
            password: "a-secret-that-must-not-travel",
            azureClientId: "00000000-0000-0000-0000-000000000000",
            managedIdentityClientId: "11111111-1111-1111-1111-111111111111");

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDefault, builder.Authentication);
        Assert.Equal(string.Empty, builder.UserID);
        Assert.Equal(string.Empty, builder.Password);
        Assert.False(builder.IntegratedSecurity);

        /* And on the rendered string too, not only on the builder's properties: an empty UserID is
           omitted from the text, so a report or a log line carrying this string cannot leak either
           keyword. Asserted on the text because that is what a connection actually travels as. */
        var rendered = builder.ConnectionString;
        Assert.DoesNotContain("Password", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("User ID", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("someone@contoso.com", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Authentication=", rendered, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ExistingSignIn_ClearsIntegratedSecurityOnAPrePopulatedBuilder()
    {
        /* The dialog and the production builder both hand in a builder they have already touched.
           Leaving IntegratedSecurity true beside an Authentication keyword is a connection string
           the driver rejects, and the arm sets it false explicitly rather than relying on the
           default. */
        var builder = new SqlConnectionStringBuilder { IntegratedSecurity = true };

        ServerConnection.ApplyAuthentication(
            builder, AuthenticationTypes.EntraDefaultCredential, null, null, null, null);

        Assert.False(builder.IntegratedSecurity);
    }

    [Fact]
    public void AuthenticationDisplay_NamesThePreconditionNotTheDriverKeyword()
    {
        var display = new ServerConnection
        {
            AuthenticationType = AuthenticationTypes.EntraDefaultCredential,
        }.AuthenticationDisplay;

        /* "Default Credential" would be the driver's word for it and would tell a user nothing
           about what the mode needs. The label has to carry the requirement, because the mode fails
           rather than prompting when the requirement is not met. */
        Assert.DoesNotContain("Default", display, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Sign-In", display, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("az login", display, StringComparison.Ordinal);
    }

    // ---- Which modes demand a stored secret ----------------------------------------------

    [Theory]
    [InlineData(AuthenticationTypes.EntraDefaultCredential)]
    [InlineData(AuthenticationTypes.Windows)]
    [InlineData(AuthenticationTypes.EntraMFA)]
    [InlineData(AuthenticationTypes.ManagedIdentity)]
    public void ZeroTouchModes_NeedNoCredentialLookupAtAll(string authType)
    {
        /* The credential service is passed as null deliberately. A zero-touch mode returns before
           it is dereferenced, so null proves the short-circuit rather than merely observing a
           "true" that a lookup could also have produced - and it runs on any OS, where a real
           Windows Credential Manager lookup does not. */
        var server = new ServerConnection { AuthenticationType = authType };

        Assert.True(ServerConnection.HasStoredCredentials(server, null!, profileLookup: null));
    }

    [Theory]
    [InlineData(AuthenticationTypes.SqlServer)]
    [InlineData(AuthenticationTypes.ServicePrincipal)]
    public void CredentialRequiringModes_StillReachTheCredentialStore(string authType)
    {
        /* The other half of the pin above, and the reason it is not vacuous: these two must NOT be
           short-circuited by the zero-touch arm. Reaching a null store is the observable difference
           between "needs a secret" and "needs nothing", so the throw IS the assertion. */
        var server = new ServerConnection { AuthenticationType = authType };

        Assert.Throws<NullReferenceException>(
            () => ServerConnection.HasStoredCredentials(server, null!, profileLookup: null));
    }

    // ---- Which modes can raise a window ---------------------------------------------------

    [Fact]
    public void ExactlyOneModeRequiresAnInteractiveSignIn()
    {
        /* Enumerated from the constants by reflection rather than listed here, so a seventh mode
           cannot be added without this pin having an opinion about it. Entra MFA is the only mode
           that can put a window in front of the user; the new mode cannot, because the driver
           excludes the interactive browser from its credential chain outright, which is why it may
           run on an unattended collection cycle. */
        var modes = AuthenticationModeConstants();

        Assert.Equal(6, modes.Length);

        var interactive = modes.Where(AuthenticationTypes.RequiresInteractiveSignIn).ToArray();

        Assert.Equal(new[] { AuthenticationTypes.EntraMFA }, interactive);
        Assert.False(AuthenticationTypes.RequiresInteractiveSignIn(AuthenticationTypes.EntraDefaultCredential));
    }

    [Fact]
    public void TheBackgroundCollectionGate_AsksWhetherTheModeIsInteractive()
    {
        /* ServerManager.CheckServerConnectivity's gate used to test equality against EntraMFA, and
           a new mode inherited "not suppressed" from an omission rather than from a decision. This
           pins that the gate asks the shared question instead.

           The anchor is asserted to EXIST first: without that this degrades into "the file does not
           contain a string", which a deleted gate would also satisfy. Comments and literals are
           stripped, so the words below cannot be satisfied by the comment that explains them. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Lite/Services/ServerManager.cs"));

        var anchor = source.IndexOf("!allowInteractiveAuth", StringComparison.Ordinal);
        Assert.True(anchor >= 0, "the background-check gate keys off !allowInteractiveAuth and must exist");

        var brace = source.IndexOf('{', anchor);
        Assert.True(brace > anchor, "the gate must govern a block");

        var condition = source[anchor..brace];

        Assert.Contains("RequiresInteractiveSignIn", condition, StringComparison.Ordinal);
        Assert.DoesNotContain("EntraMFA", condition, StringComparison.Ordinal);
    }

    // ---- Classifying a failed credential chain -------------------------------------------

    [Theory]
    [InlineData(ChainDeclinedMessage, EntraAmbientCredentialFailureKind.NoCredentialFound)]
    [InlineData(ChainFaultedMessage, EntraAmbientCredentialFailureKind.CredentialSourceFaulted)]
    public void Classify_SeparatesNothingFoundFromSomethingBroken(
        string message, EntraAmbientCredentialFailureKind expected)
    {
        Assert.Equal(expected, EntraAmbientCredentialFailure.Classify(new InvalidOperationException(message)));
    }

    [Theory]
    [InlineData(ReportedBrokerRefusal)]
    [InlineData("Login failed for user 'monitor'.")]
    [InlineData("A network-related or instance-specific error occurred while establishing a connection.")]
    [InlineData("Connection Timeout Expired.")]
    [InlineData("")]
    public void Classify_LeavesEveryOtherFailureAlone(string message)
    {
        Assert.Equal(
            EntraAmbientCredentialFailureKind.None,
            EntraAmbientCredentialFailure.Classify(new InvalidOperationException(message)));
    }

    [Fact]
    public void Classify_TreatsNullAsNone()
    {
        Assert.Equal(EntraAmbientCredentialFailureKind.None, EntraAmbientCredentialFailure.Classify(null));
    }

    [Fact]
    public void Classify_ReadsTheWholeInnerExceptionChain()
    {
        /* The shape the driver actually produces, with the marker ONLY at the bottom: SqlClient's
           own exception outermost, the Extensions.Azure AuthenticationException in the middle, the
           credential chain's CredentialUnavailableException underneath. The two outer messages are
           deliberately marker-free, so a classifier reading only the outermost Message finds
           nothing here - which is the whole reason the chain is what is asserted on. */
        var chain = new InvalidOperationException(
            "Failed to authenticate the user in Active Directory (Authentication=ActiveDirectoryDefault).",
            new InvalidOperationException(
                "Azure.Identity error occurred while acquiring a token.",
                new InvalidOperationException(ChainDeclinedMessage)));

        Assert.Equal(
            EntraAmbientCredentialFailureKind.NoCredentialFound,
            EntraAmbientCredentialFailure.Classify(chain));
    }

    [Fact]
    public void Classify_TakesTheShallowestMarkerWhenTheChainHoldsBoth()
    {
        /* Asserted in BOTH directions, and that is the point. The first assertion alone passes
           whatever the walk does, because the arm testing the fault marker is written first, so
           swapping the two arms leaves it green. The second assertion is the one that
           discriminates: it can hold only if DEPTH decides, so a bottom-up walk, or one that
           flattens every message into a single string before matching, fails it. The two markers
           are prefixes of different sentences and cannot both match one message, so arm order is
           not observable by any caller and only depth is. Deleting either half leaves a test that
           cannot fail. */
        var faultOverDeclined = new InvalidOperationException(
            ChainFaultedMessage,
            new InvalidOperationException(ChainDeclinedMessage));

        Assert.Equal(
            EntraAmbientCredentialFailureKind.CredentialSourceFaulted,
            EntraAmbientCredentialFailure.Classify(faultOverDeclined));

        var declinedOverFault = new InvalidOperationException(
            ChainDeclinedMessage,
            new InvalidOperationException(ChainFaultedMessage));

        Assert.Equal(
            EntraAmbientCredentialFailureKind.NoCredentialFound,
            EntraAmbientCredentialFailure.Classify(declinedOverFault));
    }

    [Fact]
    public void TheTwoClassifiersDoNotStealEachOthersCases()
    {
        /* Both run on every failure (see AddServerDialog.RunConnectionTestAsync), so overlap would
           show two contradictory explanations in one dialog. */
        Assert.Equal(EntraBrokerFailureKind.BrokerRejected, EntraBrokerFailure.Classify(new Exception(ReportedBrokerRefusal)));
        Assert.Equal(EntraAmbientCredentialFailureKind.None, EntraAmbientCredentialFailure.Classify(new Exception(ReportedBrokerRefusal)));

        Assert.Equal(EntraBrokerFailureKind.None, EntraBrokerFailure.Classify(new Exception(ChainDeclinedMessage)));
        Assert.Equal(EntraAmbientCredentialFailureKind.NoCredentialFound, EntraAmbientCredentialFailure.Classify(new Exception(ChainDeclinedMessage)));
    }

    [Fact]
    public void EveryClassifiedKindHasSomethingToSay()
    {
        foreach (var kind in Enum.GetValues<EntraAmbientCredentialFailureKind>()
                                 .Where(k => k != EntraAmbientCredentialFailureKind.None))
        {
            var explanation = EntraAmbientCredentialFailure.Explain(kind);
            Assert.False(
                string.IsNullOrWhiteSpace(explanation),
                $"{kind} is a classification the dialog can be handed and has no explanation");
        }

        Assert.Null(EntraAmbientCredentialFailure.Explain(EntraAmbientCredentialFailureKind.None));
    }

    [Fact]
    public void Explain_NamesTheActionRatherThanTheCredentialChain()
    {
        /* The failure this replaces said, in effect, "none of several credential providers were
           available", which is the unactionable shape #3201 existed to stop showing people. */
        var text = EntraAmbientCredentialFailure.Explain(EntraAmbientCredentialFailureKind.NoCredentialFound)!;

        Assert.Contains("az login", text, StringComparison.Ordinal);
        Assert.Contains("never prompts", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Service Principal", text, StringComparison.Ordinal);
    }

    // ---- The dialog body ------------------------------------------------------------------

    [Fact]
    public void Compose_KeepsTheDriverError_TheAmbientExplanation_AndTheLogPointer()
    {
        var body = ConnectionFailureMessage.Compose(
            "Azure.Identity error: " + ChainDeclinedMessage,
            EntraBrokerFailureKind.None,
            EntraAmbientCredentialFailureKind.NoCredentialFound,
            @"C:\Users\someone\AppData\Local\PerformanceMonitorLite-Data\logs");

        /* The driver's own text carries each source's reason, including the CLI's own "az login"
           hint, which is why it is kept rather than replaced by the explanation. */
        Assert.Contains("Azure CLI not installed", body, StringComparison.Ordinal);
        Assert.Contains("az login", body, StringComparison.Ordinal);
        Assert.Contains(@"PerformanceMonitorLite-Data\logs", body, StringComparison.Ordinal);

        Assert.True(
            body.IndexOf("Azure.Identity error", StringComparison.Ordinal)
                < body.IndexOf("never prompts", StringComparison.Ordinal),
            "the driver's own error must come before the explanation of it");
        Assert.True(
            body.IndexOf("never prompts", StringComparison.Ordinal)
                < body.IndexOf(@"PerformanceMonitorLite-Data\logs", StringComparison.Ordinal),
            "the log location must come last");
    }

    [Fact]
    public void Compose_ShowsBothExplanationsWhenBothFire()
    {
        /* A broker refusal on an existing-sign-in attempt would mean the credential chain's own
           broker link had gone live, taking this mode's whole advantage away. Composing both rather
           than choosing between them is how that would be visible at all. */
        var body = ConnectionFailureMessage.Compose(
            "Azure.Identity error: " + ReportedBrokerRefusal,
            EntraBrokerFailureKind.BrokerRejected,
            EntraAmbientCredentialFailureKind.NoCredentialFound,
            logDirectory: null);

        Assert.Contains("Web Account Manager", body, StringComparison.Ordinal);
        Assert.Contains("never prompts", body, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionTest_ClassifiesAndLogsBothFailureFamilies()
    {
        /* Behavioural coverage cannot reach this: the catch lives in a WPF Window that needs a
           dispatcher and a live form, so the call site is asserted in source (the
           EntraBrokerFailureTests idiom).

           Comments and string literals are stripped FIRST, and that is what makes this a check on
           code rather than on my own prose: the block carries a comment naming both classifiers and
           explaining why both run, so a raw substring search over the file would be satisfied by
           the comment alone and would pass with every call deleted. #3201's equivalent pin did
           exactly that until the strip was added. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Lite/Windows/AddServerDialog.xaml.cs"));

        var signature = source.IndexOf("RunConnectionTestAsync()", StringComparison.Ordinal);
        Assert.True(signature >= 0, "RunConnectionTestAsync is the connection-test entry point and must exist");

        var open = source.IndexOf('{', signature);
        Assert.True(open > signature, "RunConnectionTestAsync must have a body");

        var body = CSharpSourceWalker.BraceBalanced(source, open);

        Assert.Contains("EntraBrokerFailure.Classify", body, StringComparison.Ordinal);
        Assert.Contains("EntraAmbientCredentialFailure.Classify", body, StringComparison.Ordinal);
        Assert.Contains("AppLogger.Error", body, StringComparison.Ordinal);
    }

    // ---- The second leg of the broker-free proof -----------------------------------------

    [Fact]
    public void TheAzureIdentityBrokerPackageIsAbsent_ByTheProbeAzureIdentityItselfUses()
    {
        /* Leg 1 of this mode's broker-free claim is control flow in the driver: the
           ActiveDirectoryDefault arm returns at :264 and the MSAL public-client application that
           WithBroker attaches to is not built until :325. Leg 1 alone does NOT settle it.

           Leg 2 is this. Azure.Identity 1.18.0 puts a BrokerCredential at the TAIL of
           DefaultAzureCredential's chain and ExcludeBrokerCredential defaults to FALSE, and
           SqlClient sets only ExcludeInteractiveBrowserCredential when it constructs the options -
           so the chain does contain a broker link. That link is inert only because it resolves its
           options by reflection and throws CredentialUnavailableException when the
           Azure.Identity.Broker package is not referenced.

           So adding that package, directly or transitively, would silently put the Windows broker
           back at the end of this mode's credential chain and reintroduce the #3196 failure it
           exists to route around. This is the exact Type.GetType call
           DefaultAzureCredentialFactory.TryCreateDevelopmentBrokerOptions makes, so the pin cannot
           drift from the mechanism it guards: whatever this returns is what the chain will see. */

        /* The POSITIVE CONTROL comes first, and without it this test is decoration. Type.GetType on
           a simple assembly name answers null both when the assembly is absent and when nothing can
           be loaded by that form at all - so in any assembly whose closure does not carry
           Azure.Identity, the assertion below passes for entirely the wrong reason and would keep
           passing with the broker package added - measured, in a harness carrying no Azure
           assembly at all, where both halves of this pin were green. Proving the probe CAN return a
           type is what makes the null below evidence of anything. */
        Assert.NotNull(Type.GetType(
            "Azure.Identity.DefaultAzureCredential, Azure.Identity", throwOnError: false));

        var brokerOptions = Type.GetType(
            "Azure.Identity.Broker.DevelopmentBrokerOptions, Azure.Identity.Broker",
            throwOnError: false);

        Assert.Null(brokerOptions);
    }

    [Fact]
    public void TheAzureIdentityBrokerAssemblyIsNotShippedBesideTheApp()
    {
        /* The same claim, counted a second way that fails differently. Type.GetType answers about
           what the runtime can LOAD, which depends on the deps file and on probing paths; this
           answers about what is on disk. A package added as a private asset, or one whose type
           names change, moves one of these and not the other. */
        var beside = Path.GetDirectoryName(typeof(ServerConnection).Assembly.Location);
        Assert.False(string.IsNullOrEmpty(beside), "the app assembly must have a resolvable location");

        /* Positive control, for the same reason as above: "no file matches Azure.Identity.Broker*"
           is also true of a directory holding no Azure assemblies whatsoever, which is not the claim
           and is not evidence for it. */
        Assert.NotEmpty(Directory.GetFiles(beside!, "Azure.Identity.dll"));

        Assert.Empty(Directory.GetFiles(beside!, "Azure.Identity.Broker*"));
    }

    // ---- The doc's own count --------------------------------------------------------------

    [Fact]
    public void TheReadmeAuthenticationCountMatchesTheConstants()
    {
        /* A count in prose is a partial list with a numeral welded on: this file adds a sixth mode,
           and the README's "five authentication types" was correct until it did. Derived from
           reflection rather than restated, so the numeral cannot go stale silently again. */
        var readme = ReadRepoFile("README.md");
        var expected = AuthenticationModeConstants().Length;

        var words = new[] { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };
        Assert.True(expected < words.Length, $"no number word for {expected} modes");

        var claim = $"supports {words[expected]} authentication types";
        Assert.Contains(claim, readme, StringComparison.Ordinal);

        /* And no OTHER number word in that sentence, or a stale numeral could sit beside the right
           one and both would be found. */
        foreach (var word in words.Where(w => w != words[expected]))
        {
            Assert.DoesNotContain(
                $"supports {word} authentication types", readme, StringComparison.Ordinal);
        }
    }

    // ---- Helpers --------------------------------------------------------------------------

    /// <summary>Every <c>public const string</c> on <see cref="AuthenticationTypes"/>, by reflection.</summary>
    private static string[] AuthenticationModeConstants() =>
        typeof(AuthenticationTypes)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();

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
}
