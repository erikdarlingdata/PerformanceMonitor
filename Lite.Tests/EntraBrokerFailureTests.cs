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
using System.Runtime.CompilerServices;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3196: an externally reported Microsoft Entra MFA connection failure that produced no log entry
/// anywhere, so the only record of it was a screenshot of a dialog.
///
/// <para>These pin the two halves that are verifiable without an Entra tenant: that a broker failure
/// is classified by which stage of the handshake it died at, and that the connection test writes the
/// exception somewhere. Whether the reporter's tenant then authenticates is a real-tenant step and
/// no test here claims it.</para>
/// </summary>
public class EntraBrokerFailureTests
{
    /* The driver's rendering of an interactive-Entra failure, reconstructed from the format strings in
       Microsoft.Data.SqlClient's ActiveDirectoryAuthenticationProvider and MSAL's WamAdapters rather
       than from a reporter's paste: SqlClient interpolates the MSAL error code into its own message as
       "Error code 0x<code>", and MSAL's broker adapter renders the native error object underneath it.
       0x80070520 is ERROR_NO_SUCH_LOGON_SESSION, which is the reported case. */
    private const string ReportedBrokerRefusal =
        "Failed to authenticate the user in Active Directory (Authentication=ActiveDirectoryInteractive). "
        + "Error code 0xunknown_broker_error\n"
        + "Failed to acquire access token for ActiveDirectoryInteractive: Unknown Status: Unexpected\n"
        + "Error: 0xffffffff80070520\n"
        + "Context: (pii)\n"
        + "Tag: 0x21420087";

    [Theory]
    [InlineData(ReportedBrokerRefusal, EntraBrokerFailureKind.BrokerRejected)]
    [InlineData("Error code 0xWAM_provider_error_3399614467", EntraBrokerFailureKind.BrokerRejected)]
    [InlineData("Error code 0xwindow_handle_required", EntraBrokerFailureKind.WindowHandleMissing)]
    [InlineData("Error code 0xwam_runtime_init_failed", EntraBrokerFailureKind.BrokerUnavailable)]
    public void Classify_NamesTheStageTheBrokerHandshakeDiedAt(string message, EntraBrokerFailureKind expected)
    {
        Assert.Equal(expected, EntraBrokerFailure.Classify(new InvalidOperationException(message)));
    }

    [Fact]
    public void Classify_ReadsTheWholeInnerExceptionChain()
    {
        /* The shape the driver actually produces: SqlClient's own exception on the outside, its
           internal AuthenticationException in the middle, MSAL's broker exception at the bottom. A
           classifier that reads only the outermost Message finds nothing here, which is the whole
           reason this is asserted on a nested chain rather than a flat one. */
        var chain = new InvalidOperationException(
            "A connection was successfully established with the server, but then an error occurred.",
            new InvalidOperationException(
                "Failed to authenticate the user in Active Directory.",
                new InvalidOperationException("unknown_broker_error")));

        Assert.Equal(EntraBrokerFailureKind.BrokerRejected, EntraBrokerFailure.Classify(chain));
    }

    [Fact]
    public void Classify_PrefersTheMoreSpecificStageWhenOneMessageCarriesTwoMarkers()
    {
        /* WindowHandleMissing is a fault in this application; BrokerRejected is a condition on the
           user's machine. Reporting the second when the first is also present sends a reporter to
           their IT department over our bug, so the specific arm wins. This fails if the refusal check
           is moved ahead of the handle check. */
        var both = new InvalidOperationException("unknown_broker_error and window_handle_required");

        Assert.Equal(EntraBrokerFailureKind.WindowHandleMissing, EntraBrokerFailure.Classify(both));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Login failed for user 'someone'.")]
    [InlineData("A network-related or instance-specific error occurred while establishing a connection.")]
    [InlineData("Execution Timeout Expired.")]
    [InlineData("User canceled authentication.")]
    /* SQL Server's own Service Broker shares the word and nothing else. A substring match on
       "broker" classifies this as a WAM failure and tells a user their Windows account is broken
       when a queue is disabled. */
    [InlineData("The service broker queue 'dbo.ExampleQueue' is currently disabled.")]
    [InlineData("Service Broker needs to be enabled in this database.")]
    public void Classify_DoesNotClaimABrokerFailureForAnythingElse(string? message)
    {
        var ex = message is null ? null : new InvalidOperationException(message);

        Assert.Equal(EntraBrokerFailureKind.None, EntraBrokerFailure.Classify(ex));
    }

    [Fact]
    public void Explain_CoversEveryStageTheClassifierCanReturn()
    {
        /* Enumerated rather than listed, so a stage added to the enum without prose fails here
           instead of silently explaining nothing to the user who hit it. */
        foreach (var kind in Enum.GetValues<EntraBrokerFailureKind>())
        {
            var explanation = EntraBrokerFailure.Explain(kind);

            if (kind == EntraBrokerFailureKind.None)
            {
                Assert.Null(explanation);
                continue;
            }

            Assert.False(
                string.IsNullOrWhiteSpace(explanation),
                $"{kind} is a stage the classifier can return and has no explanation");
        }
    }

    [Fact]
    public void Compose_KeepsTheDriversErrorCodesAlongsideTheExplanation()
    {
        /* The explanation says which side failed; only the driver's text carries the error codes a
           report needs. Replacing one with the other loses the actionable half. */
        var body = ConnectionFailureMessage.Compose(
            ReportedBrokerRefusal,
            EntraBrokerFailureKind.BrokerRejected,
            @"C:\Users\someone\AppData\Local\PerformanceMonitorLite-Data\logs");

        Assert.Contains("unknown_broker_error", body, StringComparison.Ordinal);
        Assert.Contains("Web Account Manager", body, StringComparison.Ordinal);
        Assert.Contains(@"PerformanceMonitorLite-Data\logs", body, StringComparison.Ordinal);

        /* Order matters: the driver's error first, then what it means, then where the rest of it is. */
        Assert.True(
            body.IndexOf("unknown_broker_error", StringComparison.Ordinal)
                < body.IndexOf("Web Account Manager", StringComparison.Ordinal),
            "the driver's own error must come before the explanation of it");
        Assert.True(
            body.IndexOf("Web Account Manager", StringComparison.Ordinal)
                < body.IndexOf(@"PerformanceMonitorLite-Data\logs", StringComparison.Ordinal),
            "the log location must come last");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Compose_OmitsTheLogPointerRatherThanPointingNowhere(string? logDirectory)
    {
        /* A pointer with no path after it reads as "the logs are missing", which is the confusion
           this text exists to end. */
        var body = ConnectionFailureMessage.Compose("Login failed.", EntraBrokerFailureKind.None, logDirectory);

        Assert.DoesNotContain("is in the log at", body, StringComparison.Ordinal);
        Assert.Contains("Login failed.", body, StringComparison.Ordinal);
    }

    [Fact]
    public void Compose_AddsNothingWhenThereIsNothingToAdd()
    {
        Assert.Equal(string.Empty, ConnectionFailureMessage.Compose(null, EntraBrokerFailureKind.None, null));
    }

    [Fact]
    public void ConnectionTest_WritesTheExceptionToTheLog()
    {
        /* The reported defect: the dialog showed an error and the process recorded nothing, so the
           only artefact was a screenshot. Behavioural coverage cannot reach this - the catch lives in
           a WPF Window that needs a dispatcher and a live form - so the call site is asserted in
           source, per the DetachedCollectorGateTests idiom.

           Comments and string literals are stripped first, deliberately. The catch block carries a
           comment naming AppLogger.Error and explaining why it is there; a raw substring search over
           the file is satisfied by that comment alone, so it would pass with the call deleted. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Lite/Windows/AddServerDialog.xaml.cs"));

        var signature = source.IndexOf("RunConnectionTestAsync()", StringComparison.Ordinal);
        Assert.True(signature >= 0, "RunConnectionTestAsync is the connection-test entry point and must exist");

        var open = source.IndexOf('{', signature);
        Assert.True(open > signature, "RunConnectionTestAsync must have a body");

        var body = CSharpSourceWalker.BraceBalanced(source, open);

        Assert.Contains("catch", body, StringComparison.Ordinal);
        Assert.Contains("AppLogger.Error", body, StringComparison.Ordinal);
        Assert.Contains("EntraBrokerFailure.Classify", body, StringComparison.Ordinal);
    }

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
