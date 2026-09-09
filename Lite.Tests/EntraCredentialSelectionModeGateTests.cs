/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The one link <see cref="EntraCredentialSelectionTests"/> cannot make: that the app's OWN
/// authentication mode reaches the credential-selection listener.
///
/// <para><c>EntraCredentialSelectionLog.Begin</c> keys off
/// <see cref="SqlConnectionStringBuilder.Authentication"/> rather than off the mode string, because
/// that is the keyword the driver acts on and it therefore cannot disagree with the connection
/// string. The cost of that choice is one degree of indirection between
/// <see cref="AuthenticationTypes.EntraDefaultCredential"/> and the gate — so these pins run the
/// real <see cref="ServerConnection.ApplyAuthentication"/> and close it.</para>
///
/// <para>Separate file because it needs <see cref="ServerConnection"/>, whose closure reaches
/// Windows-only credential storage; the listener pins themselves need none of that and run in a
/// plain <c>net10.0</c> harness on any platform.</para>
/// </summary>
/// <remarks>
/// <para><b>In <c>app-logger-statics</c> for the event source, not for the log.</b> Nothing here
/// touches <see cref="PerformanceMonitorLite.Services.AppLogger"/> — but both pins call
/// <c>Begin</c>, which constructs a real <see cref="System.Diagnostics.Tracing.EventListener"/> over
/// <c>Azure.Identity</c>'s process-wide event source, and <c>EntraCredentialSelectionTests</c> raises
/// real events on that same source. A raised event reaches every listener attached to it anywhere in
/// the process.</para>
///
/// <para>It changes no assertion here today, because these pins only ask whether <c>Begin</c> returned
/// a listener and never read what one captured. Joined anyway, and joined to THAT name rather than a
/// new one: a class can only be in one collection, so a separate event-source collection would leave
/// the one pairing that matters — this class against the class that raises the events —
/// unserialised.</para>
/// </remarks>
[Collection("app-logger-statics")]
public class EntraCredentialSelectionModeGateTests
{
    /// <summary>
    /// <para>Derived from <see cref="AuthenticationTypes"/> by reflection rather than from a list
    /// typed here, so a seventh mode is a decision this test forces rather than a row nobody added.
    /// The count FLOOR is the positive control: "exactly one mode is instrumented" is also true of a
    /// reflection helper that enumerated one mode, or none — #3218 shipped a count pin whose helper
    /// enumerated nothing and passed.</para>
    ///
    /// <para>Every arm is handed a username, a password, an Azure client id AND a managed-identity
    /// client id, so a mode whose arm was copied from a neighbour still gets everything it could
    /// need and the gate's answer is about the mode rather than about missing inputs.</para>
    /// </summary>
    [Fact]
    public void ExactlyOneAuthenticationMode_AttachesTheListener()
    {
        var modes = AllAuthenticationModes();

        Assert.True(
            modes.Count >= 6,
            $"reflected {modes.Count} authentication modes off AuthenticationTypes; the sweep below is "
                + "vacuous unless it is enumerating the real set");

        var instrumented = new List<string>();
        foreach (var mode in modes)
        {
            var builder = new SqlConnectionStringBuilder { DataSource = "example-server" };
            ServerConnection.ApplyAuthentication(
                builder,
                mode,
                username: "someone",
                password: "a-secret",
                azureClientId: "an-azure-client-id",
                managedIdentityClientId: "a-managed-identity-client-id");

            using var listener = EntraCredentialSelectionLog.Begin(builder);
            if (listener is not null)
            {
                instrumented.Add(mode);
            }
        }

        Assert.Equal(new[] { AuthenticationTypes.EntraDefaultCredential }, instrumented);
    }

    /// <summary>
    /// The same claim stated positively and on its own, so a sweep that silently stopped covering
    /// this mode cannot leave the feature untested — <c>Assert.Equal</c> on an empty list against an
    /// empty expectation is not a failure any sweep would report here, but it is a failure of this.
    /// </summary>
    [Fact]
    public void TheEntraDefaultCredentialMode_AttachesTheListener()
    {
        var builder = new SqlConnectionStringBuilder { DataSource = "example-server" };
        ServerConnection.ApplyAuthentication(
            builder,
            AuthenticationTypes.EntraDefaultCredential,
            username: "someone",
            password: "a-secret",
            azureClientId: "an-azure-client-id",
            managedIdentityClientId: "a-managed-identity-client-id");

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDefault, builder.Authentication);

        using var listener = EntraCredentialSelectionLog.Begin(builder);
        Assert.NotNull(listener);
    }

    private static List<string> AllAuthenticationModes() =>
        typeof(AuthenticationTypes)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && !f.IsInitOnly && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
}
