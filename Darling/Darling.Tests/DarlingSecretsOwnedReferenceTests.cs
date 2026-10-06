/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A stored password reference is checked where it resolves (#5240): an <c>env:</c> or <c>file:</c> reference in a
/// server's stored secret slot that names one of this service's own configuration files or secrets is refused before
/// it is read, with the sentence the app gives when it declines to save such a reference. A reference to anything else
/// still resolves. A refused reference fails that one server's connect attempt the way an unresolvable reference does,
/// and never reaches the worker as a throw.
///
/// <para>The owned set is process-wide state, so the class is in the <c>darling-owned-secrets</c> collection with the
/// other classes that set it, and it puts back what it found.</para>
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class DarlingSecretsOwnedReferenceTests : IDisposable
{
    private const string OwnedVariable = "DARLING_TEST_OWNED_VARIABLE_5240";
    private const string OwnedVariableValue = "owned-variable-value-Q7";
    private const string OtherVariable = "DARLING_TEST_OTHER_VARIABLE_5240";
    private const string OtherVariableValue = "other-variable-value-Q7";
    private const string UnsetVariable = "DARLING_TEST_UNSET_VARIABLE_5240";
    private const string OwnedFileValue = "owned-file-value-Q7";
    private const string OtherFileValue = "other-file-value-Q7";

    private readonly string _root = Path.Combine(Path.GetTempPath(), "darling-secret-ref-" + Guid.NewGuid().ToString("N"));
    private readonly DarlingOwnedSet _before = DarlingOwnedSecrets.Current;
    private readonly string _ownedFile;
    private readonly string _otherFile;

    public DarlingSecretsOwnedReferenceTests()
    {
        var ownedDirectory = Path.Combine(_root, "own");
        var otherDirectory = Path.Combine(_root, "other");
        Directory.CreateDirectory(ownedDirectory);
        Directory.CreateDirectory(otherDirectory);
        _ownedFile = Path.Combine(ownedDirectory, "secret.txt");
        _otherFile = Path.Combine(otherDirectory, "secret.txt");
        File.WriteAllText(_ownedFile, OwnedFileValue);
        File.WriteAllText(_otherFile, OtherFileValue);
        Environment.SetEnvironmentVariable(OwnedVariable, OwnedVariableValue);
        Environment.SetEnvironmentVariable(OtherVariable, OtherVariableValue);
        Environment.SetEnvironmentVariable(UnsetVariable, null);
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(new[] { ownedDirectory }, new[] { OwnedVariable }));
    }

    public void Dispose()
    {
        DarlingOwnedSecrets.Set(_before);
        Environment.SetEnvironmentVariable(OwnedVariable, null);
        Environment.SetEnvironmentVariable(OtherVariable, null);
        try
        {
            Directory.Delete(_root, true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* best effort; a leftover scratch directory under the temp path harms nothing */
        }
    }

    private static MonitoredServer Server(string? encryptedPassword = null, string? remediationPassword = null) => new()
    {
        Name = "sql01",
        Host = "sql01.example.test",
        Auth = "sql",
        Username = "monitor",
        EncryptedPassword = encryptedPassword,
        RemediationUsername = remediationPassword is null ? null : "remediator",
        RemediationEncryptedPassword = remediationPassword,
    };

    /// <summary>An owned <c>env:</c> reference is refused before it resolves. The answer names the setting, carries the
    /// one refusal sentence, and holds neither the variable's name nor its value: the variable is set, so a resolve
    /// that ran first would have returned the value instead of throwing.</summary>
    [Fact]
    public void AnOwnedEnvReference_IsRefusedBeforeItResolves()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolvePassword(Server("env:" + OwnedVariable), out _));

        Assert.Contains("servers['sql01'].encryptedPassword", ex.Message, StringComparison.Ordinal);
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedVariable, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedVariableValue, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>An owned <c>file:</c> reference is refused before the file is read. The file exists and holds a value, so
    /// a resolve that ran first would have returned it.</summary>
    [Fact]
    public void AnOwnedFileReference_IsRefusedBeforeItResolves()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolvePassword(Server("file:" + _ownedFile), out _));

        Assert.Contains("servers['sql01'].encryptedPassword", ex.Message, StringComparison.Ordinal);
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(_ownedFile, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedFileValue, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2087: add_servers stores env:/file: secret REFERENCES verbatim in the encrypted-password slot on
    /// Linux (a pointer is not a secret, and DPAPI does not exist there). The resolver must therefore
    /// recognize a reference in that slot and resolve it instead of feeding it to DPAPI Unprotect — which
    /// would throw on every platform, since a reference is not a base64 blob. A reference that names nothing
    /// this service owns is the case that still resolves, for both shapes.
    /// </summary>
    [Fact]
    public void ResolvePassword_ReferenceInEncryptedSlot_ResolvesInsteadOfUnprotecting()
    {
        Assert.Equal(OtherVariableValue, DarlingSecrets.ResolvePassword(Server("env:" + OtherVariable), out var usedPlaintext));

        /* A reference is not plaintext-in-config — callers must not warn on it (the #1804 rule). */
        Assert.False(usedPlaintext);

        Assert.Equal(OtherFileValue, DarlingSecrets.ResolvePassword(Server("file:" + _otherFile), out usedPlaintext));
        Assert.False(usedPlaintext);
    }

    /// <summary>The remediation credential's reference is held to the same rule: an owned reference is refused, and one
    /// that names nothing owned resolves.</summary>
    [Fact]
    public void ARemediationReference_IsHeldToTheSameRule()
    {
        var env = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolveRemediationPassword(Server(remediationPassword: "env:" + OwnedVariable)));
        Assert.Contains("servers['sql01'].remediationEncryptedPassword", env.Message, StringComparison.Ordinal);
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, env.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedVariableValue, env.Message, StringComparison.Ordinal);

        var file = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolveRemediationPassword(Server(remediationPassword: "file:" + _ownedFile)));
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, file.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedFileValue, file.Message, StringComparison.Ordinal);

        Assert.Equal(OtherVariableValue, DarlingSecrets.ResolveRemediationPassword(Server(remediationPassword: "env:" + OtherVariable)));
        Assert.Equal(OtherFileValue, DarlingSecrets.ResolveRemediationPassword(Server(remediationPassword: "file:" + _otherFile)));
    }

    /// <summary>Until a configuration has loaded the owned set is not known, so no stored reference is resolved: the
    /// rule does not depend on the configuration having happened to load first.</summary>
    [Fact]
    public void WithNoConfigurationLoaded_AStoredReferenceIsRefused()
    {
        DarlingOwnedSecrets.Set(null!);

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolvePassword(Server("env:" + OtherVariable), out _));

        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OtherVariableValue, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The caller: a refused reference fails that one server's connect attempt and nothing else. The worker runs every
    /// connect through <see cref="ServerConnectProbe.AttemptAsync"/>, which never throws (bar a cancel) and hands the
    /// failure back, and the worker then logs it with the server's name and backs off for a retry. A refused reference
    /// and an unresolvable one (a variable that is not set) end the same way: a failure naming the setting, with no
    /// runtime and no throw.
    /// </summary>
    [Fact]
    public async Task ARefusedReference_FailsThatOneServersConnectAttempt_TheSameWayAnUnresolvableOneDoes()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(),
            "The stored secret slot is resolved on the Windows connect path; other platforms refuse a stored encryptedPassword before any reference is read.");

        var ct = TestContext.Current.CancellationToken;
        using var gate = new SemaphoreSlim(1, 1);

        var refused = await ServerConnectProbe.AttemptAsync(
            Server("env:" + OwnedVariable), (target, token) => DarlingServerConnector.ConnectAsync(target, null, token), gate, ct);
        Assert.Null(refused.Runtime);
        var refusal = Assert.IsType<InvalidOperationException>(refused.Failure);
        Assert.Contains("servers['sql01'].encryptedPassword", refusal.Message, StringComparison.Ordinal);
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, refusal.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedVariableValue, refusal.Message, StringComparison.Ordinal);

        var unresolvable = await ServerConnectProbe.AttemptAsync(
            Server("env:" + UnsetVariable), (target, token) => DarlingServerConnector.ConnectAsync(target, null, token), gate, ct);
        Assert.Null(unresolvable.Runtime);
        var failure = Assert.IsType<InvalidOperationException>(unresolvable.Failure);
        Assert.Contains("servers['sql01'].encryptedPassword", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingOwnedSecrets.ReferenceRefusalText, failure.Message, StringComparison.Ordinal);

        /* The gate was released both times: a third attempt is not blocked behind the first two. */
        Assert.Equal(1, gate.CurrentCount);
    }
}
