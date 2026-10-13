/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's half of the operator-group boundary: the group name it tells a refused user to ask for, the message
/// that carries it, and the edit dialog no longer decrypting a stored password into the form.
///
/// <para>The dialog is pinned textually, the way the other Add Server dialog tests are, because reproducing it
/// behaviourally means standing up the WPF window with a live store.</para>
/// </summary>
public sealed class ViewerOperatorGroupTests
{
    /// <summary>The Viewer cannot reference the service project, so it repeats the group name. A mismatch would send a
    /// refused user to a group that nothing grants.</summary>
    [Fact]
    public void TheViewerNamesTheSameGroupTheServiceGrants()
    {
        Assert.Equal(DarlingOperatorGroup.Name, ViewerOperatorGroup.Name);
        Assert.Equal(DarlingFileSecurity.OperatorGroupName, ViewerOperatorGroup.Name);
    }

    [Fact]
    public void ARefusedReadTellsThePersonWhichGroupToAskForAndToSignOutAndBackIn()
    {
        var message = ViewerOperatorGroup.AccessDeniedMessage(@"C:\Program Files\PerformanceMonitorDarling\darling.json");

        Assert.Contains(@"C:\Program Files\PerformanceMonitorDarling\darling.json", message, StringComparison.Ordinal);
        Assert.Contains($"\"{DarlingOperatorGroup.Name}\"", message, StringComparison.Ordinal);
        Assert.Contains("install-darling.ps1", message, StringComparison.Ordinal);
        Assert.Contains("sign out and back in", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOriginalExceptionTextFollowsTheAdvice_AndTheTypeAndCauseSurvive()
    {
        var original = new UnauthorizedAccessException("Access to the path is denied.");

        var rethrown = ViewerOperatorGroup.AccessDenied(@"C:\x\darling.json", original);

        Assert.StartsWith(ViewerOperatorGroup.AccessDeniedMessage(@"C:\x\darling.json"), rethrown.Message, StringComparison.Ordinal);
        Assert.EndsWith("(Access to the path is denied.)", rethrown.Message, StringComparison.Ordinal);
        Assert.Same(original, rethrown.InnerException);
    }

    /// <summary>File.Exists is false for a file the account may not reach, so a restricted file must not be reported
    /// as a missing one. The other half of that (an existing file this account is refused) needs an ACL the test
    /// account cannot always set, so only the missing case is checked here.</summary>
    [Fact]
    public void AMissingFileIsNotReportedAsRefused()
    {
        var missing = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "darling.json");

        Assert.False(ViewerOperatorGroup.IsRefused(missing));
    }

    /// <summary>The stored password is a machine-scope blob this account can decrypt. Putting the plaintext in the edit
    /// form would show it to anyone who can open the dialog, so the form starts empty and a blank box keeps the stored
    /// blob (TryResolveCredential does that already).</summary>
    [Fact]
    public void TheEditDialogNeverDecryptsAStoredSecretIntoTheForm()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");

        Assert.DoesNotContain("TryUnprotect", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PasswordBox.Password = ", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AzureClientSecretBox.Password = ", source, StringComparison.Ordinal);

        /* A blank box keeps the stored blob, for both secret-bearing modes. */
        Assert.Contains("encryptedPassword = _existing.EncryptedPassword;", source, StringComparison.Ordinal);
    }
}
