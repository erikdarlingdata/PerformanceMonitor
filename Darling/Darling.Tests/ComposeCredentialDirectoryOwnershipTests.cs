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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8141: a credentials directory another user owns, or one the service's user cannot list, stops the start before
/// the store is opened, with one sentence naming the path, both users and the chown that fixes it.
/// <see cref="CollectorStoppedMarkerTests"/> pins that the stand-down writes the marker.
/// </summary>
public sealed class ComposeCredentialDirectoryOwnershipTests
{
    private const string Dir = "/var/lib/darling/credentials";

    private static string Key(string path) => Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));

    [Fact]
    public void RootOwned_ServiceIs1654_Refuses_NamingPathBothUsersAndTheChown()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(Dir, 0);
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir);

            Assert.NotNull(refusal);
            Assert.Equal(Dir, refusal.Directory);
            Assert.Equal(1654u, refusal.UserId);
            Assert.Equal(0u, refusal.OwnerUserId);
            var sentence = refusal.Sentence;
            Assert.Contains(Dir, sentence, StringComparison.Ordinal);
            Assert.Contains("belongs to user 0", sentence, StringComparison.Ordinal);
            Assert.Contains("runs as user 1654", sentence, StringComparison.Ordinal);
            Assert.Contains("chown darling -R 1654:1654 /var/lib/darling", sentence, StringComparison.Ordinal);
            Assert.Contains("collection is stopped", sentence, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void OwnedByTheService_EvenGroupOpen_IsNull()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(Dir, 1654);
        modes.Report(Dir, "2770");
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Null(DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir));
        }
    }

    [Fact]
    public void ServiceIsRoot_IsNull()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 0 };
        modes.ReportOwner(Dir, 1654);
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Null(DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir));
        }
    }

    [Fact]
    public void NoDirectory_IsNull()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Null(DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir));
        }
    }

    [Fact]
    public void OwnedByTheService_ButNotListable_Refuses()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(Dir, 1654);
        modes.Unlistable.Add(Key(Dir));
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir);

            Assert.NotNull(refusal);
            Assert.Null(refusal.OwnerUserId);
            Assert.Contains("cannot be read by user 1654", refusal.Sentence, StringComparison.Ordinal);
            Assert.Contains("chown darling -R 1654:1654 /var/lib/darling", refusal.Sentence, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void OwnerUnreadable_Refuses_AsCannotBeRead()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.OwnerThrows[Key(Dir)] = new UnauthorizedAccessException();
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir);

            Assert.NotNull(refusal);
            Assert.Null(refusal.OwnerUserId);
            Assert.Contains("cannot be read by user 1654", refusal.Sentence, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void OwnerLookupFails_IsNull()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.OwnerThrows[Key(Dir)] = new IOException();
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Null(DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir));
        }
    }

    [Fact]
    public void NoUnixSeam_IsNull()
    {
        Assert.Null(DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(Dir));
    }
}
