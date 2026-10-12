/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The guard that fails the test which leaves PUBLIC without CONNECT on the shared test database (#5618), and its
/// comparison. No store is touched.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class PublicConnectGuardTests
{
    [Fact]
    public void OnlyAGrantedToRevokedChange_IsReported()
    {
        var before = new Dictionary<string, bool>
        {
            ["darling"] = true,
            ["suite"] = true,
            ["already_revoked"] = false,
            ["regranted"] = false,
            ["vanished"] = true,
        };
        var after = new Dictionary<string, bool>
        {
            ["darling"] = false,
            ["suite"] = true,
            ["already_revoked"] = false,
            ["regranted"] = true,
            ["newcomer"] = false,
        };

        /* "darling" lost it; "suite" kept it; "already_revoked" was not granted to begin with (a developer's own managed
           store), so it is not this test's doing; "regranted" went the other way; a database that appears or disappears
           between the two reads is not a change of the privilege. */
        Assert.Equal(["darling"], PublicConnectDelta.DatabasesThatLostPublicConnect(before, after));
    }
}
