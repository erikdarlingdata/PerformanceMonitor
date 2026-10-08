/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>The two password key rings the write-path tests ask for (#5366): one that seals and opens with a key made for
/// the test run, and one that refuses with the not-ready reason. Tests pass a ring; none sets the service's own.</summary>
internal static class TestKeyRings
{
    /// <summary>A ring over a key generated once for the test run.</summary>
    public static readonly IPasswordKeyRing Healthy = DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate());

    /// <summary>A ring that cannot seal, with <see cref="DarlingPasswordKey.NotReadyReason"/>.</summary>
    public static readonly IPasswordKeyRing NotReady = DarlingPasswordKey.Refusing(DarlingPasswordKey.NotReadyReason);
}
