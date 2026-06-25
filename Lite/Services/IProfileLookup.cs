/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Read-only seam for looking up a <see cref="CredentialProfile"/> by id, implemented by
/// <see cref="ProfileManager"/>. Exists to keep credential resolution acyclic: <see cref="ServerManager"/>
/// holds this abstraction (late-injected, default null = today's no-profile behavior) so its own
/// <c>CheckConnectionAsync</c> can resolve through the SAME fail-closed atomic-tuple logic as the
/// external consumers, WITHOUT taking a hard dependency on <see cref="ProfileManager"/> (which itself
/// depends on <see cref="ServerManager"/> for the referential-integrity query). Coupling stays one-way:
/// <c>ServerManager → IProfileLookup ← ProfileManager</c>, and separately <c>ProfileManager → ServerManager</c>.
/// </summary>
public interface IProfileLookup
{
    /// <summary>
    /// Returns the profile with the given id, or null if no such profile exists.
    /// A null return on a non-null id drives the fail-closed resolution branch (throw "not found").
    /// </summary>
    CredentialProfile? GetProfile(string id);
}
