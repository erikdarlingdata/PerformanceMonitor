/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Why the start stops over the compose credentials directory (#8141): another user owns it
/// (<paramref name="OwnerUserId"/> set) or the service's user cannot read it (<paramref name="OwnerUserId"/> null).
/// Carries only a path, two user ids and the fixed chown text, so <see cref="Sentence"/> is safe to publish.
/// </summary>
internal sealed record ComposeCredentialDirectoryRefusal(string Directory, uint UserId, uint? OwnerUserId)
{
    /// <summary>The one sentence the log and the ping body carry.</summary>
    public string Sentence
    {
        get
        {
            var euidText = UserId.ToString(CultureInfo.InvariantCulture);
            var fix = DarlingManagedRoles.CredentialOwnershipFix(UserId, Directory);
            return OwnerUserId is { } owner
                ? $"The credentials directory {Directory} belongs to user {owner.ToString(CultureInfo.InvariantCulture)}, "
                    + $"but the service runs as user {euidText}, so collection is stopped and nothing in it was read or changed; "
                    + $"to give it to the service's user, run: {fix}, then restart the service (docker compose restart darling)."
                : $"The credentials directory {Directory} cannot be read by user {euidText}, which the service runs as, "
                    + "so collection is stopped and nothing in it was read or changed; "
                    + $"to give it to that user, run: {fix}, then restart the service (docker compose restart darling).";
        }
    }
}
