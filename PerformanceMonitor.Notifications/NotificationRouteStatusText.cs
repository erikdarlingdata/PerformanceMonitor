/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The status words a notification route's row shows when its scope does not match anything (#5750), in ONE place so the Lite
/// Notification Routes window and the Darling viewer's routes grid cannot say different things. Each product's UI helper
/// points its own constant at these; nothing else spells them.
/// </summary>
public static class NotificationRouteStatusText
{
    /// <summary>A server-scoped route whose server was removed. It matches nothing.</summary>
    public const string OrphanedServer = "Orphaned: server removed";

    /// <summary>A tag-scoped route whose tag was deleted. It matches nothing.</summary>
    public const string OrphanedTag = "Orphaned: tag deleted";

    /// <summary>A tag-scoped route the loader could not expand because the tag data could not be read. It matches nothing
    /// until a reload succeeds, so it is never shown as OK.</summary>
    public const string TagDataUnreadable = "Not active: tag data unreadable";

    /// <summary>The same for a server-scoped route whose server list could not be built.</summary>
    public const string ScopeDataUnreadable = "Not active: scope data unreadable";
}
