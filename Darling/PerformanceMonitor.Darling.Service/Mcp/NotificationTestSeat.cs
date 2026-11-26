/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service's live alert settings, handed to <c>send_test_notification</c> (#9636). The worker publishes the same
/// <see cref="DarlingAlertSettings"/> its alert delivery reads, so a test send uses the channel values a real alert would use, opened
/// with the service's password key. The <c>mcp</c> store role cannot read a secret column, so the tool cannot read them from the store
/// itself. The object is the one the worker's reload hot-swaps, so a change saved in the store reaches it on the worker's next
/// configuration check. No tool exposes it: it is a service parameter, so a client cannot supply one.
/// </summary>
public sealed class NotificationTestSeat
{
    private volatile DarlingAlertSettings? _settings;

    /// <summary>The process-wide seat the worker publishes to and the tool host registers.</summary>
    public static NotificationTestSeat Shared { get; } = new();

    /// <summary>Publishes the worker's settings (once at start). Null clears them.</summary>
    public void Publish(DarlingAlertSettings? settings) => _settings = settings;

    /// <summary>The published settings, or null before the worker has published them.</summary>
    public DarlingAlertSettings? Read() => _settings;
}
