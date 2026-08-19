/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Resolves the <see cref="ITargetProvider"/> for an engine. A plain switch rather than a registry
/// dictionary: the set of engines is closed and known at compile time, and a switch makes an
/// unhandled engine a build-time concern instead of a runtime lookup miss.
/// </summary>
public static class TargetProviders
{
    /// <summary>
    /// The provider for <paramref name="engine"/>. Throws on an engine with no provider — that is a
    /// programming error, not a runtime condition, and must not degrade into a silent skip.
    /// </summary>
    public static ITargetProvider For(CollectorTargetEngine engine) => engine switch
    {
        CollectorTargetEngine.SqlServer => SqlServerTargetProvider.Instance,
        CollectorTargetEngine.PostgreSql => PostgresTargetProvider.Instance,
        _ => throw new ArgumentOutOfRangeException(nameof(engine), engine, "No target provider for this engine"),
    };

    /// <summary>The provider for a monitored target, from the engine on its probed target info.</summary>
    public static ITargetProvider For(CollectorTargetInfo target) => For(target.Engine);
}
