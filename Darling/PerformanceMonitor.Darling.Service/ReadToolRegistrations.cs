/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What one read tool contributes to the web read surface (#9477): the handler the <c>/api/read/*</c> dispatch calls, and the catalog
/// entry (category, description, parameters) the web catalog and Custom Views validation read. A tool registers these from its own
/// file, so a new tool never edits the shared alphabetical lists in <c>DarlingWebEndpoints.WebReads.cs</c> and
/// <c>DarlingWebEndpoints.Views.cs</c>. The Custom Views parameter list is the other half: a small class in the Storage project
/// (<c>IReadParamsRegistration</c>) that Storage finds itself, so the Viewer, which has no Service, sees the same reads. A test pins
/// the two parameter lists equal.
/// </summary>
internal sealed record ReadToolRegistration(
    string Name,
    DarlingWebEndpoints.ReadToolHandler Handler,
    DarlingWebEndpoints.CatalogRead Catalog);

/// <summary>
/// Implemented by a small class beside the tool it describes, with a public parameterless constructor. <see cref="ReadToolRegistrations"/>
/// finds every implementation in this assembly:
/// <code>
/// public sealed class Registration : IReadToolRegistration
/// {
///     public ReadToolRegistration Read { get; } = new("get_thing", (c, pg, an) =&gt; Tools.GetThing(pg, c.RequestAborted),
///         new(DarlingWebEndpoints.CatOverview, "What the tool answers.", Array.Empty&lt;DarlingWebEndpoints.CatalogParam&gt;()));
/// }
/// </code>
/// </summary>
internal interface IReadToolRegistration
{
    ReadToolRegistration Read { get; }
}

/// <summary>Collects the registrations once and merges them into the web dispatch and the catalog.</summary>
internal static class ReadToolRegistrations
{
    private static readonly Lazy<IReadOnlyList<ReadToolRegistration>> AllRegistrations = new(
        () => Collect(Discover()), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>Every registered read.</summary>
    internal static IReadOnlyList<ReadToolRegistration> All => AllRegistrations.Value;

    private static IEnumerable<IReadToolRegistration> Discover() =>
        typeof(IReadToolRegistration).Assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false } && typeof(IReadToolRegistration).IsAssignableFrom(t))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .Select(t => (IReadToolRegistration)Activator.CreateInstance(t)!);

    /// <summary>The registrations in order. The same name twice throws and names the tool.</summary>
    internal static IReadOnlyList<ReadToolRegistration> Collect(IEnumerable<IReadToolRegistration> sources)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var list = new List<ReadToolRegistration>();
        foreach (var source in sources)
        {
            var registration = source.Read;
            if (!seen.Add(registration.Name))
            {
                throw new InvalidOperationException($"The read tool '{registration.Name}' is registered more than once.");
            }

            list.Add(registration);
        }

        return list;
    }

    /// <summary>Adds each registration's value to <paramref name="target"/>. A name already in it throws and names the tool.
    /// Never an index initializer: that keeps the last value and hides the duplicate.</summary>
    internal static void AddTo<T>(IDictionary<string, T> target, IEnumerable<ReadToolRegistration> registrations, Func<ReadToolRegistration, T> pick)
    {
        foreach (var registration in registrations)
        {
            if (!target.TryAdd(registration.Name, pick(registration)))
            {
                throw new InvalidOperationException($"The read tool '{registration.Name}' is registered and is also in the shared list.");
            }
        }
    }
}
