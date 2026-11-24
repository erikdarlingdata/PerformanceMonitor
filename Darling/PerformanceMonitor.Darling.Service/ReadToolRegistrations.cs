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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What one read tool contributes to the web read surface (#9477): the handler the <c>/api/read/*</c> dispatch calls, and the catalog
/// entry (category, description, parameters) the web catalog and Custom Views validation read. A tool registers these from its own
/// file, so a new tool never edits the shared alphabetical lists in <c>DarlingWebEndpoints.WebReads.cs</c>,
/// <c>DarlingWebEndpoints.Views.cs</c> and <c>CustomViewValidation.ReadParams.cs</c>. The Storage side's parameter list is derived
/// from <see cref="Catalog"/>, so the two cannot disagree.
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

/// <summary>Collects the registrations once, merges them into the three lists, and hands the Custom Views half to Storage.</summary>
internal static class ReadToolRegistrations
{
    private static readonly Lazy<IReadOnlyList<ReadToolRegistration>> AllRegistrations = new(
        () => Register(Collect(Discover())), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>Every registered read. The first access also gives Storage its half, so touch this before any Custom View validation.</summary>
    internal static IReadOnlyList<ReadToolRegistration> All => AllRegistrations.Value;

    /// <summary>Same as <see cref="All"/>, named for the callers that only need the registration to have happened.</summary>
    internal static void EnsureRegistered() => _ = All;

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

    private static IReadOnlyList<ReadToolRegistration> Register(IReadOnlyList<ReadToolRegistration> registrations)
    {
        CustomViewValidation.RegisterReads(registrations.Select(r => new KeyValuePair<string, IReadOnlyList<ReadPanelParam>>(
            r.Name, r.Catalog.Params.Select(p => new ReadPanelParam(p.Name, p.Required)).ToList())));
        return registrations;
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
