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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The Custom Views half of a read tool's registration (#9477): its read name and the parameters a panel may pass it. One small class
/// per tool in <c>ReadParams/</c>, with a public parameterless constructor. <see cref="CustomViewValidation"/> finds every
/// implementation in this assembly the first time it needs the list of known reads, so the Viewer, the web, MCP and tests all see the same
/// set with no hand-off from the Service:
/// <code>
/// internal sealed class ThingReadParams : IReadParamsRegistration
/// {
///     public string Read =&gt; "get_thing";
///     public IReadOnlyList&lt;ReadPanelParam&gt; Params { get; } = new ReadPanelParam[] { new("hours", false) };
/// }
/// </code>
/// The Service registers the same tool's web handler and catalog entry in its own file. A test pins the two parameter lists equal.
/// </summary>
internal interface IReadParamsRegistration
{
    string Read { get; }

    IReadOnlyList<ReadPanelParam> Params { get; }
}

/// <summary>Finds the <see cref="IReadParamsRegistration"/> classes of this assembly and merges them into the shared read lists.</summary>
internal static class ReadParamsRegistrations
{
    internal static IReadOnlyList<IReadParamsRegistration> Discover() =>
        typeof(IReadParamsRegistration).Assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false } && typeof(IReadParamsRegistration).IsAssignableFrom(t))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .Select(t => (IReadParamsRegistration)Activator.CreateInstance(t)!)
            .ToList();

    /// <summary>
    /// The shared names and parameters with every registration added: the names in the shared list's order, then the registered ones in
    /// order. A read named twice, or named in the shared lists as well, throws and names the read.
    /// </summary>
    internal static (IReadOnlyList<string> Names, IReadOnlyDictionary<string, IReadOnlyList<ReadPanelParam>> Params) Merge(
        IReadOnlyList<string> sharedNames,
        IReadOnlyDictionary<string, IReadOnlyList<ReadPanelParam>> sharedParams,
        IEnumerable<IReadParamsRegistration> registrations)
    {
        var names = new List<string>(sharedNames);
        var parameters = new Dictionary<string, IReadOnlyList<ReadPanelParam>>(sharedParams, StringComparer.Ordinal);
        foreach (var registration in registrations)
        {
            if (!parameters.TryAdd(registration.Read, registration.Params))
            {
                throw new InvalidOperationException($"The read '{registration.Read}' is registered twice, or is also in the shared read lists.");
            }

            names.Add(registration.Read);
        }

        return (names, parameters);
    }
}
