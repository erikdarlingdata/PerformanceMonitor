/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The claim #2138 phase 1 makes about itself, asserted rather than promised: <b>this build cannot
/// write to a monitored SQL Server.</b> The force-plan bot judges, journals and shadow-runs; the
/// write path is its own change (#2731).
///
/// <para>Three independent proofs, because each one alone leaves a way through:</para>
/// <list type="number">
/// <item><b>The shipped artifact carries no write statement.</b> Every string literal in the compiled
/// <c>PerformanceMonitor.Darling.Service</c> assembly is searched for
/// <c>sp_query_store_force_plan</c>, <c>sp_query_store_unforce_plan</c> and <c>FREEPROCCACHE</c>.
/// This reads the DLL's bytes, not the source, so it cannot be satisfied by a copy that drifts — and
/// it sees SQL built inline in a command constructor, which a scan of <c>const</c> fields would
/// miss.</item>
/// <item><b>The seam has no implementation.</b> Reflection over the shipped assemblies finds no
/// concrete <see cref="IPlanForceExecutor"/>, so even code that wanted one could not obtain one.</item>
/// <item><b>The orchestrator has no route to one.</b> <see cref="PlanForceBot"/> holds no field,
/// constructor parameter or member signature that mentions the executor seam, so there is no place
/// to hand it an implementation from outside either.</item>
/// </list>
///
/// <para><b>When #2731 lands the write path, these tests go red, and that is the design.</b> The
/// property they pin is "the write path is not in this build" — a claim that stops being true on
/// purpose, in a diff whose whole subject is making it stop being true, where relaxing the pin is
/// the reviewable act rather than a silent side effect. Nothing here should be loosened for any
/// other reason.</para>
///
/// <para>Scope, stated so it cannot be overread: the search is the SERVICE assembly, which is the
/// process that holds connections to monitored servers.
/// <c>PerformanceMonitor.Analysis.FactRemediation</c> deliberately RENDERS these statements as text
/// — that is the copy-and-run remediation script an operator reads, has done since long before this
/// feature, and executes with their own hands. Rendering advice and executing it are different acts,
/// and only the second one is what "cannot write" is about.</para>
/// </summary>
public sealed class PlanForceNoWritePathTests
{
    /// <summary>The statements that change a monitored server's behaviour. Nothing else in the
    /// force-plan feature can write, so these three are the whole surface.</summary>
    private static readonly string[] WriteStatements =
    {
        "sp_query_store_force_plan",
        "sp_query_store_unforce_plan",
        "FREEPROCCACHE",
    };

    [Fact]
    public void TheShippedServiceAssembly_ContainsNoForceUnforceOrEvictStatement()
    {
        var assembly = typeof(PlanForceBot).Assembly;
        Assert.Equal("PerformanceMonitor.Darling.Service", assembly.GetName().Name);

        var path = assembly.Location;
        Assert.False(string.IsNullOrEmpty(path), "the service assembly must be on disk to be searched");

        var bytes = File.ReadAllBytes(path);

        /* String literals live in the metadata user-string heap as UTF-16, so decoding the whole file
           as UTF-16 surfaces every one of them. Both byte alignments are decoded because the heap's
           offset within the file is not guaranteed even. Type and member names are UTF-8 in a
           different heap, so a method called ForcePlanAsync cannot produce a false positive here —
           only an actual literal can. */
        var decoded = new[]
        {
            Encoding.Unicode.GetString(bytes),
            Encoding.Unicode.GetString(bytes, 1, bytes.Length - 1),
        };

        foreach (var statement in WriteStatements)
        {
            foreach (var text in decoded)
            {
                Assert.False(
                    text.Contains(statement, StringComparison.OrdinalIgnoreCase),
                    $"'{statement}' appears as a string literal in {Path.GetFileName(path)}. " +
                    "Phase 1 of #2138 ships no write path; the write path is #2731.");
            }
        }
    }

    [Fact]
    public void TheExecutorSeam_HasNoImplementation_InAnyShippedAssembly()
    {
        /* Test fakes implement the seam and must not count — they live in this assembly and never
           ship. Everything else the service loads is fair game. */
        var shipped = new[]
        {
            typeof(PlanForceBot).Assembly,
            typeof(PerformanceMonitor.Analysis.ForcePlanBotPolicy).Assembly,
            typeof(PerformanceMonitor.Darling.Storage.StorageVersion).Assembly,
        };

        var implementers = new List<string>();
        foreach (var assembly in shipped)
        {
            implementers.AddRange(
                assembly.GetTypes()
                    .Where(t => typeof(IPlanForceExecutor).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract)
                    .Select(t => t.FullName ?? t.Name));
        }

        Assert.Empty(implementers);
    }

    [Fact]
    public void TheOrchestrator_HasNoRouteToAnExecutor()
    {
        /* Belt to the implementation check's braces: even with no implementation in the box, a
           constructor parameter or settable member typed on the seam would be a door someone could
           open from outside the assembly. There is no door. Generic arguments are walked because
           the shape a factory would take is Func<string, IPlanForceExecutor>, not the bare type. */
        var bot = typeof(PlanForceBot);

        var referenced = new List<Type>();
        referenced.AddRange(bot.GetFields(BindingFlags.Instance | BindingFlags.Static
                | BindingFlags.Public | BindingFlags.NonPublic)
            .Select(f => f.FieldType));
        referenced.AddRange(bot.GetProperties(BindingFlags.Instance | BindingFlags.Static
                | BindingFlags.Public | BindingFlags.NonPublic)
            .Select(p => p.PropertyType));
        foreach (var constructor in bot.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            referenced.AddRange(constructor.GetParameters().Select(p => p.ParameterType));
        }

        foreach (var method in bot.GetMethods(BindingFlags.Instance | BindingFlags.Static
            | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
        {
            referenced.Add(method.ReturnType);
            referenced.AddRange(method.GetParameters().Select(p => p.ParameterType));
        }

        Assert.DoesNotContain(referenced.SelectMany(Flatten), t => t == typeof(IPlanForceExecutor));

        static IEnumerable<Type> Flatten(Type type)
        {
            yield return type;

            if (type.HasElementType && type.GetElementType() is Type element)
            {
                foreach (var nested in Flatten(element))
                {
                    yield return nested;
                }
            }

            foreach (var argument in type.IsGenericType ? type.GetGenericArguments() : Array.Empty<Type>())
            {
                foreach (var nested in Flatten(argument))
                {
                    yield return nested;
                }
            }
        }
    }
}
