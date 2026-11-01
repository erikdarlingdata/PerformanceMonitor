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
using System.Runtime.CompilerServices;
using System.Text;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
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
[Trait("Stage", "Guard")]
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

    /// <summary>
    /// #5632 Phase A (review round 2, L6): the agent hand-off docs teach no write. <c>Darling/AGENTS.example.md</c> is meant
    /// to be pasted into an operator's own agent's instructions, and the README section "Hand an alert to your own agent"
    /// describes that hand-off, so a write statement named in either one teaches an agent to run it. Both are read as
    /// text and held to the same <see cref="WriteStatements"/> the service assembly is.
    /// </summary>
    [Fact]
    public void TheAgentHandOffDocs_NameNoForceUnforceOrEvictStatement()
    {
        string darling = Path.Combine(RepoRoot(), "Darling");
        string agents = File.ReadAllText(Path.Combine(darling, "AGENTS.example.md"));
        string section = MarkdownSection(File.ReadAllText(Path.Combine(RepoRoot(), "docs", "darling", "agents-and-alert-sources.md")), "Hand an alert to your own agent"); // #6416: moved from Darling/README.md

        foreach (var (name, text) in new[] { ("Darling/AGENTS.example.md", agents), ("docs/darling/agents-and-alert-sources.md \"Hand an alert to your own agent\"", section) })
        {
            foreach (var statement in WriteStatements)
            {
                Assert.False(
                    text.Contains(statement, StringComparison.OrdinalIgnoreCase),
                    $"{name} names '{statement}'. The agent hand-off docs must not teach an agent a write (#5632, #2731).");
            }
        }
    }

    /// <summary>The body of the markdown section whose heading contains <paramref name="heading"/>: from that heading to
    /// the next heading of the same or a higher level. Fails when the heading is gone, so a rename cannot empty the
    /// scan.</summary>
    private static string MarkdownSection(string markdown, string heading)
    {
        var lines = markdown.Split('\n');
        int start = Array.FindIndex(lines, l => l.TrimStart().StartsWith('#') && l.Contains(heading, StringComparison.Ordinal));
        Assert.True(start >= 0, $"The README heading \"{heading}\" moved or was renamed; point the guard at it again.");

        int level = lines[start].TrimStart().TakeWhile(ch => ch == '#').Count();
        var body = new StringBuilder();
        for (int i = start + 1; i < lines.Length; i++)
        {
            string trimmed = lines[i].TrimStart();
            int next = trimmed.TakeWhile(ch => ch == '#').Count();
            if (next > 0 && next <= level && trimmed.Length > next && trimmed[next] == ' ') break;
            body.Append(lines[i]).Append('\n');
        }

        Assert.True(body.Length > 0, $"The README section \"{heading}\" is empty.");
        return body.ToString();
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above " + thisFile);
    }

    /// <summary>
    /// #5632 step 7: <see cref="StoreOwnerSource"/> hands the store owner's data source to MCP tool code, so the force tools can read one
    /// server's remediation slot, which the <c>mcp</c> role cannot read. It is registered in the MCP host's services, so any tool method could
    /// ask for it by type and then read or write the store as the owner. This pin keeps it to the one class: no other MCP tool method takes it,
    /// and the force tools take it as a service parameter, never as a client argument.
    /// </summary>
    [Fact]
    public void OnlyTheForceToolClass_TakesTheStoreOwnerSource()
    {
        var takers = typeof(PlanForceBot).Assembly.GetTypes()
            .SelectMany(type => type.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            .Where(method => method.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false).Length > 0)
            .Where(method => method.GetParameters().Any(parameter => parameter.ParameterType == typeof(StoreOwnerSource)))
            .Select(method => method.DeclaringType!.Name + "." + method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(new[]
        {
            /* #6394: a new webhook signing key is sealed for the destination's stored sealed generic URL, which the mcp role cannot read, so the tool reads that URL as the store owner. */
            nameof(DarlingMcpAlertTools) + "." + nameof(DarlingMcpAlertTools.GenerateWebhookSigningKey),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.CreatePlanGuide),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.DropPlanGuide),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.EvictCachedPlan),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.ForceQueryStorePlan),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.KillSession),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.RecompileModule),
            nameof(DarlingMcpPlanForceTools) + "." + nameof(DarlingMcpPlanForceTools.UnforceQueryStorePlan),
            /* #6230: the undo is the way back from a force, run by the same code the web route runs; its gate is the force tools (that server force_plan switch and remediation login). */
            nameof(DarlingMcpPlanUndoTools) + "." + nameof(DarlingMcpPlanUndoTools.UndoPlanChange),
        /* #6269: the server action switch tool writes config.config_server_actions, which only the store owner can write. */
        nameof(DarlingMcpServerActionTools) + "." + nameof(DarlingMcpServerActionTools.SetServerAction),
        /* #6569: set_alert_threshold_override is a Level 2 agent action, so it takes the owner source and the service, not the mcp login, writes its action rows. */
        nameof(DarlingMcpThresholdOverrideTools) + "." + nameof(DarlingMcpThresholdOverrideTools.SetAlertThresholdOverride),
        }, takers);
    }

    /// <summary>
    /// #5632 step 7: the service now references the Remediation assembly, for ONE class. Two proofs that the door has one hinge. By
    /// reflection: no field, property, method signature or local of any other service type names a Remediation type, and the executor
    /// itself is named only by <c>DarlingMcpPlanForceTools</c> (and the compiler's nested types for its async bodies). By source: the
    /// Remediation namespace and the executor's name appear in no other service file, and never in <see cref="PlanForceBot"/>'s.
    /// </summary>
    [Fact]
    public void OnlyTheForceToolClass_TouchesTheRemediationAssembly()
    {
        var service = typeof(PlanForceBot).Assembly;
        var remediation = typeof(PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor).Assembly;
        const BindingFlags all = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        static IEnumerable<Type> Flatten(Type type)
        {
            yield return type;
            if (type.HasElementType && type.GetElementType() is Type element)
                foreach (var nested in Flatten(element))
                    yield return nested;
            foreach (var argument in type.IsGenericType ? type.GetGenericArguments() : Array.Empty<Type>())
                foreach (var nested in Flatten(argument))
                    yield return nested;
        }

        static Type Owner(Type type) => type.DeclaringType is { } outer ? Owner(outer) : type;

        var offenders = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var type in service.GetTypes())
        {
            var used = new List<Type>();
            if (type.BaseType is not null)
                used.Add(type.BaseType);
            used.AddRange(type.GetInterfaces());
            used.AddRange(type.GetFields(all).Select(f => f.FieldType));
            used.AddRange(type.GetProperties(all).Select(p => p.PropertyType));
            foreach (var method in type.GetMethods(all).Cast<MethodBase>().Concat(type.GetConstructors(all)))
            {
                if (method is MethodInfo info)
                    used.Add(info.ReturnType);
                used.AddRange(method.GetParameters().Select(p => p.ParameterType));
                if (method.GetMethodBody() is { } body)
                    used.AddRange(body.LocalVariables.Select(l => l.LocalType));
            }

            if (used.SelectMany(Flatten).Any(t => t.Assembly == remediation))
                offenders.Add(Owner(type).Name);
        }

        Assert.Equal(new[] { nameof(DarlingMcpPlanForceTools) }, offenders);

        var botTypes = service.GetTypes().Where(t => Owner(t) == typeof(PlanForceBot)).ToList();
        Assert.NotEmpty(botTypes);
        Assert.DoesNotContain(offenders, name => name == nameof(PlanForceBot));
    }

    [Fact]
    public void TheExecutorsName_AppearsInOneServiceSourceFile_AndNeverInThePlanForceBot()
    {
        var serviceDir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");
        var files = Directory.GetFiles(serviceDir, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .ToArray();
        Assert.NotEmpty(files);

        var naming = files
            .Where(f =>
            {
                var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(f));
                return code.Contains("SqlServerRemediationExecutor", StringComparison.Ordinal)
                    || code.Contains("Darling.Remediation", StringComparison.Ordinal);
            })
            .Select(Path.GetFileName)
            .ToArray();
        Assert.Equal(new[] { "DarlingMcpPlanForceTools.cs", "DarlingMcpPlanForceTools.Kill.cs", "DarlingMcpPlanForceTools.PlanGuide.cs", "DarlingMcpPlanForceTools.PrepareWrite.cs", "DarlingMcpPlanForceTools.Proposal.cs", "DarlingMcpPlanForceTools.RecompileModule.cs" }, naming);
        Assert.DoesNotContain("PlanForceBot.cs", naming);
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
