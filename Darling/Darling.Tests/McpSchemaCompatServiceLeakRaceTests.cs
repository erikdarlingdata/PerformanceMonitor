// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4075: a reflection race, not a schema cache. <c>ReflectionAIFunctionDescriptor</c>'s constructor calls
/// <c>MethodInfo.GetParameters()</c> once to record which parameters are DI services, keyed by
/// <see cref="ParameterInfo"/> reference IDENTITY (it has no <c>Equals</c> override). It then calls
/// <c>GetParameters()</c> a SECOND time inside <c>AIJsonUtilities.CreateFunctionJsonSchema</c> to build the
/// schema and looks each parameter back up in that table. <c>RuntimeMethodInfo</c> fills its parameter-array
/// cache non-atomically (<c>m_parameters ??= ...</c>), so a concurrent FIRST <c>GetParameters()</c> call on
/// the SAME <see cref="MethodInfo"/>, from another thread, can return a DIFFERENT array of
/// <see cref="ParameterInfo"/> objects. The second lookup then misses on identity, the DI-service parameter
/// is treated as a plain argument, and it leaks into the served schema — exactly the "type reads as a DI
/// service, IsService = True, build count = 1" shape #4094's diagnostic caught.
///
/// This is a REFLECTION-level race (a fresh <see cref="MethodInfo"/> per emitted method never gets a
/// primed parameter cache), not the suite's own thread scheduling, so it reproduces deterministically enough
/// to prove or kill the hypothesis without depending on xUnit's parallelization. Every method is emitted
/// fresh (<c>ModuleBuilder</c>) so its parameter cache starts empty every run.
/// </summary>
public sealed class McpSchemaCompatServiceLeakRaceTests
{
    private sealed class Svc
    {
    }

    /// <summary>Builds one dynamic type carrying <paramref name="count"/> static methods shaped
    /// <c>T(Svc svc, int x)</c>, each decorated with <see cref="McpServerToolAttribute"/> and
    /// <see cref="DescriptionAttribute"/> so <see cref="McpSchemaCompat.WithGeminiCompatibleTools{T}"/>'s
    /// scan (and a hand-built <see cref="McpServerTool.Create"/> call) both recognize them as tools.</summary>
    private static Type EmitToolType(int count)
    {
        var asmName = new AssemblyName($"Mcp4075RaceProbe_{Guid.NewGuid():N}");
        var asmBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var moduleBuilder = asmBuilder.DefineDynamicModule("Main");
        var typeBuilder = moduleBuilder.DefineType("Probe", TypeAttributes.Public | TypeAttributes.Class);

        var mcpToolCtor = typeof(McpServerToolAttribute).GetConstructor(Type.EmptyTypes)!;
        var descriptionCtor = typeof(DescriptionAttribute).GetConstructor(new[] { typeof(string) })!;

        for (var i = 0; i < count; i++)
        {
            var methodBuilder = typeBuilder.DefineMethod(
                $"Tool{i}",
                MethodAttributes.Public | MethodAttributes.Static,
                typeof(string),
                new[] { typeof(Svc), typeof(int) });
            methodBuilder.DefineParameter(1, ParameterAttributes.None, "svc");
            methodBuilder.DefineParameter(2, ParameterAttributes.None, "x");
            methodBuilder.SetCustomAttribute(new CustomAttributeBuilder(mcpToolCtor, Array.Empty<object>()));
            methodBuilder.SetCustomAttribute(new CustomAttributeBuilder(descriptionCtor, new object[] { $"probe tool {i}" }));

            var il = methodBuilder.GetILGenerator();
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Ret);
        }

        return typeBuilder.CreateType();
    }

    /// <summary>Builds a fresh, populated <see cref="IServiceProvider"/> with <see cref="Svc"/> registered
    /// as a singleton, matching the shape every real Darling tool-schema build uses.</summary>
    private static IServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Svc>();
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Builds <paramref name="method"/>'s tool through the exact schema-options factory
    /// <see cref="McpSchemaCompat.WithGeminiCompatibleTools{T}"/> uses (<see cref="McpSchemaCompat.SchemaOptionsFor"/>),
    /// racing a bystander thread's <c>GetParameters()</c> call against the descriptor's own first call.
    /// Returns true if <c>svc</c> leaked into the served schema's <c>properties</c>.
    /// </summary>
    private static bool ServiceLeaked(MethodInfo method, IServiceProvider provider, bool raceEnabled)
    {
        using var start = new ManualResetEventSlim(false);
        Thread? bystander = null;
        if (raceEnabled)
        {
            bystander = new Thread(() =>
            {
                start.Wait();
                _ = method.GetParameters();
            });
            bystander.Start();
        }

        var options = McpSchemaCompat.SchemaOptionsFor(provider);
        start.Set();
        var tool = McpServerTool.Create(
            method,
            target: null,
            options: new McpServerToolCreateOptions
            {
                Services = provider,
                SchemaCreateOptions = options
            });
        bystander?.Join();

        return tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties)
            && properties.TryGetProperty("svc", out _);
    }

    /// <summary>
    /// #4075 repro: with the by-type <c>IncludeParameter</c> guard in <see cref="McpSchemaCompat.SchemaOptionsFor"/>,
    /// zero out of a large emitted population leak <c>svc</c> into the served schema under the race, matching
    /// the standalone repro's 0/5,000 result (issue comment 5806331396). Without the guard the same race
    /// measured 37/5,000; this test only proves the FIXED path holds at 0, since the guard is what makes the
    /// identity-keyed miss harmless regardless of which array wins the race.
    /// </summary>
    [Fact]
    public void RaceGetParameters_WithByTypeGuard_NeverLeaksTheServiceIntoTheSchema()
    {
        const int count = 2000;
        var type = EmitToolType(count);
        var provider = BuildProvider();
        var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Static);
        Assert.Equal(count, methods.Length);

        var leaks = 0;
        foreach (var method in methods)
        {
            if (ServiceLeaked(method, provider, raceEnabled: true))
            {
                leaks++;
            }
        }

        Assert.Equal(0, leaks);
    }

    /// <summary>
    /// Same race, WITHOUT the guard (schema options built directly, mirroring the SDK's plain
    /// <c>WithTools</c> path pre-fix): confirms the race is real and reproduces at a similar rate to the
    /// standalone repro (37/5,000, i.e. roughly 0.5–1.5% here given the smaller emitted population and this
    /// process's scheduling), proving the hypothesis rather than assuming it. A flake-tolerant lower bound
    /// (at least one leak) is asserted rather than an exact count, since thread scheduling is inherently
    /// non-deterministic; if this ever reads 0 on a slow/single-core CI runner, the race window closed for
    /// that run, not the theory.
    /// </summary>
    /* Explicit, because it asserts that a race REPRODUCES: on a slow or lightly loaded runner the window may never
       open, and a test that fails when a race does not happen is itself the flake #4075 was about. Run it by
       hand (`-method`) to re-confirm the mechanism; the guarded test above is the one CI relies on. */
    [Fact(Explicit = true)]
    public void RaceGetParameters_WithoutTheGuard_CanLeakTheServiceIntoTheSchema()
    {
        const int count = 5000;
        var type = EmitToolType(count);
        var provider = BuildProvider();
        var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Static);
        Assert.Equal(count, methods.Length);

        var isService = provider.GetRequiredService<Microsoft.Extensions.DependencyInjection.IServiceProviderIsService>();
        var leaks = 0;
        foreach (var method in methods)
        {
            using var start = new ManualResetEventSlim(false);
            var bystander = new Thread(() =>
            {
                start.Wait();
                _ = method.GetParameters();
            });
            bystander.Start();

            var unguardedOptions = new Microsoft.Extensions.AI.AIJsonSchemaCreateOptions();
            start.Set();
            var tool = McpServerTool.Create(
                method,
                target: null,
                options: new McpServerToolCreateOptions
                {
                    Services = provider,
                    SchemaCreateOptions = unguardedOptions
                });
            bystander.Join();

            if (tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties)
                && properties.TryGetProperty("svc", out _))
            {
                leaks++;
            }
        }

        Assert.True(leaks > 0,
            $"Expected the unguarded race to leak 'svc' into at least one of {count} schemas (root-cause repro), " +
            "got 0. Either the race window did not open on this run, or the hypothesis needs re-checking.");
    }
}
