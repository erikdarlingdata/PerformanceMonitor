/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// MCP tool registration that produces JSON schemas tolerated by stricter function-calling
/// validators (notably Google Gemini / Antigravity).
///
/// The stock <c>WithTools&lt;T&gt;()</c> emits parameter schemas straight from the .NET method
/// signatures. A nullable-with-default parameter such as <c>string? server_name = null</c> becomes
/// <c>{ "type": ["string", "null"], "default": null }</c>. Gemini's function-declaration validator
/// rejects union <c>type</c> arrays and the <c>default</c> keyword and drops the ENTIRE tool set,
/// so the client connects but lists zero tools (issue #1074). Claude Code / opencode are lenient
/// and accept the same schema, which is why this only bites Gemini-based clients.
///
/// This registers tools identically to the SDK's <c>WithTools&lt;T&gt;()</c> (same DI-backed
/// service-parameter exclusion, same invocation path) but installs a schema transform that:
///   - collapses nullable type unions, e.g. <c>["string","null"]</c> → <c>"string"</c>; and
///   - strips the <c>default</c> keyword.
/// Optionality is still conveyed by the parameter's absence from the schema's <c>required</c> array,
/// and the .NET default value still applies at invocation time, so nothing changes for lenient
/// clients — the tool surface and call behavior are unchanged.
///
/// <para>It is also where the two-tier description split happens (#3898 D1/D3): a description carrying
/// <see cref="McpToolGuide.Marker"/> is served as its head, and its tail is recorded in the
/// <see cref="McpToolGuideCatalog"/> that <c>get_tool_guide</c> reads. See <see cref="McpToolGuide"/>.</para>
/// </summary>
public static class McpSchemaCompat
{
    /// <summary>
    /// Shared schema-creation options applied to every Gemini-compatible tool. Immutable, so a single
    /// instance is reused across all registrations.
    /// </summary>
    private static readonly AIJsonSchemaCreateOptions GeminiCompatSchemaOptions = new()
    {
        TransformOptions = new AIJsonSchemaTransformOptions
        {
            TransformSchemaNode = CollapseGeminiUnsupportedKeywords
        }
    };

    /// <summary>
    /// #4075: a per-provider copy of <see cref="GeminiCompatSchemaOptions"/> with an <c>IncludeParameter</c>
    /// that excludes DI-service-typed parameters BY TYPE, closing a reflection race in the SDK rather than
    /// relying on identity-keyed lookups. <c>ReflectionAIFunctionDescriptor</c> calls
    /// <c>MethodInfo.GetParameters()</c> once to record which parameters are DI services (keyed by
    /// <see cref="ParameterInfo"/> reference identity, which has no <c>Equals</c> override) and then calls
    /// <c>GetParameters()</c> again inside <c>AIJsonUtilities.CreateFunctionJsonSchema</c> to build the
    /// schema, looking each parameter back up in that identity-keyed table. <c>RuntimeMethodInfo</c> fills its
    /// parameter-array cache non-atomically (<c>m_parameters ??= ...</c>), so if another thread's concurrent
    /// first <c>GetParameters()</c> call on the SAME <c>MethodInfo</c> races this one, the two calls can return
    /// two different <see cref="ParameterInfo"/> object arrays; the second lookup then misses, the parameter
    /// is treated as a plain argument instead of an excluded DI service, and it leaks into the served schema.
    /// Measured in a standalone repro against these exact package versions: 0/5,000 leaked single-threaded,
    /// 37/5,000 leaked with a concurrent second <c>GetParameters()</c> call, 0/5,000 with this guard in place.
    /// <c>IncludeParameter</c> is ANDed with the SDK's own identity-keyed exclusion, so a missed lookup can no
    /// longer let a DI service through: this check is independent of <see cref="ParameterInfo"/> identity and
    /// always sees the correct answer for the parameter's declared CLR type. Parameter BINDING (which value
    /// gets passed at invocation time) is unaffected — it consistently uses the first <c>GetParameters()</c>
    /// array — only the served SCHEMA is at risk. The SDK's own request-scoped parameter types (the ones
    /// <c>RequestServiceProvider.IsAugmentedWith</c> recognises) take the same double-<c>GetParameters()</c>
    /// path and are NOT covered here; out of scope for #4075 (Darling/Lite tools take no such parameter today).
    /// </summary>
    internal static AIJsonSchemaCreateOptions SchemaOptionsFor(IServiceProvider services)
    {
        var isService = services.GetService<IServiceProviderIsService>();
        if (isService is null)
        {
            return GeminiCompatSchemaOptions;
        }

        var baseInclude = GeminiCompatSchemaOptions.IncludeParameter;
        return GeminiCompatSchemaOptions with
        {
            IncludeParameter = parameter =>
                !isService.IsService(parameter.ParameterType) && (baseInclude is null || baseInclude(parameter))
        };
    }

    /// <summary>
    /// Adds all <see cref="McpServerToolAttribute"/>-marked static methods on <typeparamref name="TToolType"/>
    /// as MCP tools, generating Gemini-compatible parameter schemas. Drop-in replacement for the SDK's
    /// <c>WithTools&lt;TToolType&gt;()</c> for tool classes whose methods are all static.
    /// </summary>
    public static IMcpServerBuilder WithGeminiCompatibleTools<[DynamicallyAccessedMembers(
        DynamicallyAccessedMemberTypes.PublicMethods |
        DynamicallyAccessedMemberTypes.NonPublicMethods)] TToolType>(
        this IMcpServerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var catalog = GuideCatalogOf(builder.Services);

        foreach (var toolMethod in typeof(TToolType).GetMethods(
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
        {
            if (toolMethod.GetCustomAttribute<McpServerToolAttribute>() is not { } toolAttribute)
            {
                continue;
            }

            /* All Performance Monitor tools are static methods that receive their dependencies as
               parameters resolved from DI. Instance tools would need a target factory (as the SDK's
               WithTools does); fail loudly rather than silently skip if that ever changes. */
            if (!toolMethod.IsStatic)
            {
                throw new InvalidOperationException(
                    $"{typeof(TToolType).FullName}.{toolMethod.Name} is an instance method; " +
                    $"{nameof(WithGeminiCompatibleTools)} only supports static tool methods.");
            }

            /* #3898 D1/D3: the ONE place the two-tier split happens, for every tool on every SKU. tools/list
               serves the head (plus the guide pointer) of a description carrying McpToolGuide.Marker, and the
               whole description of one that does not. The tail goes to the catalog get_tool_guide reads.
               Split throws on a malformed marker, so a bad conversion fails at registration, not on the wire. */
            var description = toolMethod.GetCustomAttribute<DescriptionAttribute>()?.Description;
            var served = description is null ? null : McpToolGuide.Served(description);
            catalog.Register(toolAttribute.Name ?? toolMethod.Name, description);

            /* Mirror the SDK's static-method registration (McpServerBuilderExtensions.WithTools<T>):
               Services = the DI provider so service-typed parameters are excluded from the schema and
               resolved per-request. The additions are SchemaCreateOptions and the served Description. */
            builder.Services.AddSingleton((Func<IServiceProvider, McpServerTool>)(services =>
                McpServerTool.Create(
                    toolMethod,
                    target: null,
                    options: new McpServerToolCreateOptions
                    {
                        Services = services,
                        SchemaCreateOptions = SchemaOptionsFor(services),
                        Description = served
                    })));
        }

        return builder;
    }

    /// <summary>
    /// The one <see cref="McpToolGuideCatalog"/> per service collection: found if an earlier registration
    /// added it, added as a singleton instance otherwise. Registered as a service, so <c>get_tool_guide</c>
    /// receives it by injection and it never appears in that tool's advertised schema.
    /// </summary>
    private static McpToolGuideCatalog GuideCatalogOf(IServiceCollection services)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(McpToolGuideCatalog)
                && !descriptor.IsKeyedService
                && descriptor.ImplementationInstance is McpToolGuideCatalog existing)
            {
                return existing;
            }
        }

        var catalog = new McpToolGuideCatalog();
        services.AddSingleton(catalog);
        return catalog;
    }

    /// <summary>
    /// Rewrites a single generated schema node into the subset Gemini's function-calling validator
    /// accepts: collapses nullable <c>type</c> unions to the single non-null type and removes the
    /// <c>default</c> keyword. Non-object nodes (e.g. boolean schemas) are returned unchanged.
    /// </summary>
    private static JsonNode CollapseGeminiUnsupportedKeywords(AIJsonSchemaTransformContext context, JsonNode node)
    {
        if (node is JsonObject obj)
        {
            /* Collapse "type": ["string","null"] -> "type": "string". */
            if (obj["type"] is JsonArray typeArray)
            {
                string? singleType = null;
                foreach (var element in typeArray)
                {
                    var typeName = element?.GetValue<string>();
                    if (typeName is not null && typeName != "null")
                    {
                        singleType = typeName;
                        break;
                    }
                }

                if (singleType is not null)
                {
                    obj["type"] = singleType;
                }
            }

            /* Strip the "default" keyword; Gemini rejects it. The .NET method default still applies
               when the argument is omitted, and the description documents it. */
            obj.Remove("default");
        }

        return node;
    }
}
