/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Text.Json;
using ModelContextProtocol;

namespace PerformanceMonitor.Common;

/// <summary>One tool parameter as the tool method declares it: its CLR type, and whether it can take a null.</summary>
/// <param name="Type">The declared type. <c>int?</c> is <see cref="Nullable{T}"/> of <see cref="int"/>.</param>
/// <param name="AllowsNull">A nullable value type, or a reference type annotated as nullable.</param>
public sealed record McpParameterType(Type Type, bool AllowsNull);

/// <summary>
/// The CLR type and nullability of each parameter the caller supplies, per tool. Filled by
/// <c>McpSchemaCompat.WithGeminiCompatibleTools</c> as each tool is created (one instance per service collection,
/// found-or-added like <see cref="McpToolGuideCatalog"/>), and read by <see cref="McpUnknownArgumentGuard"/> through
/// the request's services.
///
/// <para>The served schema cannot stand in for this. It says <c>"integer"</c> for an <c>int</c>, a <c>long</c>, a
/// <c>short</c> and a <c>byte</c> alike, carries no <c>format</c>, and does not say which parameters take a null, while
/// the SDK's binder reads each value into the real type and throws on a value that type cannot hold. A parameter the
/// host resolves from DI (a data source, a logger) is left out, by the same rule that leaves it out of the schema
/// (<c>McpSchemaCompat.SchemaOptionsFor</c>), so only what a caller may send is here.</para>
/// </summary>
public sealed class McpToolParameterTypes
{
    private readonly ConcurrentDictionary<string, IReadOnlyDictionary<string, McpParameterType>> _tools =
        new(StringComparer.Ordinal);

    /// <summary>Records one tool's parameters, skipping any that <paramref name="include"/> rejects. A second
    /// registration of the same name replaces the first, so a host that builds its tools twice records the same
    /// answer twice.</summary>
    /// <param name="toolName">The name the tool is served under.</param>
    /// <param name="method">The tool method.</param>
    /// <param name="include">Whether a parameter is one the caller supplies; null includes every parameter.</param>
    public void Register(string toolName, MethodInfo method, Func<ParameterInfo, bool>? include)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(toolName);
        ArgumentNullException.ThrowIfNull(method);

        var context = new NullabilityInfoContext();
        var parameters = new Dictionary<string, McpParameterType>(StringComparer.Ordinal);

        foreach (var parameter in method.GetParameters())
        {
            if (parameter.Name is null || (include is not null && !include(parameter)))
            {
                continue;
            }

            var type = parameter.ParameterType;
            var allowsNull = type.IsValueType
                ? Nullable.GetUnderlyingType(type) is not null
                : context.Create(parameter).WriteState != NullabilityState.NotNull;

            parameters[parameter.Name] = new McpParameterType(type, allowsNull);
        }

        _tools[toolName] = parameters;
    }

    /// <summary>The parameters recorded for a tool, by name. False for a tool nothing recorded.</summary>
    public bool TryGet(string toolName, out IReadOnlyDictionary<string, McpParameterType> parameters)
    {
        if (_tools.TryGetValue(toolName, out var found))
        {
            parameters = found;
            return true;
        }

        parameters = new Dictionary<string, McpParameterType>();
        return false;
    }
}

/// <summary>
/// Decides whether the SDK's binder can read an argument's value into a parameter's declared type, and words the
/// refusal when it cannot.
///
/// <para><b>The decision is the binder's own.</b> The SDK binds a tool's arguments with
/// <c>System.Text.Json</c>, using <see cref="McpJsonUtilities.DefaultOptions"/> unless a tool is created with its
/// own. This reads the value into the same declared type with the same options and refuses exactly when
/// <see cref="JsonException"/> is thrown, which is what the binder would throw. So it needs no table of what each type
/// accepts (an <c>int</c> reads <c>"5"</c>, a <c>bool</c> does not read <c>"true"</c>, a <c>ulong</c> reads
/// 18446744073709551615, a string reads no number), and it cannot refuse a value the binder reads. Anything else the
/// serializer does (no converter for the type, say) is a case this cannot judge, and the value passes.</para>
/// </summary>
internal static class McpArgumentValueCheck
{
    /// <summary>
    /// The refusal sentence for one argument, or null when the binder reads <paramref name="value"/> into the declared
    /// type. It names the argument and the tool, says what the parameter takes, and quotes the value sent.
    /// </summary>
    internal static string? Problem(string toolName, string parameter, JsonElement value, McpParameterType declared)
    {
        if (CanBind(value, declared.Type))
        {
            return null;
        }

        var type = Nullable.GetUnderlyingType(declared.Type) ?? declared.Type;
        var takes = Takes(parameter, type);
        var sent = McpUnknownArgumentGuard.Shorten(value.GetRawText());
        var prefix = $"Argument '{parameter}' for tool '{toolName}' takes ";

        /* Only a value type with no null in it gets here with a null: a reference type, or a Nullable<T>, binds one. */
        if (value.ValueKind == JsonValueKind.Null)
        {
            return $"{prefix}{takes} and cannot be null, and the call sent null.";
        }

        if (Bounds(type) is { } bounds)
        {
            /* A whole number outside the type's range is too large or too small, which "no decimal point" would
               misstate; anything else the type cannot read (0.5, "abc", true) is not a whole number. */
            return OutOfRange(value, bounds) is string direction
                ? $"{prefix}{takes}{RangeOf(type)}, and the call sent {sent}, which is {direction}."
                : $"{prefix}{takes} with no decimal point, such as 1, and the call sent {sent}.";
        }

        return $"{prefix}{takes}, and the call sent {sent}.";
    }

    private static bool CanBind(JsonElement value, Type type)
    {
        try
        {
            JsonSerializer.Deserialize(value, type, McpJsonUtilities.DefaultOptions);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
        catch (NotSupportedException)
        {
            return true;
        }
        catch (InvalidOperationException)
        {
            return true;
        }
    }

    /// <summary>The range of an integer type, or null when <paramref name="type"/> is not one.</summary>
    private static (BigInteger Min, BigInteger Max)? Bounds(Type type) => Type.GetTypeCode(type) switch
    {
        TypeCode.SByte => (sbyte.MinValue, sbyte.MaxValue),
        TypeCode.Byte => (byte.MinValue, byte.MaxValue),
        TypeCode.Int16 => (short.MinValue, short.MaxValue),
        TypeCode.UInt16 => (ushort.MinValue, ushort.MaxValue),
        TypeCode.Int32 => (int.MinValue, int.MaxValue),
        TypeCode.UInt32 => (uint.MinValue, uint.MaxValue),
        TypeCode.Int64 => (long.MinValue, long.MaxValue),
        TypeCode.UInt64 => (ulong.MinValue, ulong.MaxValue),
        _ => null,
    };

    /// <summary>" from -2147483648 to 2147483647" for a type narrower than <see cref="long"/>, and nothing for a
    /// <see cref="long"/>, whose range a caller never runs into by accident.</summary>
    private static string RangeOf(Type type) =>
        type == typeof(long) || Bounds(type) is not { } bounds
            ? ""
            : string.Create(CultureInfo.InvariantCulture, $" from {bounds.Min} to {bounds.Max}");

    /// <summary>"too large" or "too small" when <paramref name="value"/> is a whole number (a JSON number with no
    /// fraction or exponent, or a string of digits with an optional sign) outside the type's range; otherwise null.</summary>
    private static string? OutOfRange(JsonElement value, (BigInteger Min, BigInteger Max) bounds)
    {
        var text = value.ValueKind switch
        {
            JsonValueKind.Number when value.GetRawText().AsSpan().IndexOfAny('.', 'e', 'E') < 0 => value.GetRawText(),
            JsonValueKind.String => value.GetString(),
            _ => null,
        };

        if (text is null
            || !BigInteger.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var whole))
        {
            return null;
        }

        return whole > bounds.Max ? "too large" : whole < bounds.Min ? "too small" : null;
    }

    /// <summary>What a parameter of the type takes, in words: "a whole number of hours", "true or false", "text".</summary>
    private static string Takes(string parameter, Type type)
    {
        if (Bounds(type) is not null)
        {
            return McpUnknownArgumentGuard.WholeNumberOf(parameter);
        }

        if (type == typeof(bool))
        {
            return "true or false";
        }

        if (type == typeof(string))
        {
            return "text";
        }

        if (type == typeof(char))
        {
            return "a single character as text";
        }

        if (type == typeof(double) || type == typeof(float) || type == typeof(decimal) || type == typeof(Half))
        {
            return "a number";
        }

        if (type.IsEnum)
        {
            return "one of " + string.Join(", ", Enum.GetNames(type));
        }

        if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
        {
            return "a date and time as text, such as 2026-10-02T14:30:00Z";
        }

        if (type == typeof(DateOnly))
        {
            return "a date as text, such as 2026-10-02";
        }

        if (type == typeof(TimeSpan))
        {
            return "a duration as text, such as 01:30:00";
        }

        if (type == typeof(Guid))
        {
            return "a GUID as text";
        }

        if (ElementTypeOf(type) is { } element)
        {
            return "a list (a JSON array) of " + Plural(Nullable.GetUnderlyingType(element) ?? element);
        }

        return "a value of type " + type.Name;
    }

    private static string Plural(Type element) =>
        Bounds(element) is not null ? "whole numbers"
        : element == typeof(bool) ? "true or false values"
        : element == typeof(string) ? "text values"
        : element == typeof(double) || element == typeof(float) || element == typeof(decimal) ? "numbers"
        : element.Name + " values";

    /// <summary>The element type of an array or of a generic list-like collection, or null for any other type.</summary>
    private static Type? ElementTypeOf(Type type)
    {
        if (type.IsArray)
        {
            return type.GetElementType();
        }

        if (!type.IsGenericType || type.GetGenericArguments().Length != 1)
        {
            return null;
        }

        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(ICollection<>)
            || definition == typeof(IEnumerable<>) || definition == typeof(IReadOnlyList<>)
            || definition == typeof(IReadOnlyCollection<>) || definition == typeof(HashSet<>)
            ? type.GetGenericArguments()[0]
            : null;
    }
}
