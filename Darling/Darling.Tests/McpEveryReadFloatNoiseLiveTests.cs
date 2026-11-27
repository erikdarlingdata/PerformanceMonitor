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
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: reads only the scratch database McpEveryReadStore mints. */
/// <summary>
/// No number in an MCP reply carries binary float artefacts: more than <see cref="McpReplyFloatNoiseLiveTests.MaxDecimals"/> decimals
/// (<see cref="McpReplyFloatNoiseLiveTests.NoisyNumbers"/>). This runs that rule across every read the MCP server lists
/// (<see cref="McpEveryReadStore"/>), over a seed whose decimal columns hold tenths.
///
/// <para>Each read is called over the window that holds every seeded row, and then once more for each optional parameter that narrows
/// it to one thing, because a read can build a narrowed answer on a path of its own. The value for such a parameter is one the read's
/// own answer gave under that name, else the seed's value for the column of that name.</para>
///
/// <para>No read is left out by name. A read that does not answer data here is reported with the reason, taken from its own answer, and
/// <see cref="ScannedShare"/> holds a floor under the share of the listed reads whose figures were scanned.</para>
/// </summary>
[Collection("gap-cache-serial")]
public sealed class McpEveryReadFloatNoiseLiveTests : IClassFixture<McpEveryReadStore>
{
    /// <summary>The least share of the listed reads that answer data and are scanned.</summary>
    private const double ScannedShare = 0.6;

    /// <summary>The parameters that set the server, the window and the page, which are not a narrowing to one thing.</summary>
    private static readonly HashSet<string> s_notNarrowing = new(StringComparer.Ordinal) { "server_name", "hours_back", "as_of", "limit", "top", "offset" };

    private readonly McpEveryReadStore _store;

    public McpEveryReadFloatNoiseLiveTests(McpEveryReadStore store) => _store = store;

    [Fact]
    public async Task NoReply_CarriesANumberWithFloatNoise_OnEveryRead_WideOrNarrowedToOneThing()
    {
        Assert.SkipWhen(!_store.Available, McpEveryReadStore.SkipReason);

        var noisy = new List<string>();
        var notScanned = new List<string>();
        var notNarrowed = new List<string>();
        var scanned = 0;
        var narrowed = 0;

        foreach (var tool in _store.Tools)
        {
            var (arguments, answer) = await _store.WideReadAsync(tool);
            if (answer.NotDataReason is { } reason)
            {
                notScanned.Add($"{tool.Name}: {reason}");
                continue;
            }

            scanned++;
            Scan(tool.Name, arguments, answer.Text, noisy);

            foreach (var parameter in tool.Parameters)
            {
                if (parameter.Required || s_notNarrowing.Contains(parameter.Name) || parameter.Type is not ("string" or "integer"))
                {
                    continue;
                }

                var value = FirstValueNamed(answer.Text, parameter.Name, parameter.Type) ?? _store.ValueFor(parameter);
                if (value is null)
                {
                    notNarrowed.Add(tool.Name + "." + parameter.Name);
                    continue;
                }

                var (narrowArguments, narrowAnswer) = await _store.WideReadAsync(tool, new Dictionary<string, object?> { [parameter.Name] = value });
                if (narrowAnswer.NotDataReason is null)
                {
                    narrowed++;
                    Scan(tool.Name, narrowArguments, narrowAnswer.Text, noisy);
                }
            }
        }

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{_store.Tools.Count} tools listed, {scanned} answered data and were scanned, {narrowed} narrowed answers were scanned too."
            + Environment.NewLine + "Not scanned:" + Environment.NewLine + string.Join(Environment.NewLine, notScanned)
            + Environment.NewLine + "Narrowing parameters neither the read's answer nor the seed has a value for:" + Environment.NewLine + string.Join(", ", notNarrowed));

        Assert.True(noisy.Count == 0,
            $"{noisy.Count} number(s) with more than {McpReplyFloatNoiseLiveTests.MaxDecimals} decimals. Round each where its reply is built:"
            + Environment.NewLine + string.Join(Environment.NewLine, noisy.Take(80)));
        Assert.True(scanned >= _store.Tools.Count * ScannedShare,
            $"Only {scanned} of the {_store.Tools.Count} listed reads answered data, under the floor of {ScannedShare:P0}. Not scanned:"
            + Environment.NewLine + string.Join(Environment.NewLine, notScanned));
    }

    /// <summary>Adds the reply's noisy numbers to <paramref name="noisy"/>, one line for each field of the tool whatever row it is in.</summary>
    private static void Scan(string tool, IReadOnlyDictionary<string, object?> arguments, string reply, List<string> noisy)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var hit in McpReplyFloatNoiseLiveTests.NoisyNumbers(reply))
        {
            var path = hit[..hit.IndexOf(" = ", StringComparison.Ordinal)];
            var field = path[(path.LastIndexOf('.') + 1)..];
            if (seen.Add(field))
            {
                noisy.Add($"{tool}: {hit} [{McpEveryReadStore.Describe(arguments)}]");
            }
        }
    }

    /// <summary>The first value a reply gives under <paramref name="name"/>, at any depth, of the JSON type the parameter takes.</summary>
    private static object? FirstValueNamed(string reply, string name, string type)
    {
        try
        {
            using var document = JsonDocument.Parse(reply);
            return Find(document.RootElement, name, type);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static object? Find(JsonElement element, string name, string type)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (property.NameEquals(name))
                    {
                        if (type == "string" && property.Value.ValueKind == JsonValueKind.String && property.Value.GetString() is { Length: > 0 } text) return text;
                        if (type == "integer" && property.Value.ValueKind == JsonValueKind.Number && property.Value.TryGetInt64(out var whole)) return whole;
                    }

                    if (Find(property.Value, name, type) is { } found) return found;
                }

                return null;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    if (Find(item, name, type) is { } found) return found;
                }

                return null;
            default:
                return null;
        }
    }
}
