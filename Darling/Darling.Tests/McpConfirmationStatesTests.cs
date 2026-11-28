/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>requestState</c> a confirmation question carries (#9562): a state is good for the tool and the arguments it was issued for,
/// for ten minutes, and once. The hosts' own tests go through the real endpoint. These hold the rule itself, including the clock.
/// </summary>
public sealed class McpConfirmationStatesTests
{
    private static IDictionary<string, JsonElement> Args(string json)
    {
        using var document = JsonDocument.Parse(json);
        var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in document.RootElement.EnumerateObject())
        {
            arguments[property.Name] = property.Value.Clone();
        }

        return arguments;
    }

    [Fact]
    public void AStateIsGoodForTheToolAndArgumentsItWasIssuedFor_InAnyArgumentOrder()
    {
        var states = new McpConfirmationStates(new ManualTimeProvider());
        var state = states.Issue("delete_rule", Args("{\"a\":1,\"b\":{\"x\":true,\"y\":[1,2]}}"));

        Assert.NotNull(states.Check("delete_rule", Args("{\"b\":{\"y\":[1,2],\"x\":true},\"a\":1}"), state));
    }

    [Theory]
    [InlineData("delete_other", "{\"a\":1}")]
    [InlineData("delete_rule", "{\"a\":2}")]
    [InlineData("delete_rule", "{\"a\":1,\"b\":null}")]
    [InlineData("delete_rule", "{}")]
    public void AStateIsRefusedForAnotherToolOrOtherArguments(string tool, string arguments)
    {
        var states = new McpConfirmationStates(new ManualTimeProvider());
        var state = states.Issue("delete_rule", Args("{\"a\":1}"));

        Assert.Null(states.Check(tool, Args(arguments), state));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-a-state")]
    [InlineData("!!!!")]
    public void NoStateAndJunkAreRefused(string? junk)
    {
        var states = new McpConfirmationStates(new ManualTimeProvider());

        Assert.Null(states.Check("delete_rule", Args("{\"a\":1}"), junk));
    }

    [Fact]
    public void AStateAlteredByOneCharacterIsRefused()
    {
        var states = new McpConfirmationStates(new ManualTimeProvider());
        var arguments = Args("{\"a\":1}");
        var state = states.Issue("delete_rule", arguments);

        for (var position = 0; position < state.Length; position += 7)
        {
            var altered = state.ToCharArray();
            altered[position] = altered[position] == 'A' ? 'B' : 'A';

            Assert.Null(states.Check("delete_rule", arguments, new string(altered)));
        }
    }

    [Fact]
    public void AStateIssuedByAnotherProcessIsRefused()
    {
        var arguments = Args("{\"a\":1}");
        var state = new McpConfirmationStates(new ManualTimeProvider()).Issue("delete_rule", arguments);

        Assert.Null(new McpConfirmationStates(new ManualTimeProvider()).Check("delete_rule", arguments, state));
    }

    [Fact]
    public void AStateIsRefusedOnceItIsTenMinutesOld_AndGoodUntilThen()
    {
        var clock = new ManualTimeProvider();
        var states = new McpConfirmationStates(clock);
        var arguments = Args("{\"a\":1}");
        var state = states.Issue("delete_rule", arguments);

        clock.Advance(McpConfirmationStates.MaxAge - TimeSpan.FromSeconds(1));
        Assert.NotNull(states.Check("delete_rule", arguments, state));

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null(states.Check("delete_rule", arguments, state));
    }

    [Fact]
    public void AStateIsSpentByTheFirstAnswer_AndOnlyTheFirst()
    {
        var states = new McpConfirmationStates(new ManualTimeProvider());
        var arguments = Args("{\"a\":1}");
        var state = states.Issue("delete_rule", arguments);
        var ticket = states.Check("delete_rule", arguments, state)!.Value;

        Assert.True(states.TrySpend(ticket));
        Assert.False(states.TrySpend(ticket));
        Assert.False(states.TrySpend(states.Check("delete_rule", arguments, state)!.Value));
    }

    [Fact]
    public void TwoQuestionsForTheSameCallCarryDifferentStates_SoSpendingOneLeavesTheOther()
    {
        var states = new McpConfirmationStates(new ManualTimeProvider());
        var arguments = Args("{\"a\":1}");
        var first = states.Issue("delete_rule", arguments);
        var second = states.Issue("delete_rule", arguments);

        Assert.NotEqual(first, second);
        Assert.True(states.TrySpend(states.Check("delete_rule", arguments, first)!.Value));
        Assert.True(states.TrySpend(states.Check("delete_rule", arguments, second)!.Value));
    }
}
