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
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The call-tool guard checks each argument against the CLR type of its parameter, not only against the schema's word
/// "integer". The schema cannot tell an <c>int</c> from a <c>long</c> or a <c>byte</c>, and says nothing of
/// nullability, so a value the binder cannot read (an int past int range, null for a parameter that is not
/// nullable, a number for a string, a string for a boolean, an integer array with a fraction in it) used to reach the
/// SDK and come back as "An error occurred invoking ...", with no word on which argument was wrong. Each row of
/// <see cref="McpArgumentTypeRows"/> pins both halves: the binder on its own reads or rejects the value as the row
/// says, and the guard refuses exactly the values the binder rejects, by a message that names the argument.
/// </summary>
public sealed class McpArgumentTypeTests : IClassFixture<McpArgumentTypeTests.Hosts>
{
    /// <summary>The two in-process servers every row shares: one with the guard in, one with it left out.</summary>
    public sealed class Hosts : IAsyncLifetime
    {
        internal McpInProcessHost Guarded { get; private set; } = null!;

        internal McpInProcessHost Unguarded { get; private set; } = null!;

        public async ValueTask InitializeAsync()
        {
            Guarded = await McpWholeNumberArgumentTests.StartHostAsync(true, typeof(McpArgumentTypeProbeTools));
            Unguarded = await McpWholeNumberArgumentTests.StartHostAsync(false, typeof(McpArgumentTypeProbeTools));
        }

        public async ValueTask DisposeAsync()
        {
            await Guarded.DisposeAsync();
            await Unguarded.DisposeAsync();
        }
    }

    private readonly Hosts _hosts;

    public McpArgumentTypeTests(Hosts hosts) => _hosts = hosts;

    public static TheoryData<string, string, bool, string> Rows() => McpArgumentTypeRows.All();

    [Theory]
    [MemberData(nameof(Rows))]
    public async Task EachValue_IsRefusedByName_ExactlyWhenTheBinderCannotReadIt(
        string type, string raw, bool refused, string says)
    {
        var toolTypes = McpWholeNumberArgumentTests.RegisteredToolTypes()
            .Append(typeof(McpArgumentTypeProbeTools))
            .ToList();

        await McpArgumentTypeRows.CheckAsync(
            _hosts.Guarded, _hosts.Unguarded, toolTypes, type, raw, refused, says, TestContext.Current.CancellationToken);
    }

    /// <summary>The host records a type for every parameter each tool advertises, and the types agree with the schema,
    /// so the guard's typed check covers every shipped tool instead of quietly falling back to the schema alone.</summary>
    [Fact]
    public void EveryAdvertisedParameter_HasARecordedType_ThatAgreesWithTheSchema()
    {
        var problems = McpArgumentTypeRows.RecordedTypeProblems(_hosts.Guarded);

        Assert.True(problems.Count == 0, $"{problems.Count} problems:\n" + string.Join("\n", problems.Take(40)));
    }

    /// <summary>The table is only worth having if it reaches every CLR type the shipped tools declare, so a tool added
    /// with a type the table lacks fails here by name instead of passing through unchecked.</summary>
    [Fact]
    public void EveryClrTypeAShippedToolDeclares_IsOneTheTableCovers()
    {
        var covered = McpArgumentTypeRows.CoveredTypeKeys();
        var declared = McpArgumentTypeRows.ShippedParameterTypeKeys(
            _hosts.Guarded, McpWholeNumberArgumentTests.RegisteredToolTypes());

        var missing = declared.Where(key => !covered.Contains(key)).ToList();
        Assert.True(
            missing.Count == 0,
            "Shipped tools declare parameters of these types, which the table has no row for: " + string.Join(", ", missing));
    }
}
