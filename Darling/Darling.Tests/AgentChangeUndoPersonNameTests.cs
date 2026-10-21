/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The one shape of a person's name (#6265): <see cref="AgentChangeUndo.PersonActor"/> serves the undo path, the agent switch, the
/// Viewer's writes (a Windows user name, with no domain), and the web's person writes (a sign-in name that can be an email or an
/// Auth0 subject). A real name is accepted in normal form C; a name that would pass for another person's, or draw nothing, is refused.
/// </summary>
public sealed class AgentChangeUndoPersonNameTests
{
    public static TheoryData<string, string> Accepted => new()
    {
        { "John Smith", "person:John Smith" },
        { "Jos\u00E9", "person:Jos\u00E9" },
        { "Jose\u0301", "person:Jos\u00E9" },
        { "erik+ops@example.com", "person:erik+ops@example.com" },
        { "o'brien@example.com", "person:o'brien@example.com" },
        { "auth0|abc123", "person:auth0|abc123" },
        { "CORP\\jsmith", "person:CORP\\jsmith" },
        { "  erik  ", "person:erik" },
        { "Zo\u00EB M\u00FCller-\u00C5berg", "person:Zo\u00EB M\u00FCller-\u00C5berg" },
        { "\u5C71\u7530\u592A\u90CE", "person:\u5C71\u7530\u592A\u90CE" },
        { "x\u0323\u0302", "person:x\u0323\u0302" },
        { "a.:_-@\\!#$%&'*+/=?^`{|}~", "person:a.:_-@\\!#$%&'*+/=?^`{|}~" },
        { new string('a', AgentChangeUndo.LoginMaxLength), "person:" + new string('a', AgentChangeUndo.LoginMaxLength) },
    };

    public static TheoryData<string> Refused => new()
    {
        "",
        "   ",
        new string('a', AgentChangeUndo.LoginMaxLength + 1),
        "erik  smith",
        "erik\u200Bsmith",
        "erik\u200D",
        "erik\u202Esmith",
        "erik\u2066smith",
        "erik\uFEFFsmith",
        "erik\u00ADsmith",
        "erik\u3164",
        "\u115Ferik",
        "erik\u1160",
        "erik\uFFA0",
        "erik<smith",
        "erik>smith",
        "erik;smith",
        "erik,smith",
        "erik(smith)",
        "erik[smith]",
        "erik\"smith",
        "erik\tsmith",
        "erik\nsmith",
        "erik\0smith",
        "erik\u00A0smith",
        "erik\u3000smith",
        "erik\u2003smith",
        "erik\uD83D\uDE00",
        "erik\uD800",
        "\u0301erik",
        "erik \u0301",
        "1\u0301",
        "x\u0323\u0302\u0301",
        "erik\u034F",
        "erik\uFE0F",
        "\u0661\u0662",
    };

    [Theory]
    [MemberData(nameof(Accepted))]
    public void ARealName_IsAccepted_InNormalFormC(string login, string actor) => Assert.Equal(actor, AgentChangeUndo.PersonActor(login));

    [Theory]
    [MemberData(nameof(Refused))]
    public void ANameThatDrawsNothingOrPassesForAnother_IsRefused_WithTheShapeSentence(string login)
    {
        var refused = Assert.Throws<ArgumentException>(() => AgentChangeUndo.PersonActor(login));
        Assert.StartsWith(AgentChangeUndo.LoginRefusal, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStoresPersonRule_AgreesWithTheUndoShape_ForEveryName()
    {
        foreach (var accepted in new[] { "John Smith", "Jose\u0301", "erik+ops@example.com", "auth0|abc123", new string('a', AgentChangeUndo.LoginMaxLength) })
        {
            Assert.Null(InvestigationPersonWrites.ValidateActor(InvestigationPersonWrites.PersonActor(accepted)));
        }

        foreach (var refused in new[] { new string('a', AgentChangeUndo.LoginMaxLength + 1), "erik  smith", "erik\u200B", "erik\u3164", "a<b", "a;b" })
        {
            Assert.NotNull(InvestigationPersonWrites.ValidateActor(InvestigationPersonWrites.PersonActor(refused)));
            Assert.NotNull(InvestigationPersonWrites.ValidateLogin(refused));
        }

        /* The actor must already be in normal form C and trimmed: the guard does not repair a name. */
        Assert.NotNull(InvestigationPersonWrites.ValidateActor("person:Jose\u0301"));
        Assert.NotNull(InvestigationPersonWrites.ValidateActor("person: erik"));
    }
}
