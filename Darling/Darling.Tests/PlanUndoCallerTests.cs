/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The login undo_plan_change records as the person who undid a change: <c>mcp</c> and the token by name or as <c>legacy token</c>, so the
/// journal row says which token undid it, and never the token's fingerprint. The journal row itself is read in
/// <c>PlanForceToolLiveTests</c> (the MCP undo test), which needs a SQL Server.
/// </summary>
public sealed class PlanUndoCallerTests
{
    [Theory]
    [InlineData("named token ops", "mcp named token ops")]
    [InlineData("no token", "mcp no token")]
    [InlineData(null, "mcp token unknown")]
    public void TheUndoLogin_NamesTheTokenByName(string? ambientToken, string expected)
    {
        try
        {
            ReadCaller.SetAmbientMcpToken(ambientToken);
            var login = DarlingMcpPlanUndoTools.Caller();

            Assert.Equal(expected, login);
            Assert.Null(InvestigationPersonWrites.ValidateActor(InvestigationPersonWrites.PersonActor(login)));
        }
        finally
        {
            ReadCaller.SetAmbientMcpToken(null);
        }
    }

    [Fact]
    public void TheUndoLogin_ForTheLegacyToken_HoldsNoFingerprint()
    {
        try
        {
            ReadCaller.SetAmbientMcpToken(ReadCaller.OfMcpToken("a-legacy-bearer-token"));
            var login = DarlingMcpPlanUndoTools.Caller();

            Assert.Equal("mcp legacy token", login);
            Assert.DoesNotMatch("[0-9a-f]{8}", login);
        }
        finally
        {
            ReadCaller.SetAmbientMcpToken(null);
        }
    }

    [Fact]
    public void TheUndoLogin_FallsBackToPlainMcp_WhenTheTokenNameIsNotAPersonName()
    {
        try
        {
            ReadCaller.SetAmbientMcpToken("named token " + new string('x', AgentChangeUndo.LoginMaxLength));

            Assert.Equal(DarlingMcpPlanUndoTools.Login, DarlingMcpPlanUndoTools.Caller());
        }
        finally
        {
            ReadCaller.SetAmbientMcpToken(null);
        }
    }
}
