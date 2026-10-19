/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the web page gets when a person's plan undo is refused or fails (#5632 step 9, #4283).
///
/// <para>A failed undo's message is the monitored SQL Server's own exception text. The executor scrubs the
/// remediation secret from it, but it can still name hosts and addresses, and #4283 keeps exception text off
/// every web answer. So a failure answers a fixed sentence with its code, and a refusal keeps its own sentence,
/// which the gate and the undo decision wrote. <c>WebExceptionTextCensusTests</c> allow-lists the one
/// <c>undoRefusal.Message</c> read on the strength of these two tests.</para>
/// </summary>
public sealed class PlanUndoWebBodyTests
{
    private const string ServerWords = "A network-related error occurred while connecting to sqlbox-west on port 1433. Login failed for user 'remediation'.";

    [Fact]
    public void AFailedUndo_AnswersTheFixedSentence_NeverTheServersOwnWords()
    {
        var state = new JsonObject { ["forced_plan"] = 12 };
        var (body, status) = DarlingWebEndpoints.UndoRefusalBody(
            new PlanUndoAnswer(PlanUndoAnswer.Failed, "sign_in_failed", ServerWords, "EXEC sys.sp_query_store_unforce_plan 1, 12;", state));

        Assert.Equal(StatusCodes.Status502BadGateway, status);
        Assert.Equal(DarlingWebEndpoints.UndoFailedWords, (string?)body["error"]);
        Assert.Equal("sign_in_failed", (string?)body["code"]);
        Assert.Equal(PlanUndoAnswer.Failed, (string?)body["status"]);
        Assert.Equal(12, (int?)body["state"]?["forced_plan"]);
        Assert.DoesNotContain("sqlbox-west", body.ToJsonString(), System.StringComparison.Ordinal);
        Assert.DoesNotContain("port 1433", body.ToJsonString(), System.StringComparison.Ordinal);
        Assert.DoesNotContain("Login failed", body.ToJsonString(), System.StringComparison.Ordinal);
    }

    [Fact]
    public void ARefusedUndo_KeepsItsOwnSentence_AndItsStatus()
    {
        const string words = "Plan 14 is forced now, so the undo would overwrite a later change. Nothing was written.";
        var (refused, refusedStatus) = DarlingWebEndpoints.UndoRefusalBody(PlanUndoAnswer.Refused("changed_since", words));
        Assert.Equal(StatusCodes.Status409Conflict, refusedStatus);
        Assert.Equal(words, (string?)refused["error"]);
        Assert.Equal("changed_since", (string?)refused["code"]);

        Assert.Equal(StatusCodes.Status404NotFound, DarlingWebEndpoints.UndoRefusalBody(PlanUndoAnswer.Refused("not_found", "No such change.")).Status);
        Assert.Equal(StatusCodes.Status400BadRequest, DarlingWebEndpoints.UndoRefusalBody(PlanUndoAnswer.Refused("bad_login", "Bad name.")).Status);
    }
}
