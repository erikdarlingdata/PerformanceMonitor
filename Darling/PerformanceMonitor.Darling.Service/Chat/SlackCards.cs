/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7658: the kinds of record a card's buttons act on.</summary>
public static class SlackCardKinds
{
    public const string Proposal = "proposal";
    public const string Change = "change";
    public const string Report = "report";
    public const string Note = "note";
}

/// <summary>#7658: the <c>action_id</c> of every button the bot posts.</summary>
public static class SlackCardActions
{
    public const string ProposalApproveFirst = "proposal_approve_1";
    public const string ProposalApproveConfirm = "proposal_approve_2";
    public const string ProposalRefuse = "proposal_refuse";
    public const string UndoFirst = "undo_1";
    public const string UndoConfirm = "undo_2";
    public const string VerdictRight = "verdict_right";
    public const string VerdictPartly = "verdict_partly";
    public const string VerdictWrong = "verdict_wrong";
    public const string NoteApprove = "note_approve";
    public const string NoteReject = "note_reject";

    public static IReadOnlyList<string> All { get; } = [];

    public static bool IsConfirm(string? actionId) => false;

    public static string? KindOf(string? actionId) => null;

    public static string? VerdictOf(string? actionId) => null;
}

/// <summary>#7658: what a card's buttons act on.</summary>
public sealed record SlackCardFacts(string Kind, string RecordId);

/// <summary>#7658: the value of a Confirm button.</summary>
public sealed record SlackConfirmValue(string RecordId, string Channel, string MessageTs, string? StatementHash)
{
    public string Format() => "";

    public static SlackConfirmValue? Parse(string? value) => null;

    public static string HashOf(string statement) => "";
}

/// <summary>#7658: the button blocks of the cards.</summary>
public static class SlackCards
{
    public const string PressAgain = "If nothing happens, press again.";

    public static bool HasUndo(string? kind) => false;

    public static object Button(string actionId, string text, string value, string? style = null) => new { };

    public static object Actions(string blockId, params object[] buttons) => new { };

    public static object Context(string mrkdwn) => new { };
}
