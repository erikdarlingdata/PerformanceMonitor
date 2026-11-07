/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7658: the kinds of record a card's buttons act on, as the card store writes them.</summary>
public static class SlackCardKinds
{
    public const string Proposal = "proposal";
    public const string Change = "change";
    public const string Report = "report";
    public const string Note = "note";
}

/// <summary>
/// #7658: the <c>action_id</c> of every button the bot posts. The number names the step of a two-step action: <c>_1</c> is the press on the
/// card, which answers the presser alone with the facts or the statement and a Confirm button; <c>_2</c> is that Confirm. A refusal, a verdict
/// and a note decision take one press.
/// </summary>
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

    /// <summary>Every action the handler takes. Any other <c>action_id</c> is ignored.</summary>
    public static IReadOnlyList<string> All { get; } =
        [ProposalApproveFirst, ProposalApproveConfirm, ProposalRefuse, UndoFirst, UndoConfirm, VerdictRight, VerdictPartly, VerdictWrong, NoteApprove, NoteReject];

    /// <summary>The confirming presses: their value is a <see cref="SlackConfirmValue"/> that names the card, since the press itself comes from
    /// the ephemeral answer and not from the card.</summary>
    public static bool IsConfirm(string? actionId) => actionId is ProposalApproveConfirm or UndoConfirm;

    /// <summary>The card kind an action belongs to, or null for an action the handler does not know.</summary>
    public static string? KindOf(string? actionId) => actionId switch
    {
        ProposalApproveFirst or ProposalApproveConfirm or ProposalRefuse => SlackCardKinds.Proposal,
        UndoFirst or UndoConfirm => SlackCardKinds.Change,
        VerdictRight or VerdictPartly or VerdictWrong => SlackCardKinds.Report,
        NoteApprove or NoteReject => SlackCardKinds.Note,
        _ => null,
    };

    /// <summary>The verdict word a verdict button records (<c>right</c>, <c>partly</c>, <c>wrong</c>), or null for another action.</summary>
    public static string? VerdictOf(string? actionId) => actionId switch
    {
        VerdictRight => "right",
        VerdictPartly => "partly",
        VerdictWrong => "wrong",
        _ => null,
    };
}

/// <summary>#7658: what a card's buttons act on, carried on the post content so the poster saves the card once Slack gives it a ts.</summary>
public sealed record SlackCardFacts(string Kind, string RecordId);

/// <summary>
/// #7658: the value of a Confirm button. The Confirm sits on an ephemeral answer, whose own channel and ts are not a card the service posted,
/// so the value names the card the first press came from: the record's id, the card's channel and ts, and for an undo the hash of the statement
/// the person was shown, so the confirming press runs only what was shown. <c>|</c> separates the parts; none of them holds one.
/// </summary>
public sealed record SlackConfirmValue(string RecordId, string Channel, string MessageTs, string? StatementHash)
{
    /// <summary>The button value.</summary>
    public string Format() => string.Join("|", RecordId, Channel, MessageTs, StatementHash ?? "");

    /// <summary>The value parsed, or null for a value that is not one.</summary>
    public static SlackConfirmValue? Parse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var parts = value.Split('|');
        if (parts.Length != 4 || parts[0].Length == 0 || parts[1].Length == 0 || parts[2].Length == 0)
        {
            return null;
        }

        return new SlackConfirmValue(parts[0], parts[1], parts[2], parts[3].Length == 0 ? null : parts[3]);
    }

    /// <summary>The first 16 hex characters of the SHA-256 of <paramref name="statement"/>: enough to tell one statement from another, short enough for a button.</summary>
    public static string HashOf(string statement) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(statement ?? "")))[..16];
}

/// <summary>#7658: the button blocks of the cards and the rule for which changes get Undo.</summary>
public static class SlackCards
{
    /// <summary>The line under every card with buttons: a press during a reconnect is lost, and Slack does not retry it.</summary>
    public const string PressAgain = "If nothing happens, press again.";

    /// <summary>The kinds of plan change a person can undo through the plan undo (a force, an unforce, a plan guide made or dropped).</summary>
    private static readonly HashSet<string> s_planUndo = new(StringComparer.Ordinal)
    {
        AgentChangeKinds.PlanForce, AgentChangeKinds.PlanUnforce, AgentChangeKinds.PlanGuideCreate, AgentChangeKinds.PlanGuideDrop,
    };

    /// <summary>Whether a done change of <paramref name="kind"/> gets an Undo button: the Level 2 kinds <see cref="AgentChangeUndo.UndoableKinds"/>
    /// names, and the plan changes the plan undo takes back. A kill, an eviction and a recompile have no undo and get none.</summary>
    public static bool HasUndo(string? kind) =>
        !string.IsNullOrEmpty(kind) && (AgentChangeUndo.UndoableKinds.Contains(kind) || s_planUndo.Contains(kind));

    /// <summary>One button. <paramref name="style"/> is <c>primary</c>, <c>danger</c> or null.</summary>
    public static object Button(string actionId, string text, string value, string? style = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actionId);
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        ArgumentNullException.ThrowIfNull(value);
        var button = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["type"] = "button",
            ["text"] = new { type = "plain_text", text, emoji = false },
            ["action_id"] = actionId,
            ["value"] = value,
        };
        if (!string.IsNullOrEmpty(style))
        {
            button["style"] = style;
        }

        return button;
    }

    /// <summary>An <c>actions</c> block holding <paramref name="buttons"/>. <paramref name="blockId"/> names the card's kind and record, so a press
    /// payload reads as what it is.</summary>
    public static object Actions(string blockId, params object[] buttons) => new { type = "actions", block_id = blockId, elements = buttons };

    /// <summary>A <c>context</c> block with one plain line.</summary>
    public static object Context(string mrkdwn) => new { type = "context", elements = new object[] { new { type = "mrkdwn", text = mrkdwn } } };
}
