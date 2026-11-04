/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The <c>recommendation</c> sentence of one PostgreSQL <c>audit_config</c> row (#7447).
///
/// <para><c>audit_config</c> renders every <c>CONFIG_PG_*</c> fact, healthy or not, but most advice blocks are
/// written for the case where the fact fired ("autovacuum is OFF", "block read and write times are not being
/// measured"). Printed beside a healthy value they state something false. So a row at severity 0 says what the
/// setting is in plain words and carries no advice, with three exceptions that are context by design: their block
/// describes the setting rather than a fault, and reads the same whether or not a finding fired.</para>
/// </summary>
internal static class PgConfigAuditText
{
    /// <summary>
    /// The keys whose advice block is context, not a fault. <c>checkpoint_timeout</c> and <c>wal_compression</c>
    /// are stated beside the checkpoint-pressure finding and never root a card, and <c>work_mem</c> is graded on
    /// spill evidence, so its block reads the same at any value.
    /// </summary>
    private static readonly HashSet<string> s_contextKeys = new(StringComparer.Ordinal)
    {
        PgTargetFactKeys.ConfigCheckpointTimeout,
        PgTargetFactKeys.ConfigWalCompression,
        PgTargetFactKeys.ConfigWorkMem,
    };

    private const string AuroraCheckpointSentence =
        "Not applicable on Aurora PostgreSQL: the storage layer owns checkpointing, so this value is what the parameter group holds rather than a schedule the engine keeps — read write pressure through get_pg_io_stats and the instance CPU and wait findings instead.";

    /// <param name="fact">The scored fact.</param>
    /// <param name="setting">The setting name the row shows.</param>
    /// <param name="currentValue">The value as the row shows it.</param>
    /// <param name="advice">The composed advice, or null when no block exists for the key.</param>
    /// <param name="notApplicable">The row's status is not_applicable (an Aurora target for this setting).</param>
    /// <param name="isAuroraCheckpointTimeout">The Aurora rider applies: the row is checkpoint_timeout on an Aurora target.</param>
    public static string Compose(
        Fact fact, string setting, string currentValue, AdviceBlock? advice, bool notApplicable, bool isAuroraCheckpointTimeout)
    {
        ArgumentNullException.ThrowIfNull(fact);

        string text;
        if (advice is null)
        {
            /* No key reaches here at severity > 0 today (the three connection settings, which have no block, score 0).
               A future key added without an advice arm says so rather than render an empty sentence. */
            text = fact.Severity > 0
                ? $"{Plain(fact, setting, currentValue)} No advice is composed for this setting yet, and get_analysis_facts carries the fact and its metadata."
                : Plain(fact, setting, currentValue);
        }
        else if (fact.Severity > 0 || notApplicable || s_contextKeys.Contains(fact.Key))
        {
            text = Join(advice.Headline, advice.Remediation);
        }
        else
        {
            text = Plain(fact, setting, currentValue);
        }

        return isAuroraCheckpointTimeout ? Join(text, AuroraCheckpointSentence) : text;
    }

    /// <summary>The setting and its value in plain words: no advice, nothing written for a fired check.</summary>
    private static string Plain(Fact fact, string setting, string currentValue) => fact.Key switch
    {
        PgTargetFactKeys.ConfigStatStatementsEviction => $"pg_stat_statements evicted entries in {currentValue}.",
        PgTargetFactKeys.ConfigMemoryOvercommit => $"The configured worst case ({setting}) is {currentValue}.",
        _ => $"{setting} is {currentValue}.",
    };

    /// <summary>Two sentences with exactly one space between them, and a full stop on the first only when it has no
    /// sentence-ending mark already.</summary>
    internal static string Join(string first, string second)
    {
        var head = first.TrimEnd();
        if (head.Length == 0)
            return second.Trim();
        if (head[^1] is not ('.' or '!' or '?'))
            head += ".";
        var tail = second.Trim();
        return tail.Length == 0 ? head : head + " " + tail;
    }
}
