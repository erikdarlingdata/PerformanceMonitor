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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Which AWS roles the service may assume for an Amazon RDS or Aurora target (#5452). The list comes from darling.json:
/// the top-level <c>allowedAwsRoles</c> setting plus the <c>awsRoleArn</c> of every <c>servers[]</c> entry. A role
/// saved from the web, MCP, the desktop viewer or <c>--add-server</c> runs only if it is on the list. An entry is
/// either a full role ARN, matched exactly (ordinal, case-sensitive), or an AWS account id, which allows every role
/// in that account. A bare 12-digit id is an account in the <c>aws</c> partition; an account in another partition is
/// written with it, as <c>aws-cn:123456789012</c> or <c>aws-us-gov:123456789012</c>. <see cref="Empty"/> allows none.
///
/// <para>darling.json is read once at start, so an edit to the list applies on the next restart.
/// <see cref="Current"/> is set once, where the worker builds the collector runner, the same way as
/// <see cref="DarlingPasswordKey.Current"/>. Tests pass a list to the overloads and never set it.</para>
/// </summary>
public sealed class AwsRoleAllowlist
{
    /// <summary>The error text for an <c>allowedAwsRoles</c> entry that is neither a role ARN nor a 12-digit account id.</summary>
    public const string InvalidEntryMessage =
        "Each entry must be an IAM role ARN, such as arn:aws:iam::123456789012:role/darling-monitor, or a 12-digit AWS account id, such as 123456789012 (a China or GovCloud account is written with its partition, such as aws-cn:123456789012).";

    /* A bare id is the aws partition; "aws-cn:<id>" and "aws-us-gov:<id>" name the others. */
    private static readonly Regex AccountIdPattern = new(
        @"^(?:(?<partition>aws(?:-[a-z]+)*):)?(?<account>[0-9]{12})\z",
        RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture, TimeSpan.FromSeconds(1));

    private static readonly Regex RoleAccountPattern = new(
        @"^arn:(?<partition>aws(?:-[a-z]+)*):iam::(?<account>[0-9]{12}):role/",
        RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture, TimeSpan.FromSeconds(1));

    private readonly HashSet<string> _arns;
    private readonly HashSet<string> _accounts;

    private AwsRoleAllowlist(HashSet<string> arns, HashSet<string> accounts)
    {
        _arns = arns;
        _accounts = accounts;
    }

    /// <summary>A list that allows no role.</summary>
    public static AwsRoleAllowlist Empty { get; } = new(new HashSet<string>(StringComparer.Ordinal), new HashSet<string>(StringComparer.Ordinal));

    private static volatile AwsRoleAllowlist _current = Empty;

    /// <summary>The list in use. It starts as <see cref="Empty"/> until the worker sets it at start.</summary>
    public static AwsRoleAllowlist Current
    {
        get => _current;
        internal set => _current = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>How many entries the list holds: role ARNs plus account ids.</summary>
    public int Count => _arns.Count + _accounts.Count;

    /// <summary>
    /// The error text for one <c>allowedAwsRoles</c> entry that is not usable, or null when it is a role ARN or a
    /// 12-digit account id. Pass the entry as written: it is trimmed here, and a blank entry is an error.
    /// </summary>
    public static string? ValidateEntry(string? entry)
    {
        var value = AwsRoleSettings.Normalize(entry);
        if (value is null)
        {
            return InvalidEntryMessage;
        }

        if (AccountKey(value) is not null)
        {
            return null;
        }

        return AwsRoleSettings.ValidateRole(value) is null ? null : InvalidEntryMessage;
    }

    /// <summary>
    /// Builds a list from entries. Each is trimmed, a blank or unusable one is dropped (darling.json validation
    /// refuses it at load, so none arrives from there), and a repeat is counted once.
    /// </summary>
    public static AwsRoleAllowlist From(IEnumerable<string?>? entries)
    {
        var arns = new HashSet<string>(StringComparer.Ordinal);
        var accounts = new HashSet<string>(StringComparer.Ordinal);
        foreach (var raw in entries ?? Array.Empty<string?>())
        {
            var value = AwsRoleSettings.Normalize(raw);
            if (value is null || ValidateEntry(value) is not null)
            {
                continue;
            }

            var accountKey = AccountKey(value);
            if (accountKey is not null)
            {
                accounts.Add(accountKey);
            }
            else
            {
                arns.Add(value);
            }
        }

        return arns.Count == 0 && accounts.Count == 0 ? Empty : new AwsRoleAllowlist(arns, accounts);
    }

    /// <summary>
    /// The list darling.json makes: <c>allowedAwsRoles</c> plus the <c>awsRoleArn</c> of every <c>servers[]</c> entry,
    /// so a server written in the file runs without being listed twice.
    /// </summary>
    public static AwsRoleAllowlist FromConfig(DarlingConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        var entries = new List<string?>();
        if (config.AllowedAwsRoles is not null)
        {
            entries.AddRange(config.AllowedAwsRoles);
        }

        if (config.Servers is not null)
        {
            entries.AddRange(config.Servers.Where(s => s is not null).Select(s => s.AwsRoleArn));
        }

        return From(entries);
    }

    /// <summary>
    /// True when <paramref name="roleArn"/> is on this list: an entry equals it exactly, or an account entry equals
    /// the partition and account id in it. A blank or null role is never allowed.
    /// </summary>
    public bool IsAllowed(string? roleArn)
    {
        var role = AwsRoleSettings.Normalize(roleArn);
        if (role is null)
        {
            return false;
        }

        if (_arns.Contains(role))
        {
            return true;
        }

        if (_accounts.Count == 0)
        {
            return false;
        }

        var match = RoleAccountPattern.Match(role);
        return match.Success && _accounts.Contains(match.Groups["partition"].Value + ":" + match.Groups["account"].Value);
    }

    /// <summary>
    /// The partition and account of an account entry, as <c>aws:123456789012</c>, or null when the entry is not an
    /// account entry. A bare 12-digit id is in the <c>aws</c> partition.
    /// </summary>
    private static string? AccountKey(string value)
    {
        var match = AccountIdPattern.Match(value);
        if (!match.Success)
        {
            return null;
        }

        var partition = match.Groups["partition"].Success ? match.Groups["partition"].Value : "aws";
        return partition + ":" + match.Groups["account"].Value;
    }

    /// <summary><see cref="IsAllowed(string?)"/> against the list in <paramref name="allowlist"/>, so a test names the list it means.</summary>
    public static bool IsAllowed(string? roleArn, AwsRoleAllowlist allowlist)
    {
        ArgumentNullException.ThrowIfNull(allowlist);
        return allowlist.IsAllowed(roleArn);
    }

    /// <summary><see cref="IsAllowed(string?)"/> against <see cref="Current"/>.</summary>
    public static bool IsAllowedNow(string? roleArn) => Current.IsAllowed(roleArn);
}
