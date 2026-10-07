/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The rules for the per-server AWS role (#5452): which IAM role Darling assumes to reach an Amazon RDS or Aurora
/// target, and the optional external ID that goes with it. One validator for every surface that takes the two
/// values (darling.json, the MCP and web edit paths, the desktop viewer), so they cannot disagree. The store's
/// <c>config_monitored_servers_aws_role_check</c> is a coarser backstop for a blind write; the exact rules live
/// here.
///
/// <para>Both values are trimmed, and a blank value is "not set" (<see cref="Normalize"/>). The patterns are ASCII
/// only, with no <c>\w</c>: in .NET <c>\w</c> matches Unicode letters, and AWS does not.</para>
/// </summary>
public static class AwsRoleSettings
{
    /// <summary>Shortest and longest role ARN accepted, in characters.</summary>
    public const int RoleArnMinLength = 20;

    /// <summary>The longest a role ARN may be.</summary>
    public const int RoleArnMaxLength = 2048;

    /// <summary>The longest IAM role path, slashes included.</summary>
    public const int RolePathMaxLength = 512;

    /// <summary>The longest IAM role name.</summary>
    public const int RoleNameMaxLength = 64;

    /// <summary>The shortest external ID AWS accepts.</summary>
    public const int ExternalIdMinLength = 2;

    /// <summary>The longest external ID AWS accepts.</summary>
    public const int ExternalIdMaxLength = 1224;

    /// <summary>The role ARN is malformed.</summary>
    public const string InvalidRoleMessage =
        "The AWS role must be an IAM role ARN, such as arn:aws:iam::123456789012:role/darling-monitor.";

    /// <summary>The external ID is malformed.</summary>
    public const string InvalidExternalIdMessage =
        "The external ID must be 2 to 1224 characters: letters, digits and _ + = , . @ : / - with no spaces.";

    /// <summary>An external ID was given with no role.</summary>
    public const string ExternalIdNeedsRoleMessage =
        "An external ID needs an AWS role ARN. Set the role, or clear the external ID.";

    /// <summary>A role was given for a target that is not PostgreSQL.</summary>
    public const string RoleNeedsPostgresMessage =
        "An AWS role applies only to a PostgreSQL target on Amazon RDS or Aurora.";

    /// <summary>The role changed while a stored external ID would have stayed with it unseen.</summary>
    public const string RoleChangeNeedsExternalIdMessage =
        "Changing the AWS role needs the external ID with it: send the external ID again, or clear it.";

    private static readonly Regex RoleArnPattern = new(
        @"^arn:aws(?:-[a-z]+)*:iam::[0-9]{12}:role/(?<rest>.+)\z",
        RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture, TimeSpan.FromSeconds(1));

    private static readonly Regex RoleNamePattern = new(
        @"^[A-Za-z0-9_+=,.@-]{1,64}\z",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private static readonly Regex PathSegmentPattern = new(
        @"^[\x21-\x2E\x30-\x7E]+\z",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private static readonly Regex ExternalIdPattern = new(
        @"^[A-Za-z0-9_+=,.@:/-]{2,1224}\z",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>Trims a value, and turns a blank one into null ("not set").</summary>
    public static string? Normalize(string? value)
    {
        var trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) ? null : trimmed;
    }

    /// <summary>
    /// The error text for a malformed role ARN, or null when it is fine or not set. Pass a value already through
    /// <see cref="Normalize"/>. Accepts an IAM role in the <c>aws</c> partition or an <c>aws-*</c> one (China,
    /// GovCloud), with an optional path.
    /// </summary>
    public static string? ValidateRole(string? role)
    {
        if (role is null)
        {
            return null;
        }

        if (role.Length < RoleArnMinLength || role.Length > RoleArnMaxLength)
        {
            return InvalidRoleMessage;
        }

        var match = RoleArnPattern.Match(role);
        if (!match.Success)
        {
            return InvalidRoleMessage;
        }

        // role/<path segments>/<name>: the last piece is the name, any earlier ones are the path.
        var parts = match.Groups["rest"].Value.Split('/');
        if (!RoleNamePattern.IsMatch(parts[^1]))
        {
            return InvalidRoleMessage;
        }

        var pathLength = 0;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            if (!PathSegmentPattern.IsMatch(parts[i]))
            {
                return InvalidRoleMessage;
            }

            pathLength += parts[i].Length + 1;
        }

        if (pathLength > 0 && pathLength + 1 > RolePathMaxLength)
        {
            return InvalidRoleMessage;
        }

        return null;
    }

    /// <summary>The error text for a malformed external ID, or null when it is fine or not set.</summary>
    public static string? ValidateExternalId(string? externalId)
    {
        if (externalId is null)
        {
            return null;
        }

        return ExternalIdPattern.IsMatch(externalId) ? null : InvalidExternalIdMessage;
    }

    /// <summary>
    /// The first error in a role and external ID pair, or null when the pair is fine: each value's own format,
    /// then an external ID with no role. Pass values already through <see cref="Normalize"/>.
    /// </summary>
    public static string? ValidatePair(string? role, string? externalId)
    {
        return ValidateRole(role)
            ?? ValidateExternalId(externalId)
            ?? (externalId is not null && role is null ? ExternalIdNeedsRoleMessage : null);
    }

    /// <summary>
    /// True when an edit changes the role to a new one while the target has an external ID stored and the edit
    /// sent none: the old ID would silently become the new role's, so the caller has to say what it wants. A
    /// cleared role, an unchanged role and a target with no stored ID never need one.
    /// </summary>
    public static bool RoleChangeNeedsExternalId(string? oldRole, string? newRole, bool externalIdStored, bool externalIdSent)
    {
        return newRole is not null
            && !string.Equals(oldRole, newRole, StringComparison.Ordinal)
            && externalIdStored
            && !externalIdSent;
    }
}
