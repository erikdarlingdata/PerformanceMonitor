/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The externally supplied password key (#8006): the <c>passwordKey</c> setting in darling.json names a
/// <c>file:</c> or <c>env:</c> reference the service loads its password key from, instead of generating and
/// keeping a key file in the credentials directory.
/// </summary>
public static class DarlingPasswordKeySource
{
    /// <summary>The darling.json setting name, used in every sentence about the setting.</summary>
    public const string SettingName = "passwordKey";

    /// <summary>
    /// The validation problem for a literal <c>passwordKey</c>. A constant on purpose: the sentence never repeats the
    /// value, because that value would be the key.
    /// </summary>
    public const string LiteralRefusal = "passwordKey must be a file: or env: reference to the password key, never the key itself. Put the key in a secret file or an environment variable and point passwordKey at it.";
}
