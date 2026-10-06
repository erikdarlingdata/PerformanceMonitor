/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The shared wording and address rule of a server edit. "This definition connects differently from that one" is
/// <see cref="ServerConnectionIdentity.Differ"/>: the edit core, <c>test_connect</c>, the service's darling.json matching
/// and the viewer all use it.</summary>
public static class ServerConnectionRule
{
    /// <summary>The sentence a refused change of how a server is reached gives when the row keeps its stored password. The
    /// web and MCP edit, the viewer and the store's own trigger and edit function (<c>provision-roles.sql</c>,
    /// <c>DarlingManagedRoles</c>, which keep the text literal) all answer with exactly this text.</summary>
    public const string PasswordNeededOnMoveText =
        "Changing how this server is reached needs its password again: it is stored encrypted and this surface cannot read it back.";

    /// <summary>The sentence a refused password gives when it is a reference (env: or file:). The same text in the store's
    /// trigger and edit function.</summary>
    public const string ReferenceRefusedText =
        "Enter the password itself. References (env: or file:) can only be set in the configuration file.";

    /// <summary>The sentence a refused change of how a server is reached gives when the row holds a remediation login. The
    /// same text in the store's trigger and edit function.</summary>
    public const string RemediationKeptText =
        "This server has a remediation login stored. Change how it is reached on the service host, in the configuration file or with --add-server.";

    /// <summary>
    /// "The server is at the same address": the host text is equal ordinally (a request's host is trimmed when it is
    /// read; the text is compared exactly as stored, and case counts) and the port is equal.
    /// </summary>
    public static bool SameAddress(string host, int port, string otherHost, int otherPort) =>
        string.Equals(host, otherHost, StringComparison.Ordinal) && port == otherPort;
}
