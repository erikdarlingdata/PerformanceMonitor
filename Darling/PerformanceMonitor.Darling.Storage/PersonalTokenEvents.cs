/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The journal events of the personal MCP tokens (#8737). Only the store owner journals them: <see cref="PersonalTokenStore"/> writes each in the
/// transaction of its change, on the owner connection, and the journal's owner-only trigger refuses every one of them from the <c>mcp</c>,
/// <c>admin</c>, <c>viewer</c> and <c>web</c> logins, so no other login can write a row that says a token was made, revoked or cut short.
/// The detail of every row is exactly <c>{token_id, token_name, role, reason, subject, owner_key}</c>, and never the token's value or its hash.
/// </summary>
public static class PersonalTokenEvents
{
    public const string Created = "mcp_personal_token_created";
    public const string Revoked = "mcp_personal_token_revoked";
    public const string ExpiryCut = "mcp_personal_token_expiry_cut";

    /// <summary>Every event above, for the journal trigger's owner-only list.</summary>
    public static IReadOnlyList<string> OwnerOnly { get; } = [Created, Revoked, ExpiryCut];
}
