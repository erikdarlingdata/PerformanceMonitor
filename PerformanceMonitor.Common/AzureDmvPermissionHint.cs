/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// Turns the bare "permission was denied on object 'server', database 'master'" that Azure SQL
/// Database returns for a server-scoped DMV into something that answers the question it provokes,
/// which is always the same one (#1631): "does this really need master?"
///
/// <para>It does not. The permission is not grantable at the server on Azure SQL Database at all —
/// the server is a logical concept there — so the message's mention of <c>master</c> reads as a
/// missing GRANT when it is really a statement about SERVICE OBJECTIVE. MS Learn documents
/// <c>VIEW DATABASE STATE</c> as sufficient for these DMVs on every Azure SQL Database service
/// objective EXCEPT Basic, S0, S1, and any database in an ELASTIC POOL; on those, only the server
/// admin, the Microsoft Entra admin, or a login in <c>##MS_ServerStateReader##</c> can read them.
/// A contained database user on a pooled database therefore cannot reach them however many database
/// grants it is given, and no amount of <c>master</c> access would change that either.</para>
///
/// <para>Error 262 (#2512) is the DATABASE-scoped twin, and it is the denial that got tempdb
/// monitoring switched off for the whole Azure SQL Database tier: "VIEW DATABASE PERFORMANCE STATE
/// permission denied in database 'tempdb'". It provokes the same wrong question — people go looking
/// for a GRANT to issue in tempdb, and on Azure SQL Database there is not one to issue. The gate is
/// gone (the DMVs read fine for a login that can reach them), so this is now the sentence a login
/// that CANNOT reach them gets, alongside the PERMISSIONS classification that keeps it out of the
/// error counts.</para>
///
/// <para><b>262 needs the message, not just the number, and that is the whole difference between this
/// arm and the 300 one.</b> 300 is server-scoped: on Azure SQL Database it can only ever be the service
/// objective, so the number alone settles it. 262 names a DATABASE, and the advice is opposite depending
/// on which one — in tempdb there is no grant to issue and the answer is server-level state access,
/// while in a user database there IS one and the raw error already says what it is. Keying purely off
/// the number would append tempdb guidance to a denial in someone's user database, which is worse than
/// appending nothing: it sends them looking for a role membership that would not have helped. So this
/// arm reads the database name out of the message, and says nothing when it is not tempdb or when no
/// message was supplied. The failure direction is deliberate — a caller that forgets the message
/// loses a helpful sentence rather than gaining a wrong one.</para>
///
/// <para>Appended to the stored error rather than replacing it: the raw SQL error number and text
/// stay intact for searching and for support, and the explanation rides along wherever the error is
/// already displayed — no new surface, no silent empty tab (#1591/#1679).</para>
/// </summary>
public static class AzureDmvPermissionHint
{
    /// <summary>
    /// The explanatory suffix for <paramref name="sqlErrorNumber"/>, or an empty string when none
    /// applies. Empty for every non-Azure target, so an on-prem 300 or 262 (a genuinely missing GRANT,
    /// which IS the fix there) keeps reading as one.
    /// </summary>
    /// <param name="sqlErrorNumber">The SQL Server error number.</param>
    /// <param name="isAzureSqlDb">Whether the target is Azure SQL Database.</param>
    /// <param name="errorMessage">
    /// The driver's message text. Only 262 reads it, and only to learn WHICH database was denied —
    /// see the type remarks. Omit it and the 262 arm stays silent rather than guessing.
    /// </param>
    public static string For(int sqlErrorNumber, bool isAzureSqlDb, string? errorMessage = null)
    {
        if (!isAzureSqlDb)
        {
            return string.Empty;
        }

        return sqlErrorNumber switch
        {
            300 => ServerScopedHint,
            262 => NamesTempDb(errorMessage) ? TempDbScopedHint : string.Empty,
            _ => string.Empty,
        };
    }

    /// <summary>
    /// Whether a 262 message names tempdb as the database that denied it. Matched on the QUOTED
    /// identifier, which is what error 262's format string interpolates ("... permission denied in
    /// database '%.*ls'") and which no user database can collide with — tempdb is a reserved name, so
    /// a database merely CONTAINING "tempdb" cannot produce <c>'tempdb'</c> in that position. Matching
    /// the bare word would have: a database called <c>reporting_tempdb_stage</c> would read as tempdb.
    /// Case-insensitive because the quoted name follows the server's collation, not this code's.
    /// </summary>
    private static bool NamesTempDb(string? errorMessage)
        => errorMessage is not null
        && errorMessage.Contains("'tempdb'", StringComparison.OrdinalIgnoreCase);

    private const string ServerScopedHint =
        " — On Azure SQL Database this is a SERVICE OBJECTIVE limit, not a missing grant: "
        + "VIEW SERVER STATE / VIEW SERVER PERFORMANCE STATE cannot be granted at the server there. "
        + "VIEW DATABASE STATE covers this DMV on every service objective EXCEPT Basic, S0, S1, and "
        + "any database in an elastic pool, where reading it requires the server admin, the Entra "
        + "admin, or a login in the ##MS_ServerStateReader## server role. If this database is pooled "
        + "or Basic/S0/S1, add the monitoring login to ##MS_ServerStateReader## from the master "
        + "database; otherwise GRANT VIEW DATABASE STATE in this database. Other collectors are "
        + "unaffected and keep running.";

    private const string TempDbScopedHint =
        " — This denial names TEMPDB, and on Azure SQL Database there is no grant to issue there: "
        + "tempdb permissions are not persistable, and in an elastic pool the database is not the "
        + "permission boundary. Reach tempdb's space DMVs through server-level state access instead "
        + "— the server admin, the Microsoft Entra admin, or a login added to the "
        + "##MS_ServerStateReader## server role from the master database. Only the collectors that read "
        + "tempdb are affected; everything else keeps running.";
}
