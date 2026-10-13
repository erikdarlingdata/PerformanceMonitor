/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The work behind the four external alert source verbs (<c>--add-alert-source</c>, <c>--rotate-alert-source</c>,
/// <c>--remove-alert-source</c>, <c>--list-alert-sources</c>, #5654). A source is a registry row in
/// <c>config.external_alert_source</c> plus one database login of its own; the login can do one thing, call
/// <c>config.submit_external_alert</c>, and the function names the source from the login, so no source can use another's name.
///
/// <para><b>Runs as the store owner</b>, on an open connection the caller supplies, so a live test drives the same code the verb
/// does. The grants are not written here: the login is created and registered, and then the provisioning step that runs on every
/// start (<see cref="DarlingManagedRoles.BuildExternalAlertSourceRolesSql"/>) is run once, so the verb and the service cannot
/// disagree about what a source login may hold.</para>
///
/// <para><b>The password is never a statement.</b> It is generated here, turned into a SCRAM-SHA-256 verifier
/// (<see cref="ScramSha256Verifier"/>, the rule <see cref="DarlingManagedRoles.BuildProvisioningSql"/> follows), and only the
/// verifier is sent to the server, so no log or statistics view that records statement text can capture it. The caller prints the
/// password once, inside a connection string, and nothing keeps it.</para>
///
/// <para><b>An owner without CREATEROLE</b> (a bring-your-own store whose owner is not a superuser) cannot make, change or drop a
/// login. The verbs then change the registry only, and return the statements a database administrator runs for the login.</para>
/// </summary>
internal static class ExternalAlertSourceAdmin
{
    /// <summary>The prefix of a source's login name, so the logins are easy to tell apart from Darling's own roles.</summary>
    public const string RolePrefix = "darling_src_";

    private static readonly Regex s_name = new("^[a-z][a-z0-9_]{1,30}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private const string Schema = PgSchemaGenerator.ConfigSchema;

    /// <summary>The source name rule, the same as the registry's CHECK constraint (a letter, then 1 to 30 lower-case letters, digits or underscores).</summary>
    public static bool IsValidName(string? name) => name is not null && s_name.IsMatch(name);

    /// <summary>The login a source name gets.</summary>
    public static string RoleNameFor(string name) => RolePrefix + name;

    /// <summary>What a verb did.</summary>
    public enum Outcome
    {
        /// <summary>Done, in full.</summary>
        Done,

        /// <summary>The registry was changed but the owner cannot manage logins: <see cref="Result.DbaSql"/> holds what a database administrator runs.</summary>
        NeedsDbaSql,

        /// <summary>The name is not a valid source name.</summary>
        InvalidName,

        /// <summary>The store has not been upgraded to the version that holds the external alert tables.</summary>
        NoTables,

        /// <summary>Add: a source with this name (or this login name) is already registered.</summary>
        AlreadyRegistered,

        /// <summary>Add: a login with the source's name exists but is not registered, so it is not adopted.</summary>
        LoginExists,

        /// <summary>Rotate or remove: no source has this name.</summary>
        NotRegistered,

        /// <summary>Rotate: the registered login is missing, or it does not carry the marker comment, so it is not touched.</summary>
        LoginNotManaged,
    }

    /// <summary>The result of one verb. <see cref="Password"/> is set only by add and rotate when the login was changed, and is shown once.</summary>
    internal sealed record Result(
        Outcome Outcome,
        string RoleName,
        string? Password = null,
        string? DbaSql = null,
        long RemovedRows = 0,
        long RemovedUndelivered = 0,
        bool LoginDropped = false);

    /// <summary>One line of <c>--list-alert-sources</c>. No secret: the registry does not hold one.</summary>
    internal sealed record SourceRow(
        string Name, string RoleName, bool Enabled, DateTimeOffset CreatedAt, DateTimeOffset? LastSubmit, long Undelivered);

    private static readonly string s_loginAttributes =
        "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT CONNECTION LIMIT "
        + DarlingManagedRoles.AlertSourceConnectionLimit;

    /// <summary>The connection string a source's client uses: where the store is (taken from the owner's string), the source's login and the password. Nothing else of the owner's string is carried.</summary>
    public static string BuildSourceConnectionString(string ownerConnectionString, string roleName, string password)
    {
        var owner = new NpgsqlConnectionStringBuilder(ownerConnectionString);
        var source = new NpgsqlConnectionStringBuilder
        {
            Host = owner.Host,
            Port = owner.Port,
            Database = owner.Database,
            Username = roleName,
            Password = password,
        };
        if (owner.ContainsKey("SSL Mode") && owner.SslMode != SslMode.Prefer)
        {
            source.SslMode = owner.SslMode;
        }

        return source.ConnectionString;
    }

    /// <summary>Creates a source: its login, its registry row and its grants. The password is in the result, once.</summary>
    public static async Task<Result> AddAsync(NpgsqlConnection owner, string name, CancellationToken ct)
    {
        var role = IsValidName(name) ? RoleNameFor(name) : string.Empty;
        if (role.Length == 0)
        {
            return new Result(Outcome.InvalidName, string.Empty);
        }

        if (!await TablesExistAsync(owner, ct))
        {
            return new Result(Outcome.NoTables, role);
        }

        await using var transaction = await owner.BeginTransactionAsync(ct);
        if (await ScalarAsync<bool>(owner, transaction, $"SELECT EXISTS (SELECT 1 FROM {Schema}.external_alert_source WHERE source_name = $1 OR role_name = $2)", ct, name, role))
        {
            return new Result(Outcome.AlreadyRegistered, role);
        }

        if (await ScalarAsync<bool>(owner, transaction, "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1)", ct, role))
        {
            return new Result(Outcome.LoginExists, role);
        }

        var password = DarlingManagedPostgres.GeneratePassword();
        var verifier = ScramSha256Verifier.Create(password);
        var canManageLogins = await CanManageLoginsAsync(owner, transaction, ct);

        if (canManageLogins)
        {
            await ExecAsync(owner, transaction, $"CREATE ROLE {Quote(role)} {s_loginAttributes} PASSWORD '{verifier}'", ct);
            await ExecAsync(owner, transaction, $"COMMENT ON ROLE {Quote(role)} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
        }

        await ExecAsync(owner, transaction, $"INSERT INTO {Schema}.external_alert_source (source_name, role_name) VALUES ($1, $2)", ct, name, role);

        if (!canManageLogins)
        {
            await transaction.CommitAsync(ct);
            return new Result(Outcome.NeedsDbaSql, role, password, BuildAddSqlForDba(role, verifier, await DatabaseNameAsync(owner, ct), owner.PostgreSqlVersion.Major));
        }

        /* The shared step also makes the login a member of darling_alert_sources (the group the pg_hba line for
           postgres.network.role "sources" names), in this transaction, so the new source is admitted from the network as
           soon as the verb returns wherever that line is written. */
        await ExecAsync(owner, transaction, DarlingManagedRoles.BuildExternalAlertSourceRolesSql(Schema), ct);
        await transaction.CommitAsync(ct);
        return new Result(Outcome.Done, role, password);
    }

    /// <summary>Gives a registered source a new password. The old password stops working at once.</summary>
    public static async Task<Result> RotateAsync(NpgsqlConnection owner, string name, CancellationToken ct)
    {
        var role = IsValidName(name) ? RoleNameFor(name) : string.Empty;
        if (role.Length == 0)
        {
            return new Result(Outcome.InvalidName, string.Empty);
        }

        if (!await TablesExistAsync(owner, ct))
        {
            return new Result(Outcome.NoTables, role);
        }

        await using var transaction = await owner.BeginTransactionAsync(ct);
        var registered = await ScalarAsync<string?>(owner, transaction, $"SELECT role_name FROM {Schema}.external_alert_source WHERE source_name = $1", ct, name);
        if (registered is null)
        {
            return new Result(Outcome.NotRegistered, role);
        }

        if (!await IsMarkedLoginAsync(owner, transaction, registered, ct))
        {
            return new Result(Outcome.LoginNotManaged, registered);
        }

        var password = DarlingManagedPostgres.GeneratePassword();
        var verifier = ScramSha256Verifier.Create(password);
        if (!await CanManageLoginsAsync(owner, transaction, ct))
        {
            return new Result(Outcome.NeedsDbaSql, registered, password, $"ALTER ROLE {Quote(registered)} PASSWORD '{verifier}';");
        }

        await ExecAsync(owner, transaction, $"ALTER ROLE {Quote(registered)} PASSWORD '{verifier}'", ct);
        await transaction.CommitAsync(ct);
        return new Result(Outcome.Done, registered, password);
    }

    /// <summary>
    /// Removes a source: the registry row first (the function refuses an unregistered login at once), the queued alerts and
    /// firing state with it (the foreign keys cascade), then the login. The alert history the source already wrote stays.
    /// A login that does not carry the marker comment is not dropped.
    /// </summary>
    public static async Task<Result> RemoveAsync(NpgsqlConnection owner, string name, CancellationToken ct)
    {
        var role = IsValidName(name) ? RoleNameFor(name) : string.Empty;
        if (role.Length == 0)
        {
            return new Result(Outcome.InvalidName, string.Empty);
        }

        if (!await TablesExistAsync(owner, ct))
        {
            return new Result(Outcome.NoTables, role);
        }

        await using var transaction = await owner.BeginTransactionAsync(ct);
        var registered = await ScalarAsync<string?>(owner, transaction, $"SELECT role_name FROM {Schema}.external_alert_source WHERE source_name = $1", ct, name);
        if (registered is null)
        {
            return new Result(Outcome.NotRegistered, role);
        }

        var sourceId = await ScalarAsync<int>(owner, transaction, $"SELECT source_id FROM {Schema}.external_alert_source WHERE source_name = $1", ct, name);
        var rows = await ScalarAsync<long>(owner, transaction, $"SELECT count(*) FROM {Schema}.external_alert_pending WHERE source_id = $1", ct, sourceId);
        var undelivered = await ScalarAsync<long>(owner, transaction,
            $"SELECT count(*) FROM {Schema}.external_alert_pending WHERE source_id = $1 AND status IN ('pending', 'claimed')", ct, sourceId);
        var marked = await IsMarkedLoginAsync(owner, transaction, registered, ct);
        var canManageLogins = await CanManageLoginsAsync(owner, transaction, ct);

        await ExecAsync(owner, transaction, $"DELETE FROM {Schema}.external_alert_source WHERE source_id = $1", ct, sourceId);

        string? dbaSql = null;
        var dropped = false;
        if (marked && canManageLogins)
        {
            var quoted = Quote(registered);
            await ExecAsync(owner, transaction, $"REVOKE ALL ON FUNCTION {Schema}.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM {quoted}", ct);
            await ExecAsync(owner, transaction, $"REVOKE ALL ON SCHEMA {Schema} FROM {quoted}", ct);
            await ExecAsync(owner, transaction, $"REVOKE ALL ON DATABASE {Quote(await DatabaseNameAsync(owner, ct))} FROM {quoted}", ct);
            await ExecAsync(owner, transaction, $"DROP ROLE {quoted}", ct);
            dropped = true;
        }
        else if (marked)
        {
            dbaSql = BuildRemoveSqlForDba(registered, await DatabaseNameAsync(owner, ct));
        }

        await transaction.CommitAsync(ct);
        return new Result(dbaSql is null ? Outcome.Done : Outcome.NeedsDbaSql, registered, DbaSql: dbaSql,
            RemovedRows: rows, RemovedUndelivered: undelivered, LoginDropped: dropped);
    }

    /// <summary>Every registered source, oldest first. The last submit and the undelivered count come from the queue rows the store still keeps.</summary>
    public static async Task<IReadOnlyList<SourceRow>?> ListAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        if (!await TablesExistAsync(owner, ct))
        {
            return null;
        }

        var rows = new List<SourceRow>();
        await using var command = new NpgsqlCommand($@"
SELECT s.source_name, s.role_name, s.enabled, s.created_at,
       (SELECT max(p.submitted_at) FROM {Schema}.external_alert_pending AS p WHERE p.source_id = s.source_id),
       (SELECT count(*) FROM {Schema}.external_alert_pending AS p WHERE p.source_id = s.source_id AND p.status IN ('pending', 'claimed'))
FROM {Schema}.external_alert_source AS s
ORDER BY s.source_id", owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new SourceRow(
                reader.GetString(0), reader.GetString(1), reader.GetBoolean(2),
                new DateTimeOffset(reader.GetDateTime(3), TimeSpan.Zero),
                reader.IsDBNull(4) ? null : new DateTimeOffset(reader.GetDateTime(4), TimeSpan.Zero),
                reader.GetInt64(5)));
        }

        return rows;
    }

    /// <summary>
    /// The statements a database administrator runs to make a source's login when the store owner cannot (the verifier is the only
    /// secret in them, and it is not a password). They end with the membership in <see cref="DarlingManagedRoles.AlertSourceGroupRoleName"/>
    /// that the provisioning step grants (WITH INHERIT FALSE, SET FALSE from PostgreSQL 16, which <paramref name="serverMajorVersion"/>
    /// decides); <c>tools/provision-roles.sql</c> makes the group.
    /// </summary>
    internal static string BuildAddSqlForDba(string role, string verifier, string database, int serverMajorVersion)
    {
        var quoted = Quote(role);
        return string.Join(Environment.NewLine,
            $"CREATE ROLE {quoted} {s_loginAttributes} PASSWORD '{verifier}';",
            $"COMMENT ON ROLE {quoted} IS '{DarlingManagedRoles.AlertSourceRoleMarker}';",
            $"GRANT CONNECT ON DATABASE {Quote(database)} TO {quoted};",
            $"GRANT USAGE ON SCHEMA {Schema} TO {quoted};",
            $"GRANT EXECUTE ON FUNCTION {Schema}.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) TO {quoted};",
            $"GRANT {Quote(DarlingManagedRoles.AlertSourceGroupRoleName)} TO {quoted}{(serverMajorVersion >= 16 ? " WITH INHERIT FALSE, SET FALSE" : string.Empty)};");
    }

    /// <summary>The statements a database administrator runs to drop a source's login when the store owner cannot.</summary>
    internal static string BuildRemoveSqlForDba(string role, string database)
    {
        var quoted = Quote(role);
        return string.Join(Environment.NewLine,
            $"REVOKE ALL ON FUNCTION {Schema}.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM {quoted};",
            $"REVOKE ALL ON SCHEMA {Schema} FROM {quoted};",
            $"REVOKE ALL ON DATABASE {Quote(database)} FROM {quoted};",
            $"DROP ROLE {quoted};");
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static async Task<bool> TablesExistAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT to_regclass('{Schema}.external_alert_source') IS NOT NULL AND to_regclass('{Schema}.external_alert_pending') IS NOT NULL", owner);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> CanManageLoginsAsync(NpgsqlConnection owner, NpgsqlTransaction transaction, CancellationToken ct) =>
        await ScalarAsync<bool>(owner, transaction, "SELECT rolsuper OR rolcreaterole FROM pg_catalog.pg_roles WHERE rolname = current_user", ct);

    private static async Task<bool> IsMarkedLoginAsync(NpgsqlConnection owner, NpgsqlTransaction transaction, string role, CancellationToken ct) =>
        await ScalarAsync<bool>(owner, transaction,
            $"SELECT COALESCE((SELECT shobj_description(r.oid, 'pg_authid') = '{DarlingManagedRoles.AlertSourceRoleMarker}' FROM pg_catalog.pg_roles AS r WHERE r.rolname = $1), false)", ct, role);

    private static async Task<string> DatabaseNameAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT current_database()", owner);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection owner, NpgsqlTransaction transaction, string sql, CancellationToken ct, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, owner, transaction);
        foreach (var arg in args)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = arg });
        }

        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? default! : (T)value;
    }

    private static async Task ExecAsync(NpgsqlConnection owner, NpgsqlTransaction transaction, string sql, CancellationToken ct, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, owner, transaction);
        foreach (var arg in args)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = arg });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
