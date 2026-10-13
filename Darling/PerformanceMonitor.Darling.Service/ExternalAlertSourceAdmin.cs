/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Notifications;
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

    private const string Schema = PgSchemaGenerator.ConfigSchema;

    /// <summary>The source name rule, the same as the registry's CHECK constraint (a letter, then 1 to 30 lower-case letters, digits or underscores).</summary>
    /// <remarks>It is <see cref="MuteRule.IsValidExternalSource"/>, the one rule every entry point uses: it checks each character
    /// up to the string's true end, so a name with a trailing line feed is refused (#5654).</remarks>
    public static bool IsValidName(string? name) => MuteRule.IsValidExternalSource(name);

    /// <summary>The login a source name gets.</summary>
    public static string RoleNameFor(string name) => RolePrefix + name;

    /// <summary>What a verb did.</summary>
    public enum Outcome
    {
        /// <summary>Done, in full.</summary>
        Done,

        /// <summary>
        /// The owner cannot make, change or drop the login (it lacks CREATEROLE, or the server refused the step:
        /// <see cref="Result.DbaReason"/> says why). Nothing of the login was changed; add and remove changed the registry, rotate
        /// changed nothing. <see cref="Result.DbaSql"/> holds what a database administrator runs.
        /// </summary>
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
        bool LoginDropped = false,
        string? DbaReason = null,
        long? SessionsEnded = null,
        string? EndSessionsSqlForDba = null,
        string? StatementStatisticsResetSql = null);

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

        await using var transaction = await BeginOwnerTransactionAsync(owner, ct);
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
        string? reason = null;
        if (canManageLogins)
        {
            var statisticsReset = await StopStatementTrackingAsync(owner, transaction, ct);

            /* The shared step also makes the login a member of darling_alert_sources (the group the pg_hba line for
               postgres.network.role "sources" names), in this transaction, so the new source is admitted from the network as
               soon as the verb returns wherever that line is written. A CREATEROLE owner that is not a superuser may still be
               refused part of it (an attribute it may not set, a group another role made): then nothing of the login is kept,
               the source is registered, and the verb returns the statements a database administrator runs, with the reason. */
            await transaction.SaveAsync(LoginSavepoint, ct);
            try
            {
                await ExecAsync(owner, transaction, $"CREATE ROLE {Quote(role)} {s_loginAttributes} PASSWORD '{verifier}'", ct);
                await ExecAsync(owner, transaction, $"COMMENT ON ROLE {Quote(role)} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
                await ExecAsync(owner, transaction, $"INSERT INTO {Schema}.external_alert_source (source_name, role_name) VALUES ($1, $2)", ct, name, role);
                await ExecAsync(owner, transaction, DarlingManagedRoles.BuildExternalAlertSourceRolesSql(Schema), ct);
                await transaction.CommitAsync(ct);
                return new Result(Outcome.Done, role, password, StatementStatisticsResetSql: statisticsReset);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                await transaction.RollbackAsync(LoginSavepoint, ct);
                reason = ex.MessageText;
            }
        }

        await ExecAsync(owner, transaction, $"INSERT INTO {Schema}.external_alert_source (source_name, role_name) VALUES ($1, $2)", ct, name, role);
        await transaction.CommitAsync(ct);
        return new Result(Outcome.NeedsDbaSql, role, password, BuildAddSqlForDba(role, verifier, await DatabaseNameAsync(owner, ct), owner.PostgreSqlVersion.Major),
            DbaReason: reason);
    }

    private const string LoginSavepoint = "alert_source_login";

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

        string? statisticsReset;
        string registered;
        string password;
        await using (var transaction = await BeginOwnerTransactionAsync(owner, ct))
        {
            var found = await ScalarAsync<string?>(owner, transaction, $"SELECT role_name FROM {Schema}.external_alert_source WHERE source_name = $1", ct, name);
            if (found is null)
            {
                return new Result(Outcome.NotRegistered, role);
            }

            registered = found;
            if (!await IsMarkedLoginAsync(owner, transaction, registered, ct))
            {
                return new Result(Outcome.LoginNotManaged, registered);
            }

            password = DarlingManagedPostgres.GeneratePassword();
            var verifier = ScramSha256Verifier.Create(password);
            if (!await CanManageLoginsAsync(owner, transaction, ct))
            {
                return new Result(Outcome.NeedsDbaSql, registered, password,
                    string.Join(Environment.NewLine, $"ALTER ROLE {Quote(registered)} PASSWORD '{verifier}';", BuildEndSessionsSqlForDba(registered)));
            }

            var dbaSql = string.Join(Environment.NewLine, $"ALTER ROLE {Quote(registered)} PASSWORD '{verifier}';", BuildEndSessionsSqlForDba(registered));
            statisticsReset = await StopStatementTrackingAsync(owner, transaction, ct);
            try
            {
                await ExecAsync(owner, transaction, $"ALTER ROLE {Quote(registered)} PASSWORD '{verifier}'", ct);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                /* From PostgreSQL 16 a CREATEROLE owner changes only a login it holds ADMIN OPTION on (one it made itself). */
                return new Result(Outcome.NeedsDbaSql, registered, password, dbaSql, DbaReason: ex.MessageText);
            }

            await transaction.CommitAsync(ct);
        }

        /* #5654: a new password governs new connections only, so the sessions the login already holds are ended once the new
           password is committed (ending them first would let a client log straight back in with the old one before the commit). */
        var (ended, endSql) = await EndSessionsAsync(owner, "a.usename = $1", registered, BuildEndSessionsSqlForDba(registered), ct);
        return new Result(Outcome.Done, registered, password,
            SessionsEnded: ended, EndSessionsSqlForDba: endSql, StatementStatisticsResetSql: statisticsReset);
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

        await using var transaction = await BeginOwnerTransactionAsync(owner, ct);
        var registered = await ScalarAsync<string?>(owner, transaction, $"SELECT role_name FROM {Schema}.external_alert_source WHERE source_name = $1", ct, name);
        if (registered is null)
        {
            return new Result(Outcome.NotRegistered, role);
        }

        var sourceId = await ScalarAsync<int>(owner, transaction, $"SELECT source_id FROM {Schema}.external_alert_source WHERE source_name = $1", ct, name);
        var rows = await ScalarAsync<long>(owner, transaction, $"SELECT pg_catalog.count(*) FROM {Schema}.external_alert_pending WHERE source_id = $1", ct, sourceId);
        var undelivered = await ScalarAsync<long>(owner, transaction,
            $"SELECT pg_catalog.count(*) FROM {Schema}.external_alert_pending WHERE source_id = $1 AND status IN ('pending', 'claimed')", ct, sourceId);
        var marked = await IsMarkedLoginAsync(owner, transaction, registered, ct);
        var canManageLogins = await CanManageLoginsAsync(owner, transaction, ct);

        await ExecAsync(owner, transaction, $"DELETE FROM {Schema}.external_alert_source WHERE source_id = $1", ct, sourceId);

        string? dbaSql = null;
        string? reason = null;
        var dropped = false;
        uint loginOid = 0;
        if (marked && canManageLogins)
        {
            loginOid = await ScalarAsync<uint>(owner, transaction, "SELECT r.oid FROM pg_catalog.pg_roles AS r WHERE r.rolname = $1", ct, registered);
            var quoted = Quote(registered);
            await transaction.SaveAsync(LoginSavepoint, ct);
            try
            {
                await ExecAsync(owner, transaction, $"REVOKE ALL ON FUNCTION {Schema}.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM {quoted}", ct);
                await ExecAsync(owner, transaction, $"REVOKE ALL ON SCHEMA {Schema} FROM {quoted}", ct);
                await ExecAsync(owner, transaction, $"REVOKE ALL ON DATABASE {Quote(await DatabaseNameAsync(owner, ct))} FROM {quoted}", ct);
                await ExecAsync(owner, transaction, $"DROP ROLE {quoted}", ct);
                dropped = true;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                /* The registry row still goes (the function refuses the login at once); the login is left for a database administrator. */
                await transaction.RollbackAsync(LoginSavepoint, ct);
                reason = ex.MessageText;
            }
        }

        if (marked && !dropped)
        {
            dbaSql = BuildRemoveSqlForDba(registered, await DatabaseNameAsync(owner, ct));
        }

        await transaction.CommitAsync(ct);

        /* #5654: DROP ROLE leaves the dropped login's open sessions running, so they are ended once the drop is committed (found
           by the login's oid, which pg_stat_activity still carries after the name is gone). Ending them before the commit would
           let a client log back in while the login still existed. */
        long? ended = null;
        string? endSql = null;
        if (dropped)
        {
            (ended, endSql) = await EndSessionsAsync(owner, "a.usesysid = $1::pg_catalog.oid", (long)loginOid,
                $"SELECT pg_catalog.pg_terminate_backend(a.pid) FROM pg_catalog.pg_stat_activity AS a WHERE a.usesysid = {loginOid};", ct);
        }

        return new Result(dbaSql is null ? Outcome.Done : Outcome.NeedsDbaSql, registered, DbaSql: dbaSql,
            RemovedRows: rows, RemovedUndelivered: undelivered, LoginDropped: dropped, DbaReason: reason,
            SessionsEnded: ended, EndSessionsSqlForDba: endSql);
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
       (SELECT pg_catalog.max(p.submitted_at) FROM {Schema}.external_alert_pending AS p WHERE p.source_id = s.source_id),
       (SELECT pg_catalog.count(*) FROM {Schema}.external_alert_pending AS p WHERE p.source_id = s.source_id AND p.status IN ('pending', 'claimed'))
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
            BuildEndSessionsSqlForDba(role),
            $"DROP ROLE {quoted};");
    }

    /// <summary>The statement a database administrator runs to end a source login's open sessions when the store owner may not (#5654).</summary>
    internal static string BuildEndSessionsSqlForDba(string role) =>
        $"SELECT pg_catalog.pg_terminate_backend(a.pid) FROM pg_catalog.pg_stat_activity AS a WHERE a.usename = '{role.Replace("'", "''", StringComparison.Ordinal)}';";

    /// <summary>
    /// The settings through which a statement statistics extension records utility statements, the role DDL that carries a
    /// verifier among them: the pair <c>tools/provision-roles.sql</c> turns off before its own password statements (#5654).
    /// </summary>
    internal static readonly (string Setting, string ResetSql)[] StatementTrackingSettings =
    [
        ("pg_stat_statements.track_utility", "SELECT pg_stat_statements_reset();"),
        ("pg_stat_monitor.pgsm_track_utility", "SELECT pg_stat_monitor_reset();"),
    ];

    /// <summary>
    /// Turns utility statement tracking off for the rest of <paramref name="transaction"/>, as <c>tools/provision-roles.sql</c>
    /// does, so the CREATE ROLE or ALTER ROLE that follows (its verifier is statement text) is not recorded. A setting this owner
    /// may not change (PGC_SUSET without a GRANT SET) is left alone; when it is on, the reset statement a database administrator
    /// runs afterwards is returned. Null when nothing that is on was left alone.
    /// </summary>
    private static async Task<string?> StopStatementTrackingAsync(NpgsqlConnection owner, NpgsqlTransaction transaction, CancellationToken ct)
    {
        var resets = new List<string>();
        foreach (var (setting, resetSql) in StatementTrackingSettings)
        {
            /* Absent (the extension is not loaded): nothing records statements through it. */
            if (await ScalarAsync<string?>(owner, transaction, "SELECT pg_catalog.current_setting($1, true)", ct, setting) is null)
            {
                continue;
            }

            await transaction.SaveAsync("darling_statement_tracking", ct);
            try
            {
                await ExecAsync(owner, transaction, $"SET LOCAL {setting} = off", ct);
                await transaction.ReleaseAsync("darling_statement_tracking", ct);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                await transaction.RollbackAsync("darling_statement_tracking", ct);
                if (await ScalarAsync<bool>(owner, transaction, "SELECT pg_catalog.lower(pg_catalog.current_setting($1, true)) IN ('on', 'yes', 'true', '1')", ct, setting))
                {
                    resets.Add(resetSql);
                }
            }
        }

        return resets.Count == 0 ? null : string.Join(Environment.NewLine, resets);
    }

    /// <summary>
    /// Ends the open sessions of one source login, outside any transaction, and returns how many (#5654). An owner that may not
    /// signal those backends (neither a superuser nor a member of pg_signal_backend) gets a null count and
    /// <paramref name="dbaSql"/> back, the statement a database administrator runs instead.
    /// </summary>
    private static async Task<(long? Ended, string? DbaSql)> EndSessionsAsync(
        NpgsqlConnection owner, string predicate, object key, string dbaSql, CancellationToken ct)
    {
        try
        {
            await using var command = new NpgsqlCommand(
                $"SELECT pg_catalog.count(*) FILTER (WHERE pg_catalog.pg_terminate_backend(a.pid)) FROM pg_catalog.pg_stat_activity AS a WHERE {predicate}", owner);
            command.Parameters.Add(new NpgsqlParameter { Value = key });
            return ((long)(await command.ExecuteScalarAsync(ct))!, null);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return (null, dbaSql);
        }
    }

    /// <summary>
    /// Begins a verb's transaction with <c>search_path</c> set to <c>pg_catalog, pg_temp</c> for that transaction (#5654). The
    /// verbs run as the store owner, so no function or relation another role created in a schema on the session's path can
    /// stand in for a catalog one, in this file's statements or in the shared provisioning step the add verb runs.
    /// </summary>
    private static async Task<NpgsqlTransaction> BeginOwnerTransactionAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var transaction = await owner.BeginTransactionAsync(ct);
        await ExecAsync(owner, transaction, "SET LOCAL search_path = pg_catalog, pg_temp", ct);
        return transaction;
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static async Task<bool> TablesExistAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT pg_catalog.to_regclass('{Schema}.external_alert_source') IS NOT NULL AND pg_catalog.to_regclass('{Schema}.external_alert_pending') IS NOT NULL", owner);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> CanManageLoginsAsync(NpgsqlConnection owner, NpgsqlTransaction transaction, CancellationToken ct) =>
        await ScalarAsync<bool>(owner, transaction, "SELECT rolsuper OR rolcreaterole FROM pg_catalog.pg_roles WHERE rolname = current_user", ct);

    private static async Task<bool> IsMarkedLoginAsync(NpgsqlConnection owner, NpgsqlTransaction transaction, string role, CancellationToken ct) =>
        await ScalarAsync<bool>(owner, transaction,
            $"SELECT COALESCE((SELECT pg_catalog.shobj_description(r.oid, 'pg_authid') = '{DarlingManagedRoles.AlertSourceRoleMarker}' FROM pg_catalog.pg_roles AS r WHERE r.rolname = $1), false)", ct, role);

    private static async Task<string> DatabaseNameAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT pg_catalog.current_database()", owner);
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
