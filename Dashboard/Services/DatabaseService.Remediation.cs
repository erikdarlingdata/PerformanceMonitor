/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Services.Remediation;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Narrow, purpose-built remediation execution methods. There is no general
    /// SQL-exec API here: the IDs are always typed BigInt parameters and the
    /// target database is applied ONLY as the connection's InitialCatalog, built
    /// solely through <see cref="SqlConnectionStringBuilder"/> (never concatenated).
    ///
    /// <para>
    /// These methods run under the EXISTING per-server monitoring connection
    /// (<see cref="DatabaseService.ConnectionString"/>), retargeted to the user
    /// database by catalog only. There is no elevated path: <c>UserID</c>/
    /// <c>Password</c> are never set from any prompt and no credential is captured.
    /// They are <c>internal</c> and reachable only through
    /// <see cref="Remediation.DatabaseServiceRemediationExecutor"/>; nothing in the
    /// MCP or menu/command surface calls them (verified by a no-caller test).
    /// </para>
    /// </summary>
    public partial class DatabaseService
    {
        private const int RemediationCommandTimeoutSeconds = 30;

        /*
        sp_query_store_force_plan requires the standard ANSI SET options (it touches
        Query Store internals that error 1934 without them). Microsoft.Data.SqlClient
        opens connections with ARITHABORT OFF by default, so we set the full block
        explicitly right after opening, matching install/_template.sql. SET options are
        session-scoped, so they persist for the gate read AND the EXEC on the same
        connection — no re-open, R2-MOD-1 intact.
        */
        private const string RemediationSetOptions = @"
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;";

        private static async Task ApplySessionOptionsAsync(SqlConnection connection, CancellationToken ct)
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = RemediationCommandTimeoutSeconds;
            command.CommandText = RemediationSetOptions;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Read-only display probe (advisory only). Opens its own retargeted
        /// connection; it is NOT the authoritative gate.
        /// </summary>
        internal async Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
        {
            var builder = new SqlConnectionStringBuilder(_connectionString) { InitialCatalog = database };
            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            // Identity + permission first, using only always-accessible intrinsics
            // (DB_NAME/SUSER_SNAME/HAS_PERMS_BY_NAME). The Query Store catalog read is
            // attempted only when ALTER is held and the DB matches — a least-privilege
            // login lacks VIEW DATABASE STATE and would error reading those views, and
            // for the display path the disposition is already decided by DB/ALTER.
            var gate = await ReadGateIdentityAsync(connection, ct).ConfigureAwait(false);
            if (gate.HasAlter && string.Equals(gate.CurrentDatabase, database, StringComparison.Ordinal))
                await ReadQueryStoreStateAsync(connection, queryId, planId, gate, ct).ConfigureAwait(false);

            return new TargetPreflight
            {
                Database = database,
                QueryId = queryId,
                PlanId = planId,
                ExecutingLogin = gate.ExecutingLogin,
                CurrentDatabase = gate.CurrentDatabase,
                HasAlter = gate.HasAlter,
                QueryStoreState = gate.QueryStoreState,
                PlanPresent = gate.PlanPresent,
                IsForcedPlan = gate.IsForcedPlan,
                ForceFailureCount = gate.ForceFailureCount
            };
        }

        /// <summary>
        /// Self-gating force. R2-MOD-1: the gate (DB_NAME assert + ALTER check +
        /// freshness) and the <c>sp_query_store_force_plan</c> EXEC run on ONE open
        /// retargeted connection with no re-open between them. <see cref="ForcePlanOutcome.GateSpid"/>
        /// and <see cref="ForcePlanOutcome.ExecSpid"/> are emitted so a caller can
        /// prove the gate and the mutation shared the same SPID.
        /// </summary>
        internal Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => ForceOrUnforceAsync(database, queryId, planId, isUnforce: false, ct);

        /// <summary>Self-gating inverse of <see cref="ForcePlanAsync"/>.</summary>
        internal Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => ForceOrUnforceAsync(database, queryId, planId, isUnforce: true, ct);

        private async Task<ForcePlanOutcome> ForceOrUnforceAsync(string database, long queryId, long planId, bool isUnforce, CancellationToken ct)
        {
            var builder = new SqlConnectionStringBuilder(_connectionString) { InitialCatalog = database };

            // ONE connection for the whole gate + mutation. No re-open between the
            // gate read and the EXEC — this is what makes the gate unbypassable and
            // closes the preflight→apply TOCTOU (R2-MOD-1).
            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            // Gate part 1 — identity + permission, intrinsics only (always accessible).
            var gate = await ReadGateIdentityAsync(connection, ct).ConfigureAwait(false);

            // (1) Correct-database assert (A5). Open with InitialCatalog normally
            // fails closed before here; this makes the wrong-DB boundary explicit.
            if (!string.Equals(gate.CurrentDatabase, database, StringComparison.Ordinal))
            {
                return Outcome(database, queryId, planId, RemediationStatus.Blocked, forced: false, gate,
                    $"Connected database '{gate.CurrentDatabase}' does not match target '{database}'; no change made.");
            }

            // (2) ALTER permission — fail closed with grant guidance, no elevation.
            // Checked BEFORE any Query Store catalog read: a login lacking ALTER also
            // typically lacks VIEW DATABASE STATE, so reading the QS views would throw
            // before we could report this clean, actionable result.
            if (!gate.HasAlter)
            {
                return Outcome(database, queryId, planId, RemediationStatus.PermissionDenied, forced: false, gate,
                    $"The monitoring login '{gate.ExecutingLogin}' lacks ALTER on '{database}'. " +
                    $"Map it into '{database}' (CREATE USER ... FOR LOGIN) and GRANT ALTER, or connect with a login that has it.");
            }

            // Gate part 2 — Query Store state, on the SAME open connection (R2-MOD-1),
            // only now that ALTER is confirmed.
            await ReadQueryStoreStateAsync(connection, queryId, planId, gate, ct).ConfigureAwait(false);

            // (3) Freshness — plan present, Query Store forceable, current force state.
            if (!gate.PlanPresent)
            {
                return Outcome(database, queryId, planId, RemediationStatus.Skipped, forced: false, gate,
                    "The plan/query is no longer present in Query Store — the suggestion may be superseded.");
            }
            if (!string.Equals(gate.QueryStoreState, "READ_WRITE", StringComparison.OrdinalIgnoreCase))
            {
                return Outcome(database, queryId, planId, RemediationStatus.Skipped, forced: false, gate,
                    $"Query Store is '{gate.QueryStoreState}' (not READ_WRITE) on '{database}' — forcing is not possible.");
            }

            if (!isUnforce && gate.IsForcedPlan)
            {
                return Outcome(database, queryId, planId, RemediationStatus.Skipped, forced: false, gate,
                    "Already forced to this plan — nothing to do.");
            }
            if (isUnforce && !gate.IsForcedPlan)
            {
                return Outcome(database, queryId, planId, RemediationStatus.Skipped, forced: false, gate,
                    "This plan is not currently forced — nothing to unforce.");
            }

            string? warning = null;
            if (!isUnforce && gate.ForceFailureCount > 0)
            {
                // Warn-and-proceed (the operator has confirmed the apply). Re-forcing
                // a failing plan may not help; the warning rides the outcome message.
                warning = $"Note: force_failure_count was {gate.ForceFailureCount}; re-forcing may not help.";
            }

            // Gate passed — issue the mutation on the SAME open connection.
            var proc = isUnforce ? "sys.sp_query_store_unforce_plan" : "sys.sp_query_store_force_plan";
            int? execSpid;
            try
            {
                using var exec = connection.CreateCommand();
                exec.CommandTimeout = RemediationCommandTimeoutSeconds;
                exec.CommandText = $"EXEC {proc} @query_id = @query_id, @plan_id = @plan_id; SELECT exec_spid = @@SPID;";
                exec.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
                exec.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
                var raw = await exec.ExecuteScalarAsync(ct).ConfigureAwait(false);
                execSpid = raw is null || raw is DBNull ? (int?)null : Convert.ToInt32(raw);
            }
            catch (SqlException ex)
            {
                var status = ex.Number is 297 or 15247 or 229 ? RemediationStatus.PermissionDenied : RemediationStatus.Error;
                return Outcome(database, queryId, planId, status, forced: false, gate,
                    $"sp_query_store_{(isUnforce ? "unforce" : "force")}_plan failed (error {ex.Number}): {ex.Message}");
            }

            var verb = isUnforce ? "Unforced" : "Forced";
            var message = warning is null ? $"{verb} plan {planId} for query {queryId}." : $"{verb} plan {planId} for query {queryId}. {warning}";
            return new ForcePlanOutcome
            {
                Database = database,
                QueryId = queryId,
                PlanId = planId,
                Status = RemediationStatus.Success,
                Forced = true,
                ExecutingLogin = gate.ExecutingLogin,
                Message = message,
                GateSpid = gate.Spid,
                ExecSpid = execSpid
            };
        }

        private static ForcePlanOutcome Outcome(string database, long queryId, long planId, RemediationStatus status, bool forced, GateReading gate, string message) => new()
        {
            Database = database,
            QueryId = queryId,
            PlanId = planId,
            Status = status,
            Forced = forced,
            ExecutingLogin = gate.ExecutingLogin,
            Message = message,
            GateSpid = gate.Spid,
            ExecSpid = null
        };

        // ── B3 Phase 2: always-safe DB-config fixes (ALTER DATABASE SET …) ──────────
        //
        // The one genuinely new sharp edge vs force-plan: the database is a syntactic
        // IDENTIFIER in the statement and CANNOT be a parameter. The no-injection
        // mechanism is three mandatory layers, all on ONE monitoring connection (no
        // InitialCatalog retarget — we stay on PerformanceMonitor, where even the
        // README's least-privilege login is db_owner and the by-name permission probe
        // is fail-closed):
        //   1. validate the candidate name against sys.databases, PARAMETERIZED
        //      (@db NVARCHAR(128), no concatenation);
        //   2. bracket the EXACT validated string with an inline QUOTENAME-equivalent
        //      (']'→']]') — the same-string invariant (M-1): the string validated in
        //      layer 1 is byte-identical to the string bracketed here;
        //   3. the SET clause is a HARDCODED compile-time literal chosen by a switch
        //      over DbConfigSetting — never data/operator-supplied.
        // The display renderer's rendered text is NEVER executed; this executor builds
        // its own statement. FactRemediation.QuoteName is private in another assembly,
        // so we implement our own byte-identical doubling routine here (m-B).

        /// <summary>
        /// Inline QUOTENAME-equivalent (byte-identical to FactRemediation.QuoteName,
        /// which is private in PerformanceMonitor.Analysis — m-B). Wraps an identifier
        /// in brackets and doubles any embedded close-bracket so even a pathological
        /// database name is inert. This is the ONLY non-constant token placed in the
        /// ALTER statement, and only AFTER the name was validated against sys.databases.
        /// </summary>
        internal static string QuoteIdentifier(string identifier) =>
            "[" + identifier.Replace("]", "]]") + "]";

        /// <summary>
        /// The hardcoded SET-clause literal for each setting. NOTHING here comes from
        /// data or operator input — the enum is the entire selection surface (§1 layer 3).
        /// </summary>
        internal static string SetClauseFor(DbConfigSetting setting) => setting switch
        {
            DbConfigSetting.AutoShrinkOff => "SET AUTO_SHRINK OFF",
            DbConfigSetting.AutoCloseOff => "SET AUTO_CLOSE OFF",
            DbConfigSetting.PageVerifyChecksum => "SET PAGE_VERIFY CHECKSUM",
            // B3 Phase 3: DESTRUCTIVE — routed only via the RcsiHandler behind the
            // informed-consent gate. The SET clause is still a hardcoded compile-time
            // literal selected by the enum; the identifier is the only variable token.
            DbConfigSetting.ReadCommittedSnapshotOn => "SET READ_COMMITTED_SNAPSHOT ON",
            _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown DbConfigSetting")
        };

        /// <summary>
        /// Builds the exact ALTER DATABASE statement: the ONLY non-constant token is
        /// the bracketed, ALREADY-VALIDATED identifier; the SET clause is a hardcoded
        /// enum-selected literal (§1). Exposed internal so the §11 no-injection /
        /// same-string tests run against the executor's OWN builder (not the display
        /// renderer). Callers MUST pass the exact string that the sys.databases
        /// existence check validated (M-1 same-string invariant).
        /// </summary>
        internal static string BuildAlterStatement(string validatedName, DbConfigSetting setting) =>
            $"ALTER DATABASE {QuoteIdentifier(validatedName)} {SetClauseFor(setting)};";

        /// <summary>
        /// Read-only display probe (advisory only). Runs on the monitoring connection
        /// (no retarget); reads existence + ALTER permission + the live setting value.
        /// </summary>
        internal async Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct)
        {
            // Monitoring connection — InitialCatalog left as PerformanceMonitor.
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            var gate = await ReadDbConfigGateAsync(connection, database, setting, ct).ConfigureAwait(false);

            return new DbConfigPreflight
            {
                Database = database,
                Setting = setting,
                DatabaseExists = gate.DatabaseExists,
                HasAlter = gate.HasAlter,
                AlreadyInDesiredState = gate.AlreadyInDesiredState,
                ExecutingLogin = gate.ExecutingLogin,
                CurrentValue = gate.CurrentValue
            };
        }

        /// <summary>
        /// Self-gating always-safe DB-config ALTER (R2-MOD-1). The gate (parameterized
        /// existence + ALTER permission + live freshness) and the ALTER run on ONE
        /// open monitoring connection with no re-open between them. The validated @db
        /// string is the SAME string that gets bracketed and placed in the statement
        /// (M-1 same-string invariant).
        /// </summary>
        internal async Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct)
        {
            // ONE monitoring connection for gate + mutation. No InitialCatalog
            // retarget (least-priv login is db_owner on PerformanceMonitor, not a user
            // in the target DB), no re-open — this is what closes the gate→ALTER TOCTOU.
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            // Layer 1 (existence, PARAMETERIZED) + permission + live freshness, in one
            // gate read on this open connection. `database` is bound as @db NVARCHAR(128).
            var gate = await ReadDbConfigGateAsync(connection, database, setting, ct).ConfigureAwait(false);

            // (1) Existence — absent => block, no ALTER.
            if (!gate.DatabaseExists)
            {
                return DbOutcome(database, setting, RemediationStatus.Blocked, applied: false, gate,
                    $"Database '{database}' was not found on the server (renamed or dropped since the finding); no change made.");
            }

            // (2) Permission — fail closed, no elevation, no retry.
            if (!gate.HasAlter)
            {
                return DbOutcome(database, setting, RemediationStatus.PermissionDenied, applied: false, gate,
                    $"The monitoring login '{gate.ExecutingLogin}' lacks ALTER on '{database}'. " +
                    $"Map it into '{database}' (CREATE USER ... FOR LOGIN) and GRANT ALTER, or connect with a login that has it.");
            }

            // (3) Freshness / idempotency — already in desired state => skip, no ALTER.
            if (gate.AlreadyInDesiredState)
            {
                return DbOutcome(database, setting, RemediationStatus.Skipped, applied: false, gate,
                    $"'{database}' is already in the desired state for {SetClauseFor(setting)}; nothing to do.");
            }

            // Gate passed. Build the statement: the ONLY non-constant token is the
            // bracketed, ALREADY-VALIDATED identifier (M-1 same-string: gate.ValidatedName
            // is the exact @db value sys.databases confirmed). The SET clause is a
            // hardcoded literal. No parameter is possible for a DDL identifier; QUOTENAME
            // doubling + prior existence validation is the control.
            var statement = BuildAlterStatement(gate.ValidatedName!, setting);

            int? execSpid;
            try
            {
                using var exec = connection.CreateCommand();
                exec.CommandTimeout = RemediationCommandTimeoutSeconds;
                // ALTER DATABASE SET is not permitted inside an explicit transaction;
                // it runs in autocommit. It is an online metadata change (no blocking,
                // no forced disconnects) — which is what makes these three always-safe.
                exec.CommandText = statement + " SELECT exec_spid = @@SPID;";
                var raw = await exec.ExecuteScalarAsync(ct).ConfigureAwait(false);
                execSpid = raw is null || raw is DBNull ? (int?)null : Convert.ToInt32(raw);
            }
            catch (SqlException ex)
            {
                var status = ex.Number is 297 or 15247 or 229 ? RemediationStatus.PermissionDenied : RemediationStatus.Error;
                return DbOutcome(database, setting, status, applied: false, gate,
                    $"ALTER DATABASE {SetClauseFor(setting)} failed (error {ex.Number}): {ex.Message}", statement);
            }

            return new DbConfigOutcome
            {
                Database = database,
                Setting = setting,
                Status = RemediationStatus.Success,
                Applied = true,
                ExecutingLogin = gate.ExecutingLogin,
                Message = $"Applied {SetClauseFor(setting)} on '{database}' (was {gate.CurrentValue}).",
                PriorValue = gate.CurrentValue,
                GeneratedSql = statement,
                GateSpid = gate.Spid,
                ExecSpid = execSpid
            };
        }

        private static DbConfigOutcome DbOutcome(string database, DbConfigSetting setting, RemediationStatus status, bool applied, DbConfigGateReading gate, string message, string? generatedSql = null) => new()
        {
            Database = database,
            Setting = setting,
            Status = status,
            Applied = applied,
            ExecutingLogin = gate.ExecutingLogin,
            Message = message,
            PriorValue = gate.CurrentValue,
            GeneratedSql = generatedSql,
            GateSpid = gate.Spid,
            ExecSpid = null
        };

        // ── WS3: percent-autogrowth fix (ALTER DATABASE … MODIFY FILE FILEGROWTH) ────
        //
        // Same safety class as the always-safe DB-config arm (metadata-only, online,
        // non-destructive) and the SAME no-injection mechanism, with ONE extra validated
        // identifier (the logical file name). All on ONE monitoring connection (no
        // InitialCatalog retarget — `ALTER DATABASE [db] MODIFY FILE` names the database
        // in the statement, exactly like ALTER DATABASE SET; mirror SetDatabaseOptionAsync,
        // NOT ForcePlanAsync):
        //   1. validate BOTH candidate names — the database against sys.databases and the
        //      logical file against sys.master_files joined to that database — PARAMETERIZED
        //      (@db / @logical NVARCHAR(128), no concatenation);
        //   2. bracket the EXACT validated strings with the inline QUOTENAME-equivalent
        //      (M-1 same-string: the strings validated in layer 1 are byte-identical to the
        //      strings bracketed here);
        //   3. the FILEGROWTH value is the already-computed int growthMb rendered as a
        //      bare integer literal (1024/64) — never data/operator free text, never a
        //      parameter (MODIFY FILE does not accept one for FILEGROWTH).
        // The growth target is NOT recomputed here — it is passed in from the finding's
        // FileGrowthTarget.RecommendedGrowthMb.

        /// <summary>
        /// Builds the exact ALTER DATABASE … MODIFY FILE statement: the ONLY non-constant
        /// tokens are the two bracketed, ALREADY-VALIDATED identifiers + the int growthMb
        /// literal. Callers MUST pass the exact strings the sys.databases / sys.master_files
        /// existence checks validated (M-1 same-string invariant). Exposed internal so the
        /// no-injection / same-string tests run against the executor's OWN builder.
        /// </summary>
        internal static string BuildModifyFileStatement(string validatedDatabase, string validatedLogicalName, int growthMb) =>
            $"ALTER DATABASE {QuoteIdentifier(validatedDatabase)} MODIFY FILE (NAME = {QuoteIdentifier(validatedLogicalName)}, FILEGROWTH = {growthMb}MB);";

        /// <summary>
        /// Read-only display probe (advisory only). Runs on the monitoring connection
        /// (no retarget); reads database + file existence + ALTER permission + the live
        /// current growth value. Mirrors <see cref="PreflightDbConfigAsync"/>.
        /// </summary>
        internal async Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            var gate = await ReadFileGrowthGateAsync(connection, database, logicalFileName, growthMb, ct).ConfigureAwait(false);

            return new FileGrowthPreflight
            {
                Database = database,
                LogicalFileName = logicalFileName,
                RecommendedGrowthMb = growthMb,
                DatabaseExists = gate.DatabaseExists,
                FileExists = gate.FileExists,
                HasAlter = gate.HasAlter,
                AlreadyInDesiredState = gate.AlreadyInDesiredState,
                ExecutingLogin = gate.ExecutingLogin,
                CurrentValue = gate.CurrentGrowthDisplay
            };
        }

        /// <summary>
        /// Self-gating percent-autogrowth ALTER (R2-MOD-1). The gate (parameterized
        /// sys.databases + sys.master_files existence + ALTER permission + live freshness)
        /// and the <c>ALTER DATABASE [db] MODIFY FILE (NAME = [logical], FILEGROWTH = N MB)</c>
        /// run on ONE open monitoring connection with no re-open between them. The validated
        /// @db / @logical strings are the SAME strings bracketed and placed in the statement
        /// (M-1 same-string invariant). MODIFY FILE runs in autocommit (no explicit
        /// transaction), like ALTER DATABASE SET. <paramref name="growthMb"/> is the
        /// already-computed target — never recomputed.
        /// </summary>
        internal async Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
        {
            // Defense-in-depth (security review L-2): growthMb is sourced only from
            // RecommendedGrowthMbFor (64 / 1024 today), but enforce the positive-MB invariant
            // HERE, where the FILEGROWTH literal is rendered, so a future producer change can
            // never emit a negative/zero growth. Caught per-target by the handler's try/catch.
            if (growthMb <= 0)
                throw new ArgumentOutOfRangeException(nameof(growthMb), growthMb, "FILEGROWTH must be a positive MB value.");

            // ONE monitoring connection for gate + mutation. No InitialCatalog retarget
            // (the database is named in the MODIFY FILE statement), no re-open — closes the
            // gate→ALTER TOCTOU.
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            var gate = await ReadFileGrowthGateAsync(connection, database, logicalFileName, growthMb, ct).ConfigureAwait(false);

            // (1) Database existence — absent => block, no ALTER.
            if (!gate.DatabaseExists)
            {
                return FileOutcome(database, logicalFileName, RemediationStatus.Blocked, applied: false, gate,
                    $"Database '{database}' was not found on the server (renamed or dropped since the finding); no change made.");
            }

            // (2) File existence — absent => block, no ALTER.
            if (!gate.FileExists)
            {
                return FileOutcome(database, logicalFileName, RemediationStatus.Blocked, applied: false, gate,
                    $"Logical file '{logicalFileName}' was not found on database '{database}' (renamed or removed since the finding); no change made.");
            }

            // (3) Permission — fail closed, no elevation, no retry.
            if (!gate.HasAlter)
            {
                return FileOutcome(database, logicalFileName, RemediationStatus.PermissionDenied, applied: false, gate,
                    $"The monitoring login '{gate.ExecutingLogin}' lacks ALTER on '{database}'. " +
                    $"Map it into '{database}' (CREATE USER ... FOR LOGIN) and GRANT ALTER, or connect with a login that has it.");
            }

            // (4) Freshness / idempotency — already at the desired fixed-MB growth => skip.
            if (gate.AlreadyInDesiredState)
            {
                return FileOutcome(database, logicalFileName, RemediationStatus.Skipped, applied: false, gate,
                    $"File '{logicalFileName}' on '{database}' is already set to FILEGROWTH = {growthMb}MB; nothing to do.");
            }

            // Gate passed. Build the statement: the ONLY non-constant tokens are the two
            // bracketed, ALREADY-VALIDATED identifiers (M-1 same-string) + the int growthMb
            // literal. No parameter is possible for a MODIFY FILE identifier or FILEGROWTH
            // value; QUOTENAME doubling + prior existence validation is the control.
            var statement = BuildModifyFileStatement(gate.ValidatedDatabase!, gate.ValidatedLogicalName!, growthMb);

            int? execSpid;
            try
            {
                using var exec = connection.CreateCommand();
                exec.CommandTimeout = RemediationCommandTimeoutSeconds;
                // ALTER DATABASE … MODIFY FILE is an online metadata change and runs in
                // autocommit (it is not permitted inside an explicit transaction).
                exec.CommandText = statement + " SELECT exec_spid = @@SPID;";
                var raw = await exec.ExecuteScalarAsync(ct).ConfigureAwait(false);
                execSpid = raw is null || raw is DBNull ? (int?)null : Convert.ToInt32(raw);
            }
            catch (SqlException ex)
            {
                var status = ex.Number is 297 or 15247 or 229 ? RemediationStatus.PermissionDenied : RemediationStatus.Error;
                return FileOutcome(database, logicalFileName, status, applied: false, gate,
                    $"ALTER DATABASE MODIFY FILE failed (error {ex.Number}): {ex.Message}", statement);
            }

            return new FileGrowthOutcome
            {
                Database = database,
                LogicalFileName = logicalFileName,
                Status = RemediationStatus.Success,
                Applied = true,
                ExecutingLogin = gate.ExecutingLogin,
                Message = $"Set FILEGROWTH = {growthMb}MB on '{logicalFileName}' in '{database}' (was {gate.CurrentGrowthDisplay}).",
                PriorValue = gate.CurrentGrowthDisplay,
                GeneratedSql = statement,
                GateSpid = gate.Spid,
                ExecSpid = execSpid
            };
        }

        private static FileGrowthOutcome FileOutcome(string database, string logicalFileName, RemediationStatus status, bool applied, FileGrowthGateReading gate, string message, string? generatedSql = null) => new()
        {
            Database = database,
            LogicalFileName = logicalFileName,
            Status = status,
            Applied = applied,
            ExecutingLogin = gate.ExecutingLogin,
            Message = message,
            PriorValue = gate.CurrentGrowthDisplay,
            GeneratedSql = generatedSql,
            GateSpid = gate.Spid,
            ExecSpid = null
        };

        /// <summary>
        /// The percent-autogrowth gate read: identity + parameterized sys.databases existence +
        /// parameterized sys.master_files file existence + ALTER permission + the live current
        /// growth (MB when fixed, NULL when still percent) + is_percent flag, in one round-trip
        /// on the open monitoring connection. Captures <see cref="FileGrowthGateReading.ValidatedDatabase"/>
        /// / <see cref="FileGrowthGateReading.ValidatedLogicalName"/> = the exact @db / @logical
        /// values (the same-string source for QUOTENAME, M-1). <c>growth</c> is in 8 KB pages, so
        /// <c>growth / 128</c> yields MB. AlreadyInDesiredState = the file is already a fixed-MB
        /// growth equal to <paramref name="growthMb"/>.
        /// </summary>
        private static async Task<FileGrowthGateReading> ReadFileGrowthGateAsync(SqlConnection connection, string database, string logicalFileName, int growthMb, CancellationToken ct)
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = RemediationCommandTimeoutSeconds;

            command.CommandText = @"
DECLARE @db_exists bit =
    CASE WHEN EXISTS (SELECT 1 FROM sys.databases AS d WHERE d.name = @db) THEN 1 ELSE 0 END;

DECLARE @file_exists bit =
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM sys.master_files AS mf
        JOIN sys.databases AS d
          ON mf.database_id = d.database_id
        WHERE d.name = @db
        AND   mf.name = @logical
    ) THEN 1 ELSE 0 END;

SELECT
    db_exists = @db_exists,
    file_exists = @file_exists,
    executing_login = SUSER_SNAME(),
    has_alter = CONVERT(int, ISNULL(HAS_PERMS_BY_NAME(@db, 'DATABASE', 'ALTER'), 0)),
    current_growth_mb =
    (
        SELECT CASE WHEN mf.is_percent_growth = 1 THEN NULL ELSE mf.growth / 128 END
        FROM sys.master_files AS mf
        JOIN sys.databases AS d
          ON mf.database_id = d.database_id
        WHERE d.name = @db
        AND   mf.name = @logical
    ),
    is_percent =
    (
        SELECT mf.is_percent_growth
        FROM sys.master_files AS mf
        JOIN sys.databases AS d
          ON mf.database_id = d.database_id
        WHERE d.name = @db
        AND   mf.name = @logical
    ),
    spid = @@SPID;";
            command.Parameters.Add(new SqlParameter("@db", SqlDbType.NVarChar, 128) { Value = database });
            command.Parameters.Add(new SqlParameter("@logical", SqlDbType.NVarChar, 128) { Value = logicalFileName });

            using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return new FileGrowthGateReading();

            var dbExists = !reader.IsDBNull(0) && reader.GetBoolean(0);
            var fileExists = !reader.IsDBNull(1) && reader.GetBoolean(1);
            var isPercent = !reader.IsDBNull(5) && reader.GetBoolean(5);
            int? currentGrowthMb = reader.IsDBNull(4) ? (int?)null : Convert.ToInt32(reader.GetValue(4));

            return new FileGrowthGateReading
            {
                DatabaseExists = dbExists,
                FileExists = fileExists,
                // Same-string invariant (M-1): only treat each candidate as validated when its
                // existence check passed; the bracketed tokens are THESE exact strings,
                // byte-identical to the validated @db / @logical parameter values.
                ValidatedDatabase = dbExists ? database : null,
                ValidatedLogicalName = fileExists ? logicalFileName : null,
                ExecutingLogin = reader.IsDBNull(2) ? null : reader.GetString(2),
                HasAlter = !reader.IsDBNull(3) && reader.GetInt32(3) == 1,
                CurrentGrowthMb = currentGrowthMb,
                IsPercentGrowth = isPercent,
                Spid = reader.IsDBNull(6) ? (int?)null : Convert.ToInt32(reader.GetInt16(6)),
                // Already in desired state ONLY when the file is a fixed-MB growth equal to the
                // target. A percent-growth file (current_growth_mb NULL) is never "desired".
                AlreadyInDesiredState = !isPercent && currentGrowthMb == growthMb
            };
        }

        private sealed class FileGrowthGateReading
        {
            public bool DatabaseExists { get; init; }
            public bool FileExists { get; init; }
            /// <summary>The validated @db value (== existence-checked string) — an M-1 same-string source for QUOTENAME.</summary>
            public string? ValidatedDatabase { get; init; }
            /// <summary>The validated @logical value (== existence-checked string) — an M-1 same-string source for QUOTENAME.</summary>
            public string? ValidatedLogicalName { get; init; }
            public string? ExecutingLogin { get; init; }
            public bool HasAlter { get; init; }
            /// <summary>Current fixed growth in MB, or null when the file still grows by percent.</summary>
            public int? CurrentGrowthMb { get; init; }
            public bool IsPercentGrowth { get; init; }
            public bool AlreadyInDesiredState { get; init; }
            public int? Spid { get; init; }

            /// <summary>Human prior-value for display/audit: "N%" while percent, else "N MB".</summary>
            public string? CurrentGrowthDisplay =>
                IsPercentGrowth ? "percent" : (CurrentGrowthMb is { } mb ? $"{mb} MB" : null);
        }

        // ── Clear cached plan (DBCC FREEPROCCACHE behind ALTER SERVER STATE) ────────
        //
        // The two catastrophic axes (get these EXACTLY right):
        //
        //  1. NEVER the bare/whole-cache form. The only DBCC text this method ever
        //     builds is the single-handle form `DBCC FREEPROCCACHE(@plan_handle)` with a
        //     typed varbinary(64) param bound to a NON-NULL, non-zero-length handle. The
        //     live-resolve selects only `plan_handle IS NOT NULL`; every resolved handle
        //     is re-checked in C# (null / zero-length → rejected) BEFORE any DBCC string
        //     is constructed. An empty resolve set is a Skip. There is no code path that
        //     emits `DBCC FREEPROCCACHE` with no argument.
        //
        //  2. Permission gate = the NAMED server permission
        //     `ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER SERVER STATE'), 0) = 1`,
        //     fail-closed (NULL/error → denied). NOT the generic 'ALTER' server token
        //     (which returns NULL even for sysadmin — the project_has_perms_by_name_alter
        //     gotcha). The documented least-privilege login (VIEW SERVER STATE + db_owner)
        //     LACKS ALTER SERVER STATE, so PermissionDenied is the PRIMARY runtime path —
        //     returned with a `GRANT ALTER SERVER STATE` guidance string, no elevation,
        //     no retry. The feature is opt-in.
        //
        // Single-connection self-gating (R2-MOD-1): the gate, the live resolve, and every
        // DBCC run on ONE open connection to the TARGET server (FREEPROCCACHE is
        // server-scoped — no InitialCatalog retarget). GateSpid == ExecSpid proves it.
        // No transaction (DBCC is not transactional). Per-handle independence: one
        // handle's DBCC error never aborts the rest.

        /// <summary>
        /// The number of query-text snippet characters captured per resolved handle for
        /// the M-2 disclosure (display only — never an execution input).
        /// </summary>
        private const int ClearPlanSnippetLength = 200;

        /// <summary>
        /// Self-gating clear-cached-plan. See <see cref="IRemediationExecutor.ClearProcCacheAsync"/>.
        /// The query hash is bound as a typed <c>binary(8)</c> param (the BAD_ACTOR
        /// precedent, never concatenated); each resolved plan handle is bound as a typed
        /// <c>varbinary(64)</c> param. The ONLY DBCC text built is the single-handle form.
        /// </summary>
        internal async Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
        {
            // Parse the hex query_hash ("0x...") to the raw binary(8) bytes in C#, so the
            // value bound to SQL is a typed binary param — never a string concatenated
            // into the text. A malformed value yields an empty/oversized array → Skip
            // (it can never resolve a handle, and never produces a DBCC string).
            var hashBytes = TryParseHexHandle(queryHash);
            if (hashBytes is null || hashBytes.Length != 8)
            {
                return new ClearPlanOutcome
                {
                    QueryHash = queryHash,
                    Status = RemediationStatus.Skipped,
                    Cleared = false,
                    HandlesCleared = 0,
                    Message = "The query hash was not a valid 8-byte hex value; nothing to clear.",
                    PriorValue = "0 plans (invalid query hash)"
                };
            }

            // ONE monitoring connection to the TARGET server. No InitialCatalog retarget:
            // FREEPROCCACHE is server-scoped. Gate + resolve + DBCC all ride this one open
            // connection — no re-open between gate and mutation (R2-MOD-1).
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            // ── Gate: NAMED ALTER SERVER STATE, fail-closed ─────────────────────────
            string? executingLogin;
            int? gateSpid;
            bool hasAlterServerState;
            {
                using var gate = connection.CreateCommand();
                gate.CommandTimeout = RemediationCommandTimeoutSeconds;
                // The NAMED server permission. The generic (NULL,NULL,'ALTER') token
                // returns NULL even for sysadmin (project_has_perms_by_name_alter_form);
                // the named 'ALTER SERVER STATE' evaluates 1 for a holder, 0 otherwise.
                // ISNULL(...,0) makes NULL/indeterminate fail closed.
                gate.CommandText = @"
SELECT
    executing_login = SUSER_SNAME(),
    has_alter_server_state = CONVERT(int, ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER SERVER STATE'), 0)),
    spid = @@SPID;";
                using var reader = await gate.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    return new ClearPlanOutcome
                    {
                        QueryHash = queryHash,
                        Status = RemediationStatus.Error,
                        Cleared = false,
                        Message = "Could not read the permission gate result; no change made."
                    };
                }
                executingLogin = reader.IsDBNull(0) ? null : reader.GetString(0);
                hasAlterServerState = !reader.IsDBNull(1) && reader.GetInt32(1) == 1;
                gateSpid = reader.IsDBNull(2) ? (int?)null : Convert.ToInt32(reader.GetInt16(2));
            }

            if (!hasAlterServerState)
            {
                // The DOMINANT runtime outcome on a least-privilege install. Fail closed
                // with grant guidance — no elevation, no retry, no DBCC.
                return new ClearPlanOutcome
                {
                    QueryHash = queryHash,
                    Status = RemediationStatus.PermissionDenied,
                    Cleared = false,
                    HandlesCleared = 0,
                    ExecutingLogin = executingLogin,
                    Message = $"The monitoring login '{executingLogin}' lacks ALTER SERVER STATE on this server, " +
                              "which clearing a cached plan requires. To opt in, run " +
                              $"GRANT ALTER SERVER STATE TO [{executingLogin}]; on the target server, or connect with a login that has it. No change was made.",
                    PriorValue = "0 plans (permission denied)",
                    GateSpid = gateSpid
                };
            }

            // ── Live resolve: current cached plan_handle(s) for this query_hash ─────
            // Reuse the BAD_ACTOR precedent (plan_handle IS NOT NULL). Capture per handle
            // the raw varbinary handle + database_name + a short query_text snippet (M-2).
            var resolved = new List<(byte[] Handle, string? Database, string? Snippet)>();
            {
                using var resolve = connection.CreateCommand();
                resolve.CommandTimeout = RemediationCommandTimeoutSeconds;
                resolve.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT DISTINCT
    qs.plan_handle,
    database_name = qs.database_name,
    query_text = LEFT(CAST(DECOMPRESS(qs.query_text) AS nvarchar(max)), @snippet_len)
FROM collect.query_stats AS qs
WHERE qs.query_hash = @query_hash
AND   qs.plan_handle IS NOT NULL
AND   qs.collection_time =
      (
          SELECT MAX(qs2.collection_time)
          FROM collect.query_stats AS qs2
          WHERE qs2.query_hash = @query_hash
          AND   qs2.plan_handle = qs.plan_handle
      );";
                resolve.Parameters.Add(new SqlParameter("@query_hash", SqlDbType.Binary, 8) { Value = hashBytes });
                resolve.Parameters.Add(new SqlParameter("@snippet_len", SqlDbType.Int) { Value = ClearPlanSnippetLength });

                using var reader = await resolve.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    // M-1 null/zero-length guard: reject in C# BEFORE any DBCC string is
                    // built. A null or empty handle never becomes a target.
                    if (reader.IsDBNull(0)) continue;
                    var handle = (byte[])reader.GetValue(0);
                    if (handle is null || handle.Length == 0) continue;

                    var db = reader.IsDBNull(1) ? null : reader.GetString(1);
                    var snippet = reader.IsDBNull(2) ? null : reader.GetString(2);
                    resolved.Add((handle, db, snippet));
                }
            }

            if (resolved.Count == 0)
            {
                // Nothing currently cached for this hash (aged out / recompiled). Skip —
                // the freshness/idempotency analog. No DBCC.
                return new ClearPlanOutcome
                {
                    QueryHash = queryHash,
                    Status = RemediationStatus.Skipped,
                    Cleared = false,
                    HandlesCleared = 0,
                    ExecutingLogin = executingLogin,
                    Message = "No cached plan is currently present for this query hash (it may have aged out or recompiled); nothing to clear.",
                    PriorValue = "0 plans (no surviving handle)",
                    GateSpid = gateSpid
                };
            }

            // ── Execute: one DBCC FREEPROCCACHE(@plan_handle) per surviving handle ──
            // Per-handle independence: one handle's error never aborts the rest.
            var handleContexts = new List<ClearPlanHandleContext>(resolved.Count);
            var sqlLog = new System.Text.StringBuilder();
            int cleared = 0;
            int? execSpid = null;

            foreach (var (handle, db, snippet) in resolved)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    using var dbcc = connection.CreateCommand();
                    dbcc.CommandTimeout = RemediationCommandTimeoutSeconds;
                    // The ONLY DBCC text ever built: the single-handle form with a typed
                    // varbinary(64) param. NEVER the bare form. The handle is bound, never
                    // concatenated.
                    dbcc.CommandText = "DBCC FREEPROCCACHE(@plan_handle); SELECT exec_spid = @@SPID;";
                    dbcc.Parameters.Add(new SqlParameter("@plan_handle", SqlDbType.VarBinary, 64) { Value = handle });
                    var raw = await dbcc.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    execSpid = raw is null || raw is DBNull ? execSpid : Convert.ToInt32(raw);

                    cleared++;
                    sqlLog.AppendLine($"DBCC FREEPROCCACHE(0x{Convert.ToHexString(handle)});" +
                                      (string.IsNullOrEmpty(db) ? "" : $"   -- {db}"));
                    handleContexts.Add(new ClearPlanHandleContext
                    {
                        Database = db,
                        QueryTextSnippet = snippet,
                        Cleared = true
                    });
                }
                catch (SqlException ex)
                {
                    handleContexts.Add(new ClearPlanHandleContext
                    {
                        Database = db,
                        QueryTextSnippet = snippet,
                        Cleared = false,
                        Error = $"DBCC FREEPROCCACHE failed (error {ex.Number}): {ex.Message}"
                    });
                }
            }

            var priorValue = $"{resolved.Count} plan(s) cached for this query hash";
            if (cleared == 0)
            {
                // Every handle errored (e.g. all aged out between resolve and DBCC). Report
                // an error so it is visible; per-handle errors are carried for detail.
                return new ClearPlanOutcome
                {
                    QueryHash = queryHash,
                    Status = RemediationStatus.Error,
                    Cleared = false,
                    HandlesCleared = 0,
                    ExecutingLogin = executingLogin,
                    Message = $"All {resolved.Count} resolved plan handle(s) failed to clear; see per-handle detail.",
                    Handles = handleContexts,
                    GeneratedSql = sqlLog.Length == 0 ? null : sqlLog.ToString().TrimEnd(),
                    PriorValue = priorValue,
                    GateSpid = gateSpid,
                    ExecSpid = execSpid
                };
            }

            return new ClearPlanOutcome
            {
                QueryHash = queryHash,
                Status = RemediationStatus.Success,
                Cleared = true,
                HandlesCleared = cleared,
                ExecutingLogin = executingLogin,
                Message = $"Cleared {cleared} cached plan(s) for this query hash; they will recompile on next execution.",
                Handles = handleContexts,
                GeneratedSql = sqlLog.ToString().TrimEnd(),
                PriorValue = priorValue,
                GateSpid = gateSpid,
                ExecSpid = execSpid
            };
        }

        /// <summary>
        /// Parses a hex handle string ("0x" + even-length hex) to its raw bytes, or null
        /// when malformed. Used for the query_hash → binary(8) bind (the value is ALWAYS
        /// a typed param, never concatenated) and never to build DBCC text.
        /// </summary>
        internal static byte[]? TryParseHexHandle(string? hex)
        {
            if (string.IsNullOrEmpty(hex)) return null;
            var s = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? hex.Substring(2) : hex;
            if (s.Length == 0 || (s.Length % 2) != 0) return null;
            try
            {
                return Convert.FromHexString(s);
            }
            catch
            {
                return null;
            }
        }

        // ── WS3: server-level config (sp_configure + RECONFIGURE) ───────────────────
        //
        // The sharp edges (get these EXACTLY right):
        //
        //  1. The sp_configure NAME is a HARDCODED compile-time literal selected by the
        //     ServerConfigSetting enum (SpConfigureNameLiteral) — NEVER built from data. The value
        //     is bound as an @value INT parameter (range-validated in C# first). There is no path
        //     that concatenates either into the batch.
        //
        //  2. ONLY Maxdop + CostThreshold are executable. MaxServerMemory / MinServerMemory throw
        //     before any gate/mutation — their right value is RAM/workload-dependent and must never
        //     be applied from a guessed number (advise-only; the card is copy-paste only).
        //
        //  3. Permission gate = the NAMED server permission ALTER SETTINGS (fail-closed), OR the
        //     sysadmin / serveradmin fixed roles (which can sp_configure even without the named
        //     perm). Deny → PermissionDenied with grant guidance, no elevation, no retry. The
        //     generic (NULL,NULL,'ALTER') token returns NULL even for sysadmin
        //     (project_has_perms_by_name_alter_form), so it is NOT used here.
        //
        // Single-connection self-gating (R2-MOD-1): the gate read + the show-advanced-options +
        // RECONFIGURE + the sp_configure of the target + RECONFIGURE all run on ONE open connection
        // to the TARGET server (server-scoped — no InitialCatalog retarget). GateSpid == ExecSpid
        // proves it. Plain RECONFIGURE (not WITH OVERRIDE).

        /// <summary>The sane inclusive value range for an executable server-config setting (defense-in-depth before binding @value).</summary>
        private static (long Min, long Max) ServerConfigValueRange(ServerConfigSetting setting) => setting switch
        {
            ServerConfigSetting.Maxdop => (0, 32767),
            ServerConfigSetting.CostThreshold => (0, 32767),
            _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Only MAXDOP / CostThreshold are executable.")
        };

        /// <summary>
        /// The HARDCODED <c>sp_configure</c> name literal for the two EXECUTABLE settings. This is
        /// the entire selection surface (§1) — nothing here comes from data. Memory settings throw:
        /// they are advise-only and must never reach the executor.
        /// </summary>
        internal static string SpConfigureNameLiteral(ServerConfigSetting setting) => setting switch
        {
            ServerConfigSetting.Maxdop => "max degree of parallelism",
            ServerConfigSetting.CostThreshold => "cost threshold for parallelism",
            ServerConfigSetting.MaxServerMemory => throw new ArgumentOutOfRangeException(nameof(setting), setting, "max server memory is advise-only — never applied."),
            ServerConfigSetting.MinServerMemory => throw new ArgumentOutOfRangeException(nameof(setting), setting, "min server memory is advise-only — never applied."),
            _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown ServerConfigSetting")
        };

        /// <summary>Whether a server-config setting can be applied (Maxdop / CostThreshold) vs advise-only (memory).</summary>
        internal static bool IsExecutableServerConfig(ServerConfigSetting setting) =>
            setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold;

        /// <summary>
        /// Read-only display probe (advisory only). Runs on the monitoring connection to the target
        /// server (no retarget); reads the named/role permission + the live current value. Mirrors
        /// <see cref="PreflightDbConfigAsync"/>. Advise-only settings report Executable = false.
        /// </summary>
        internal async Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            var gate = await ReadServerConfigGateAsync(connection, setting, ct).ConfigureAwait(false);
            var executable = IsExecutableServerConfig(setting);

            return new ServerConfigPreflight
            {
                Setting = setting,
                RecommendedValue = recommendedValue,
                Executable = executable,
                HasPermission = gate.HasPermission,
                CurrentValue = gate.CurrentValue,
                ExecutingLogin = gate.ExecutingLogin,
                // "Already desired" only applies to executable settings (we never apply memory).
                AlreadyInDesiredState = executable && gate.CurrentValue == recommendedValue
            };
        }

        /// <summary>
        /// Self-gating server-config apply (R2-MOD-1). The gate (named ALTER SETTINGS / sysadmin /
        /// serveradmin + live current value) and the
        /// <c>sp_configure 'show advanced options',1; RECONFIGURE; sp_configure '&lt;hardcoded&gt;',@value; RECONFIGURE;</c>
        /// batch run on ONE open connection to the target server, no re-open between gate and
        /// mutation (closes the TOCTOU). The name is a HARDCODED literal selected by
        /// <paramref name="setting"/>; the value is bound as @value INT (range-validated first).
        /// IsDestructive context = false (online metadata change). Only Maxdop / CostThreshold are
        /// executable — memory settings return an error outcome WITHOUT touching the server.
        /// </summary>
        internal async Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
        {
            // (0) Advise-only guard — BEFORE any connection. max/min server memory must never be
            // applied from a guessed value; the handler also blocks them, this is defense-in-depth.
            if (!IsExecutableServerConfig(setting))
            {
                return new ServerConfigOutcome
                {
                    Setting = setting,
                    Status = RemediationStatus.Blocked,
                    Applied = false,
                    Message = $"{FactRemediation.SpConfigureName(setting)} is advise-only — its correct value is RAM/workload-dependent and is never applied automatically. Copy the statement and set the value you choose.",
                    PriorValue = null
                };
            }

            // (0b) Range validation — defense-in-depth before binding @value, where the literal is
            // sent. A future producer change can never push an out-of-range value to sp_configure.
            var (min, max) = ServerConfigValueRange(setting);
            if (value < min || value > max)
            {
                return new ServerConfigOutcome
                {
                    Setting = setting,
                    Status = RemediationStatus.Error,
                    Applied = false,
                    Message = $"Recommended value {value} for {FactRemediation.SpConfigureName(setting)} is outside the valid range [{min}, {max}]; no change made.",
                    PriorValue = null
                };
            }

            // ONE connection to the target server (server-scoped — no InitialCatalog retarget). Gate
            // + RECONFIGUREs + sp_configure all ride this one open connection (R2-MOD-1).
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ApplySessionOptionsAsync(connection, ct).ConfigureAwait(false);

            var gate = await ReadServerConfigGateAsync(connection, setting, ct).ConfigureAwait(false);

            // (1) Permission — fail closed with grant guidance, no elevation, no retry.
            if (!gate.HasPermission)
            {
                return new ServerConfigOutcome
                {
                    Setting = setting,
                    Status = RemediationStatus.PermissionDenied,
                    Applied = false,
                    ExecutingLogin = gate.ExecutingLogin,
                    PriorValue = gate.CurrentValue,
                    Message = $"The monitoring login '{gate.ExecutingLogin}' lacks ALTER SETTINGS on this server, which sp_configure requires. " +
                              $"GRANT ALTER SETTINGS TO [{gate.ExecutingLogin}]; on the target server (or use a sysadmin/serveradmin login). No change was made.",
                    GateSpid = gate.Spid
                };
            }

            // (2) Freshness / idempotency — already at the recommended value => skip, no sp_configure.
            if (gate.CurrentValue == value)
            {
                return new ServerConfigOutcome
                {
                    Setting = setting,
                    Status = RemediationStatus.Skipped,
                    Applied = false,
                    ExecutingLogin = gate.ExecutingLogin,
                    PriorValue = gate.CurrentValue,
                    Message = $"{FactRemediation.SpConfigureName(setting)} is already {value}; nothing to do.",
                    GateSpid = gate.Spid
                };
            }

            // Gate passed. Build the batch: the name is a HARDCODED literal selected by the enum;
            // the value is bound as @value INT. show advanced options is required because both
            // MAXDOP and CTFP are advanced. Plain RECONFIGURE (not WITH OVERRIDE).
            var nameLiteral = SpConfigureNameLiteral(setting);
            var statement =
                "EXEC sys.sp_configure N'show advanced options', 1; RECONFIGURE; " +
                $"EXEC sys.sp_configure N'{nameLiteral}', @value; RECONFIGURE;";

            int? execSpid;
            try
            {
                using var exec = connection.CreateCommand();
                exec.CommandTimeout = RemediationCommandTimeoutSeconds;
                exec.CommandText = statement + " SELECT exec_spid = @@SPID;";
                exec.Parameters.Add(new SqlParameter("@value", SqlDbType.Int) { Value = (int)value });
                var raw = await exec.ExecuteScalarAsync(ct).ConfigureAwait(false);
                execSpid = raw is null || raw is DBNull ? (int?)null : Convert.ToInt32(raw);
            }
            catch (SqlException ex)
            {
                var status = ex.Number is 297 or 15247 or 229 ? RemediationStatus.PermissionDenied : RemediationStatus.Error;
                return new ServerConfigOutcome
                {
                    Setting = setting,
                    Status = status,
                    Applied = false,
                    ExecutingLogin = gate.ExecutingLogin,
                    PriorValue = gate.CurrentValue,
                    Message = $"sp_configure '{FactRemediation.SpConfigureName(setting)}' failed (error {ex.Number}): {ex.Message}",
                    GeneratedSql = statement,
                    GateSpid = gate.Spid
                };
            }

            return new ServerConfigOutcome
            {
                Setting = setting,
                Status = RemediationStatus.Success,
                Applied = true,
                ExecutingLogin = gate.ExecutingLogin,
                PriorValue = gate.CurrentValue,
                Message = $"Set {FactRemediation.SpConfigureName(setting)} to {value} (was {gate.CurrentValue}).",
                GeneratedSql = statement,
                GateSpid = gate.Spid,
                ExecSpid = execSpid
            };
        }

        /// <summary>
        /// The server-config gate read: identity + named ALTER SETTINGS / sysadmin / serveradmin
        /// permission + the live current value_in_use for the setting, in one round-trip on the open
        /// connection. The configuration NAME used in the live-value lookup is a HARDCODED literal
        /// selected by the enum (constant SQL, never data), bound only as the @config_name read
        /// parameter (read-only lookup — not the mutation path). Fail-closed permission.
        /// </summary>
        private static async Task<ServerConfigGateReading> ReadServerConfigGateAsync(SqlConnection connection, ServerConfigSetting setting, CancellationToken ct)
        {
            // The configuration name for the read-only current-value lookup. For advise-only memory
            // settings (which never apply) we still want a correct prior-value, so use the shared
            // display name; for executable settings this equals the hardcoded literal.
            var configName = FactRemediation.SpConfigureName(setting);

            using var command = connection.CreateCommand();
            command.CommandTimeout = RemediationCommandTimeoutSeconds;
            // Named ALTER SETTINGS (fail-closed via ISNULL) OR the sysadmin/serveradmin fixed roles
            // (which can sp_configure without the named perm). The generic (NULL,NULL,'ALTER') token
            // is intentionally NOT used (returns NULL even for sysadmin).
            command.CommandText = @"
SELECT
    executing_login = SUSER_SNAME(),
    has_permission = CONVERT(int,
        CASE WHEN ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER SETTINGS'), 0) = 1
                  OR IS_SRVROLEMEMBER('sysadmin') = 1
                  OR IS_SRVROLEMEMBER('serveradmin') = 1
             THEN 1 ELSE 0 END),
    current_value = ISNULL((SELECT CONVERT(bigint, c.value_in_use) FROM sys.configurations AS c WHERE c.name = @config_name), 0),
    spid = @@SPID;";
            command.Parameters.Add(new SqlParameter("@config_name", SqlDbType.NVarChar, 128) { Value = configName });

            using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return new ServerConfigGateReading();

            return new ServerConfigGateReading
            {
                ExecutingLogin = reader.IsDBNull(0) ? null : reader.GetString(0),
                HasPermission = !reader.IsDBNull(1) && reader.GetInt32(1) == 1,
                CurrentValue = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                Spid = reader.IsDBNull(3) ? (int?)null : Convert.ToInt32(reader.GetInt16(3))
            };
        }

        private sealed class ServerConfigGateReading
        {
            public string? ExecutingLogin { get; init; }
            public bool HasPermission { get; init; }
            public long CurrentValue { get; init; }
            public int? Spid { get; init; }
        }

        /// <summary>
        /// The DB-config gate read: identity + parameterized sys.databases existence +
        /// ALTER permission + the live current setting value, in one round-trip on the
        /// open monitoring connection. The setting column is selected by the enum (a
        /// constant column expression — not data). Captures <see cref="DbConfigGateReading.ValidatedName"/>
        /// = the exact @db value (the same-string source for QUOTENAME, M-1).
        /// </summary>
        private static async Task<DbConfigGateReading> ReadDbConfigGateAsync(SqlConnection connection, string database, DbConfigSetting setting, CancellationToken ct)
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = RemediationCommandTimeoutSeconds;

            // The current-value column expression is chosen by the enum (constant SQL,
            // never data). is_auto_shrink_on / is_auto_close_on / page_verify_option_desc
            // are all server-visible from the monitoring connection (VIEW ANY DATABASE →
            // public). desired-state is computed server-side so the skip decision uses
            // the LIVE read, never the extractor's possibly-stale CurrentValue (m-2).
            string currentValueExpr;
            string desiredExpr;
            switch (setting)
            {
                case DbConfigSetting.AutoShrinkOff:
                    currentValueExpr = "CASE WHEN d.is_auto_shrink_on = 1 THEN N'ON' ELSE N'OFF' END";
                    desiredExpr = "CASE WHEN d.is_auto_shrink_on = 0 THEN 1 ELSE 0 END";
                    break;
                case DbConfigSetting.AutoCloseOff:
                    currentValueExpr = "CASE WHEN d.is_auto_close_on = 1 THEN N'ON' ELSE N'OFF' END";
                    desiredExpr = "CASE WHEN d.is_auto_close_on = 0 THEN 1 ELSE 0 END";
                    break;
                case DbConfigSetting.PageVerifyChecksum:
                    currentValueExpr = "d.page_verify_option_desc";
                    desiredExpr = "CASE WHEN d.page_verify_option_desc = N'CHECKSUM' THEN 1 ELSE 0 END";
                    break;
                case DbConfigSetting.ReadCommittedSnapshotOn:
                    // B3 Phase 3 (R2-MOD-1): the live freshness column is
                    // is_read_committed_snapshot_on. Desired state = on (= 1) → Skip if
                    // already on. The current value (ON/OFF) is captured for prior_value.
                    currentValueExpr = "CASE WHEN d.is_read_committed_snapshot_on = 1 THEN N'ON' ELSE N'OFF' END";
                    desiredExpr = "CASE WHEN d.is_read_committed_snapshot_on = 1 THEN 1 ELSE 0 END";
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown DbConfigSetting");
            }

            command.CommandText = $@"
DECLARE @exists bit =
    CASE WHEN EXISTS (SELECT 1 FROM sys.databases AS d WHERE d.name = @db) THEN 1 ELSE 0 END;

SELECT
    db_exists = @exists,
    executing_login = SUSER_SNAME(),
    has_alter = CONVERT(int, ISNULL(HAS_PERMS_BY_NAME(@db, 'DATABASE', 'ALTER'), 0)),
    current_value = (SELECT {currentValueExpr} FROM sys.databases AS d WHERE d.name = @db),
    already_desired = ISNULL((SELECT {desiredExpr} FROM sys.databases AS d WHERE d.name = @db), 0),
    spid = @@SPID;";
            command.Parameters.Add(new SqlParameter("@db", SqlDbType.NVarChar, 128) { Value = database });

            using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return new DbConfigGateReading();

            var dbExists = !reader.IsDBNull(0) && reader.GetBoolean(0);
            return new DbConfigGateReading
            {
                DatabaseExists = dbExists,
                // Same-string invariant (M-1): only treat the candidate as validated
                // when the existence check passed; the bracketed token is THIS exact
                // string, byte-identical to the validated @db parameter value.
                ValidatedName = dbExists ? database : null,
                ExecutingLogin = reader.IsDBNull(1) ? null : reader.GetString(1),
                HasAlter = !reader.IsDBNull(2) && reader.GetInt32(2) == 1,
                CurrentValue = reader.IsDBNull(3) ? null : reader.GetString(3),
                AlreadyInDesiredState = !reader.IsDBNull(4) && reader.GetInt32(4) == 1,
                Spid = reader.IsDBNull(5) ? (int?)null : Convert.ToInt32(reader.GetInt16(5))
            };
        }

        private sealed class DbConfigGateReading
        {
            public bool DatabaseExists { get; init; }
            /// <summary>The validated @db value (== existence-checked string) — the M-1 same-string source for QUOTENAME.</summary>
            public string? ValidatedName { get; init; }
            public string? ExecutingLogin { get; init; }
            public bool HasAlter { get; init; }
            public string? CurrentValue { get; init; }
            public bool AlreadyInDesiredState { get; init; }
            public int? Spid { get; init; }
        }

        /// <summary>
        /// Gate part 1: identity + permission, using only always-accessible intrinsics
        /// (DB_NAME / SUSER_SNAME / HAS_PERMS_BY_NAME / @@SPID). Safe for a
        /// least-privilege login that cannot read the Query Store catalog views.
        /// </summary>
        private static async Task<GateReading> ReadGateIdentityAsync(SqlConnection connection, CancellationToken ct)
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = RemediationCommandTimeoutSeconds;
            // HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER') is the DB-scoped form:
            // it returns 1 for a principal holding ALTER on the connected database
            // (incl. sysadmin / db_owner) and 0 otherwise. The (NULL, NULL, 'ALTER')
            // server-scoped form the plan's O5 specified returns NULL even for
            // sysadmin — verified against sql2022 — so it would fail closed for every
            // login. DB_NAME() (not a literal) keeps it correct after the catalog
            // retarget. This is the permission sp_query_store_force_plan actually needs.
            command.CommandText = @"
SELECT
    current_db = DB_NAME(),
    executing_login = SUSER_SNAME(),
    has_alter = CONVERT(int, ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER'), 0)),
    spid = @@SPID;";

            using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return new GateReading();

            return new GateReading
            {
                CurrentDatabase = reader.IsDBNull(0) ? null : reader.GetString(0),
                ExecutingLogin = reader.IsDBNull(1) ? null : reader.GetString(1),
                HasAlter = !reader.IsDBNull(2) && reader.GetInt32(2) == 1,
                Spid = reader.IsDBNull(3) ? (int?)null : Convert.ToInt32(reader.GetInt16(3))
            };
        }

        /// <summary>
        /// Gate part 2: Query Store freshness/state for the target, on the SAME open
        /// connection. Reads sys.database_query_store_options / sys.query_store_plan
        /// (requires VIEW DATABASE STATE), so it must only run after the ALTER check
        /// has passed. Mutates <paramref name="gate"/> in place. The IDs are typed
        /// BigInt parameters; nothing is concatenated.
        /// </summary>
        private static async Task ReadQueryStoreStateAsync(SqlConnection connection, long queryId, long planId, GateReading gate, CancellationToken ct)
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = RemediationCommandTimeoutSeconds;
            command.CommandText = @"
SELECT
    qs_state = (SELECT TOP (1) dqso.actual_state_desc FROM sys.database_query_store_options AS dqso),
    plan_present = CASE WHEN EXISTS (SELECT 1 FROM sys.query_store_plan AS qsp WHERE qsp.query_id = @query_id AND qsp.plan_id = @plan_id) THEN 1 ELSE 0 END,
    is_forced = ISNULL((SELECT TOP (1) CONVERT(int, qsp.is_forced_plan) FROM sys.query_store_plan AS qsp WHERE qsp.query_id = @query_id AND qsp.plan_id = @plan_id), 0),
    force_failure_count = ISNULL((SELECT TOP (1) qsp.force_failure_count FROM sys.query_store_plan AS qsp WHERE qsp.query_id = @query_id AND qsp.plan_id = @plan_id), 0);";
            command.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
            command.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });

            using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return;

            gate.QueryStoreState = reader.IsDBNull(0) ? null : reader.GetString(0);
            gate.PlanPresent = !reader.IsDBNull(1) && reader.GetInt32(1) == 1;
            gate.IsForcedPlan = !reader.IsDBNull(2) && reader.GetInt32(2) == 1;
            gate.ForceFailureCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3);
        }

        private sealed class GateReading
        {
            // Part 1 (identity) — init-set at construction.
            public string? CurrentDatabase { get; init; }
            public string? ExecutingLogin { get; init; }
            public bool HasAlter { get; init; }
            public int? Spid { get; init; }

            // Part 2 (Query Store state) — populated only after the ALTER check passes.
            public string? QueryStoreState { get; set; }
            public bool PlanPresent { get; set; }
            public bool IsForcedPlan { get; set; }
            public long ForceFailureCount { get; set; }
        }
    }
}
