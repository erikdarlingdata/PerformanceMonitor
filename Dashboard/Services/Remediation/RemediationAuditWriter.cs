/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// Persists remediation audit rows to <c>config.remediation_action_log</c> on
    /// the MONITORING connection (the PerformanceMonitor DB), NOT the retargeted
    /// user-DB connection. Also answers the two monitoring-side gate questions:
    /// whether the audit table exists at all (A1 / R2-MOD-2) and whether a prior
    /// B3 force exists for a target (so un-apply only undoes what B3 applied).
    /// </summary>
    public sealed class RemediationAuditWriter
    {
        private readonly string _monitoringConnectionString;

        public RemediationAuditWriter(string monitoringConnectionString)
        {
            _monitoringConnectionString = monitoringConnectionString;
        }

        /// <summary>
        /// True when <c>config.remediation_action_log</c> exists. The connection
        /// uses the monitoring connection string as-is (InitialCatalog =
        /// PerformanceMonitor), built only from the existing connection string.
        /// </summary>
        public async Task<bool> TableExistsAsync(CancellationToken ct)
        {
            using var connection = new SqlConnection(_monitoringConnectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT object_id = OBJECT_ID(N'config.remediation_action_log', N'U');";
            var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return result is not null && result is not DBNull;
        }

        /// <summary>
        /// True when a successful <c>force</c> for this target has been recorded and
        /// not since matched by a successful <c>unforce</c>.
        /// </summary>
        public async Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct)
        {
            using var connection = new SqlConnection(_monitoringConnectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT has_prior_force =
    CASE
        WHEN
        (
            SELECT COUNT_BIG(*)
            FROM config.remediation_action_log AS r
            WHERE r.target_database = @target_database
            AND   r.query_id = @query_id
            AND   r.plan_id = @plan_id
            AND   r.action = 'force'
            AND   r.result = 'success'
        )
        >
        (
            SELECT COUNT_BIG(*)
            FROM config.remediation_action_log AS r
            WHERE r.target_database = @target_database
            AND   r.query_id = @query_id
            AND   r.plan_id = @plan_id
            AND   r.action = 'unforce'
            AND   r.result = 'success'
        )
        THEN 1
        ELSE 0
    END;";
            command.Parameters.Add(new SqlParameter("@target_database", SqlDbType.NVarChar, 128) { Value = database });
            command.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
            command.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });

            var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
        }

        /// <summary>
        /// Writes one audit row (parameterized). Returns false if the INSERT fails
        /// against a present table (the O3 applied-but-unlogged case) — it never
        /// throws, so a logging failure cannot turn a successful mutation into a
        /// reported error. Fills <see cref="RemediationAuditRecord.TargetServer"/>
        /// from the connection's DataSource when the caller left it null.
        /// </summary>
        public async Task<bool> WriteAsync(RemediationAuditRecord record, CancellationToken ct)
        {
            try
            {
                using var connection = new SqlConnection(_monitoringConnectionString);
                await connection.OpenAsync(ct).ConfigureAwait(false);

                var targetServer = record.TargetServer
                    ?? new SqlConnectionStringBuilder(_monitoringConnectionString).DataSource;

                using var command = connection.CreateCommand();
                command.CommandText = @"
INSERT INTO
    config.remediation_action_log
(
    operator_identity,
    executing_login,
    used_elevated_cred,
    target_server,
    target_database,
    finding_fact_key,
    query_id,
    plan_id,
    action,
    prior_value,
    generated_sql,
    result,
    error_message,
    consent_acknowledged,
    source_alert_ref
)
VALUES
(
    @operator_identity,
    @executing_login,
    0,
    @target_server,
    @target_database,
    @finding_fact_key,
    @query_id,
    @plan_id,
    @action,
    @prior_value,
    @generated_sql,
    @result,
    @error_message,
    @consent_acknowledged,
    @source_alert_ref
);";
                command.Parameters.Add(new SqlParameter("@operator_identity", SqlDbType.NVarChar, 256) { Value = (object?)record.OperatorIdentity ?? DBNull.Value });
                command.Parameters.Add(new SqlParameter("@executing_login", SqlDbType.NVarChar, 128) { Value = (object?)record.ExecutingLogin ?? DBNull.Value });
                command.Parameters.Add(new SqlParameter("@target_server", SqlDbType.NVarChar, 256) { Value = (object?)targetServer ?? DBNull.Value });
                command.Parameters.Add(new SqlParameter("@target_database", SqlDbType.NVarChar, 128) { Value = record.TargetDatabase });
                command.Parameters.Add(new SqlParameter("@finding_fact_key", SqlDbType.VarChar, 64) { Value = record.FactKey });
                // B-1: DB_CONFIG rows have no query_id/plan_id — write DBNull, not 0.
                command.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = (object?)record.QueryId ?? DBNull.Value });
                command.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = (object?)record.PlanId ?? DBNull.Value });
                // B-3: widen to VarChar(32) — the DB_CONFIG taxonomy (e.g.
                // 'set_page_verify_checksum' = 24 chars) silently truncates at 16
                // here, BEFORE the widened column is reached. Column width alone is
                // insufficient; this client parameter is the first truncation point.
                command.Parameters.Add(new SqlParameter("@action", SqlDbType.VarChar, 32) { Value = record.Action });
                command.Parameters.Add(new SqlParameter("@prior_value", SqlDbType.NVarChar, 128) { Value = (object?)record.PriorValue ?? DBNull.Value });
                command.Parameters.Add(new SqlParameter("@generated_sql", SqlDbType.NVarChar, -1) { Value = (object?)record.GeneratedSql ?? DBNull.Value });
                command.Parameters.Add(new SqlParameter("@result", SqlDbType.VarChar, 16) { Value = record.Result });
                command.Parameters.Add(new SqlParameter("@error_message", SqlDbType.NVarChar, -1) { Value = (object?)record.ErrorMessage ?? DBNull.Value });
                // M-3: a REAL bound Bit param (it varies per row) — NOT the inline-`0`
                // pattern used for used_elevated_cred above (which is always 0).
                command.Parameters.Add(new SqlParameter("@consent_acknowledged", SqlDbType.Bit) { Value = record.ConsentAcknowledged });
                command.Parameters.Add(new SqlParameter("@source_alert_ref", SqlDbType.NVarChar, 256) { Value = (object?)record.SourceAlertRef ?? DBNull.Value });

                var rows = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                return rows == 1;
            }
            catch (SqlException)
            {
                // O3: a transient INSERT failure against a present table. Surface as
                // applied-but-unlogged; never roll back the (reversible) mutation.
                return false;
            }
        }
    }
}
