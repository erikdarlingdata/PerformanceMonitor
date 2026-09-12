/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Typed CRUD over <c>config.custom_alert_rules</c> (#3285) — the user-authored alert rules. Deliberately the
/// same store discipline as <see cref="CustomViewStore"/>: public-const SQL, bound <c>$N</c> parameters,
/// <c>definition</c> bound as <c>jsonb</c>, bare table name (the pool's <c>search_path = collect, config,
/// public</c> resolves <c>custom_alert_rules</c> to <c>config.custom_alert_rules</c>). The CRUD callers (the
/// web editor + MCP tools) connect on the least-privilege viewer/mcp pool, which carries the single narrow
/// <c>INSERT/UPDATE/DELETE ON config.custom_alert_rules</c> grant (see <c>DarlingManagedRoles</c>); the
/// <see cref="CustomAlertEvaluator"/> instead instantiates this store on the worker's owner pool to
/// <see cref="ListEnabledAsync"/> each sweep.
///
/// <para>Adds one column beyond the <c>custom_views</c> shape: <c>enabled</c>, which the evaluator filters on
/// so a rule can be paused without deleting it (and its accumulated <c>custom_alert_state</c>). Optimistic
/// concurrency (<c>WHERE id = $1 AND version = $n</c>) and the 0-rowcount NotFound-vs-Conflict disambiguation
/// are identical to <see cref="CustomViewStore"/>; the route/tool layer owns the definition STRUCTURE
/// validation (the compose <c>TryParsePanel</c> authority + the rule validator), this store adds only light
/// argument guards so it is safe to call independently.</para>
/// </summary>
public sealed class CustomAlertRuleStore
{
    private readonly NpgsqlDataSource _dataSource;

    public CustomAlertRuleStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <summary>
    /// The lightweight list read: the (potentially large) <c>definition</c> body is never selected. LEFT JOINs
    /// each rule to when it LAST FIRED (#3360) — <c>MAX(alert_time)</c> over the <c>config_alert_log</c> rows keyed
    /// on the rule's IMMUTABLE fire key <c>'Custom:' || id</c> (<see cref="CustomAlertEvaluator.MetricNameFor"/>).
    ///
    /// <para>The correlation is that EXACT fire key only. A resolve / teardown row is written under a DIFFERENT
    /// <c>metric_name</c> — <c>'&lt;display name&gt; Resolved'</c> (<c>CustomAlertEvaluator.DeliverResolveAsync</c> /
    /// <c>WriteTeardownResolutionAsync</c>) — which can never equal <c>'Custom:&lt;numeric id&gt;'</c>, so a resolve
    /// is never counted as a fire and one rule's fires never bleed into another's. The <c>LIKE 'Custom:%'</c> is only
    /// a pre-filter that shrinks the grouped set; the <c>= 'Custom:' || id</c> join key is what actually correlates,
    /// so even a (contrived) row like <c>'Custom:5 Resolved'</c> — which passes the LIKE — does NOT join to rule 5.</para>
    ///
    /// <para>Every fire DELIVERY writes a <c>Custom:&lt;id&gt;</c> row regardless of mute / channel (the deliver path
    /// records the incident even when muted or channel-less), so <c>MAX(alert_time)</c> over those rows means "the
    /// last time the rule's condition fired (the incident was recorded)" — the intended meaning of "last fired" —
    /// and the read is deliberately NOT filtered on <c>alert_sent</c> / <c>notification_type</c>. It is ONE
    /// round-trip: the grouped subquery scans the fire rows once and joins to the whole rule set, never a per-rule
    /// query. The <c>idx_config_alert_log_time (server_id, metric_name, alert_time)</c> index leads with
    /// <c>server_id</c>, so this deliberately cross-server aggregate scans rather than seeks — acceptable on this
    /// cold, operator-triggered list render over the retention-bounded history table (the per-(server, metric)
    /// cooldown seeds still seek that index). A rule that has never fired LEFT-JOINs to a NULL <c>last_fired</c>.</para>
    /// </summary>
    public const string ListSql = @"
SELECT
    r.id,
    r.name,
    r.description,
    r.enabled,
    r.version,
    r.updated_at,
    r.updated_by,
    f.last_fired
FROM custom_alert_rules AS r
LEFT JOIN
(
    SELECT
        metric_name,
        MAX(alert_time) AS last_fired
    FROM config_alert_log
    WHERE metric_name LIKE 'Custom:%'
    GROUP BY metric_name
) AS f
    ON f.metric_name = 'Custom:' || r.id
ORDER BY r.name";

    /// <summary>The full single-rule read (includes <c>definition</c>). $1 id.</summary>
    public const string GetSql = @"
SELECT id, name, definition, description, enabled, version, created_at, updated_at, updated_by
FROM custom_alert_rules
WHERE id = $1";

    /// <summary>Every ENABLED rule in full — the evaluator's per-sweep read. Ordered by id so evaluation order
    /// is stable across sweeps, and bounded by <c>LIMIT $1</c> (the evaluator's hard ceiling) so a table that
    /// somehow holds more enabled rows than the cap can never make one sweep evaluate an unbounded set. $1 is
    /// bound to <see cref="EnabledRuleCap"/> + 1 — one beyond the cap — so the caller can DETECT (and warn about)
    /// an over-cap table while still reading a bounded number of rows.</summary>
    public const string ListEnabledSql = @"
SELECT id, name, definition, description, enabled, version, created_at, updated_at, updated_by
FROM custom_alert_rules
WHERE enabled
ORDER BY id
LIMIT $1";

    /// <summary>Resolves each given fleet-tag id to the set of server ids DIRECTLY assigned to it (#3350 tag
    /// scope) — one round-trip for every tag via <c>= ANY($1)</c>. Reads <c>config.server_tag_map</c> (bare
    /// name; the pool's search_path resolves it, the same as <c>custom_alert_rules</c> above). $1 the tag ids.
    ///
    /// <para>A tag with no rows — never assigned, or deleted so the map rows cascaded away — is simply absent
    /// from the result, which the caller treats as "resolves to no server", so a tag-scoped rule on it never
    /// fires (the #3350 integrity requirement; the 0-server case then flows to the #3304 never-firing surface).
    /// Membership is DIRECT only: descendant tags' servers are NOT pulled in (a v1 decision — the tree is not
    /// traversed here; that is a possible later slice).</para></summary>
    public const string ListTagMembersSql = @"
SELECT tag_id, server_id
FROM server_tag_map
WHERE tag_id = ANY($1)";

    /// <summary>Inserts a new rule at version 1. $1 name, $2 definition (jsonb), $3 description, $4 enabled, $5 updated_by.</summary>
    public const string InsertSql = @"
INSERT INTO custom_alert_rules (name, definition, description, enabled, version, created_at, updated_at, updated_by)
VALUES ($1, $2, $3, $4, 1, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'), $5)
RETURNING id, name, definition, description, enabled, version, created_at, updated_at, updated_by";

    /// <summary>Optimistic-concurrency update. $1 id, $2 name, $3 definition (jsonb), $4 description,
    /// $5 enabled, $6 updated_by, $7 expected_version. Returns the row only when the expected version matched.</summary>
    public const string UpdateSql = @"
UPDATE custom_alert_rules
SET name = $2,
    definition = $3,
    description = $4,
    enabled = $5,
    version = version + 1,
    updated_at = (now() AT TIME ZONE 'UTC'),
    updated_by = $6
WHERE id = $1 AND version = $7
RETURNING id, name, definition, description, enabled, version, created_at, updated_at, updated_by";

    /// <summary>Disambiguates a 0-rowcount update: the current version, or no row when the id is gone. $1 id.</summary>
    public const string VersionProbeSql = "SELECT version FROM custom_alert_rules WHERE id = $1";

    /// <summary>Deletes a rule by id (its <c>custom_alert_state</c> rows cascade). $1 id.</summary>
    public const string DeleteSql = "DELETE FROM custom_alert_rules WHERE id = $1";

    /// <summary>Postgres <c>unique_violation</c> — a duplicate <c>name</c> insert/rename.</summary>
    private const string UniqueViolation = "23505";

    /// <summary>An abuse bound on the rule name (the column itself is unbounded text).</summary>
    public const int MaxNameLength = 200;

    /// <summary>
    /// The hard cap on ENABLED alert rules across the whole store (#3285, Round-2 "rule-count cap"). Only
    /// ENABLED rules cost anything: each sweep, the evaluator runs one compose scalar query per (enabled rule,
    /// in-scope server) pair — so the per-sweep store load scales as enabled_rules × in_scope_servers × the
    /// sweep frequency, and nothing else bounds it. Disabled rules are never loaded or evaluated, so they are
    /// deliberately NOT counted here (storing many paused rules is free). 100 sits far above any realistic
    /// hand-authored rule set (operators run a handful to low dozens) while bounding the runaway/DoS worst case:
    /// 100 enabled rules against a large fleet is already thousands of compose queries per sweep. Deliberately a
    /// <c>const</c>, not a config knob (defaults over speculative config) — if a real deployment ever needs more,
    /// that is a design change with a load conversation, not a setting to quietly raise.
    /// </summary>
    public const int EnabledRuleCap = 100;

    /// <summary>The caller-facing refusal when a create would push the enabled count over <see cref="EnabledRuleCap"/>.</summary>
    internal static readonly string EnabledCapCreateMessage =
        $"Enabled custom alert rule limit ({EnabledRuleCap}) reached. Disable or delete an existing rule, or create this rule disabled.";

    /// <summary>The caller-facing refusal when enabling an existing rule would push the enabled count over <see cref="EnabledRuleCap"/>.</summary>
    internal static readonly string EnabledCapEnableMessage =
        $"Enabled custom alert rule limit ({EnabledRuleCap}) reached. Disable or delete another rule before enabling this one.";

    /// <summary>
    /// A fixed advisory-lock key that serializes the enabled-rule cap's count-then-write critical section via
    /// <c>pg_advisory_xact_lock</c>, so two concurrent enabled creates/enables cannot both read a stale
    /// under-cap count and race past the ceiling (READ COMMITTED alone does not prevent that phantom — the count
    /// subquery sees the same pre-insert total in both transactions). Distinct from
    /// <c>PgMigrations.MigrationLockKey</c> (0x4441524C494E47 "DARLING") so the two never collide in Postgres's
    /// single 64-bit advisory-lock key space.
    /// </summary>
    private const long EnabledCapAdvisoryLockKey = 0x4441524C_43415021; // "DARLCAP!"

    /// <summary>Takes the transaction-scoped cap lock (released automatically on commit/rollback). $1 lock key.</summary>
    private const string AcquireEnabledCapLockSql = "SELECT pg_advisory_xact_lock($1)";

    /// <summary>Inserts a new ENABLED rule at version 1 ONLY while the enabled count is under the cap; returns
    /// zero rows (nothing inserted) when the cap is already reached. $1 name, $2 definition (jsonb),
    /// $3 description, $4 updated_by, $5 cap. Run inside the advisory-locked transaction so the count and the
    /// insert are one atomic step.</summary>
    public const string InsertEnabledIfUnderCapSql = @"
INSERT INTO custom_alert_rules (name, definition, description, enabled, version, created_at, updated_at, updated_by)
SELECT $1, $2, $3, true, 1, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'), $4
WHERE (SELECT count(*) FROM custom_alert_rules WHERE enabled) < $5
RETURNING id, name, definition, description, enabled, version, created_at, updated_at, updated_by";

    /// <summary>Enables (or re-saves an already-enabled) rule under optimistic concurrency, refusing a
    /// disabled-&gt;enabled transition that would exceed the cap. $1 id, $2 name, $3 definition (jsonb),
    /// $4 description, $5 updated_by, $6 expected_version, $7 cap. Zero rows means id gone OR stale version OR
    /// (the row was disabled AND the cap is reached) — disambiguated by <see cref="EnabledStateProbeSql"/>. The
    /// <c>enabled = true OR …</c> guard lets an already-enabled rule be re-saved without consuming a cap slot.
    /// Run inside the advisory-locked transaction.</summary>
    public const string UpdateEnableIfUnderCapSql = @"
UPDATE custom_alert_rules
SET name = $2,
    definition = $3,
    description = $4,
    enabled = true,
    version = version + 1,
    updated_at = (now() AT TIME ZONE 'UTC'),
    updated_by = $5
WHERE id = $1 AND version = $6
  AND (enabled = true OR (SELECT count(*) FROM custom_alert_rules WHERE enabled) < $7)
RETURNING id, name, definition, description, enabled, version, created_at, updated_at, updated_by";

    /// <summary>Disambiguates a 0-row enable: the current version and enabled state, or no row when the id is
    /// gone. $1 id.</summary>
    public const string EnabledStateProbeSql = "SELECT version, enabled FROM custom_alert_rules WHERE id = $1";

    /// <summary>Lists every rule as a lightweight summary (no <c>definition</c>), ordered by name.</summary>
    public async Task<IReadOnlyList<CustomAlertRuleSummary>> ListAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<CustomAlertRuleSummary>();
        await using var command = _dataSource.CreateCommand(ListSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new CustomAlertRuleSummary(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetBoolean(3),
                reader.GetInt32(4),
                reader.GetDateTime(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                // last_fired: naive-UTC like updated_at (timestamp column -> Kind=Unspecified), NULL when never fired.
                reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7)));
        }

        return results;
    }

    /// <summary>Reads every ENABLED rule in full — the evaluator's per-sweep read.</summary>
    public async Task<IReadOnlyList<CustomAlertRule>> ListEnabledAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<CustomAlertRule>();
        await using var command = _dataSource.CreateCommand(ListEnabledSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        // Read one beyond the cap: the evaluator's ceiling trims to the cap and warns if this extra row is present.
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = EnabledRuleCap + 1 });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadFullRule(reader));
        }

        return results;
    }

    /// <summary>
    /// Resolves each fleet-tag id to the set of server ids directly assigned to it (#3350 tag scope), in ONE
    /// round-trip. The single shared tag-membership read: the evaluator calls it on the owner pool each cache
    /// refresh, and the evaluate-now test tool calls it on the least-privilege mcp/viewer pool — both of which
    /// carry <c>SELECT</c> on <c>config.server_tag_map</c> (it holds no secret column). A tag with no assigned
    /// servers is absent from the returned map (the caller reads that as an empty set → the rule matches no
    /// server). Returns an empty map for an empty input without touching the store.
    /// </summary>
    public async Task<IReadOnlyDictionary<int, IReadOnlySet<int>>> ListTagMembersAsync(
        IReadOnlyCollection<int> tagIds, CancellationToken cancellationToken = default)
    {
        if (tagIds.Count == 0)
        {
            return new Dictionary<int, IReadOnlySet<int>>();
        }

        var byTag = new Dictionary<int, HashSet<int>>();
        await using var command = _dataSource.CreateCommand(ListTagMembersSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = System.Linq.Enumerable.ToArray(tagIds) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var tagId = reader.GetInt32(0);
            var serverId = reader.GetInt32(1);
            if (!byTag.TryGetValue(tagId, out var set))
            {
                set = new HashSet<int>();
                byTag[tagId] = set;
            }

            set.Add(serverId);
        }

        var result = new Dictionary<int, IReadOnlySet<int>>(byTag.Count);
        foreach (var pair in byTag)
        {
            result[pair.Key] = pair.Value;
        }

        return result;
    }

    /// <summary>Reads one rule in full (with <c>definition</c>), or <see cref="CustomAlertRuleResult.NotFound"/>.</summary>
    public async Task<CustomAlertRuleResult> GetAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(GetSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CustomAlertRuleResult.NotFound();
        }

        return new CustomAlertRuleResult.Ok(ReadFullRule(reader));
    }

    /// <summary>
    /// Inserts a new rule. Returns <see cref="CustomAlertRuleResult.Ok"/> with the stored row (version 1),
    /// <see cref="CustomAlertRuleResult.Conflict"/> on a duplicate name (23505), or
    /// <see cref="CustomAlertRuleResult.Invalid"/> on a failed argument guard OR when creating this rule ENABLED
    /// would push the enabled count over <see cref="EnabledRuleCap"/>. A DISABLED create is always allowed (a
    /// paused rule never evaluates, so it does not count against the cap). The caller is expected to have
    /// validated <paramref name="definitionJson"/>'s structure first.
    /// </summary>
    public async Task<CustomAlertRuleResult> CreateAsync(
        string name, string? description, string definitionJson, bool enabled, string? updatedBy,
        CancellationToken cancellationToken = default)
    {
        var invalid = ValidateArgs(name, definitionJson);
        if (invalid is not null)
        {
            return invalid;
        }

        // A disabled rule never evaluates, so it does not count against the cap and takes the plain insert.
        if (!enabled)
        {
            return await InsertDisabledAsync(name, description, definitionJson, updatedBy, cancellationToken);
        }

        // An ENABLED create must not push the enabled count over the cap. Serialize the count-then-insert with a
        // transaction-scoped advisory lock so two concurrent enabled creates cannot both pass a stale under-cap
        // count (the conditional insert alone, under READ COMMITTED, would let that phantom race through).
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await AcquireCapLockAsync(connection, transaction, cancellationToken);

        await using var command = new NpgsqlCommand(InsertEnabledIfUnderCapSql, connection, transaction)
        {
            CommandTimeout = McpCommandDeadlines.ReadSeconds,
        };
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $1
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $2
        AddNullableText(command, description);                                                                      // $3
        AddNullableText(command, updatedBy);                                                                        // $4
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = EnabledRuleCap });                          // $5

        try
        {
            CustomAlertRule? row = null;
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    row = ReadFullRule(reader);
                }
            }

            if (row is null)
            {
                // 0 rows: the cap guard failed (enabled count already at the cap). Nothing was inserted.
                await transaction.RollbackAsync(cancellationToken);
                return new CustomAlertRuleResult.Invalid(EnabledCapCreateMessage);
            }

            await transaction.CommitAsync(cancellationToken);
            return new CustomAlertRuleResult.Ok(row);
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CustomAlertRuleResult.Conflict($"An alert rule named '{name}' already exists.");
        }
    }

    /// <summary>The plain insert for a DISABLED rule (no cap check needed). Shares the create result contract.</summary>
    private async Task<CustomAlertRuleResult> InsertDisabledAsync(
        string name, string? description, string definitionJson, string? updatedBy, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(InsertSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $1
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $2
        AddNullableText(command, description);                                                                      // $3
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });                                  // $4 (disabled)
        AddNullableText(command, updatedBy);                                                                        // $5

        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return new CustomAlertRuleResult.Ok(ReadFullRule(reader));
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomAlertRuleResult.Conflict($"An alert rule named '{name}' already exists.");
        }
    }

    /// <summary>
    /// Updates a rule under optimistic concurrency. Returns <see cref="CustomAlertRuleResult.Ok"/> with the
    /// new row (version bumped) on success; <see cref="CustomAlertRuleResult.Conflict"/> on a stale
    /// <paramref name="expectedVersion"/> OR a duplicate name; <see cref="CustomAlertRuleResult.NotFound"/>
    /// when the id no longer exists; <see cref="CustomAlertRuleResult.Invalid"/> on a failed argument guard OR
    /// when a disabled-&gt;enabled transition would push the enabled count over <see cref="EnabledRuleCap"/>
    /// (else the create-time cap would be trivially bypassed by creating disabled then enabling). Disabling, or
    /// re-saving an already-enabled rule, never increases the enabled count and so is never capped.
    /// </summary>
    public async Task<CustomAlertRuleResult> UpdateAsync(
        long id, string name, string? description, string definitionJson, bool enabled, int expectedVersion,
        string? updatedBy, CancellationToken cancellationToken = default)
    {
        var invalid = ValidateArgs(name, definitionJson);
        if (invalid is not null)
        {
            return invalid;
        }

        // Disabling, or staying disabled, can never increase the enabled count, so it takes the plain versioned
        // update. Only a transition to enabled has to be checked against (and serialized around) the cap.
        if (!enabled)
        {
            return await UpdateDisabledAsync(id, name, description, definitionJson, expectedVersion, updatedBy, cancellationToken);
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await AcquireCapLockAsync(connection, transaction, cancellationToken);

        await using var command = new NpgsqlCommand(UpdateEnableIfUnderCapSql, connection, transaction)
        {
            CommandTimeout = McpCommandDeadlines.ReadSeconds,
        };
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });                                     // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $2
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $3
        AddNullableText(command, description);                                                                      // $4
        AddNullableText(command, updatedBy);                                                                        // $5
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = expectedVersion });                         // $6
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = EnabledRuleCap });                          // $7

        try
        {
            CustomAlertRule? row = null;
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    row = ReadFullRule(reader);
                }
            }

            if (row is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                return new CustomAlertRuleResult.Ok(row);
            }

            // 0 rows: id gone, stale version, OR (the row was disabled AND the cap is reached). Probe under the
            // same lock to tell the cap refusal apart from a NotFound / stale-version Conflict.
            var classified = await ClassifyMissedEnableAsync(connection, transaction, id, expectedVersion, cancellationToken);
            await transaction.RollbackAsync(cancellationToken);
            return classified;
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CustomAlertRuleResult.Conflict($"An alert rule named '{name}' already exists.");
        }
    }

    /// <summary>The plain versioned update for a rule being set (or left) DISABLED (no cap check needed).</summary>
    private async Task<CustomAlertRuleResult> UpdateDisabledAsync(
        long id, string name, string? description, string definitionJson, int expectedVersion, string? updatedBy,
        CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(UpdateSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });                                     // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $2
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $3
        AddNullableText(command, description);                                                                      // $4
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });                                  // $5 (disabled)
        AddNullableText(command, updatedBy);                                                                        // $6
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = expectedVersion });                         // $7

        try
        {
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    return new CustomAlertRuleResult.Ok(ReadFullRule(reader));
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomAlertRuleResult.Conflict($"An alert rule named '{name}' already exists.");
        }

        /* 0 rows updated: the id is gone (NotFound) or the version moved under us (stale Conflict). */
        return await ClassifyMissedUpdateAsync(id, cancellationToken);
    }

    /// <summary>Takes the transaction-scoped advisory lock that serializes the enabled-rule cap's
    /// count-then-write critical section. Released automatically when the transaction commits or rolls back.</summary>
    private static async Task AcquireCapLockAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(AcquireEnabledCapLockSql, connection, transaction)
        {
            CommandTimeout = McpCommandDeadlines.ReadSeconds,
        };
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = EnabledCapAdvisoryLockKey });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Disambiguates a 0-row enable (see <see cref="UpdateEnableIfUnderCapSql"/>): NotFound when the id
    /// is gone, Conflict on a stale version, else Invalid (the cap) — because a matching version on a row that
    /// was NOT already enabled means the cap guard is the only clause that could have failed. Runs on the same
    /// connection/transaction (still holding the cap lock) so it sees a consistent snapshot.</summary>
    private static async Task<CustomAlertRuleResult> ClassifyMissedEnableAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, long id, int expectedVersion,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(EnabledStateProbeSql, connection, transaction)
        {
            CommandTimeout = McpCommandDeadlines.ReadSeconds,
        };
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CustomAlertRuleResult.NotFound();
        }

        var currentVersion = reader.GetInt32(0);
        var currentEnabled = reader.GetBoolean(1);
        if (currentVersion != expectedVersion)
        {
            return new CustomAlertRuleResult.Conflict(
                $"This rule was changed by someone else (current version {currentVersion}). Reload it and re-apply your edit.");
        }

        // Version matched and the row was NOT already enabled, so the ONLY clause that could have blocked the
        // guarded update is the cap: enabling this rule would exceed the limit.
        if (!currentEnabled)
        {
            return new CustomAlertRuleResult.Invalid(EnabledCapEnableMessage);
        }

        // Version matched AND already enabled: the guard's `enabled = true` branch was satisfied, so the update
        // should have fired. Reaching here means the row changed between the update and this probe — report a
        // concurrency conflict rather than inventing a cap error.
        return new CustomAlertRuleResult.Conflict(
            $"This rule was changed by someone else (current version {currentVersion}). Reload it and re-apply your edit.");
    }

    /// <summary>Deletes a rule (its <c>custom_alert_state</c> rows cascade). Returns
    /// <see cref="CustomAlertRuleResult.Ok"/> (no body) when a row was deleted, else
    /// <see cref="CustomAlertRuleResult.NotFound"/>. The caller force-resolves any open incident BEFORE
    /// calling this — the cascade drops the state rows, so the resolve delivery must happen first.</summary>
    public async Task<CustomAlertRuleResult> DeleteAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DeleteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        return affected > 0 ? new CustomAlertRuleResult.Ok(null) : new CustomAlertRuleResult.NotFound();
    }

    private async Task<CustomAlertRuleResult> ClassifyMissedUpdateAsync(long id, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(VersionProbeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var current = await command.ExecuteScalarAsync(cancellationToken);
        if (current is null or DBNull)
        {
            return new CustomAlertRuleResult.NotFound();
        }

        var currentVersion = Convert.ToInt32(current, CultureInfo.InvariantCulture);
        return new CustomAlertRuleResult.Conflict(
            $"This rule was changed by someone else (current version {currentVersion}). Reload it and re-apply your edit.");
    }

    /// <summary>
    /// PURE argument guard the store applies before any write (the route/tool's validator owns the
    /// definition STRUCTURE; this only guards presence/length so the store is safe to call on its own).
    /// Returns <see cref="CustomAlertRuleResult.Invalid"/> on a bad argument, else null.
    /// </summary>
    internal static CustomAlertRuleResult? ValidateArgs(string? name, string? definitionJson)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return new CustomAlertRuleResult.Invalid("'name' is required.");
        }

        if (name.Length > MaxNameLength)
        {
            return new CustomAlertRuleResult.Invalid($"'name' exceeds the maximum length of {MaxNameLength} characters.");
        }

        if (string.IsNullOrWhiteSpace(definitionJson))
        {
            return new CustomAlertRuleResult.Invalid("'definition' is required.");
        }

        return null;
    }

    /// <summary>Reads a full-rule row (column order: id, name, definition, description, enabled, version,
    /// created_at, updated_at, updated_by — shared by the SELECT/RETURNING lists). <c>definition</c> is jsonb,
    /// read as its text representation.</summary>
    private static CustomAlertRule ReadFullRule(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.GetFieldValue<string>(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.GetBoolean(4),
        reader.GetInt32(5),
        reader.GetDateTime(6),
        reader.GetDateTime(7),
        reader.IsDBNull(8) ? null : reader.GetString(8));

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });
}

/// <summary>A saved custom alert rule in full — the <c>definition</c> is the raw rule JSON text (jsonb).</summary>
public sealed record CustomAlertRule(
    long Id,
    string Name,
    string DefinitionJson,
    string? Description,
    bool Enabled,
    int Version,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    string? UpdatedBy);

/// <summary>The lightweight list projection — every field except the <c>definition</c> body. <see cref="LastFired"/>
/// (#3360) is the UTC instant this rule most recently FIRED, or null when it never has — see
/// <see cref="CustomAlertRuleStore.ListSql"/> for how it is correlated on the immutable <c>Custom:&lt;id&gt;</c>
/// fire key (resolve rows excluded).</summary>
public sealed record CustomAlertRuleSummary(
    long Id,
    string Name,
    string? Description,
    bool Enabled,
    int Version,
    DateTime UpdatedAt,
    string? UpdatedBy,
    DateTime? LastFired);

/// <summary>
/// The discriminated outcome of a store operation. Closed (the private base constructor blocks external
/// subclassing): <see cref="Ok"/> (success — carries the row for read/create/update, null body for delete),
/// <see cref="NotFound"/>, <see cref="Conflict"/> (duplicate name OR stale optimistic-concurrency version),
/// <see cref="Invalid"/> (a failed argument guard). The route layer maps these to 200/201/204, 404, 409, 400.
/// </summary>
public abstract record CustomAlertRuleResult
{
    private CustomAlertRuleResult()
    {
    }

    public sealed record Ok(CustomAlertRule? Rule) : CustomAlertRuleResult;

    public sealed record NotFound : CustomAlertRuleResult;

    public sealed record Conflict(string Message) : CustomAlertRuleResult;

    public sealed record Invalid(string Message) : CustomAlertRuleResult;
}
