using System;

namespace PerformanceMonitorLite.Models;

public class MuteRule
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public bool Enabled { get; set; } = true;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? ExpiresAtUtc { get; set; }
    public string? Reason { get; set; }

    public string? ServerName { get; set; }
    public string? MetricName { get; set; }
    public string? DatabasePattern { get; set; }
    public string? QueryTextPattern { get; set; }
    public string? WaitTypePattern { get; set; }
    public string? JobNamePattern { get; set; }

    public bool IsExpired => ExpiresAtUtc.HasValue && DateTime.UtcNow >= ExpiresAtUtc.Value;

    public MuteRule Clone() => new()
    {
        Id = Id,
        Enabled = Enabled,
        CreatedAtUtc = CreatedAtUtc,
        ExpiresAtUtc = ExpiresAtUtc,
        Reason = Reason,
        ServerName = ServerName,
        MetricName = MetricName,
        DatabasePattern = DatabasePattern,
        QueryTextPattern = QueryTextPattern,
        WaitTypePattern = WaitTypePattern,
        JobNamePattern = JobNamePattern
    };

    public string ExpiresDisplay => ExpiresAtUtc.HasValue
        ? (IsExpired ? "Expired" : ExpiresAtUtc.Value.ToLocalTime().ToString("g"))
        : "Never";

    public string Summary
    {
        get
        {
            var parts = new System.Collections.Generic.List<string>();
            if (MetricName != null) parts.Add(MetricName);
            if (ServerName != null) parts.Add($"on {ServerName}");
            if (DatabasePattern != null) parts.Add($"db≈{DatabasePattern}");
            if (QueryTextPattern != null) parts.Add($"query≈{QueryTextPattern}");
            if (WaitTypePattern != null) parts.Add($"wait≈{WaitTypePattern}");
            if (JobNamePattern != null) parts.Add($"job≈{JobNamePattern}");
            return parts.Count > 0 ? string.Join(", ", parts) : "(matches all alerts)";
        }
    }

    public bool Matches(AlertMuteContext context)
    {
        if (!Enabled || IsExpired) return false;

        if (ServerName != null &&
            !string.Equals(ServerName, context.ServerName, StringComparison.OrdinalIgnoreCase))
            return false;

        if (MetricName != null &&
            !string.Equals(MetricName, context.MetricName, StringComparison.OrdinalIgnoreCase))
            return false;

        if (DatabasePattern != null &&
            (context.DatabaseName == null ||
             !context.DatabaseName.Contains(DatabasePattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (QueryTextPattern != null &&
            (context.QueryText == null ||
             !context.QueryText.Contains(QueryTextPattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (WaitTypePattern != null &&
            (context.WaitType == null ||
             !context.WaitType.Contains(WaitTypePattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (JobNamePattern != null &&
            (context.JobName == null ||
             !context.JobName.Contains(JobNamePattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        return true;
    }
}

public class AlertMuteContext
{
    public string ServerName { get; set; } = "";
    public string MetricName { get; set; } = "";
    public string? DatabaseName { get; set; }
    public string? QueryText { get; set; }
    public string? WaitType { get; set; }
    public string? JobName { get; set; }
}
