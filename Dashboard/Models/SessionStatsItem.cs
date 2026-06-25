using System;

namespace PerformanceMonitorDashboard.Models
{
    public class SessionStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public int TotalSessions { get; set; }
        public int RunningSessions { get; set; }
        public int SleepingSessions { get; set; }
        public int BackgroundSessions { get; set; }
        public int DormantSessions { get; set; }
        public int IdleSessionsOver30Min { get; set; }
        public int SessionsWaitingForMemory { get; set; }
        public int DatabasesWithConnections { get; set; }
        public string? TopApplicationName { get; set; }
        public int? TopApplicationConnections { get; set; }
        public string? TopHostName { get; set; }
        public int? TopHostConnections { get; set; }
    }
}
