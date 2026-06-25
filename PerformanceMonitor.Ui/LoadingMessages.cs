using System;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Randomized loading messages displayed while data is being fetched.
    /// </summary>
    public static class LoadingMessages
    {
        private static readonly string[] Messages =
        [
            "Reticulating splines...",
            "Consulting the oracle...",
            "Asking the database nicely...",
            "Crunching numbers...",
            "Thinking really hard...",
            "Mulling it over...",
            "Communing with SQL Server...",
            "Summoning data spirits...",
            "Decoding the matrix...",
            "Interrogating indexes...",
            "Parsing the void...",
            "Calibrating flux capacitors...",
            "Negotiating with stored procedures...",
            "Convincing queries to run faster...",
            "Herding cursors...",
            "Massaging execution plans...",
            "Whispering to wait stats...",
            "Befriending buffer pools...",
            "Coaxing data from disk...",
            "Pondering performance...",
            "Untangling spaghetti queries...",
            "Poking the query optimizer...",
            "Warming up the plan cache...",
            "Dusting off statistics...",
            "Reasoning with RESOURCE_SEMAPHORE...",
            "Apologizing to tempdb...",
            "Flattering the cardinality estimator...",
            "Bribing the lock manager...",
            "Decrypting wait stats hieroglyphics...",
            "Teaching cursors to fly...",
        ];

        public static string GetRandom() => Messages[Random.Shared.Next(Messages.Length)];
    }
}
