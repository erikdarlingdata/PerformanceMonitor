namespace Darling.Tests;

internal static class Diag9943
{
    public static void W(string s)
    {
        System.Console.WriteLine(s);
        System.IO.File.AppendAllText("diag9943.txt", s + "\n");
    }
}
