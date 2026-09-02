using System.Net.Sockets;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services;

public static class SqlTransient
{
    private static readonly HashSet<int> TransientSqlErrors =
    [
        -2,    // timeout
        1205,  // deadlock
        40197,
        40501,
        40540,
        40613,
        40627,
        10928,
        10929,
        10053,
        10054,
        10060
    ];

    public static bool IsTransient(Exception exception)
    {
        for (var ex = exception; ex is not null; ex = ex.InnerException)
        {
            if (ex is SqlException sql && sql.Errors.Cast<SqlError>().Any(e => TransientSqlErrors.Contains(e.Number)))
                return true;

            if (ex is IOException or SocketException)
                return true;
        }

        return false;
    }

    public static async Task DelayBeforeRetryAsync(int attempt, int baseDelayMs, CancellationToken cancellationToken)
    {
        if (baseDelayMs <= 0)
            return;

        var exponent = Math.Min(attempt, 10);
        var delayMs = Math.Min(baseDelayMs * (1 << exponent), 60_000);
        var jitterMs = Random.Shared.Next(0, Math.Max(1, delayMs / 4));
        await Task.Delay(delayMs + jitterMs, cancellationToken).ConfigureAwait(false);
    }
}
