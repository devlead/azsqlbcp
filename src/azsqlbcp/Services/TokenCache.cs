using Azure.Core;
using Azure.Identity;

namespace AzSqlBcp.Services;

public sealed class TokenCache
{
    private static readonly string[] Scopes = ["https://database.windows.net/.default"];
    private static readonly TimeSpan ExpirySkew = TimeSpan.FromMinutes(5);

    private readonly DefaultAzureCredential _credential = new();
    private readonly SemaphoreSlim _lock = new(1, 1);
    private AccessToken? _cached;

    public async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken = default)
    {
        if (IsFresh(_cached))
            return _cached!.Value.Token;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsFresh(_cached))
                return _cached!.Value.Token;

            _cached = await _credential
                .GetTokenAsync(new TokenRequestContext(Scopes), cancellationToken)
                .ConfigureAwait(false);

            return _cached.Value.Token;
        }
        finally
        {
            _lock.Release();
        }
    }

    private static bool IsFresh(AccessToken? token) =>
        token is { } t && t.ExpiresOn > DateTimeOffset.UtcNow.Add(ExpirySkew);
}
