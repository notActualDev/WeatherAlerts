using Microsoft.Extensions.Caching.Memory;

public sealed class SoftBlockService
{
    private readonly IMemoryCache _cache;
    private static readonly TimeSpan BlockTime = TimeSpan.FromMinutes(10);

    public SoftBlockService(IMemoryCache cache)
    {
        _cache = cache;
    }

    public bool IsBlocked(string ip, string email)
    {
        return
            _cache.TryGetValue($"ip:{ip}", out _) ||
            _cache.TryGetValue($"email:{email}", out _) ||
            _cache.TryGetValue($"combo:{ip}:{email}", out _);
    }

    public void Block(string ip, string email)
    {
        _cache.Set($"ip:{ip}", true, BlockTime);
        _cache.Set($"email:{email}", true, BlockTime);
        _cache.Set($"combo:{ip}:{email}", true, BlockTime);
    }
}