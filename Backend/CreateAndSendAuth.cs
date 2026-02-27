using System.Security.Cryptography;
using System.Text;

public static class CreateAndSendAuth
{
    public static bool IsCreateAndSendTokenValid(string providedToken, string expectedToken)
    {
        static string Normalize(string value)
        {
            var normalized = value.Trim();
            if (normalized.Length >= 2)
            {
                if ((normalized.StartsWith('"') && normalized.EndsWith('"')) ||
                    (normalized.StartsWith('\'') && normalized.EndsWith('\'')))
                {
                    normalized = normalized[1..^1].Trim();
                }
            }

            return normalized;
        }

        var provided = Normalize(providedToken);
        var expected = Normalize(expectedToken);

        var providedHash = SHA256.HashData(Encoding.UTF8.GetBytes(provided));
        var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(expected));

        return CryptographicOperations.FixedTimeEquals(providedHash, expectedHash);
    }
}
