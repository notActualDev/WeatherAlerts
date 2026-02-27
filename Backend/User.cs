using System.Data;
using Microsoft.Data.SqlClient;

public interface IUserService
{
    Task<GetUserCitiesResult> GetUserCitiesAsync(TokenDto token);
}

public abstract record GetUserCitiesResult
{
    private GetUserCitiesResult() { }
    public sealed record Success(List<SubscriptionCityDto> Locations) : GetUserCitiesResult;
    public sealed record InvalidToken : GetUserCitiesResult;
    public sealed record Failed(string Reason) : GetUserCitiesResult;
}

public sealed class UserService : IUserService
{
    private readonly IConfiguration configuration;

    public UserService(IConfiguration configuration)
    {
        this.configuration = configuration;
    }

    public async Task<GetUserCitiesResult> GetUserCitiesAsync(TokenDto token)
    {
        try
        {
            if (token is null || string.IsNullOrWhiteSpace(token.Token) || !Guid.TryParse(token.Token, out var parsedToken))
            {
                return new GetUserCitiesResult.InvalidToken();
            }

            var connectionString = configuration.GetConnectionString("Default");
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var emailCommand = new SqlCommand(@"
SELECT TOP 1 Email
FROM dbo.SubscriptionRequests
WHERE Token = @Token
  AND RequestType = 1
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
", connection);

            emailCommand.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = parsedToken;
            var email = await emailCommand.ExecuteScalarAsync() as string;

            if (email is null)
            {
                return new GetUserCitiesResult.InvalidToken();
            }

            var locations = new List<SubscriptionCityDto>();

            var citiesCommand = new SqlCommand(@"
SELECT c.Name, co.Name, co.Iso2
FROM dbo.Users u
JOIN dbo.UserCities uc ON uc.UserID = u.ID
JOIN dbo.Cities c ON c.ID = uc.CityID
JOIN dbo.Countries co ON co.ID = c.CountryID
WHERE u.Email = @Email
", connection);

            citiesCommand.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;

            await using var reader = await citiesCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                locations.Add(new SubscriptionCityDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2)));
            }

            return new GetUserCitiesResult.Success(locations);
        }
        catch (Exception ex)
        {
            return new GetUserCitiesResult.Failed(ex.Message);
        }
    }
}
