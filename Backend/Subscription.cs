using System.Data;
using Microsoft.Data.SqlClient;

public sealed record SubscriptionRequestDto(string Email);
public sealed record UnsubscriptionRequestDto(string Email);
public sealed record SubscriptionSettingsRequestDto(string Token);
public sealed record ConfirmSubscriptionDto(string Token, List<SubscriptionCityDto> Locations);

public interface ISubscriptionRequestService
{
    Task<SubscriptionRequestCreationResult> CreateActiveSubscriptionRequestAsync(string email);
    Task<SubscriptionConfirmationMailSendResult> SendSubscriptionConfirmationMailAsync(string email);
    Task<HasActiveSubscriptionRequestResult> HasActiveSubscriptionRequestAsync(string email);
    Task<HasActiveSubscriptionTokenResult> HasActiveSubscriptionTokenAsync(Guid token);
    Task<GetSubscriptionRequestEmailByTokenResult> GetSubscriptionRequestEmailByTokenAsync(Guid token);
    Task<SubscriptionConfirmationResult> ConfirmSubscriptionAsync(Guid token, List<SubscriptionCityDto> locations);
    Task<SubscriptionRequestCreationResult> CreateUnsubscriptionRequestAsync(string email);
    Task<SubscriptionConfirmationMailSendResult> SendUnsubscriptionConfirmationMailAsync(string email);
    Task<SubscriptionConfirmationResult> ConfirmUnsubscriptionAsync(Guid token);
}

public abstract record SubscriptionRequestCreationResult
{
    private SubscriptionRequestCreationResult() { }
    public sealed record Created : SubscriptionRequestCreationResult;
    public sealed record Failed(string Reason) : SubscriptionRequestCreationResult;
}

public abstract record SubscriptionConfirmationMailSendResult
{
    private SubscriptionConfirmationMailSendResult() { }
    public sealed record Success : SubscriptionConfirmationMailSendResult;
    public sealed record Failed(string Reason) : SubscriptionConfirmationMailSendResult;
}

public abstract record HasActiveSubscriptionRequestResult
{
    private HasActiveSubscriptionRequestResult() { }
    public sealed record Exists : HasActiveSubscriptionRequestResult;
    public sealed record NotExists : HasActiveSubscriptionRequestResult;
    public sealed record Failed(string Reason) : HasActiveSubscriptionRequestResult;
}

public abstract record HasActiveSubscriptionTokenResult
{
    private HasActiveSubscriptionTokenResult() { }
    public sealed record Valid : HasActiveSubscriptionTokenResult;
    public sealed record Invalid : HasActiveSubscriptionTokenResult;
    public sealed record Failed(string Reason) : HasActiveSubscriptionTokenResult;
}

public abstract record SubscriptionConfirmationResult
{
    private SubscriptionConfirmationResult() { }
    public sealed record Confirmed(IReadOnlyList<int>? NewCityIds = null) : SubscriptionConfirmationResult;
    public sealed record InvalidToken : SubscriptionConfirmationResult;
    public sealed record Failed(string Reason) : SubscriptionConfirmationResult;
}

public abstract record GetSubscriptionRequestEmailByTokenResult
{
    private GetSubscriptionRequestEmailByTokenResult() { }
    public sealed record Found(string Email) : GetSubscriptionRequestEmailByTokenResult;
    public sealed record NotFound : GetSubscriptionRequestEmailByTokenResult;
    public sealed record Failed(string Reason) : GetSubscriptionRequestEmailByTokenResult;
}

public sealed class SubscriptionRequestService : ISubscriptionRequestService
{
    private readonly IConfiguration configuration;
    private readonly IEmailService emailService;

    public SubscriptionRequestService(IConfiguration configuration, IEmailService emailService)
    {
        this.configuration = configuration;
        this.emailService = emailService;
    }

    public async Task<SubscriptionRequestCreationResult> CreateActiveSubscriptionRequestAsync(string email)
    {
        try
        {
            var connectionString = configuration.GetConnectionString("Default");
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            await CreateNewSubscriptionRequestAsync(connection, email);

            return new SubscriptionRequestCreationResult.Created();
        }
        catch (Exception ex)
        {
            return new SubscriptionRequestCreationResult.Failed(ex.Message);
        }
    }

    public async Task<HasActiveSubscriptionRequestResult> HasActiveSubscriptionRequestAsync(string email)
    {
        try
        {
            var connectionString = configuration.GetConnectionString("Default");
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var command = new SqlCommand(@"
SELECT TOP 1 1
FROM dbo.SubscriptionRequests
WHERE Email = @Email
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
", connection);

            command.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;

            var exists = await command.ExecuteScalarAsync();
            return exists is not null
                ? new HasActiveSubscriptionRequestResult.Exists()
                : new HasActiveSubscriptionRequestResult.NotExists();
        }
        catch (Exception ex)
        {
            return new HasActiveSubscriptionRequestResult.Failed(ex.Message);
        }
    }

    public async Task<HasActiveSubscriptionTokenResult> HasActiveSubscriptionTokenAsync(Guid token)
    {
        try
        {
            var connectionString = configuration.GetConnectionString("Default");
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var command = new SqlCommand(@"
SELECT TOP 1 1
FROM dbo.SubscriptionRequests
WHERE Token = @Token
  AND RequestType = 1
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
", connection);

            command.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;

            var exists = await command.ExecuteScalarAsync();
            return exists is not null
                ? new HasActiveSubscriptionTokenResult.Valid()
                : new HasActiveSubscriptionTokenResult.Invalid();
        }
        catch (Exception ex)
        {
            return new HasActiveSubscriptionTokenResult.Failed(ex.Message);
        }
    }

    public async Task<SubscriptionConfirmationResult> ConfirmSubscriptionAsync(
    Guid token,
    List<SubscriptionCityDto> locations)
{
    try
    {
        var connectionString = configuration.GetConnectionString("Default");
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync();

        try
        {
            // 1️⃣ Pobierz email po tokenie
            var getEmailByTokenCommand = new SqlCommand(@"
SELECT TOP 1 Email
FROM dbo.SubscriptionRequests
WHERE Token = @Token
  AND RequestType = 1
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
", connection, transaction);

            getEmailByTokenCommand.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;
            var email = await getEmailByTokenCommand.ExecuteScalarAsync() as string;

            if (email is null)
            {
                await transaction.RollbackAsync();
                return new SubscriptionConfirmationResult.InvalidToken();
            }

            // 2️⃣ Pobierz lub utwórz użytkownika
            var getUserIdCommand = new SqlCommand(@"
SELECT TOP 1 ID
FROM dbo.Users
WHERE Email = @Email
", connection, transaction);

            getUserIdCommand.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;
            var existingUserId = await getUserIdCommand.ExecuteScalarAsync();

            int userId;

            if (existingUserId is null)
            {
                var createUserCommand = new SqlCommand(@"
INSERT INTO dbo.Users (Email, IsSubscribed)
VALUES (@Email, 1);
SELECT CAST(SCOPE_IDENTITY() AS int);
", connection, transaction);

                createUserCommand.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;
                userId = (int)(await createUserCommand.ExecuteScalarAsync())!;
            }
            else
            {
                userId = Convert.ToInt32(existingUserId);

                var subscribeUserCommand = new SqlCommand(@"
UPDATE dbo.Users
SET IsSubscribed = 1
WHERE ID = @UserID
", connection, transaction);

                subscribeUserCommand.Parameters.Add("@UserID", SqlDbType.Int).Value = userId;
                await subscribeUserCommand.ExecuteNonQueryAsync();
            }

            // 3️⃣ Wyczyść stare miasta
            var clearUserCitiesCommand = new SqlCommand(@"
DELETE FROM dbo.UserCities
WHERE UserID = @UserID
", connection, transaction);

            clearUserCitiesCommand.Parameters.Add("@UserID", SqlDbType.Int).Value = userId;
            await clearUserCitiesCommand.ExecuteNonQueryAsync();

            var uniqueLocations = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var safeLocations = locations ?? new List<SubscriptionCityDto>();

            var newCityIds = new List<int>();
            foreach (var location in safeLocations)
            {
                if (string.IsNullOrWhiteSpace(location.City) ||
                    string.IsNullOrWhiteSpace(location.Country) ||
                    string.IsNullOrWhiteSpace(location.CountryCode))
                {
                    await transaction.RollbackAsync();
                    return new SubscriptionConfirmationResult.Failed("Invalid location data.");
                }

                var city = location.City.Trim();
                var country = location.Country.Trim();
                var countryCode = location.CountryCode.Trim().ToUpperInvariant();

                if (countryCode.Length != 2)
                {
                    await transaction.RollbackAsync();
                    return new SubscriptionConfirmationResult.Failed("Invalid country code.");
                }

                var locationKey = $"{city}|{countryCode}";
                if (!uniqueLocations.Add(locationKey))
                    continue;

                // 4️⃣ Pobierz lub utwórz kraj
                var getCountryIdCommand = new SqlCommand(@"
SELECT TOP 1 ID
FROM dbo.Countries
WHERE Iso2 = @Iso2
", connection, transaction);

                getCountryIdCommand.Parameters.Add("@Iso2", SqlDbType.Char, 2).Value = countryCode;
                var existingCountryId = await getCountryIdCommand.ExecuteScalarAsync();

                int countryId;

                if (existingCountryId is null)
                {
                    var createCountryCommand = new SqlCommand(@"
INSERT INTO dbo.Countries (Iso2, Name)
VALUES (@Iso2, @Name);
SELECT CAST(SCOPE_IDENTITY() AS int);
", connection, transaction);

                    createCountryCommand.Parameters.Add("@Iso2", SqlDbType.Char, 2).Value = countryCode;
                    createCountryCommand.Parameters.Add("@Name", SqlDbType.NVarChar, 120).Value = country;

                    countryId = (int)(await createCountryCommand.ExecuteScalarAsync())!;
                }
                else
                {
                    countryId = Convert.ToInt32(existingCountryId);
                }

                // 5️⃣ Pobierz lub utwórz miasto
                var getCityIdCommand = new SqlCommand(@"
SELECT TOP 1 ID
FROM dbo.Cities
WHERE Name = @City
  AND CountryID = @CountryID
", connection, transaction);

                getCityIdCommand.Parameters.Add("@City", SqlDbType.NVarChar, 200).Value = city;
                getCityIdCommand.Parameters.Add("@CountryID", SqlDbType.Int).Value = countryId;

                var existingCityId = await getCityIdCommand.ExecuteScalarAsync();

                int cityId;

                if (existingCityId is null)
                {
                    var createCityCommand = new SqlCommand(@"
INSERT INTO dbo.Cities (Name, CountryID)
VALUES (@City, @CountryID);
SELECT CAST(SCOPE_IDENTITY() AS int);
", connection, transaction);

                    createCityCommand.Parameters.Add("@City", SqlDbType.NVarChar, 200).Value = city;
                    createCityCommand.Parameters.Add("@CountryID", SqlDbType.Int).Value = countryId;

                    cityId = (int)(await createCityCommand.ExecuteScalarAsync())!;
                    newCityIds.Add(cityId);
                }
                else
                {
                    cityId = Convert.ToInt32(existingCityId);
                }

                // 6️⃣ Dodaj relację user-city
                var addUserCityCommand = new SqlCommand(@"
IF NOT EXISTS (
    SELECT 1
    FROM dbo.UserCities
    WHERE UserID = @UserID
      AND CityID = @CityID
)
INSERT INTO dbo.UserCities (UserID, CityID)
VALUES (@UserID, @CityID);
", connection, transaction);

                addUserCityCommand.Parameters.Add("@UserID", SqlDbType.Int).Value = userId;
                addUserCityCommand.Parameters.Add("@CityID", SqlDbType.Int).Value = cityId;

                await addUserCityCommand.ExecuteNonQueryAsync();
            }

            // 7️⃣ Oznacz request jako confirmed
            var confirmRequestCommand = new SqlCommand(@"
UPDATE dbo.SubscriptionRequests
SET ConfirmedAt = SYSUTCDATETIME()
WHERE Token = @Token
  AND ConfirmedAt IS NULL
", connection, transaction);

            confirmRequestCommand.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;
            await confirmRequestCommand.ExecuteNonQueryAsync();

            await transaction.CommitAsync();
            return new SubscriptionConfirmationResult.Confirmed(newCityIds);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            return new SubscriptionConfirmationResult.Failed(ex.Message);
        }
    }
    catch (Exception ex)
    {
        return new SubscriptionConfirmationResult.Failed(ex.Message);
    }
}

    public async Task<GetSubscriptionRequestEmailByTokenResult> GetSubscriptionRequestEmailByTokenAsync(Guid token)
    {
        try
        {
            var connectionString = configuration.GetConnectionString("Default");
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var command = new SqlCommand(@"
SELECT TOP 1 Email
FROM dbo.SubscriptionRequests
WHERE Token = @Token
", connection);

            command.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;

            var email = await command.ExecuteScalarAsync() as string;
            return email is not null
                ? new GetSubscriptionRequestEmailByTokenResult.Found(email)
                : new GetSubscriptionRequestEmailByTokenResult.NotFound();
        }
        catch (Exception ex)
        {
            return new GetSubscriptionRequestEmailByTokenResult.Failed(ex.Message);
        }
    }

    private async Task CreateNewSubscriptionRequestAsync(
        SqlConnection con,
        string email)
    {
        var token = Guid.NewGuid();

        var insertCmd = new SqlCommand(@"
INSERT INTO dbo.SubscriptionRequests
(
    Email,
    EmailHash,
    RequestType,
    Token,
    TokenExpiresAt,
    RequestedAt,
    EmailSendAttemptCount
)
VALUES
(
    @Email,
    HASHBYTES('SHA2_256', LOWER(@Email)),
    1,
    @Token,
    DATEADD(hour, 24, SYSUTCDATETIME()),
    SYSUTCDATETIME(),
    0
)
", con);

        insertCmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;
        insertCmd.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;

        await insertCmd.ExecuteNonQueryAsync();
    }

    public async Task<SubscriptionConfirmationMailSendResult>
        SendSubscriptionConfirmationMailAsync(string email)
    {
        try
        {
            var token = await GetLatestActiveTokenAsync(email);
            if (token is null)
            {
                return new SubscriptionConfirmationMailSendResult
                    .Failed("No active subscription request");
            }

            var confirmUrl =
                $"https://subscriptionwebsite.z36.web.core.windows.net/opcjeSubskrypcji?token={token}";

            var mailResult = await emailService.SendAsync(
                email,
                "Potwierdzenie subskrypcji",
                $"Kliknij aby potwierdzić subskrypcję:\n{confirmUrl}");

            if (mailResult is EmailSendResult.Failed failed)
            {
                return new SubscriptionConfirmationMailSendResult
                    .Failed(failed.Reason);
            }

            await MarkEmailAttemptAsync(token.Value);

            return new SubscriptionConfirmationMailSendResult.Success();
        }
        catch (Exception ex)
        {
            return new SubscriptionConfirmationMailSendResult.Failed(ex.Message);
        }
    }

    private async Task<Guid?> GetLatestActiveTokenAsync(string email)
    {
        var cs = configuration.GetConnectionString("Default");
        await using var con = new SqlConnection(cs);
        await con.OpenAsync();

        var cmd = new SqlCommand(@"
SELECT TOP 1 Token
FROM dbo.SubscriptionRequests
WHERE Email = @Email
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
ORDER BY RequestedAt DESC
", con);

        cmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;

        var result = await cmd.ExecuteScalarAsync();
        return result as Guid?;
    }

    private async Task MarkEmailAttemptAsync(Guid token)
    {
        var cs = configuration.GetConnectionString("Default");
        await using var con = new SqlConnection(cs);
        await con.OpenAsync();

        var cmd = new SqlCommand(@"
UPDATE dbo.SubscriptionRequests
SET
    EmailSentAt = SYSUTCDATETIME(),
    EmailSendAttemptCount = EmailSendAttemptCount + 1,
    LastEmailSendAttemptAt = SYSUTCDATETIME()
WHERE Token = @Token
", con);

        cmd.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<SubscriptionRequestCreationResult>
    CreateUnsubscriptionRequestAsync(string email)
{
    try
    {
        var connectionString = configuration.GetConnectionString("Default");
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var token = Guid.NewGuid();

        var insertCmd = new SqlCommand(@"
INSERT INTO dbo.SubscriptionRequests
(
    Email,
    EmailHash,
    RequestType,
    Token,
    TokenExpiresAt,
    RequestedAt,
    EmailSendAttemptCount
)
VALUES
(
    @Email,
    HASHBYTES('SHA2_256', LOWER(@Email)),
    2,
    @Token,
    DATEADD(hour, 24, SYSUTCDATETIME()),
    SYSUTCDATETIME(),
    0
)
", connection);

        insertCmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;
        insertCmd.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;

        await insertCmd.ExecuteNonQueryAsync();

        return new SubscriptionRequestCreationResult.Created();
    }
    catch (Exception ex)
    {
        return new SubscriptionRequestCreationResult.Failed(ex.Message);
    }
}

public async Task<SubscriptionConfirmationMailSendResult>
    SendUnsubscriptionConfirmationMailAsync(string email)
{
    try
    {
        var cs = configuration.GetConnectionString("Default");
        await using var con = new SqlConnection(cs);
        await con.OpenAsync();

        var cmd = new SqlCommand(@"
SELECT TOP 1 Token
FROM dbo.SubscriptionRequests
WHERE Email = @Email
  AND RequestType = 2
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
ORDER BY RequestedAt DESC
", con);

        cmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;

        var token = await cmd.ExecuteScalarAsync() as Guid?;

        if (token is null)
            return new SubscriptionConfirmationMailSendResult
                .Failed("No active unsubscription request");

        var confirmUrl =
            $"https://weatheralerts-eefkdachhtdeechp.westeurope-01.azurewebsites.net/api/confirmUnsubscription?token={token}";

        var mailResult = await emailService.SendAsync(
            email,
            "Potwierdzenie wypisania",
            $"Kliknij aby wypisać się z alertów:\n{confirmUrl}");

        return new SubscriptionConfirmationMailSendResult.Success();
    }
    catch (Exception ex)
    {
        return new SubscriptionConfirmationMailSendResult.Failed(ex.Message);
    }
}

public async Task<SubscriptionConfirmationResult>
    ConfirmUnsubscriptionAsync(Guid token)
{
    try
    {
        var cs = configuration.GetConnectionString("Default");
        await using var con = new SqlConnection(cs);
        await con.OpenAsync();

        await using var transaction =
            (SqlTransaction)await con.BeginTransactionAsync();

        var getEmailCmd = new SqlCommand(@"
SELECT TOP 1 Email
FROM dbo.SubscriptionRequests
WHERE Token = @Token
  AND RequestType = 2
  AND ConfirmedAt IS NULL
  AND TokenExpiresAt > SYSUTCDATETIME()
", con, transaction);

        getEmailCmd.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;

        var email = await getEmailCmd.ExecuteScalarAsync() as string;

        if (email is null)
        {
            await transaction.RollbackAsync();
            return new SubscriptionConfirmationResult.InvalidToken();
        }

        // 🔥 ZMIANA: tylko zmieniamy IsSubscribed
        var updateUserCmd = new SqlCommand(@"
UPDATE dbo.Users
SET IsSubscribed = 0
WHERE Email = @Email
", con, transaction);

        updateUserCmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;
        await updateUserCmd.ExecuteNonQueryAsync();

        // ❌ USUNIĘTO delete z UserCities

        var confirmCmd = new SqlCommand(@"
UPDATE dbo.SubscriptionRequests
SET ConfirmedAt = SYSUTCDATETIME()
WHERE Token = @Token
", con, transaction);

        confirmCmd.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = token;
        await confirmCmd.ExecuteNonQueryAsync();

        await transaction.CommitAsync();
        return new SubscriptionConfirmationResult.Confirmed();
    }
    catch (Exception ex)
    {
        return new SubscriptionConfirmationResult.Failed(ex.Message);
    }
}
}
