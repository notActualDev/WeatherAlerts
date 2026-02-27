using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Net.Sockets;

public interface IAlertsService
{
    Task<List<CityToCheck>> GetSubscribedCitiesAsync(CancellationToken ct = default);
    Task<List<CityToCheck>> GetCitiesByIdsAsync(IReadOnlyCollection<int> cityIds, CancellationToken ct = default);
    Task<List<Weather3DayData>> Get3DayForecastsAsync(CancellationToken ct = default);
    Task<List<AlertRow>> CreateAlertsFromForecastsAsync(List<Weather3DayData> forecasts);
    Task<int> InsertAlertsAsync(List<AlertRow> alerts, CancellationToken ct = default);
    Task<CreateAlertsSummary> CreateAndStoreAlertsAsync(CancellationToken ct = default);
    Task<CreateAlertsAndSendSummary> CreateStoreAndSendAlertsAsync(CancellationToken ct = default);
    Task<int> CreateAndStoreAlertsForCitiesAsync(IReadOnlyCollection<int> cityIds, CancellationToken ct = default);
    Task<SendExistingAlertsToUserResult> SendExistingAlertsToUserAsync(string email, CancellationToken ct = default);
}

public sealed class AlertsService : IAlertsService
{
    private static readonly TimeZoneInfo Warsaw = TimeZoneInfo.FindSystemTimeZoneById(
        OperatingSystem.IsWindows() ? "Central European Standard Time" : "Europe/Warsaw");

    private readonly IConfiguration configuration;
    private readonly HttpClient httpClient;
    private readonly IEmailService emailService;
    private readonly ILogger<AlertsService> logger;

    public AlertsService(
        IConfiguration configuration,
        HttpClient httpClient,
        IEmailService emailService,
        ILogger<AlertsService> logger)
    {
        this.configuration = configuration;
        this.httpClient = httpClient;
        this.emailService = emailService;
        this.logger = logger;
    }

    public async Task<List<CityToCheck>> GetSubscribedCitiesAsync(CancellationToken ct = default)
    {
        const string sql = @"
SELECT DISTINCT
    c.ID,
    c.Name,
    co.Name,
    co.Iso2
FROM dbo.Cities c
JOIN dbo.UserCities uc ON uc.CityID = c.ID
JOIN dbo.Users u ON u.ID = uc.UserID
JOIN dbo.Countries co ON co.ID = c.CountryID
WHERE u.IsSubscribed = 1;";

        var connectionString = GetRequiredDefaultConnectionString();
        var result = new List<CityToCheck>();

        await using var con = await OpenSqlConnectionWithRetryAsync(connectionString, ct);

        await using var cmd = new SqlCommand(sql, con);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            result.Add(new CityToCheck
            {
                CityId = reader.GetInt32(0),
                Name = reader.GetString(1),
                CountryName = reader.GetString(2),
                Iso2 = reader.GetString(3),
            });
        }

        return result;
    }

    public async Task<List<CityToCheck>> GetCitiesByIdsAsync(
        IReadOnlyCollection<int> cityIds,
        CancellationToken ct = default)
    {
        if (cityIds.Count == 0)
        {
            return new List<CityToCheck>();
        }

        var connectionString = GetRequiredDefaultConnectionString();
        await using var con = await OpenSqlConnectionWithRetryAsync(connectionString, ct);

        var ids = cityIds.Distinct().ToList();
        var paramNames = ids.Select((_, i) => $"@Id{i}").ToList();

        var sql = $@"
SELECT
    c.ID,
    c.Name,
    co.Name,
    co.Iso2
FROM dbo.Cities c
JOIN dbo.Countries co ON co.ID = c.CountryID
WHERE c.ID IN ({string.Join(", ", paramNames)});";

        var list = new List<CityToCheck>();
        await using var cmd = new SqlCommand(sql, con);

        for (var i = 0; i < ids.Count; i++)
        {
            cmd.Parameters.Add(paramNames[i], SqlDbType.Int).Value = ids[i];
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(new CityToCheck
            {
                CityId = reader.GetInt32(0),
                Name = reader.GetString(1),
                CountryName = reader.GetString(2),
                Iso2 = reader.GetString(3),
            });
        }

        return list;
    }

    public async Task<List<Weather3DayData>> Get3DayForecastsAsync(CancellationToken ct = default)
    {
        var cities = await GetSubscribedCitiesAsync(ct);
        var forecasts = new List<Weather3DayData>(cities.Count);

        foreach (var city in cities)
        {
            var forecast = await Fetch3DaysAsync(city, ct);
            forecasts.Add(forecast);
        }

        return forecasts;
    }

    public async Task<List<AlertRow>> CreateAlertsFromForecastsAsync(List<Weather3DayData> forecasts)
    {
        var nowUtc = DateTime.UtcNow;
        var todayLocal = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, Warsaw).Date;
        var nextDayStartLocal = DateTime.SpecifyKind(todayLocal.AddDays(1), DateTimeKind.Unspecified);
        var nextDayEndLocal = DateTime.SpecifyKind(todayLocal.AddDays(2), DateTimeKind.Unspecified);
        var nextDayStartUtc = TimeZoneInfo.ConvertTimeToUtc(nextDayStartLocal, Warsaw);
        var nextDayEndUtc = TimeZoneInfo.ConvertTimeToUtc(nextDayEndLocal, Warsaw);
        var alerts = new List<AlertRow>();

        foreach (var data in forecasts)
        {
            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.WindshieldScraping,
                i => IsTrue(data.Temperature, i, t => t <= 0) &&
                     IsTrue(data.Humidity, i, h => h >= 85),
                nowUtc,
                70,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.Snowfall,
                i => IsTrue(data.Snowfall, i, s => s >= 0.2),
                nowUtc,
                75,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.Rain,
                i => IsTrue(data.Rain, i, r => r >= 0.2),
                nowUtc,
                70,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.HeavyRain,
                i => IsTrue(data.Rain, i, r => r >= 4.0) ||
                     IsTrue(data.Precip, i, p => p >= 5.0),
                nowUtc,
                85,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.CommuteDifficulties,
                i => IsTrue(data.Visibility, i, v => v < 1000) ||
                     IsTrue(data.Snowfall, i, s => s >= 1.0) ||
                     IsTrue(data.Rain, i, r => r >= 4.0) ||
                     IsTrue(data.WindSpeed, i, w => w >= 60.0),
                nowUtc,
                80,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.PoorAirQuality,
                i => IsTrue(data.Visibility, i, v => v < 3000) &&
                     IsTrue(data.WindSpeed, i, w => w < 12.0) &&
                     !IsTrue(data.Precip, i, p => p > 0.1),
                nowUtc,
                55,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.SignificantTempChange,
                i => i >= 6 &&
                     data.Temperature[i].HasValue &&
                     data.Temperature[i - 6].HasValue &&
                     Math.Abs(data.Temperature[i]!.Value - data.Temperature[i - 6]!.Value) >= 8.0,
                nowUtc,
                75,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.UnfavourableBiometeo,
                i => IsTrue(data.Pressure, i, p => p < 1000) ||
                     IsTrue(data.WindSpeed, i, w => w >= 50) ||
                     (IsTrue(data.Humidity, i, h => h >= 90) && IsTrue(data.Temperature, i, t => t <= 0)),
                nowUtc,
                65,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.Fog,
                i => IsTrue(data.Visibility, i, v => v < 500),
                nowUtc,
                85,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.StrongWind,
                i => IsTrue(data.WindSpeed, i, w => w >= 50.0) ||
                     IsTrue(data.WindGust, i, g => g >= 70.0),
                nowUtc,
                80,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.FreezingRainOrBlackIce,
                i => IsTrue(data.Temperature, i, t => t <= 0.0) &&
                     IsTrue(data.Rain, i, r => r > 0.1),
                nowUtc,
                85,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.SnowOnRoad,
                i => IsTrue(data.SnowDepth, i, d => d >= 1.0) ||
                     IsTrue(data.Snowfall, i, s => s >= 0.5),
                nowUtc,
                75,
                nextDayStartUtc,
                nextDayEndUtc));

            alerts.AddRange(BuildAlertRanges(
                data,
                AlertType.AquaplaningRisk,
                i => IsTrue(data.Rain, i, r => r >= 2.0) ||
                     IsTrue(data.Precip, i, p => p >= 3.0),
                nowUtc,
                80,
                nextDayStartUtc,
                nextDayEndUtc));
        }

        return await Task.FromResult(alerts);
    }

    public async Task<int> InsertAlertsAsync(List<AlertRow> alerts, CancellationToken ct = default)
    {
        if (alerts.Count == 0)
        {
            return 0;
        }

        const string sql = @"
INSERT INTO dbo.Alerts
(
    CityID,
    AlertTypeID,
    ValidFrom,
    ValidTo,
    Confidence,
    CreatedAt
)
VALUES
(
    @CityID,
    @AlertTypeID,
    @ValidFrom,
    @ValidTo,
    @Confidence,
    @CreatedAt
);";

        var connectionString = GetRequiredDefaultConnectionString();
        await using var con = await OpenSqlConnectionWithRetryAsync(connectionString, ct);

        await using var tx = (SqlTransaction)await con.BeginTransactionAsync(ct);
        var inserted = 0;

        try
        {
            foreach (var alert in alerts)
            {
                await using var cmd = new SqlCommand(sql, con, tx);
                cmd.Parameters.Add("@CityID", SqlDbType.Int).Value = alert.CityId;
                cmd.Parameters.Add("@AlertTypeID", SqlDbType.Int).Value = alert.AlertTypeId;
                cmd.Parameters.Add("@ValidFrom", SqlDbType.DateTime2).Value = alert.ValidFromUtc;
                cmd.Parameters.Add("@ValidTo", SqlDbType.DateTime2).Value = alert.ValidToUtc;
                cmd.Parameters.Add("@Confidence", SqlDbType.TinyInt).Value = (object?)alert.Confidence ?? DBNull.Value;
                cmd.Parameters.Add("@CreatedAt", SqlDbType.DateTime2).Value = alert.CreatedAtUtc;
                inserted += await cmd.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
            return inserted;
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }

    public async Task<CreateAlertsSummary> CreateAndStoreAlertsAsync(CancellationToken ct = default)
    {
        var pipeline = await GenerateAndStoreAlertsAsync(ct);

        return new CreateAlertsSummary(
            pipeline.Cities.Count,
            pipeline.Forecasts.Count,
            pipeline.Alerts.Count,
            pipeline.InsertedCount);
    }

    public async Task<CreateAlertsAndSendSummary> CreateStoreAndSendAlertsAsync(CancellationToken ct = default)
    {
        var pipeline = await GenerateAndStoreAlertsAsync(ct);
        var send = await SendAlertsToSubscribedUsersAsync(pipeline.Alerts, pipeline.Cities, ct);

        return new CreateAlertsAndSendSummary(
            pipeline.Cities.Count,
            pipeline.Forecasts.Count,
            pipeline.Alerts.Count,
            pipeline.InsertedCount,
            send.UsersWithAlertsCount,
            send.EmailsSentCount,
            send.EmailsFailedCount);
    }

    public async Task<int> CreateAndStoreAlertsForCitiesAsync(
        IReadOnlyCollection<int> cityIds,
        CancellationToken ct = default)
    {
        var cities = await GetCitiesByIdsAsync(cityIds, ct);
        if (cities.Count == 0)
        {
            return 0;
        }

        var forecasts = new List<Weather3DayData>(cities.Count);
        foreach (var city in cities)
        {
            forecasts.Add(await Fetch3DaysAsync(city, ct));
        }

        var alerts = await CreateAlertsFromForecastsAsync(forecasts);
        return await InsertAlertsAsync(alerts, ct);
    }

    public async Task<SendExistingAlertsToUserResult> SendExistingAlertsToUserAsync(
        string email,
        CancellationToken ct = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(email))
            {
                return new SendExistingAlertsToUserResult.Failed("Email is required.");
            }

            var connectionString = GetRequiredDefaultConnectionString();
            await using var con = await OpenSqlConnectionWithRetryAsync(connectionString, ct);

            var checkUserCmd = new SqlCommand(@"
SELECT TOP 1 1
FROM dbo.Users
WHERE Email = @Email
  AND IsSubscribed = 1;
", con);
            checkUserCmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;

            var exists = await checkUserCmd.ExecuteScalarAsync(ct);
            if (exists is null)
            {
                return new SendExistingAlertsToUserResult.UserNotFound();
            }

            var alertsCmd = new SqlCommand(@"
WITH UserCitiesCte AS
(
    SELECT c.ID AS CityID, c.Name AS CityName, co.Name AS CountryName, co.Iso2 AS Iso2
    FROM dbo.Users u
    JOIN dbo.UserCities uc ON uc.UserID = u.ID
    JOIN dbo.Cities c ON c.ID = uc.CityID
    JOIN dbo.Countries co ON co.ID = c.CountryID
    WHERE u.Email = @Email
      AND u.IsSubscribed = 1
),
LatestAlertPerCity AS
(
    SELECT a.CityID, MAX(a.CreatedAt) AS LatestCreatedAt
    FROM dbo.Alerts a
    JOIN UserCitiesCte uc ON uc.CityID = a.CityID
    WHERE a.ValidTo > SYSUTCDATETIME()
    GROUP BY a.CityID
)
SELECT
    uc.CityID,
    uc.CityName,
    uc.CountryName,
    uc.Iso2,
    a.AlertTypeID,
    a.ValidFrom,
    a.ValidTo,
    a.Confidence,
    a.CreatedAt
FROM UserCitiesCte uc
JOIN LatestAlertPerCity la ON la.CityID = uc.CityID
JOIN dbo.Alerts a ON a.CityID = uc.CityID
    AND a.CreatedAt = la.LatestCreatedAt
  AND a.ValidTo > SYSUTCDATETIME()
ORDER BY uc.CityID, a.ValidFrom, a.AlertTypeID;
", con);

            alertsCmd.Parameters.Add("@Email", SqlDbType.NVarChar, 320).Value = email;

            var alerts = new List<AlertRow>();
            var cityById = new Dictionary<int, CityToCheck>();

            await using var reader = await alertsCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var cityId = reader.GetInt32(0);

                if (!cityById.ContainsKey(cityId))
                {
                    cityById[cityId] = new CityToCheck
                    {
                        CityId = cityId,
                        Name = reader.GetString(1),
                        CountryName = reader.GetString(2),
                        Iso2 = reader.GetString(3),
                    };
                }

                alerts.Add(new AlertRow
                {
                    CityId = cityId,
                    AlertTypeId = reader.GetInt32(4),
                    ValidFromUtc = reader.GetDateTime(5),
                    ValidToUtc = reader.GetDateTime(6),
                    Confidence = reader.IsDBNull(7) ? null : reader.GetByte(7),
                    CreatedAtUtc = reader.GetDateTime(8)
                });
            }

            if (alerts.Count == 0)
            {
                return new SendExistingAlertsToUserResult.NoAlerts();
            }

            var body = BuildAlertEmailBody(email, alerts, cityById);
            var subject = "WeatherAlerts: Aktualne alerty po aktywacji subskrypcji";
            var mailResult = await emailService.SendAsync(email, subject, body);

            if (mailResult is EmailSendResult.Failed failed)
            {
                return new SendExistingAlertsToUserResult.Failed(failed.Reason);
            }

            return new SendExistingAlertsToUserResult.Success(alerts.Count);
        }
        catch (Exception ex)
        {
            return new SendExistingAlertsToUserResult.Failed(ex.Message);
        }
    }

    private async Task<Weather3DayData> Fetch3DaysAsync(CityToCheck city, CancellationToken ct)
    {
        var geoUrl = $"https://geocoding-api.open-meteo.com/v1/search?name={Uri.EscapeDataString(city.Name)}&count=5&format=json";
        using var geoDoc = JsonDocument.Parse(await httpClient.GetStringAsync(geoUrl, ct));

        if (!geoDoc.RootElement.TryGetProperty("results", out var geoResults) || geoResults.GetArrayLength() == 0)
        {
            throw new InvalidOperationException($"Geocoding results not found for city '{city.Name}'.");
        }

        var geo = PickBestGeoResult(geoResults, city);
        var lat = geo.GetProperty("latitude").GetDouble();
        var lon = geo.GetProperty("longitude").GetDouble();

        var todayLocal = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, Warsaw).Date;
        var yesterday = todayLocal.AddDays(-1).ToString("yyyy-MM-dd");
        var tomorrow = todayLocal.AddDays(1).ToString("yyyy-MM-dd");

        const string hourly =
            "temperature_2m,dewpoint_2m,relative_humidity_2m,precipitation,rain,snowfall," +
            "snow_depth,weathercode,windspeed_10m,windgusts_10m,pressure_msl,visibility,soil_temperature_0cm";

        var weatherUrl =
            $"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}" +
            $"&start_date={yesterday}&end_date={tomorrow}&hourly={hourly}&timezone=Europe/Warsaw";

        using var doc = JsonDocument.Parse(await httpClient.GetStringAsync(weatherUrl, ct));
        var hourlyNode = doc.RootElement.GetProperty("hourly");

        var localTimes = hourlyNode.GetProperty("time")
            .EnumerateArray()
            .Select(x => DateTime.SpecifyKind(DateTime.Parse(x.GetString()!), DateTimeKind.Unspecified))
            .ToArray();

        var utcTimes = localTimes
            .Select(t => TimeZoneInfo.ConvertTimeToUtc(t, Warsaw))
            .ToArray();

        double?[] GetD(string name) =>
            hourlyNode.TryGetProperty(name, out var arr)
                ? arr.EnumerateArray().Select(x => x.ValueKind == JsonValueKind.Null ? (double?)null : x.GetDouble()).ToArray()
                : Enumerable.Repeat<double?>(null, utcTimes.Length).ToArray();

        int?[] GetI(string name) =>
            hourlyNode.TryGetProperty(name, out var arr)
                ? arr.EnumerateArray().Select(x => x.ValueKind == JsonValueKind.Null ? (int?)null : x.GetInt32()).ToArray()
                : Enumerable.Repeat<int?>(null, utcTimes.Length).ToArray();

        return new Weather3DayData
        {
            CityId = city.CityId,
            TimeUtc = utcTimes,
            Temperature = GetD("temperature_2m"),
            DewPoint = GetD("dewpoint_2m"),
            Humidity = GetD("relative_humidity_2m"),
            Precip = GetD("precipitation"),
            Rain = GetD("rain"),
            Snowfall = GetD("snowfall"),
            SnowDepth = GetD("snow_depth"),
            WeatherCode = GetI("weathercode"),
            WindSpeed = GetD("windspeed_10m"),
            WindGust = GetD("windgusts_10m"),
            Pressure = GetD("pressure_msl"),
            Visibility = GetD("visibility"),
            SoilTemp = GetD("soil_temperature_0cm"),
        };
    }

    private static JsonElement PickBestGeoResult(JsonElement results, CityToCheck city)
    {
        var preferred = results.EnumerateArray().FirstOrDefault(r =>
            r.TryGetProperty("country_code", out var c) &&
            string.Equals(c.GetString(), city.Iso2, StringComparison.OrdinalIgnoreCase));

        if (preferred.ValueKind != JsonValueKind.Undefined)
        {
            return preferred;
        }

        return results[0];
    }

    private static bool IsTrue(double?[] source, int index, Func<double, bool> predicate)
    {
        return index >= 0 && index < source.Length && source[index].HasValue && predicate(source[index]!.Value);
    }

    private static List<AlertRow> BuildAlertRanges(
        Weather3DayData data,
        AlertType alertType,
        Func<int, bool> predicate,
        DateTime createdAtUtc,
        byte confidence,
        DateTime windowStartUtc,
        DateTime windowEndUtc)
    {
        var output = new List<AlertRow>();
        var i = 0;

        while (i < data.TimeUtc.Length)
        {
            if (!predicate(i))
            {
                i++;
                continue;
            }

            var start = i;
            while (i + 1 < data.TimeUtc.Length && predicate(i + 1))
            {
                i++;
            }

            var end = i;
            var rangeStartUtc = data.TimeUtc[start];
            var rangeEndUtc = data.TimeUtc[end].AddHours(1);

            if (rangeEndUtc <= windowStartUtc || rangeStartUtc >= windowEndUtc)
            {
                i++;
                continue;
            }

            var clippedStartUtc = rangeStartUtc < windowStartUtc ? windowStartUtc : rangeStartUtc;
            var clippedEndUtc = rangeEndUtc > windowEndUtc ? windowEndUtc : rangeEndUtc;

            output.Add(new AlertRow
            {
                CityId = data.CityId,
                AlertTypeId = (int)alertType,
                ValidFromUtc = clippedStartUtc,
                ValidToUtc = clippedEndUtc,
                CreatedAtUtc = createdAtUtc,
                Confidence = confidence
            });

            i++;
        }

        return output;
    }

    private async Task<(List<CityToCheck> Cities, List<Weather3DayData> Forecasts, List<AlertRow> Alerts, int InsertedCount)>
        GenerateAndStoreAlertsAsync(CancellationToken ct)
    {
        var cities = await GetSubscribedCitiesAsync(ct);
        var forecasts = new List<Weather3DayData>(cities.Count);

        foreach (var city in cities)
        {
            forecasts.Add(await Fetch3DaysAsync(city, ct));
        }

        var alerts = await CreateAlertsFromForecastsAsync(forecasts);
        var inserted = await InsertAlertsAsync(alerts, ct);

        return (cities, forecasts, alerts, inserted);
    }

    private async Task<(int UsersWithAlertsCount, int EmailsSentCount, int EmailsFailedCount)>
        SendAlertsToSubscribedUsersAsync(
            List<AlertRow> alerts,
            List<CityToCheck> cities,
            CancellationToken ct)
    {
        var users = await GetSubscribedUsersWithCitiesAsync(ct);
        if (users.Count == 0 || alerts.Count == 0)
        {
            return (0, 0, 0);
        }

        var cityById = cities.ToDictionary(c => c.CityId, c => c);
        var sent = 0;
        var failed = 0;
        var usersWithAlerts = 0;

        foreach (var user in users)
        {
            var cityIds = user.CityIds;
            var userAlerts = alerts
                .Where(a => cityIds.Contains(a.CityId))
                .OrderBy(a => a.CityId)
                .ThenBy(a => a.ValidFromUtc)
                .ThenBy(a => a.AlertTypeId)
                .ToList();

            if (userAlerts.Count == 0)
            {
                continue;
            }

            usersWithAlerts++;

            var mailBody = BuildAlertEmailBody(user.Email, userAlerts, cityById);
            var subject = "WeatherAlerts: Alerty pogodowe na jutro";
            var mailResult = await emailService.SendAsync(user.Email, subject, mailBody);

            if (mailResult is EmailSendResult.Success)
            {
                sent++;
            }
            else if (mailResult is EmailSendResult.Failed mailFailed)
            {
                failed++;
                logger.LogWarning("Sending alert mail failed for {Email}. Reason: {Reason}", user.Email, mailFailed.Reason);
            }
        }

        return (usersWithAlerts, sent, failed);
    }

    private async Task<List<UserSubscription>> GetSubscribedUsersWithCitiesAsync(CancellationToken ct)
    {
        const string sql = @"
SELECT
    u.Email,
    c.ID
FROM dbo.Users u
JOIN dbo.UserCities uc ON uc.UserID = u.ID
JOIN dbo.Cities c ON c.ID = uc.CityID
WHERE u.IsSubscribed = 1
  AND u.Email IS NOT NULL;";

        var connectionString = GetRequiredDefaultConnectionString();
        var users = new Dictionary<string, HashSet<int>>(StringComparer.OrdinalIgnoreCase);

        await using var con = await OpenSqlConnectionWithRetryAsync(connectionString, ct);

        await using var cmd = new SqlCommand(sql, con);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var email = reader.GetString(0);
            var cityId = reader.GetInt32(1);

            if (!users.TryGetValue(email, out var set))
            {
                set = new HashSet<int>();
                users[email] = set;
            }

            set.Add(cityId);
        }

        return users
            .Select(kvp => new UserSubscription(kvp.Key, kvp.Value))
            .ToList();
    }

    private static string BuildAlertEmailBody(
        string email,
        List<AlertRow> alerts,
        IReadOnlyDictionary<int, CityToCheck> cityById)
    {
        var byCity = alerts
            .GroupBy(a => a.CityId)
            .OrderBy(g =>
            {
                if (cityById.TryGetValue(g.Key, out var city))
                {
                    return city.Name;
                }

                return g.Key.ToString();
            });

        var lines = new List<string>
        {
            "Czesc,",
            "",
            "Ponizej znajdziesz alerty pogodowe na jutro dla Twoich miast.",
            ""
        };

        foreach (var cityGroup in byCity)
        {
            if (!cityById.TryGetValue(cityGroup.Key, out var city))
            {
                continue;
            }

            lines.Add($"{city.Name}, {city.CountryName}");
            lines.Add("");

            var alertsByType = cityGroup
                .GroupBy(a => a.AlertTypeId)
                .OrderBy(g => g.Key);

            foreach (var typeGroup in alertsByType)
            {
                lines.Add($"   {GetAlertTypeLabel(typeGroup.Key)}");

                foreach (var alert in typeGroup.OrderBy(a => a.ValidFromUtc))
                {
                    var fromLocal = TimeZoneInfo.ConvertTimeFromUtc(alert.ValidFromUtc, Warsaw);
                    var toLocal = TimeZoneInfo.ConvertTimeFromUtc(alert.ValidToUtc, Warsaw);
                    lines.Add($"      {fromLocal:HH:mm} - {toLocal:HH:mm}");
                }

                lines.Add("");
            }

            lines.Add("");
        }

        lines.Add("Dbaj o bezpieczenstwo i milego dnia :)");
        lines.Add("WeatherAlerts");

        return string.Join("\n", lines);
    }

    private static string GetAlertTypeLabel(int alertTypeId)
    {
        return alertTypeId switch
        {
            (int)AlertType.WindshieldScraping => "Potrzebne skrobanie szyb",
            (int)AlertType.Snowfall => "Opady sniegu",
            (int)AlertType.Rain => "Opady deszczu",
            (int)AlertType.HeavyRain => "Silne opady deszczu",
            (int)AlertType.CommuteDifficulties => "Utrudnienia komunikacyjne",
            (int)AlertType.PoorAirQuality => "Slaba jakosc powietrza",
            (int)AlertType.SignificantTempChange => "Znaczna zmiana temperatury",
            (int)AlertType.UnfavourableBiometeo => "Niekorzystny biometeo",
            (int)AlertType.Fog => "Mgla",
            (int)AlertType.StrongWind => "Silny wiatr",
            (int)AlertType.FreezingRainOrBlackIce => "Marznacy deszcz / gololedz",
            (int)AlertType.SnowOnRoad => "Snieg na drogach",
            (int)AlertType.AquaplaningRisk => "Ryzyko aquaplaningu",
            _ => $"Alert typu {alertTypeId}"
        };
    }

    private sealed record UserSubscription(string Email, HashSet<int> CityIds);

    private static async Task<SqlConnection> OpenSqlConnectionWithRetryAsync(
        string connectionString,
        CancellationToken ct,
        int maxAttempts = 4)
    {
        var attempt = 0;
        Exception? last = null;

        while (attempt < maxAttempts)
        {
            attempt++;
            try
            {
                var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(ct);
                return connection;
            }
            catch (Exception ex) when (IsTransientSqlOpenError(ex) && attempt < maxAttempts)
            {
                last = ex;
                var delayMs = 1000 * attempt;
                await Task.Delay(delayMs, ct);
            }
        }

        throw new InvalidOperationException("Unable to open SQL connection after retries.", last);
    }

    private static bool IsTransientSqlOpenError(Exception ex)
    {
        if (ex is TimeoutException || ex is SocketException)
        {
            return true;
        }

        if (ex is SqlException sqlEx)
        {
            return sqlEx.Number is -2 or 40613 or 40197 or 40501 or 10928 or 10929;
        }

        if (ex.InnerException is not null)
        {
            return IsTransientSqlOpenError(ex.InnerException);
        }

        return false;
    }

    private string GetRequiredDefaultConnectionString()
    {
        var connectionString = configuration.GetConnectionString("Default");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException("Missing connection string: Default");
        }

        return connectionString;
    }
}
