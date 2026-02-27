public sealed class CityToCheck
{
    public required int CityId { get; init; }
    public required string Name { get; init; }
    public required string CountryName { get; init; }
    public required string Iso2 { get; init; }
}

public enum AlertType
{
    WindshieldScraping = 1,
    Snowfall = 2,
    Rain = 3,
    HeavyRain = 4,
    CommuteDifficulties = 5,
    PoorAirQuality = 6,
    SignificantTempChange = 7,
    UnfavourableBiometeo = 8,
    Fog = 9,
    StrongWind = 10,
    FreezingRainOrBlackIce = 11,
    SnowOnRoad = 12,
    AquaplaningRisk = 13,
}

public sealed class Weather3DayData
{
    public required int CityId { get; init; }
    public required DateTime[] TimeUtc { get; init; }
    public required double?[] Temperature { get; init; }
    public required double?[] DewPoint { get; init; }
    public required double?[] Humidity { get; init; }
    public required double?[] Precip { get; init; }
    public required double?[] Rain { get; init; }
    public required double?[] Snowfall { get; init; }
    public required double?[] SnowDepth { get; init; }
    public required int?[] WeatherCode { get; init; }
    public required double?[] WindSpeed { get; init; }
    public required double?[] WindGust { get; init; }
    public required double?[] Pressure { get; init; }
    public required double?[] Visibility { get; init; }
    public required double?[] SoilTemp { get; init; }
}

public sealed class AlertRow
{
    public required int CityId { get; init; }
    public required int AlertTypeId { get; init; }
    public required DateTime ValidFromUtc { get; init; }
    public required DateTime ValidToUtc { get; init; }
    public required DateTime CreatedAtUtc { get; init; }
    public byte? Confidence { get; init; }
}

public sealed record CreateAlertsSummary(
    int CitiesCount,
    int ForecastsCount,
    int AlertsGeneratedCount,
    int AlertsInsertedCount);

public sealed record CreateAlertsAndSendRequestDto(string Token);

public sealed record CreateAlertsAndSendSummary(
    int CitiesCount,
    int ForecastsCount,
    int AlertsGeneratedCount,
    int AlertsInsertedCount,
    int UsersWithAlertsCount,
    int EmailsSentCount,
    int EmailsFailedCount);

public abstract record SendExistingAlertsToUserResult
{
    private SendExistingAlertsToUserResult() { }
    public sealed record Success(int AlertsCount) : SendExistingAlertsToUserResult;
    public sealed record UserNotFound : SendExistingAlertsToUserResult;
    public sealed record NoAlerts : SendExistingAlertsToUserResult;
    public sealed record Failed(string Reason) : SendExistingAlertsToUserResult;
}
