# et8-ecs-redis
基于et8 ecs 的ClickHouse驱动


public static ClickHouseComponent GetZoneClickHouse(this DBManagerComponent self, int zone)
{
    ClickHouseComponent clickHouseComponent = self.GetChild<ClickHouseComponent>(zone);
    if (clickHouseComponent != null)
    {
        return clickHouseComponent;
    }
    return null;
}
public static ClickHouseComponent CreateZoneClickHouse(this DBManagerComponent self, int zone, string connectionString, string dbName)
{
    /*
        connectionString 说明：
    "Host=127.0.0.1;Port=8123;Database=game_analytics;User=default;Password=;"
    */
    ClickHouseComponent clickHouseComponent = self.GetChild<ClickHouseComponent>(zone);
    if (clickHouseComponent != null)
    {
        throw new Exception($"zone: {zone} already created clickHouse");
    }
    if (string.IsNullOrEmpty(dbName))
    {
        dbName = "default";
    }
    return self.AddChildWithId<ClickHouseComponent, string,string>(zone, connectionString, dbName);
}
