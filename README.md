# et8-ecs-clickhouse

基于 ET8 ECS 框架的 ClickHouse 驱动

## 使用示例

```csharp
public static ClickHouseComponent GetZoneClickHouse(this DBManagerComponent self, int zone)
{
    ClickHouseComponent clickHouseComponent = self.GetChild<ClickHouseComponent>(zone);
    if (clickHouseComponent != null)
    {
        return clickHouseComponent;
    }
    return null;
}

public static ClickHouseComponent CreateZoneClickHouse(this DBManagerComponent self, int zone, string connectionString, string dbName = "default")
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
    return self.AddChildWithId<ClickHouseComponent, string, string>(zone, connectionString, dbName);
}
