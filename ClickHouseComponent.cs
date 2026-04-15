/*
 * 文件名: ClickHouseComponent.cs
 * 作者: zengxin
 * 创建日期: 2026-3-10
 */
using ClickHouse.Driver;
using System;
using System.Reflection;
using System.Collections.Concurrent;
namespace ET.Server
{
    [ChildOf(typeof(DBManagerComponent))]
    public class ClickHouseComponent : Entity, IAwake<string, string>, IDestroy
    {
        public string ConnectionString { get; set; }
        public string DatabaseName { get; set; }
        
        // ClickHouse 客户端（线程安全，可作为单例）
        public ClickHouseClient Client { get; set; }
        
        [StaticField]
        public static readonly ConcurrentDictionary<Type, PropertyInfo[]> propertiesCache = new();
    }
}