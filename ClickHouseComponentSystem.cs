/*
 * 文件名: ClickHouseComponentSystem.cs
 * 作者: zengxin
 * 创建日期: 2026-3-10
 */
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;
namespace ET.Server
{
    [EntitySystemOf(typeof(ClickHouseComponent))]
    [FriendOf(typeof(ClickHouseComponent))]
    public static partial class ClickHouseComponentSystem
    {
        #region 生命周期
        
        [EntitySystem]
        private static void Awake(this ClickHouseComponent self, string connectionString, string dbName)
        {
            self.ConnectionString = connectionString;
            self.DatabaseName = dbName;
            
            try
            {
                var settings = new ClickHouseClientSettings(connectionString);
                self.Client = new ClickHouseClient(settings);
                
                // 测试连接
                var version = self.Client.ExecuteScalarAsync("SELECT version()").GetAwaiter().GetResult();
                Log.Info($"ClickHouseComponent 初始化成功: {dbName}, 版本: {version}");
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouseComponent 初始化失败: {dbName}, {ex.Message}");
            }
        }
        
        [EntitySystem]
        private static void Destroy(this ClickHouseComponent self)
        {
            self.Client?.Dispose();
            Log.Info($"ClickHouseComponent 已销毁: {self.DatabaseName}");
        }
        
        #endregion
        
        #region 私有辅助方法
        
        /// <summary>
        /// 将匿名对象转换为 ClickHouseParameterCollection
        /// </summary>
        private static ClickHouseParameterCollection ToParameterCollection(object parameters)
        {
            var collection = new ClickHouseParameterCollection();
            
            if (parameters == null)
                return collection;
            
            // 如果是字典类型
            if (parameters is IDictionary<string, object> dict)
            {
                foreach (var kv in dict)
                {
                    collection.AddParameter(kv.Key, kv.Value);
                }
            }
            else
            {
                // 如果是匿名对象，反射获取属性
                var properties = parameters.GetType().GetProperties();
                foreach (var prop in properties)
                {
                    collection.AddParameter(prop.Name, prop.GetValue(parameters));
                }
            }
            
            return collection;
        }
        
        /// <summary>
        /// 将DataReader读取为List<T>
        /// </summary>
        private static async Task<List<T>> ReadReaderToListAsync<T>(DbDataReader reader) where T : class, new()
        {
            var result = new List<T>();
            var properties = typeof(T).GetProperties();
            var columnNames = Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.GetName(i))
                .ToList();
            
            while (await reader.ReadAsync())
            {
                var item = new T();
                for (int i = 0; i < columnNames.Count; i++)
                {
                    var columnName = columnNames[i];
                    var prop = properties.FirstOrDefault(p => 
                        p.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                    
                    if (prop != null && !await reader.IsDBNullAsync(i))
                    {
                        var value = reader.GetValue(i);
                        try
                        {
                            if (value.GetType() != prop.PropertyType)
                            {
                                value = Convert.ChangeType(value, prop.PropertyType);
                            }
                            prop.SetValue(item, value);
                        }
                        catch (Exception ex)
                        {
                            Log.Warning($"属性 {prop.Name} 类型转换失败: {ex.Message}");
                        }
                    }
                }
                result.Add(item);
            }
            return result;
        }
        
        /// <summary>
        /// 将DataReader读取为单个实体
        /// </summary>
        private static async Task<T> ReadReaderFirstOrDefaultAsync<T>(DbDataReader reader) where T : class, new()
        {
            var properties = typeof(T).GetProperties();
            var columnNames = Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.GetName(i))
                .ToList();
            
            if (await reader.ReadAsync())
            {
                var item = new T();
                for (int i = 0; i < columnNames.Count; i++)
                {
                    var columnName = columnNames[i];
                    var prop = properties.FirstOrDefault(p => 
                        p.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                    
                    if (prop != null && !await reader.IsDBNullAsync(i))
                    {
                        var value = reader.GetValue(i);
                        try
                        {
                            if (value.GetType() != prop.PropertyType)
                            {
                                value = Convert.ChangeType(value, prop.PropertyType);
                            }
                            prop.SetValue(item, value);
                        }
                        catch (Exception ex)
                        {
                            Log.Warning($"属性 {prop.Name} 类型转换失败: {ex.Message}");
                        }
                    }
                }
                return item;
            }
            
            return null;
        }
        
        /// <summary>
        /// 将DataReader读取为动态列表
        /// </summary>
        private static async Task<List<dynamic>> ReadReaderToDynamicListAsync(DbDataReader reader)
        {
            var result = new List<dynamic>();
            var columnNames = Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.GetName(i))
                .ToList();
            
            while (await reader.ReadAsync())
            {
                var expando = new System.Dynamic.ExpandoObject() as IDictionary<string, object>;
                for (int i = 0; i < columnNames.Count; i++)
                {
                    if (!await reader.IsDBNullAsync(i))
                    {
                        expando[columnNames[i]] = reader.GetValue(i);
                    }
                    else
                    {
                        expando[columnNames[i]] = null;
                    }
                }
                result.Add(expando);
            }
            return result;
        }
        

        
        #endregion
        
        #region 查询操作
        
        /// <summary>
        /// 执行查询并返回强类型列表
        /// </summary>
        public static async ETTask<List<T>> QueryAsync<T>(this ClickHouseComponent self, string sql, object parameters = null) where T : class, new()
        {
            try
            {
                var paramCollection = ToParameterCollection(parameters);
                using var reader = await self.Client.ExecuteReaderAsync(sql, paramCollection);
                return await ReadReaderToListAsync<T>(reader);
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse查询失败: {ex.Message}\n{sql}");
                return new List<T>();
            }
        }
        
        /// <summary>
        /// 查询单个实体
        /// </summary>
        public static async ETTask<T> QuerySingleAsync<T>(this ClickHouseComponent self, string sql, object parameters = null) where T : class, new()
        {
            try
            {
                var paramCollection = ToParameterCollection(parameters);
                using var reader = await self.Client.ExecuteReaderAsync(sql, paramCollection);
                return await ReadReaderFirstOrDefaultAsync<T>(reader);
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse查询失败: {ex.Message}\n{sql}");
                return null;
            }
        }
        
        /// <summary>
        /// 查询动态类型
        /// </summary>
        public static async ETTask<List<dynamic>> QueryDynamicAsync(this ClickHouseComponent self, string sql, object parameters = null)
        {
            try
            {
                var paramCollection = ToParameterCollection(parameters);
                using var reader = await self.Client.ExecuteReaderAsync(sql, paramCollection);
                return await ReadReaderToDynamicListAsync(reader);
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse查询失败: {ex.Message}\n{sql}");
                return new List<dynamic>();
            }
        }
        
        /// <summary>
        /// 查询所有
        /// </summary>
        public static async ETTask<List<T>> QueryAllAsync<T>(this ClickHouseComponent self, string tableName, int limit = 1000) where T : class, new()
        {
            string sql = $"SELECT * FROM {tableName} LIMIT {limit}";
            return await self.QueryAsync<T>(sql);
        }
        
        /// <summary>
        /// 条件查询
        /// </summary>
        public static async ETTask<List<T>> QueryAsync<T>(this ClickHouseComponent self, string tableName, string where, object parameters = null, int limit = 1000) where T : class, new()
        {
            string sql = $"SELECT * FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            sql += $" LIMIT {limit}";
            
            return await self.QueryAsync<T>(sql, parameters);
        }
        
        /// <summary>
        /// 带排序的查询
        /// </summary>
        public static async ETTask<List<T>> QueryWithSortAsync<T>(this ClickHouseComponent self,
            string tableName,
            string where,
            string orderBy,
            bool desc = false,
            int limit = 1000,
            object parameters = null) where T : class, new()
        {
            string sql = $"SELECT * FROM {tableName}";
            
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            if (!string.IsNullOrEmpty(orderBy))
            {
                sql += $" ORDER BY {orderBy} {(desc ? "DESC" : "ASC")}";
            }
            
            sql += $" LIMIT {limit}";
            
            return await self.QueryAsync<T>(sql, parameters);
        }
        
        /// <summary>
        /// 分页查询
        /// </summary>
        public static async ETTask<(List<T> Items, ulong Total)> QueryPagedAsync<T>(this ClickHouseComponent self,
            string tableName,
            string where,
            string orderBy,
            int page,
            int pageSize,
            object parameters = null) where T : class, new()
        {
            int offset = (page - 1) * pageSize;
            
            // 查询总数
            string countSql = $"SELECT COUNT() FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                countSql += $" WHERE {where}";
            }
            
            // 查询数据
            string dataSql = $"SELECT * FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                dataSql += $" WHERE {where}";
            }
            
            if (!string.IsNullOrEmpty(orderBy))
            {
                dataSql += $" ORDER BY {orderBy}";
            }
            
            dataSql += $" LIMIT {pageSize} OFFSET {offset}";
            
            try
            {
                var paramCollection = ToParameterCollection(parameters);
                
                // 查询总数
                var totalObj = await self.Client.ExecuteScalarAsync(countSql, paramCollection);
                ulong total = totalObj != null ? Convert.ToUInt64(totalObj) : 0UL;
                
                // 查询数据
                using var reader = await self.Client.ExecuteReaderAsync(dataSql, paramCollection);
                var items = await ReadReaderToListAsync<T>(reader);
                
                return (items, total);
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse分页查询失败: {ex.Message}");
                return (new List<T>(), 0);
            }
        }
        
        /// <summary>
        /// 按时间范围查询
        /// </summary>
        public static async ETTask<List<T>> QueryByTimeRangeAsync<T>(this ClickHouseComponent self,
            string tableName,
            string timeColumn,
            DateTime startTime,
            DateTime endTime,
            string where = null,
            object parameters = null) where T : class, new()
        {
            string timeCondition = $"{timeColumn} >= '{startTime:yyyy-MM-dd HH:mm:ss}' AND {timeColumn} < '{endTime:yyyy-MM-dd HH:mm:ss}'";
            string finalWhere = string.IsNullOrEmpty(where) ? timeCondition : $"({where}) AND {timeCondition}";
            
            return await self.QueryAsync<T>(tableName, finalWhere, parameters);
        }
        
        #endregion
        
        #region 聚合查询
        
        /// <summary>
        /// 执行标量查询
        /// </summary>
        public static async ETTask<T> ExecuteScalarAsync<T>(this ClickHouseComponent self, string sql, object parameters = null)
        {
            try
            {
                var paramCollection = ToParameterCollection(parameters);
                var result = await self.Client.ExecuteScalarAsync(sql, paramCollection);
                
                if (result == null || result == DBNull.Value)
                {
                    return default;
                }
                
                return (T)Convert.ChangeType(result, typeof(T));
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse标量查询失败: {ex.Message}\n{sql}");
                return default;
            }
        }
        
        /// <summary>
        /// 计数
        /// </summary>
        public static async ETTask<ulong> CountAsync(this ClickHouseComponent self, string tableName, string where = null, object parameters = null)
        {
            string sql = $"SELECT COUNT() FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.ExecuteScalarAsync<ulong>(sql, parameters);
        }
        
        /// <summary>
        /// 求和
        /// </summary>
        public static async ETTask<T> SumAsync<T>(this ClickHouseComponent self, string tableName, string column, string where = null, object parameters = null)
        {
            string sql = $"SELECT SUM({column}) FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.ExecuteScalarAsync<T>(sql, parameters);
        }
        
        /// <summary>
        /// 平均值
        /// </summary>
        public static async ETTask<double> AvgAsync(this ClickHouseComponent self, string tableName, string column, string where = null, object parameters = null)
        {
            string sql = $"SELECT AVG({column}) FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.ExecuteScalarAsync<double>(sql, parameters);
        }
        
        /// <summary>
        /// 最大值
        /// </summary>
        public static async ETTask<T> MaxAsync<T>(this ClickHouseComponent self, string tableName, string column, string where = null, object parameters = null)
        {
            string sql = $"SELECT MAX({column}) FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.ExecuteScalarAsync<T>(sql, parameters);
        }
        
        /// <summary>
        /// 最小值
        /// </summary>
        public static async ETTask<T> MinAsync<T>(this ClickHouseComponent self, string tableName, string column, string where = null, object parameters = null)
        {
            string sql = $"SELECT MIN({column}) FROM {tableName}";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.ExecuteScalarAsync<T>(sql, parameters);
        }
        
        #endregion
        
        #region 写入操作
        
        /// <summary>
        /// 批量插入（最高性能，推荐）
        /// </summary>
        public static async ETTask InsertBatchAsync<T>(this ClickHouseComponent self, string tableName, IEnumerable<T> entities) where T : class
        {
            var list = entities.ToList();
            if (!list.Any()) return;
            
            try
            {
                var properties = ClickHouseComponent.propertiesCache.GetOrAdd(typeof(T), t =>
                        t.GetProperties()
                                .Where(p => !p.PropertyType.IsGenericType)
                                .ToArray()
                );
        
                var columns = properties.Select(p => p.Name).ToArray();
        
                var rows = new List<object[]>(list.Count);
                foreach (var e in list)
                {
                    var row = new object[properties.Length];
                    for (int i = 0; i < properties.Length; i++)
                    {
                        row[i] = properties[i].GetValue(e);
                    }
                    rows.Add(row);
                }
        
                await self.Client.InsertBinaryAsync(tableName, columns, rows);
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse批量写入失败: {tableName}, {ex.Message}");
                throw;
            }
        }
        
        /// <summary>
        /// 插入单条
        /// </summary>
        public static async ETTask InsertAsync<T>(this ClickHouseComponent self, string tableName, T entity) where T : class
        {
            await self.InsertBatchAsync(tableName, new[] { entity });
        }
        
        /// <summary>
        /// 执行非查询SQL（DDL或DML）- 使用 ExecuteNonQueryAsync
        /// </summary>
        public static async ETTask<int> ExecuteNonQueryAsync(this ClickHouseComponent self, string sql, object parameters = null)
        {
            try
            {
                var paramCollection = ToParameterCollection(parameters);
                return await self.Client.ExecuteNonQueryAsync(sql, paramCollection);
            }
            catch (Exception ex)
            {
                Log.Error($"ClickHouse执行失败: {ex.Message}\n{sql}");
                return 0;
            }
        }
        
        /// <summary>
        /// 创建表
        /// </summary>
        public static async ETTask CreateTableAsync(this ClickHouseComponent self, string createTableSql)
        {
            await self.ExecuteNonQueryAsync(createTableSql);
        }
        
        /// <summary>
        /// 删除表
        /// </summary>
        public static async ETTask DropTableAsync(this ClickHouseComponent self, string tableName)
        {
            await self.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
        
        #endregion
        
        #region 检查方法
        
        /// <summary>
        /// 检查表是否存在
        /// </summary>
        public static async ETTask<bool> TableExistsAsync(this ClickHouseComponent self, string tableName)
        {
            try
            {
                string sql = $"EXISTS TABLE {tableName}";
                var result = await self.ExecuteScalarAsync<byte>(sql);
                return result == 1;
            }
            catch
            {
                return false;
            }
        }
        
        /// <summary>
        /// 获取表大小（字节）
        /// </summary>
        public static async ETTask<ulong> GetTableSizeAsync(this ClickHouseComponent self, string tableName)
        {
            string sql = $@"
                SELECT sum(bytes) 
                FROM system.parts 
                WHERE table = '{tableName}' AND active
            ";
            
            return await self.ExecuteScalarAsync<ulong>(sql);
        }
        
        /// <summary>
        /// 获取表行数
        /// </summary>
        public static async ETTask<ulong> GetTableRowsAsync(this ClickHouseComponent self, string tableName)
        {
            string sql = $@"
                SELECT sum(rows) 
                FROM system.parts 
                WHERE table = '{tableName}' AND active
            ";
            
            return await self.ExecuteScalarAsync<ulong>(sql);
        }
        
        #endregion
    }
}