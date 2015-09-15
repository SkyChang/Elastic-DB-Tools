// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleStarterKit
{
    internal static class ShardManagementUtils
    {
        /// <summary>
        /// Tries to get the ShardMapManager that is stored in the specified database.
        /// </summary>
        public static ShardMapManager TryGetShardMapManager(string shardMapManagerServerName, string shardMapManagerDatabaseName)
        {
            string shardMapManagerConnectionString =
                    Configuration.GetConnectionString(
                        Configuration.ShardMapManagerServerName,
                        Configuration.ShardMapManagerDatabaseName);

            if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerServerName, shardMapManagerDatabaseName))
            {
                // Shard Map Manager database has not yet been created
                return null;
            }

            ShardMapManager shardMapManager;
            bool smmExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                shardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy,
                out shardMapManager);

            if (!smmExists)
            {
                // Shard Map Manager database exists, but Shard Map Manager has not been created
                return null;
            }

            return shardMapManager;
        }

        /// <summary>
        /// 指定 connection String 來建立 shard map manager 到 DB
        /// </summary>
        public static ShardMapManager CreateOrGetShardMapManager(string shardMapManagerConnectionString)
        {
            // 取得 shard map manager 的 connection string 後
            // 嘗試取得存放於 shard map database 的 Shard Map Manager。
            ShardMapManager shardMapManager;
            bool shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                shardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy,
                out shardMapManager);

            if (shardMapManagerExists)
            {
                ConsoleUtils.WriteInfo("Shard Map Manager already exists");
            }
            else
            {
                // 假如不存在，就建立
                shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(shardMapManagerConnectionString);
                ConsoleUtils.WriteInfo("Created Shard Map Manager");
            }

            return shardMapManager;
        }

        /// <summary>
        /// 使用指定的名稱來建立一個新的 Range Shard Map，如果它已經存在，則取得。
        /// </summary>
        public static RangeShardMap<T> CreateOrGetRangeShardMap<T>(ShardMapManager shardMapManager, string shardMapName)
        {
            // 嘗試取得 Shard Map
            RangeShardMap<T> shardMap;
            bool shardMapExists = shardMapManager.TryGetRangeShardMap(shardMapName, out shardMap);

            if (shardMapExists)
            {
                ConsoleUtils.WriteInfo("Shard Map {0} already exists", shardMap.Name);
            }
            else
            {
                // 如果不存在就建立
                shardMap = shardMapManager.CreateRangeShardMap<T>(shardMapName);
                ConsoleUtils.WriteInfo("Created Shard Map {0}", shardMap.Name);
            }

            return shardMap;
        }

        /// <summary>
        /// 增加 Shards 到 Shard Map, 如果已經存在，則直接返回.
        /// </summary>
        public static Shard CreateOrGetShard(ShardMap shardMap, ShardLocation shardLocation)
        {
            // 嘗試取得 Shard
            Shard shard;
            bool shardExists = shardMap.TryGetShard(shardLocation, out shard);

            if (shardExists)
            {
                ConsoleUtils.WriteInfo("Shard {0} has already been added to the Shard Map", shardLocation.Database);
            }
            else
            {
                //不存在，則建立
                shard = shardMap.CreateShard(shardLocation);
                ConsoleUtils.WriteInfo("Added shard {0} to the Shard Map", shardLocation.Database);
            }

            return shard;
        }
    }
}
