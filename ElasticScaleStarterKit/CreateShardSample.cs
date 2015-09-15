// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleStarterKit
{
    internal class CreateShardSample
    {
        /// <summary>
        /// 建立，或使用現有的 shard , 並加入到 shard map,
        /// 並且分配指定位置
        /// </summary>
        public static void CreateShard(RangeShardMap<int> shardMap, Range<int> rangeForNewShard)
        {
            // 建立 new shard, or 取得現有的 shard
            Shard shard = CreateOrGetEmptyShard(shardMap);

            // 針對 shard 建立 mapping
            RangeMapping<int> mappingForNewShard = shardMap.CreateRangeMapping(rangeForNewShard, shard);
            ConsoleUtils.WriteInfo("Mapped range {0} to shard {1}", mappingForNewShard.Value, shard.Location.Database);
        }

        /// <summary>
        /// Script file that will be executed to initialize a shard.
        /// </summary>
        private const string InitializeShardScriptFile = "InitializeShard.sql";

        /// <summary>
        /// Format to use for creating shard name. {0} is the number of shards that have already been created.
        /// </summary>
        private const string ShardNameFormat = "ElasticScaleStarterKit_Shard{0}";

        /// <summary>
        /// 建立 new shard, 或取得現有空的 shard (i.e. a shard 可能還沒 mapper ).
        /// 空的 shard 可能存在的原因是因為，創建和初始化的時候，我們無法 mapping 他
        /// </summary>
        private static Shard CreateOrGetEmptyShard(RangeShardMap<int> shardMap)
        {
            // 假如已經有了，則取得空的 shard
            Shard shard = FindEmptyShard(shardMap);
            if (shard == null)
            {
                // 為 Null，表示沒有空的，所以要建立

                // 更改 shard name
                string databaseName = string.Format(ShardNameFormat, shardMap.GetShards().Count());

                // 假如資料庫不存在，則建立資料庫
                // 假如存在，則會返回
                if (!SqlDatabaseUtils.DatabaseExists(Configuration.ShardMapManagerServerName, databaseName))
                {
                    SqlDatabaseUtils.CreateDatabase(Configuration.ShardMapManagerServerName, databaseName);
                }

                // 建立 schema 和相關的資料到 db
                // 並使用初始化腳本初始化，如果資料庫已經存在，會發生問題
                // 
                SqlDatabaseUtils.ExecuteSqlScript(
                    Configuration.ShardMapManagerServerName, databaseName, InitializeShardScriptFile);

                // 產生shard Location ， 並取得 Shard
                ShardLocation shardLocation = new ShardLocation(Configuration.ShardMapManagerServerName, databaseName);
                shard = ShardManagementUtils.CreateOrGetShard(shardMap, shardLocation);
            }

            return shard;
        }

        /// <summary>
        /// 尋找現有為空的 Shard ，假如沒有回傳 null
        /// </summary>
        private static Shard FindEmptyShard(RangeShardMap<int> shardMap)
        {
            // 從 shard map 取得所有 Shard
            IEnumerable<Shard> allShards = shardMap.GetShards();

            // 從 shard map 取得所有 mappings
            IEnumerable<RangeMapping<int>> allMappings = shardMap.GetMappings();

            // 確認 shards 是否有 mappings
            HashSet<Shard> shardsWithMappings = new HashSet<Shard>(allMappings.Select(m => m.Shard));

            // 取得第一個 shard ( 依據名稱排序 )
            return allShards.OrderBy(s => s.Location.Database).FirstOrDefault(s => !shardsWithMappings.Contains(s));
        }
    }
}
