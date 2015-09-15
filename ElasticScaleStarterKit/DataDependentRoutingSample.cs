// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleStarterKit
{
    internal static class DataDependentRoutingSample
    {
        private static string[] s_customerNames = new[]
        {
            "AdventureWorks Cycles",
            "Contoso Ltd.",
            "Microsoft Corp.",
            "Northwind Traders",
            "ProseWare, Inc.",
            "Lucerne Publishing",
            "Fabrikam, Inc.",
            "Coho Winery",
            "Alpine Ski House",
            "Humongous Insurance"
        };

        private static Random s_r = new Random();

        public static void ExecuteDataDependentRoutingQuery(RangeShardMap<int> shardMap, string credentialsConnectionString)
        {
            // 亂數產生一筆 Key
            //亂數最高範圍
            int currentMaxHighKey = shardMap.GetMappings().Max(m => m.Value.High);
            //亂數取得 CustomerID
            int customerId = GetCustomerId(currentMaxHighKey);
            //亂數產生 Customer Name
            string customerName = s_customerNames[s_r.Next(s_customerNames.Length)];
            int regionId = 0;
            int productId = 0;

            AddCustomer(
                shardMap,
                credentialsConnectionString,
                customerId,
                customerName,
                regionId);

            AddOrder(
                shardMap,
                credentialsConnectionString,
                customerId,
                productId);
        }

        /// <summary>
        /// Adds a customer to the customers table (or updates the customer if that id already exists).
        /// </summary>
        private static void AddCustomer(
            ShardMap shardMap,
            string credentialsConnectionString,
            int customerId,
            string name,
            int regionId)
        {
            //失敗的話重試
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                // 依據 Key 判斷要取回哪個 Shard
                using (SqlConnection conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString))
                {
                    // 產生 Command
                    SqlCommand cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                    IF EXISTS (SELECT 1 FROM Customers WHERE CustomerId = @customerId)
                        UPDATE Customers
                            SET Name = @name, RegionId = @regionId
                            WHERE CustomerId = @customerId
                    ELSE
                        INSERT INTO Customers (CustomerId, Name, RegionId)
                        VALUES (@customerId, @name, @regionId)";
                    cmd.Parameters.AddWithValue("@customerId", customerId);
                    cmd.Parameters.AddWithValue("@name", name);
                    cmd.Parameters.AddWithValue("@regionId", regionId);
                    cmd.CommandTimeout = 60;

                    // Execute the command
                    cmd.ExecuteNonQuery();
                }
            });
        }

        /// <summary>
        /// Adds an order to the orders table for the customer.
        /// </summary>
        private static void AddOrder(
            ShardMap shardMap,
            string credentialsConnectionString,
            int customerId,
            int productId)
        {
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                // Looks up the key in the shard map and opens a connection to the shard
                using (SqlConnection conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString))
                {
                    // Create a simple command that will insert a new order
                    SqlCommand cmd = conn.CreateCommand();

                    // Create a simple command
                    cmd.CommandText = @"INSERT INTO dbo.Orders (CustomerId, OrderDate, ProductId)
                                        VALUES (@customerId, @orderDate, @productId)";
                    cmd.Parameters.AddWithValue("@customerId", customerId);
                    cmd.Parameters.AddWithValue("@orderDate", DateTime.Now.Date);
                    cmd.Parameters.AddWithValue("@productId", productId);
                    cmd.CommandTimeout = 60;

                    // Execute the command
                    cmd.ExecuteNonQuery();
                }
            });

            ConsoleUtils.WriteInfo("Inserted order for customer ID: {0}", customerId);
        }

        /// <summary>
        /// Gets a customer ID to insert into the customers table.
        /// </summary>
        private static int GetCustomerId(int maxid)
        {
            //亂數產生 Customer ID
            return s_r.Next(0, maxid);
        }
    }
}
