-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  customer
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our users using a single insert with many rows
CREATE DATABASE customer;

USE customer;
-- GRANT CONTROL ON DATABASE::customer TO sa;
EXEC sys.sp_cdc_enable_db;

CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);

INSERT INTO customers
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (109,'user_4','Shanghai','123567891234'),
       (110,'user_5','Shanghai','123567891234'),
       (111,'user_6','Shanghai','123567891234'),
       (118,'user_7','Shanghai','123567891234'),
       (121,'user_8','Shanghai','123567891234'),
       (123,'user_9','Shanghai','123567891234'),
       (1009,'user_10','Shanghai','123567891234'),
       (1010,'user_11','Shanghai','123567891234'),
       (1011,'user_12','Shanghai','123567891234'),
       (1012,'user_13','Shanghai','123567891234'),
       (1013,'user_14','Shanghai','123567891234'),
       (1014,'user_15','Shanghai','123567891234'),
       (1015,'user_16','Shanghai','123567891234'),
       (1016,'user_17','Shanghai','123567891234'),
       (1017,'user_18','Shanghai','123567891234'),
       (1018,'user_19','Shanghai','123567891234'),
       (1019,'user_20','Shanghai','123567891234'),
       (2000,'user_21','Shanghai','123567891234');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers', @role_name = NULL, @supports_net_changes = 0;

-- table has same name prefix with 'customers.*'
CREATE TABLE customers_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);

INSERT INTO customers_1
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (109,'user_4','Shanghai','123567891234'),
       (110,'user_5','Shanghai','123567891234'),
       (111,'user_6','Shanghai','123567891234'),
       (118,'user_7','Shanghai','123567891234'),
       (121,'user_8','Shanghai','123567891234'),
       (123,'user_9','Shanghai','123567891234'),
       (1009,'user_10','Shanghai','123567891234'),
       (1010,'user_11','Shanghai','123567891234'),
       (1011,'user_12','Shanghai','123567891234'),
       (1012,'user_13','Shanghai','123567891234'),
       (1013,'user_14','Shanghai','123567891234'),
       (1014,'user_15','Shanghai','123567891234'),
       (1015,'user_16','Shanghai','123567891234'),
       (1016,'user_17','Shanghai','123567891234'),
       (1017,'user_18','Shanghai','123567891234'),
       (1018,'user_19','Shanghai','123567891234'),
       (1019,'user_20','Shanghai','123567891234'),
       (2000,'user_21','Shanghai','123567891234');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers_1', @role_name = NULL, @supports_net_changes = 0;

-- table has combined primary key and one of the primary key is evenly
CREATE TABLE evenly_shopping_cart (
  product_no INT NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY(product_kind, product_no, user_id)
);

insert into evenly_shopping_cart
VALUES (101, 'KIND_001', 'user_1', 'my shopping cart'),
       (102, 'KIND_002', 'user_1', 'my shopping cart'),
       (103, 'KIND_007', 'user_1', 'my shopping cart'),
       (104, 'KIND_008', 'user_1', 'my shopping cart'),
       (105, 'KIND_100', 'user_2', 'my shopping list'),
       (105, 'KIND_999', 'user_3', 'my shopping list'),
       (107, 'KIND_010', 'user_4', 'my shopping list'),
       (108, 'KIND_009', 'user_4', 'my shopping list'),
       (109, 'KIND_002', 'user_5', 'leo list'),
       (111, 'KIND_007', 'user_5', 'leo list'),
       (111, 'KIND_008', 'user_5', 'leo list'),
       (112, 'KIND_009', 'user_6', 'my shopping cart');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'evenly_shopping_cart', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE [base] (
  [base_id] INT NOT NULL,
  [start_time] DATETIME NULL,
  [update_date] DATE NULL,
  [update_time] DATETIME NOT NULL DEFAULT GETDATE(),
  [price] DECIMAL(5, 2) NULL,
  [json_content] NVARCHAR(MAX) NULL, -- 使用 NVARCHAR(MAX) 存储 JSON 数据
  [col_blob] VARBINARY(MAX) NULL,    -- 对应 MySQL 的 BLOB 类型
  [col_text] NVARCHAR(MAX) NULL,     -- 对应 MySQL 的 TEXT 类型
  CONSTRAINT [PK_base] PRIMARY KEY CLUSTERED ([base_id] ASC)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'base', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE [base_01] (
  [base_id] INT NOT NULL,
  [start_time] DATETIME NULL,
  [update_date] DATE NULL,
  [update_time] DATETIME NOT NULL DEFAULT GETDATE(),
  [price] DECIMAL(5, 2) NULL,
  [json_content] NVARCHAR(MAX) NULL, -- 使用 NVARCHAR(MAX) 存储 JSON 数据
  [col_blob] VARBINARY(MAX) NULL,    -- 对应 MySQL 的 BLOB 类型
  [col_text] NVARCHAR(MAX) NULL,     -- 对应 MySQL 的 TEXT 类型
  CONSTRAINT [PK_base_01] PRIMARY KEY CLUSTERED ([base_id] ASC)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'base_01', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE [base_02] (
  [base_id] INT NOT NULL,
  [start_time] DATETIME NULL,
  [update_date] DATE NULL,
  [update_time] DATETIME NOT NULL DEFAULT GETDATE(),
  [price] DECIMAL(5, 2) NULL,
  [json_content] NVARCHAR(MAX) NULL, -- 使用 NVARCHAR(MAX) 存储 JSON 数据
  [col_blob] VARBINARY(MAX) NULL,    -- 对应 MySQL 的 BLOB 类型
  [col_text] NVARCHAR(MAX) NULL,     -- 对应 MySQL 的 TEXT 类型
  CONSTRAINT [PK_base_02] PRIMARY KEY CLUSTERED ([base_id] ASC)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'base_02', @role_name = NULL, @supports_net_changes = 0;


INSERT INTO [base] ([base_id], [start_time], [update_date], [update_time], [price], [json_content], [col_blob], [col_text])
VALUES (1, GETDATE(), CAST(GETDATE() AS DATE), GETDATE(), 1.1, N'{}', CAST('123' AS VARBINARY(MAX)), N'123');

INSERT INTO [base] ([base_id], [start_time], [update_date], [update_time], [price], [json_content], [col_blob], [col_text])
VALUES (2, GETDATE(), CAST(GETDATE() AS DATE), GETDATE(), 1.1, N'{}', CAST('321' AS VARBINARY(MAX)), N'321');

INSERT INTO [base_01] ([base_id], [start_time], [update_date], [update_time], [price], [json_content], [col_blob], [col_text])
VALUES (1, GETDATE(), CAST(GETDATE() AS DATE), GETDATE(), 1.1, N'{}', CAST('123' AS VARBINARY(MAX)), N'123');

INSERT INTO [base_02] ([base_id], [start_time], [update_date], [update_time], [price], [json_content], [col_blob], [col_text])
VALUES (2, GETDATE(), CAST(GETDATE() AS DATE), GETDATE(), 1.1, N'{}', CAST('321' AS VARBINARY(MAX)), N'321');

