CREATE DATABASE testct;
GO

ALTER DATABASE testct SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
GO

USE testct;
GO

ALTER DATABASE testct
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO

CREATE TABLE [User] (
    id BIGINT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(100)
);
GO

CREATE TABLE [Customer] (
    CustomerId int IDENTITY(1,1) NOT NULL,
    ExternalCode nvarchar(50) NOT NULL,
    FullName nvarchar(200) NOT NULL,
    Email nvarchar(200) NULL,
    Status tinyint DEFAULT 1 NOT NULL,
    CreatedAt datetime2(0) DEFAULT sysutcdatetime() NOT NULL,
    UpdatedAt datetime2(0) NULL,
    PRIMARY KEY (CustomerId),
    UNIQUE (ExternalCode)
);
GO

CREATE TABLE [Product] (
    id bigint NOT NULL,
    Name text NULL,
    Category varchar(100) NULL,
    Price numeric(10,2) NULL,
    PRIMARY KEY (id)
);
GO

ALTER TABLE [User]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

ALTER TABLE [Customer]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

ALTER TABLE [Product]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

CREATE QUEUE SyncDDLQueue;
GO

CREATE SERVICE SyncDDLService
ON QUEUE SyncDDLQueue
([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]);
GO

CREATE EVENT NOTIFICATION SyncDDLEvents
ON DATABASE
FOR DDL_TABLE_EVENTS, DDL_INDEX_EVENTS, RENAME
TO SERVICE 'SyncDDLService', 'current database';
GO
