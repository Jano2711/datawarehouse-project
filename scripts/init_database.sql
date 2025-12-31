/*Creta database, if exists drop it.

Create schemas
*/
USE MASTER;
GO
-- Drop database if exists

IF EXISTS (Select 1 * from sys.databases where name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
CREATE DATABASE DataWarehouse;
GO
use DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;