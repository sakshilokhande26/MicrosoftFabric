CREATE TABLE [dbo].[AllSales] (

	[SaleID] int NOT NULL, 
	[Division] varchar(50) NOT NULL, 
	[ProductName] varchar(100) NULL, 
	[Amount] decimal(10,2) NULL, 
	[SaleDate] date NULL, 
	[CustomerName] varchar(100) NULL, 
	[SalesRep] varchar(100) NULL, 
	[CustomerEmail] varchar(100) NULL
);