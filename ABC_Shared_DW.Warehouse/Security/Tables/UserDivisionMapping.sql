CREATE TABLE [Security].[UserDivisionMapping] (

	[UserID] int NOT NULL, 
	[UserEmail] varchar(200) NOT NULL, 
	[Division] varchar(50) NOT NULL, 
	[FullName] varchar(100) NULL, 
	[IsActive] bit NULL, 
	[CreatedDate] date NULL
);