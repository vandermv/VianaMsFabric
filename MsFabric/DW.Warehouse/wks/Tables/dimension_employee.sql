CREATE TABLE [wks].[dimension_employee] (

	[EmployeeKey] int NULL, 
	[WWIEmployeeID] int NULL, 
	[Employee] varchar(8000) NULL, 
	[PreferredName] varchar(8000) NULL, 
	[IsSalesperson] bit NULL, 
	[Photo] varbinary(8000) NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL, 
	[LineageKey] int NULL
);