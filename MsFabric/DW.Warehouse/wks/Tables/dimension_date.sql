CREATE TABLE [wks].[dimension_date] (

	[Date] datetime2(6) NULL, 
	[DayNumber] int NULL, 
	[Day] varchar(8000) NULL, 
	[Month] varchar(8000) NULL, 
	[ShortMonth] varchar(8000) NULL, 
	[CalendarMonthNumber] int NULL, 
	[CalendarMonthLabel] varchar(8000) NULL, 
	[CalendarYear] int NULL, 
	[CalendarYearLabel] varchar(8000) NULL, 
	[FiscalMonthNumber] int NULL, 
	[FiscalMonthLabel] varchar(8000) NULL, 
	[FiscalYear] int NULL, 
	[FiscalYearLabel] varchar(8000) NULL, 
	[ISOWeekNumber] int NULL
);