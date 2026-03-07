CREATE TABLE [wks].[dimension_stock_item] (

	[StockItemKey] int NULL, 
	[WWIStockItemID] int NULL, 
	[StockItem] varchar(8000) NULL, 
	[Color] varchar(8000) NULL, 
	[SellingPackage] varchar(8000) NULL, 
	[BuyingPackage] varchar(8000) NULL, 
	[Brand] varchar(8000) NULL, 
	[Size] varchar(8000) NULL, 
	[LeadTimeDays] int NULL, 
	[QuantityPerOuter] int NULL, 
	[IsChillerStock] bit NULL, 
	[Barcode] varchar(8000) NULL, 
	[TaxRate] decimal(18,3) NULL, 
	[UnitPrice] decimal(18,2) NULL, 
	[RecommendedRetailPrice] decimal(18,2) NULL, 
	[TypicalWeightPerUnit] decimal(18,3) NULL, 
	[Photo] varbinary(8000) NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL, 
	[LineageKey] int NULL
);