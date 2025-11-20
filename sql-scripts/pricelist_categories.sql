USE [ESIDB]
GO

/****** Object:  View [dbo].[pricelist_categories]    Script Date: 11/19/2025 12:26:07 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER view [dbo].[pricelist_categories] as
select distinct trim(price_code) as id, '' as name
from esidb.dbo.sofpl;
GO