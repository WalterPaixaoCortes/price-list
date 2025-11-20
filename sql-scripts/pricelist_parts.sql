USE [ESIDB]
GO

/****** Object:  View [dbo].[pricelist_parts]    Script Date: 11/19/2025 12:27:03 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER view [dbo].[pricelist_parts] as
select distinct trim(part_id) as id, '' as label
from esidb.dbo.sofpl
union 
select 'ALL' as id, '' as label;
GO

