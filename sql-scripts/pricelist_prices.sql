USE [ESIDB]
GO

/****** Object:  View [dbo].[pricelist_prices]    Script Date: 11/19/2025 12:21:36 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER view [dbo].[pricelist_prices] as
select trim(part_id) as partid, trim(price_code) as catid, recnum as sku,
       '' as description, so_unit_price as price,
       curr_code as currency, effective_date, ORDER_QTY as qty
    from esidb.dbo.sofpl;
GO