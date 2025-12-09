
with raw_data as (
SELECT partid, sku, description, effective_date, [DLR-1],[DLR-6],[DLR-25],[DLR-100],[DLR-250],[DST-1],[EUR-1],[EXP-1],[GLD-1],[GLD-250],[MAP-1],[PLT-1],[PMR-1],[PMR-25],[PMR-100],[PMR-250],[RTL-1],[SIL-1],[SIL-100],[SIL-250]
FROM (
    SELECT
        TRIM(part_id) AS partid,
        recnum AS sku,
        '' AS description,
        effective_date,
        so_unit_price AS price,
        TRIM(price_code) + '-' + CONVERT(VARCHAR(10), order_qty) AS category
    FROM esidemo.dbo.sofpl
) AS src
PIVOT (
    MAX(price) FOR category IN ([DLR-1],[DLR-6],[DLR-25],[DLR-100],[DLR-250],[DST-1],[EUR-1],[EXP-1],[GLD-1],[GLD-250],[MAP-1],[PLT-1],[PMR-1],[PMR-25],[PMR-100],[PMR-250],[RTL-1],[SIL-1],[SIL-100],[SIL-250])
) AS p)
select partid, description, effective_date,
MAX([DLR-1]) as [DLR-1],
MAX([DLR-6]) as [DLR-6],
MAX([DLR-25]) as [DLR-25],
MAX([DLR-100]) as [DLR-100],
MAX([DLR-250]) as [DLR-250],
MAX([DST-1]) as [DST-1],
MAX([EUR-1]) as [EUR-1],
MAX([EXP-1]) as [EXP-1],
MAX([GLD-1]) as [GLD-1],
MAX([GLD-250]) as [GLD-250],
MAX([MAP-1]) as [MAP-1],
MAX([PLT-1]) as [PLT-1],
MAX([PMR-1]) as [PMR-1],
MAX([PMR-25]) as [PMR-25],
MAX([PMR-100]) as [PMR-100],
MAX([PMR-250]) as [PMR-250],
MAX([RTL-1]) as [RTL-1],
MAX([SIL-1]) as [SIL-1],
MAX([SIL-100]) as [SIL-100],
MAX([SIL-250]) as [SIL-250]
from raw_data
where effective_date = (select max(effective_date) from raw_data rd2 where rd2.partid = raw_data.partid)
group by partid, description, effective_date
order by partid;