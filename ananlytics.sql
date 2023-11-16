CREATE OR REPLACE TABLE `project-purin.project_sme_thai_2565.analytics` AS
SELECT 
f.smeID,
p.Province,
rg.BusinessRegion,
st.BusinessSector,
dg2.BusinessTypeCode_2dg,
dg2.BusinessTypeDescription_2dg,
dg5.BusinessTypeCode_5dg,
dg5.BusinessTypeDescription_5dg,
sz.BusinessSize,
ty.TypeOfData
FROM 
`project-purin.project_sme_thai_2565.fact_table` f
JOIN `project-purin.project_sme_thai_2565.province_dim` p
ON f.ProvinceID = p.ProvinceID
JOIN `project-purin.project_sme_thai_2565.business_region_dim` rg
ON f.BusinessRegionID = rg.BusinessRegionID
JOIN `project-purin.project_sme_thai_2565.business_sector_dim` st
ON f.BusinessSectorID = st.BusinessSectorID
JOIN `project-purin.project_sme_thai_2565.business_type2dg_dim` dg2
ON f.BusinessTypeCode_2dg = dg2.BusinessTypeCode_2dg
JOIN `project-purin.project_sme_thai_2565.business_type5dg_dim` dg5
ON f.BusinessTypeCode_5dg = dg5.BusinessTypeCode_5dg
JOIN `project-purin.project_sme_thai_2565.business_size_dim` sz
ON f.BusinessSizeID = sz.BusinessSizeID
JOIN `project-purin.project_sme_thai_2565.type_data_dim` ty
ON f.TypeOfDataID = ty.TypeOfDataID
ORDER BY smeID ASC
;