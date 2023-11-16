if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    #Translate Column Names
    Translate_Column = {
        'จังหวัด':'Province',
        'กลุ่มจังหวัด':'BusinessRegion',
        'ภาคธุรกิจ':'BusinessSector',
        'รหัสประเภทธุรกิจ(TSIC-2dg)':'BusinessTypeCode_2dg',
        'รายละเอียดประเภทธุรกิจ(TSIC-2dg)':'BusinessTypeDescription_2dg',
        'รหัสประเภทธุรกิจ(TSIC-5dg)':'BusinessTypeCode_5dg',
        'รายละเอียดประเภทธุรกิจ(TSIC-5dg)':'BusinessTypeDescription_5dg',
        'ขนาดธุรกิจ':'BusinessSize',
        'จำนวนผู้ประกอบการ':'NumberOfEntrepreneurs',
        'ประเภทของข้อมูล':'TypeOfData',
        'ปี':'Year'
    }

    df.rename(columns=Translate_Column, inplace=True)

    #Check NaN
    df[df.isna().any(axis=1)]

    #Delete row that have NaN
    df.drop(20322, inplace=True)

    #Add Index for Dataframe
    df['smeID'] = df.index.astype('int')

    #Create Dimension Tables
    province_dim = df[['Province']].drop_duplicates().reset_index(drop=True)
    province_dim['ProvinceID'] = province_dim.index.astype('int')
    business_region_dim = df[['BusinessRegion']].sort_values(by=['BusinessRegion']).drop_duplicates().reset_index(drop=True)
    business_region_dim['BusinessRegionID'] = business_region_dim.index.astype('int')
    business_sector_dim = df[['BusinessSector']].drop_duplicates().reset_index(drop=True)
    business_sector_dim['BusinessSectorID'] = business_sector_dim.index.astype('int')
    business_type2dg_dim = df[['BusinessTypeDescription_2dg']].drop_duplicates()
    business_type2dg_dim['BusinessTypeCode_2dg'] = df['BusinessTypeCode_2dg'].astype('int')
    business_type5dg_dim = df[['BusinessTypeDescription_5dg']].drop_duplicates()
    business_type5dg_dim['BusinessTypeCode_5dg'] = df['BusinessTypeCode_5dg'].astype('int')
    
    #found some error word is the same but type in lowercase
    #fix make "Micro" to "MICRO" in dataframe
    df['BusinessSize'] = df['BusinessSize'].replace("Micro","MICRO")
    
    business_size_dim = df[['BusinessSize']].drop_duplicates().reset_index(drop=True)
    business_size_dim['BusinessSizeID'] = business_size_dim.index.astype('int')
    type_data_dim = df[['TypeOfData']].drop_duplicates().reset_index(drop=True)
    type_data_dim['TypeOfDataID'] = type_data_dim.index.astype('int')

    # Map dimension IDs to the fact table
    df['ProvinceID'] = df['Province'].map(province_dim.set_index('Province')['ProvinceID'])
    df['BusinessRegionID'] = df['BusinessRegion'].map(business_region_dim.set_index('BusinessRegion')['BusinessRegionID'])
    df['BusinessSectorID'] = df['BusinessSector'].map(business_sector_dim.set_index('BusinessSector')['BusinessSectorID'])
    df['BusinessTypeCode_2dg'] = df['BusinessTypeDescription_2dg'].map(business_type2dg_dim.set_index('BusinessTypeDescription_2dg')['BusinessTypeCode_2dg'])
    df['BusinessTypeCode_5dg'] = df['BusinessTypeDescription_5dg'].map(business_type5dg_dim.set_index('BusinessTypeDescription_5dg')['BusinessTypeCode_5dg'])
    df['BusinessSizeID'] = df['BusinessSize'].map(business_size_dim.set_index('BusinessSize')['BusinessSizeID'])
    df['TypeOfDataID'] = df['TypeOfData'].map(type_data_dim.set_index('TypeOfData')['TypeOfDataID'])

    # Create the fact table
    fact_table = df[['smeID','ProvinceID', 'BusinessRegionID', 'BusinessSectorID', 'BusinessTypeCode_2dg', 'BusinessTypeCode_5dg', 'BusinessSizeID', 'TypeOfDataID', 'NumberOfEntrepreneurs', 'Year']]

    return {
        "province_dim":province_dim.to_dict(orient="dict"),
        "business_region_dim":business_region_dim.to_dict(orient="dict"),
        "business_sector_dim":business_sector_dim.to_dict(orient="dict"),
        "business_type2dg_dim":business_type2dg_dim.to_dict(orient="dict"),
        "business_type5dg_dim":business_type5dg_dim.to_dict(orient="dict"),
        "business_size_dim":business_size_dim.to_dict(orient="dict"),
        "type_data_dim":type_data_dim.to_dict(orient="dict"),
        "fact_table":fact_table.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
