with fscmtopmodelam_finextractam_gl as (
  select * from raw.fscmtopmodelam_finextractam_glbiccextractam_fiscalperiodextractpvo),
CT_GL_PERIODS_D_STG as (
  select
    fscmtopmodelam_finextractam_gl.periodperiodyear || right(
      repeat('0', 2) || cast(
        fscmtopmodelam_finextractam_gl.periodperiodnum as string
      ),
      2
    ) as period_id,
    fscmtopmodelam_finextractam_gl.periodperiodname as period_name,
    fscmtopmodelam_finextractam_gl.periodperiodsetname as period_set_name,
    cast(
      fscmtopmodelam_finextractam_gl.periodstartdate as string
    ) as start_dt,
    cast(
      fscmtopmodelam_finextractam_gl.periodenddate as string
    ) as end_dt,
    cast(
      fscmtopmodelam_finextractam_gl.periodyearstartdate as string
    ) as year_start_dt,
    cast(
      fscmtopmodelam_finextractam_gl.periodquarterstartdate as string
    ) as quarter_start_dt,
    fscmtopmodelam_finextractam_gl.periodperiodtype as period_type,
    cast(
      fscmtopmodelam_finextractam_gl.periodperiodyear as int
    ) as period_year,
    cast(
      fscmtopmodelam_finextractam_gl.periodperiodnum as string
    ) as period_num,
    -- In the data we have string values, so changed datatype to string
    cast(
      fscmtopmodelam_finextractam_gl.periodquarternum as string
    ) as quarter_num,
    -- In the data we have string values, so changed datatype to string
    'Q' || fscmtopmodelam_finextractam_gl.periodquarternum || '-' || fscmtopmodelam_finextractam_gl.periodperiodyear as quarter_name,
    fscmtopmodelam_finextractam_gl.periodenteredperiodname as entered_period_name,
    fscmtopmodelam_finextractam_gl.periodadjustmentperiodflag as adjustment_period_flag,
    fscmtopmodelam_finextractam_gl.perioddescription as description,
    fscmtopmodelam_finextractam_gl.periodattribute1 as attribute1,
    fscmtopmodelam_finextractam_gl.periodattribute2 as attribute2,
    fscmtopmodelam_finextractam_gl.periodattribute3 as attribute3,
    fscmtopmodelam_finextractam_gl.periodattribute4 as attribute4,
    fscmtopmodelam_finextractam_gl.periodattribute5 as attribute5,
    fscmtopmodelam_finextractam_gl.periodattribute6 as attribute6,
    fscmtopmodelam_finextractam_gl.periodattribute7 as attribute7,
    fscmtopmodelam_finextractam_gl.periodattribute8 as attribute8,
    cast(
      fscmtopmodelam_finextractam_gl.periodcreationdate as date
    ) as creation_dt,
    fscmtopmodelam_finextractam_gl.periodcreatedby as created_by,
    cast(
      fscmtopmodelam_finextractam_gl.periodlastupdatedate as date
    ) as last_update_dt,
    fscmtopmodelam_finextractam_gl.periodlastupdatedby as last_updated_by,
    fscmtopmodelam_finextractam_gl.periodlastupdatelogin as last_update_login,
    fscmtopmodelam_finextractam_gl.periodperiodname || '~' || fscmtopmodelam_finextractam_gl.periodperiodsetname as integration_id,
    1000 as datasource_num_id,
    fscmtopmodelam_finextractam_gl.periodattributecategory as attribute_category,
    fscmtopmodelam_finextractam_gl.periodobjectversionnumber as object_version_number,
    fscmtopmodelam_finextractam_gl.periodconfirmationstatus as confirmation_status,
    null as enterprise_id,
    fscmtopmodelam_finextractam_gl.periodperiodyear as fiscal_year,
    fscmtopmodelam_finextractam_gl.periodglobalattributecategory as global_attribute_category,
    fscmtopmodelam_finextractam_gl.periodglobalattribute1 as global_attribute1,
    fscmtopmodelam_finextractam_gl.periodglobalattribute2 as global_attribute2,
    fscmtopmodelam_finextractam_gl.periodglobalattribute3 as global_attribute3,
    fscmtopmodelam_finextractam_gl.periodglobalattribute4 as global_attribute4,
    fscmtopmodelam_finextractam_gl.periodglobalattribute5 as global_attribute5,
    fscmtopmodelam_finextractam_gl.periodglobalattribute6 as global_attribute6,
    fscmtopmodelam_finextractam_gl.periodglobalattribute7 as global_attribute7,
    fscmtopmodelam_finextractam_gl.periodglobalattribute8 as global_attribute8,
    fscmtopmodelam_finextractam_gl.periodglobalattribute9 as global_attribute9,
    fscmtopmodelam_finextractam_gl.periodglobalattribute10 as global_attribute10,
    fscmtopmodelam_finextractam_gl.periodglobalattribute11 as global_attribute11,
    fscmtopmodelam_finextractam_gl.periodglobalattribute12 as global_attribute12,
    fscmtopmodelam_finextractam_gl.periodglobalattribute13 as global_attribute13,
    fscmtopmodelam_finextractam_gl.periodglobalattribute14 as global_attribute14,
    fscmtopmodelam_finextractam_gl.periodglobalattribute15 as global_attribute15,
    fscmtopmodelam_finextractam_gl.periodglobalattribute16 as global_attribute16,
    fscmtopmodelam_finextractam_gl.periodglobalattribute17 as global_attribute17,
    fscmtopmodelam_finextractam_gl.periodglobalattribute18 as global_attribute18,
    fscmtopmodelam_finextractam_gl.periodglobalattribute19 as global_attribute19,
    fscmtopmodelam_finextractam_gl.periodglobalattribute20 as global_attribute20,
    fscmtopmodelam_finextractam_gl.periodglobalattributenumber1 as global_attribute_number1,
    fscmtopmodelam_finextractam_gl.periodglobalattributenumber2 as global_attribute_number2,
    fscmtopmodelam_finextractam_gl.periodglobalattributenumber3 as global_attribute_number3,
    fscmtopmodelam_finextractam_gl.periodglobalattributenumber4 as global_attribute_number4,
    fscmtopmodelam_finextractam_gl.periodglobalattributenumber5 as global_attribute_number5,
    fscmtopmodelam_finextractam_gl.periodglobalattributedate1 as global_attribute_date1,
    fscmtopmodelam_finextractam_gl.periodglobalattributedate2 as global_attribute_date2,
    fscmtopmodelam_finextractam_gl.periodglobalattributedate3 as global_attribute_date3,
    fscmtopmodelam_finextractam_gl.periodglobalattributedate4 as global_attribute_date4,
    fscmtopmodelam_finextractam_gl.periodglobalattributedate5 as global_attribute_date5
  from
    fscmtopmodelam_finextractam_gl
)
select
  *
from
  CT_GL_PERIODS_D_STG