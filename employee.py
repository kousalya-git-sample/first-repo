from wd.handler.importer import *
from wd.jobs.config.employee_config import *

import sys

configure_spark(spark, dbutils)

job_mode = sys.argv[1]
# job_mode = 'full'

input_files = dict(
            file1 = dict(partition_key = 'fms-employees', layer = 'snapshot', path = None, df_type = 'delta', view_name = 'employees', mode = job_mode, is_primary = 'true')
         )
_job_layers = {'bronze': core_column_dict}

_JOB_CONFIG = dict(
    input_file = input_files,
    confidentiality = 'confidential',
    entity = 'externalemployee',
    region = 'global',
    source = 'fms'
)

def max_date():
    initial_df = spark.sql("""
        select max(to_date(udp_update_ts)) max_udp_update_ts from employees
 """)
    return initial_df

def mapping(max_udp_update_ts):
 initial_df = spark.sql(f"""
 select
 concat(trim(C_ID), trim(EMPID), 'fms') id,
 concat(trim(C_ID), '|fms') companyidentifier,
 trim(EMPID) sourceentityidentifier,
 trim(COMPANY) companyname,
 trim(COSTCENTER) costcenterdesc,
 trim(EMAIL) emailaddressdesc,
 case when trim(EMPSTAT) = 'ACTIVE' then 1 else 0 end employeestatusname,--case when active then 1 else 0
 trim(EMPTYPE) employeetypedesc,
 trim(FNAME) employeefirstname,
 trim(FNCODE) functioncode,
 trim(GRADELEVEL) gradeleveldesc,
 trim(GROUP_) groupcode,
 trim(LNAME) employeelastname,
 trim(MAILSTOP) mailstopdesc,
 trim(MAN_MNG) employeemanualrecord,
 trim(MANAGERID) employeemanagerid,--Circular Reference to Employee record
 trim(MNAME) employeemiddlename,
 trim(OCC1) employeeoccupiedtext,
 trim(PHONE) employeephonenumber,
 trim(POS_ID) placeofserviceid,
 trim(TITLE) employeetitlename,
 trim(ABBR_NAME) employeeabbreviationname,
 trim(BADGEID) badgenumber, 
 DATEADDED employeeaddeddatetime,
 trim(DISP_NAME) employeedisplayname,
 HDATE employeehiredatetime,
 trim(HRLOCDESC) hirelocationdesc,
 ISSVCPRVDR isserviceproviderflag,
 trim(PRIMARYLOC) employeeprimarylocationname,
 trim(SPACESTD) spacestandardtext,
 trim(SPCLNEEDS) employeespecialneedsdesc,
 trim(SPCODE) spacestandardidentifier,--FK to Space Standard table
 TDATE employeeterminationdatetime,
 UDATE employeerecordupdatedatetime,
 trim(USER_ID) employeeuserid,
 trim(WORKTYPE) employeedescription,
trim(C_TEXT04) clientuserdefinedtext04,
 CREATEDBY sourcecreatedby,
 CREATEDON sourcecreateddatetime,
 MODIFIEDBY sourcemodifiedby,
 MODIFIEDON sourcemodifieddatetime,
 'fms' sourcesystem,
 udp_create_ts,
 udp_update_ts,
 case when to_date(udp_update_ts) < to_date('{max_udp_update_ts}') then "Y" else "N" end udp_delete_flag
  from employees

 """)

 return initial_df


if _name_ == "_main_":
 jobs.JOB_CONFIG = _JOB_CONFIG
 jobs.job_layers = _job_layers

 jobs.pre_process(dbutils, job_mode)

 max_udp_update_ts = max_date().collect()[0][0]

 initial_df = mapping(max_udp_update_ts)

 bronze_df = jobs.bronze_layer_processing(df = initial_df)

 jobs.silver_layer_processing(df = bronze_df)

 dbh = DatabricksHandler(dbutils)

 dbh.create_external_table(dbutils, "silver_global", "externalemployee", "/global/confidential/externalemployee/wrkdnc/silver_layer/externalemployee/")