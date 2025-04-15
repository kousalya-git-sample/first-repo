# Databricks notebook source
core_column_dict = {
 'id': {'datatype': 'string', 'rename': 'id'},
 'companyidentifier': {'datatype': 'string', 'rename': 'companyidentifier'},
 'sourceentityidentifier': {'datatype': 'string', 'rename': 'sourceemployeeidentifier'},
 'companyname': {'datatype': 'string', 'rename': 'companyname'},
 'costcenterdesc': {'datatype': 'string', 'rename': 'costcenterdesc'},
 'emailaddressdesc': {'datatype': 'string', 'rename': 'emailaddressdesc'},
 'employeestatusname': {'datatype': 'string', 'rename': 'employeestatusname'},
 'employeetypedesc': {'datatype': 'string', 'rename': 'employeetypedesc'},
 'employeefirstname': {'datatype': 'string', 'rename': 'employeefirstname'},
 'functioncode': {'datatype': 'string', 'rename': 'functioncode'},
 'gradeleveldesc': {'datatype': 'string', 'rename': 'gradeleveldesc'},
 'groupcode': {'datatype': 'string', 'rename': 'groupcode'},
 'employeelastname': {'datatype': 'string', 'rename': 'employeelastname'},
 'mailstopdesc': {'datatype': 'string', 'rename': 'mailstopdesc'},
 'employeemanualrecord': {'datatype': 'string', 'rename': 'employeemanualrecord'},
 'employeemanagerid': {'datatype': 'string', 'rename': 'employeemanagerid'},
 'employeemiddlename': {'datatype': 'string', 'rename': 'employeemiddlename'},
 'employeeoccupiedtext': {'datatype': 'string', 'rename': 'employeeoccupiedtext'},
 'employeephonenumber': {'datatype': 'string', 'rename': 'employeephonenumber'},
 'placeofserviceid': {'datatype': 'string', 'rename': 'placeofserviceid'},
 'employeetitlename': {'datatype': 'string', 'rename': 'employeetitlename'},
 'employeeabbreviationname': {'datatype': 'string', 'rename': 'employeeabbreviationname'},
 'badgenumber': {'datatype': 'string', 'rename': 'badgenumber'},
 'employeeaddeddatetime': {'datatype': 'string', 'rename': 'employeeaddeddatetime'},
 'employeedisplayname': {'datatype': 'string', 'rename': 'employeedisplayname'},
 'employeehiredatetime': {'datatype': 'string', 'rename': 'employeehiredatetime'},
 'hirelocationdesc': {'datatype': 'string', 'rename': 'hirelocationdesc'},
 'isserviceproviderflag': {'datatype': 'string', 'rename': 'isserviceproviderflag'},
 'employeeprimarylocationname': {'datatype': 'string', 'rename': 'employeeprimarylocationname'},
 'spacestandardtext': {'datatype': 'string', 'rename': 'spacestandardtext'},
 'employeespecialneedsdesc': {'datatype': 'string', 'rename': 'employeespecialneedsdesc'},
 'spacestandardidentifier': {'datatype': 'string', 'rename': 'spacestandardidentifier'},
 'employeeterminationdatetime': {'datatype': 'datetime', 'rename': 'employeeterminationdatetime'},
 'employeerecordupdatedatetime': {'datatype': 'datetime', 'rename': 'employeerecordupdatedatetime'},
 'employeeuserid': {'datatype': 'string', 'rename': 'employeeuserid'},
 'employeedescription': {'datatype': 'string', 'rename': 'employeedescription'},
 'clientuserdefinedtext04': {'datatype': 'string', 'rename': 'clientuserdefinedtext04'},
 'sourcecreatedby': {'datatype': 'string', 'rename': 'sourcecreatedby'},
 'sourcecreateddatetime': {'datatype': 'datetime', 'rename': 'sourcecreateddatetime'},
 'sourcemodifiedby': {'datatype': 'string', 'rename': 'sourcemodifiedby'},
 'sourcemodifieddatetime': {'datatype': 'datetime', 'rename': 'sourcemodifieddatetime'},
 'sourcesystem': {'datatype': 'string', 'rename': 'sourcesystem'},
 'udp_create_ts': {'datatype': 'datetime', 'rename': 'udp_create_ts'},
 'udp_update_ts': {'datatype': 'datetime', 'rename': 'udp_update_ts'},
 'udp_delete_flag': {'datatype': 'string', 'rename': 'udp_delete_flag'}
}