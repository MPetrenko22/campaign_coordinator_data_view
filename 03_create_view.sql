CREATE VIEW  app_lv.campaign_coordinator_data_view AS (
SELECT
     REP.cid
   , REP.list_id
   , REP.data_mapped_sheet_id
   , REP.contact_id
   , REP.column_id
   , REP.attribute
   , case when FL.field is not null and (REP.field is null or REP.field = '') then FL.field else REP.field end field /*Fill field from dictionary (FL)*/
   , REP.value
   , REP.column_type
   , REP.updated_at
   FROM
     (
      SELECT
        RAW_REP.cid
      , RAW_REP.list_id
      , RAW_REP.data_mapped_sheet_id
      , RAW_REP.contact_id
      , RAW_REP.column_id
      , RAW_REP.attribute
      , RAW_REP.field
      , RAW_REP.value
      , RAW_REP.column_type
      , RAW_REP.updated_at
      , ROW_NUMBER() OVER (PARTITION BY RAW_REP.contact_id, RAW_REP.column_id ORDER BY RAW_REP.rtm_id DESC) RN
      FROM
        (
         SELECT
           ca.cid
         , l.id list_id
         , lr.data_mapped_sheet_id
         , r.row_id contact_id
         , dmc.column_id
         , dmc.title attribute
         , pf.field
         , REPLACE(CAST(JSON_EXTRACT(r.cells, CONCAT('$.', CAST(dmc.column_id AS varchar(20)), '.value')) AS varchar(255)), '"', '') value
         , CAST(JSON_EXTRACT(dmc.properties, '$.reportHeading.type') AS varchar(255)) column_type
         , pf.id rtm_id
         , r.updated_at 
         FROM
           (((((app_lv.campaigns ca
         INNER JOIN app_lv.lists l ON ((l.campaign_id = ca.id) AND (l.local_type = 4)))
         INNER JOIN app_lv.list_report_data_mapped lr ON (lr.list_id = l.id))
         INNER JOIN app_lv.data_mapped_columns dmc ON (dmc.sheet_id = lr.data_mapped_sheet_id))
         INNER JOIN app_lv.data_mapped_rows r ON (r.sheet_id = lr.data_mapped_sheet_id))
         LEFT JOIN (
            SELECT
              rtm1.campaign_id
            , par.parameter
            , f.field
            , rtm1.id
            FROM
              ((app_lv.campaign_template_columns rtm1
            INNER JOIN app_lv.campaign_template_parameters par ON (par.id = rtm1.parameter_id))
            INNER JOIN app_lv.campaign_template_fields f ON (f.id = rtm1.field_id))
         )  pf ON ((pf.campaign_id = ca.id) AND (pf.parameter = dmc.title)))
         WHERE ((r.row_id > 0) AND ((NOT (pf.field IN ('first_name', 'last_name', 'phone', 'email', 'address', 'city', 'zip'))) OR (pf.field IS NULL)) AND (NOT (dmc.title LIKE 'phone%')) AND (NOT (dmc.title LIKE 'mobile%')) AND (NOT (dmc.title LIKE 'email%')) AND (NOT (dmc.title LIKE 'address%')) AND (NOT (dmc.title LIKE 'city%')) AND (NOT (dmc.title LIKE 'post%code%')))
      )  RAW_REP
   )  REP
   LEFT JOIN power_bi_cc_field_mapping FL ON LOWER(FL.attribute) = LOWER(REP.attribute) /*Add dictionary to view query */
   WHERE (REP.RN = 1) 
);
