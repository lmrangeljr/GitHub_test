/*******************************************************************************************************************
* PROJECT:      ADS TRAINING
* DEVELOPER:    LUIS MANUEL RANGEL JR
* TABLE:        A_VISITS
* DATE:         02/20/2023
*******************************************************************************************************************/
/*******************************************************************************************************************
* NECESSARY SQL PARAMETERS FOR STARTING A PROJECT
*******************************************************************************************************************/
-- thses are things that will be required below.
-- THIS WILL TELL THE COMPUTER WHO CAN READ/WRITE TO THE DIRECTORY.
use role research;

-- THIS IS THE COMPUTER POWER;
use warehouse query_wh_large;

-- THIS IS SO i DON'T NEED TO BE TYPING THIS MULTIPLE TIMES BUT I AM STILL GOING TO DO THAT.
use database delivered_211;

/*******************************************************************************************************************
* THESE ARE THE VISITS FOR THE FOLLOWING:
********************************************************************************************************************
* Encounter Type:
* Outpatient
* ED
********************************************************************************************************************
* Provider Specialty:
* Pulmonologist              boc002384
* Allergist                  boc002358
* Otolaryngologist           boc002359
********************************************************************************************************************
* CRS                        boc004248       Chronic sinusitis (NCQA value set)
* NP                         boc004265       Nasal Polyps (ECRI plus value set)
*******************************************************************************************************************/
--CREATE OR REPLACE TEMPORARY TABLE PT1 AS
CREATE OR REPLACE TABLE RESEARCH.GSK_CRSWNP.A_VISITS_LMRJR AS
WITH CUIS AS (SELECT DISTINCT A.PROVIDER_ID
                                        ,b.SPECIALTY_CUI
                        FROM DELIVERED_211.profile_store.provider_specialty AS A
                        INNER JOIN DELIVERED_211.profile_store.specialty AS B ON A.specialty_id = B.specialty_id
                        WHERE b.SPECIALTY_CUI IN('boc002384','boc002358','boc002359'))
,DX_CUIS as (select distinct a.code_type as code_type_cui
                                        ,A.mapped_diagnosis_code
                                        ,A.boc_cui
                                        ,A.boc_name
                                        ,B.boc_name as code_type_name
                        FROM MAPSET_20231004.public.boc_map_diagnosis_rollup a 
                        INNER JOIN MAPSET_20231004.public.boc_map_diagnosis_code_type b on a.code_type = b.boc_cui
                        WHERE a.boc_cui in('boc004248','boc004265'))
,ALL_DX as (SELECT DISTINCT MAIN.PATIENT_ID
                                        ,A.ENCOUNTER_ID
                                        ,TO_DATE(A.DIAGNOSIS_DATE) AS DX_DATE
                                        ,B.boc_cui
                        FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
                        INNER JOIN DELIVERED_211.profile_store.diagnosis AS A ON MAIN.PATIENT_ID=A.PATIENT_ID
                        INNER JOIN DX_CUIS b ON (trim(lower(a.code)) = trim(lower(b.mapped_diagnosis_code)) AND trim(lower(a.code_type_name)) = trim(lower(b.code_type_name))))
,ENC_DATA AS (SELECT MAIN.PATIENT_ID
                                        ,BASE.ENCOUNTER_ID
                                        ,PROV.PROVIDER_ID
                                        ,MM.ENCOUNTER_TYPE
                                        ,TO_DATE(MM.START_DATE) AS ENC_DATE
                        FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
                        JOIN DELIVERED_211.PROFILE_STORE.CLINICAL_ENCOUNTER AS MM ON MAIN.PATIENT_ID=MM.PATIENT_ID
                        JOIN DELIVERED_211.PROFILE_STORE.CLINICAL_ENCOUNTER_LINK AS BASE ON MAIN.PATIENT_ID=BASE.PATIENT_ID AND MM.CLINICAL_ENCOUNTER_ID=BASE.CLINICAL_ENCOUNTER_ID
                        JOIN DELIVERED_211.PROFILE_STORE.ENCOUNTER_PROVIDER AS PROV ON BASE.ENCOUNTER_ID=PROV.ENCOUNTER_ID
                        WHERE TO_DATE(MM.START_DATE) BETWEEN DATEADD(DAY, -12*30.4375, MAIN.INDEX_DATE) AND DATEADD(DAY, 6*30.4375, (DATEADD(DAY, 1, MAIN.INDEX_DATE)))
                                        AND LOWER(MM.ENCOUNTER_TYPE) IN('outpatient','ed'))
,TT AS (SELECT DISTINCT ED.PATIENT_ID
                                ,ED.ENC_DATE
                                ,CAT.SPECIALTY_CUI
                                ,DXD.BOC_CUI
                                ,DX_DATE
                                ,ENCOUNTER_TYPE
                                ,case when ABS(DATEDIFF(DAY, DXD.DX_DATE, ED.ENC_DATE)) <= 5 then 1 else 0 end as within5days
                  FROM ENC_DATA AS ED
                  LEFT JOIN CUIS AS CAT ON ED.PROVIDER_ID=CAT.PROVIDER_ID
                  LEFT JOIN ALL_DX AS DXD ON ED.PATIENT_ID=DXD.PATIENT_ID)
SELECT DISTINCT MAIN.PATIENT_ID
                ,MAIN.INDEX_DATE
                ,CASE
                                WHEN BASE.ENC_DATE IS NOT NULL AND (DATEADD(DAY, -12*30.4375, MAIN.INDEX_DATE) <= BASE.ENC_DATE AND BASE.ENC_DATE < DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE)) THEN 'BL_12MO'
                                WHEN BASE.ENC_DATE IS NOT NULL AND (DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE) <= BASE.ENC_DATE AND BASE.ENC_DATE <= MAIN.INDEX_DATE) THEN 'BL_6MO'
                                WHEN BASE.ENC_DATE IS NOT NULL AND (MAIN.INDEX_DATE < BASE.ENC_DATE AND BASE.ENC_DATE <= DATEADD(DAY, 6*30.4375, (DATEADD(DAY, 1, MAIN.INDEX_DATE)))) THEN 'FU_6MO'
                                ELSE 'NO DATA'
                END AS RUN_ID
        ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' THEN ENC_DATE ELSE null END) AS OP_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'ed' THEN ENC_DATE ELSE null END) AS ED_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and BASE.SPECIALTY_CUI = 'boc002384' THEN ENC_DATE ELSE null END) AS PLMG_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and BASE.SPECIALTY_CUI = 'boc002358' THEN ENC_DATE ELSE null END) AS ALGY_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and BASE.SPECIALTY_CUI = 'boc002359' THEN ENC_DATE ELSE null END) AS ORL_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and BASE.SPECIALTY_CUI = 'boc002384' and within5days = 1 THEN ENC_DATE ELSE null END) AS CRSwNP_PLMG_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and BASE.SPECIALTY_CUI = 'boc002358' and within5days = 1 THEN ENC_DATE ELSE null END) AS CRSwNP_ALGY_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and BASE.SPECIALTY_CUI = 'boc002359' and within5days = 1 THEN ENC_DATE ELSE null END) AS CRSwNP_ORL_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'outpatient' and within5days = 1 THEN ENC_DATE ELSE null END) AS CRSwNP_OP_visit
                ,count(distinct CASE WHEN LOWER(BASE.ENCOUNTER_TYPE) = 'ed' and within5days = 1 THEN ENC_DATE ELSE null END) AS CRSwNP_ED_visit
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
LEFT JOIN TT AS BASE ON MAIN.PATIENT_ID=BASE.PATIENT_ID
GROUP BY 1,2,3;