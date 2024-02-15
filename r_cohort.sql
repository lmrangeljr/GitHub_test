/*******************************************************************************************************************
* PROJECT:                 ADS TRAINING
* DEVELOPER:    LUIS MANUEL RANGEL JR
* TABLE:                R_COHORT
* DATE:                 11/22/2023
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
* COHORT TABLE
* WHAT WE ARE DOING IS CREATING THE COHORT TABLE. 
* I NEED PATIENTS THAT HAVE
*       A MEPOLIZUMAB INITITATION ON OR AFTER THE 29TH OF JULY 2021. tHIS WILL BE MY INDEX DATE
*       AGE >= 18 ON INDEX DATE
*       EMR AND CLAIMS BETWEEN (iNDEX DATE - 12MO) AND (INDEX DATE + 6MO)
*               AND HAVE EMR SPECIALIST ENCOUNTER DURRING (INDEX DATE -12MO)
*       AT LEAST ONE CRS-associated NP DIAGNOSIS CODE DURING THE 12 MONTHS PRIOR TO OR ON THE INDEX DATE
* WHAT i WANT IN THIS TABLE
* PATIENT ID, INDEX DATE, AGE, lINKED DATA(EMR + CLAIMS), SPECIALIST FLAG, CRS-FLAG
*******************************************************************************************************************/
-- THIS IS A TABLE OF MEPOLIZUMAB MEDICATION USAGE. 
-- WHY DON'T I REDUCE THE DATA BY JULY 29TH 2021... THIS IS BECAUSE THE DATA I AM DELETING MIGHT BE THE MINIMUM
-- MEDICATION START OF ONE PERSON THUS THIS WOULD AFFECT THE STUDY.
-- LIST OF ALL MEPO PATIENTS AND ALL MEDICATIONS OF MEPOLIZUMAB PER PATIENTS
CREATE OR REPLACE TEMPORARY TABLE ID1 as
WITH MED_CODES AS (
                SELECT A.CODE_TYPE,
                           A.MAPPED_MEDICATION_CODE,
                           A.BOC_CUI,
                           A.BOC_NAME
                FROM MAPSET_20231004.PUBLIC.BOC_MAP_MEDICATION_ROLLUP AS A
                WHERE A.BOC_CUI IN('boc005245'))
SELECT DISTINCT ME.PATIENT_ID
                                ,ME.MEDICATION_START AS MEPO_DATE
                                ,DENSE_RANK() OVER (PARTITION BY ME.PATIENT_ID ORDER BY ME.MEDICATION_START) AS RANKING
FROM DELIVERED_211.PROFILE_STORE.MEDICATION_EVENT AS ME
INNER JOIN DELIVERED_211.PROFILE_STORE.MEDICATION_INFO AS B ON ME.MEDICATION_INFO_ID=B.MEDICATION_INFO_ID
INNER JOIN MED_CODES AS C ON TRIM(LOWER(B.CODE))=TRIM(LOWER(C.MAPPED_MEDICATION_CODE)) 
                AND TRIM(LOWER(C.CODE_TYPE))=TRIM(LOWER(B.CODE_TYPE))
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE INDEX_DTS AS
SELECT MAIN.*
FROM ID1 AS MAIN
WHERE MAIN.RANKING = 1;

-- DISTINCT PATIENTS & ROWS 39,609
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM INDEX_DTS;

/*******************************************************************************************************************
-- CREATING THE BASELINES AND FOLLOW UP LIMITS... THIS IS ALSO A SAFE PLACE TO REDUCE THE DATA BY JULY DATE AND AGE.
-- NEXT QUESTION: DO THESE PEOPLE HAVE EMR AND CLAIMS DATA BETWEEN THOSE BL AND FU DATE LIMITS?
*******************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE MEPO_DATES AS
SELECT DISTINCT A.PATIENT_ID
                ,A.MEPO_DATE AS INDEX_DATE
                ,B.YEAR_OF_BIRTH
                ,DENSE_RANK() OVER (PARTITION BY A.PATIENT_ID ORDER BY A.MEPO_DATE) AS RANKING
                ,TO_DATE('2020-07-29') AS STUDY_START
                ,TO_DATE('2023-06-30') AS STUDY_END
                ,TO_DATE('2021-07-29') AS START_PAT_ID
                ,TO_DATE('2022-12-31') AS END_PAT_ID
                ,DATEADD(DAY, -12*30.4375, A.MEPO_DATE) AS BL_FULL_12MO
                ,DATEADD(DAY, -6*30.4375, A.MEPO_DATE) AS BL_FULL_6MO
                ,DATEADD(DAY, 6*30.4375, (DATEADD(DAY, -1, A.MEPO_DATE))) AS FU_6MO
                ,DATEADD(DAY, -12*30.4375, (DATEADD(DAY, -1, A.MEPO_DATE))) AS BL_12MO
                ,DATEADD(DAY, -6*30.4375, (DATEADD(DAY, -1, A.MEPO_DATE))) AS BL_6MO
FROM INDEX_DTS AS A
LEFT JOIN DELIVERED_211.PROFILE_STORE.PATIENT AS B ON A.PATIENT_ID=B.PATIENT_ID
WHERE A.MEPO_DATE >= START_PAT_ID
GROUP BY 1, 2, 3;

-- ROWS & DISTINCT PATIETNS 14031
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM MEPO_DATES;

CREATE OR REPLACE TEMPORARY TABLE TIME_INFO AS
SELECT MAIN.*
FROM MEPO_DATES AS MAIN
WHERE MAIN.RANKING = 1;

-- ROWS & DISTINCT PATIETNS 14031
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM TIME_INFO;

/*******************************************************************************************************************
-- FINDING OUT IF THE PEOPLE HAVE EMR AND CLAIMS DATA WITHIN THE STUDY LIMITS
-- NEXT QUESTION: IS THERE AT LEAST ONE CRS_ASSOCIATED NP DIAGNOSIS DURING THE BL PERIOD.
*******************************************************************************************************************/
-- A DATA PULL OF ENCOUNTERS
CREATE OR REPLACE TEMPORARY TABLE PT1 AS
SELECT MAIN.PATIENT_ID
        ,ENC.ENCOUNTER_START
        ,ENC.DATA_SOURCE_TYPE_ID
FROM TIME_INFO AS MAIN
LEFT JOIN DELIVERED_211.PROFILE_STORE.ENCOUNTER AS ENC ON MAIN.PATIENT_ID=ENC.PATIENT_ID
WHERE ENC.DATA_SOURCE_TYPE_ID IN(0, 1);
        /*AND (ENC.ENCOUNTER_START >= MAIN.BL_FULL_12MO AND ENC.ENCOUNTER_START <= MAIN.FU_6MO);*/

-- ENCOUNTER REQUIRMENTS FOR BOTH EMR AND CLAIMS AS WELL AS THE SPECIALIST
CREATE OR REPLACE TEMPORARY TABLE LINKED_FLAGS AS
SELECT MAIN.PATIENT_ID
                ,MAIN.INDEX_DATE
                ,MAIN.YEAR_OF_BIRTH
                ,MAIN.STUDY_START
                ,MAIN.STUDY_END
                ,MAIN.START_PAT_ID
                ,MAIN.END_PAT_ID
                ,MAIN.BL_FULL_12MO
                ,MAIN.BL_FULL_6MO
                ,MAIN.FU_6MO
                ,MAIN.BL_12MO
                ,MAIN.BL_6MO
                /*THIS AGGREGATE FUNCTION IS DEFINED TO FIND VALUES GREATER THAN THE LIMITS OF -12MO AND +6MO FROM INDEX. THUS ANYONE WITH A VALUE OF ONE SHOULD HAVE BL & FU DURATION OF 12 AND 6*/
                ,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 0 AND (SPEC.ENCOUNTER_START <= MAIN.BL_FULL_12MO) THEN 1 ELSE 0 END) AS BL_EMR
                ,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 0 AND (MAIN.FU_6MO <= SPEC.ENCOUNTER_START) THEN 1 ELSE 0 END) AS FU_EMR
                ,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 1 AND (SPEC.ENCOUNTER_START <= MAIN.BL_FULL_12MO) THEN 1 ELSE 0 END) AS BL_CLM
                ,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 1 AND (MAIN.FU_6MO <= SPEC.ENCOUNTER_START) THEN 1 ELSE 0 END) AS FU_CLM
                /*,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 0 AND (MAIN.BL_FULL_12MO <= SPEC.ENCOUNTER_START AND SPEC.ENCOUNTER_START <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_EMR*/
                /*,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 0 AND (MAIN.INDEX_DATE <= SPEC.ENCOUNTER_START AND SPEC.ENCOUNTER_START <= MAIN.FU_6MO) THEN 1 ELSE 0 END) AS FU_EMR*/
                /*,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 1 AND (MAIN.BL_FULL_12MO <= SPEC.ENCOUNTER_START AND SPEC.ENCOUNTER_START <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_CLM*/
                /*,SUM(CASE WHEN SPEC.DATA_SOURCE_TYPE_ID = 1 AND (MAIN.INDEX_DATE <= SPEC.ENCOUNTER_START AND SPEC.ENCOUNTER_START <= MAIN.FU_6MO) THEN 1 ELSE 0 END) AS FU_CLM*/
FROM TIME_INFO AS MAIN
LEFT JOIN PT1 AS SPEC ON MAIN.PATIENT_ID=SPEC.PATIENT_ID
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;

-- ROWS & DISTINCT PATIETNS 14031
SELECT COUNT(1) AS ROW_CNT
                                ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM LINKED_FLAGS;

-- DTA PULL FOR SPECIALITY EMR DATA REQUIRMENT
CREATE OR REPLACE TEMPORARY TABLE PT1 AS
SELECT MAIN.PATIENT_ID
        ,MAIN.DATA_SOURCE_ID
        ,MAIN.DATA_SOURCE_TYPE_ID
        ,MAIN.DATA_SOURCE_TYPE
        ,TO_DATE(MAIN.ENCOUNTER_START) AS ENCOUNTER_START
FROM DELIVERED_211.PROFILE_STORE.ENCOUNTER AS MAIN
WHERE MAIN.DATA_SOURCE_ID IN (5, 7, 50);

/*SELECT * FROM PT1 LIMIT 10;*/
CREATE OR REPLACE TEMPORARY TABLE DATA_SOURCE_FLAGS AS
SELECT MAIN.PATIENT_ID
                ,MAIN.INDEX_DATE
                ,MAIN.YEAR_OF_BIRTH
                ,MAIN.STUDY_START
                ,MAIN.STUDY_END
                ,MAIN.START_PAT_ID
                ,MAIN.END_PAT_ID
                ,MAIN.BL_FULL_12MO
                ,MAIN.BL_FULL_6MO
                ,MAIN.FU_6MO
                ,MAIN.BL_12MO
                ,MAIN.BL_6MO
                ,MAIN.BL_EMR
                ,MAIN.FU_EMR
                ,MAIN.BL_CLM
                ,MAIN.FU_CLM
                ,MAX(CASE WHEN B.DATA_SOURCE_ID = 50 AND (MAIN.BL_FULL_12MO <= TO_DATE(B.ENCOUNTER_START) AND TO_DATE(B.ENCOUNTER_START) <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_FU_SPIDERS
/*              ,MAX(CASE WHEN B.DATA_SOURCE_ID = 50 AND (MAIN.BL_FULL_12MO <= TO_DATE(B.ENCOUNTER_START) AND TO_DATE(B.ENCOUNTER_START) <= MAIN.FU_6MO) THEN 1 ELSE 0 END) AS BL_FU_SPIDERS*/
FROM LINKED_FLAGS AS MAIN
LEFT JOIN PT1 AS B ON MAIN.PATIENT_ID=B.PATIENT_ID
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16;

-- ROWS & DISTINCT PATIETNS 14031
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM DATA_SOURCE_FLAGS;

/*******************************************************************************************************************
* FINDING OUT IF THE PEOPLE HAVE AT LEAST ONE CRS_ASSOCIATED NP DIAGNOSIS DURING THE BL PERIOD.
* NEXT QUESTION: IS THERE A SEVER ASTHMA DIAGNOSIS (J45.5X) DURING BL
* THE CUI FOR NP ONLY CONTAINS NP DIAGNOSIS THAT ARE RELATED TO CRS.  THIS WAS PART OF THE SOT DOC DEVELOPMENT.
**********************************************************************************************
Diagnosis  Request                              * CUI                   * Name
**********************************************************************************************
CRS                                             * boc004248             * Chronic sinusitis (NCQA value set)
NP                                              * boc004265             * Nasal Polyps (ECRI plus value set)
Severe Asthma - client requests J45.5x  * boc012772             * Severe Asthma (S)
**********************************************************************************************
*******************************************************************************************************************/
--baseline CRS , NP, Asthma
create or replace TEMPORARY table crs_np_flags as
with cuis as (
                select distinct a.code_type as code_type_cui
                        ,A.mapped_diagnosis_code
                        ,A.boc_cui
                        ,A.boc_name
                        ,B.boc_name as code_type_name
                from MAPSET_20231004.public.boc_map_diagnosis_rollup a 
                join MAPSET_20231004.public.boc_map_diagnosis_code_type b on a.code_type = b.boc_cui
                where a.boc_cui in('boc004248','boc004265','boc012772')),
all_dxs as (SELECT A.PATIENT_ID
                                ,to_date(A.diagnosis_date) AS DX_DATE
                                ,A.code
                                ,B.boc_cui
                                ,B.boc_name
                        FROM DELIVERED_211.profile_store.diagnosis a
                        JOIN cuis b ON (trim(lower(a.code)) = trim(lower(b.mapped_diagnosis_code)) AND trim(lower(a.code_type_name)) = trim(lower(b.code_type_name))))
SELECT MAIN.PATIENT_ID
                ,MAIN.INDEX_DATE
                ,MAIN.YEAR_OF_BIRTH
                ,MAIN.STUDY_START
                ,MAIN.STUDY_END
                ,MAIN.START_PAT_ID
                ,MAIN.END_PAT_ID
                ,MAIN.BL_FULL_12MO
                ,MAIN.BL_FULL_6MO
                ,MAIN.FU_6MO
                ,MAIN.BL_12MO
                ,MAIN.BL_6MO
                ,MAIN.BL_EMR
                ,MAIN.FU_EMR
                ,MAIN.BL_CLM
                ,MAIN.FU_CLM
                ,MAIN.BL_FU_SPIDERS
/*              ,MAX(CASE WHEN B.BOC_CUI = 'boc004248' AND B.DX_DATE BETWEEN MAIN.BL_FULL_12MO AND MAIN.INDEX_DATE THEN 1 ELSE 0 END) AS BL_crs*/
                ,MAX(CASE WHEN B.BOC_CUI = 'boc004265' AND (MAIN.BL_FULL_12MO <= B.DX_DATE AND B.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_np
                ,MAX(CASE WHEN B.BOC_CUI = 'boc012772' AND (MAIN.BL_FULL_12MO <= B.DX_DATE AND B.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_asthma
from DATA_SOURCE_FLAGS AS MAIN
left join all_dxs AS B on MAIN.PATIENT_ID = B.PATIENT_ID
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17;

-- ROWS & DISTINCT PATIETNS 14031
SELECT COUNT(1) AS ROW_CNT
                                ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM crs_np_flags;

/*******************************************************************************************************************
* THIS IS THE UNIQUE LOGIC FOR REDSOX AND TARHEEL
* THIS IS FOR GETTING THE SPECIALIST EMR ENCOUNTERS FOR REDSOX AND TARHEEL LOOKING FOR:
********************************************************************************************************************
Provider Specialty:             *       Pulmonologist           *       boc002384       *       Pulmonary Disease
                                                *       Allergist                       *       boc002358       *       Allergy/Immunology
                                                *       Otolaryngologist        *       boc002359       *       Otolaryngology
********************************************************************************************************************
*******************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE SPECIALTY_FLAGS AS
WITH sp_cuis AS (
        SELECT d.PATIENT_ID
                        ,b.SPECIALTY_CUI
                        ,a.provider_id
                        ,to_date(d.encounter_start) AS ENC_DATE
                        ,d.DATA_SOURCE_ID
        FROM DELIVERED_211.profile_store.provider_specialty a
        JOIN DELIVERED_211.profile_store.specialty b ON a.specialty_id = b.specialty_id
        JOIN DELIVERED_211.profile_store.encounter_provider c ON c.provider_id = a.provider_id
        JOIN DELIVERED_211.profile_store.encounter d ON d.encounter_id = c.encounter_id
        WHERE b.SPECIALTY_CUI IN('boc002358', 'boc002359', 'boc002384')
                AND d.DATA_SOURCE_ID IN (5, 7))
SELECT MAIN.PATIENT_ID
                ,MAIN.INDEX_DATE
                ,MAIN.YEAR_OF_BIRTH
                ,MAIN.STUDY_START
                ,MAIN.STUDY_END
                ,MAIN.START_PAT_ID
                ,MAIN.END_PAT_ID
                ,MAIN.BL_FULL_12MO
                ,MAIN.BL_FULL_6MO
                ,MAIN.FU_6MO
                ,MAIN.BL_12MO
                ,MAIN.BL_6MO
                ,MAIN.BL_EMR
                ,MAIN.FU_EMR
                ,MAIN.BL_CLM
                ,MAIN.FU_CLM
                ,MAIN.BL_FU_SPIDERS
                ,MAIN.BL_NP
                ,MAIN.BL_ASTHMA
                ,max(CASE WHEN B.SPECIALTY_CUI IN('boc002358','boc002359','boc002384') AND B.DATA_SOURCE_ID IN (7) AND (MAIN.BL_FULL_12MO <= B.ENC_DATE AND B.ENC_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_FU_SP_REDSOX
                ,max(CASE WHEN B.SPECIALTY_CUI IN('boc002358','boc002359','boc002384') AND B.DATA_SOURCE_ID IN (5) AND (MAIN.BL_FULL_12MO <= B.ENC_DATE AND B.ENC_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS BL_FU_SP_TARHEEL
/*              ,max(CASE WHEN B.SPECIALTY_CUI IN('boc002358','boc002359','boc002384') AND B.DATA_SOURCE_ID IN (7) AND (MAIN.BL_FULL_12MO <= B.ENC_DATE AND B.ENC_DATE <= MAIN.FU_6MO) THEN 1 ELSE 0 END) AS BL_FU_SP_REDSOX*/
/*              ,max(CASE WHEN B.SPECIALTY_CUI IN('boc002358','boc002359','boc002384') AND B.DATA_SOURCE_ID IN (5) AND (MAIN.BL_FULL_12MO <= B.ENC_DATE AND B.ENC_DATE <= MAIN.FU_6MO) THEN 1 ELSE 0 END) AS BL_FU_SP_TARHEEL*/
FROM crs_np_flags AS MAIN
LEFT JOIN sp_cuis AS B ON MAIN.PATIENT_ID = B.PATIENT_ID
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19;

-- ROWS & DISTINCT PATIETNS 14031
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM SPECIALTY_FLAGS;

/*******************************************************************************************************************
* MEPOLIZUMAB COUNT AFTER INDEX
*******************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE PT1 AS
SELECT MAIN.PATIENT_ID
        ,COUNT(MAIN.MEPO_DATE) AS MEPO_CNT
FROM INDEX_DTS AS MAIN
INNER JOIN SPECIALTY_FLAGS AS BASE ON MAIN.PATIENT_ID = BASE.PATIENT_ID
WHERE BASE.INDEX_DATE <= MAIN.MEPO_DATE AND MAIN.MEPO_DATE <= BASE.END_PAT_ID
GROUP BY MAIN.PATIENT_ID;

-- 21,367
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM PT1;

/*******************************************************************************************************************
* FINAL COHORT TABLE
*******************************************************************************************************************/
CREATE OR REPLACE TABLE RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR AS
-- CREATE OR REPLACE TEMPORARY TABLE CHECK_ME AS
select DISTINCT MAIN.PATIENT_ID
                ,MAIN.INDEX_DATE
                ,MAIN.YEAR_OF_BIRTH
                ,MAIN.STUDY_START
                ,MAIN.STUDY_END
                ,MAIN.START_PAT_ID
                ,MAIN.END_PAT_ID
                ,MAIN.BL_FULL_12MO
                ,MAIN.BL_FULL_6MO
                ,MAIN.FU_6MO
                ,MAIN.BL_12MO
                ,MAIN.BL_6MO
                ,MAIN.BL_EMR
                ,MAIN.FU_EMR
                ,MAIN.BL_CLM
                ,MAIN.FU_CLM
                ,MAIN.BL_NP
                ,MAIN.BL_ASTHMA
                ,MAIN.BL_FU_SPIDERS
                ,MAIN.BL_FU_SP_REDSOX
                ,MAIN.BL_FU_SP_TARHEEL
                ,BASE.MEPO_CNT
                ,CASE
                        WHEN MAIN.INDEX_DATE <= MAIN.END_PAT_ID
                                AND (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) >= 18 
                                AND (MAIN.BL_EMR > 0 AND MAIN.BL_CLM > 0)
                                AND (MAIN.FU_EMR > 0 AND MAIN.FU_CLM > 0)
                                AND (MAIN.BL_FU_SPIDERS = 1 OR MAIN.BL_FU_SP_REDSOX = 1  OR MAIN.BL_FU_SP_TARHEEL = 1)
                                AND (MAIN.bl_np = 1) THEN 1
                        ELSE 0
                END AS MEETS_COHORT_CRITERIA
                ,CASE
                        WHEN MEETS_COHORT_CRITERIA = 1 AND MAIN.BL_ASTHMA = 1 THEN 1
                        ELSE 0
                END AS OBJECTIVE_FIVE
FROM SPECIALTY_FLAGS AS MAIN
LEFT JOIN PT1 AS BASE ON MAIN.PATIENT_ID = BASE.PATIENT_ID;

-- 14,031
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR;

-- 8665
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR
WHERE INDEX_DATE <= END_PAT_ID;

-- AGE: 8293
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR
WHERE INDEX_DATE <= END_PAT_ID
        AND (YEAR(INDEX_DATE) - YEAR_OF_BIRTH) >= 18;
        
-- EMR & CLAIMS: 435
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR
WHERE INDEX_DATE <= END_PAT_ID
        AND ((YEAR(INDEX_DATE) - YEAR_OF_BIRTH) >= 18)
        AND (BL_EMR > 0 AND BL_CLM > 0)
        AND (FU_EMR > 0 AND FU_CLM > 0)
        AND (BL_FU_SPIDERS = 1 OR BL_FU_SP_REDSOX = 1  OR BL_FU_SP_TARHEEL = 1);

-- MEETS CRITERIA: 180
SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR
WHERE INDEX_DATE <= END_PAT_ID
        AND ((YEAR(INDEX_DATE) - YEAR_OF_BIRTH) >= 18)
        AND (BL_EMR > 0 AND BL_CLM > 0)
        AND (FU_EMR > 0 AND FU_CLM > 0)
        AND (BL_FU_SPIDERS = 1 OR BL_FU_SP_REDSOX = 1  OR BL_FU_SP_TARHEEL = 1)
        AND (bl_np = 1);

SELECT COUNT(1) AS ROW_CNT
        ,COUNT(DISTINCT PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR
WHERE MEETS_COHORT_CRITERIA = 1;

SELECT *
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR
WHERE MEETS_COHORT_CRITERIA = 1;

