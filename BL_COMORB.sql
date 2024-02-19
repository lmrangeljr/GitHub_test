/*******************************************************************************************************************
* PROJECT:      ADS TRAINING
* DEVELOPER:    LUIS MANUEL RANGEL JR
* TABLE:        BL_COMORBIDITIES
* DATE:         11/22/2023
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
*EVERYONE WITH A Charlson comorbidity index dx code: VERSION 1
*******************************************************************************************************************/
create or replace TEMPORARY table CUIS as 
select distinct a.code_type as code_type_cui
		,A.mapped_diagnosis_code
		,A.boc_cui
		,A.boc_name
		,B.boc_name as code_type_name
from MAPSET_20231004.public.boc_map_diagnosis_rollup a 
INNER JOIN MAPSET_20231004.public.boc_map_diagnosis_code_type b on a.code_type = b.boc_cui
where a.boc_cui in('boc000294','boc000291','boc000281','boc000283','boc000279','boc000282','boc000288','boc000287','boc000289','boc000293','boc000286','boc000292','boc000278','boc000285','boc000280','boc000290','boc000284');

create or replace TEMPORARY table PATS as 
SELECT A.PATIENT_ID
	,A.INDEX_DATE
	,B.ENCOUNTER_ID
	,C.EVIDENCE_ID
	,to_date(b.occurrence_date) AS cci_dt
	,c.value_as_number AS cci_raw
	,datediff(DAY,cci_dt,a.index_date) AS dayi
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS a
LEFT JOIN DELIVERED_211.PROFILE_STORE.observation AS b ON a.patient_id = b.patient_id
INNER JOIN DELIVERED_211.PROFILE_STORE.observation_attribute AS c ON b.observation_id = c.observation_id
WHERE trim(lower(TYPE)) = 'charlson comorbidity index (cci)'
	AND to_date(b.occurrence_date) <= a.index_date;

SELECT COUNT(1) AS ROW_CNT,
                COUNT(PATIENT_ID) AS PAT_CNT,
                COUNT(DISTINCT PATIENT_ID) AS D_PAT_CNT
FROM PATS;


create or replace TEMPORARY table ALL_DX as 
SELECT DISTINCT MAIN.PATIENT_ID
	,DIAG.ENCOUNTER_ID
	,DIAG.EVIDENCE_ID
	,MAIN.INDEX_DATE
	,ROLLS.BOC_CUI
	,DIAG.DIAGNOSIS_DATE AS DX_DATE
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
INNER JOIN DELIVERED_211.PROFILE_STORE.DIAGNOSIS AS DIAG ON MAIN.PATIENT_ID=DIAG.PATIENT_ID
INNER JOIN CUIS AS ROLLS ON (trim(lower(DIAG.CODE)) = trim(lower(ROLLS.mapped_diagnosis_code)) AND trim(lower(DIAG.CODE_TYPE_NAME)) = trim(lower(ROLLS.code_type_name)));

SELECT COUNT(1) AS ROW_CNT,
                COUNT(PATIENT_ID) AS PAT_CNT,
                COUNT(DISTINCT PATIENT_ID) AS D_PAT_CNT
FROM ALL_DX;

create or replace TEMPORARY table TEMP as 
SELECT DISTINCT TOP.PATIENT_ID
        ,TOP.INDEX_DATE
        ,TOP.CCI_DT
	,CASE WHEN TOP.CCI_RAW IS NULL THEN 0 ELSE TOP.CCI_RAW END AS CCI_RAW
	,rank() over (partition by TOP.PATIENT_ID order by TOP.CCI_RAW DESC) as rnk
FROM PATS AS TOP
LEFT JOIN ALL_DX AS NTX ON TOP.PATIENT_ID=NTX.PATIENT_ID AND TOP.ENCOUNTER_ID=NTX.ENCOUNTER_ID AND TOP.EVIDENCE_ID=NTX.EVIDENCE_ID ;

SELECT COUNT(1) AS ROW_CNT,
                COUNT(PATIENT_ID) AS PAT_CNT,
                COUNT(DISTINCT PATIENT_ID) AS D_PAT_CNT
FROM TEMP;

SELECT *
FROM TEMP
ORDER BY PATIENT_ID, RNK;

create or replace TEMPORARY table CHECK_ME as 
SELECT HP.*
FROM TEMP AS HP
WHERE HP.RNK=1 ;

/*
SELECT COUNT(1) AS ROW_CNT,
		COUNT(PATIENT_ID) AS PAT_CNT,
		COUNT(DISTINCT PATIENT_ID) AS D_PAT_CNT
FROM CHECK_ME;

SELECT *
FROM CHECK_ME
ORDER BY PATIENT_ID
LIMIT 100;
*/

/*******************************************************************************************************************
* GETTING THE DATA FOR:
*******************************************************************************************************************
				DIAGNOSIS               *   CUI
*******************************************************************************************************************
				Asthma                  *   boc002932
Severe Asthma - client requests J45.5x  *   boc012772
			Allergic Rhinitis           *   boc002329
			Acute Sinusistis            *   boc009075
				COPD                    *   boc002940
			Cystic Fibrosis             *   boc005411
			Sleep Apnea                 *   boc003990
*******************************************************************************************************************/
CREATE OR REPLACE TABLE RESEARCH.GSK_CRSWNP.A_COMORBID_LMRJR AS
with CUIS as (select distinct a.code_type as code_type_cui
		,A.mapped_diagnosis_code
		,A.boc_cui
		,A.boc_name
		,B.boc_name as code_type_name
from MAPSET_20231004.public.boc_map_diagnosis_rollup a 
INNER JOIN MAPSET_20231004.public.boc_map_diagnosis_code_type b on a.code_type = b.boc_cui
where a.boc_cui in('boc002932','boc012772','boc002329','boc009075','boc002940','boc005411','boc003990')),
ALL_DX AS (SELECT DISTINCT MAIN.PATIENT_ID
			   ,TO_DATE(DIAG.DIAGNOSIS_DATE) AS DX_DATE
			   ,DIAG.CODE
			   ,ROLLS.BOC_CUI
			   ,ROLLS.BOC_NAME
		   FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
		   INNER JOIN DELIVERED_211.PROFILE_STORE.DIAGNOSIS AS DIAG ON MAIN.PATIENT_ID=DIAG.PATIENT_ID
		   INNER JOIN CUIS AS ROLLS ON (trim(lower(DIAG.CODE)) = trim(lower(ROLLS.mapped_diagnosis_code)) AND trim(lower(DIAG.CODE_TYPE_NAME)) = trim(lower(ROLLS.code_type_name))))
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
		,TT.CCI_DT AS CCI_DT
		,CASE 
				WHEN TT.CCI_DT IS NULL THEN 0 ELSE TT.CCI_RAW
		END AS CCI_VALUE
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc002932' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS Asthma
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc012772' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS Sev_Asthma
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc002329' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS Alg_rhinitis
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc009075' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS Acute_Sinus
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc002940' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS COPD
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc005411' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS Cys_Fibrosis
		,MAX(CASE WHEN BASE.BOC_CUI = 'boc003990' AND (MAIN.BL_FULL_12MO <= BASE.DX_DATE AND BASE.DX_DATE <= MAIN.INDEX_DATE) THEN 1 ELSE 0 END) AS Sleep_Apnea
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
LEFT JOIN ALL_DX AS BASE ON MAIN.PATIENT_ID=BASE.PATIENT_ID
LEFT JOIN CHECK_ME AS TT ON MAIN.PATIENT_ID=TT.PATIENT_ID
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;

SELECT COUNT(1) AS ROW_CNT, COUNT(PATIENT_ID) AS PAT_CNT
FROM RESEARCH.GSK_CRSWNP.A_COMORBID_LMRJR;