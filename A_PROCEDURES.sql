/*******************************************************************************************************************
* PROJECT:      ADS TRAINING
* DEVELOPER:    LUIS MANUEL RANGEL JR
* TABLE:        A_PROCEDURES
* DATE:         02/23/2024
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
* BASE JOINING TABLE
*******************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE FINAL_TABLE AS
SELECT MAIN.PATIENT_ID, MAIN.INDEX_DATE, 'BL_12MO' AS RUN_ID FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
UNION ALL
SELECT MAIN.PATIENT_ID, MAIN.INDEX_DATE, 'BL_6MO' AS RUN_ID FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
UNION ALL
SELECT MAIN.PATIENT_ID, MAIN.INDEX_DATE, 'FU_6MO' AS RUN_ID FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN;

/*******************************************************************************************************************
* PROCEDURES REQUIRMENTS
********************************************************************************************************************
* CUIs
********************************************************************************************************************
*               boc009098               *               Sinus diagnostic endoscopy (S)
*               boc004289               *               Nasal Sinus Computed Tomography (CT) (S)
*               boc009100               *               Nasal Sinus MRI (S)
*               boc004249               *               Functional endoscopic sinus surgery (FESS)
*               boc004267               *               Nasal polypectomy (S)
* 
********************************************************************************************************************
* CPT/HCPCS 
********************************************************************************************************************
*               31237                   *               Nasal/sinus endoscopy, surgical; with biopsy, polypectomy or debridement (separate procedure)
*               S2342                   *               Nasal endoscopy for postoperative debridement following functional endoscopic sinus surgery, nasal and/or sinus cavity(s), unilateral or bilateral
*******************************************************************************************************************/
-- GETTING MY PROCEDURES
CREATE OR REPLACE TEMPORARY TABLE MY_PROCS AS
WITH ALL_PROCS AS (
		SELECT MAIN.PATIENT_ID
				,TO_DATE(MAIN.PROCEDURE_DATE) AS PROC_DATE
				,BASE.BOC_CUI
				,BASE.MAPPED_PROCEDURE_CODE
		FROM DELIVERED_211.PROFILE_STORE.PROCEDURE AS MAIN
		JOIN MAPSET_20231004.PUBLIC.BOC_MAP_PROCEDURE_ROLLUP AS BASE ON MAIN.CODE=BASE.MAPPED_PROCEDURE_CODE AND MAIN.CODE_TYPE_CUI=BASE.CODE_TYPE
		WHERE BASE.BOC_CUI IN ('boc009098','boc004289','boc009100','boc004249','boc004267')
			OR BASE.MAPPED_PROCEDURE_CODE IN('31237','S2342'))
SELECT MAIN.PATIENT_ID
		,MAIN.INDEX_DATE
		,CASE
				WHEN BASE.PROC_DATE IS NOT NULL AND (DATEADD(DAY, -12*30.4375, MAIN.INDEX_DATE) <= BASE.PROC_DATE AND BASE.PROC_DATE < DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE)) THEN 'BL_12MO'
				WHEN BASE.PROC_DATE IS NOT NULL AND (DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE) <= BASE.PROC_DATE AND BASE.PROC_DATE <= MAIN.INDEX_DATE) THEN 'BL_6MO'
				WHEN BASE.PROC_DATE IS NOT NULL AND (MAIN.INDEX_DATE < BASE.PROC_DATE AND BASE.PROC_DATE <= DATEADD(DAY, 6*30.4375, (DATEADD(DAY, 1, MAIN.INDEX_DATE)))) THEN 'FU_6MO'
				ELSE 'NO DATA'
		END AS RUN_ID
		,COUNT(DISTINCT CASE WHEN BASE.BOC_CUI = 'boc009098' THEN BASE.PROC_DATE ELSE NULL END) AS NASAL_ENDO
		,COUNT(DISTINCT CASE WHEN BASE.BOC_CUI = 'boc004289' THEN BASE.PROC_DATE ELSE NULL END) AS SINUS_CT
		,COUNT(DISTINCT CASE WHEN BASE.BOC_CUI = 'boc009100' THEN BASE.PROC_DATE ELSE NULL END) AS SINUS_MRI
		,COUNT(DISTINCT CASE WHEN BASE.BOC_CUI = 'boc004249' THEN BASE.PROC_DATE ELSE NULL END) AS ESS
		,COUNT(DISTINCT CASE WHEN BASE.MAPPED_PROCEDURE_CODE IN ('31237','S2342') THEN BASE.PROC_DATE ELSE NULL END) AS SINUS_DEBRI
		,COUNT(DISTINCT CASE WHEN BASE.BOC_CUI = 'boc004267' THEN BASE.PROC_DATE ELSE NULL END) AS POLYPEC
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
LEFT JOIN ALL_PROCS AS BASE ON MAIN.PATIENT_ID = BASE.PATIENT_ID
GROUP BY 1, 2, 3;

/*******************************************************************************************************************
* PUTTING IT ALL TOGETHER
* MAKING SURE EACH TIME POINT HAS A VALUE
*******************************************************************************************************************/
CREATE OR REPLACE TABLE RESEARCH.GSK_CRSWNP.A_PROCEDURES_LMRJR AS
SELECT MAIN.PATIENT_ID
		,MAIN.INDEX_DATE
		,MAIN.RUN_ID
		,CASE WHEN FTA.NASAL_ENDO IS NULL THEN 0 ELSE FTA.NASAL_ENDO END AS NASAL_ENDO
		,CASE WHEN FTA.SINUS_CT IS NULL THEN 0 ELSE FTA.SINUS_CT END AS SINUS_CT
		,CASE WHEN FTA.SINUS_MRI IS NULL THEN 0 ELSE FTA.SINUS_MRI END AS SINUS_MRI
		,CASE WHEN FTA.ESS IS NULL THEN 0 ELSE FTA.ESS END AS ESS
		,CASE WHEN FTA.SINUS_DEBRI IS NULL THEN 0 ELSE FTA.SINUS_DEBRI END AS SINUS_DEBRI
		,CASE WHEN FTA.POLYPEC IS NULL THEN 0 ELSE FTA.POLYPEC END AS POLYPEC
FROM FINAL_TABLE AS MAIN
LEFT JOIN MY_PROCS AS FTA ON MAIN.PATIENT_ID=FTA.PATIENT_ID AND MAIN.INDEX_DATE=FTA.INDEX_DATE AND MAIN.RUN_ID=FTA.RUN_ID;