/*******************************************************************************************************************
* PROJECT: 		ADS TRAINING
* DEVELOPER:	LUIS MANUEL RANGEL JR
* TABLE:		A_PATIENT
* DATE:			11/22/2023
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
* GETTING POPULATION AND PATIENT CHARACTERISTICS
*******************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE PT1 AS
SELECT DISTINCT MAIN.PATIENT_ID
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
		,(YEAR(INDEX_DATE) - MAIN.YEAR_OF_BIRTH) AS AGE
		,CASE
			WHEN (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) > 17 AND (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) <= 24 THEN '18 - 24'
			WHEN (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) > 24 AND (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) <= 34 THEN '25 - 34'
			WHEN (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) > 34 AND (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) <= 44 THEN '35 - 44'
			WHEN (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) > 44 AND (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) <= 54 THEN '45 - 54'
			WHEN (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) > 54 AND (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) <= 64 THEN '55 - 64'
			WHEN (YEAR(MAIN.INDEX_DATE) - MAIN.YEAR_OF_BIRTH) > 64 THEN '65+'
			ELSE 'ERROR'
		END AS CAT_AGE
		,B.SEX_NAME AS SEX
		,CASE
			WHEN B.RACE_CUI IN ('boc000073') THEN B.RACE_NAME
			WHEN B.RACE_CUI IN ('boc000072') THEN B.RACE_NAME
			WHEN B.RACE_CUI IN ('boc000075') THEN B.RACE_NAME
			WHEN B.RACE_CUI IN ('boc000071','boc000097','boc000074','boc000076') THEN 'Other'
			WHEN B.RACE_CUI IN ('boc000539') THEN 'Unknown or not reported'
			ELSE 'Unknown or not reported'
		END AS RACE
		,CASE
			WHEN B.ETHNICITY_NAME IS NULL THEN 'Unknown or not reported'
			WHEN B.ETHNICITY_NAME = 'Other Ethnicity' THEN 'Unknown or not reported'
			ELSE B.ETHNICITY_NAME
		END AS ETHNICITY
		,CASE
			WHEN B.address_state_name IN('Maine', 'New Hampshire', 'Vermont', 'Massachusetts', 'Connecticut', 'Rhode Island') THEN 'New England'
			WHEN B.address_state_name IN('New York', 'New Jersey', 'Pennsylvania') THEN 'Middle Atlantic'
			WHEN B.address_state_name IN('Maryland', 'Delaware', 'District of Columbia', 'West Virginia', 'Virginia', 'North Carolina', 'South Carolina', 'Georgia', 'Florida') THEN 'South Atlantic'
			WHEN B.address_state_name IN('Kentucky', 'Tennessee', 'Alabama', 'Mississippi') THEN 'East South Central'
			WHEN B.address_state_name IN('Arkansas', 'Louisiana', 'Oklahoma', 'Texas') THEN 'West South Central'
			WHEN B.address_state_name IN('Ohio', 'Indiana', 'Michigan', 'Illinois', 'Wisconsin') THEN 'East North Central'
			WHEN B.address_state_name IN('Minnesota', 'Iowa', 'Missouri', 'North Dakota', 'South Dakota', 'Nebraska', 'Kansas') THEN 'West North Central'
			WHEN B.address_state_name IN('Montana', 'Wyoming', 'Idaho', 'Nevada', 'Utah', 'Colorado', 'Arizona', 'New Mexico') THEN 'Mountain'
			WHEN B.address_state_name IN('Hawaii', 'Alaska', 'Washington', 'Oregon', 'California') THEN 'Pacific' 
			ELSE 'Unknown'
		END AS CENSUS_DIVISION
		,CASE 
			WHEN CENSUS_DIVISION IN('Pacific', 'Mountain') THEN 'West'
			WHEN CENSUS_DIVISION IN('East South Central', 'West South Central', 'South Atlantic') THEN 'South'
			WHEN CENSUS_DIVISION IN('East North Central', 'West North Central') THEN 'Midwest'
			WHEN CENSUS_DIVISION IN('New England', 'Middle Atlantic') THEN 'Northeast'
			ELSE 'Unknown'
		END AS CENSUS_REGION
FROM RESEARCH.GSK_CRSWNP.R_COHORT_LMRJR AS MAIN
LEFT JOIN DELIVERED_211.PROFILE_STORE.PATIENT AS B ON MAIN.PATIENT_ID = B.PATIENT_ID
WHERE MAIN.MEETS_COHORT_CRITERIA = 1;

SELECT COUNT(1) AS ROW_CNT, COUNT(DISTINCT PATIENT_ID) AS PAT_CNT FROM PT1;

/*******************************************************************************************************************
* JUST GETTING THE INSURENCE DATA AND WE ARE DONE
*******************************************************************************************************************/
CREATE OR REPLACE TABLE RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS
/*CREATE OR REPLACE TEMPORARY TABLE CHECK_ME AS*/
WITH INS_PREP AS (SELECT A.PATIENT_ID
						,A.INDEX_DATE
						,B.ENCOUNTER_ID
						,TO_DATE(COALESCE(C.ENCOUNTER_START, B.START_DATE)) AS INS_DT
						,B.financial_class_cui as insurance_cui
						,B.financial_class_name
						,rank() over (partition by A.patient_id, A.index_date order by INS_DT desc) as rnk
				  FROM PT1 AS A
				  INNER JOIN DELIVERED_211.PROFILE_STORE.INSURANCE AS B ON A.PATIENT_ID = B.PATIENT_ID
				  INNER JOIN DELIVERED_211.PROFILE_STORE.ENCOUNTER AS C ON B.PATIENT_ID = C.PATIENT_ID AND B.ENCOUNTER_ID = C.ENCOUNTER_ID
				  WHERE INS_DT <= A.INDEX_DATE),
RANKED_INS AS (SELECT D.PATIENT_ID
					,D.INDEX_DATE
					,D.INSURANCE_CUI
					,D.FINANCIAL_CLASS_NAME
					,D.INS_DT
			   FROM INS_PREP AS D
			   WHERE D.RNK = 1),
FINAL_TAB AS (SELECT E.PATIENT_ID
					,E.INDEX_DATE
					,MAX(CASE WHEN E.INSURANCE_CUI IN('boc000765') THEN 1 ELSE 0 END) AS commercial
					,MAX(CASE WHEN E.INSURANCE_CUI IN('boc000767') THEN 1 ELSE 0 END) AS medicaid
					,MAX(CASE WHEN E.INSURANCE_CUI IN('boc000766', 'boc009031', 'boc009033', 'boc009034', 'boc009024', 'boc009035', 'boc001464') THEN 1 ELSE 0 END) AS medicare
					,MAX(CASE WHEN E.INSURANCE_CUI IS NULL OR insurance_cui = 'boc000774' THEN 1 ELSE 0 END) AS unknown_ins
			  FROM RANKED_INS AS E
			  GROUP BY 1, 2)
SELECT DISTINCT MAIN.PATIENT_ID
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
		,MAIN.AGE
		,MAIN.CAT_AGE
		,MAIN.SEX
		,MAIN.RACE
		,MAIN.ETHNICITY
		,MAIN.CENSUS_REGION
		,CASE
			WHEN D.COMMERCIAL = 1 THEN 'Commercial'
			WHEN D.MEDICAID = 1 THEN 'Medicaid'
			WHEN D.MEDICARE = 1 THEN 'Medicare'
			WHEN (D.COMMERCIAL + D.MEDICAID + D.MEDICARE) > 1 THEN 'Multiple'
			WHEN D.UNKNOWN_INS = 1 THEN 'Unknown'
			ELSE 'Other'
		END AS INSURANCE
FROM PT1 AS MAIN
LEFT JOIN FINAL_TAB AS D ON MAIN.PATIENT_ID = D.PATIENT_ID AND MAIN.INDEX_DATE=D.INDEX_DATE;

SELECT COUNT(1) AS ROW_CNT, COUNT(DISTINCT PATIENT_ID) AS PAT_CNT FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR;
