/*******************************************************************************************************************
* PROJECT:      ADS TRAINING
* DEVELOPER:    LUIS MANUEL RANGEL JR
* TABLE:        A_MEDICATIONS
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
* MEDICAITON REQUIRMENTS
********************************************************************************************************************
* MEDICATION CUIs
********************************************************************************************************************
*		boc000600		* 	Steroids, Oral
*		boc000600		* 	Steroids, Oral for CRSwNP
*		boc003042		* 	Steroids, Nasal Inhaled
*		boc008640		* 	Antibiotics, Oral
*		boc008640		* 	Antibiotics, Oral for CRSwNP
*		boc004274 		*	Dupilumab
*		boc005247 		*	Omalizumab
*		boc005244 		*	Benralizumab
*		boc005246 		*	Reslizumab
*		boc011543 		*	Tezepelumab
*		boc012578 		*	Asthma - Anticholinergic (SAMA)
*		boc004653 		*	Asthma - Corticosteroids (ICS)
*		boc004431 		*	Long-Acting Beta Agonist (LABA), Asthma
*		boc011298 		*	Short-Acting Beta Agonist (SABA)
*		boc005248 		*	Asthma - Leukotriene Modifiers
*		boc005256 		*	Theophylline
*		boc005255 		*	Cromolyn Sodium, inhaled
*		boc005288 		*	Albuterol, oral
*		boc004654 		*	Asthma - Corticosteroid/Long-Acting Beta Agonist (ICS/LABA)
*		boc005280 		*	Short-Acting Anti-Muscarinic (SAMA)/Short-Acting Beta Agonist (SABA)
*		boc012579 		*	Asthma - Anticholinergic/Corticosteroid/Long-Acting Beta Agonist (LAMA/ICS/LABA) 
* 
********************************************************************************************************************
* CRSwNP CUIs
********************************************************************************************************************
* 			CRS         *   boc004248  *	Chronic sinusitis (NCQA value set)
* 			NP          *   boc004265  *	Nasal Polyps (ECRI plus value set)
*   Severe Asthma (S)	*	boc012772  * 	Severe Asthma (S)
********************************************************************************************************************
* SPECIALIST CUIs
********************************************************************************************************************
* 		boc002384		*	Pulmonary Disease			*
* 		boc002358		*	Allergy/Immunology			*
********************************************************************************************************************
* Procedur CUIs
********************************************************************************************************************
*               boc004249               *               Functional endoscopic sinus surgery (FESS)
*               boc004267               *               Nasal polypectomy (S)
*******************************************************************************************************************/

SELECT *
FROM RAW_EXTRACTIONS_DEIDPROD.CRS_SYMPTOMS_D216_20240227.CRS_SYMPTOMS_COMBINED_RESULTS 
LIMIT 10;

-- GETTING MY PROCEDURES
CREATE OR REPLACE
TEMPORARY TABLE MY_MEDS AS WITH FP_CNT AS
	(SELECT DISTINCT MAIN.PATIENT_ID
					 ,BASE.SOURCE_ENCOUNTER_ID
					 ,BASE.NOTE_RECOREDED_DATE
	 FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
	 LEFT JOIN RAW_EXTRACTIONS_DEIDPROD.CRS_SYMPTOMS_D216_20240227.CRS_SYMPTOMS_COMBINED_RESULTS AS BASE ON MAIN.PATIENT_ID=BASE.BOC_PATIENT_ID
	 WHERE CONTAINS(REGEXP_REPLACE(UPPER(BASE.EXTRACTION), '\\s+', ''), 'FACEPAIN')
		 OR CONTAINS(REGEXP_REPLACE(UPPER(BASE.EXTRACTION), '\\s+', ''), 'FACIALPAIN')
		 OR CONTAINS(REGEXP_REPLACE(UPPER(BASE.EXTRACTION), '\\s+', ''), 'FACEPRESSURE')
		 OR CONTAINS(REGEXP_REPLACE(UPPER(BASE.EXTRACTION), '\\s+', ''), 'FACIALPRESSURE')),
OBSTRUCT AS (SELECT DISTINCT MAIN.PATIENT_ID
					 ,BASE.SOURCE_ENCOUNTER_ID
					 ,BASE.NOTE_RECOREDED_DATE
	 FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
	 LEFT JOIN RAW_EXTRACTIONS_DEIDPROD.CRS_SYMPTOMS_D216_20240227.CRS_SYMPTOMS_COMBINED_RESULTS AS BASE ON MAIN.PATIENT_ID=BASE.BOC_PATIENT_ID
	 WHERE CONTAINS(UPPER(BASE.EXTRACTION), 'NASAL')
		 AND CONTAINS(UPPER(BASE.EXTRACTION), 'OBSTRUCTION')),
DISCHARGE AS (SELECT DISTINCT MAIN.PATIENT_ID
					 ,BASE.SOURCE_ENCOUNTER_ID
					 ,BASE.NOTE_RECOREDED_DATE
	 FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
	 LEFT JOIN RAW_EXTRACTIONS_DEIDPROD.CRS_SYMPTOMS_D216_20240227.CRS_SYMPTOMS_COMBINED_RESULTS AS BASE ON MAIN.PATIENT_ID=BASE.BOC_PATIENT_ID
	 WHERE CONTAINS(UPPER(BASE.EXTRACTION), 'NASAL')
		 AND CONTAINS(UPPER(BASE.EXTRACTION), 'DISCHARGE')),
ANOSMIA AS (SELECT DISTINCT MAIN.PATIENT_ID
					 ,BASE.SOURCE_ENCOUNTER_ID
					 ,BASE.NOTE_RECOREDED_DATE
	 FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
	 LEFT JOIN RAW_EXTRACTIONS_DEIDPROD.CRS_SYMPTOMS_D216_20240227.CRS_SYMPTOMS_COMBINED_RESULTS AS BASE ON MAIN.PATIENT_ID=BASE.BOC_PATIENT_ID
	 WHERE CONTAINS(UPPER(BASE.EXTRACTION), 'ANOSMIA')
		 OR CONTAINS(REGEXP_REPLACE(UPPER(BASE.EXTRACTION), '\\s+', ''), 'LOSSOFSENSEOFSMELL'))
SELECT DISTINCT MAIN.PATIENT_ID
	,MAIN.INDEX_DATE
	,CASE
		WHEN BASE.MEDICATION_START IS NOT NULL AND (DATEADD(DAY, -12*30.4375, MAIN.INDEX_DATE) <= BASE.MEDICATION_START AND BASE.MEDICATION_START < DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE)) THEN 'BL_12MO'
		WHEN BASE.MEDICATION_START IS NOT NULL AND (DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE) <= BASE.MEDICATION_START AND BASE.MEDICATION_START <= MAIN.INDEX_DATE) THEN 'BL_6MO'
		WHEN BASE.MEDICATION_START IS NOT NULL AND (MAIN.INDEX_DATE < BASE.MEDICATION_START AND BASE.MEDICATION_START <= DATEADD(DAY, 6*30.4375, (DATEADD(DAY, 1, MAIN.INDEX_DATE)))) THEN 'FU_6MO'
		ELSE 'NO DATA'
	END AS RUN_ID
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc000600' THEN BASE.MEDICATION_START ELSE NULL END) AS OCS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc000600' AND BASE.CRSwNP_CUIS IN('boc004248','boc004265') AND (BASE.WITHIN5DAYS = 1 OR BASE.WITHIN30DAYS = 1) THEN BASE.MEDICATION_START ELSE NULL END) AS CRSwNP_OCS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc003042' THEN BASE.MEDICATION_START ELSE NULL END) AS INCS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc008640' THEN BASE.MEDICATION_START ELSE NULL END) AS ANTI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc008640' AND BASE.CRSwNP_CUIS IN('boc004248','boc004265') AND (BASE.WITHIN5DAYS = 1 OR BASE.WITHIN30DAYS = 1) THEN BASE.MEDICATION_START ELSE NULL END) AS CRSwNP_ANTI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc004274' THEN BASE.MEDICATION_START ELSE NULL END) AS DUPI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005247' THEN BASE.MEDICATION_START ELSE NULL END) AS OMAL
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005244' THEN BASE.MEDICATION_START ELSE NULL END) AS BENR
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005246' THEN BASE.MEDICATION_START ELSE NULL END) AS RESI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc011543' THEN BASE.MEDICATION_START ELSE NULL END) AS TEZE
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS IN('boc000600','boc003042','boc008640','boc004274','boc005247','boc005244','boc005246','boc011543') AND BASE.SPECIALTY_CUI = 'boc002384' THEN BASE.MEDICATION_START ELSE NULL END) AS SPEC_PULM
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS IN('boc000600','boc003042','boc008640','boc004274','boc005247','boc005244','boc005246','boc011543') AND BASE.SPECIALTY_CUI = 'boc002358' THEN BASE.MEDICATION_START ELSE NULL END) AS SPEC_ALRG
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc012578' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_INHALED
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc004653' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_ICS
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc004431' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_LABA
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc011298' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_SABA
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc005248' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_LEUK
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc005256' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_THEO
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc005255' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_CROM
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc005288' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_ALBU
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc004654' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_ICS_LABA
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc005280' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_SABA_SAMA
	,COUNT(DISTINCT CASE WHEN BASE.CRSwNP_CUIS = 'boc012772' AND BASE.MED_CUIS = 'boc012579' THEN BASE.MEDICATION_START ELSE NULL END) AS ICS_LABA_LAMA
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
LEFT JOIN FP_CNT AS A ON MAIN.PATIENT_ID=A.PATIENT_ID
LEFT JOIN OBSTRUCT AS B ON MAIN.PATIENT_ID=B.PATIENT_ID
LEFT JOIN DISCHARGE AS C ON MAIN.PATIENT_ID=C.PATIENT_ID
LEFT JOIN ANOSMIA AS D ON MAIN.PATIENT_ID=D.PATIENT_ID
GROUP BY 1,2,3;

/*******************************************************************************************************************
* PUTTING IT ALL TOGETHER
* MAKING SURE EACH TIME POINT HAS A VALUE
*******************************************************************************************************************/
CREATE OR REPLACE TABLE RESEARCH.GSK_CRSWNP.A_MEDICATION_LMRJR AS
SELECT MAIN.PATIENT_ID
				,MAIN.INDEX_DATE
				,MAIN.RUN_ID
				,CASE WHEN FTA.OCS IS NULL THEN 0 ELSE FTA.OCS END AS OCS
				,CASE WHEN FTA.CRSwNP_OCS IS NULL THEN 0 ELSE FTA.CRSwNP_OCS END AS CRSwNP_OCS
				,CASE WHEN FTA.INCS IS NULL THEN 0 ELSE FTA.INCS END AS INCS
				,CASE WHEN FTA.ANTI IS NULL THEN 0 ELSE FTA.ANTI END AS ANTI
				,CASE WHEN FTA.CRSwNP_ANTI IS NULL THEN 0 ELSE FTA.CRSwNP_ANTI END AS CRSwNP_ANTI
				,CASE WHEN FTA.DUPI IS NULL THEN 0 ELSE FTA.DUPI END AS DUPI
				,CASE WHEN FTA.OMAL IS NULL THEN 0 ELSE FTA.OMAL END AS OMAL
				,CASE WHEN FTA.BENR IS NULL THEN 0 ELSE FTA.BENR END AS BENR
				,CASE WHEN FTA.RESI IS NULL THEN 0 ELSE FTA.RESI END AS RESI
				,CASE WHEN FTA.TEZE IS NULL THEN 0 ELSE FTA.TEZE END AS TEZE
				,CASE WHEN FTA.SPEC_PULM > 0 THEN 1 ELSE 0 END AS SPEC_PULM
				,CASE WHEN FTA.SPEC_ALRG > 0 THEN 1 ELSE 0 END AS SPEC_ALRG
				,CASE WHEN FTA.ASTH_INHALED IS NULL THEN 0 ELSE FTA.ASTH_INHALED END AS ASTH_INHALED
				,CASE WHEN FTA.ASTH_ICS IS NULL THEN 0 ELSE FTA.ASTH_ICS END AS ASTH_ICS
				,CASE WHEN FTA.ASTH_LABA IS NULL THEN 0 ELSE FTA.ASTH_LABA END AS ASTH_LABA
				,CASE WHEN FTA.ASTH_SABA IS NULL THEN 0 ELSE FTA.ASTH_SABA END AS ASTH_SABA
				,CASE WHEN FTA.ASTH_LEUK IS NULL THEN 0 ELSE FTA.ASTH_LEUK END AS ASTH_LEUK
				,CASE WHEN FTA.ASTH_THEO IS NULL THEN 0 ELSE FTA.ASTH_THEO END AS ASTH_THEO
				,CASE WHEN FTA.ASTH_CROM IS NULL THEN 0 ELSE FTA.ASTH_CROM END AS ASTH_CROM
				,CASE WHEN FTA.ASTH_ALBU IS NULL THEN 0 ELSE FTA.ASTH_ALBU END AS ASTH_ALBU
				,CASE WHEN FTA.ASTH_ICS_LABA IS NULL THEN 0 ELSE FTA.ASTH_ICS_LABA END AS ASTH_ICS_LABA
				,CASE WHEN FTA.ASTH_SABA_SAMA IS NULL THEN 0 ELSE FTA.ASTH_SABA_SAMA END AS ASTH_SABA_SAMA
				,CASE WHEN FTA.ICS_LABA_LAMA IS NULL THEN 0 ELSE FTA.ICS_LABA_LAMA END AS ICS_LABA_LAMA
FROM FINAL_TABLE AS MAIN
LEFT JOIN MY_MEDS AS FTA ON MAIN.PATIENT_ID=FTA.PATIENT_ID AND MAIN.INDEX_DATE=FTA.INDEX_DATE AND MAIN.RUN_ID=FTA.RUN_ID;

SELECT *
FROM RESEARCH.GSK_CRSWNP.A_MEDICATION_LMRJR
LIMIT 100;