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
* 			CRS         *   boc004248  *  Chronic sinusitis (NCQA value set)
* 			NP          *   boc004265  *  Nasal Polyps (ECRI plus value set)
* 
********************************************************************************************************************
* SPECIALIST CUIs
********************************************************************************************************************
* 		boc002384		*	Pulmonary Disease
* 		boc002358		*	Allergy/Immunology
* 
*******************************************************************************************************************/
-- GETTING MY PROCEDURES
CREATE OR REPLACE TEMPORARY TABLE MY_MEDS AS
WITH CRSwNP_CUIS as (select distinct a.code_type as code_type_cui
					,A.mapped_diagnosis_code
					,A.boc_cui
					,A.boc_name
					,B.boc_name as code_type_name
			FROM MAPSET_20231004.public.boc_map_diagnosis_rollup a 
			INNER JOIN MAPSET_20231004.public.boc_map_diagnosis_code_type b on a.code_type=b.boc_cui
			WHERE a.boc_cui in('boc004248','boc004265')),
ALL_DX as (SELECT DISTINCT MAIN.PATIENT_ID
					,A.ENCOUNTER_ID
					,TO_DATE(A.DIAGNOSIS_DATE) AS DX_DATE
					,B.boc_cui
			FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
			INNER JOIN DELIVERED_211.PROFILE_STORE.DIAGNOSIS AS A ON MAIN.PATIENT_ID=A.PATIENT_ID
			INNER JOIN CRSwNP_CUIS B ON (trim(lower(a.code)) = trim(lower(b.mapped_diagnosis_code)) AND trim(lower(a.code_type_name)) = trim(lower(b.code_type_name)))),
SPEC_CUIS AS (SELECT DISTINCT A.PROVIDER_ID
					,B.SPECIALTY_CUI
			FROM DELIVERED_211.PROFILE_STORE.PROVIDER_SPECIALTY AS A
			INNER JOIN DELIVERED_211.PROFILE_STORE.SPECIALTY AS B ON A.SPECIALTY_ID=B.SPECIALTY_ID
			WHERE B.SPECIALTY_CUI IN('boc002384','boc002358')),
MEDS_CUIS AS (SELECT DISTINCT A.PATIENT_ID
				,B.ENCOUNTER_ID
				,B.PRESCRIBING_PROVIDER_ID
				,B.MEDICATION_START
				,D.BOC_CUI
			FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS A
			LEFT JOIN DELIVERED_211.PROFILE_STORE.MEDICATION_EVENT AS B ON A.PATIENT_ID=B.PATIENT_ID
			JOIN DELIVERED_211.PROFILE_STORE.MEDICATION_INFO AS C ON B.MEDICATION_INFO_ID=C.MEDICATION_INFO_ID
			JOIN MAPSET_20231004.PUBLIC.BOC_MAP_MEDICATION_ROLLUP AS D ON trim(lower(C.CODE))=trim(lower(D.MAPPED_MEDICATION_CODE)) AND trim(lower(C.CODE_TYPE))=trim(lower(D.CODE_TYPE))
			WHERE D.BOC_CUI IN ('boc000600','boc003042','boc008640','boc004274','boc005247','boc005244','boc005246','boc011543','boc012578','boc004653','boc004431','boc011298','boc005248','boc005256','boc005255','boc005288','boc004654','boc005280','boc012579')),
TT AS (SELECT DISTINCT MED.PATIENT_ID
			,MED.ENCOUNTER_ID
			,SPEC.SPECIALTY_CUI
			,MED.MEDICATION_START
			,MED.BOC_CUI AS MED_CUIS
			,DXD.DX_DATE
			,DXD.BOC_CUI AS CRSwNP_CUIS
			,CASE WHEN ABS(DATEDIFF(DAY, DXD.DX_DATE, MED.MEDICATION_START)) <= 5 THEN 1 ELSE 0 END AS WITHIN5DAYS
		FROM MEDS_CUIS AS MED
		LEFT JOIN SPEC_CUIS AS SPEC ON MED.PRESCRIBING_PROVIDER_ID=SPEC.PROVIDER_ID
		LEFT JOIN ALL_DX AS DXD ON MED.PATIENT_ID=DXD.PATIENT_ID)
SELECT DISTINCT MAIN.PATIENT_ID
	,MAIN.INDEX_DATE
	,CASE
		WHEN BASE.MEDICATION_START IS NOT NULL AND (DATEADD(DAY, -12*30.4375, MAIN.INDEX_DATE) <= BASE.MEDICATION_START AND BASE.MEDICATION_START < DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE)) THEN 'BL_12MO'
		WHEN BASE.MEDICATION_START IS NOT NULL AND (DATEADD(DAY, -6*30.4375, MAIN.INDEX_DATE) <= BASE.MEDICATION_START AND BASE.MEDICATION_START <= MAIN.INDEX_DATE) THEN 'BL_6MO'
		WHEN BASE.MEDICATION_START IS NOT NULL AND (MAIN.INDEX_DATE < BASE.MEDICATION_START AND BASE.MEDICATION_START <= DATEADD(DAY, 6*30.4375, (DATEADD(DAY, 1, MAIN.INDEX_DATE)))) THEN 'FU_6MO'
		ELSE 'NO DATA'
	END AS RUN_ID
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc000600' THEN BASE.MEDICATION_START ELSE NULL END) AS OCS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc000600' AND BASE.CRSwNP_CUIS IN('boc004248','boc004265') AND BASE.WITHIN5DAYS = 1 THEN BASE.MEDICATION_START ELSE NULL END) AS CRSwNP_OCS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc003042' THEN BASE.MEDICATION_START ELSE NULL END) AS INCS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc008640' THEN BASE.MEDICATION_START ELSE NULL END) AS ANTI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc008640' AND BASE.CRSwNP_CUIS IN('boc004248','boc004265') AND BASE.WITHIN5DAYS = 1 THEN BASE.MEDICATION_START ELSE NULL END) AS CRSwNP_ANTI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc004274' THEN BASE.MEDICATION_START ELSE NULL END) AS DUPI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005247' THEN BASE.MEDICATION_START ELSE NULL END) AS OMAL
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005244' THEN BASE.MEDICATION_START ELSE NULL END) AS BENR
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005246' THEN BASE.MEDICATION_START ELSE NULL END) AS RESI
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc011543' THEN BASE.MEDICATION_START ELSE NULL END) AS TEZE
	--,CASE WHEN BASE.SPECIALTY_CUI = 'boc002384' THEN 1 ELSE 0 END AS SPEC_PULM
	--,CASE WHEN BASE.SPECIALTY_CUI = 'boc002358' THEN 1 ELSE 0 END AS SPEC_ALRG
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc012578' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_INHALED
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc004653' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_ICS
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc004431' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_LABA
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc011298' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_SABA
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005248' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_LEUK
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005256' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_THEO
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005255' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_CROM
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005288' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_ALBU
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc004654' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_ICS_LABA
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc005280' THEN BASE.MEDICATION_START ELSE NULL END) AS ASTH_SABA_SAMA
	,COUNT(DISTINCT CASE WHEN BASE.MED_CUIS = 'boc012579' THEN BASE.MEDICATION_START ELSE NULL END) AS ICS_LABA_LAMA
FROM RESEARCH.GSK_CRSWNP.A_PATIENT_LMRJR AS MAIN
LEFT JOIN TT AS BASE ON MAIN.PATIENT_ID=BASE.PATIENT_ID
GROUP BY 1,2,3;


SELECT *
FROM MY_MEDS
LIMIT 100;
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
				,CASE WHEN FTA.SPEC_PULM IS NULL THEN 0 ELSE FTA.SPEC_PULM END AS SPEC_PULM
				,CASE WHEN FTA.SPEC_ALRG IS NULL THEN 0 ELSE FTA.SPEC_ALRG END AS SPEC_ALRG
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

























































/*--------------------------------------------
Project: Bayer CKD
Programmer: Mudit Bhartia
Date: 12/06/2023
Purpose: Create med_era_prep

--------------------------------------------*/
use warehouse query_wh_large;
use Delivered_205;

--Then we make a med_era_prep dataset with all the necessary information
create or replace table research.bayer.med_era_prep_finerenone as
select distinct a.patient_id, medication_event_id as medication_id, encounter_id, 
		case when d.mapped_medication_code in ('30354030000310','30354030000320','50419054001','50419054002',
								'50419054070','2562816','2562822') then 'boc999999' else 'boc000000' end as code, 
		'boc' as code_type,                         
		/*d.boc_name*/ case when d.mapped_medication_code in ('30354030000310','30354030000320','50419054001','50419054002',
								'50419054070','2562816','2562822') then 'Finereone Ten' else 'Finereone Twenty' end as name, 
		medication_start,
		case when days_supply is not null then dateadd(day, days_supply, medication_start) 
			 else dateadd(day, 60, medication_start) end as medication_end, 
		30 as persistence_window, 
		case when admin_start is not null then True
				else False end as is_admin,
		'none' as deoverlap_class, null as notes
from research.bayer.a_patient as a
left join profile_store.medication_event as b
		on a.patient_id = b.patient_id
join profile_store.medication_info as c
		on b.medication_info_id = c.medication_info_id
join MAPSET_20231121.public.boc_map_medication_rollup as d
		on trim(lower(c.code)) = trim(lower(d.mapped_medication_code))
				and trim(lower(c.code_type)) = trim(lower(d.code_type))
where d.boc_cui = 'boc010219'
;


--inspect results: 
select * from research.bayer.med_era_prep_finerenone;
select count(*),count(distinct patient_id) from research.biostat.med_era_prep_finerenone; 

