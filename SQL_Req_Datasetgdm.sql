-- Q48. Display the last inserted row in demographics table without using limit.

-- creating the table to log the new added participants in the the demographics.
CREATE TABLE demographics_insert_audit (
audit_id SERIAL PRIMARY KEY,
participant_id INT,
insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
user_name VARCHAR(50) DEFAULT CURRENT_USER
)

--creating a trigger to that adds the row in the demographics_insert_audit table when some insertion happens on demographics table
CREATE OR REPLACE FUNCTION insert_new_participant()
RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO demographics_insert_audit(
	participant_id
	)
	VALUES (
	NEW.participant_id --id of newly inserted participant in the demogarphics
	);
	RETURN NEW; 
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER add_new_id
AFTER INSERT ON demographics
FOR EACH ROW 
EXECUTE FUNCTION insert_new_participant();

DROP TRIGGER add_new_id ON demographics

--inserting new rows in the demographic table
INSERT INTO demographics(participant_id,
						ethnicity,
						age_above_30,
						height_m,
						bmi_kgm2_v1,
						smoking,
						alcohol_intake,
						family_history,
						highrisk,
						medications,
						nutritional_counselling
						)
VALUES(602,'White',1,1.56,27.25,'Never',0,0,0,0,0);
		
INSERT INTO demographics(participant_id,
						ethnicity,
						age_above_30,
						height_m,
						bmi_kgm2_v1,
						smoking,
						alcohol_intake,
						family_history,
						highrisk,
						medications,
						nutritional_counselling
						)
VALUES(601,'White',1,1.56,27.25,'Never',0,0,0,0,0);
INSERT INTO demographics(participant_id,
						ethnicity,
						age_above_30,
						height_m,
						bmi_kgm2_v1,
						smoking,
						alcohol_intake,
						family_history,
						highrisk,
						medications,
						nutritional_counselling
						)
VALUES(605,'White',1,1.56,27.25,'Never',0,0,0,0,0);
		

SELECT * 
FROM demographics
WHERE participant_id = (SELECT participant_id 
						FROM demographics_insert_audit
						WHERE insert_date = (SELECT MAX(insert_date)
											FROM demographics_insert_audit					
											) 
						)
--**********************************************************************************************************************

-- Q49. Count of patients by first letter of insulin_metformnin column.Replace blank values to Unknown
SELECT * FROM glucose_tests

--replacing the balck value to 'Unknown' in the column insulin_metformnin
UPDATE glucose_tests
SET insulin_metformnin = 'Unknown'
WHERE insulin_metformnin IS NULL;

-- getting the counts of patients
SELECT LEFT(insulin_metformnin, 1) AS first_letter_of_insulin_metformnin, COUNT(*) AS count_of_patients
FROM glucose_tests
GROUP BY LEFT(insulin_metformnin, 1)
ORDER BY COUNT(*) DESC, first_letter_of_insulin_metformnin;

--**********************************************************************************************************************

/*Q50. Create a Index on Ethnicity column. Check whether index is used in below Query:
select ethnicity,count(participant_id) 
from public.demographics
group by ethnicity.Make sure Above Query to use the index */

--before creating the index, explaining the query
EXPLAIN
SELECT ethnicity,COUNT(participant_id) 
FROM public.demographics
GROUP BY ethnicity;

--setting the enable_seqscan off for the usage of index
SET enable_seqscan = OFF;

--creating index on ethnicity column
CREATE INDEX idx_ethnicity
ON demographics(ethnicity);


--displaying the index created
SELECT indexname,
  		indexdef
FROM pg_indexes
WHERE tablename = 'demographics';

--dropping the INDEX
DROP INDEX idx_ethnicity;

--explaing the query after creating the index
EXPLAIN
SELECT ethnicity,COUNT(participant_id) 
FROM public.demographics
GROUP BY ethnicity;

--again setting the enable_seqscan on
SET enable_seqscan = ON;

--**********************************************************************************************************************

-- Q51. Calculate the conception date or Last menstrual for all participants. Generate new attribute

-- adding a new attribute in table pregnancy_info
ALTER TABLE pregnancy_info
ADD COLUMN conception_date DATE;

--calculating the new attribute values by updating the table
UPDATE pregnancy_info
SET conception_date = edd_v1 - INTERVAL '280 days'; --considering edd_v1 and not US EDD as edd_v1 has no null values whereas US EDD has some null values

--displaying the conception_date
SELECT *
FROM pregnancy_info;

--**********************************************************************************************************************

-- Q52. Display different set of 10 patients (every time) who were diagnosed with gestational diabetes from their demographic details.

SELECT d.participant_id,
		d.ethnicity,
		d.age_above_30,
		d.height_m,
		d.bmi_kgm2_v1,
		d.smoking,
		d.alcohol_intake,
		d.family_history,
		d.highrisk,
		d.medications,
		d.nutritional_counselling,
		gt.diagnosed_gdm
FROM glucose_tests gt
JOIN demographics d
USING (participant_id)
WHERE diagnosed_gdm = 1
ORDER BY RANDOM() -- retreives random rows from the table
LIMIT 10;

--**********************************************************************************************************************

-- Q53. Display list of patients with abnormal Alt_change % and diagnosed with vitamin D deficiency.

SELECT participant_id,
		alt_v3 AS abnormal_alt_visit_3,
		alt_change_percent AS abnormal_alt_change_percent,
		diagnosed_with_vitd_deficiency
FROM biomarkers
WHERE alt_v3 >= 25 --abnormal alt, which may result in abnormal alt change percent
	  AND diagnosed_with_vitd_deficiency = 1;

--**********************************************************************************************************************

-- Q54. What is the distribution of participants by ethnicity and their GDM  status (either 'gdm' or 'non-gdm') in the database?
SELECT d.ethnicity, 
		CASE --dispalying the values of diagnosed_gdm, 0 as Non-GDM 1 as GDM and null as Unknown
			WHEN gt.diagnosed_gdm = 0 THEN 'Non-GDM'
			WHEN gt.diagnosed_gdm = 1 THEN 'GDM'
		ELSE 'Unknown'
		END as GDM_status,
		COUNT(*) as count_of_patients
FROM demographics d
JOIN glucose_tests gt
USING (participant_id)
GROUP BY d.ethnicity, GDM_status
ORDER BY count_of_patients DESC;

--**********************************************************************************************************************

--Q55. Display all the details of 2nd tallest participant details using windows function

--defining CTE for assigning dense ranks to paticipant ids
WITH ranking_ids_by_height AS(
							SELECT *,
							DENSE_RANK() OVER (ORDER BY height_m DESC) AS rank_by_height
							FROM demographics
)

--getting the details of the id with rank 2
SELECT *
FROM ranking_ids_by_height
WHERE rank_by_height = 2;

--**********************************************************************************************************************

-- Q56. Create a trigger that raises a notice when trying to insert a duplicate participant_id into the demographics table. 
-- Provide a screenshot of the test result.

CREATE OR REPLACE FUNCTION compare_new_id_from_demographics()
RETURNS TRIGGER AS $$
BEGIN
	IF 
		NEW.participant_id IN (SELECT participant_id FROM demographics) --comparing the new inserted id with already existing ids in demographics
	THEN 
		RAISE NOTICE 'Participant ID % already exists', NEW.participant_id;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER check_participant_id
BEFORE INSERT ON demographics
FOR EACH ROW
EXECUTE FUNCTION compare_new_id_from_demographics();

INSERT INTO demographics(participant_id,
						ethnicity,
						age_above_30,
						height_m,
						bmi_kgm2_v1,
						smoking,
						alcohol_intake,
						family_history,
						highrisk,
						medications,
						nutritional_counselling
						)
VALUES(1,'White',1,1.56,27.25,'Never',0,0,0,0,0) --inserting duplicate id, so it should raise notice and no insertion should happen

--**********************************************************************************************************************

-- Q57. Compare the number of participants who signed the form on each day of the week and identify the day with the highest number of unique participants.

--comparing the number of participants who signed the form on each day of the week
SELECT EXTRACT(DOW FROM date_form_signed) AS day_of_the_week, --getting the day of the weekfrom the date_form_signed with SUNDAY being 0 and SATURDAY being 6
	 			COUNT(DISTINCT participant_id) AS count_of_participants
		FROM documentation_track
		GROUP BY day_of_the_week
		ORDER BY count_of_participants DESC

--identifying the day with the highest number of unique participants by use of CTE
WITH participant_counts_signed_form AS(
		SELECT TO_CHAR(date_form_signed, 'Day') AS day_of_the_week, --getting the day of the week in textual representation
	 			COUNT(DISTINCT participant_id) AS count_of_participants
		FROM documentation_track
		GROUP BY day_of_the_week
		ORDER BY count_of_participants DESC
)

SELECT  *
FROM participant_counts_signed_form
WHERE count_of_participants = (
								SELECT MAX(count_of_participants)
								FROM participant_counts_signed_form
);

--**********************************************************************************************************************

-- Q58. What is the standard deviation of 'U creatinine_V1'? Display the result in two decimal places.

--getting the standard deviation
SELECT ROUND(STDDEV(creatinine_v1), 2) AS standard_deviation_of_UcreatinineV1
FROM kidney_function;

--**********************************************************************************************************************

-- Q59. Create a Range Partition and show us how the partition is used in a Query.

--creating a partition TABLE
CREATE TABLE orders(
		order_id SERIAL NOT NULL,
		product VARCHAR(250),
		quantity INT,
		order_date DATE,
		CONSTRAINT id_date_pkey
			PRIMARY KEY (order_id, order_date)
		
) PARTITION BY RANGE (order_date);

--creating partitions by month
CREATE TABLE orders_january_2024
PARTITION OF orders
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE orders_february_2024
PARTITION OF orders
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE orders_march_2024
PARTITION OF orders
FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

CREATE TABLE orders_april_2024
PARTITION OF orders
FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE orders_may_2024
PARTITION OF orders
FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');

CREATE TABLE orders_june_2024
PARTITION OF orders
FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');

CREATE TABLE orders_july_2024
PARTITION OF orders
FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

CREATE TABLE orders_august_2024
PARTITION OF orders
FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

CREATE TABLE orders_september_2024
PARTITION OF orders
FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE orders_october_2024
PARTITION OF orders
FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE orders_november_2024
PARTITION OF orders
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE orders_december_2024
PARTITION OF orders
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

--displaying the range partitions
SELECT nsp_parent.nspname AS parent_schema,
    parent.relname AS parent_table,
    nsp_child.nspname AS partition_schema,
    child.relname AS partition_name,
    pg_get_expr(child.relpartbound, child.oid) AS partition_range --to extract partition range
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nsp_parent ON nsp_parent.oid = parent.relnamespace
JOIN pg_namespace nsp_child ON nsp_child.oid = child.relnamespace
WHERE parent.relname = 'orders'; --getting all child partitions of the orders table

--inserting the data into orders table
INSERT INTO orders(product, quantity, order_date)
VALUES('P1', 2, '2024-01-15'),
('P2', 2, '2024-01-28'),
('P5', 1, '2024-02-10'),
('P8', 2, '2024-03-03'),
('P3', 1, '2024-03-12'),
('P7', 3, '2024-04-09'),
('P10', 3, '2024-04-15'),
('P2', 4, '2024-05-20'),
('P1', 5, '2024-06-07'),
('P10', 2, '2024-06-18'),
('P5', 2, '2024-07-14'),
('P3', 1, '2024-07-22'),
('P6', 2, '2024-08-17'),
('P1', 6, '2024-08-23'),
('P2', 1, '2024-09-15'),
('P4', 2, '2024-09-24'),
('P6', 4, '2024-10-10'),
('P7', 2, '2024-10-19'),
('P9', 1, '2024-11-03'),
('P3', 2, '2024-11-25'),
('P10', 2, '2024-12-05'),
('P3', 6, '2024-12-20');



--retreiving data 
SELECT * FROM orders WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'

--expaling the query that will only access the relevant partition for January 2024, through explain clause. 
EXPLAIN SELECT * FROM orders WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'

--displaying the data from orders_january_2024;
SELECT * FROM orders_january_2024;

--this query will only access the relevant partition for June 2024. 
SELECT * FROM orders WHERE order_date BETWEEN '2024-06-01' AND '2024-06-30'


--**********************************************************************************************************************

-- Q60. Calculate the BMI for Visit 3 and Display the Highest BMI  and their participant details.
SELECT * FROM body_compositions_view where participant_id =1
SELECT * FROM demographics
--creating view to get the distinct participants from body_compositions table
CREATE OR REPLACE VIEW body_compositions_view AS
SELECT DISTINCT * 
FROM body_compositions

--creating view to get all the rows of demographics table
CREATE OR REPLACE  VIEW demographics_view AS
SELECT * 
FROM demographics

--creating a view to join above views
CREATE OR REPLACE VIEW body_compositions_demographics_view AS
SELECT * FROM body_compositions_view
JOIN demographics_view
USING (participant_id)


--calculating the BMI for Visit 3	
SELECT participant_id,
		ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) AS bmi_kgm2_v3
FROM body_compositions_demographics_view 
ORDER BY bmi_kgm2_v3 DESC;

--getting the highest BMI and their participants details with the use of CTE
WITH ids_with_bmi_v1_v3 AS(
			SELECT participant_id,
					ethnicity,
					age_above_30,
					height_m,
					bmi_kgm2_v1,
					smoking,
					alcohol_intake,
					family_history,
					highrisk,
					medications,
					nutritional_counselling,
					ROUND((weight_v3/POWER(height_m, 2))::NUMERIC, 2) AS bmi_kgm2_v3
			FROM body_compositions_demographics_view
)

SELECT * FROM ids_with_bmi_v1_v3
WHERE bmi_kgm2_v3 = (SELECT MAX(bmi_kgm2_v3)
					FROM ids_with_bmi_v1_v3					
);

--**********************************************************************************************************************

-- Q61. How do we gather statistics of table and check when it was done before.

--gathering the statistics of dempgraphics table
ANALYZE 
demographics;

--displaying the statistics of demographics table
SELECT * FROM pg_stats
WHERE tablename = 'demographics';

-- checking the time when last analyzing was done for demographics table
SELECT relname,
		last_analyze,
		last_autoanalyze
FROM pg_stat_all_tables --view to view information about tables 
WHERE relname = 'demographics';

--**********************************************************************************************************************

/* Q62. Create a stored procedure that calculates the average OGTT value and compares it against a specified glucose threshold. 
 If the average exceeds the threshold, classify the participant as "Gestational diabetes is suspected" */
SELECT * FROM glucose_tests
--creating the stored procedure
CREATE OR REPLACE PROCEDURE avg_OGTT_for_participants(p_id INT, threshold NUMERIC)
LANGUAGE plpgsql
AS $$
DECLARE 
	avg_OGTT DOUBLE PRECISION = 0; --variable for average OGTT
BEGIN
	SELECT
			(COALESCE("0H_OGTT_Value",0)+ --coalesce for handlng the null values
 			COALESCE("1H_OGTT_Value",0)+
		 	COALESCE("2H_OGTT_Value",0))/
			NULLIF((CASE WHEN "0H_OGTT_Value" IS NULL THEN 0 ELSE 1 END)+ 
			(CASE WHEN "1H_OGTT_Value" IS NULL THEN 0 ELSE 1 END)+
			(CASE WHEN "2H_OGTT_Value" IS NULL THEN 0 ELSE 1 END))
	INTO avg_OGTT
 	FROM glucose_tests
	 WHERE participant_id = p_id;

	IF 
		avg_OGTT >= threshold --comparing the average OGTT value with specified glucose threshold
	THEN 
		RAISE NOTICE 'Gestational diabetes is suspected for patient id %', p_id;
	ELSE 
		RAISE NOTICE 'Patient id % is not suspected for gestational diabetes', p_id;
	END IF;
		
END;
$$;

--calling the stored procedure
CALL avg_OGTT_for_participants(425, 6.0);

SELECT * FROM glucose_tests
WHERE diagnosed_gdm = 1
--**********************************************************************************************************************

/* Q63. Calculate Number of days difference between expected delivery date and ultrasound EDD
*/

SELECT participant_id,
		edd_v1,
		"US EDD",
		edd_v1 - "US EDD" AS difference_of_days --subtracting two dates will give the difference of days between the dates
FROM pregnancy_info
JOIN documentation_track
USING (participant_id)
ORDER BY participant_id





--**********************************************************************************************************************

/* Q64. Estimate the follow up visit dates for all participants for each trimester and display them.
*/

SELECT conception_date, 
	(conception_date + INTERVAL '8 weeks')::DATE AS trimester1_visit1, --first trimester visit 1 at week 8
	(conception_date + INTERVAL '10 weeks')::DATE AS trimester1_visit2, --first trimester visit 2 at week 10
	(conception_date + INTERVAL '12 weeks')::DATE AS trimester1_visit3, --first trimester visit 3 at week 12
	(conception_date + INTERVAL '16 weeks')::DATE AS trimester2_visit1, --second trimester visit 1 at week 16
	(conception_date + INTERVAL '20 weeks')::DATE AS trimester2_visit2, --second trimester visit 2 at week 20
	(conception_date + INTERVAL '24 weeks')::DATE AS trimester2_visit3, --second trimester visit 3 at week 24
	(conception_date + INTERVAL '28 weeks')::DATE AS trimester3_visit1, --third trimester visit 1 at week 28
	(conception_date + INTERVAL '32 weeks')::DATE AS trimester3_visit2, --third trimester visit 2 at week 32
	(conception_date + INTERVAL '36 weeks')::DATE AS trimester3_visit3, --third trimester visit 3 at week 36
	(conception_date + INTERVAL '40 weeks')::DATE AS trimester3_visit4, --third trimester visit 4 at week 40
	edd_v1
FROM pregnancy_info



--**********************************************************************************************************************

-- Q9. Display participants with a significant increase (greater than 20%) in both creatinine and urine albumin levels between visit 1 and visit 3.


SELECT participant_id,
		creatinine_v1,
		creatinine_v3,
		ROUND((((creatinine_v3 ::NUMERIC - creatinine_v1::NUMERIC)/creatinine_v1::NUMERIC)*100),2) AS creatinine_change_percentage,
		"U Albumin_V1",
		"U Albumin_V3",
		ROUND(((("U Albumin_V3" - "U Albumin_V1")/"U Albumin_V1")*100)::NUMERIC, 2) AS Ualbumin_change_percentage
FROM kidney_function
WHERE ROUND((((creatinine_v3 ::NUMERIC - creatinine_v1::NUMERIC)/creatinine_v1::NUMERIC)*100),2) > 20
	AND ROUND(((("U Albumin_V3" - "U Albumin_V1")/"U Albumin_V1")*100)::NUMERIC, 2) > 20


--SELECT * FROM kidney_function

--**********************************************************************************************************************

-- Q5. Create a trigger on the Demographics table that monitors and logs all INSERT, UPDATE, and DELETE operations performed on the table.

-- creating the table to log the operations performed in the the demographics.
CREATE TABLE demographics_audit (
audit_id SERIAL PRIMARY KEY,
participant_id INT,
operation_performed VARCHAR(50),
operation_performed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
user_name VARCHAR(50) DEFAULT CURRENT_USER
);

--DROP TABLE demographics_audit

--creating a trigger function that adds the rows in the demographics_audit table when some operation is performed on demographics table
CREATE OR REPLACE FUNCTION log_operation_performed()
RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'INSERT' THEN --use of special variable to log the operation performed
		INSERT INTO demographics_audit(
		participant_id,
		operation_performed
		)
		VALUES (
		NEW.participant_id, --id of newly inserted participant in the demogarphics
		'INSERT'
		);
		RETURN NEW; 
		
	ELSEIF TG_OP = 'UPDATE' THEN
		INSERT INTO demographics_audit(
		participant_id,
		operation_performed
		)
		VALUES (
		OLD.participant_id, --id of updated participant in the demogarphics
		'UPDATE'
		);
		RETURN OLD;
		
	ELSEIF TG_OP = 'DELETE' THEN
		INSERT INTO demographics_audit(
		participant_id,
		operation_performed
		)
		VALUES (
		OLD.participant_id, --id of deleted participant in the demogarphics
		'DELETE'
		);
		RETURN OLD;
	END IF;
	
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER log_operation_on_demographics
AFTER INSERT OR UPDATE OR DELETE ON demographics
FOR EACH ROW 
EXECUTE FUNCTION log_operation_performed();

DROP TRIGGER log_operation_on_demographics ON demographics

--inserting 2 new rows in demographics table to check insertion is being logged in the demographics_audit table
INSERT INTO demographics(participant_id,
						ethnicity,
						age_above_30,
						height_m,
						bmi_kgm2_v1,
						smoking,
						alcohol_intake,
						family_history,
						highrisk,
						medications,
						nutritional_counselling
						)
VALUES(610,'White',1,1.56,27.25,'Never',0,0,0,0,0),
(611,'White',1,1.36,25.13,'Never',0,0,0,0,0);



SELECT * FROM demographics_audit --displaying the operation logged in the demographics_audit table 

--updating the participant 611 ethnicity to check updation is being logged in the demographics_audit table
UPDATE demographics
SET ethnicity = 'White'
WHERE participant_id = 611;



SELECT * FROM demographics_audit --displaying the operation logged in the demographics_audit table

--deleting the participant 611 to check deletion is being logged in the demographics_audit table
DELETE FROM demographics
WHERE participant_id =611;


SELECT * FROM demographics_audit --displaying the operation logged in the demographics_audit table

--**********************************************************************************************************************

-- Q65. Compare the average change in hemoglobin levels based on ethnicity using window function 
select distinct on (ethnicity)
	ethnicity, hb_v3, hb_v1,
	round(avg(hb_v3 - hb_v1) OVER (partition by ethnicity )::numeric,2)
as avg_hemoglobin_change
from demographics join biomarkers
using (participant_id)

--**********************************************************************************************************************

-- Q66. Create a function to load data from an existing table into a new table, inserting records in batches of 100.

--creating a new table for loading the data from demographics 
CREATE TABLE demographics_batch_load (
participant_id INT PRIMARY KEY,
ethnicity VARCHAR(50),
age_above_30 INT ,
height_m DOUBLE PRECISION,
bmi_kgm2_v1 DOUBLE PRECISION,
smoking TEXT,
alcohol_intake INT,
family_history INT,
highrisk INT,
medications INT,
nutritional_counselling INT,
high_risk_pregnancy boolean
);

DROP TABLE demographics_batch_load
SELECT  * FROM demographics



CREATE OR REPLACE FUNCTION load_next_batch()
RETURNS BOOLEAN AS $$
DECLARE
    batch_size INT := 100;
BEGIN
    INSERT INTO demographics_batch_load (
        participant_id, ethnicity, age_above_30, height_m, bmi_kgm2_v1,
        smoking, alcohol_intake, family_history, highrisk,
        medications, nutritional_counselling, high_risk_pregnancy
    )
    SELECT d.participant_id, d.ethnicity, d.age_above_30, d.height_m, d.bmi_kgm2_v1,
           d.smoking, d.alcohol_intake, d.family_history, d.highrisk,
           d.medications, d.nutritional_counselling, d.high_risk_pregnancy
    FROM demographics d
    LEFT JOIN demographics_batch_load b
      ON d.participant_id = b.participant_id
    WHERE b.participant_id IS NULL --left join and condition IS NULL ensures only next 100 rows are inserted
    ORDER BY d.participant_id
    LIMIT batch_size; --restricts the number of rows as 100 per call

    RETURN FOUND; -- Return TRUE if rows were inserted, FALSE if done
END;
$$ LANGUAGE plpgsql;

--calling the function and inserting 100 rows on each call
SELECT load_next_batch();

--calling the function in a loop
DO $$
BEGIN
    WHILE load_next_batch() LOOP
        RAISE NOTICE 'Inserted 100 rows';
    END LOOP;
END $$;

SELECT * FROM demographics_batch_load


--**********************************************************************************************************************

-- Q65. Show the position of letter 'n' in the insulin_metformnin column.Replace blank values to Unknown.List only distinct values.Hint:'n' is not case sensitive

SELECT
  DISTINCT ON (insulin_metformnin) insulin_metformnin,
  ARRAY(   --collects the postions of character 'n' into an array
    SELECT i
    FROM GENERATE_SERIES(1, CHAR_LENGTH(insulin_metformnin)) AS i -- loops over character positions in insulin_metformnin
    WHERE SUBSTRING(LOWER(insulin_metformnin) FROM i FOR 1) = 'n' --substring() gets one character at position i in insulin_metformnin and filters to the character 'n'
  ) AS n_positions
FROM glucose_tests;


SELECT * FROM SCREENING
WHERE ghp =1

SELECT * FROM maternal_health_info
where "Pre-eclampsia"=1


--**********************************************************************************************************************

-- Q12. Create materialized view, calculate and categorize MAP for all participants. Display the ranking and analyze the distribution across MAP categories.
CREATE MATERIALIZED VIEW map_for_all_patients AS (
					SELECT participant_id ,
       						((systolic_bp_v3+ 2*diastolic_bp_v3)/3) AS MAP_visit_3,
	   						CASE --categorizing MAP
	   							WHEN ((systolic_bp_v3+ 2*diastolic_bp_v3)/3) IS NULL THEN NULL
								WHEN ((systolic_bp_v3+ 2*diastolic_bp_v3)/3) < 70 THEN 'Low MAP (Hypotension)'
								WHEN ((systolic_bp_v3+ 2*diastolic_bp_v3)/3) > 100 THEN 'High MAP (Hypertension)'
							ELSE 'Normal MAP'
							END AS "MAP_categories"
					FROM vital_signs
);

-- dropping the view to rerun
--DROP MATERIALIZED VIEW map_for_all_patients;

--using CTE for getting the counts in each MAP category and then ranking them in the main query
WITH map_categories_count AS (
					SELECT DISTINCT ON ("MAP_categories") "MAP_categories",
							COUNT(participant_id) OVER (PARTITION BY "MAP_categories") AS count_of_participants
					FROM map_for_all_patients
					WHERE "MAP_categories" IS NOT NULL
)
SELECT *,
		DENSE_RANK() OVER ( ORDER BY count_of_participants DESC ) AS ranking
FROM map_categories_count;


--**********************************************************************************************************************

-- Q79. Create a stored procedure to fetch past and current GDM status and their birth outcome. Call the procedure recursively. If the participant GDM is 'Yes'.
CREATE OR REPLACE PROCEDURE fetch_participant_GDMstatus_birthoutcomes(  p_id INT,
														INOUT past_GDM TEXT,
														INOUT current_GDM TEXT,
														INOUT infant_scbu TEXT,
														INOUT pre_term TEXT,
														INOUT infant_birth_weight DOUBLE PRECISION,
														INOUT miscarraige TEXT
														)
LANGUAGE plpgsql
AS $$
BEGIN
		-- cheking if the participant id exists
		IF NOT EXISTS (SELECT 1 FROM glucose_tests WHERE participant_id =p_id) THEN
       	    RAISE EXCEPTION 'Participant ID % does not exist', p_id ;
    	END IF;
			
		SELECT
			CASE 
				WHEN previous_gdm = 1 THEN 'Yes'
			 	WHEN previous_gdm = 0 THEN 'No'
			ELSE 'UNKNOWN'
			END AS previous_gdm,
			
			CASE 
				WHEN diagnosed_gdm = 1 THEN 'Yes'
				WHEN diagnosed_gdm = 0 THEN 'No'
			ELSE 'UNKNOWN'
			END AS diagnosed_gdm, 
			
			CASE 
				WHEN scbu = 1 THEN 'Yes'
				WHEN scbu = 0 THEN 'No'
			ELSE 'UNKNOWN'
			END AS scbu, 
			
			CASE 
				WHEN delivered_before_36_weeks = 1 THEN 'Yes'
				WHEN delivered_before_36_weeks = 0 THEN 'No'
			ELSE 'UNKNOWN'
			END AS delivered_before_36_weeks,
			
			birth_weight,
			
			CASE 
				WHEN "Miscarried 10" = 1 THEN 'Yes'
				WHEN "Miscarried 10" = 0 THEN 'No'
			ELSE 'UNKNOWN'
			END AS "Miscarried 10"
		INTO past_GDM, current_GDM, infant_scbu, pre_term, infant_birth_weight, miscarraige
 		FROM glucose_tests g
		JOIN pregnancy_info
		USING (participant_id)
		JOIN screening
		USING (participant_id)
		JOIN maternal_health_info
		USING (participant_id)
		JOIN infant_outcomes
		USING (participant_id)
		WHERE g.participant_id = p_id;
		
		IF past_GDM = 'Yes' AND current_GDM='Yes' THEN
			RAISE NOTICE 'Participant % has past and current GDM status as Yes and the birth outcomes are Infanct scbu: %, Preterm: %, Infant Birth Weight: %, Miscarraige: %', p_id,infant_scbu, pre_term, infant_birth_weight, miscarraige;
			CALL fetch_participant_GDMstatus_birthoutcomes(p_id, past_GDM, current_GDM, infant_scbu, pre_term, infant_birth_weight, miscarraige);
		ELSE 
			RAISE NOTICE 'Participant % has past and current GDM status as No', p_id;
		END IF;
		
END;
$$;

--calling the procedure with participant GDM as yes
CALL fetch_participant_GDMstatus_birthoutcomes(433, NULL, NULL, NULL, NULL, NULL, NULL);

--calling the procedure with participant GDM as not yes
CALL fetch_participant_GDMstatus_birthoutcomes(1, NULL, NULL, NULL, NULL, NULL, NULL);

DROP PROCEDURE fetch_gdmstatus_birthoutcome(integer,text,text,text,text,double precision,text)

CALL fetch_GDMstatus_birthoutcomes(NULL,NULL, NULL, NULL, NULL, NULL,NULL)

SELECT * FROM glucose_tests

SELECT * FROM infant_outcomes


CREATE OR REPLACE PROCEDURE fetch_pastcurrentGDM_birthoutcomes_recursive(p_index INT DEFAULT 1)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
BEGIN
    SELECT *
    INTO rec
    FROM (
        SELECT 
            ROW_NUMBER() OVER () AS row_num,
            g.participant_id,
            CASE WHEN previous_gdm = 1 THEN 'Yes' WHEN previous_gdm = 0 THEN 'No' ELSE 'UNKNOWN' END AS past_GDM,
            CASE WHEN diagnosed_gdm = 1 THEN 'Yes' WHEN diagnosed_gdm = 0 THEN 'No' ELSE 'UNKNOWN' END AS current_GDM,
            CASE WHEN scbu = 1 THEN 'Yes' WHEN scbu = 0 THEN 'No' ELSE 'UNKNOWN' END AS infant_scbu,
            CASE WHEN delivered_before_36_weeks = 1 THEN 'Yes' WHEN delivered_before_36_weeks = 0 THEN 'No' ELSE 'UNKNOWN' END AS pre_term,
            birth_weight AS infant_birth_weight,
            CASE WHEN "Miscarried 10" = 1 THEN 'Yes' WHEN "Miscarried 10" = 0 THEN 'No' ELSE 'UNKNOWN' END AS miscarraige
        FROM glucose_tests g
        JOIN pregnancy_info USING (participant_id)
        JOIN screening USING (participant_id)
        JOIN maternal_health_info USING (participant_id)
        JOIN infant_outcomes USING (participant_id)
    ) AS numbered
    WHERE row_num = p_index;

    -- Stop if no row found (end of data)
    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Your condition
    IF rec.past_GDM = 'Yes' AND rec.current_GDM = 'Yes' THEN
        RAISE NOTICE 'Match Found: ID %, GDM = Yes/Yes, SCBU: %, PreTerm: %, Weight: %, Miscarriage: %',
            rec.participant_id, rec.infant_scbu, rec.pre_term, rec.infant_birth_weight, rec.miscarraige;
    END IF;

    -- Recurse to next row
    CALL fetch_pastcurrentGDM_birthoutcomes_recursive(p_index + 1);
END;
$$;

CALL fetch_pastcurrentGDM_birthoutcomes_recursive();
DROP PROCEDURE fetch_pastcurrentGDM_birthoutcomes_recursive(int)
--CALL fetch_pastcurrentGDM_birthoutcomes();

SELECT * FROM pregnancy_info WHERE still-birth = 1
SELECT * FROM glucose_tests
SELECT * FROM screening
SELECT * FROM maternal_health_info

CREATE OR REPLACE PROCEDURE fetch_pastcurrentGDM_birthoutcomes()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT 
            g.participant_id,
            CASE WHEN previous_gdm = 1 THEN 'Yes' ELSE 'No' END AS past_GDM,
            CASE WHEN diagnosed_gdm = 1 THEN 'Yes' ELSE 'No' END AS current_GDM,
            CASE WHEN scbu = 1 THEN 'Yes' ELSE 'No' END AS infant_scbu,
            CASE WHEN delivered_before_36_weeks = 1 THEN 'Yes' ELSE 'No' END AS pre_term,
            birth_weight AS infant_birth_weight,
            CASE WHEN "Miscarried 10" = 1 THEN 'Yes' ELSE 'No' END AS miscarraige
        FROM glucose_tests g
        JOIN pregnancy_info USING (participant_id)
        JOIN screening USING (participant_id)
        JOIN maternal_health_info USING (participant_id)
        JOIN infant_outcomes USING (participant_id)
    LOOP
        IF rec.past_GDM = 'Yes' AND rec.current_GDM = 'Yes' THEN
            RAISE NOTICE 'Participant % matched', rec.participant_id;
            -- Do whatever you want here (call other procs, etc)
        END IF;
    END LOOP;
END;
$$;


call fetch_pastcurrentGDM_birthoutcomes()
DROP PROCEDURE fetch_pastcurrentGDM_birthoutcomes()
CALL fetch_pastcurrentGDM_birthoutcomes_recursive(1, 700);

--*******************************************************************************************************************

--*********************************************EXTRA QUESTIONS*************************************************************

--1. Display all the details of 3rd highest weight in visit 3, participant details

WITH ranking_ids_by_weight_v3 AS(
							SELECT d.participant_id, 
									ethnicity,
									age_above_30 ,
									height_m ,
									bmi_kgm2_v1 ,
									smoking ,
									alcohol_intake ,
									family_history ,
									highrisk ,
									medications ,
									nutritional_counselling,
									weight_v3,
							DENSE_RANK() OVER (ORDER BY weight_v3 DESC) AS rank_by_weight_v3
							FROM body_compositions b
							JOIN demographics d
							USING (participant_id)
							WHERE weight_v3 IS NOT NULL
)

--getting the details of the id with rank 3
SELECT *
FROM ranking_ids_by_weight_v3
WHERE rank_by_weight_v3 = 3;

--2 Create a stored procedure to accept the participant_id and calculate its BMI for visit 3 and display which category does the participant fall under?

--procedure for calculating BMI and categorizing
CREATE OR REPLACE PROCEDURE calculate_bmi_and_categorize(p_id int)
LANGUAGE plpgsql
AS $$
DECLARE 
	bmi DOUBLE PRECISION;
BEGIN
	SELECT participant_id,
		ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) AS bmi_kgm2_v3
	--INTO bmi
	FROM body_compositions b
	JOIN demographics d
	USING(participant_id)
	WHERE b.participant_id = p_id;

	IF bmi < 18.5 
	THEN RAISE NOTICE 'Underweight';
	ELSEIF bmi >= 18.5 AND bmi < 25
	THEN RAISE NOTICE 'Normal Weight';
	ELSEIF bmi >= 25 AND bmi < 30
	THEN RAISE NOTICE 'Overweight';
	ELSEIF bmi >= 30 
	THEN RAISE NOTICE 'Obesity';
	ELSE RAISE NOTICE 'weight for the participant is NULL so category cannot be defined';
	END IF;
END;
$$;


--calling the PROCEDURE
CALL calculate_bmi_and_categorize(268);


--***************************************************************************
WITH bmi AS (SELECT (participant_id),
		ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) AS bmi_kgm2_v3,
	--INTO bmi  
	CASE WHEN ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) < 18.5 THEN 'Underweight'
		WHEN ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) >= 18.5 AND ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) < 25 THEN 'Normal Weight' 
		WHEN ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) >= 25 AND ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) < 30 THEN 'Overweight'
		WHEN ROUND((weight_v3/ POWER( height_m, 2))::NUMERIC, 2) >= 30 THEN 'Obesity'
	ELSE 'Unknown'
	END AS BMI_category
	FROM body_compositions b
	JOIN demographics d
	USING(participant_id)
	)

SELECT count(*),  BMI_category
FROM bmi 
GROUP BY BMI_category




	

	