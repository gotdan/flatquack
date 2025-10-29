CREATE OR REPLACE MACRO anon_date_to_year(date) AS (
	CASE
		-- Handle null or invalid date strings
		WHEN date IS NULL OR len(date) < 4 THEN NULL
		-- Extract first 4 characters (year)
		ELSE substring(date, 1, 4)
	END
);

CREATE OR REPLACE MACRO anon_redact_string(str) AS (
	CASE
		-- Handle null strings
		WHEN str IS NULL THEN NULL
		-- Replace with five asterisks
		ELSE '*****'
	END
);

CREATE OR REPLACE MACRO anon_is_usa(country) AS (
	CASE 
		WHEN (
			lower(country) IN ('usa', 'us', 'u.s.a', 'u.s.', 'u.s.a.') OR
			lower(country) LIKE '%united states%'
		) THEN true
		ELSE false
	END
);

CREATE OR REPLACE MACRO anon_zip(postalCode) AS (
	CASE
		WHEN postalCode IS NOT NULL AND regexp_matches(postalCode, '^[0-9]{5}') THEN
			-- Omit if in low-population ZIP code list (≤10,000 people based on 2010 census)
			CASE 
				WHEN substring(postalCode, 1, 3) IN (
					'036', '059', '102', '203', '205', '369', '556', 
					'692', '821', '823', '878', '879', '884', '893'
				) THEN NULL
				ELSE substring(postalCode, 1, 3)
			END
		ELSE NULL
	END
);

CREATE OR REPLACE MACRO anon_hash(str, pepper) AS (
	CASE WHEN str IS NULL THEN NULL
	ELSE sha256(str || coalesce(pepper, ''))
	END
);